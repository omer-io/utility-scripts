import os
import json
import time
import boto3
import shutil
import gspread
import logging
import argparse
import subprocess
from pathlib import Path
from botocore.config import Config
from oauth2client.service_account import ServiceAccountCredentials

S3_CLIENT_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "standard"} 
)

def extract_metrics_from_log(log_file_path):
    try:
        logging.debug(f"📄 Extracting metrics from log file: {log_file_path}")
        # Extract block compute units → select last 4
        cu_cmd = (
            f"grep 'simulated bank slot+delta' {log_file_path} | "
            "grep '(frozen)' | "
            "awk -F 'costs: | fees:' '{print $2}' | "
            "tail -n 4 | "
            "awk -F '[(),]' '{gsub(/ /, \"\", $2); print $2}'"
        )
        cu_output = subprocess.check_output(cu_cmd, shell=True, text=True).strip().split("\n")
        block_cu = [int(val) for val in cu_output if val]
        logging.info(f"📊 Extracted block compute units: {block_cu}")
        if len(block_cu) != 4:
            raise ValueError(f"Expected 4 compute unit entries, got {len(block_cu)}: {block_cu}")

        # Extract block rewards → select last 4
        reward_cmd = f"grep 'bank frozen' {log_file_path} | awk '{{print $8}}' | sed 's/,//g'"
        reward_output = subprocess.check_output(reward_cmd, shell=True, text=True).strip().split("\n")
        block_rewards = reward_output[-4:]
        logging.info(f"📊 Extracted block rewards: {block_rewards}")
        if len(block_rewards) != 4:
            raise ValueError(f"Expected 4 block reward entries, got {len(block_rewards)}: {block_rewards}")
        
        # Extract Total Jito tips
        tips_cmd = (
            f"grep 'Total Jito tip account balance before:' {log_file_path} | "
            "sed -n 's/.*Total tips: \\([0-9]\\+\\) lamports.*/\\1/p' | tail -n 1"
        )
        tips_output = subprocess.check_output(tips_cmd, shell=True, text=True).strip()
        total_tips = int(tips_output) if tips_output else 0
        logging.info(f"💰 Extracted Total Jito tips: {total_tips}")

        return list(map(int, block_cu)), list(map(int, block_rewards)), total_tips

    except (subprocess.CalledProcessError, ValueError) as e:
        logging.error(f"❌ Failed to extract metrics from log: {e}")
        return [], [], 0

def upload_to_sheet(sheet_id, first_slot, test_name, block_cu, block_rewards, total_tips, log_file_path):
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open_by_key(sheet_id)

    try:
        sheet = spreadsheet.worksheet(str(first_slot))
        logging.info(f"📄 Found existing worksheet: {first_slot}")

        empty_row = ["--"]
        sheet.append_row(empty_row, value_input_option='USER_ENTERED')
    
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=str(first_slot), rows="1000", cols="200")
        logging.info(f"🆕 Created new worksheet: {first_slot}")

        header = ["TestName", "FirstSlot"]
        header += ["--"]
        header += [f"CU-{i+1}" for i in range(len(block_cu))]
        header += ["AvgCU"]
        header += ["--"]
        header += [f"Reward-{i+1}" for i in range(len(block_rewards))]
        header += ["SumReward"]
        header += ["Tips"]
        sheet.append_row(header, value_input_option='USER_ENTERED')

        format_as_num_start = header.index("FirstSlot")
        format_as_num_end = format_as_num_start + 14
        format_requests = {
            "requests": [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet._properties['sheetId'],
                            "startRowIndex": 0,
                            "endRowIndex": 1
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "bold": True
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat.bold"
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet._properties['sheetId'],
                            "startRowIndex": 1,  # skip header row
                            "startColumnIndex": format_as_num_start,
                            "endColumnIndex": format_as_num_end
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {
                                    "type": "NUMBER",
                                    "pattern": "#,##0"
                                }
                            }
                        },
                        "fields": "userEnteredFormat.numberFormat"
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet._properties['sheetId'],
                            "gridProperties": {
                                "frozenRowCount": 1,
                                "frozenColumnCount": 2
                            }
                        },
                        "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet._properties['sheetId'],
                            "startRowIndex": 0,
                            "startColumnIndex": 0,
                            "endColumnIndex": 20
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "horizontalAlignment": "CENTER"
                            }
                        },
                        "fields": "userEnteredFormat.horizontalAlignment"
                    }
                }
            ]
        }
        sheet.spreadsheet.batch_update(format_requests)

    cu_sum = sum(block_cu)
    cu_avg = round(cu_sum / len(block_cu), 2)

    reward_sum = sum(block_rewards)
    # reward_avg = round(reward_sum / len(block_rewards), 2)

    row = [test_name, first_slot] + ["--"] + block_cu + [cu_avg] + ["--"] + block_rewards + [reward_sum] + [total_tips]
    sheet.append_row(row, value_input_option='USER_ENTERED')

    logging.info(f"📤 Uploaded results for slot {first_slot}, test name {test_name} to Google Sheet: {sheet_id}, tab name: {first_slot}")

    try:
        log_parser_cmd = ['./logs_parser.sh', log_file_path, f"{test_name}_{first_slot}"]
        subprocess.run(log_parser_cmd, check=True)
        logging.info(f"📄 Ran logs_parser.sh on {log_file_path} for tab {test_name}_{first_slot}")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Failed to run logs_parser.sh: {e}")

def clean_download_dir(download_path, keep_dirs):
    for item in os.listdir(download_path):
        item_path = os.path.join(download_path, item)
        if item in keep_dirs:
            continue
        try:
            if os.path.isdir(item_path):
                logging.info(f"🧹 Removing directory: {item_path}")
                shutil.rmtree(item_path)
            else:
                logging.info(f"🧽 Removing file: {item_path}")
                os.remove(item_path)
        except Exception as e:
            logging.warning(f"⚠️ Failed to remove {item_path}: {e}")

def download_snapshot(s3, bucket, full_prefix, local_base_dir):
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=full_prefix):
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            s3_key = obj['Key']
            relative_path = s3_key[len(full_prefix):]
            if not relative_path:  # skip directory itself
                continue
            local_path = os.path.join(local_base_dir, relative_path)
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, s3_key, local_path)

def simulate_snapshot(snapshot_dir, first_slot, name, log_dir, repo_path, test_name, sheet_id):
    os.makedirs(log_dir, exist_ok=True)
    log_filename = f"{first_slot}_{test_name}.log" if test_name else f"{first_slot}.log"
    log_file_path = os.path.join(log_dir, log_filename)
    ledger_tool_path = os.path.join(repo_path, 'target', 'release', 'agave-ledger-tool')

    cmd = [
        ledger_tool_path,
        '-l', snapshot_dir,
        'simulate-block-production',
        '--first-simulated-slot', str(first_slot)
    ]

    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = f"{env.get('LD_LIBRARY_PATH', '')}:{os.path.join(repo_path, 'target/release')}"

    logging.info(f'Running simulation for {name}... Logging to {log_file_path}')

    with open(log_file_path, 'w') as log_file:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=None,
            env=env,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        try:
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
                if "Sleeping a bit before signaling exit" in line:
                    logging.info(f"🟡 Detected shutdown log in {name}")
                    logging.info(f"🔴 Terminating {name} due to exit signal...")
                    time.sleep(10)
                    process.terminate()
                    process.wait()
                    break

            exit_code = process.wait()
            if exit_code == 0 or exit_code == -15 or exit_code == 101:
                logging.info(f"✅ Simulation completed for {name}")
                block_cu, block_rewards, total_tips = extract_metrics_from_log(log_file_path)
                logging.info(f"📊 Block CU: {block_cu}, Block Rewards: {block_rewards}, Total Tips: {total_tips}")
                if block_cu and block_rewards:
                    upload_to_sheet(sheet_id, first_slot, test_name, block_cu, block_rewards, total_tips, log_file_path)
                else:
                    logging.error(f"❌ No valid metrics extracted from log for {name}. Check the log file: {log_file_path}")
            else:
                logging.error(f"❌ Simulation failed for {name} with exit code {exit_code}")

        except Exception as e:
            logging.error(f"❌ Error while running simulation for {name}: {e}")
            process.terminate()
            process.wait()

    # Cleanup
    try:
        snapshot_parent_dir = Path(snapshot_dir).parent
        snapshot_dirs = [d for d in snapshot_parent_dir.iterdir() if d.is_dir()]
        # if len(snapshot_dirs) > 5:
        #     logging.info(f"🧹 Removing entire snapshot directory for {name} since there are more than 4 snapshot dirs")
        #     shutil.rmtree(snapshot_dir)
        # else:
        cleanup_cmd = ["rm", "-rf", "accounts", "ledger_tool", "snapshot", "banking_retrace"]
        subprocess.run(cleanup_cmd, cwd=snapshot_dir)
        logging.info(f"🧽 Cleaned up inner dirs of snapshot {name}")
    except Exception as e:
        logging.error(f"⚠️ Cleanup failed for {name}: {e}")

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    parser = argparse.ArgumentParser(description="Run snapshot simulations")
    parser.add_argument('--snapshot_dir', help='Name of the snapshot directory to simulate')
    parser.add_argument('--slot', type=int, help='First simulated slot for the snapshot')
    parser.add_argument('--test_name', help='Override test name')
    args = parser.parse_args()

    with open("config.json") as f:
        config = json.load(f)

    session = boto3.Session()
    s3 = session.client('s3', config=S3_CLIENT_CONFIG)
    bucket = config['bucket']
    prefix = config['prefix']
    download_path = config['download_path']
    repo_path = config['test_repo_path']
    test_name = args.test_name if args.test_name else config.get('test_name', '')
    sheet_id = config['spreadsheet_id']
    log_dir = os.path.join(repo_path, 'simulation_logs')

    # Decide input mode: CLI args or config
    if args.snapshot_dir and args.slot:
        entries = [{"name": args.snapshot_dir.rstrip('/'), "first_simulated_slot": args.slot}]
        logging.info(f"📦 Running single snapshot from CLI args: snapshot dir={args.snapshot_dir} first slot={args.slot}")
    else:
        entries = [
            {"name": entry["name"].rstrip('/'), "first_simulated_slot": entry["first_simulated_slot"]}
            for entry in config["directories"]
        ]
        logging.info(f"📦 Running batch simulation for {len(entries)} directories from config.json")
        # keep_dirs = [entry["name"].rstrip('/') for entry in config["directories"]]
        # clean_download_dir(download_path, keep_dirs)
    
    for entry in entries:
        name = entry['name'].rstrip('/')
        first_slot = entry['first_simulated_slot']
        full_prefix = prefix + name + "/"
        local_dir = os.path.join(download_path, name)

        if os.path.exists(local_dir):
            logging.info(f"⏭️ Skipping downloading {name}: already exists at {local_dir}")
        else:
            logging.info(f"⬇️ Downloading snapshot {name} from S3...")
            download_snapshot(s3, bucket, full_prefix, local_dir)

        simulate_snapshot(local_dir, first_slot, name, log_dir, repo_path, test_name, sheet_id)

    logging.info("✅ All simulations completed.")
if __name__ == "__main__":
    main()
