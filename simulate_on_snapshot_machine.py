import os
import json
import time
import shutil
import gspread
import logging
import argparse
import subprocess
from pathlib import Path
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials

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

def update_sheets(current_epoch):
    """
    Create new summary and logs Google Sheets, give 'rakurai.io' domain edit access,
    and update simulate_on_snapshot_machine_config.json.
    """
    logging.info(f"🔄 Creating new sheets for epoch {current_epoch}...")

    # Google Sheets API auth
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    # Create Summary Sheet
    summary_title = f"Summary_Epoch_{current_epoch}"
    summary_sheet = gc.create(summary_title)
    summary_id = summary_sheet.id
    logging.info(f"📄 Created Summary sheet: {summary_title} (ID: {summary_id})")

    # Create Logs Sheet
    logs_title = f"Logs_Epoch_{current_epoch}"
    logs_sheet = gc.create(logs_title)
    logs_id = logs_sheet.id
    logging.info(f"📄 Created Logs sheet: {logs_title} (ID: {logs_id})")

    # Share with organization domain (editor access)
    try:
        summary_sheet.share(None, perm_type='domain', role='writer', domain='rakurai.io')
        logs_sheet.share(None, perm_type='domain', role='writer', domain='rakurai.io')
        logging.info("🌐 Granted 'rakurai.io' domain editor access to new sheets.")
    except Exception as e:
        logging.warning(f"⚠️ Could not set domain sharing: {e}")

    # Load existing config
    config_path = "simulate_on_snapshot_machine_config.json"
    with open(config_path, "r") as f:
        config = json.load(f)

    # Update config with new sheet IDs and epoch
    config['summary_spreadsheet_id'] = summary_id
    config['logs_spreadsheet_id'] = logs_id
    config['epoch'] = current_epoch

    # Save updated config
    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    logging.info(f"✅ Updated {config_path} with new sheet IDs and epoch {current_epoch}")

def upload_to_sheet(summary_sheet_id, logs_sheet_id, first_slot, test_name, block_cu, block_rewards, total_tips, log_file_path):
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open_by_key(summary_sheet_id)

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
        header += ["LogsSheet"]
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

    row = [test_name, first_slot] + ["--"] + block_cu + [cu_avg] + ["--"] + block_rewards + [reward_sum] + [total_tips] + [f"https://docs.google.com/spreadsheets/d/{logs_sheet_id}/edit"]
    sheet.append_row(row, value_input_option='USER_ENTERED')

    logging.info(f"📤 Uploaded results for slot {first_slot}, test name {test_name} to Google Sheet: {summary_sheet_id}, tab name: {first_slot}")

    try:
        log_parser_cmd = ['./logs_parser.sh', log_file_path, f"{test_name}_{first_slot}, {logs_sheet_id}"]
        subprocess.run(log_parser_cmd, check=True)
        logging.info(f"📄 Ran logs_parser.sh on {log_file_path} for tab {test_name}_{first_slot}")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Failed to run logs_parser.sh: {e}")

def simulate_snapshot(snapshot_dir, first_slot, log_dir, repo_path, test_name, summary_sheet_id, logs_sheet_id):
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

    logging.info(f'Running simulation for slot {first_slot}, test name {test_name}... Logging to {log_file_path}')

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
                    logging.info(f"🟡 Detected shutdown log in {log_file_path}")
                    logging.info(f"🔴 Terminating due to exit signal...")
                    time.sleep(10)
                    process.terminate()
                    process.wait()
                    break

            exit_code = process.wait()
            if exit_code == 0 or exit_code == -15 or exit_code == 101:
                logging.info(f"✅ Simulation completed for slot {first_slot}, test name {test_name}")
                block_cu, block_rewards, total_tips = extract_metrics_from_log(log_file_path)
                logging.info(f"📊 Block CU: {block_cu}, Block Rewards: {block_rewards}, Total Tips: {total_tips}")
                if block_cu and block_rewards:
                    upload_to_sheet(summary_sheet_id, logs_sheet_id, first_slot, test_name, block_cu, block_rewards, total_tips, log_file_path)
                else:
                    logging.error(f"❌ No valid metrics extracted from log file. Check the log file: {log_file_path}")
            else:
                logging.error(f"❌ Simulation failed for {test_name} with exit code {exit_code}")

        except Exception as e:
            logging.error(f"❌ Error while running simulation for {test_name}: {e}")
            process.terminate()
            process.wait()

    # Cleanup
    try:
        cleanup_cmd = ["rm", "-rf", "accounts", "ledger_tool", "snapshot", "banking_retrace"]
        subprocess.run(cleanup_cmd, cwd=snapshot_dir)
        logging.info(f"🧽 Cleaned up inner dirs of snapshot {first_slot}")
    except Exception as e:
        logging.error(f"⚠️ Cleanup failed for {first_slot}: {e}")

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Run snapshot simulations")
    parser.add_argument('snapshot_dir', help='Name of the snapshot directory to simulate')
    parser.add_argument('slot', type=int, help='First simulated slot for the snapshot')
    args = parser.parse_args()

    snapshot_dir = args.snapshot_dir
    first_simulated_slot = args.slot 
    with open("simulate_on_snapshot_machine_config.json") as f:
        config = json.load(f)

    test_repo_paths = config['test_repo_paths']
    summary_sheet_id = config['summary_spreadsheet_id']
    logs_sheet_id = config['logs_spreadsheet_id']
    epoch = config['epoch']

    current_epoch = first_simulated_slot // 432000
    if current_epoch != epoch:
        update_sheets(current_epoch)

    logging.info(f"📦 Running batch simulation for {len(test_repo_paths)} directories from simulate_on_snapshot_machine_config.json")
    logging.info(f"📦 CLI args: snapshot dir={args.snapshot_dir} first slot={args.slot}")

    for repo in test_repo_paths:
        path = repo['path'].rstrip('/')
        test_name = repo['test_name']
        log_dir = os.path.join(path, 'simulation_logs')
        simulate_snapshot(snapshot_dir, first_simulated_slot, log_dir, path, test_name, summary_sheet_id, logs_sheet_id)

    logging.info("✅ All simulations completed.")

if __name__ == "__main__":
    main()
