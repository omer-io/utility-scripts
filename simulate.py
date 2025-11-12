import os
import re
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

def download_snapshot(bucket, full_prefix, local_base_dir, snapshot_dir, first_slot):
    """
    Downloads a snapshot directory from GCP and processes it into individual slot snapshots.

    Args:
        bucket (str): GCS bucket name.
        full_prefix (str): Path inside bucket (e.g. 'snapshots/mainnet/snapshot-123-124-125').
        local_base_dir (str): Base local directory to store snapshots.
        first_slot (int): First slot (used only for naming consistency if needed).

    Returns:
        None
    """
    try:
        # Step 1: Prepare GCS URI and download
        gcs_uri = f"gs://{bucket}/{full_prefix.rstrip('/')}/"
        logging.info(f"Starting snapshot download from {gcs_uri}")

        # Download entire directory using gcloud CLI
        subprocess.run(
            ["gcloud", "storage", "cp", "-r", gcs_uri, local_base_dir],
            check=True,
            capture_output=True,
            text=True
        )

        logging.info(f"Downloaded snapshot directory: {snapshot_dir}")

        # Step 3: Check for incremental snapshot files
        files_in_snapshot = os.listdir(snapshot_dir)
        incremental_files = [f for f in files_in_snapshot if f.startswith("incremental")]

        if not incremental_files:
            logging.warning("No incremental snapshot files found — skipping processing.")
            return

        # Step 4: Parse slots from snapshot directory name
        match = re.search(r"snapshot-(.+)$", os.path.basename(snapshot_dir))
        if not match:
            logging.error("Could not extract slots from directory name.")
            return

        slots = match.group(1).split("-")
        if len(slots) == 1:
            logging.info("Only one slot found — no splitting needed.")
            return

        logging.info(f"Found multiple slots: {slots}, processing...")

        # Step 5: Create individual snapshot directories
        individual_dirs = []
        for slot in slots:
            slot_dir = os.path.join(local_base_dir, f"snapshot-{slot}")
            os.makedirs(slot_dir, exist_ok=True)
            individual_dirs.append((slot, slot_dir))
            logging.debug(f"Created individual slot directory: {slot_dir}")

        # Step 6: Copy snapshot* files to each slot directory
        for file_name in files_in_snapshot:
            if file_name.startswith("snapshot"):
                src_file = os.path.join(snapshot_dir, file_name)
                for _, slot_dir in individual_dirs:
                    shutil.copy2(src_file, slot_dir)
        logging.info("Copied base snapshot files to all individual directories.")

        # Step 7: Copy rocksdb directory
        rocksdb_path = os.path.join(snapshot_dir, "rocksdb")
        if os.path.isdir(rocksdb_path):
            for _, slot_dir in individual_dirs:
                dest = os.path.join(slot_dir, "rocksdb")
                shutil.copytree(rocksdb_path, dest, dirs_exist_ok=True)
            logging.info("Copied rocksdb directory to all individual directories.")
        else:
            logging.warning("No rocksdb directory found in snapshot.")

        # Step 8: Copy relevant banking_trace directories
        all_banking_trace_path = os.path.join(snapshot_dir, "all_banking_trace")
        if os.path.isdir(all_banking_trace_path):
            all_trace_dirs = os.listdir(all_banking_trace_path)
            for slot, slot_dir in individual_dirs:
                pattern = f"banking_trace-{slot}"
                matching_traces = [d for d in all_trace_dirs if d.startswith(pattern)]
                if matching_traces:
                    trace_src = os.path.join(all_banking_trace_path, matching_traces[0])
                    trace_dest = os.path.join(slot_dir, "banking_trace")
                    shutil.copytree(trace_src, trace_dest, dirs_exist_ok=True)
                    logging.debug(f"Copied banking_trace for slot {slot}")
        else:
            logging.warning("No all_banking_trace directory found in snapshot.")

        # Step 9: Copy genesis.bin
        genesis_path = os.path.join(snapshot_dir, "genesis.bin")
        if os.path.exists(genesis_path):
            for _, slot_dir in individual_dirs:
                shutil.copy2(genesis_path, slot_dir)
            logging.info("Copied genesis.bin to all individual directories.")
        else:
            logging.warning("No genesis.bin found in snapshot directory.")

        # Step 10: Assign incremental snapshots to nearest lower slot
        incremental_info = []
        for inc_file in incremental_files:
            m = re.search(r"incremental-snapshot-\d+-(\d+)-", inc_file)
            if m:
                incremental_info.append((int(m.group(1)), inc_file))

        incremental_info.sort(key=lambda x: x[0])

        for slot_str, slot_dir in individual_dirs:
            slot = int(slot_str)
            smaller = [i for i in incremental_info if i[0] < slot]
            if not smaller:
                continue
            nearest_inc = max(smaller, key=lambda x: x[0])
            src_file = os.path.join(snapshot_dir, nearest_inc[1])
            shutil.copy2(src_file, slot_dir)
            logging.debug(f"Assigned incremental {nearest_inc[1]} to slot {slot}")

        logging.info("Completed splitting and distributing snapshot data successfully.")

        shutil.rmtree(snapshot_dir)

    except subprocess.CalledProcessError as e:
        logging.error(f"GCloud command failed: {e.stderr.strip()}")
    except Exception as e:
        logging.exception(f"Unexpected error occurred during snapshot processing: {e}")

# def download_snapshot(bucket, full_prefix, local_base_dir):
#     client = storage.Client()  # Uses default credentials
#     bucket = client.bucket(bucket)
#     blobs = client.list_blobs(bucket, prefix=full_prefix)

#     for blob in blobs:
#         gcs_path = blob.name
#         relative_path = gcs_path[len(full_prefix):].lstrip("/")  # remove prefix and any leading slash

#         if not relative_path or gcs_path.endswith("/"):
#             # Skip the prefix directory itself or "folders" (GCS is flat, but folder-like objects end with '/')
#             continue

#         local_path = os.path.join(local_base_dir, relative_path)
#         Path(local_path).parent.mkdir(parents=True, exist_ok=True)

#         blob.download_to_filename(local_path)

def simulate_snapshot(snapshot_dir, first_slot, name, log_dir, repo_path, test_name, sheet_id, version):
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

    if version:
        cmd.extend(['--version', version])
        
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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Run snapshot simulations")
    parser.add_argument('--snapshot_dir', help='Name of the snapshot directory to simulate')
    parser.add_argument('--slot', type=int, help='First simulated slot for the snapshot')
    parser.add_argument('--test_name', help='Override test name')
    args = parser.parse_args()

    with open("config.json") as f:
        config = json.load(f)

    bucket = config['bucket'].rstrip('/')
    prefix = config['prefix'].rstrip('/') + "/"
    download_path = config['download_path'].rstrip('/')
    repo_path = config['test_repo_path']
    tracedata_version = config.get('tracedata_version')
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

        individual_name = f"snapshot-{first_slot}"
        individual_dir = os.path.join(download_path, individual_name)

        if os.path.exists(local_dir):
            logging.info(f"⏭️ Skipping downloading {name}: already exists at {local_dir}")
        elif os.path.exists(individual_dir):
            logging.info(f"⏭️ Skipping downloading {name}: individual snapshot {individual_name} already exists at {individual_dir}")
        else:
            logging.info(f"⬇️ Downloading snapshot {name} from GCP...")
            download_snapshot(bucket, full_prefix, download_path, local_dir, first_slot)

        if os.path.exists(individual_dir):
            local_dir = individual_dir
        simulate_snapshot(local_dir, first_slot, name, log_dir, repo_path, test_name, sheet_id, tracedata_version)

    logging.info("✅ All simulations completed.")
if __name__ == "__main__":
    main()
