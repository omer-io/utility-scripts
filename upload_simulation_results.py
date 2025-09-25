import json
import gspread
import logging
import argparse
import subprocess
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

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Upload simulation results to Google Sheet")
    parser.add_argument('--logfile', help='Path to the log file', required=True)
    parser.add_argument('--first-slot', type=int, help='First simulated slot for the snapshot', required=True)
    parser.add_argument('--test-name', help='Test name')
    args = parser.parse_args()

    with open("config.json") as f:
        config = json.load(f)

    test_name = args.test_name if args.test_name else config['test_name']
    sheet_id = config['spreadsheet_id']
    
    block_cu, block_rewards, total_tips = extract_metrics_from_log(args.logfile)
    upload_to_sheet(sheet_id, args.first_slot, test_name, block_cu, block_rewards, total_tips, args.logfile)

if __name__ == "__main__":
    main()
