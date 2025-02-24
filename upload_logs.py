from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import pandas as pd
import argparse
import string

SPREADSHEET_ID = '1oP0TuRCuIFb0OocZFzrkn142qXigoIdRqTW4aL1qd1Y'
# Path to your service account key JSON file
SERVICE_ACCOUNT_FILE = 'credentials.json'

# Scope for Google Sheets and Google Drive APIs
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 
          'https://www.googleapis.com/auth/drive']

# Authenticate and create a service client
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=credentials)
drive_service = build('drive', 'v3', credentials=credentials)

def get_excel_column_letter(col_index):
    """Convert column index (0-based) to Excel column letter (A, B, ..., Z, AA, AB, ...)."""
    result = []
    while col_index >= 0:
        result.append(string.ascii_uppercase[col_index % 26])
        col_index = col_index // 26 - 1
    return ''.join(reversed(result))

# Function to add a new tab (sheet) to an existing Google Sheet
def add_tab_to_google_sheet(SPREADSHEET_ID, tab_title):
    body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": tab_title
                    }
                }
            }
        ]
    }
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body=body
    ).execute()
    return response['replies'][0]['addSheet']['properties']['sheetId']

def process_csv(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()

    sections = content.split("\n\n")  # Split into sections based on empty lines
    formatted_data = {}

    for section in sections:
        lines = section.strip().split("\n")
        name = [lines[0].strip()]  # First line is the section name

        col_sums = [[float(value) if value.replace('.', '', 1).isdigit() else value for value in lines[1].split(",")]] # Second line contains column sums
        col_names = [lines[2].split(",")]  # Third line contains column names

        # Extract actual data
        data = [[float(value) if value.replace('.', '', 1).isdigit() else value for value in line.split(",")] for line in lines[3:]]

        # Combine all parts in the required format
        full_data = [name] + col_sums + col_names + data
        formatted_data[name[0]] = full_data  # Store in dictionary

    return formatted_data
    
# Function to upload data to a specific tab in a Google Sheet
def upload_csv_to_tab(SPREADSHEET_ID, tab_title, section_name, data, start_col, start_row):
    # Prepare the body for the request
    col_letter = get_excel_column_letter(start_col)  # Convert to Excel column letter
    body = {
        'values': data
    }
    
    # Upload data to the specified tab
    try:    
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{tab_title}!{col_letter}{start_row}', 
            valueInputOption='RAW',  
            body=body
        ).execute()
        print(f"Uploaded {section_name} to tab '{tab_title}' in Google Sheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    except Exception as e:
        print (f"error occurred uploading: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", help="Path to the CSV file")
    parser.add_argument("tab_title", help="Title for the tab")
    args = parser.parse_args()

    csv_file = args.csv_file
    tab_title = args.tab_title

    # Add a new tab to the existing Google Sheet
    try:
        add_tab_to_google_sheet(SPREADSHEET_ID, tab_title)
        print(f"Added new tab '{tab_title}' to Google Sheet with ID: {SPREADSHEET_ID}")
    except Exception as e:
        print(f"Tab '{tab_title}' might already exist. Error: {e}")

    sections_data = process_csv(csv_file)
    current_col = 0
    row_tracker = {}
    for section_name, data in sections_data.items():
        num_columns = max(len(row) for row in data)  # Calculate max columns for the section
        num_rows = len(data) 
        if section_name == "banking_stage_scheduler_reception_slot_counts":
            start_col = 0  # Column A
            start_row = row_tracker.get("banking_stage_scheduler_reception_counts", 1)
        elif section_name == "banking_stage_scheduler_slot_counts":
            start_col = 15  # Column P
            start_row = row_tracker.get("banking_stage_scheduler_counts", 1)
        else:
            start_col = current_col
            start_row = 1
        upload_csv_to_tab(SPREADSHEET_ID, tab_title, section_name, data, start_col, start_row)
        row_tracker[section_name] = start_row + num_rows
        if section_name not in ("banking_stage_scheduler_reception_slot_counts", "banking_stage_scheduler_slot_counts"):
            current_col += num_columns + 2