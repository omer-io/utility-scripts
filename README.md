# Solana Logs Uploader to Google Sheets  

This repository provides a simple way to parse log files and upload parsed data to a Google Spreadsheet.  

## 📌 How It Works  

1. **Grant Access to the Service Account**  
   - Open your Google Sheet.  
   - Click on **Share** (top-right corner).  
   - Add this email as an **Editor**:  
     ```
     upload-logs@upload-logs-451020.iam.gserviceaccount.com
     ```
   - Click **Send** to grant access.  

2. **Get Your Google Spreadsheet ID**  
   - Copy the Spreadsheet ID from the URL, the part between d/ and /edit in URL.  
     ```
     https://docs.google.com/spreadsheets/d/your_spreadsheet_id/edit
     ```
   - Replace `SPREADSHEET_ID` in `upload_logs.py` on line 7 with your actual Spreadsheet ID.  

3. **Install Requirements**  
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     pip install -r requirements.txt
     ```

4. **Upload Logs**  
   - Run the script `logs_parser.sh` with:  
     ```bash
     ./logs_parser.sh <log_file_path> <tab_title>
     ```
   - This will parse the log file and upload the data to the specified tab in Google Sheets.  
