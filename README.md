# Utility Scripts  

This repository contains scripts for:  
- **Logs Parser**: Parses solana log files and uploads data to Google Sheets.  
- **Jito Reward Finder**: Finds Jito rewards for given slots.  

---

**Install Requirements**  
  ```bash
  python3 -m venv venv
  source venv/bin/activate
  pip install -r requirements.txt
  ```
## 📌 Simulation  

1. **Grant Access to the Service Account**  
   - Open your Google Sheet.  
   - Click on **Share** (top-right corner).  
   - Add this email as an **Editor**:  
     ```
     upload-logs@upload-logs-451118.iam.gserviceaccount.com
     ```
   - Click **Send** to grant access.  

2. **Get Your Google Spreadsheet ID**  
   - Copy the Spreadsheet ID from the URL, the part between d/ and /edit in URL.  
     ```
     https://docs.google.com/spreadsheets/d/your_spreadsheet_id/edit
     ```  

3. **Update config.json**  
   - Update spreadsheet id, repo paths and other required fields. 

4. **Run Simulations**  
   - Run the script `simulate.py` with:  
     ```bash
     python3 simulate.py
     ```
   - This will run simulations and upload results to Google Sheets. 

## 📌 Logs Parser  

1. **Grant Access to the Service Account**  
   - Open your Google Sheet.  
   - Click on **Share** (top-right corner).  
   - Add this email as an **Editor**:  
     ```
     upload-logs@upload-logs-451118.iam.gserviceaccount.com
     ```
   - Click **Send** to grant access.  

2. **Get Your Google Spreadsheet ID**  
   - Copy the Spreadsheet ID from the URL, the part between d/ and /edit in URL.  
     ```
     https://docs.google.com/spreadsheets/d/your_spreadsheet_id/edit
     ```
   - Replace `SPREADSHEET_ID` in `upload_logs.py` on line 7 with your actual Spreadsheet ID.  

3. **Upload Logs**  
   - Run the script `logs_parser.sh` with:  
     ```bash
     ./logs_parser.sh <log_file_path> <tab_title>
     ```
   - This will parse the log file and upload the data to the specified tab in Google Sheets.  

---

## 📌 Jito Reward Finder     

Run the script with one or more slot numbers:  
```bash
python3 get_jito_rewards.py <slot1> <slot2> ...
```