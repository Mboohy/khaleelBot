import os
import sys
import json
import time
from datetime import datetime
import requests
import pandas as pd
import numpy as np
import gspread

# ==========================================
# 1. CONFIGURATION & SETUP
# ==========================================

# Pipeline End Date Check
END_DATE = "2026-12-31" 

if datetime.now().strftime('%Y-%m-%d') > END_DATE:
    print(f"Current date is past the end date ({END_DATE}). Exiting pipeline.")
    sys.exit(0)

# Institute Configuration
INSTITUTE_NAME = "Albuhuti"
BASE_URL = "https://api.albuhutifiqh.com"
LOGIN_URL = f"{BASE_URL}/api/login"
GOOGLE_SHEET_ID = "14gzPMTDZO-CZpMJCy1u5v6BN3wg6r4sjnaoq6vX2G-c"
ADMIN_EMAIL = "admin@albuhutifiqh.com"
ADMIN_PASSWORD = "DzMzn@$D3#Wg}z#t"

# API Endpoints
URLS = {
    "students": f"{BASE_URL}/admin/accepted/users",
    "applicants": f"{BASE_URL}/admin/pending/users",
    "payments": f"{BASE_URL}/admin/payments"
}

# ==========================================
# 2. AUTHENTICATION (GOOGLE & API)
# ==========================================

print(f"=== {INSTITUTE_NAME} Automated Data Pipeline ===")
print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Google Unattended Authentication
print("🔑 Authenticating with Google Service Account...")
try:
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not found. (Did you set it in GitHub Secrets?)")
    
    creds_dict = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds_dict)
    print("✓ Successfully authenticated with Google Sheets")
except Exception as e:
    print(f"❌ Google Authentication failed: {str(e)}")
    sys.exit(1)

# Albuhuti API Authentication
def authenticate_api():
    print(f"🔑 Authenticating with {INSTITUTE_NAME} API...")
    try:
        response = requests.post(
            LOGIN_URL,
            json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
            timeout=15,
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print("✓ Successfully authenticated with API")
        return response.json().get('token')
    except Exception as e:
        print(f"❌ API Authentication failed: {str(e)}")
        sys.exit(1)

token = authenticate_api()

# ==========================================
# 3. DATA FETCHING FUNCTIONS
# ==========================================

def fetch_paginated_data(url, params, label, skip_pages=None):
    """Generic function to handle pagination and retries for all endpoints"""
    all_data = []
    page = 1
    max_retries = 3
    skip_pages = skip_pages or []

    print(f"\n📥 Fetching {label} data...")
    
    while True:
        if page in skip_pages:
            print(f"⏭️ Skipping page {page}")
            page += 1
            continue

        params['page'] = page
        success = False

        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url,
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                success = True
                break
            except requests.exceptions.RequestException as e:
                wait_time = (2 ** attempt) * 2
                print(f"Attempt {attempt + 1} failed on page {page}. Retrying in {wait_time}s...")
                time.sleep(wait_time)

        if not success:
            if page >= 60:  # Auto-skip threshold to prevent crashing on deep pages
                print(f"⏭️ Auto-skipping page {page} due to persistent errors")
                page += 1
                continue
            else:
                print(f"❌ Stopping {label} fetch due to persistent errors on page {page}")
                break

        data = response.json()
        
        # Handle varying API response structures
        if 'data' in data:
            if isinstance(data['data'], list):
                records = data['data']
            elif isinstance(data['data'], dict) and 'data' in data['data']:
                records = data['data']['data']
            else:
                records = []
        else:
            records = []

        valid_records = [r for r in records if isinstance(r, dict)]
        all_data.extend(valid_records)

        if not records:
            break

        print(f"📄 Page {page}: Found {len(valid_records)} records (Total: {len(all_data)})")

        # Pagination control
        last_page = data.get('last_page', 1)
        if page >= last_page:
            break

        page += 1
        time.sleep(0.5)

    print(f"✅ Finished fetching {label}. Total records: {len(all_data)}")
    return all_data

# ==========================================
# 4. MASTER UPLOAD FUNCTION
# ==========================================

def upload_to_google_sheets(data, worksheet_name):
    """Reliable Google Sheets upload with smart handling and batching"""
    if not data:
        print(f"🔄 No data to upload for {worksheet_name}. Skipping.")
        return

    try:
        df = pd.DataFrame(data)
        # Clean data to prevent Google Sheets API errors
        df = (df.replace([np.inf, -np.inf, None], np.nan)
              .fillna('')
              .applymap(lambda x: str(x)[:50000] if pd.notna(x) else ''))

        print(f"\n📤 Uploading {len(df)} records to '{worksheet_name}'...")
        sheet = gc.open_by_key(GOOGLE_SHEET_ID)

        # Worksheet handling
        try:
            worksheet = sheet.worksheet(worksheet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(
                title=worksheet_name,
                rows=max(1000, len(df)+200),
                cols=max(20, len(df.columns)+5)
            )

        # Batch upload
        batch_size = 150
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            if i == 0:
                worksheet.update(
                    [df.columns.tolist()] + batch.values.tolist(),
                    'A1',
                    value_input_option='USER_ENTERED'
                )
            else:
                worksheet.append_rows(
                    batch.values.tolist(),
                    value_input_option='USER_ENTERED'
                )
            print(f"⬆️ Uploaded rows {i+1}-{min(i+batch_size, len(df))}")

        print(f"✅ Successfully updated worksheet: '{worksheet_name}'")

    except Exception as e:
        print(f"❌ Upload error for {worksheet_name}: {str(e)}")

# ==========================================
# 5. PIPELINE EXECUTION
# ==========================================
today = datetime.now().strftime('%Y-%m-%d')

# --- A. STUDENTS ---
students_params = {
    "format": "json", "per_page": 100, "grade[]": 8, "year[]": 32,
    "from": "2024-12-01", "to": today, "field": "name"
}
students_data = fetch_paginated_data(URLS['students'], students_params, "Students")
upload_to_google_sheets(students_data, "students")

# --- B. APPLICANTS (DEEP SEARCH VERSION) ---
applicants_params = {
    "format": "json", 
    "per_page": 20,           # Increase to 100 to pull more per page
    "from": "2025-01-01",      # Extreme date range to catch EVERYONE
    "to": today
}

# Run the fetch
applicants_data = fetch_paginated_data(URLS['applicants'], applicants_params, "Applicants")

# If you still only get 20, it confirms that the 'pending' endpoint 
# only sees 20 users currently waiting in the system.
upload_to_google_sheets(applicants_data, "applicants")
# --- C. INSTALLMENTS ---
installments_params = {
    "format": "json", "per_page": 150, 
    "from": "2025-12-14", "to": today, "type": "installments", "status": "accepted"
}
installments_data = fetch_paginated_data(URLS['payments'], installments_params, "Installments")
upload_to_google_sheets(installments_data, "installments")

# --- D. CASH / INVOICES ---
cash_params = {
    "format": "json", "per_page": 100, 
    "from": "2025-12-14", "to": today, "type": "invoices", "status": "accepted"
}
cash_data = fetch_paginated_data(URLS['payments'], cash_params, "Cash")
upload_to_google_sheets(cash_data, "cash")


print("\n==========================================")
print(f"🎉 Pipeline Complete! End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("==========================================")
