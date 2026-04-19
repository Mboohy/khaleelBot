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

END_DATE = "2026-12-31" 
if datetime.now().strftime('%Y-%m-%d') > END_DATE:
    sys.exit(0)

INSTITUTE_NAME = "Albuhuti"
BASE_URL = "https://api.albuhutifiqh.com"
LOGIN_URL = f"{BASE_URL}/api/login"
GOOGLE_SHEET_ID = "14gzPMTDZO-CZpMJCy1u5v6BN3wg6r4sjnaoq6vX2G-c"
ADMIN_EMAIL = "admin@albuhutifiqh.com"
ADMIN_PASSWORD = "DzMzn@$D3#Wg}z#t"

URLS = {
    "students": f"{BASE_URL}/admin/accepted/users",
    "applicants": f"{BASE_URL}/admin/pending/users",
    "payments": f"{BASE_URL}/admin/payments"
}

# ==========================================
# 2. AUTHENTICATION
# ==========================================

print(f"=== {INSTITUTE_NAME} Data Pipeline ===")

try:
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    creds_dict = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds_dict)
    print("✓ Google Authentication Successful")
except Exception as e:
    print(f"❌ Google Auth Failed: {repr(e)}")
    sys.exit(1)

def authenticate_api():
    try:
        response = requests.post(LOGIN_URL, json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}, timeout=15)
        response.raise_for_status()
        return response.json().get('token')
    except Exception as e:
        print(f"❌ API Auth Failed: {repr(e)}")
        sys.exit(1)

token = authenticate_api()

# ==========================================
# 3. FETCH DATA (SAFE PAGINATION)
# ==========================================

def fetch_paginated_data(url, params, label, skip_pages=None, max_pages=5000):
    all_data = []
    page = 1
    previous_batch = [] 

    print(f"\n📥 Fetching {label}...")
    while page <= max_pages: 
        params['page'] = page
        try:
            response = requests.get(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            records = data.get('data', [])
            if isinstance(records, dict): records = records.get('data', [])
            
            if not records or len(records) == 0: break
            if records == previous_batch:
                print(f"⚠️ API repeating data on page {page}. Breaking.")
                break

            all_data.extend(records)
            previous_batch = records
            print(f"📄 Page {page}: Found {len(records)} (Total: {len(all_data)})")
            
            last_page = data.get('last_page')
            if last_page and page >= int(last_page): break
            page += 1
            time.sleep(0.4)
        except Exception as e:
            print(f"❌ Error: {repr(e)}")
            break
    return all_data

# ==========================================
# 4. UPLOAD DATA (QUOTAS & PERMISSIONS SAFE)
# ==========================================

def upload_to_google_sheets(data, worksheet_name):
    if not data: return
    try:
        df = pd.DataFrame(data).astype(str)
        df = df.replace({'nan': '', 'None': '', '<NA>': ''})

        print(f"📤 Uploading {len(df)} to '{worksheet_name}'...")
        sheet = gc.open_by_key(GOOGLE_SHEET_ID)

        try:
            worksheet = sheet.worksheet(worksheet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=worksheet_name, rows=len(df)+200, cols=25)

        batch_size = 2500 
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            if i == 0:
                worksheet.update([df.columns.tolist()] + batch.values.tolist(), 'A1', value_input_option='USER_ENTERED')
            else:
                worksheet.append_rows(batch.values.tolist(), value_input_option='USER_ENTERED')
            print(f"⬆️ Uploaded {i+1}-{min(i+batch_size, len(df))}")
            time.sleep(2) # Protect API Quota

        print(f"✅ Updated: '{worksheet_name}'")
    except Exception as e:
        print(f"❌ Upload error for {worksheet_name}: {repr(e)}")

# ==========================================
# 5. EXECUTION
# ==========================================
today = datetime.now().strftime('%Y-%m-%d')

# Students
upload_to_google_sheets(fetch_paginated_data(URLS['students'], {"format": "json", "per_page": 100, "from": "2024-12-01", "to": today}, "Students"), "students")

# Applicants
upload_to_google_sheets(fetch_paginated_data(URLS['applicants'], {"format": "json", "per_page": 100, "from": "2025-01-01", "to": today}, "Applicants"), "applicants")

# Installments
upload_to_google_sheets(fetch_paginated_data(URLS['payments'], {"format": "json", "per_page": 150, "from": "2025-12-14", "to": today, "type": "installments", "status": "accepted"}, "Installments"), "installments")

# Cash
upload_to_google_sheets(fetch_paginated_data(URLS['payments'], {"format": "json", "per_page": 100, "from": "2025-12-14", "to": today, "type": "invoices", "status": "accepted"}, "Cash"), "cash")

# Sync Timestamp
try:
    gc.open_by_key(GOOGLE_SHEET_ID).get_worksheet(0).update('Z1', f"Last Sync: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
except: pass

print("\n🎉 Pipeline Complete!")
