import time
import requests
import pandas as pd
import json
import os
import re
from tkinter import Tk, filedialog
from tqdm import tqdm
from datetime import datetime
import winreg

# Registry configuration
REGISTRY_PATH = r"Software\DFIRVault\CSV2ELK"

# =============== CONFIGURATION ===============
print("")
print("Developed by Jacob Wilson - Version 0.2")
print("dfirvault@gmail.com")
print("")
def load_config():
    """Load configuration from Windows Registry or return empty values if not exists"""
    config = {
        'ELASTICSEARCH_URL': '',
        'USERNAME': '',
        'PASSWORD': ''
    }
    
    try:
        # Try to open the registry key
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, REGISTRY_PATH, 0, winreg.KEY_READ)
        
        for value_name in config.keys():
            try:
                value, _ = winreg.QueryValueEx(key, value_name)
                config[value_name] = value
            except FileNotFoundError:
                # Value doesn't exist, keep default empty string
                pass
        
        winreg.CloseKey(key)
        return config
        
    except FileNotFoundError:
        # Registry key doesn't exist, prompt for credentials
        print("⚠️ Configuration not found in registry. Please enter your Elasticsearch credentials:")
        config['ELASTICSEARCH_URL'] = input("Elasticsearch URL (e.g., https://hostname:9200): ").strip()
        config['USERNAME'] = input("Username: ").strip()
        config['PASSWORD'] = input("Password: ").strip()
        print("Configuration will be stored in registry: HKCU\\" + REGISTRY_PATH)
        return config

def save_config(url, username, password):
    """Save configuration to Windows Registry"""
    try:
        # Create or open the registry key
        key = winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, REGISTRY_PATH, 0, winreg.KEY_WRITE)
        
        # Store the values
        winreg.SetValueEx(key, 'ELASTICSEARCH_URL', 0, winreg.REG_SZ, url)
        winreg.SetValueEx(key, 'USERNAME', 0, winreg.REG_SZ, username)
        winreg.SetValueEx(key, 'PASSWORD', 0, winreg.REG_SZ, password)
        
        winreg.CloseKey(key)
        print(f"✅ Configuration saved to registry: HKCU\\{REGISTRY_PATH}")
        
    except Exception as e:
        print(f"❌ Failed to save configuration to registry: {e}")

# Load initial configuration
config = load_config()
ELASTICSEARCH_URL = config['ELASTICSEARCH_URL']
USERNAME = config['USERNAME']
PASSWORD = config['PASSWORD']
# =============== CONFIGURATION ===============
requests.packages.urllib3.disable_warnings()

DEFAULT_FIELD_MAPPINGS = {
    "timestamp_field": {"type": "date"}#,
    #"repo_field": {"type": "keyword"},
    #"user": {"type": "keyword"},
    #"status": {"type": "keyword"},
    #"host": {"type": "keyword"},
    #"message": {"type": "text"},
    #"log": {"type": "text"}
}
        
def ensure_elasticsearch_connection():
    global ELASTICSEARCH_URL, USERNAME, PASSWORD
    while True:
        try:
            response = requests.get(f"{ELASTICSEARCH_URL}/_cluster/health", auth=(USERNAME, PASSWORD), verify=False, timeout=5)
            if response.status_code == 200:
                print(f"✅ Connected to Elasticsearch at {ELASTICSEARCH_URL}")
                # Save the successful configuration
                save_config(ELASTICSEARCH_URL, USERNAME, PASSWORD)
                return True
            elif response.status_code == 401:
                print("❌ Authentication failed. Please enter correct credentials.")
                USERNAME = input("Username: ")
                PASSWORD = input("Password: ")
            else:
                print(f"❌ Error connecting: {response.status_code} - {response.text}")
                ELASTICSEARCH_URL = input("Enter correct Elasticsearch URL (e.g., https://hostname:9200): ").strip()
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Failed to reach Elasticsearch URL ({ELASTICSEARCH_URL}): {e}")
            ELASTICSEARCH_URL = input("Enter correct Elasticsearch URL (e.g., https://hostname:9200): ").strip()

def get_indices_info():
    stats_url = f"{ELASTICSEARCH_URL}/_cat/indices?h=index,docs.count,store.size&format=json"
    response = requests.get(stats_url, auth=(USERNAME, PASSWORD), verify=False)
    if response.status_code == 200:
        data = response.json()
        return [idx for idx in data if not (idx['index'].startswith('.') or idx['index'].startswith('log'))]
    else:
        print("Error retrieving index info.")
        return []
def extract_date_from_index(index_name):
    match = re.search(r'_(\d{8})$', index_name)
    if match:
        return match.group(1)
    return '00000000'  # fallback for indexes without date
    
def sanitize_index_name(name):
    name = name.lower()
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'[^a-z0-9_]', '', name)
    return name

def create_index_with_mapping(index_base_name):
    index_base_name = sanitize_index_name(index_base_name)
    today = datetime.today().strftime('%Y%m%d')
    index_name = f"{index_base_name}_{today}"
    mapping = {
        "mappings": {
            "properties": DEFAULT_FIELD_MAPPINGS
        }
    }
    response = requests.put(
        f'{ELASTICSEARCH_URL}/{index_name}',
        auth=(USERNAME, PASSWORD),
        headers={'Content-Type': 'application/json'},
        data=json.dumps(mapping),
        verify=False
    )
    if response.status_code == 200:
        print(f"✅ Index '{index_name}' created with default mappings.")
    else:
        print(f"❌ Failed to create index. Status: {response.status_code}, Response: {response.text}")
    return index_name

def guess_timestamp_column(columns):
    priority = ['timestamp', '@timestamp', 'time', 'datetime', 'date']
    for p in priority:
        for col in columns:
            if re.search(p, col, re.IGNORECASE):
                return col
    return None

def select_timestamp_column(df):
    print("\nCSV Headers with Sample Values (row 1):")
    if df.empty:
        print("⚠️ DataFrame is empty. Cannot determine timestamp column.")
        return None

    sample_row = df.iloc[0].to_dict()
    for i, col in enumerate(df.columns, 1):
        sample_val = sample_row.get(col, '')
        print(f"{i}. {col} - {sample_val}")

    default_guess = guess_timestamp_column(df.columns)
    if default_guess:
        example_val = sample_row.get(default_guess, 'N/A')
        print(f"\n📌 Suggested timestamp column: {default_guess} (e.g. {example_val})")

    while True:
        selection = input(f"Select timestamp column, either in Epoch time or ISO-8601 (YYYY-MM-DDTHH:MM:SSZ) [press Enter to accept '{default_guess}']: ")
        if selection.strip() == '' and default_guess:
            selected_col = default_guess
            break
        try:
            index = int(selection) - 1
            selected_col = df.columns[index]
            break
        except (IndexError, ValueError):
            print("❌ Invalid selection. Try again.")

    samples = df[selected_col].dropna().astype(str).head(5).tolist()
    print(f"\n📋 Sample values from '{selected_col}':")
    for s in samples:
        iso_version = None
        try:
            num = float(s)
            # Heuristics: 13-digit is ms, 10-digit is s
            if len(str(int(num))) >= 13:
                iso_version = datetime.utcfromtimestamp(num / 1000).isoformat() + "Z"
            elif len(str(int(num))) == 10:
                iso_version = datetime.utcfromtimestamp(num).isoformat() + "Z"
        except:
            pass

        if iso_version:
            print(f"  - {s} → {iso_version}")
        else:
            print(f"  - {s}")
    print("🔍 Make sure these are valid ISO-8601 timestamps (or parseable by Elasticsearch).")

    confirm = input("✅ Proceed with this timestamp field? (y/n): ").strip().lower()
    if confirm != 'y':
        return select_timestamp_column(df)

    return selected_col

def convert_csv_to_json(csv_path, selected_index, index_name=None, timestamp_column=None):
    df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False, on_bad_lines='warn')
    df = df.where(pd.notnull(df), None)
    print(f"✅ Successfully read CSV with encoding: utf-8")
    print(f"📄 Writing JSON with index: {selected_index}")

    def deduplicate_columns(columns):
        seen = {}
        result = []
        for col in columns:
            if col not in seen:
                seen[col] = 0
                result.append(col)
            else:
                seen[col] += 1
                result.append(f"{col}_{seen[col]}")
        return result

    df.columns = deduplicate_columns(df.columns)
    df.columns = [sanitize_column(col) for col in df.columns]

    json_path = csv_path.replace(".csv", ".json")

    with open(json_path, 'w', encoding='utf-8') as f:
        for _, row in tqdm(df.iterrows(), total=len(df), desc="🔄 Writing JSON"):
            action = {"index": {"_index": index_name}}
            f.write(json.dumps(action, ensure_ascii=False) + "\n")

            row_dict = row.to_dict()

            if timestamp_column and timestamp_column in row_dict:
                ts_val = row_dict[timestamp_column]
                if pd.notna(ts_val) and str(ts_val).strip():
                    try:
                        # Handle numeric epoch time (seconds or milliseconds)
                        if isinstance(ts_val, (int, float)) or re.match(r'^\d+(\.\d+)?$', str(ts_val)):
                            ts_float = float(ts_val)
                            if ts_float > 1e12:  # likely in milliseconds
                                iso_ts = datetime.utcfromtimestamp(ts_float / 1000).isoformat() + 'Z'
                            else:  # assume seconds
                                iso_ts = datetime.utcfromtimestamp(ts_float).isoformat() + 'Z'
                        else:
                            iso_ts = pd.to_datetime(ts_val, utc=True).isoformat()
                        row_dict["timestamp_field"] = iso_ts
                    except Exception as e:
                        print(f"⚠️ Failed to parse timestamp: {ts_val} ({e})")

            # 🧼 Clean up non-JSON-compliant values like NaN and Infinity
            row_dict = clean_data(row_dict)

            f.write(json.dumps(row_dict, ensure_ascii=False) + "\n")

    return json_path

def sanitize_column(name):
    name = name.replace('.', '_')
    name = re.sub(r'[^\w@#]', '_', name)
    return name

def clean_data(obj):
    """Recursively replace NaN, inf, -inf with None (null in JSON)"""
    if isinstance(obj, dict):
        return {k: clean_data(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_data(v) for v in obj]
    elif isinstance(obj, float):
        if pd.isna(obj) or obj in (float('inf'), float('-inf')):
            return None
    return obj

def upload_to_index(index_name, json_file_path, chunk_size=10000, max_retries=30, retry_delay=1):
    print("🚀 Uploading data to Elasticsearch in chunks...")

    def chunk_file(file_path, chunk_size):
        with open(file_path, 'r', encoding='utf-8') as f:
            chunk = []
            for i, line in enumerate(f, 1):
                chunk.append(line)
                if i % chunk_size == 0:
                    yield ''.join(chunk)
                    chunk = []
            if chunk:
                yield ''.join(chunk)

    success = True
    chunks = list(chunk_file(json_file_path, chunk_size))
    for i, chunk in enumerate(tqdm(chunks, desc="📤 Uploading chunks")):
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(
                    f'{ELASTICSEARCH_URL}/{index_name}/_bulk',
                    auth=(USERNAME, PASSWORD),
                    headers={'Content-Type': 'application/x-ndjson'},
                    data=chunk.encode('utf-8'),
                    verify=False,
                    timeout=10  # optional: limit wait time
                )
                if response.status_code not in [200, 201]:
                    print(f"❌ Chunk upload failed (attempt {attempt}). Status: {response.status_code}")
                    print(response.text)
                    if attempt == max_retries:
                        success = False
                    else:
                        time.sleep(retry_delay)
                else:
                    result = response.json()
                    if result.get("errors"):
                        print("⚠️ Some items in chunk failed to index:")
                        for item in result["items"]:
                            if 'error' in item.get('index', {}):
                                print(json.dumps(item['index']['error'], indent=2))
                    if attempt > 1:
                        print(f"🔁 Retry successful for chunk {i+1} on attempt {attempt}.")
                    break  # success, exit retry loop
            except requests.exceptions.RequestException as e:
                print(f"⚠️ Request failed (attempt {attempt}): {e}")
                if attempt == max_retries:
                    success = False
                else:
                    time.sleep(retry_delay)

        if not success:
            print("🚫 Giving up on current chunk due to repeated failures.")
            break

    os.remove(json_file_path)
    if success:
        print("✅ All chunks uploaded successfully.")
    else:
        print("❌ Upload completed with some errors.")

def delete_index(index_name):
    response = requests.delete(f'{ELASTICSEARCH_URL}/{index_name}', auth=(USERNAME, PASSWORD), verify=False)
    if response.status_code == 200:
        print(f"🗑️ Index '{index_name}' deleted.")
    else:
        print(f"❌ Failed to delete index. Status: {response.status_code}, Response: {response.text}")

def select_csv_file():
    print("📂 Reading CSV...")
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    root.destroy()
    return file_path

def select_index():
    index_info = get_indices_info()
    if not index_info:
        print("⚠️ No eligible indexes found.")
        return None
    
    # Sort by extracted date from index name (ascending)
    index_info.sort(key=lambda x: extract_date_from_index(x['index']))
    
    print("\nAvailable indexes:")
    print("0. Return to main menu")
    for idx, entry in enumerate(index_info, start=1):
        name = entry['index']
        docs = f"{int(entry['docs.count']):,}"
        size = entry['store.size']
        print(f"{idx}. {name} - {docs} documents - {size}")

    selected = input("Select an index (number): ")
    if selected == '0':
        return None
    try:
        return index_info[int(selected) - 1]['index']
    except (IndexError, ValueError):
        print("Invalid selection.")
        return None

def main():
    ensure_elasticsearch_connection()
    while True:
        print("\n=== Elasticsearch CSV Uploader ===")
        print("1. Create new index and upload data")
        print("2. Upload data to existing index")
        print("3. Manage index (delete)")
        print("0. Exit")

        choice = input("Enter choice: ")

        if choice == '1':
            base_name = input("Enter name for new index (case or project name): ")
            index_name = create_index_with_mapping(base_name)
            selected_index = index_name
            csv_path = select_csv_file()
            if not csv_path:
                print("No file selected. Returning to menu.")
                continue
            df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False, on_bad_lines='warn')
            df = df.where(pd.notnull(df), None)
            timestamp_column = select_timestamp_column(df)
            json_file = convert_csv_to_json(csv_path, selected_index, index_name=selected_index, timestamp_column=timestamp_column)
            upload_to_index(selected_index, json_file)

        elif choice == '2':
            selected_index = select_index()
            if not selected_index:
                continue
            csv_path = select_csv_file()
            if not csv_path:
                print("No file selected. Returning to menu.")
                continue
            df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False, on_bad_lines='warn')
            df = df.where(pd.notnull(df), None)
            timestamp_column = select_timestamp_column(df)
            json_file = convert_csv_to_json(csv_path, selected_index, index_name=selected_index, timestamp_column=timestamp_column)
            upload_to_index(selected_index, json_file)

        elif choice == '3':
            selected_index = select_index()
            if not selected_index:
                continue
            confirm = input(f"Are you sure you want to delete '{selected_index}'? (y/n): ")
            if confirm.lower() in ['y', 'yes']:
                delete_index(selected_index)
            else:
                print("Cancelled.")

        elif choice == '0':
            print("Goodbye!")
            break

        else:
            print("Invalid choice.")

if __name__ == '__main__':
    main()
