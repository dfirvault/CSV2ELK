"""CSV2ELK v0.4: strict IP discovery, GeoIP, timestamp and Kibana Data Views.
Requires: pip install pandas requests tqdm
Windows-only registry configuration. Run with Python on Windows.
"""
import ipaddress
import fnmatch
from urllib.parse import urlsplit, urlunsplit
import json
import os
import re
import time
import uuid
import winreg
from datetime import datetime, timezone
from pathlib import Path
from tkinter import Tk, filedialog

import pandas as pd
import requests
from tqdm import tqdm
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REGISTRY_PATH = r"Software\DFIRVault\CSV2ELK"
CHUNK_DOCS = 1000
TIMEOUT = 120


def load_config():
    """Preserve the original CSV2ELK registry credential workflow."""
    cfg = {'ELASTICSEARCH_URL': '', 'USERNAME': '', 'PASSWORD': ''}
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, REGISTRY_PATH) as key:
            for name in cfg:
                try:
                    cfg[name] = winreg.QueryValueEx(key, name)[0]
                except FileNotFoundError:
                    pass
    except FileNotFoundError:
        print('⚠️ Configuration not found. Enter Elasticsearch connection details.')
    if not cfg['ELASTICSEARCH_URL']:
        cfg['ELASTICSEARCH_URL'] = input('Elasticsearch URL: ').strip()
    if not cfg['USERNAME']:
        cfg['USERNAME'] = input('Username: ').strip()
    if not cfg['PASSWORD']:
        cfg['PASSWORD'] = input('Password: ').strip()
    return cfg


def save_config(cfg):
    """Maintain backwards compatibility with original HKCU settings."""
    with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, REGISTRY_PATH, 0, winreg.KEY_WRITE) as key:
        for name in ('ELASTICSEARCH_URL', 'USERNAME', 'PASSWORD'):
            winreg.SetValueEx(key, name, 0, winreg.REG_SZ, cfg[name])


def connect_elasticsearch():
    while True:
        cfg = load_config()
        elastic = Elastic(cfg)
        try:
            elastic.request('GET', '_cluster/health')
            save_config(cfg)
            print(f"✅ Connected to Elasticsearch at {cfg['ELASTICSEARCH_URL']}")
            return elastic
        except requests.exceptions.SSLError as exc:
            print('❌ SSL connection error:', exc)
            raise
        except Exception as exc:
            print('❌ Elasticsearch connection failed:', exc)
            if input('Re-enter connection details? [Y/n]: ').strip().lower() == 'n':
                raise
            # Only clear bad credentials in memory. Original registry values remain
            # until a successful connection is established.
            cfg['ELASTICSEARCH_URL'] = input('Elasticsearch URL: ').strip() or cfg['ELASTICSEARCH_URL']
            cfg['USERNAME'] = input('Username: ').strip() or cfg['USERNAME']
            cfg['PASSWORD'] = input('Password: ').strip()
            elastic = Elastic(cfg)
            try:
                elastic.request('GET', '_cluster/health')
                save_config(cfg)
                return elastic
            except Exception as exc2:
                print('❌ Connection still unsuccessful:', exc2)


def sanitize_index_name(name):
    result = re.sub(r'[^a-z0-9_-]', '', re.sub(r'\s+', '_', name.lower()))
    if not result or result[0] in '_-+.' or result in ('.', '..'):
        raise ValueError('Invalid index name')
    return result


def sanitize_column(name):
    return re.sub(r'[^\w@#]', '_', str(name).replace('.', '_'))


def deduplicate_columns(columns):
    counts, output = {}, []
    for name in columns:
        base = sanitize_column(name) or 'unnamed'
        candidate = base
        while candidate in counts:
            counts[base] = counts.get(base, 0) + 1
            candidate = f'{base}_{counts[base]}'
        counts[candidate] = 0
        output.append(candidate)
    return output


def is_ip(value):
    if value is None or pd.isna(value):
        return False
    try:
        ipaddress.ip_address(str(value).strip())
        return True
    except ValueError:
        return False


def detect_ip_fields(df):
    """Only classify a column if EVERY nonempty value is a standalone IP address.

    This scans the entire CSV, not a sample. Mixed-content fields are excluded,
    even if only one row contains a URL, port, CIDR, message, or malformed IP.
    Empty values are allowed, but at least one valid address is required.
    """
    detected = []
    for field in df.columns:
        valid = 0
        invalid_example = None
        for value in df[field]:
            if value is None or pd.isna(value) or not str(value).strip():
                continue
            if not is_ip(value):
                invalid_example = str(value)[:100]
                break
            valid += 1
        if invalid_example is None and valid:
            detected.append(field)
            print(f'  🌍 IP field: {field} (all {valid:,} nonempty values valid)')
        elif invalid_example is not None:
            print(f'  ↪ Skipped {field}: contains non-IP value {invalid_example!r}')
    return detected


def guess_timestamp_column(columns):
    """Original CSV2ELK priority-based timestamp field suggestion."""
    priority = ['timestamp', '@timestamp', 'time', 'datetime', 'date']
    for candidate in priority:
        for column in columns:
            if re.search(candidate, column, re.IGNORECASE):
                return column
    return None


def choose_timestamp(df):
    """Preserve original header preview, sample conversion and confirmation."""
    if df.empty:
        print('⚠️ DataFrame is empty. Cannot determine timestamp column.')
        return None
    print('\nCSV Headers with Sample Values (row 1):')
    first_row = df.iloc[0].to_dict()
    for i, column in enumerate(df.columns, 1):
        print(f'{i}. {column} - {first_row.get(column, "")}')
    guess = guess_timestamp_column(df.columns)
    if guess:
        print(f'\n📌 Suggested timestamp column: {guess} (e.g. {first_row.get(guess, "N/A")})')
    while True:
        prompt = (f"Select timestamp column, either in Epoch time or ISO-8601 "
                  f"(YYYY-MM-DDTHH:MM:SSZ) [Enter for '{guess or 'none'}', "
                  "0 for no timestamp, or field number]: ")
        choice = input(prompt).strip()
        if choice == '0':
            print('⚠️ No timestamp field selected.')
            return None
        if not choice and guess:
            selected = guess
        elif not choice:
            print('⚠️ No automatic timestamp match. Select a field number or 0.')
            continue
        else:
            try:
                number = int(choice)
                if not 1 <= number <= len(df.columns):
                    raise ValueError()
                selected = df.columns[number - 1]
            except ValueError:
                print('❌ Invalid selection. Try again.')
                continue
        print(f"\n📋 Sample values from '{selected}':")
        for sample in df[selected].head(5):
            converted = parse_timestamp(sample)
            if converted:
                print(f'  - {sample} → {converted}')
            else:
                print(f'  - {sample} (not parsed)')
        print('🔍 Make sure these timestamps are valid before importing.')
        confirm = input('✅ Proceed with this timestamp field? (y/n): ').strip().lower()
        if confirm in ('y', 'yes'):
            return selected
        print('↩️ Select a different timestamp field.')


def parse_timestamp(value):
    if value is None or pd.isna(value) or str(value).strip() == '':
        return None
    try:
        s = str(value).strip()
        if re.fullmatch(r'\d+(\.\d+)?', s):
            n = float(s)
            if n > 1e11:
                n /= 1000
            return datetime.fromtimestamp(n, timezone.utc).isoformat()
        return pd.to_datetime(value, utc=True, errors='raise').isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if pd.isna(value):
        return None
    return value.item() if hasattr(value, 'item') else value


class Elastic:
    def __init__(self, cfg):
        self.url = cfg['ELASTICSEARCH_URL'].rstrip('/')
        self.session = requests.Session()
        self.session.auth = (cfg['USERNAME'], cfg['PASSWORD'])
        self.session.verify = False  # Original lab behaviour; prefer trusted CA for production
        self.session.headers.update({'Accept': 'application/json'})

    def request(self, method, path, **kwargs):
        r = self.session.request(method, self.url + '/' + path.lstrip('/'), timeout=TIMEOUT, **kwargs)
        if not r.ok:
            raise RuntimeError(f'{method} {path}: HTTP {r.status_code}: {r.text[:3000]}')
        return r.json() if r.content else {}

    def index_exists(self, name):
        r = self.session.head(self.url + '/' + name, timeout=TIMEOUT)
        if r.status_code == 404:
            return False
        if not r.ok:
            raise RuntimeError(f'Checking index failed: {r.status_code} {r.text}')
        return True


def pipeline_definition(ip_fields):
    processors = []
    for field in ip_fields:
        # Target names are generated from sanitized CSV column names.
        processors.append({'geoip': {
            'field': field,
            'target_field': f'geoip.{field}',
            'ignore_missing': True,
            'ignore_failure': True
        }})
    return {'description': 'CSV2ELK per-upload IP geolocation', 'processors': processors}


def geo_mapping(ip_fields):
    props = {'timestamp_field': {'type': 'date'}}
    if ip_fields:
        props['geoip'] = {'properties': {field: {'properties': {
            'location': {'type': 'geo_point'}
        }} for field in ip_fields}}
    # Preserve original strings, including invalid/non-IP outliers; only map the derived location.
    return props


def prepare_index(elastic, index_name, ip_fields, new_index):
    properties = geo_mapping(ip_fields)
    if new_index:
        elastic.request('PUT', index_name, json={'mappings': {'properties': properties},
                                                 'settings': {'number_of_replicas': 0}})
    else:
        # Check timestamp mapping even if no IP fields were detected.
        # Mapping updates cannot change existing field types. Fail before uploading.
        current = elastic.request('GET', f'{index_name}/_mapping')[index_name]['mappings'].get('properties', {})
        existing_ts = current.get('timestamp_field', {})
        if existing_ts and existing_ts.get('type') not in ('date', 'date_nanos'):
            raise RuntimeError('Existing timestamp_field is not a date mapping; reindex required')
        if not existing_ts:
            elastic.request('PUT', f'{index_name}/_mapping', json={'properties': {'timestamp_field': {'type': 'date'}}})
        if not ip_fields:
            return
        geo = current.get('geoip', {})
        if geo and geo.get('type') not in (None, 'object'):
            raise RuntimeError('Existing geoip field is not an object. Choose a different index.')
        existing_fields = geo.get('properties', {})
        for field in ip_fields:
            loc = existing_fields.get(field, {}).get('properties', {}).get('location', {})
            if loc and loc.get('type') != 'geo_point':
                raise RuntimeError(f'geoip.{field}.location is not geo_point in existing index')
        elastic.request('PUT', f'{index_name}/_mapping', json={'properties': {'geoip': properties['geoip']}})


def upload_csv(elastic, csv_path, index_name, new_index):
    # Read as strings so addresses (including IPv6) and forensic identifiers are not coerced.
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False, on_bad_lines='warn')
    if df.empty:
        raise ValueError('CSV has no rows')
    df.columns = deduplicate_columns(df.columns)
    print(f'✅ Loaded {len(df):,} rows, {len(df.columns)} fields')
    timestamp = choose_timestamp(df)
    if timestamp:
        print('✅ Timestamp mapping: timestamp_field (date), sourced from ' + timestamp)
    ip_fields = detect_ip_fields(df)
    if ip_fields:
        print('🌍 Strictly validated IP columns:', ', '.join(ip_fields))
    else:
        print('⚠️ No exclusively-IP columns detected; importing without GeoIP enrichment')
    if input('Continue with these fields? [Y/n]: ').strip().lower() == 'n':
        return False

    # Each upload has its own pipeline; mapping is index-wide and extended per upload.
    pipeline_id = f'csv2elk_geoip_{index_name}_{uuid.uuid4().hex[:8]}' if ip_fields else None
    if pipeline_id:
        elastic.request('PUT', f'_ingest/pipeline/{pipeline_id}', json=pipeline_definition(ip_fields))
        print('✅ Created GeoIP pipeline:', pipeline_id)
    prepare_index(elastic, index_name, ip_fields, new_index)
    print('✅ Index mapping ready')

    indexed, failed = 0, 0
    for offset in tqdm(range(0, len(df), CHUNK_DOCS), desc='📤 Uploading CSV'):
        rows = df.iloc[offset:offset + CHUNK_DOCS]
        lines = []
        for record in rows.to_dict(orient='records'):
            record = {k: clean_value(v) for k, v in record.items()}
            if timestamp:
                parsed = parse_timestamp(record.get(timestamp))
                if parsed:
                    record['timestamp_field'] = parsed
            # Preserve original evidence values, including malformed IPs.
            # GeoIP ignore_failure skips values it cannot enrich.
            lines.append(json.dumps({'index': {'_index': index_name}}, ensure_ascii=False))
            lines.append(json.dumps(record, ensure_ascii=False, default=str))
        body = ('\n'.join(lines) + '\n').encode('utf-8')
        params = {'pipeline': pipeline_id} if pipeline_id else {}
        result = elastic.request('POST', '_bulk', params=params, data=body,
                                 headers={'Content-Type': 'application/x-ndjson'})
        for item in result.get('items', []):
            detail = item.get('index', {})
            if detail.get('error') or detail.get('status', 500) >= 300:
                failed += 1
                if failed <= 10:
                    print('❌ Indexing error:', json.dumps(detail.get('error', detail)))
            else:
                indexed += 1
    print(f"{'✅' if failed == 0 else '⚠️'} Upload finished: {indexed:,} indexed, {failed:,} failed")
    if pipeline_id:
        print(f'✅ Pipeline retained: {pipeline_id}')
        print('🌍 Geo locations: ' + ', '.join(f'geoip.{f}.location' for f in ip_fields))
    if failed:
        print('⚠️ WARNING: Some documents failed; investigate errors before re-uploading.')
    return indexed > 0



def kibana_url_from_elasticsearch(elastic):
    """Suggest Kibana's local port; permit an override for reverse proxies/HTTPS."""
    parts = urlsplit(elastic.url)
    # Kibana commonly runs HTTP on 5601 even if Elasticsearch uses HTTPS on 9200.
    return urlunsplit(('http', f'{parts.hostname}:5601', '', '', ''))


def kibana_request(elastic, kibana_url, method, path, **kwargs):
    headers = {'kbn-xsrf': 'csv2elk', 'Accept': 'application/json'}
    headers.update(kwargs.pop('headers', {}))
    response = elastic.session.request(
        method, kibana_url.rstrip('/') + '/api/' + path.lstrip('/'),
        headers=headers, timeout=TIMEOUT, **kwargs
    )
    if not response.ok:
        raise RuntimeError(f'Kibana {method} {path}: HTTP {response.status_code}: {response.text[:1500]}')
    return response.json() if response.content else {}


def view_matches_index(pattern, index_name):
    """Match Kibana comma-separated index expressions, including -exclusions."""
    expressions = [part.strip() for part in pattern.split(',') if part.strip()]
    included = any(fnmatch.fnmatchcase(index_name, expr) for expr in expressions if not expr.startswith('-'))
    excluded = any(fnmatch.fnmatchcase(index_name, expr[1:]) for expr in expressions if expr.startswith('-'))
    return included and not excluded


def ask_number(prompt, count, allow_zero=True):
    while True:
        answer = input(prompt).strip()
        if answer.isdigit() and (0 if allow_zero else 1) <= int(answer) <= count:
            return int(answer)
        print('❌ Invalid selection. Try again.')


def offer_data_view(elastic, index_name):
    """Optional Kibana Data View workflow; failures never roll back uploaded evidence."""
    print(f'\n🔎 Kibana Data View setup for {index_name}')
    print('1. Select an existing Data View')
    print('2. Create a new Data View')
    print('0. Skip')
    choice = ask_number('Select an option: ', 2)
    if choice == 0:
        return

    default_url = kibana_url_from_elasticsearch(elastic)
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, REGISTRY_PATH) as key:
            default_url = winreg.QueryValueEx(key, 'KIBANA_URL')[0] or default_url
    except (FileNotFoundError, OSError):
        pass
    kibana_url = input(f'Kibana URL [{default_url}]: ').strip() or default_url
    try:
        # Reuse the working Elasticsearch credentials/session; Kibana may have separate auth.
        data = kibana_request(elastic, kibana_url, 'GET', 'data_views')
        views = data.get('data_view', [])
        if not isinstance(views, list):
            raise RuntimeError('Unexpected Kibana Data Views API response')
    except Exception as exc:
        print(f'❌ Cannot retrieve Kibana Data Views: {exc}')
        print('⚠️ Your CSV upload is unaffected. Check the Kibana URL, credentials and permissions.')
        return

    try:
        if choice == 1:
            if not views:
                print('⚠️ No existing Data Views found. Switching to creation.')
                choice = 2
            else:
                print('\nExisting Kibana Data Views:')
                for i, view in enumerate(views, 1):
                    pattern = view.get('title', '')
                    status = '✅ MATCH' if view_matches_index(pattern, index_name) else '❌ NO MATCH'
                    print(f"{i}. {view.get('name') or pattern} [{pattern}] — {status}")
                selected = ask_number('Select a Data View (0 to cancel): ', len(views))
                if selected == 0:
                    return
                view = views[selected - 1]
                pattern = view.get('title', '')
                if view_matches_index(pattern, index_name):
                    print(f'✅ Existing Data View {pattern!r} includes index {index_name!r}.')
                    print('🔎 Open Kibana Discover and select that Data View.')
                    return
                print(f'⚠️ Data View {pattern!r} does NOT match index {index_name!r}.')
                print('The uploaded documents will not appear through this Data View.')
                if input('Create a new Data View for this index instead? [Y/n]: ').strip().lower() == 'n':
                    return
                choice = 2

        if choice == 2:
            existing_exact = next((v for v in views if v.get('title') == index_name), None)
            if existing_exact:
                print(f'✅ An exact-match Data View already exists: {existing_exact.get("name") or index_name}')
                return
            suggested = index_name
            pattern = input(f'Index pattern [{suggested}]: ').strip() or suggested
            if not view_matches_index(pattern, index_name):
                print(f'❌ Pattern {pattern!r} does not match {index_name!r}. No Data View created.')
                return
            name = input(f'Data View name [{index_name}]: ').strip() or index_name
            # timestamp_field is the normalized date produced by the existing uploader.
            timestamp_mapping = elastic.request('GET', f'{index_name}/_mapping')
            props = timestamp_mapping.get(index_name, {}).get('mappings', {}).get('properties', {})
            has_timestamp = props.get('timestamp_field', {}).get('type') in ('date', 'date_nanos')
            body = {'data_view': {'title': pattern, 'name': name}}
            if has_timestamp:
                print('📅 Timestamp field: timestamp_field')
                body['data_view']['timeFieldName'] = 'timestamp_field'
            else:
                print('⚠️ No timestamp_field date mapping; creating Data View without a time filter.')
            if input(f'Create Data View {name!r} with pattern {pattern!r}? [Y/n]: ').strip().lower() == 'n':
                return
            created = kibana_request(elastic, kibana_url, 'POST', 'data_views/data_view', json=body)
            view_id = created.get('data_view', {}).get('id', '(ID not returned)')
            print(f'✅ Created Kibana Data View: {name} (ID: {view_id})')
            print(f'✅ Confirmed index pattern {pattern!r} matches {index_name!r}.')
            print('🔎 Open Kibana Discover and select your new Data View.')
        try:
            with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, REGISTRY_PATH, 0, winreg.KEY_WRITE) as key:
                winreg.SetValueEx(key, 'KIBANA_URL', 0, winreg.REG_SZ, kibana_url)
        except OSError:
            print('⚠️ Could not save Kibana URL to registry.')
    except Exception as exc:
        print(f'❌ Kibana Data View operation failed: {exc}')
        print('⚠️ Uploaded documents remain in Elasticsearch.')


def select_index(elastic):
    indices = elastic.request('GET', '_cat/indices?format=json&h=index,docs.count,store.size')
    indices = [i for i in indices if not i['index'].startswith('.') and not i['index'].startswith('log')]
    indices.sort(key=lambda item: item['index'])
    if not indices:
        print('⚠️ No eligible indices found.')
        return None
    print('\nAvailable indexes:')
    print('0. Return to main menu')
    for n, item in enumerate(indices, 1):
        print(f'{n}. {item["index"]} - {item.get("docs.count", "?")} documents - {item.get("store.size", "?")}')
    try:
        selected = int(input('Select an index (number): '))
        if selected == 0:
            return None
        return indices[selected - 1]['index'] if 1 <= selected <= len(indices) else None
    except ValueError:
        print('❌ Invalid selection')
        return None


def select_csv_file():
    root = Tk()
    root.withdraw()
    path = filedialog.askopenfilename(filetypes=[('CSV files', '*.csv')])
    root.destroy()
    return path


def main():
    print('')
    print('Developed by Jacob Wilson - Version 0.6')
    print('dfirvault@gmail.com')
    print('')
    elastic = connect_elasticsearch()
    while True:
        print('\n=== Elasticsearch CSV Uploader ===')
        print('1. Create new index and upload data')
        print('2. Upload data to existing index')
        print('3. Manage index (delete)')
        print('0. Exit')
        choice = input('Enter choice: ').strip()
        if choice == '0':
            print('👋 Goodbye!')
            break
        if choice == '3':
            name = select_index(elastic)
            if name and input(f'Are you sure you want to delete {name}? (y/n): ').lower() in ('y', 'yes'):
                elastic.request('DELETE', name)
                print(f'🗑️ Deleted {name}')
            continue
        if choice not in ('1', '2'):
            print('❌ Invalid choice')
            continue
        try:
            if choice == '1':
                base = sanitize_index_name(input('Enter name for new index (case or project name): ').strip())
                name = f'{base}_{datetime.now().strftime("%Y%m%d")}'
                if elastic.index_exists(name):
                    print(f'⚠️ Index {name} already exists. Use option 2.')
                    continue
                new_index = True
            else:
                name = select_index(elastic)
                if not name:
                    continue
                new_index = False
            path = select_csv_file()
            if not path:
                print('⚠️ No file selected. Returning to menu.')
                continue
            uploaded = upload_csv(elastic, path, name, new_index)
            if uploaded:
                offer_data_view(elastic, name)
        except Exception as exc:
            print(f'❌ UPLOAD STOPPED: {exc}')


if __name__ == '__main__':
    main()
