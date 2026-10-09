"""CSV2ELK v0.6: strict IP discovery, GeoIP, timestamp and Kibana Data Views.
Requires: pip install pandas requests tqdm
Windows-only registry configuration. Run with Python on Windows.

Kibana URL resolution: uses registry KIBANA_URL when set; otherwise probes the
Elasticsearch hostname over http://:5601 then https://:5601 (and same-host
fallbacks) and persists the first reachable URL back to the registry.
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
    """Build candidate Kibana base URLs from the Elasticsearch hostname.

    Tries the common lab layout first (HTTP :5601), then HTTPS :5601, then
    same-scheme / same-host variants so reverse proxies and TLS setups work
    without a pre-configured KIBANA_URL.
    """
    parts = urlsplit(elastic.url)
    host = parts.hostname or 'localhost'
    scheme = parts.scheme or 'https'
    candidates = [
        urlunsplit(('http', f'{host}:5601', '', '', '')),
        urlunsplit(('https', f'{host}:5601', '', '', '')),
    ]
    # Same host/port as Elasticsearch is uncommon for Kibana but useful behind
    # a reverse proxy that routes /app and /api to Kibana.
    if parts.port and parts.port != 5601:
        candidates.append(urlunsplit((scheme, f'{host}:{parts.port}', '', '', '')))
    candidates.append(urlunsplit((scheme, host, '', '', '')))
    # Deduplicate while preserving order
    seen, ordered = set(), []
    for url in candidates:
        if url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


def probe_kibana(elastic, kibana_url):
    """Return True if Kibana responds at kibana_url (auth may still be required)."""
    try:
        response = elastic.session.request(
            'GET',
            kibana_url.rstrip('/') + '/api/status',
            headers={'kbn-xsrf': 'csv2elk', 'Accept': 'application/json'},
            timeout=min(15, TIMEOUT),
        )
        # Any HTTP response (including 401) means the service is reachable.
        return response.status_code < 500
    except requests.exceptions.RequestException:
        return False


def resolve_kibana_url(elastic):
    """Resolve a working Kibana base URL: registry first, then auto-discovery.

    Discovery order:
      1. KIBANA_URL from HKCU registry (if present and reachable)
      2. Elasticsearch hostname with http://:5601 then https://:5601 (and
         same-host fallbacks)
    On a successful probe the working URL is written back to the registry so
    subsequent runs skip the probe. Failures are non-fatal for the upload.
    """
    registry_url = None
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, REGISTRY_PATH) as key:
            registry_url = str(winreg.QueryValueEx(key, 'KIBANA_URL')[0]).strip().rstrip('/')
    except (FileNotFoundError, OSError):
        pass

    candidates = []
    if registry_url and registry_url.startswith(('http://', 'https://')):
        candidates.append(registry_url)
    else:
        if registry_url:
            print(f'⚠️ Ignoring invalid registry KIBANA_URL: {registry_url!r}')
        print('ℹ️ KIBANA_URL not set in registry; discovering from Elasticsearch hostname…')

    for url in kibana_url_from_elasticsearch(elastic):
        if url not in candidates:
            candidates.append(url)

    last_error = None
    for url in candidates:
        print(f'  🔎 Probing Kibana at {url} …')
        if probe_kibana(elastic, url):
            print(f'✅ Kibana reachable at {url}')
            if url != registry_url:
                try:
                    with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, REGISTRY_PATH, 0, winreg.KEY_WRITE) as key:
                        winreg.SetValueEx(key, 'KIBANA_URL', 0, winreg.REG_SZ, url)
                    print(f'✅ Saved KIBANA_URL to registry for future runs')
                except OSError as exc:
                    print(f'⚠️ Could not persist KIBANA_URL to registry: {exc}')
            return url
        last_error = f'no response from {url}'

    print('❌ Unable to reach Kibana at any candidate URL.')
    if last_error:
        print(f'   Last attempt: {last_error}')
    print(r'   Tip: set HKCU\Software\DFIRVault\CSV2ELK\KIBANA_URL to the correct base URL')
    print('        (e.g. https://kibana.example.com:5601) and re-run.')
    return None


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
    """Automatically find a matching Kibana Data View, or offer to create one.

    This is a post-upload convenience operation. Errors never roll back data.
    """
    print(f'\n🔎 Checking Kibana Data Views for {index_name}')
    print('ℹ️ A Kibana Data View is required to see uploaded data in the Discover analytics workspace.')
    kibana_url = resolve_kibana_url(elastic)
    if not kibana_url:
        print('⚠️ Upload succeeded; Data View check was skipped.')
        return

    try:
        response = kibana_request(elastic, kibana_url, 'GET', 'data_views')
        views = response.get('data_view', [])
        if not isinstance(views, list):
            raise RuntimeError('Unexpected Kibana Data Views API response')
    except Exception as exc:
        print(f'❌ Unable to query existing Kibana Data Views: {exc}')
        print('⚠️ Uploaded documents remain in Elasticsearch. Check Kibana permissions and connectivity.')
        return

    matches = [v for v in views if view_matches_index(v.get('title', ''), index_name)]
    if matches:
        # Prefer an exact index, then a case-specific prefix, then general wildcards.
        def specificity(view):
            patterns = [x.strip() for x in view.get('title', '').split(',')
                        if x.strip() and not x.strip().startswith('-')]
            relevant = [x for x in patterns if fnmatch.fnmatchcase(index_name, x)]
            return max(((int(x == index_name), len(x.replace('*', '').replace('?', '')),
                         -x.count('*') - x.count('?')) for x in relevant),
                       default=(0, 0, 0))

        best = max(matches, key=specificity)
        name = best.get('name') or best.get('title')
        print(f'✅ Existing Kibana Data View found: {name} (pattern: {best.get("title")})')
        print(f'✅ Confirmed pattern includes index: {index_name}')
        if len(matches) > 1:
            print(f'ℹ️ {len(matches)} matching Data Views found; showing the most specific match.')
        print(f'🔎 In Kibana → Analytics → Discover, make sure the Data View "{name}" is selected.')
        print('ℹ️ If documents are missing, check the Discover time filter and timestamp field.')
        return

    print(f'⚠️ No existing Kibana Data View matches index {index_name}.')
    print('ℹ️ Create a Data View to see the uploaded data in Kibana Discover.')
    print('   Example: an index named case0001_20261008 can use pattern case*')
    print('   That pattern will also include future indices beginning with case.')
    if input('Would you like to create a Data View now? [Y/n]: ').strip().lower() in ('n', 'no'):
        print('ℹ️ You can create one later in Kibana → Stack Management → Data Views.')
        return

    suggested = index_name
    pattern = input(f'Index pattern [{suggested}] (e.g. case*): ').strip() or suggested
    if not view_matches_index(pattern, index_name):
        print(f'❌ Pattern {pattern!r} does not match index {index_name!r}.')
        print('⚠️ No Data View created; choose an exact index name or a matching wildcard.')
        return
    name = input(f'Data View name [{index_name}]: ').strip() or index_name
    try:
        mapping = elastic.request('GET', f'{index_name}/_mapping')
        props = mapping.get(index_name, {}).get('mappings', {}).get('properties', {})
        has_timestamp = props.get('timestamp_field', {}).get('type') in ('date', 'date_nanos')
        body = {'data_view': {'title': pattern, 'name': name}}
        if has_timestamp:
            body['data_view']['timeFieldName'] = 'timestamp_field'
            print('📅 Timestamp field: timestamp_field')
        else:
            print('⚠️ No timestamp_field date mapping found; creating without a time filter.')
        if input(f'Create Data View {name!r} with pattern {pattern!r}? [Y/n]: ').strip().lower() in ('n', 'no'):
            print('ℹ️ Data View creation cancelled.')
            return
        created = kibana_request(elastic, kibana_url, 'POST', 'data_views/data_view', json=body)
        view_id = created.get('data_view', {}).get('id', '(ID not returned)')
        print(f'✅ Created Kibana Data View: {name} (ID: {view_id})')
        print(f'✅ Confirmed index pattern {pattern!r} matches {index_name!r}.')
        print(f'🔎 Open Kibana Discover and select the Data View "{name}".')
    except Exception as exc:
        print(f'❌ Kibana Data View creation failed: {exc}')
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
