"""CSV2ELK v1.0: strict IP discovery, GeoIP, timestamp and Kibana Data Views.
Requires: pip install pandas requests tqdm
Windows-only registry configuration. Run with Python on Windows.
"""
import csv
import subprocess
import shutil
import ctypes
import random
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import codecs
import io
import zipfile
import base64
import itertools
import webbrowser
import socket
import sys
from getpass import getpass
import ipaddress
import fnmatch
from urllib.parse import urlsplit, urlunsplit, quote
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
CHUNK_DOCS = 5000
UPLOAD_BATCH_BYTES = 100 * 1024 * 1024  # Fixed 100 MiB payload cap.
DEFAULT_UPLOAD_WORKERS = 2
MAX_UPLOAD_WORKERS = 32
BULK_429_RETRIES = 5
BULK_READ_TIMEOUT = 600
BULK_RETRY_WAIT = 15
BULK_RETRY_MAX_WAIT = 60
TIMEOUT = 120


def suggested_urls():
    hostname = socket.gethostname()
    # User-requested defaults; deployments may use different ports.
    return {'ELASTICSEARCH_URL': f'https://{hostname}:5601',
            'KIBANA_URL': f'http://{hostname}:9200'}


def prompt_url(label, default):
    while True:
        value = input(f'{label} [{default}] (Enter to accept): ').strip() or default
        parts = urlsplit(value)
        try:
            valid = parts.scheme in ('http', 'https') and parts.hostname and parts.port != 0
        except ValueError:
            valid = False
        if valid and not parts.username and not parts.password and not parts.query and not parts.fragment:
            return value.rstrip('/')
        print('❌ Enter a valid http:// or https:// URL with a hostname and optional port/path.')


def masked_password(prompt='Password: '):
    """Windows console input with feedback, backspace and clipboard paste."""
    if os.name != 'nt' or not sys.stdin.isatty():
        return getpass(prompt)
    import msvcrt
    print(prompt, end='', flush=True)
    chars = []
    while True:
        char = msvcrt.getwch()
        if char in ('\r', '\n'):
            print()
            return ''.join(chars)
        if char == '\x03':
            print()
            raise KeyboardInterrupt
        if char in ('\x00', '\xe0'):
            msvcrt.getwch()  # Ignore function/arrow key scan codes.
            continue
        if char == '\x08':
            if chars:
                chars.pop()
                print('\b \b', end='', flush=True)
            continue
        if char == '\x16':  # Ctrl+V if the terminal did not already paste text.
            root = Tk()
            root.withdraw()
            try:
                pasted = root.clipboard_get()
            except Exception:
                pasted = ''
            finally:
                root.destroy()
            pasted = ''.join(c for c in pasted if c.isprintable())
            chars.extend(pasted)
            print('*' * len(pasted), end='', flush=True)
            continue
        if char.isprintable():
            chars.append(char)
            print('*', end='', flush=True)


def validated_workers(value):
    try:
        workers = int(value)
    except (ValueError, TypeError):
        return DEFAULT_UPLOAD_WORKERS
    return workers if 1 <= workers <= MAX_UPLOAD_WORKERS else DEFAULT_UPLOAD_WORKERS


def prompt_workers(current):
    current = validated_workers(current)
    while True:
        answer = input(f'Upload workers [{current}] (1–{MAX_UPLOAD_WORKERS}, Enter to retain): ').strip()
        if answer.lower() in ('', 'y', 'yes'):
            return current
        if answer.isdigit() and 1 <= int(answer) <= MAX_UPLOAD_WORKERS:
            return int(answer)
        print(f'❌ Enter a whole number between 1 and {MAX_UPLOAD_WORKERS}.')


def load_config():
    """Preserve the original CSV2ELK registry credential workflow."""
    cfg = {'ELASTICSEARCH_URL': '', 'USERNAME': '', 'PASSWORD': '', 'KIBANA_URL': '',
           'UPLOAD_WORKERS': DEFAULT_UPLOAD_WORKERS}
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, REGISTRY_PATH) as key:
            for name in cfg:
                try:
                    cfg[name] = winreg.QueryValueEx(key, name)[0]
                except FileNotFoundError:
                    pass
    except FileNotFoundError:
        print('⚠️ Configuration not found. Enter Elasticsearch connection details.')
    cfg['UPLOAD_WORKERS'] = validated_workers(cfg.get('UPLOAD_WORKERS'))
    defaults = suggested_urls()
    if not cfg['ELASTICSEARCH_URL']:
        cfg['ELASTICSEARCH_URL'] = prompt_url('Elasticsearch URL', defaults['ELASTICSEARCH_URL'])
    if not cfg['KIBANA_URL']:
        cfg['KIBANA_URL'] = prompt_url('Kibana URL', defaults['KIBANA_URL'])
    if not cfg['USERNAME']:
        cfg['USERNAME'] = input('Username: ').strip()
    if not cfg['PASSWORD']:
        cfg['PASSWORD'] = masked_password('Password: ')
    return cfg


def save_config(cfg):
    """Maintain backwards compatibility with original HKCU settings."""
    with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, REGISTRY_PATH, 0, winreg.KEY_WRITE) as key:
        for name in ('ELASTICSEARCH_URL', 'USERNAME', 'PASSWORD', 'KIBANA_URL'):
            winreg.SetValueEx(key, name, 0, winreg.REG_SZ, cfg.get(name, ''))
        winreg.SetValueEx(key, 'UPLOAD_WORKERS', 0, winreg.REG_DWORD,
                          validated_workers(cfg.get('UPLOAD_WORKERS')))


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
            cfg['ELASTICSEARCH_URL'] = prompt_url('Elasticsearch URL', cfg['ELASTICSEARCH_URL'])
            cfg['KIBANA_URL'] = prompt_url('Kibana URL', cfg['KIBANA_URL'])
            cfg['USERNAME'] = input('Username: ').strip() or cfg['USERNAME']
            cfg['PASSWORD'] = masked_password('Password: ')
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
    print('\nCSV and Expanded JSON Fields with Sample Values:')
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
        if choice.lower() in ('', 'y', 'yes') and guess:
            selected = guess
        elif choice.lower() in ('', 'y', 'yes'):
            print('ℹ️ No timestamp field selected.')
            return None
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
        confirm = input('✅ Proceed with this timestamp field? [Y/n]: ').strip().lower()
        if confirm in ('', 'y', 'yes'):
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
        # Preserve nanosecond timestamps through pandas; datetime handles <=6 digits directly.
        if not re.search(r'\.\d{7,}', s):
            try:
                instant = datetime.fromisoformat(s.replace('Z', '+00:00'))
                if instant.tzinfo is None:
                    instant = instant.replace(tzinfo=timezone.utc)
                return instant.astimezone(timezone.utc).isoformat()
            except ValueError:
                pass
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


class ElasticHTTPError(RuntimeError):
    def __init__(self, message, status_code):
        super().__init__(message)
        self.status_code = status_code


class Elastic:
    def __init__(self, cfg):
        self.cfg = dict(cfg)
        self.url = cfg['ELASTICSEARCH_URL'].rstrip('/')
        self.session = requests.Session()
        self.session.auth = (cfg['USERNAME'], cfg['PASSWORD'])
        self.session.verify = False  # Original lab behaviour; prefer trusted CA for production
        self.session.headers.update({'Accept': 'application/json'})

    def request(self, method, path, **kwargs):
        r = self.session.request(method, self.url + '/' + path.lstrip('/'), timeout=kwargs.pop('timeout', TIMEOUT), **kwargs)
        if not r.ok:
            raise ElasticHTTPError(f'{method} {path}: HTTP {r.status_code}: {r.text[:3000]}', r.status_code)
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
        processors.append({'geoip': {
            'field': field, 'target_field': f'asn.{field}',
            'database_file': 'GeoLite2-ASN.mmdb',
            'properties': ['asn', 'organization_name', 'network'],
            'ignore_missing': True, 'ignore_failure': True
        }})
    return {'description': 'CSV2ELK per-upload geolocation and ASN', 'processors': processors}


def geo_mapping(ip_fields):
    props = {'timestamp_field': {'type': 'date'}}
    if ip_fields:
        props['geoip'] = {'properties': {field: {'properties': {
            'location': {'type': 'geo_point'}
        }} for field in ip_fields}}
    if ip_fields:
        props['asn'] = {'properties': {field: {'properties': {
            'asn': {'type': 'long'}, 'organization_name': {'type': 'keyword'},
            'network': {'type': 'keyword'}
        }} for field in ip_fields}}
    # Preserve original strings, including invalid/non-IP outliers; only map the derived location.
    return props


def prepare_index(elastic, index_name, ip_fields, new_index, field_count=0):
    properties = geo_mapping(ip_fields)
    # Dynamic strings may add text + keyword mappings. GeoIP also adds many objects/leaves.
    required_limit = max(10000, field_count * 4 + len(ip_fields) * 32 + 1000)
    if new_index:
        elastic.request('PUT', index_name, json={'mappings': {'date_detection': False, 'numeric_detection': False, 'properties': properties},
                                                 'settings': {'number_of_replicas': 0,
                                                              'index.mapping.total_fields.limit': required_limit}})
        print(f'✅ Index field limit: {required_limit:,}')
    else:
        settings = elastic.request('GET', f'{index_name}/_settings', params={'flat_settings': 'true'})
        current_limit = int(settings[index_name]['settings'].get('index.mapping.total_fields.limit', 1000))
        mapping = elastic.request('GET', f'{index_name}/_mapping')[index_name]['mappings']
        def count_fields(properties):
            return sum(1 + count_fields(v.get('properties', {})) + count_fields(v.get('fields', {}))
                       for v in properties.values())
        required_limit = max(required_limit, count_fields(mapping.get('properties', {})) +
                             len(mapping.get('runtime', {})) + field_count * 4 + len(ip_fields) * 32 + 1000)
        if current_limit < required_limit:
            elastic.request('PUT', f'{index_name}/_settings',
                            json={'index.mapping.total_fields.limit': required_limit})
        print(f'✅ Index field limit: {max(current_limit, required_limit):,}')
        # Check timestamp mapping even if no IP fields were detected.
        # Mapping updates cannot change existing field types. Fail before uploading.
        current = elastic.request('GET', f'{index_name}/_mapping')[index_name]['mappings'].get('properties', {})
        date_updates = {field: {'type': info['type'], 'ignore_malformed': True}
                        for field, info in current.items()
                        if field != 'timestamp_field' and info.get('type') in ('date', 'date_nanos')}
        elastic.request('PUT', f'{index_name}/_mapping',
                        json={'date_detection': False, 'numeric_detection': False, 'properties': date_updates})
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
        elastic.request('PUT', f'{index_name}/_mapping', json={'properties': {'geoip': properties['geoip'], 'asn': properties['asn']}})


def detect_csv_format(path):
    """BOM first, then strict UTF-8 detection; legacy fallback is Windows-1252."""
    with open(path, 'rb') as raw:
        data = raw.read(256 * 1024)
    encoding = None
    for bom, candidate in ((codecs.BOM_UTF32_LE, 'utf-32-le'), (codecs.BOM_UTF32_BE, 'utf-32-be'),
                           (codecs.BOM_UTF16_LE, 'utf-16-le'), (codecs.BOM_UTF16_BE, 'utf-16-be'),
                           (codecs.BOM_UTF8, 'utf-8-sig')):
        if data.startswith(bom):
            encoding = candidate
            break
    if encoding is None:
        try:
            codecs.getincrementaldecoder('utf-8')().decode(data, final=False)
            encoding = 'utf-8-sig'
        except UnicodeDecodeError:
            encoding = 'cp1252'
            print('ℹ️ Input is not UTF-8 in the format sample; using Windows-1252.')
    text = codecs.getincrementaldecoder(encoding)(errors='replace').decode(data, final=False).lstrip('\ufeff')
    # Header alone prevents broken quotes in later rows from confusing the sniffer.
    header = text.splitlines()[0] if text.splitlines() else ''
    try:
        delimiter = csv.Sniffer().sniff(header, delimiters=',;\t|').delimiter
    except csv.Error:
        delimiter = ','
    return encoding, delimiter


class CSVStream:
    """Bounded logical CSV records with automatic malformed-record recovery."""
    def __init__(self, path, delimiter, encoding, recovery, log=False):
        self.path, self.delimiter, self.encoding = path, delimiter, encoding
        self.recovery, self.log = recovery, log
        self.rejected = 0
        self.bytes_read = 0
        self.log_path = str(path) + '.rejected.' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.jsonl'

    def rows(self):
        csv.field_size_limit(16 * 1024 * 1024)
        with open(self.path, 'rb') as raw:
            if self.recovery:
                def physical():
                    line_no = 0
                    def read_line():
                        data = raw.readline(16 * 1024 * 1024 + 1)
                        extra = {'utf-16-le': 1, 'utf-32-le': 3}.get(self.encoding, 0)
                        if data.endswith(b'\n') and extra:
                            data += raw.read(extra)
                        return data
                    while True:
                        offset, first_line = raw.tell(), line_no + 1
                        chunks, first_end, total = [], None, 0
                        def record_lines():
                            nonlocal line_no, first_end, total
                            for _ in range(10000):
                                data = read_line()
                                if not data:
                                    return
                                line_no += 1
                                chunks.append(data)
                                if first_end is None:
                                    first_end = raw.tell()
                                total += len(data)
                                if total > 16 * 1024 * 1024:
                                    raise csv.Error('Logical CSV record exceeds 16 MiB')
                                text = data.decode(self.encoding)
                                yield text.lstrip('\ufeff') if offset == 0 and len(chunks) == 1 else text
                            raise csv.Error('Logical CSV record exceeds 10,000 physical lines')
                        try:
                            row = next(csv.reader(record_lines(), delimiter=self.delimiter, strict=True))
                        except StopIteration:
                            break
                        except (csv.Error, UnicodeError) as exc:
                            # Reset after the first line so a broken quote cannot consume the rest of the file.
                            if first_end is None:
                                break
                            raw.seek(first_end)
                            line_no = first_line
                            self.bytes_read = raw.tell()
                            self.reject(offset, first_line, chunks[0], str(exc))
                            continue
                        self.bytes_read = raw.tell()
                        yield row, offset, first_line, b''.join(chunks)
                source = physical()
            else:
                text = io.TextIOWrapper(raw, encoding=self.encoding, newline='')
                reader = csv.reader(text, delimiter=self.delimiter, strict=True)
                def multiline():
                    try:
                        for row in reader:
                            self.bytes_read = raw.tell()
                            yield row, None, reader.line_num, None
                    except (csv.Error, UnicodeError) as exc:
                        raise ValueError(f'Strict CSV parsing stopped near physical line {reader.line_num}: {exc}. Use recovery mode only if records occupy one line.') from exc
                source = multiline()
            header = None
            for row, offset, line_no, data in source:
                if not row:
                    continue
                if header is None:
                    header = deduplicate_columns(row)
                    yield header
                elif len(row) != len(header):
                    if not self.recovery:
                        raise ValueError(f'Wrong field count near line {line_no}: expected {len(header)}, got {len(row)}')
                    self.reject(offset, line_no, data, f'Expected {len(header)} fields, got {len(row)}')
                else:
                    yield row

    def reject(self, offset, line, data, reason):
        self.rejected += 1
        if self.log:
            # Base64 retains the exact original bytes, including invalid encoding.
            with open(self.log_path, 'a', encoding='utf-8') as out:
                out.write(json.dumps({'offset': offset, 'physical_line': line, 'reason': reason,
                                      'raw_base64': base64.b64encode(data).decode('ascii')}) + '\n')
            if self.rejected <= 5:
                print(f'⚠️ Skipping physical line {line}: {reason}')


def bulk_documents(elastic, actions, pipeline=None, retry_transport=False):
    """Retry rejections; ambiguous network errors only for stable-ID index actions."""
    if retry_transport:
        for line in actions[::2]:
            action = json.loads(line)
            if list(action) != ['index'] or not action['index'].get('_id'):
                raise ValueError('Network retries require index actions with explicit document IDs')
    pending = list(range(len(actions) // 2))
    good = [False] * len(pending)
    for attempt in range(BULK_429_RETRIES + 1):
        reason = '429 rejection'
        batch = [line for i in pending for line in actions[2*i:2*i+2]]
        body = ('\n'.join(batch) + '\n').encode('utf-8')
        try:
            result = elastic.request('POST', '_bulk', params={'pipeline': pipeline} if pipeline else {},
                                     data=body, headers={'Content-Type': 'application/x-ndjson'},
                                     timeout=(15, BULK_READ_TIMEOUT))
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            if not retry_transport or attempt == BULK_429_RETRIES:
                raise
            reason = type(exc).__name__
            items = None
        except ElasticHTTPError as exc:
            if exc.status_code == 413 and len(pending) > 1:
                middle = len(pending) // 2
                tqdm.write('⚠️ Endpoint rejected payload size (413); splitting the rejected batch.')
                for subset in (pending[:middle], pending[middle:]):
                    smaller = [line for i in subset for line in actions[2*i:2*i+2]]
                    for i, ok in zip(subset, bulk_documents(elastic, smaller, pipeline, retry_transport=retry_transport)):
                        good[i] = ok
                return good
            retryable = exc.status_code == 429 or (retry_transport and exc.status_code in (502, 503, 504))
            if not retryable or attempt == BULK_429_RETRIES:
                raise
            reason = f'HTTP {exc.status_code}'
            items = None
        else:
            items = result.get('items', [])
            if len(items) != len(pending):
                raise RuntimeError('Unexpected bulk response item count; upload status uncertain')
        if items is not None:
            retry = []
            for i, item in zip(pending, items):
                detail = next(iter(item.values()))
                status = detail.get('status', 500)
                if (status == 429 or (retry_transport and status in (502, 503, 504))) and attempt < BULK_429_RETRIES:
                    retry.append(i)
                    continue
                good[i] = not detail.get('error') and 200 <= status < 300
                if not good[i]:
                    tqdm.write('❌ Indexing error: ' + json.dumps(detail.get('error', detail))[:800])
            pending = retry
        if not pending:
            return good
        if attempt < BULK_429_RETRIES:
            delay = min(BULK_RETRY_MAX_WAIT, BULK_RETRY_WAIT * 2 ** attempt)
            tqdm.write(f'⚠️ {reason}: waiting {delay}s, then retry {attempt + 1}/{BULK_429_RETRIES} for {len(pending):,} documents')
            time.sleep(delay)
    return good


class JSONExpander:
    """Flatten JSON objects/arrays and JSON encoded inside their string values.

    Original CSV values are retained. Derived leaves remain strings or null,
    matching the CSV import policy and avoiding number/string mapping conflicts.
    One name registry is shared by sampling and upload to prevent collisions.
    """
    def __init__(self, columns):
        self.names = {}
        self.used = set(columns) | {'timestamp_field', 'geoip', 'asn'}

    def field_name(self, path, prefixed=False):
        key = (path, prefixed)
        if key not in self.names:
            parts = path if prefixed else path[1:]
            base = '_'.join(sanitize_column(str(part[1])) or 'empty' for part in parts)
            # Plain names intentionally merge equal values across CSV and JSON sources.
            if not prefixed and base not in {'timestamp_field', 'geoip', 'asn'}:
                name = base
            else:
                if not prefixed:
                    base = '_'.join(sanitize_column(str(part[1])) or 'empty' for part in path)
                name, suffix = base, 2
                while name in self.used:
                    name = f'{base}_{suffix}'
                    suffix += 1
            self.names[key] = name
            self.used.add(name)
        return self.names[key]

    @staticmethod
    def container(value):
        if isinstance(value, str) and value.lstrip().startswith(('{', '[')):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, (dict, list)):
                    return parsed
            except (ValueError, RecursionError):
                pass
        return value

    def expand(self, record):
        result = dict(record)
        for column, original in record.items():
            value = self.container(original)
            if not isinstance(value, (dict, list)):
                continue
            stack = [((('column', column),), value)]
            while stack:
                path, value = stack.pop()
                value = self.container(value)
                if isinstance(value, dict) and value:
                    stack.extend((path + (('key', key),), child) for key, child in reversed(list(value.items())))
                elif isinstance(value, list) and value:
                    stack.extend((path + (('index', i),), value[i]) for i in reversed(range(len(value))))
                elif len(path) > 1:
                    if value is None:
                        leaf = None
                    elif isinstance(value, str):
                        leaf = value
                    else:
                        leaf = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
                    name = self.field_name(path)
                    if name not in result or result[name] in (None, ''):
                        result[name] = leaf
                    elif leaf not in (None, '') and result[name] != leaf:
                        # Preserve differing evidence under the source-prefixed name.
                        result[self.field_name(path, prefixed=True)] = leaf
        return result


def upload_csv(elastic, csv_path, index_name, new_index):
    encoding, delimiter = detect_csv_format(csv_path)
    recovery = True
    print(f'CSV format: {encoding}, delimiter {delimiter!r}. Multiline records supported; malformed records logged/skipped.')
    print('Reviewing up to 100,000 valid rows for field types and sparse IP columns...')
    sample_stream = CSVStream(csv_path, delimiter, encoding, recovery)
    sample_rows = sample_stream.rows()
    sample, reviewed = [], 0
    preview_bytes = 0
    try:
        csv_columns = next(sample_rows)
        expander = JSONExpander(csv_columns)
        stats = {col: {'nonempty': 0, 'ip': True, 'integer': True, 'example': ''} for col in csv_columns}
        with tqdm(total=100000, desc='Review sample', unit='row') as bar:
            for row in itertools.islice(sample_rows, 100000):
                reviewed += 1
                # Keep only a small preview; aggregate the larger review in constant memory.
                record = expander.expand(dict(zip(csv_columns, row)))
                row_bytes = sum(len(str(v).encode('utf-8')) for v in record.values())
                if len(sample) < 1000 and (not sample or preview_bytes + row_bytes <= 4 * 1024 * 1024):
                    sample.append(record)
                    preview_bytes += row_bytes
                for col, value in record.items():
                    state = stats.setdefault(col, {'nonempty': 0, 'ip': True, 'integer': True, 'example': ''})
                    if value is None or not str(value).strip():
                        continue
                    state['nonempty'] += 1
                    if not state['example']:
                        state['example'] = str(value)[:500]
                    if state['ip'] and not is_ip(value):
                        state['ip'] = False
                    if state['integer'] and not re.fullmatch(r'[+-]?\d+', str(value)):
                        state['integer'] = False
                bar.update(1)
    finally:
        sample_rows.close()
    if not sample:
        raise ValueError('No valid data rows in sample')
    columns = list(stats)
    # Show useful nonempty examples even when a sparse derived field appeared late.
    preview = {col: state['example'] for col, state in stats.items()}
    df = pd.DataFrame([preview] + sample, columns=columns).fillna('')
    print(f'Expanded {len(columns) - len(csv_columns):,} JSON fields in the review.')
    print(f'✅ Reviewed {reviewed:,} valid rows, {len(columns)} fields; file {os.path.getsize(csv_path)/1024**3:.2f} GiB')
    print('Sample type hints (source fields remain strings):')
    for col, state in stats.items():
        hint = ('empty in sample' if not state['nonempty'] else 'IP candidate' if state['ip'] else
                'integer-like' if state['integer'] else 'string')
        print(f"  {col}: {hint} ({state['nonempty']:,} nonempty values)")
    timestamp = choose_timestamp(df)
    ip_fields = [col for col, state in stats.items() if state['nonempty'] and state['ip']]
    print('Sample-based GeoIP candidates:', ', '.join(ip_fields) or 'none')
    print('Later non-IP values are preserved; GeoIP enrichment ignores invalid values.')
    if input('Continue with these fields? [Y/n]: ').strip().lower() == 'n':
        return False, None
    pipeline_id = f'csv2elk_geoip_{index_name}_{uuid.uuid4().hex[:8]}' if ip_fields else None
    if pipeline_id:
        elastic.request('PUT', f'_ingest/pipeline/{pipeline_id}', json=pipeline_definition(ip_fields))
    prepare_index(elastic, index_name, ip_fields, new_index, len(columns))
    try:
        batch_limit = server_upload_limit(elastic)
    except Exception as exc:
        batch_limit = UPLOAD_BATCH_BYTES
        print(f'⚠️ Server limit unavailable; using {batch_limit / 1024**2:g} MiB: {exc}')
    print(f'📦 Batch payload cap: {batch_limit / 1024**2:g} MiB; up to 50,000 documents per batch.')
    stream = CSVStream(csv_path, delimiter, encoding, recovery, log=True)
    rows = stream.rows()
    indexed = failed = uncertain = 0
    upload_id = uuid.uuid4().hex
    row_number = 0
    time_min = time_max = None
    actions, times, batch_bytes = [], [], 0
    pending = deque()
    worker_state = threading.local()
    worker_clients = []
    upload_started = time.perf_counter()
    bulk_seconds = completed_batches = 0
    def send_batch(batch):
        if not hasattr(worker_state, 'elastic'):
            worker_state.elastic = Elastic(elastic.cfg)
            worker_clients.append(worker_state.elastic)
        started = time.perf_counter()
        results = bulk_documents(worker_state.elastic, batch, pipeline_id, retry_transport=True)
        return results, time.perf_counter() - started
    workers = validated_workers(elastic.cfg.get('UPLOAD_WORKERS'))
    max_pending_batches = workers * 2
    pool = ThreadPoolExecutor(max_workers=workers)
    print(f'📤 Upload workers: {workers}; at most {max_pending_batches} batches queued/in flight')
    def collect():
        nonlocal indexed, failed, uncertain, time_min, time_max, bulk_seconds, completed_batches
        future, batch_times = pending.popleft()
        try:
            results, elapsed = future.result()
        except Exception:
            uncertain += len(batch_times)
            raise
        bulk_seconds += elapsed
        completed_batches += 1
        indexed += sum(results)
        failed += len(results) - sum(results)
        for ok, instant in zip(results, batch_times):
            if ok and instant:
                time_min = instant if time_min is None else min(time_min, instant)
                time_max = instant if time_max is None else max(time_max, instant)
    def update_progress(bar, refresh=False):
        elapsed = max(time.perf_counter() - upload_started, 0.001)
        bar.set_postfix(indexed=indexed, failed=failed, skipped=stream.rejected,
                        docs_s=round(indexed / elapsed), pending=len(pending),
                        bulk_ms=round(1000 * bulk_seconds / max(completed_batches, 1)), refresh=refresh)
    def flush():
        nonlocal actions, times, batch_bytes
        if not actions:
            return
        if len(pending) >= max_pending_batches:
            collect()  # Backpressure bounds both memory and load on Elasticsearch.
        pending.append((pool.submit(send_batch, actions), times))
        actions, times, batch_bytes = [], [], 0
        while pending and pending[0][0].done():
            collect()
    print(f'Network policy: {BULK_READ_TIMEOUT}s read timeout, {BULK_429_RETRIES} retries, waits 15/30/60/60/60s.')
    try:
        next(rows)  # header was already previewed
        with tqdm(total=os.path.getsize(csv_path), unit='B', unit_scale=True, desc='CSV read/upload') as bar:
            for row in rows:
                record = expander.expand(dict(zip(csv_columns, row)))
                parsed = parse_timestamp(record.get(timestamp)) if timestamp else None
                if parsed:
                    record['timestamp_field'] = parsed
                row_number += 1
                bulk_action = json.dumps({'index': {'_index': index_name, '_id': f'{upload_id}_{row_number}'}}, separators=(',', ':'))
                pair = [bulk_action, json.dumps(record, ensure_ascii=False, separators=(',', ':'))]
                size = sum(len(v.encode('utf-8')) + 1 for v in pair)
                if size > batch_limit:
                    raise ValueError(f'One document is {size:,} bytes, exceeding the batch/request budget {batch_limit:,}. No oversized request sent.')
                if actions and (len(times) >= 50000 or batch_bytes + size > batch_limit):
                    flush()
                actions.extend(pair)
                times.append(datetime.fromisoformat(parsed) if parsed else None)
                batch_bytes += size
                bar.update(max(0, stream.bytes_read - bar.n))
                if (indexed + len(times)) % 100 == 0:
                    update_progress(bar)
            flush()
            while pending:
                collect()
                update_progress(bar, refresh=True)
            bar.update(max(0, os.path.getsize(csv_path) - bar.n))
    finally:
        pool.shutdown(wait=True)
        while pending:
            try:
                collect()
            except Exception as exc:
                print(f'⚠️ Batch result uncertain: {exc}')
        for client in worker_clients:
            client.session.close()
        rows.close()
        elapsed = max(time.perf_counter() - upload_started, 0.001)
        print(f'Upload totals: {indexed:,} indexed, {failed:,} failed, {uncertain:,} uncertain, {stream.rejected:,} rejected CSV records')
        print(f'Average upload: {indexed / elapsed:,.0f} docs/s; mean bulk request: {bulk_seconds / max(completed_batches, 1):.2f}s')
        if uncertain:
            print('⚠️ Uncertain batches may already be indexed. Counters reflect confirmed responses only.')
        if stream.rejected:
            print('Rejected evidence log:', stream.log_path)
    for attempt in range(3):
        try:
            elastic.request('POST', f'{index_name}/_refresh', timeout=(15, BULK_READ_TIMEOUT))
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, ElasticHTTPError) as exc:
            if isinstance(exc, ElasticHTTPError) and exc.status_code not in (429, 502, 503, 504):
                print(f'⚠️ Documents uploaded, but refresh failed: {exc}')
                break
            if attempt == 2:
                print(f'⚠️ Documents uploaded, but refresh could not be confirmed: {exc}')
            else:
                delay = BULK_RETRY_WAIT * (attempt + 1)
                print(f'⚠️ Refresh delayed; retrying in {delay}s.')
                time.sleep(delay)
    return indexed > 0, ((time_min.isoformat(), time_max.isoformat()) if time_min else None)


def server_upload_limit(elastic):
    global UPLOAD_BATCH_BYTES
    data = elastic.request('GET', '_nodes/http')
    limits = [int(node['http']['max_content_length_in_bytes'])
              for node in data.get('nodes', {}).values() if node.get('http')]
    if not limits or min(limits) <= 0:
        raise RuntimeError('Elasticsearch did not report an HTTP content limit')
    UPLOAD_BATCH_BYTES = min(limits)
    return UPLOAD_BATCH_BYTES


def powershell(script):
    result = subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command',
                             "$ErrorActionPreference='Stop'; " + script],
                            capture_output=True, text=True, timeout=120,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or 'PowerShell operation failed')
    return result.stdout.strip().lstrip('\ufeff')


def service_config_path(service):
    # Procrun stores the environment/options captured when the service was installed.
    for prefix in (r'SOFTWARE\Apache Software Foundation\Procrun 2.0',
                   r'SOFTWARE\WOW6432Node\Apache Software Foundation\Procrun 2.0'):
        for suffix, values in (('Parameters', ['Environment']), ('Parameters\\Java', ['Options', 'Options9'])):
            try:
                with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, prefix + '\\' + service['Name'] + '\\' + suffix) as key:
                    for value in values:
                        try:
                            options = winreg.QueryValueEx(key, value)[0]
                        except FileNotFoundError:
                            continue
                        for option in options if isinstance(options, list) else [options]:
                            match = re.search(r'(?:-Des.path.conf=|ES_PATH_CONF=)(.*)', str(option))
                            if match:
                                return Path(os.path.expandvars(match[1].strip().strip('"'))) / 'elasticsearch.yml'
            except FileNotFoundError:
                pass
    command = service.get('PathName', '')
    match = re.match(r'\s*"([^"]+\.exe)"|\s*(.*?\.exe)(?:\s|$)', command, re.I)
    if match:
        executable = Path(os.path.expandvars(match[1] or match[2]))
        return executable.parent.parent / 'config' / 'elasticsearch.yml'
    return None


def configured_yaml(original, value):
    """Edit only this scalar; preserve other config and support flat/nested http syntax."""
    text = original.decode('utf-8-sig')
    newline = '\r\n' if '\r\n' in text else '\n'
    flat = re.compile(r'^http\.max_content_length\s*:.*$', re.M)
    matches = list(flat.finditer(text))
    nested = re.search(r'^http:\s*(?:#[^\r\n]*)?\r?$', text, re.M)
    if len(matches) > 1 or (matches and nested):
        raise ValueError('Ambiguous duplicate http configuration; resolve it before using this menu')
    if matches:
        text = flat.sub('http.max_content_length: ' + value, text)
    elif nested:
        start = nested.end()
        rest = text[start:]
        boundary = re.search(r'\n[^\s#][^\n]*', rest)
        end = start + boundary.start() if boundary else len(text)
        block = text[start:end]
        pattern = re.compile(r'^[ \t]+max_content_length\s*:.*$', re.M)
        entries = list(pattern.finditer(block))
        if len(entries) > 1:
            raise ValueError('Duplicate nested max_content_length settings')
        if entries:
            block = pattern.sub('  max_content_length: ' + value, block)
        else:
            block = newline + '  max_content_length: ' + value + block
        text = text[:start] + block + text[end:]
    else:
        text = text.rstrip('\r\n') + newline + 'http.max_content_length: ' + value + newline
    return (codecs.BOM_UTF8 if original.startswith(codecs.BOM_UTF8) else b'') + text.encode('utf-8')


def optimise_elasticsearch(elastic):
    if os.name != 'nt':
        raise RuntimeError('Service configuration requires Windows')
    if not ctypes.windll.shell32.IsUserAnAdmin():
        raise RuntimeError('Run CSV2ELK as Administrator to edit config and restart the service')
    hostname = urlsplit(elastic.url).hostname
    local_names = {socket.gethostname().lower(), socket.getfqdn().lower(), 'localhost', '127.0.0.1', '::1'}
    try:
        local_ips = {info[4][0] for info in socket.getaddrinfo(socket.gethostname(), None)}
        target_ips = {info[4][0] for info in socket.getaddrinfo(hostname, None)}
    except socket.gaierror:
        local_ips, target_ips = set(), set()
    if hostname.lower() not in local_names and not local_ips.intersection(target_ips):
        raise RuntimeError('Configured Elasticsearch is remote. Run this menu on that server; local services were not changed.')
    current = server_upload_limit(elastic)
    print(f'Current server HTTP limit: {current / 1024**2:g} MiB')
    print('http.max_content_length is static: config edit and restart are required; PUT cannot update it.')
    services = json.loads(powershell("@(Get-CimInstance Win32_Service | Where-Object { $_.Name -like '*elasticsearch-service*' -or $_.DisplayName -like '*elasticsearch-service*' } | Select-Object Name,DisplayName,PathName,State) | ConvertTo-Json -Compress" ) or '[]')
    if isinstance(services, dict):
        services = [services]
    if not services:
        raise RuntimeError('No local Windows service containing elasticsearch-service found')
    for i, service in enumerate(services, 1):
        print(f"{i}. {service['Name']} ({service['State']})")
    service = services[0] if len(services) == 1 else None
    if len(services) > 1:
        selected = ask_number('Select service (0 cancels): ', len(services))
        if selected == 0:
            return
        service = services[selected - 1]
    while True:
        value = input('New http.max_content_length [500mb] (Enter to accept): ').strip().lower() or '500mb'
        match = re.fullmatch(r'([1-9]\d*)(kb|mb|gb)', value)
        if match:
            desired = int(match[1]) * {'kb': 1024, 'mb': 1024**2, 'gb': 1024**3}[match[2]]
            if 1024**2 <= desired < 2**31:
                break
        print('❌ Enter a size from 1mb to below 2gb, such as 500mb.')
    suggested = service_config_path(service)
    if suggested and suggested.is_file():
        path = suggested
    else:
        root = Tk()
        root.withdraw()
        try:
            selected = filedialog.askopenfilename(title=f"Select active elasticsearch.yml for {service['Name']}", filetypes=[('Elasticsearch YAML', '*.yml')])
        finally:
            root.destroy()
        if not selected:
            return
        path = Path(selected)
    original = path.read_bytes()
    updated = configured_yaml(original, value)
    backup = path.with_name(path.name + '.csv2elk_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.bak')
    shutil.copy2(path, backup)
    print(f'Updating {path}; backup: {backup}')
    print(f"Restarting {service['Name']}. Limits above 100mb can increase memory pressure; speed must be measured.")
    # Preserve file ACLs by writing the existing file rather than replacing it.
    path.write_bytes(updated)
    service_name = service['Name'].replace("'", "''")
    try:
        powershell(f"Restart-Service -Name '{service_name}' -Force; (Get-Service -Name '{service_name}').WaitForStatus('Running',[TimeSpan]::FromSeconds(90))")
        deadline = time.monotonic() + 180
        verified = False
        while time.monotonic() < deadline:
            try:
                data = elastic.session.get(elastic.url + '/_nodes/http', timeout=5)
                if data.ok:
                    nodes = data.json().get('nodes', {})
                    limits = [int(node['http']['max_content_length_in_bytes']) for node in nodes.values() if node.get('http')]
                    if desired in limits:
                        global UPLOAD_BATCH_BYTES
                        UPLOAD_BATCH_BYTES = min(limits)
                        verified = True
                        break
            except (requests.RequestException, ValueError, KeyError):
                pass
            time.sleep(2)
        if not verified:
            raise RuntimeError('New limit was not verified after restart; check the active config path and Elasticsearch logs')
    except Exception:
        path.write_bytes(original)
        print('Configuration restored from backup. Attempting to restart with the previous settings.')
        try:
            powershell(f"Restart-Service -Name '{service_name}' -Force")
            UPLOAD_BATCH_BYTES = current
        except Exception as rollback_error:
            print(f'❌ Service recovery failed: {rollback_error}. Backup: {backup}')
        raise
    print(f'✅ Verified server limit; uploads now use {UPLOAD_BATCH_BYTES / 1024**2:g} MiB batches.')
    if UPLOAD_BATCH_BYTES < desired:
        print('Other cluster nodes retain a lower limit; upload uses the smallest reported limit.')


def file_dialog(save=False):
    root = Tk()
    root.withdraw()
    try:
        if save:
            return filedialog.asksaveasfilename(defaultextension='.zip', filetypes=[('ZIP archive', '*.zip')])
        return filedialog.askopenfilename(filetypes=[('ZIP archive', '*.zip')])
    finally:
        root.destroy()


def export_index(elastic):
    name = select_index(elastic)
    if not name:
        return
    path = file_dialog(save=True)
    if not path:
        return
    temp = path + '.partial'
    scroll_id = None
    count = 0
    try:
        metadata = elastic.request('GET', name)[name]
        if metadata.get('mappings', {}).get('_source', {}).get('enabled') is False:
            raise ValueError('Cannot export an index with _source disabled')
        result = elastic.request('POST', f'{name}/_search', params={'scroll': '10m'},
                                 json={'size': CHUNK_DOCS, 'sort': ['_doc'], 'query': {'match_all': {}}, 'track_total_hits': True})
        scroll_id = result.get('_scroll_id')
        with zipfile.ZipFile(temp, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=3, allowZip64=True) as archive:
            with archive.open('documents.ndjson', 'w', force_zip64=True) as out, tqdm(desc='Export documents', unit='doc') as bar:
                while True:
                    scroll_id = result.get('_scroll_id', scroll_id)
                    if result.get('timed_out') or result.get('_shards', {}).get('failed', 0):
                        raise RuntimeError('Incomplete search response; export aborted')
                    hits = result.get('hits', {}).get('hits', [])
                    if not hits:
                        break
                    for hit in hits:
                        if '_source' not in hit:
                            raise ValueError('Document has no _source; export aborted')
                        action = {'_id': hit['_id']}
                        if '_routing' in hit:
                            action['routing'] = hit['_routing']
                        out.write((json.dumps({'index': action}) + '\n' + json.dumps(hit['_source'], ensure_ascii=False) + '\n').encode('utf-8'))
                    count += len(hits)
                    bar.update(len(hits))
                    result = elastic.request('POST', '_search/scroll', json={'scroll': '10m', 'scroll_id': scroll_id})
            archive.writestr('metadata.json', json.dumps({'format': 'CSV2ELK-NDJSON', 'version': 1, 'index': name,
                                'document_count': count, 'exported_utc': datetime.now(timezone.utc).isoformat(),
                                'mappings': metadata.get('mappings', {}), 'settings': metadata.get('settings', {})}, indent=2))
        os.replace(temp, path)
        print(f'✅ Exported {count:,} documents to {path}')
    finally:
        if scroll_id:
            try:
                elastic.request('DELETE', '_search/scroll', json={'scroll_id': [scroll_id]})
            except Exception as exc:
                print('⚠️ Scroll cleanup:', exc)


def import_index(elastic):
    path = file_dialog()
    if not path:
        return
    with zipfile.ZipFile(path) as archive:
        meta = json.loads(archive.read('metadata.json'))
        if meta.get('format') != 'CSV2ELK-NDJSON' or meta.get('version') != 1:
            raise ValueError('Unsupported archive format')
        name = sanitize_index_name(input(f"Destination index [{meta['index']}]: ").strip() or meta['index'])
        exists = elastic.index_exists(name)
        print('Existing destination: import uses create-only; duplicate IDs fail and are not overwritten.' if exists else 'Creating a new index with archived mappings and portable settings.')
        if input('Proceed with import? [Y/n]: ').strip().lower() == 'n':
            return
        if not exists:
            settings = dict(meta.get('settings', {}).get('index', {}))
            for key in ('uuid', 'version', 'creation_date', 'provided_name', 'history', 'resize', 'routing', 'blocks',
                        'default_pipeline', 'final_pipeline', 'lifecycle', 'verified_before_close'):
                settings.pop(key, None)
            elastic.request('PUT', name, json={'mappings': meta.get('mappings', {}), 'settings': settings})
        actions, batch_bytes = [], 0
        good = bad = seen = 0
        def flush():
            nonlocal good, bad, actions, batch_bytes
            if actions:
                results = bulk_documents(elastic, actions, '_none')
                good += sum(results)
                bad += len(results) - sum(results)
                actions, batch_bytes = [], 0
        with archive.open('documents.ndjson') as raw, tqdm(total=meta.get('document_count'), desc='Import', unit='doc') as bar:
            while True:
                action_line = raw.readline()
                if not action_line:
                    break
                source_line = raw.readline()
                if not source_line:
                    raise ValueError('Truncated NDJSON action/source pair')
                action = json.loads(action_line)
                if list(action) != ['index']:
                    raise ValueError('Unexpected NDJSON action')
                detail = action['index']
                source = json.loads(source_line)
                if not isinstance(source, dict) or not detail.get('_id'):
                    raise ValueError('Invalid source or document ID')
                dest = {'_index': name, '_id': detail['_id']}
                if 'routing' in detail:
                    dest['routing'] = detail['routing']
                pair = [json.dumps({'create': dest}), json.dumps(source, ensure_ascii=False)]
                size = sum(len(v.encode('utf-8')) + 1 for v in pair)
                if actions and (len(actions) // 2 >= CHUNK_DOCS or batch_bytes + size > 5 * 1024 * 1024):
                    flush()
                actions.extend(pair)
                batch_bytes += size
                seen += 1
                bar.update(1)
            flush()
        elastic.request('POST', f'{name}/_refresh')
        print(f'Import: {good:,} created, {bad:,} failed/conflicting IDs')
        if seen != meta.get('document_count'):
            raise ValueError('Archive document count mismatch; destination may contain a partial import')
        if good:
            offer_data_view(elastic, name)


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


def configure_registry(elastic):
    """Edit saved settings; validate new credentials before saving."""
    cfg = dict(elastic.cfg)
    for key, label in [('ELASTICSEARCH_URL', 'Elasticsearch URL'),
                       ('USERNAME', 'Username'), ('KIBANA_URL', 'Kibana URL')]:
        if key.endswith('_URL'):
            cfg[key] = prompt_url(label, cfg.get(key) or suggested_urls()[key])
        else:
            cfg[key] = input(f"{label} [{cfg.get(key, '')}] (Enter to retain): ").strip() or cfg.get(key, '')
    cfg['PASSWORD'] = masked_password('Password (Enter to retain): ') or cfg['PASSWORD']
    cfg['UPLOAD_WORKERS'] = prompt_workers(cfg.get('UPLOAD_WORKERS'))
    candidate = Elastic(cfg)
    try:
        candidate.request('GET', '_cluster/health')
        save_config(cfg)
    except Exception:
        candidate.session.close()
        raise
    elastic.session.close()
    print('✅ Configuration saved; new connection active.')
    return candidate


def offer_data_view(elastic, index_name, csv_time_range=None):
    """Resolve a matching time-based view, or offer creation; open Discover."""
    print('ℹ️ A Data View is required to see uploaded data in Discover.')
    kibana_url = elastic.cfg.get('KIBANA_URL', '').strip().rstrip('/')
    if not kibana_url.startswith(('http://', 'https://')):
        print('⚠️ Configure KIBANA_URL using menu option 4, then open Discover manually.')
        return None
    try:
        data = kibana_request(elastic, kibana_url, 'GET', 'data_views')
        views = data.get('data_view', [])
        matches = [v for v in views if view_matches_index(v.get('title', ''), index_name)]
        # Full metadata supplies the time field; list responses may omit it.
        candidates = []
        for view in matches:
            full = kibana_request(elastic, kibana_url, 'GET',
                                  'data_views/data_view/' + quote(view['id'], safe=''))['data_view']
            candidates.append(full)
        candidates.sort(key=lambda v: (v.get('timeFieldName') != 'timestamp_field',
                                        v.get('title') != index_name, v.get('title', '')))
        view = candidates[0] if candidates else None
        if view and csv_time_range and view.get('timeFieldName') != 'timestamp_field':
            print('⚠️ Matching view uses another time field; CSV time range needs timestamp_field.')
            view = None
        if view:
            print(f"✅ Existing Data View found: {view.get('name') or view['title']} ({view['id']}).")
        else:
            print(f'No suitable Data View found for {index_name}.')
            suggested_pattern = index_name[:3].lower() + '*'
            print(f'Suggested pattern: {suggested_pattern} — matches indices beginning with {index_name[:3].lower()!r}.')
            if input('Add a Data View? [Y/n]: ').strip().lower() == 'n':
                return None
            pattern = input(f'Index pattern [{suggested_pattern}]: ').strip() or suggested_pattern
            if not view_matches_index(pattern, index_name):
                print('❌ Pattern does not match the uploaded index.')
                return None
            body = {'data_view': {'title': pattern, 'name': index_name}}
            if csv_time_range:
                body['data_view']['timeFieldName'] = 'timestamp_field'
            view = kibana_request(elastic, kibana_url, 'POST', 'data_views/data_view', json=body)['data_view']
            print(f"✅ Created Data View: {view['id']}")
        url = discover_url(kibana_url, view['id'], index_name, csv_time_range)
        print('🔎 Selecting this Data View in Discover: ' + url)
        if csv_time_range:
            print(f'📅 CSV time range: {csv_time_range[0]} to {csv_time_range[1]}')
        else:
            print('⚠️ No valid CSV timestamps; Discover uses its default time range.')
        if not webbrowser.open_new(url):
            print('⚠️ Browser could not open automatically. Use the URL above.')
        return view['id']
    except Exception as exc:
        print(f'⚠️ Discover setup failed: {exc}. Uploaded data remains in Elasticsearch.')
        return None


def rison_string(value):
    return "'" + str(value).replace('!', '!!').replace("'", "!'") + "'"


def discover_url(kibana_url, view_id, index_name, csv_time_range=None):
    # Restrict a broad data view to the index uploaded in this operation.
    query = '_index:' + json.dumps(index_name)
    state = ("(columns:!(),dataSource:(dataViewId:" + rison_string(view_id) +
             ",type:dataView),index:" + rison_string(view_id) +
             ",filters:!(),query:(language:kuery,query:" + rison_string(query) + "))")
    url = kibana_url.rstrip('/') + '/app/discover#/?_a=' + quote(state, safe='')
    if csv_time_range:
        start, end = csv_time_range
        global_state = ("(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:" +
                        rison_string(start) + ",to:" + rison_string(end) + '))')
        url += '&_g=' + quote(global_state, safe='')
    return url


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
    print('Developed by Jacob Wilson - Version 1.0')
    print('github.com/dfirvault/CSV2ELK')
    print('dfirvault@gmail.com')
    print('')
    elastic = connect_elasticsearch()
    while True:
        print('\n=== Elasticsearch CSV Uploader ===')
        print('1. Create new index and upload data')
        print('2. Upload data to existing index')
        print('3. Manage index (delete)')
        print('4. Configure registry settings (URLs, username, password)')
        print('5. Export index → NDJSON ZIP')
        print('6. Import NDJSON ZIP → index')
        print('7. Optimise local Elasticsearch service / upload request limit')
        print('0. Exit')
        choice = input('Enter choice: ').strip()
        if choice == '0':
            print('👋 Goodbye!')
            break
        if choice == '7':
            try:
                optimise_elasticsearch(elastic)
            except Exception as exc:
                print(f'❌ Service configuration stopped: {exc}')
            continue
        if choice in ('5', '6'):
            try:
                (export_index if choice == '5' else import_index)(elastic)
            except Exception as exc:
                print(f'❌ Transfer stopped: {exc}. Import may be partial; export .partial is not a completed backup.')
            continue
        if choice == '4':
            try:
                elastic = configure_registry(elastic)
            except Exception as exc:
                print(f'❌ Configuration unchanged: {exc}')
            continue
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
            uploaded, csv_time_range = upload_csv(elastic, path, name, new_index)
            if uploaded:
                offer_data_view(elastic, name, csv_time_range)
        except Exception as exc:
            print(f'❌ UPLOAD STOPPED: {exc}')


if __name__ == '__main__':
    main()
