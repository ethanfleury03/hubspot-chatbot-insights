#!/usr/bin/env python3
"""
HubSpot Forms Discovery and Submission Counting Tool.

Required private app scope: forms

This script discovers all HubSpot forms (archived and non-archived) and counts
submissions for each form. It produces a summary report with statistics.

Usage examples:
  python get_forms.py --init
  python get_forms.py --init --max-forms 5
  python get_forms.py --init --out out/forms_summary.json
"""

import argparse
import csv
import hashlib
import html
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback for Python < 3.9
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None

BASE_URL = "https://api.hubapi.com"

DEFAULT_TIMEOUT = 30


def load_dotenv(path: str = '.env') -> Dict[str, str]:
    """Load .env file and return dict of key=value pairs."""
    env_vars = {}
    if not os.path.exists(path):
        return env_vars
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                # Handle KEY=value format
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    env_vars[key] = value
    except Exception as e:
        print(f"Warning: Failed to load .env file: {e}", file=sys.stderr)
    
    return env_vars


def get_old_access_token() -> str:
    """
    Get OLD_ACCESS_TOKEN from OS environment (priority) or .env file.
    
    Exits with error if token not found.
    """
    # Check OS environment first
    token = os.environ.get('OLD_ACCESS_TOKEN')
    if token:
        return token
    
    # Check .env file
    env_vars = load_dotenv()
    token = env_vars.get('OLD_ACCESS_TOKEN')
    if token:
        return token
    
    print("Error: OLD_ACCESS_TOKEN not found in environment or .env file.", file=sys.stderr)
    sys.exit(2)


def get_access_token() -> str:
    """
    Get ACCESS_TOKEN (NEW portal) from OS environment (priority) or .env file.
    
    Exits with error if token not found.
    """
    # Check OS environment first
    token = os.environ.get('ACCESS_TOKEN')
    if token:
        return token
    
    # Check .env file
    env_vars = load_dotenv()
    token = env_vars.get('ACCESS_TOKEN')
    if token:
        return token
    
    print("Error: ACCESS_TOKEN not found in environment or .env file.", file=sys.stderr)
    sys.exit(2)


def hubspot_get(url: str, params: Optional[Dict[str, Any]] = None,
                headers: Optional[Dict[str, str]] = None,
                token: str = None, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Make HTTP GET request to HubSpot API using urllib.
    
    Adds Authorization Bearer token.
    Retries on 429/5xx with exponential backoff (1s, 2s, 4s, 8s; max 5 tries).
    On 403/401: prints clear message about forms scope and raises.
    
    Returns JSON dict.
    """
    if params is None:
        params = {}
    if headers is None:
        headers = {}
    
    # Build URL - params must be raw strings, not pre-encoded
    parsed = urlparse(url)
    
    # Start with new params dict (all values are raw strings)
    query_params = {}
    
    # If path has existing query string, parse it carefully
    if parsed.query:
        # Parse existing query string and unquote values to get raw strings
        for pair in parsed.query.split('&'):
            if pair and '=' in pair:
                k, v = pair.split('=', 1)
                # Unquote to get raw value (in case it was encoded)
                query_params[unquote(k)] = unquote(v)
    
    # Add new params (all should be raw strings)
    query_params.update(params)
    
    # Rebuild URL - urlencode() will encode exactly once
    url_final = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        urlencode(query_params),  # Single encoding pass
        parsed.fragment
    ))
    
    # Create request
    request = Request(url_final)
    request.add_header('Authorization', f'Bearer {token}')
    request.add_header('Content-Type', 'application/json')
    
    # Add custom headers
    for k, v in headers.items():
        request.add_header(k, v)
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = urlopen(request, timeout=timeout)
            status = response.getcode()
            
            body_bytes = response.read()
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            json_data = {}
            if body_str:
                try:
                    json_data = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            return json_data
            
        except HTTPError as e:
            status = e.code
            body_bytes = e.read() if hasattr(e, 'read') else b''
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            json_data = {}
            if body_str:
                try:
                    json_data = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            if status == 401:
                print("Forbidden/Unauthorized: verify private app has `forms` scope and token is for the correct portal.", file=sys.stderr)
                sys.exit(2)
            elif status == 403:
                print("Forbidden/Unauthorized: verify private app has `forms` scope and token is for the correct portal.", file=sys.stderr)
                sys.exit(2)
            elif status == 429:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"Rate limited (429). Waiting {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print("Error: Rate limit exceeded after retries.", file=sys.stderr)
                    sys.exit(2)
            elif 500 <= status < 600:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"Server error ({status}). Retrying in {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"Error: Server error {status} after retries.", file=sys.stderr)
                    sys.exit(2)
            else:
                error_msg = f"Error: HTTP {status}"
                if json_data and 'message' in json_data:
                    error_msg += f": {json_data['message']}"
                print(error_msg, file=sys.stderr)
                sys.exit(2)
        except URLError as e:
            print(f"Error: Network error: {e.reason}", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"Error: Unexpected error: {e}", file=sys.stderr)
            sys.exit(2)
    
    print("Error: Max retries exceeded.", file=sys.stderr)
    sys.exit(2)


def hubspot_post(url: str, data: Dict[str, Any],
                 headers: Optional[Dict[str, str]] = None,
                 token: str = None, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Make HTTP POST request to HubSpot API using urllib.
    
    Adds Authorization Bearer token.
    Retries on 429/5xx with exponential backoff (1s, 2s, 4s, 8s; max 5 tries).
    On 403/401: prints clear message and raises.
    
    Returns JSON dict.
    """
    if headers is None:
        headers = {}
    
    # Create request
    request = Request(url, data=json.dumps(data).encode('utf-8'))
    request.add_header('Authorization', f'Bearer {token}')
    request.add_header('Content-Type', 'application/json')
    
    # Add custom headers
    for k, v in headers.items():
        request.add_header(k, v)
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = urlopen(request, timeout=timeout)
            status = response.getcode()
            
            body_bytes = response.read()
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            json_data = {}
            if body_str:
                try:
                    json_data = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            return json_data
            
        except HTTPError as e:
            status = e.code
            body_bytes = e.read() if hasattr(e, 'read') else b''
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            json_data = {}
            if body_str:
                try:
                    json_data = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            if status == 401:
                print("Forbidden/Unauthorized: verify private app has required scopes and token is for the correct portal.", file=sys.stderr)
                sys.exit(2)
            elif status == 403:
                print("Forbidden/Unauthorized: verify private app has required scopes and token is for the correct portal.", file=sys.stderr)
                sys.exit(2)
            elif status == 429:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"Rate limited (429). Waiting {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print("Error: Rate limit exceeded after retries.", file=sys.stderr)
                    sys.exit(2)
            elif 500 <= status < 600:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"Server error ({status}). Retrying in {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"Error: Server error {status} after retries.", file=sys.stderr)
                    sys.exit(2)
            else:
                error_msg = f"Error: HTTP {status}"
                if json_data and 'message' in json_data:
                    error_msg += f": {json_data['message']}"
                print(error_msg, file=sys.stderr)
                sys.exit(2)
        except URLError as e:
            print(f"Error: Network error: {e.reason}", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"Error: Unexpected error: {e}", file=sys.stderr)
            sys.exit(2)
    
    print("Error: Max retries exceeded.", file=sys.stderr)
    sys.exit(2)


def list_forms(archived: bool, token: str, timeout: int = DEFAULT_TIMEOUT) -> List[Dict[str, Any]]:
    """
    List all HubSpot forms (archived or non-archived).
    
    Uses Marketing forms v3 API with pagination.
    Returns list of form records.
    """
    all_forms = []
    after = None
    
    while True:
        params = {
            'limit': 100,
            'formTypes': 'all',
            'archived': 'true' if archived else 'false'
        }
        
        if after is not None:
            params['after'] = after
        
        url = f"{BASE_URL}/marketing/v3/forms/"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        results = response.get('results', [])
        all_forms.extend(results)
        
        # Check for next page
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        next_after_raw = unquote(next_after_encoded) if next_after_encoded else None
        
        if not next_after_raw or len(results) == 0:
            break
        
        after = next_after_raw
    
    return all_forms


def count_form_submissions(form_guid: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[int, Counter]:
    """
    Count submissions for a form via Legacy form-integrations v1 API.
    
    Returns (submission_count, Counter(pageUrl)).
    Missing pageUrl is counted as "(unknown)".
    """
    submission_count = 0
    page_url_counter = Counter()
    after = None
    
    while True:
        params = {
            'limit': 50
        }
        
        if after is not None:
            params['after'] = after
        
        url = f"{BASE_URL}/form-integrations/v1/submissions/forms/{form_guid}"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        results = response.get('results', [])
        
        for submission in results:
            submission_count += 1
            page_url = submission.get('pageUrl')
            if not page_url:
                page_url = "(unknown)"
            page_url_counter[page_url] += 1
        
        # Check for next page
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        next_after_raw = unquote(next_after_encoded) if next_after_encoded else None
        
        if not next_after_raw or len(results) == 0:
            break
        
        after = next_after_raw
    
    return (submission_count, page_url_counter)


def iter_form_submissions_old(form_guid: str, token: str, limit: int = 50, timeout: int = DEFAULT_TIMEOUT):
    """
    Iterator that yields submission dicts from OLD portal, handling both pagination styles.
    
    Handles:
    - New-style: paging.next.after cursor
    - Legacy: hasMore + offset
    
    Yields: submission dicts
    Returns: (pages_fetched, submissions_yielded) when exhausted
    """
    pages_fetched = 0
    submissions_yielded = 0
    after = None
    offset = None
    seen_cursors = set()
    use_legacy = None  # None = auto-detect, True = legacy, False = new-style
    
    while True:
        params = {'limit': limit}
        
        # Determine pagination method
        if use_legacy is None:
            # First page - no cursor
            pass
        elif use_legacy:
            # Legacy pagination
            if offset is not None:
                params['offset'] = offset
        else:
            # New-style pagination
            if after is not None:
                params['after'] = after
        
        url = f"{BASE_URL}/form-integrations/v1/submissions/forms/{form_guid}"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        pages_fetched += 1
        
        results = response.get('results', [])
        if not results:
            break
        
        # Yield all results
        for submission in results:
            submissions_yielded += 1
            yield submission
        
        # Detect pagination style on first page
        if use_legacy is None:
            paging = response.get('paging', {})
            has_more = response.get('hasMore', False)
            
            if paging and paging.get('next', {}).get('after'):
                use_legacy = False
            elif has_more:
                use_legacy = True
            else:
                # No pagination info, assume done
                break
        
        # Handle new-style pagination
        if use_legacy is False:
            paging = response.get('paging', {})
            next_page = paging.get('next', {})
            next_after = next_page.get('after')
            
            if not next_after:
                break
            
            # Check for infinite loop
            cursor_key = ('after', next_after)
            if cursor_key in seen_cursors:
                print(f"Warning: Pagination appears stuck at after={next_after}. Breaking loop.", file=sys.stderr)
                break
            seen_cursors.add(cursor_key)
            
            after = next_after
        
        # Handle legacy pagination
        elif use_legacy is True:
            has_more = response.get('hasMore', False)
            if not has_more:
                break
            
            # Advance offset
            current_offset = response.get('offset', 0)
            new_offset = current_offset + len(results)
            
            # Check for infinite loop
            cursor_key = ('offset', new_offset)
            if cursor_key in seen_cursors:
                print(f"Warning: Pagination appears stuck at offset={new_offset}. Breaking loop.", file=sys.stderr)
                break
            seen_cursors.add(cursor_key)
            
            offset = new_offset
    
    return (pages_fetched, submissions_yielded)


def get_form_submissions(form_guid: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get all submissions for a form (convenience wrapper around iter_form_submissions_old).
    
    Returns (list of submission records, number of pages fetched).
    """
    all_submissions = []
    pages_fetched = 0
    
    for submission in iter_form_submissions_old(form_guid, token, limit=50, timeout=timeout):
        all_submissions.append(submission)
        # Track pages by checking generator return (we'll need to handle this differently)
    
    # Re-fetch to get page count (inefficient but maintains API)
    # Actually, let's just count as we go
    pages_fetched = 0
    all_submissions = []
    seen_pages = set()
    
    after = None
    offset = None
    use_legacy = None
    
    while True:
        params = {'limit': 50}
        
        if use_legacy is None:
            pass
        elif use_legacy:
            if offset is not None:
                params['offset'] = offset
        else:
            if after is not None:
                params['after'] = after
        
        url = f"{BASE_URL}/form-integrations/v1/submissions/forms/{form_guid}"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        pages_fetched += 1
        
        results = response.get('results', [])
        if not results:
            break
        
        all_submissions.extend(results)
        
        if use_legacy is None:
            paging = response.get('paging', {})
            has_more = response.get('hasMore', False)
            
            if paging and paging.get('next', {}).get('after'):
                use_legacy = False
            elif has_more:
                use_legacy = True
            else:
                break
        
        if use_legacy is False:
            paging = response.get('paging', {})
            next_page = paging.get('next', {})
            next_after = next_page.get('after')
            
            if not next_after:
                break
            
            if after == next_after:
                print(f"Warning: Pagination appears stuck at after={next_after}. Breaking loop.", file=sys.stderr)
                break
            
            after = next_after
        
        elif use_legacy is True:
            has_more = response.get('hasMore', False)
            if not has_more:
                break
            
            current_offset = response.get('offset', 0)
            new_offset = current_offset + len(results)
            
            if offset == new_offset:
                print(f"Warning: Pagination appears stuck at offset={new_offset}. Breaking loop.", file=sys.stderr)
                break
            
            offset = new_offset
    
    return (all_submissions, pages_fetched)


def flatten_strings(obj: Any, max_strings: int = 200) -> List[str]:
    """
    Recursively walk dict/list and collect all string values.
    
    Returns list of strings (limited to max_strings).
    """
    strings = []
    
    def _flatten(item: Any, depth: int = 0):
        if depth > 10:  # Prevent infinite recursion
            return
        if len(strings) >= max_strings:
            return
        
        if isinstance(item, str):
            if item.strip():
                strings.append(item.strip())
        elif isinstance(item, dict):
            for v in item.values():
                _flatten(v, depth + 1)
        elif isinstance(item, list):
            for v in item:
                _flatten(v, depth + 1)
        elif isinstance(item, (int, float)):
            strings.append(str(item))
    
    _flatten(obj)
    return strings


def html_escape(s: Optional[str]) -> str:
    """HTML-escape a string. Returns empty string for None."""
    if s is None:
        return ''
    return html.escape(str(s), quote=True)


def normalize_email(s: str) -> Optional[str]:
    """Normalize email: strip and lowercase. Validate format."""
    if not s:
        return None
    s = s.strip().lower()
    # Must contain @ and dot in domain
    if '@' in s and '.' in s.split('@')[1]:
        return s
    return None


def normalize_phone(s: str) -> Optional[str]:
    """Normalize phone: keep leading + if present, remove spaces/dashes/parentheses."""
    if not s:
        return None
    # Remove spaces, dashes, parentheses
    cleaned = re.sub(r'[\s\-\(\)]', '', s)
    # Keep leading +
    if cleaned.startswith('+'):
        return cleaned
    else:
        # Extract digits only
        return re.sub(r'\D', '', cleaned)


def digits_only(s: str) -> Optional[str]:
    """Remove all non-digits from string."""
    if not s:
        return None
    digits = re.sub(r'\D', '', s)
    return digits if digits else None


def values_list_to_map(values_list: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Convert values list to field map.
    
    For each item in list:
    - name = item.get("name") or item.get("fieldName") or item.get("key")
    - value = item.get("value")
    - If name is str and value is not None: map[name.lower().strip()] = str(value).strip()
    """
    field_map = {}
    
    for item in values_list:
        if not isinstance(item, dict):
            continue
        
        name = item.get("name") or item.get("fieldName") or item.get("key")
        value = item.get("value")
        
        if name and value is not None:
            name_str = str(name).lower().strip()
            value_str = str(value).strip()
            if name_str and value_str:
                field_map[name_str] = value_str
    
    return field_map


def extract_from_values(values: Any) -> Dict[str, str]:
    """
    Extract field map from values (dict or list).
    
    Returns dict: {field_name_lower: value_str}
    """
    field_map = {}
    
    if isinstance(values, list):
        # List of objects: [{"name": "...", "value": "..."}]
        field_map = values_list_to_map(values)
    elif isinstance(values, dict):
        # Direct dict: {fieldName: value}
        for key, val in values.items():
            if isinstance(val, str) and val.strip():
                field_map[key.lower()] = val.strip()
            elif isinstance(val, (int, float)):
                field_map[key.lower()] = str(val).strip()
            elif isinstance(val, list) and len(val) > 0:
                # Try converting list to map
                nested_map = values_list_to_map(val)
                field_map.update(nested_map)
    
    return field_map


def extract_identifiers(submission: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Extract email and phone from submission payload with robust extraction.
    
    Based on actual data structure: values is list[dict] with "name" and "value" keys.
    
    Returns dict with: email, phone, phone_digits, first_name, last_name
    """
    result = {
        'email': None,
        'phone': None,
        'phone_digits': None,
        'first_name': None,
        'last_name': None
    }
    
    # A) Build field_map from submission["values"]
    values = submission.get('values')
    if not values:
        return result
    
    # Convert values list to field map
    field_map = extract_from_values(values)
    
    # 1) Email extraction
    # First check direct keys
    email_candidates_exact = ["email", "hs_email", "email_address", "e-mail", "your-email"]
    for key in email_candidates_exact:
        if key in field_map:
            email_val = normalize_email(field_map[key])
            if email_val:
                result['email'] = email_val
                break
    
    # Then any key containing "email"
    if not result['email']:
        for key in field_map.keys():
            if "email" in key:
                email_val = normalize_email(field_map[key])
                if email_val:
                    result['email'] = email_val
                    break
    
    # Regex fallback: scan all string values in field_map
    if not result['email']:
        email_pattern = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
        for val in field_map.values():
            match = email_pattern.search(val)
            if match:
                email_val = normalize_email(match.group(0))
                if email_val:
                    result['email'] = email_val
                    break
    
    # 2) Phone extraction
    # Check exact keys
    phone_candidates_exact = ["phone", "hs_phone", "mobile", "mobilephone", "tel", "telephone"]
    for key in phone_candidates_exact:
        if key in field_map:
            phone_val = normalize_phone(field_map[key])
            if phone_val:
                phone_digits_val = digits_only(phone_val)
                if phone_digits_val and len(phone_digits_val) >= 10:
                    result['phone'] = phone_val
                    result['phone_digits'] = phone_digits_val
                    break
    
    # Then any key containing "phone" or "mobile" or "tel"
    if not result['phone']:
        for key in field_map.keys():
            if "phone" in key or "mobile" in key or "tel" in key:
                phone_val = normalize_phone(field_map[key])
                if phone_val:
                    phone_digits_val = digits_only(phone_val)
                    if phone_digits_val and len(phone_digits_val) >= 10:
                        result['phone'] = phone_val
                        result['phone_digits'] = phone_digits_val
                        break
    
    # Regex/heuristic fallback: scan for phone-like numbers (>=10 digits)
    if not result['phone']:
        best_phone = None
        best_digits = None
        best_length = 0
        
        for val in field_map.values():
            # Extract digits
            digits = digits_only(val)
            if digits and len(digits) >= 10 and len(digits) <= 15:
                # Check if original had leading +
                has_plus = val.strip().startswith('+')
                
                if len(digits) > best_length:
                    best_length = len(digits)
                    if has_plus:
                        best_phone = '+' + digits
                    else:
                        best_phone = digits
                    best_digits = digits
        
        if best_phone:
            result['phone'] = best_phone
            result['phone_digits'] = best_digits
    
    # 3) Name fields
    first_name_keys = ["firstname", "first_name", "first-name", "fname", "your-name", "name"]
    for key in first_name_keys:
        if key in field_map:
            name_value = field_map[key]
            if key == "name" and " " in name_value:
                # Split "name" field if it contains spaces (best-effort)
                parts = name_value.split(None, 1)
                if len(parts) >= 1:
                    result['first_name'] = parts[0]
                if len(parts) >= 2:
                    result['last_name'] = parts[1]
            else:
                result['first_name'] = name_value
            break
    
    # Only set last_name if not already set from "name" split
    if not result['last_name']:
        last_name_keys = ["lastname", "last_name", "last-name", "lname"]
        for key in last_name_keys:
            if key in field_map:
                result['last_name'] = field_map[key]
                break
    
    return result


def search_contact_new_portal_by_email(email: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[bool, Optional[str]]:
    """
    Search for contact in NEW portal by email.
    
    Returns (found_bool, contactId_or_None).
    """
    url = f"{BASE_URL}/crm/v3/objects/contacts/search"
    
    payload = {
        'filterGroups': [
            {
                'filters': [
                    {
                        'propertyName': 'email',
                        'operator': 'EQ',
                        'value': email
                    }
                ]
            }
        ],
        'properties': ['email', 'phone'],
        'limit': 1
    }
    
    try:
        response = hubspot_post(url, payload, token=token, timeout=timeout)
        total = response.get('total', 0)
        results = response.get('results', [])
        
        # Consider found if results non-empty OR total > 0
        if (results and len(results) > 0) or total > 0:
            if results and len(results) > 0:
                contact_id = results[0].get('id')
                return (True, contact_id)
            # If total > 0 but no results in response, still consider found
            return (True, None)
        
        return (False, None)
    except Exception as e:
        print(f"Warning: Error searching contact by email {email}: {e}", file=sys.stderr)
        return (False, None)


def search_contact_new_portal_by_phone(phone: str, phone_digits: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[bool, Optional[str]]:
    """
    Search for contact in NEW portal by phone.
    
    Tries phone EQ first, then phone CONTAINS_TOKEN with digits_only.
    
    Returns (found_bool, contactId_or_None).
    """
    url = f"{BASE_URL}/crm/v3/objects/contacts/search"
    
    # First try: exact phone match
    payload_eq = {
        'filterGroups': [
            {
                'filters': [
                    {
                        'propertyName': 'phone',
                        'operator': 'EQ',
                        'value': phone
                    }
                ]
            }
        ],
        'properties': ['email', 'phone'],
        'limit': 1
    }
    
    try:
        response = hubspot_post(url, payload_eq, token=token, timeout=timeout)
        total = response.get('total', 0)
        results = response.get('results', [])
        
        # Consider found if results non-empty OR total > 0
        if (results and len(results) > 0) or total > 0:
            if results and len(results) > 0:
                contact_id = results[0].get('id')
                return (True, contact_id)
            return (True, None)
    except Exception as e:
        print(f"Warning: Error searching contact by phone EQ {phone}: {e}", file=sys.stderr)
    
    # Second try: CONTAINS_TOKEN with digits_only (if available and different)
    if phone_digits and phone_digits != phone and len(phone_digits) >= 7:
        payload_contains = {
            'filterGroups': [
                {
                    'filters': [
                        {
                            'propertyName': 'phone',
                            'operator': 'CONTAINS_TOKEN',
                            'value': phone_digits
                        }
                    ]
                }
            ],
            'properties': ['email', 'phone'],
            'limit': 1
        }
        
        try:
            response = hubspot_post(url, payload_contains, token=token, timeout=timeout)
            total = response.get('total', 0)
            results = response.get('results', [])
            
            # Consider found if results non-empty OR total > 0
            if (results and len(results) > 0) or total > 0:
                if results and len(results) > 0:
                    contact_id = results[0].get('id')
                    return (True, contact_id)
                return (True, None)
        except Exception as e:
            print(f"Warning: Error searching contact by phone CONTAINS_TOKEN {phone_digits}: {e}", file=sys.stderr)
    
    return (False, None)


def debug_submissions(form_guid: Optional[str], debug_max: int, token: str, timeout: int, all_forms: List[Dict[str, Any]]):
    """
    Debug mode: dump raw submission payloads to disk for inspection.
    
    Uses OLD portal token only.
    """
    from pathlib import Path
    
    forms_to_debug = all_forms
    if form_guid:
        forms_to_debug = [f for f in all_forms if f.get('id') == form_guid]
        if not forms_to_debug:
            print(f"Error: Form GUID {form_guid} not found.", file=sys.stderr)
            return
    
    print(f"\nDEBUG MODE: Processing {len(forms_to_debug)} form(s)", file=sys.stderr)
    
    for form in forms_to_debug:
        form_id = form.get('id', '')
        form_name = form.get('name', 'Unknown')
        
        print(f"\nDebugging form: {form_name} ({form_id})", file=sys.stderr)
        
        # Create output directory
        debug_dir = Path(f"./out/forms_debug/{form_id}")
        debug_dir.mkdir(parents=True, exist_ok=True)
        
        # Fetch submissions with pagination (using iterator)
        pages_fetched = 0
        submissions_processed = 0
        submissions_to_dump = []
        first_page_response = None
        
        # Use iterator to get submissions
        after = None
        offset = None
        use_legacy = None
        seen_cursors = set()
        
        while True:
            params = {'limit': 50}
            
            if use_legacy is None:
                pass  # First page
            elif use_legacy:
                if offset is not None:
                    params['offset'] = offset
            else:
                if after is not None:
                    params['after'] = after
            
            url = f"{BASE_URL}/form-integrations/v1/submissions/forms/{form_id}"
            response = hubspot_get(url, params=params, token=token, timeout=timeout)
            
            pages_fetched += 1
            
            # Save first page envelope
            if pages_fetched == 1:
                first_page_response = response
                envelope_path = debug_dir / "page_0_envelope.json"
                with open(envelope_path, 'w', encoding='utf-8') as f:
                    json.dump(response, f, indent=2, ensure_ascii=False)
                print(f"  Saved page envelope to: {envelope_path}", file=sys.stderr)
            
            results = response.get('results', [])
            if not results:
                break
            
            # Collect submissions to dump
            for submission in results:
                submissions_processed += 1
                if len(submissions_to_dump) < debug_max:
                    submissions_to_dump.append((submissions_processed - 1, submission))
            
            # Detect pagination style on first page
            if use_legacy is None:
                paging = response.get('paging', {})
                has_more = response.get('hasMore', False)
                
                if paging and paging.get('next', {}).get('after'):
                    use_legacy = False
                elif has_more:
                    use_legacy = True
                else:
                    break
            
            # Handle new-style pagination
            if use_legacy is False:
                paging = response.get('paging', {})
                next_page = paging.get('next', {})
                next_after = next_page.get('after')
                
                if not next_after:
                    break
                
                cursor_key = ('after', next_after)
                if cursor_key in seen_cursors:
                    print(f"  Warning: Pagination appears stuck at after={next_after}. Breaking loop.", file=sys.stderr)
                    break
                seen_cursors.add(cursor_key)
                
                after = next_after
            
            # Handle legacy pagination
            elif use_legacy is True:
                has_more = response.get('hasMore', False)
                if not has_more:
                    break
                
                current_offset = response.get('offset', 0)
                new_offset = current_offset + len(results)
                
                cursor_key = ('offset', new_offset)
                if cursor_key in seen_cursors:
                    print(f"  Warning: Pagination appears stuck at offset={new_offset}. Breaking loop.", file=sys.stderr)
                    break
                seen_cursors.add(cursor_key)
                
                offset = new_offset
        
        # Print diagnostics
        print(f"  Pages fetched: {pages_fetched}", file=sys.stderr)
        print(f"  Submissions processed: {submissions_processed}", file=sys.stderr)
        
        if first_page_response:
            envelope_keys = list(first_page_response.keys())
            print(f"  Envelope top-level keys: {envelope_keys}", file=sys.stderr)
        
        # Dump submissions
        for idx, submission in submissions_to_dump:
            submission_path = debug_dir / f"submission_{idx}.json"
            with open(submission_path, 'w', encoding='utf-8') as f:
                json.dump(submission, f, indent=2, ensure_ascii=False)
            
            # Print submission diagnostics
            has_page_url = 'pageUrl' in submission
            has_submitted_at = 'submittedAt' in submission or 'createdAt' in submission
            values = submission.get('values')
            values_type = type(values).__name__ if values is not None else 'None'
            
            print(f"    Submission {idx}: pageUrl={has_page_url}, submittedAt={has_submitted_at}, values={values_type}", file=sys.stderr)
            
            if values:
                if isinstance(values, dict):
                    sample_keys = list(values.keys())[:10]
                    print(f"      Sample field names (dict): {sample_keys}", file=sys.stderr)
                elif isinstance(values, list) and len(values) > 0:
                    first_item = values[0]
                    if isinstance(first_item, dict):
                        sample_keys = list(first_item.keys())[:10]
                        print(f"      Sample field names (list[dict]): {sample_keys}", file=sys.stderr)
        
        print(f"  Dumped {len(submissions_to_dump)} submissions to: {debug_dir}", file=sys.stderr)


def validate_extraction(form_guid: Optional[str], validate_max: int, dump_samples: bool, token: str, timeout: int, all_forms: List[Dict[str, Any]]):
    """
    Validation mode: verify identifier extraction from submissions.
    
    Does NOT call NEW portal. Only validates extraction logic.
    """
    forms_to_validate = all_forms
    if form_guid:
        forms_to_validate = [f for f in all_forms if f.get('id') == form_guid]
        if not forms_to_validate:
            print(f"Error: Form GUID {form_guid} not found.", file=sys.stderr)
            return
    
    print(f"\nVALIDATION MODE: Processing {len(forms_to_validate)} form(s)", file=sys.stderr)
    
    # Overall counters
    overall_submissions_checked = 0
    overall_email_found = 0
    overall_phone_found = 0
    overall_either_found = 0
    overall_none_found = 0
    
    # Per-form results
    form_results = []
    
    # Sample arrays (max 5 each)
    samples_with_email = []
    samples_with_phone_only = []
    samples_with_none = []
    
    for form in forms_to_validate:
        form_id = form.get('id', '')
        form_name = form.get('name', 'Unknown')
        
        print(f"\nValidating form: {form_name} ({form_id})", file=sys.stderr)
        
        # Per-form counters
        form_submissions_checked = 0
        form_email_found = 0
        form_phone_found = 0
        form_either_found = 0
        form_none_found = 0
        
        try:
            # Iterate submissions using existing iterator
            for submission in iter_form_submissions_old(form_id, token, limit=50, timeout=timeout):
                if form_submissions_checked >= validate_max:
                    break
                
                form_submissions_checked += 1
                overall_submissions_checked += 1
                
                # Extract identifiers
                identifiers = extract_identifiers(submission)
                email = identifiers['email']
                phone = identifiers['phone']
                phone_digits = identifiers['phone_digits']
                first_name = identifiers['first_name']
                last_name = identifiers['last_name']
                
                submitted_at = submission.get('submittedAt') or submission.get('createdAt')
                page_url = submission.get('pageUrl') or "(unknown)"
                
                # Build field_map for preview
                values = submission.get('values', [])
                field_map = extract_from_values(values) if values else {}
                field_names_preview = list(field_map.keys())[:15]
                
                # Build sample object
                sample_obj = {
                    'formGuid': form_id,
                    'formName': form_name,
                    'submittedAt': submitted_at,
                    'pageUrl': page_url,
                    'email': email,
                    'phone': phone,
                    'phone_digits': phone_digits,
                    'first_name': first_name,
                    'last_name': last_name,
                    'field_names_preview': field_names_preview
                }
                
                # Classify and count
                has_email = email is not None
                has_phone = phone is not None
                
                if has_email:
                    form_email_found += 1
                    overall_email_found += 1
                    
                    if len(samples_with_email) < 5:
                        samples_with_email.append(sample_obj)
                
                if has_phone:
                    form_phone_found += 1
                    overall_phone_found += 1
                
                if has_email or has_phone:
                    form_either_found += 1
                    overall_either_found += 1
                    
                    if has_phone and not has_email and len(samples_with_phone_only) < 5:
                        samples_with_phone_only.append(sample_obj)
                else:
                    form_none_found += 1
                    overall_none_found += 1
                    
                    if len(samples_with_none) < 5:
                        samples_with_none.append(sample_obj)
            
            # Calculate percentages
            email_pct = (form_email_found / form_submissions_checked * 100) if form_submissions_checked > 0 else 0.0
            phone_pct = (form_phone_found / form_submissions_checked * 100) if form_submissions_checked > 0 else 0.0
            either_pct = (form_either_found / form_submissions_checked * 100) if form_submissions_checked > 0 else 0.0
            none_pct = (form_none_found / form_submissions_checked * 100) if form_submissions_checked > 0 else 0.0
            
            form_results.append({
                'formId': form_id,
                'formName': form_name,
                'submissions_checked': form_submissions_checked,
                'email_found': form_email_found,
                'phone_found': form_phone_found,
                'either_found': form_either_found,
                'none_found': form_none_found,
                'email_pct': round(email_pct, 2),
                'phone_pct': round(phone_pct, 2),
                'either_pct': round(either_pct, 2),
                'none_pct': round(none_pct, 2)
            })
            
            print(f"  submissions_checked: {form_submissions_checked}", file=sys.stderr)
            print(f"  email_found: {form_email_found} ({email_pct:.2f}%)", file=sys.stderr)
            print(f"  phone_found: {form_phone_found} ({phone_pct:.2f}%)", file=sys.stderr)
            print(f"  either_found: {form_either_found} ({either_pct:.2f}%)", file=sys.stderr)
            print(f"  none_found: {form_none_found} ({none_pct:.2f}%)", file=sys.stderr)
            
        except Exception as e:
            print(f"Warning: Failed to validate form {form_id} ({form_name}): {e}", file=sys.stderr)
            form_results.append({
                'formId': form_id,
                'formName': form_name,
                'error': str(e)
            })
    
    # Print overall summary
    overall_email_pct = (overall_email_found / overall_submissions_checked * 100) if overall_submissions_checked > 0 else 0.0
    overall_phone_pct = (overall_phone_found / overall_submissions_checked * 100) if overall_submissions_checked > 0 else 0.0
    overall_either_pct = (overall_either_found / overall_submissions_checked * 100) if overall_submissions_checked > 0 else 0.0
    overall_none_pct = (overall_none_found / overall_submissions_checked * 100) if overall_submissions_checked > 0 else 0.0
    
    print("\n" + "=" * 60, file=sys.stderr)
    print("EXTRACTION VALIDATION SUMMARY", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Overall totals:", file=sys.stderr)
    print(f"  submissions_checked: {overall_submissions_checked}", file=sys.stderr)
    print(f"  email_found: {overall_email_found} ({overall_email_pct:.2f}%)", file=sys.stderr)
    print(f"  phone_found: {overall_phone_found} ({overall_phone_pct:.2f}%)", file=sys.stderr)
    print(f"  either_found: {overall_either_found} ({overall_either_pct:.2f}%)", file=sys.stderr)
    print(f"  none_found: {overall_none_found} ({overall_none_pct:.2f}%)", file=sys.stderr)
    
    # Write samples JSON if requested
    if dump_samples:
        output = {
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'overall': {
                'submissions_checked': overall_submissions_checked,
                'email_found': overall_email_found,
                'phone_found': overall_phone_found,
                'either_found': overall_either_found,
                'none_found': overall_none_found,
                'email_pct': round(overall_email_pct, 2),
                'phone_pct': round(overall_phone_pct, 2),
                'either_pct': round(overall_either_pct, 2),
                'none_pct': round(overall_none_pct, 2)
            },
            'perForm': form_results,
            'samples': {
                'with_email': samples_with_email,
                'with_phone_only': samples_with_phone_only,
                'with_none': samples_with_none
            }
        }
        
        samples_path = './out/forms_extraction_samples.json'
        os.makedirs(os.path.dirname(samples_path), exist_ok=True)
        with open(samples_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\nExtraction samples written to: {samples_path}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description='HubSpot Forms Discovery and Submission Counting',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--init',
        action='store_true',
        help='Initialize forms discovery and counting'
    )
    parser.add_argument(
        '--count-contacts',
        action='store_true',
        help='Count contacts in NEW portal for form submissions'
    )
    parser.add_argument(
        '--count-contactsold',
        action='store_true',
        help='Count contacts in OLD portal for form submissions'
    )
    parser.add_argument(
        '--new-contacts',
        action='store_true',
        help='Compute contacts needed in NEW portal based on OLD portal submissions'
    )
    parser.add_argument(
        '--new-contacts-include-present',
        action='store_true',
        help='Include present emails list in JSON output (use with --new-contacts)'
    )
    parser.add_argument(
        '--new-contacts-limit',
        type=int,
        default=0,
        help='Stop after N unique emails for testing (0 = no limit, use with --new-contacts)'
    )
    parser.add_argument(
        '--since-migrate',
        action='store_true',
        help='Analyze contacts from form submissions since cutoff date (default: Nov 1, 2025)'
    )
    parser.add_argument(
        '--since-date',
        type=str,
        default='2025-11-01',
        help='Cutoff date in YYYY-MM-DD format (default: 2025-11-01 for --since-migrate, optional for --create-notes, timezone: America/Toronto)'
    )
    parser.add_argument(
        '--db-difference',
        action='store_true',
        help='Count contacts in OLD vs NEW portal and print the difference'
    )
    parser.add_argument(
        '--get-one',
        action='store_true',
        help='Print raw HubSpot API response for a single contact that exists in both portals'
    )
    parser.add_argument(
        '--email',
        type=str,
        help='Specific email to fetch (use with --get-one). If not provided, auto-picks from overlap.'
    )
    parser.add_argument(
        '--portal',
        type=str,
        choices=['old', 'new', 'both'],
        default='both',
        help='Which portal(s) to print (default: both). Use with --get-one.'
    )
    parser.add_argument(
        '--get-one-form-guid',
        type=str,
        help='Restrict submission search to a specific form GUID (use with --get-one)'
    )
    parser.add_argument(
        '--get-one-max-scan',
        type=int,
        default=5000,
        help='Max number of submissions to scan before giving up (default: 5000, use with --get-one)'
    )
    parser.add_argument(
        '--test-10',
        action='store_true',
        help='Generate reliability test set of 10 examples (submission + contacts + note body)'
    )
    parser.add_argument(
        '--test-n',
        type=int,
        default=10,
        help='Number of test examples to generate (default: 10, use with --test-10)'
    )
    parser.add_argument(
        '--test-max-scan',
        type=int,
        default=20000,
        help='Max submissions to scan before stopping (default: 20000, use with --test-10)'
    )
    parser.add_argument(
        '--test-since-date',
        type=str,
        help='Only include submissions >= this date YYYY-MM-DD (Toronto timezone, use with --test-10)'
    )
    parser.add_argument(
        '--create-notes',
        action='store_true',
        help='Create notes in NEW portal for form submissions (requires --dry-run or explicit confirmation)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview note creation without writing to HubSpot (use with --create-notes)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=0,
        help='Maximum notes to create (0 = no limit, use with --create-notes)'
    )
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from previously saved progress (loads created_note_keys.jsonl, use with --create-notes)'
    )
    parser.add_argument(
        '--max-scan',
        type=int,
        default=0,
        help='Maximum submissions to scan before stopping (0 = scan all, use with --create-notes)'
    )
    parser.add_argument(
        '--forms',
        type=str,
        help='Comma-separated form GUIDs to process (use with --create-notes)'
    )
    parser.add_argument(
        '--debug-submissions',
        action='store_true',
        help='Debug mode: dump raw submission payloads to disk'
    )
    parser.add_argument(
        '--debug-form-guid',
        type=str,
        help='Only debug this specific form GUID (use with --debug-submissions)'
    )
    parser.add_argument(
        '--debug-max',
        type=int,
        default=5,
        help='Maximum submissions to dump per form in debug mode (default: 5)'
    )
    parser.add_argument(
        '--validate-extraction',
        action='store_true',
        help='Validate identifier extraction from submissions (does not call NEW portal)'
    )
    parser.add_argument(
        '--validate-form-guid',
        type=str,
        help='Only validate extraction for this specific form GUID (use with --validate-extraction)'
    )
    parser.add_argument(
        '--validate-max',
        type=int,
        default=50,
        help='Maximum submissions to inspect per form in validation mode (default: 50)'
    )
    parser.add_argument(
        '--validate-dump',
        action='store_true',
        help='Dump extraction samples to JSON file (use with --validate-extraction)'
    )
    parser.add_argument(
        '--out',
        type=str,
        default='./out/forms_init_summary.json',
        help='Output JSON file path (default: ./out/forms_init_summary.json)'
    )
    parser.add_argument(
        '--include-archived',
        action='store_true',
        default=True,
        help='Include archived forms (default: True, always included in --init)'
    )
    parser.add_argument(
        '--max-forms',
        type=int,
        default=0,
        help='Maximum number of forms to process (0 = no limit, default: 0)'
    )
    parser.add_argument(
        '--top-pages',
        type=int,
        default=20,
        help='Number of top page URLs to include (default: 20)'
    )
    parser.add_argument(
        '--top-forms',
        type=int,
        default=10,
        help='Number of top forms by submissions to include (default: 10)'
    )
    parser.add_argument(
        '--timeout-seconds',
        type=int,
        default=30,
        help='Request timeout in seconds (default: 30)'
    )
    
    args = parser.parse_args()
    
    if not args.init and not args.count_contacts and not args.count_contactsold and not args.new_contacts and not args.since_migrate and not args.db_difference and not args.debug_submissions and not args.validate_extraction and not args.get_one and not args.test_10 and not args.create_notes:
        parser.error("At least one of --init, --count-contacts, --count-contactsold, --new-contacts, --since-migrate, --db-difference, --debug-submissions, --validate-extraction, --get-one, --test-10, or --create-notes is required")
    
    # Validate --create-notes flags
    if args.create_notes:
        if args.dry_run:
            print("Warning: Both --create-notes and --dry-run provided. Treating as dry-run (no writes).", file=sys.stderr)
    
    # Get tokens
    old_token = None
    new_token = None
    
    if args.init or args.count_contacts or args.count_contactsold or args.new_contacts or args.since_migrate or args.db_difference or args.debug_submissions or args.validate_extraction or args.get_one or args.test_10 or args.create_notes:
        old_token = get_old_access_token()
        print("Using OLD_ACCESS_TOKEN from .env file", file=sys.stderr)
    
    if args.count_contacts or args.new_contacts or args.since_migrate or args.db_difference or args.get_one or args.test_10 or args.create_notes:
        new_token = get_access_token()
        print("Using ACCESS_TOKEN from .env file", file=sys.stderr)
    
    # Ensure output directory exists
    out_dir = os.path.dirname(args.out)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    
    # Fetch forms (live + archived) - needed for --init, --count-contacts, --count-contactsold, --new-contacts, --since-migrate, --debug-submissions, and --validate-extraction
    all_forms = []
    if args.init or args.count_contacts or args.count_contactsold or args.new_contacts or args.since_migrate or args.debug_submissions or args.validate_extraction:
        print("Fetching live forms...", file=sys.stderr)
        live_forms = list_forms(archived=False, token=old_token, timeout=args.timeout_seconds)
        print(f"Found {len(live_forms)} live forms", file=sys.stderr)
        
        print("Fetching archived forms...", file=sys.stderr)
        archived_forms = list_forms(archived=True, token=old_token, timeout=args.timeout_seconds)
        print(f"Found {len(archived_forms)} archived forms", file=sys.stderr)
        
        # De-duplicate by form id (preserve archived flag from record)
        forms_dict = {}
        for form in live_forms:
            form_id = form.get('id')
            if form_id:
                forms_dict[form_id] = form
                forms_dict[form_id]['archived'] = False
        
        for form in archived_forms:
            form_id = form.get('id')
            if form_id:
                # If already exists, preserve archived=True if this one is archived
                if form_id in forms_dict:
                    if form.get('archived', False):
                        forms_dict[form_id]['archived'] = True
                else:
                    forms_dict[form_id] = form
                    forms_dict[form_id]['archived'] = form.get('archived', False)
        
        all_forms = list(forms_dict.values())
        total_forms = len(all_forms)
        print(f"\nTotal unique forms: {total_forms}", file=sys.stderr)
    
    # Run --debug-submissions if requested
    if args.debug_submissions:
        debug_submissions(args.debug_form_guid, args.debug_max, old_token, args.timeout_seconds, all_forms)
    
    # Run --validate-extraction if requested
    if args.validate_extraction:
        validate_extraction(args.validate_form_guid, args.validate_max, args.validate_dump, old_token, args.timeout_seconds, all_forms)
    
    # Run --init if requested
    if args.init:
        # Process forms and count submissions
        total_submissions = 0
        submissions_by_form = []
        all_page_urls_counter = Counter()
        
        forms_to_process = all_forms
        if args.max_forms > 0:
            forms_to_process = all_forms[:args.max_forms]
            print(f"Processing first {len(forms_to_process)} forms (--max-forms={args.max_forms})...", file=sys.stderr)
        
        for i, form in enumerate(forms_to_process, 1):
            form_id = form.get('id', '')
            form_name = form.get('name', 'Unknown')
            form_type = form.get('formType')
            is_archived = form.get('archived', False)
            
            print(f"Processed forms {i}/{len(forms_to_process)} | total_submissions_so_far={total_submissions}", file=sys.stderr)
            
            try:
                submission_count, page_url_counter = count_form_submissions(
                    form_id, old_token, timeout=args.timeout_seconds
                )
                total_submissions += submission_count
                
                # Accumulate page URLs
                all_page_urls_counter.update(page_url_counter)
                
                submissions_by_form.append({
                    'id': form_id,
                    'name': form_name,
                    'formType': form_type,
                    'archived': is_archived,
                    'submissionCount': submission_count
                })
            except Exception as e:
                print(f"Warning: Failed to count submissions for form {form_id} ({form_name}): {e}", file=sys.stderr)
                submissions_by_form.append({
                    'id': form_id,
                    'name': form_name,
                    'formType': form_type,
                    'archived': is_archived,
                    'submissionCount': 0,
                    'error': str(e)
                })
        
        # Compute statistics
        forms_live = sum(1 for f in submissions_by_form if not f.get('archived', False))
        forms_archived = sum(1 for f in submissions_by_form if f.get('archived', False))
        
        # Top forms by submissions
        top_forms_by_submissions = sorted(
            submissions_by_form,
            key=lambda x: x.get('submissionCount', 0),
            reverse=True
        )[:args.top_forms]
        
        # Top page URLs
        top_page_urls = [
            {'pageUrl': url, 'count': count}
            for url, count in all_page_urls_counter.most_common(args.top_pages)
        ]
        unique_page_urls = len(all_page_urls_counter)
        
        # Print summary to terminal
        print("\n" + "=" * 60, file=sys.stderr)
        print("FORMS SUMMARY", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        print(f"forms_total: {total_forms}", file=sys.stderr)
        print(f"submissions_total: {total_submissions}", file=sys.stderr)
        print(f"archived_forms: {forms_archived}", file=sys.stderr)
        print(f"live_forms: {forms_live}", file=sys.stderr)
        print(f"\ntop_forms:", file=sys.stderr)
        for form in top_forms_by_submissions:
            print(f"  - {form['name']} (id: {form['id']}, count: {form['submissionCount']})", file=sys.stderr)
        print(f"\ntop_pages:", file=sys.stderr)
        for page in top_page_urls[:10]:  # Show top 10 in terminal
            print(f"  - {page['pageUrl']}: {page['count']}", file=sys.stderr)
        
        # Build JSON output
        output = {
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'formsTotal': total_forms,
            'formsLive': forms_live,
            'formsArchived': forms_archived,
            'submissionsTotal': total_submissions,
            'submissionsByForm': submissions_by_form,
            'topFormsBySubmissions': top_forms_by_submissions,
            'pageUrlStats': {
                'uniquePageUrls': unique_page_urls,
                'topPageUrls': top_page_urls
            }
        }
        
        # Write JSON file
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\nJSON summary written to: {args.out}", file=sys.stderr)
    
    # Run --count-contacts if requested
    if args.count_contacts:
        count_contacts_for_forms(
            contacts_token=new_token,
            output_prefix='forms_count_contacts',
            label='NEW PORTAL',
            old_token=old_token,
            all_forms=all_forms,
            timeout=args.timeout_seconds,
            max_forms=args.max_forms
        )
    
    # Run --count-contactsold if requested
    if args.count_contactsold:
        count_contacts_for_forms(
            contacts_token=old_token,
            output_prefix='forms_count_contacts_old',
            label='OLD PORTAL',
            old_token=old_token,
            all_forms=all_forms,
            timeout=args.timeout_seconds,
            max_forms=args.max_forms
        )
    
    # Run --new-contacts if requested
    if args.new_contacts:
        run_new_contacts_needed(
            old_token=old_token,
            new_token=new_token,
            all_forms=all_forms,
            timeout=args.timeout_seconds,
            include_present=args.new_contacts_include_present,
            email_limit=args.new_contacts_limit
        )
    
    # Run --since-migrate if requested
    if args.since_migrate:
        run_since_migrate(
            since_date_str=args.since_date,
            old_token=old_token,
            new_token=new_token,
            all_forms=all_forms,
            timeout=args.timeout_seconds
        )
    
    # Run --db-difference if requested
    if args.db_difference:
        run_db_difference(
            old_token=old_token,
            new_token=new_token,
            timeout=args.timeout_seconds
        )
    
    # Run --get-one if requested
    if args.get_one:
        run_get_one(
            old_token=old_token,
            new_token=new_token,
            email=args.email,
            portal=args.portal,
            form_guid=getattr(args, 'get_one_form_guid', None),
            max_scan=getattr(args, 'get_one_max_scan', 5000),
            timeout=args.timeout_seconds
        )
    
    if args.test_10:
        run_test_10(
            old_token=old_token,
            new_token=new_token,
            n_requested=getattr(args, 'test_n', 10),
            max_scan=getattr(args, 'test_max_scan', 20000),
            since_date=getattr(args, 'test_since_date', None),
            timeout=args.timeout_seconds
        )
    
    if args.create_notes:
        run_create_notes(
            old_token=old_token,
            new_token=new_token,
            dry_run=args.dry_run or False,  # If both flags, dry_run wins
            limit=getattr(args, 'limit', 0),
            since_date=getattr(args, 'since_date', None),
            resume=getattr(args, 'resume', False),
            max_scan=getattr(args, 'max_scan', 0),
            forms_filter=getattr(args, 'forms', None),
            timeout=args.timeout_seconds
        )


def count_contacts_for_forms(contacts_token: str, output_prefix: str, label: str, old_token: str, all_forms: List[Dict[str, Any]], timeout: int, max_forms: int = 0):
    """
    Generic function to count contacts for form submissions.
    
    Args:
        contacts_token: Token used for searching contacts (OLD or NEW portal)
        output_prefix: Prefix for output files (e.g., "forms_count_contacts" or "forms_count_contacts_old")
        label: Label for console output (e.g., "NEW PORTAL" or "OLD PORTAL")
        old_token: Token for fetching submissions (always OLD_ACCESS_TOKEN)
        all_forms: List of forms to process
        timeout: Request timeout
        max_forms: Maximum forms to process (0 = all)
    """
    # Initialize caches
    email_cache: Dict[str, Tuple[bool, Optional[str]]] = {}
    phone_cache: Dict[str, Tuple[bool, Optional[str]]] = {}
    
    # Overall counters
    overall_matched = 0
    overall_missed = 0
    overall_no_identifier = 0
    
    # Unique identifier tracking
    unique_emails_in_submissions = set()
    unique_emails_matched = set()
    unique_phones_in_submissions = set()
    unique_phones_matched = set()
    
    # Per-form results
    form_results = []
    form_diagnostics = []
    
    # Sample arrays (max 5 each)
    matched_samples = []
    missed_samples = []
    no_identifier_samples = []
    
    forms_to_process = all_forms
    if max_forms > 0:
        forms_to_process = all_forms[:max_forms]
        print(f"\nProcessing first {len(forms_to_process)} forms for contact counting (--max-forms={max_forms})...", file=sys.stderr)
    
    for i, form in enumerate(forms_to_process, 1):
        form_id = form.get('id', '')
        form_name = form.get('name', 'Unknown')
        is_archived = form.get('archived', False)
        
        print(f"\nProcessing form {i}/{len(forms_to_process)}: {form_name} ({form_id})", file=sys.stderr)
        
        # Per-form counters
        form_matched = 0
        form_missed = 0
        form_no_identifier = 0
        form_emails_found = 0
        form_phones_found = 0
        form_submissions_iterated = 0
        form_pages_fetched = 0
        form_sample_extracted = []
        
        try:
            # Get all submissions for this form (returns tuple: submissions, pages_fetched)
            submissions, pages_fetched = get_form_submissions(form_id, old_token, timeout=timeout)
            form_pages_fetched = pages_fetched
            form_submissions_iterated = len(submissions)
            
            print(f"  Pages fetched: {pages_fetched}, Submissions iterated: {len(submissions)}", file=sys.stderr)
            
            for submission in submissions:
                # Extract identifiers
                identifiers = extract_identifiers(submission)
                email = identifiers['email']
                phone = identifiers['phone']
                phone_digits = identifiers['phone_digits']
                
                submitted_at = submission.get('submittedAt') or submission.get('createdAt')
                page_url = submission.get('pageUrl') or "(unknown)"
                
                # Track identifier extraction
                if email:
                    form_emails_found += 1
                    unique_emails_in_submissions.add(email)
                if phone_digits:
                    form_phones_found += 1
                    unique_phones_in_submissions.add(phone_digits)
                
                # Add to sample extracted if needed
                if len(form_sample_extracted) < 5 and (email or phone):
                    form_sample_extracted.append({
                        'submittedAt': submitted_at,
                        'pageUrl': page_url,
                        'email': email,
                        'phone': phone,
                        'phone_digits': phone_digits
                    })
                
                # Determine outcome
                matched = False
                contact_id = None
                
                # Classification: if email OR phone exists => attempt lookup
                if email or phone:
                    # Try email first (preferred)
                    if email:
                        # Check cache first
                        if email in email_cache:
                            matched, contact_id = email_cache[email]
                        else:
                            # Search contacts using provided token
                            matched, contact_id = search_contact_new_portal_by_email(email, contacts_token, timeout=timeout)
                            email_cache[email] = (matched, contact_id)
                        
                        if matched:
                            unique_emails_matched.add(email)
                    
                    # If email didn't match, try phone (fallback)
                    if not matched and phone:
                        phone_key = phone if phone else phone_digits
                        if phone_key:
                            # Check cache first
                            if phone_key in phone_cache:
                                matched, contact_id = phone_cache[phone_key]
                            else:
                                # Search contacts using provided token
                                matched, contact_id = search_contact_new_portal_by_phone(phone, phone_digits, contacts_token, timeout=timeout)
                                phone_cache[phone_key] = (matched, contact_id)
                            
                            if matched and phone_digits:
                                unique_phones_matched.add(phone_digits)
                    
                    # Classify result
                    if matched:
                        form_matched += 1
                        overall_matched += 1
                        
                        # Add to samples if needed
                        if len(matched_samples) < 5:
                            matched_samples.append({
                                'submittedAt': submitted_at,
                                'pageUrl': page_url,
                                'email': email,
                                'phone': phone,
                                'phone_digits': phone_digits,
                                'contactId': contact_id
                            })
                    else:
                        form_missed += 1
                        overall_missed += 1
                        
                        # Add to samples if needed
                        if len(missed_samples) < 5:
                            missed_samples.append({
                                'submittedAt': submitted_at,
                                'pageUrl': page_url,
                                'email': email,
                                'phone': phone,
                                'phone_digits': phone_digits
                            })
                else:
                    # No identifier found (both email and phone are missing)
                    form_no_identifier += 1
                    overall_no_identifier += 1
                    
                    # Add to samples if needed
                    if len(no_identifier_samples) < 5:
                        sample = {
                            'submittedAt': submitted_at,
                            'pageUrl': page_url
                        }
                        if identifiers['first_name']:
                            sample['first_name'] = identifiers['first_name']
                        if identifiers['last_name']:
                            sample['last_name'] = identifiers['last_name']
                        no_identifier_samples.append(sample)
            
            # Calculate match rate
            total_with_identifier = form_matched + form_missed
            match_rate = (form_matched / total_with_identifier * 100) if total_with_identifier > 0 else 0.0
            
            form_results.append({
                'formId': form_id,
                'formName': form_name,
                'archived': is_archived,
                'submissionsProcessed': form_submissions_iterated,
                'matched': form_matched,
                'missed': form_missed,
                'noIdentifier': form_no_identifier,
                'matchRate': round(match_rate, 2)
            })
            
            form_diagnostics.append({
                'formId': form_id,
                'formName': form_name,
                'pagesFetched': form_pages_fetched,
                'submissionsIterated': form_submissions_iterated,
                'emailsFoundCount': form_emails_found,
                'phonesFoundCount': form_phones_found,
                'noIdentifierCount': form_no_identifier,
                'sampleExtracted': form_sample_extracted
            })
            
            print(f"  Results: pages_fetched={form_pages_fetched}, submissions_iterated={form_submissions_iterated}", file=sys.stderr)
            print(f"  Identifiers: emails_found={form_emails_found}, phones_found={form_phones_found}, no_identifier={form_no_identifier}", file=sys.stderr)
            print(f"  Matching: matched={form_matched}, missed={form_missed}, match_rate={match_rate:.2f}%", file=sys.stderr)
            
        except Exception as e:
            print(f"Warning: Failed to process form {form_id} ({form_name}): {e}", file=sys.stderr)
            form_results.append({
                'formId': form_id,
                'formName': form_name,
                'archived': is_archived,
                'error': str(e)
            })
            form_diagnostics.append({
                'formId': form_id,
                'formName': form_name,
                'error': str(e)
            })
    
    # Calculate overall match rate
    overall_total_with_identifier = overall_matched + overall_missed
    overall_match_rate = (overall_matched / overall_total_with_identifier * 100) if overall_total_with_identifier > 0 else 0.0
    
    # Calculate unique identifier metrics
    unique_emails_missing = len(unique_emails_in_submissions) - len(unique_emails_matched)
    unique_phones_missing = len(unique_phones_in_submissions) - len(unique_phones_matched)
    
    # Print summary
    print("\n" + "=" * 60, file=sys.stderr)
    print(f"CONTACT COUNTING SUMMARY ({label})", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Overall totals:", file=sys.stderr)
    print(f"  matched: {overall_matched}", file=sys.stderr)
    print(f"  missed: {overall_missed}", file=sys.stderr)
    print(f"  no_identifier: {overall_no_identifier}", file=sys.stderr)
    print(f"  match_rate: {overall_match_rate:.2f}%", file=sys.stderr)
    print(f"\nUnique identifier metrics:", file=sys.stderr)
    print(f"  unique_emails_in_submissions: {len(unique_emails_in_submissions)}", file=sys.stderr)
    print(f"  unique_emails_matched_in_{label.lower().replace(' ', '_')}: {len(unique_emails_matched)}", file=sys.stderr)
    print(f"  unique_emails_missing_in_{label.lower().replace(' ', '_')}: {unique_emails_missing}", file=sys.stderr)
    print(f"  unique_phones_in_submissions: {len(unique_phones_in_submissions)}", file=sys.stderr)
    print(f"  unique_phones_matched_in_{label.lower().replace(' ', '_')}: {len(unique_phones_matched)}", file=sys.stderr)
    print(f"  unique_phones_missing_in_{label.lower().replace(' ', '_')}: {unique_phones_missing}", file=sys.stderr)
    print(f"\nPer-form breakdown:", file=sys.stderr)
    for result in form_results:
        if 'error' not in result:
            print(f"  {result['formName']} ({result['formId']}):", file=sys.stderr)
            print(f"    submissions_processed: {result['submissionsProcessed']}", file=sys.stderr)
            print(f"    matched: {result['matched']}", file=sys.stderr)
            print(f"    missed: {result['missed']}", file=sys.stderr)
            print(f"    no_identifier: {result['noIdentifier']}", file=sys.stderr)
            print(f"    match_rate: {result['matchRate']:.2f}%", file=sys.stderr)
    
    # Print diagnostics summary
    print(f"\nDiagnostics:", file=sys.stderr)
    for diag in form_diagnostics:
        if 'error' not in diag:
            print(f"  {diag['formName']} ({diag['formId']}):", file=sys.stderr)
            print(f"    pages_fetched: {diag['pagesFetched']}", file=sys.stderr)
            print(f"    submissions_iterated: {diag['submissionsIterated']}", file=sys.stderr)
            print(f"    emails_found_count: {diag['emailsFoundCount']}", file=sys.stderr)
            print(f"    phones_found_count: {diag['phonesFoundCount']}", file=sys.stderr)
            print(f"    no_identifier_count: {diag['noIdentifierCount']}", file=sys.stderr)
    
    # Build JSON output
    output = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'portal': label,
        'overallCounts': {
            'matched': overall_matched,
            'missed': overall_missed,
            'noIdentifier': overall_no_identifier,
            'matchRate': round(overall_match_rate, 2)
        },
        'uniqueIdentifiers': {
            'unique_emails_in_submissions': len(unique_emails_in_submissions),
            'unique_emails_matched': len(unique_emails_matched),
            'unique_emails_missing': unique_emails_missing,
            'unique_phones_in_submissions': len(unique_phones_in_submissions),
            'unique_phones_matched': len(unique_phones_matched),
            'unique_phones_missing': unique_phones_missing
        },
        'perFormBreakdown': form_results,
        'samples': {
            'matched': matched_samples,
            'missed': missed_samples,
            'noIdentifier': no_identifier_samples
        }
    }
    
    # Write JSON file
    contacts_out_path = f'./out/{output_prefix}.json'
    os.makedirs(os.path.dirname(contacts_out_path), exist_ok=True)
    with open(contacts_out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nJSON results written to: {contacts_out_path}", file=sys.stderr)
    
    # Write diagnostics JSON
    diagnostics_output = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'portal': label,
        'perFormDiagnostics': form_diagnostics
    }
    
    diagnostics_out_path = f'./out/{output_prefix}_diagnostics.json'
    with open(diagnostics_out_path, 'w', encoding='utf-8') as f:
        json.dump(diagnostics_output, f, indent=2, ensure_ascii=False)
    
        print(f"Diagnostics written to: {diagnostics_out_path}", file=sys.stderr)


def fetch_new_portal_contact_email_index(token: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[set, Dict[str, Any]]:
    """
    Fetch all contacts from NEW portal and build email index.
    
    Returns (new_contact_emails_set, summary_dict).
    """
    new_contact_emails_set = set()
    contacts_fetched = 0
    emails_indexed = 0
    pages_fetched = 0
    after = None
    
    print("Fetching NEW portal contacts to build email index...", file=sys.stderr)
    
    while True:
        params = {
            'limit': 100,
            'properties': 'email'
        }
        
        if after is not None:
            params['after'] = after
        
        url = f"{BASE_URL}/crm/v3/objects/contacts"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        pages_fetched += 1
        
        results = response.get('results', [])
        if not results:
            break
        
        contacts_fetched += len(results)
        
        for contact in results:
            email = contact.get('properties', {}).get('email')
            if email:
                email_normalized = email.strip().lower()
                if email_normalized:
                    new_contact_emails_set.add(email_normalized)
                    emails_indexed += 1
        
        # Check for next page
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after = next_page.get('after')
        
        if not next_after:
            break
        
        after = next_after
        
        # Progress update every 1000 contacts
        if contacts_fetched % 1000 == 0:
            print(f"  Progress: {pages_fetched} pages, {contacts_fetched} contacts, {emails_indexed} emails indexed", file=sys.stderr)
    
    summary = {
        'contacts_fetched': contacts_fetched,
        'emails_indexed': emails_indexed,
        'pages_fetched': pages_fetched
    }
    
    print(f"  Completed: {pages_fetched} pages, {contacts_fetched} contacts, {emails_indexed} emails indexed", file=sys.stderr)
    
    # Write index summary
    index_summary_path = './out/new_portal_contact_index_summary.json'
    os.makedirs(os.path.dirname(index_summary_path), exist_ok=True)
    index_output = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'contacts_fetched': contacts_fetched,
        'emails_indexed': emails_indexed,
        'pages_fetched': pages_fetched
    }
    with open(index_summary_path, 'w', encoding='utf-8') as f:
        json.dump(index_output, f, indent=2, ensure_ascii=False)
    
    print(f"  Index summary written to: {index_summary_path}", file=sys.stderr)
    
    return (new_contact_emails_set, summary)


def run_new_contacts_needed(old_token: str, new_token: str, all_forms: List[Dict[str, Any]], timeout: int, include_present: bool, email_limit: int):
    """
    Compute contacts needed in NEW portal based on OLD portal submissions.
    """
    print("\n" + "=" * 60, file=sys.stderr)
    print("NEW CONTACTS NEEDED ANALYSIS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Step 1: Collect unique emails from OLD portal submissions
    print("\nStep 1: Collecting emails from OLD portal submissions...", file=sys.stderr)
    
    submission_emails_set = set()
    submission_phone_digits_set = set()
    email_first_seen: Dict[str, Dict[str, Any]] = {}
    total_submissions_processed = 0
    submissions_with_email = 0
    submissions_with_phone = 0
    
    per_form_stats = []
    form_emails_map: Dict[str, set] = {}  # form_id -> set of emails
    form_submissions_count: Dict[str, int] = {}  # form_id -> count
    
    for i, form in enumerate(all_forms, 1):
        form_id = form.get('id', '')
        form_name = form.get('name', 'Unknown')
        
        print(f"Processing form {i}/{len(all_forms)}: {form_name} ({form_id})", file=sys.stderr)
        
        form_submissions_iterated = 0
        form_unique_emails = set()
        form_emails_map[form_id] = form_unique_emails
        
        try:
            # Iterate submissions using existing iterator
            for submission in iter_form_submissions_old(form_id, old_token, limit=50, timeout=timeout):
                total_submissions_processed += 1
                form_submissions_iterated += 1
                
                # Extract identifiers
                identifiers = extract_identifiers(submission)
                email = identifiers['email']
                phone_digits = identifiers['phone_digits']
                
                submitted_at = submission.get('submittedAt') or submission.get('createdAt')
                page_url = submission.get('pageUrl') or "(unknown)"
                
                if email:
                    submissions_with_email += 1
                    email_normalized = email.strip().lower()
                    submission_emails_set.add(email_normalized)
                    form_unique_emails.add(email_normalized)
                    
                    # Track first seen
                    if email_normalized not in email_first_seen:
                        email_first_seen[email_normalized] = {
                            'formGuid': form_id,
                            'formName': form_name,
                            'submittedAt': submitted_at,
                            'pageUrl': page_url
                        }
                
                if phone_digits:
                    submissions_with_phone += 1
                    submission_phone_digits_set.add(phone_digits)
                
                # Check limit
                if email_limit > 0 and len(submission_emails_set) >= email_limit:
                    print(f"  Reached email limit ({email_limit}), stopping collection", file=sys.stderr)
                    break
            
            form_submissions_count[form_id] = form_submissions_iterated
            print(f"  Processed {form_submissions_iterated} submissions, {len(form_unique_emails)} unique emails", file=sys.stderr)
            
            if email_limit > 0 and len(submission_emails_set) >= email_limit:
                break
                
        except Exception as e:
            print(f"Warning: Failed to process form {form_id} ({form_name}): {e}", file=sys.stderr)
            form_emails_map[form_id] = set()
            form_submissions_count[form_id] = 0
    
    print(f"\nCollection complete:", file=sys.stderr)
    print(f"  Total submissions processed: {total_submissions_processed}", file=sys.stderr)
    print(f"  Submissions with email: {submissions_with_email}", file=sys.stderr)
    print(f"  Submissions with phone: {submissions_with_phone}", file=sys.stderr)
    print(f"  Unique submission emails: {len(submission_emails_set)}", file=sys.stderr)
    print(f"  Unique submission phone digits: {len(submission_phone_digits_set)}", file=sys.stderr)
    
    # Step 2: Fetch NEW portal contact email index
    print("\nStep 2: Building NEW portal contact email index...", file=sys.stderr)
    new_contact_emails_set, index_summary = fetch_new_portal_contact_email_index(new_token, timeout=timeout)
    
    # Step 3: Compute missing emails
    print("\nStep 3: Computing missing contacts...", file=sys.stderr)
    missing_emails = sorted(submission_emails_set - new_contact_emails_set)
    present_emails = sorted(submission_emails_set & new_contact_emails_set)
    
    # Build per-form stats with missing counts
    for form_id, form_unique_emails in form_emails_map.items():
        form = next((f for f in all_forms if f.get('id') == form_id), None)
        if form:
            form_name = form.get('name', 'Unknown')
            form_missing = [e for e in form_unique_emails if e in missing_emails]
            per_form_stats.append({
                'formGuid': form_id,
                'formName': form_name,
                'submissions_iterated': form_submissions_count.get(form_id, 0),
                'unique_emails_in_form': len(form_unique_emails),
                'missing_emails_in_form_count': len(form_missing)
            })
    
    # Print summary
    print("\n" + "=" * 60, file=sys.stderr)
    print("RESULTS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Total submissions processed: {total_submissions_processed}", file=sys.stderr)
    print(f"Unique submission emails: {len(submission_emails_set)}", file=sys.stderr)
    print(f"New portal contacts indexed: {index_summary['contacts_fetched']} contacts, {index_summary['emails_indexed']} emails", file=sys.stderr)
    print(f"Unique submission emails present in NEW portal: {len(present_emails)}", file=sys.stderr)
    print(f"Unique submission emails missing in NEW portal: {len(missing_emails)}", file=sys.stderr)
    
    if len(submission_emails_set) > 0:
        present_pct = (len(present_emails) / len(submission_emails_set) * 100)
        missing_pct = (len(missing_emails) / len(submission_emails_set) * 100)
        print(f"Percent present: {present_pct:.2f}%", file=sys.stderr)
        print(f"Percent missing: {missing_pct:.2f}%", file=sys.stderr)
    
    # Build JSON output
    output = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'totals': {
            'total_submissions_processed': total_submissions_processed,
            'unique_submission_emails': len(submission_emails_set),
            'unique_submission_phones': len(submission_phone_digits_set),
            'new_portal_contacts_fetched': index_summary['contacts_fetched'],
            'new_portal_emails_indexed': index_summary['emails_indexed'],
            'unique_emails_present_in_new_portal': len(present_emails),
            'unique_emails_missing_in_new_portal': len(missing_emails)
        },
        'missing_emails': missing_emails,
        'perFormStats': per_form_stats
    }
    
    if include_present:
        output['present_emails'] = present_emails
    else:
        output['present_emails_count_only'] = len(present_emails)
    
    # Write JSON file
    json_path = './out/new_contacts_needed.json'
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nJSON results written to: {json_path}", file=sys.stderr)
    
    # Write CSV file
    csv_path = './out/new_contacts_needed.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['email', 'first_seen_formGuid', 'first_seen_formName', 'first_seen_submittedAt', 'first_seen_pageUrl'])
        
        for email in missing_emails:
            first_seen = email_first_seen.get(email, {})
            writer.writerow([
                email,
                first_seen.get('formGuid', ''),
                first_seen.get('formName', ''),
                first_seen.get('submittedAt', ''),
                first_seen.get('pageUrl', '')
            ])
    
    print(f"CSV results written to: {csv_path}", file=sys.stderr)
    print(f"\nContacts to create/migrate: {len(missing_emails)} unique emails", file=sys.stderr)


def run_since_migrate(since_date_str: str, old_token: str, new_token: str, all_forms: List[Dict[str, Any]], timeout: int):
    """
    Analyze contacts from form submissions since cutoff date.
    
    Compares OLD portal submissions (since cutoff) against NEW portal contacts.
    """
    print("\n" + "=" * 60, file=sys.stderr)
    print("SINCE MIGRATE ANALYSIS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Step 1: Parse cutoff date to epoch ms
    if ZoneInfo is None:
        print("Error: zoneinfo not available. Please use Python 3.9+ or install backports.zoneinfo", file=sys.stderr)
        sys.exit(2)
    
    try:
        # Parse date string YYYY-MM-DD
        year, month, day = map(int, since_date_str.split('-'))
        # Create datetime at midnight in America/Toronto
        cutoff_local = datetime(year, month, day, 0, 0, 0, tzinfo=ZoneInfo("America/Toronto"))
        # Convert to UTC
        cutoff_utc = cutoff_local.astimezone(timezone.utc)
        # Convert to epoch milliseconds
        cutoff_ms = int(cutoff_utc.timestamp() * 1000)
        
        print(f"\nCutoff date: {since_date_str} 00:00:00 America/Toronto", file=sys.stderr)
        print(f"Cutoff (UTC): {cutoff_utc.isoformat()}", file=sys.stderr)
        print(f"Cutoff (epoch ms): {cutoff_ms}", file=sys.stderr)
    except Exception as e:
        print(f"Error: Failed to parse --since-date '{since_date_str}': {e}", file=sys.stderr)
        print("Expected format: YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)
    
    # Step 2: Collect submission emails since cutoff from OLD portal
    print("\nStep 1: Collecting emails from OLD portal submissions since cutoff...", file=sys.stderr)
    
    since_submission_emails_set = set()
    email_first_seen: Dict[str, Dict[str, Any]] = {}
    total_submissions_since_cutoff = 0
    submissions_since_cutoff_with_email = 0
    
    per_form_stats = []
    
    for i, form in enumerate(all_forms, 1):
        form_id = form.get('id', '')
        form_name = form.get('name', 'Unknown')
        
        form_submissions_since_cutoff = 0
        form_unique_emails_since_cutoff = set()
        
        try:
            # Iterate submissions using existing iterator
            for submission in iter_form_submissions_old(form_id, old_token, limit=50, timeout=timeout):
                # Filter by submittedAt
                submitted_at = submission.get('submittedAt')
                if not submitted_at:
                    continue
                
                # Handle both int (epoch ms) and string formats
                if isinstance(submitted_at, str):
                    try:
                        # Try parsing ISO format or epoch string
                        if submitted_at.isdigit():
                            submitted_at_ms = int(submitted_at)
                        else:
                            # Try ISO format
                            dt = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
                            submitted_at_ms = int(dt.timestamp() * 1000)
                    except (ValueError, AttributeError):
                        continue
                elif isinstance(submitted_at, int):
                    submitted_at_ms = submitted_at
                else:
                    continue
                
                # Check if submission is after cutoff
                if submitted_at_ms < cutoff_ms:
                    continue
                
                total_submissions_since_cutoff += 1
                form_submissions_since_cutoff += 1
                
                # Extract identifiers
                identifiers = extract_identifiers(submission)
                email = identifiers['email']
                
                page_url = submission.get('pageUrl') or "(unknown)"
                
                if email:
                    submissions_since_cutoff_with_email += 1
                    email_normalized = email.strip().lower()
                    since_submission_emails_set.add(email_normalized)
                    form_unique_emails_since_cutoff.add(email_normalized)
                    
                    # Track first seen
                    if email_normalized not in email_first_seen:
                        email_first_seen[email_normalized] = {
                            'formGuid': form_id,
                            'formName': form_name,
                            'submittedAt': submitted_at_ms,
                            'pageUrl': page_url
                        }
            
            if form_submissions_since_cutoff > 0:
                print(f"  {form_name} ({form_id}): {form_submissions_since_cutoff} submissions since cutoff, {len(form_unique_emails_since_cutoff)} unique emails", file=sys.stderr)
            
            per_form_stats.append({
                'formGuid': form_id,
                'formName': form_name,
                'submissions_since_cutoff': form_submissions_since_cutoff,
                'unique_emails_since_cutoff': len(form_unique_emails_since_cutoff)
            })
            
        except Exception as e:
            print(f"Warning: Failed to process form {form_id} ({form_name}): {e}", file=sys.stderr)
            per_form_stats.append({
                'formGuid': form_id,
                'formName': form_name,
                'error': str(e)
            })
    
    print(f"\nCollection complete:", file=sys.stderr)
    print(f"  Total submissions since cutoff: {total_submissions_since_cutoff}", file=sys.stderr)
    print(f"  Submissions with email since cutoff: {submissions_since_cutoff_with_email}", file=sys.stderr)
    print(f"  Unique submission emails since cutoff: {len(since_submission_emails_set)}", file=sys.stderr)
    
    if total_submissions_since_cutoff == 0:
        print("\nWarning: No submissions found since cutoff date.", file=sys.stderr)
        # Still write files with empty data
    
    # Step 3: Build NEW portal contact email index
    print("\nStep 2: Building NEW portal contact email index...", file=sys.stderr)
    new_contact_emails_set, index_summary = fetch_new_portal_contact_email_index(new_token, timeout=timeout)
    
    # Step 4: Compute missing contacts
    print("\nStep 3: Computing missing contacts...", file=sys.stderr)
    present_emails_since = sorted(since_submission_emails_set & new_contact_emails_set)
    missing_emails_since = sorted(since_submission_emails_set - new_contact_emails_set)
    
    # Calculate percentages
    if len(since_submission_emails_set) > 0:
        percent_present = (len(present_emails_since) / len(since_submission_emails_set) * 100)
        percent_missing = (len(missing_emails_since) / len(since_submission_emails_set) * 100)
    else:
        percent_present = 0.0
        percent_missing = 0.0
    
    # Step 5: Print console summary
    print("\n" + "=" * 60, file=sys.stderr)
    print("RESULTS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Cutoff date: {since_date_str} 00:00:00 America/Toronto (epoch ms: {cutoff_ms})", file=sys.stderr)
    print(f"Total submissions since cutoff: {total_submissions_since_cutoff}", file=sys.stderr)
    print(f"Unique submission emails since cutoff: {len(since_submission_emails_set)}", file=sys.stderr)
    print(f"Unique submission emails present in NEW portal: {len(present_emails_since)}", file=sys.stderr)
    print(f"Unique submission emails missing in NEW portal: {len(missing_emails_since)}", file=sys.stderr)
    print(f"Percent present: {percent_present:.2f}%", file=sys.stderr)
    print(f"Percent missing: {percent_missing:.2f}%", file=sys.stderr)
    
    if len(missing_emails_since) > 0:
        print(f"\nContacts to migrate/create since November: {len(missing_emails_since)} unique emails", file=sys.stderr)
    
    # Step 6: Write output files
    # A) JSON
    json_output = {
        'cutoff': {
            'since_date': since_date_str,
            'timezone': 'America/Toronto',
            'cutoff_ms': cutoff_ms
        },
        'totals': {
            'submissions_since_cutoff': total_submissions_since_cutoff,
            'unique_submission_emails_since_cutoff': len(since_submission_emails_set),
            'unique_emails_present_in_new_portal': len(present_emails_since),
            'unique_emails_missing_in_new_portal': len(missing_emails_since),
            'percent_present': round(percent_present, 2),
            'percent_missing': round(percent_missing, 2)
        },
        'missing_emails': missing_emails_since,
        'per_form': [s for s in per_form_stats if 'error' not in s]
    }
    
    json_path = './out/since_migrate_analysis.json'
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(json_output, f, indent=2, ensure_ascii=False)
    
    print(f"\nJSON results written to: {json_path}", file=sys.stderr)
    
    # B) CSV
    csv_path = './out/since_migrate_missing_contacts.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['email', 'first_seen_formGuid', 'first_seen_formName', 'first_seen_submittedAt', 'first_seen_pageUrl'])
        
        for email in missing_emails_since:
            first_seen = email_first_seen.get(email, {})
            writer.writerow([
                email,
                first_seen.get('formGuid', ''),
                first_seen.get('formName', ''),
                first_seen.get('submittedAt', ''),
                first_seen.get('pageUrl', '')
            ])
    
    print(f"CSV results written to: {csv_path}", file=sys.stderr)


def get_portal_details(token: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Get portal identity details using account-info v3 API.
    
    Returns dict with portalId, companyName, timeZone, domain, etc. plus raw keys list.
    """
    url = f"{BASE_URL}/account-info/v3/details"
    
    try:
        response = hubspot_get(url, params=None, token=token, timeout=timeout)
        
        # Extract common fields
        details = {}
        details['portalId'] = response.get('portalId') or response.get('portal_id')
        details['timeZone'] = response.get('timeZone') or response.get('timezone')
        details['companyName'] = response.get('companyName') or response.get('company_name')
        details['domain'] = response.get('domain')
        details['name'] = response.get('name')
        details['user'] = response.get('user')
        
        # Include all other fields
        for key, value in response.items():
            if key not in details:
                details[key] = value
        
        # Add raw keys list for debugging
        details['keys'] = list(response.keys())
        
        return details
    except Exception as e:
        return {
            'error': str(e),
            'keys': []
        }


def count_contacts_search_total(token: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Get total contact count using search API (authoritative total).
    
    Returns dict with: search_total, status, error
    """
    url = f"{BASE_URL}/crm/v3/objects/contacts/search"
    
    payload = {
        'filterGroups': [],
        'limit': 1,
        'properties': ['email']
    }
    
    try:
        response = hubspot_post(url, payload, token=token, timeout=timeout)
        
        search_total = response.get('total')
        
        return {
            'search_total': search_total,
            'status': 200,
            'error': None
        }
    except Exception as e:
        # Try to extract status from exception if possible
        status = 0
        error_msg = str(e)
        
        if hasattr(e, 'code'):
            status = e.code
        elif isinstance(e, HTTPError):
            status = e.code
        
        return {
            'search_total': None,
            'status': status,
            'error': error_msg
        }


def fetch_contact_email_set(token: str, label: str, timeout: int = DEFAULT_TIMEOUT, track_contact_ids: bool = False) -> Dict[str, Any]:
    """
    Fetch all contacts and build a set of normalized emails.
    
    Also tracks email -> contact_id mapping if track_contact_ids=True.
    
    Returns dict with:
    - contacts_total: total contacts counted
    - emails_with_value: contacts that have non-empty email
    - unique_emails: count of unique normalized emails
    - email_set: set of normalized emails (not serialized in return)
    - email_to_contact_id: dict mapping email -> contact_id (if track_contact_ids=True)
    - pages_fetched: number of pages fetched
    """
    pages_fetched = 0
    contacts_total = 0
    emails_with_value = 0
    email_set = set()
    email_to_contact_id = {}  # email -> contact_id (first seen)
    after = None
    
    print(f"\nBuilding email set for {label} portal...", file=sys.stderr)
    
    while True:
        params = {
            'limit': 100,
            'properties': 'email'
        }
        
        if after is not None:
            params['after'] = after
        
        url = f"{BASE_URL}/crm/v3/objects/contacts"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        pages_fetched += 1
        
        results = response.get('results', [])
        if not results:
            break
        
        contacts_total += len(results)
        
        # Process each contact
        for contact in results:
            contact_id = contact.get('id')
            email_raw = contact.get('properties', {}).get('email')
            
            if email_raw and email_raw.strip():
                emails_with_value += 1
                email_normalized = normalize_email(email_raw)
                
                if email_normalized:
                    email_set.add(email_normalized)
                    
                    # Track email -> contact_id mapping (first seen)
                    if track_contact_ids and email_normalized not in email_to_contact_id:
                        email_to_contact_id[email_normalized] = contact_id
        
        # Progress update every 10 pages
        if pages_fetched % 10 == 0:
            print(f"  {label}: pages={pages_fetched} contacts={contacts_total} emails_with_value={emails_with_value} unique_emails={len(email_set)}", file=sys.stderr)
        
        # Check for next page
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after = next_page.get('after')
        
        if not next_after:
            break
        
        after = next_after
    
    print(f"  {label}: pages={pages_fetched} contacts={contacts_total} emails_with_value={emails_with_value} unique_emails={len(email_set)}", file=sys.stderr)
    
    result = {
        'contacts_total': contacts_total,
        'emails_with_value': emails_with_value,
        'unique_emails': len(email_set),
        'pages_fetched': pages_fetched
    }
    
    # Store email_set and email_to_contact_id in result (for in-memory use)
    result['_email_set'] = email_set
    if track_contact_ids:
        result['_email_to_contact_id'] = email_to_contact_id
    
    return result


def count_contacts_in_portal(token: str, label: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Count total contacts in a portal using CRM v3 objects list endpoint.
    
    Counts ALL contacts (does not filter by email presence).
    Only emails_with_value is tracked separately.
    
    Returns dict with: label, pages_fetched, contacts_count, emails_with_value
    """
    pages_fetched = 0
    contacts_count = 0
    emails_present_count = 0
    after = None
    
    print(f"\nCounting contacts in {label} portal (list)...", file=sys.stderr)
    
    while True:
        params = {
            'limit': 100,
            'properties': 'email'
        }
        
        if after is not None:
            params['after'] = after
        
        url = f"{BASE_URL}/crm/v3/objects/contacts"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        pages_fetched += 1
        
        results = response.get('results', [])
        if not results:
            break
        
        # Count ALL contacts (do not filter)
        contacts_count += len(results)
        
        # Count contacts with email value (separate metric)
        for contact in results:
            email = contact.get('properties', {}).get('email')
            if email and email.strip():
                emails_present_count += 1
        
        # Progress update every 10 pages
        if pages_fetched % 10 == 0:
            print(f"  {label}: pages={pages_fetched} contacts={contacts_count} emails_with_value={emails_present_count}", file=sys.stderr)
        
        # Check for next page
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after = next_page.get('after')
        
        if not next_after:
            break
        
        after = next_after
    
    print(f"  {label}: pages={pages_fetched} contacts={contacts_count} emails_with_value={emails_present_count}", file=sys.stderr)
    
    return {
        'label': label,
        'pages_fetched': pages_fetched,
        'contacts_count': contacts_count,
        'emails_with_value': emails_present_count
    }


def search_contact_id_by_email(token: str, email: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Search for a contact by email using POST /crm/v3/objects/contacts/search.
    
    Returns tuple: (contact_id or None, raw_search_response_json)
    """
    url = f"{BASE_URL}/crm/v3/objects/contacts/search"
    
    payload = {
        "filterGroups": [{
            "filters": [{
                "propertyName": "email",
                "operator": "EQ",
                "value": email
            }]
        }],
        "limit": 1,
        "properties": ["email"]
    }
    
    try:
        response = hubspot_post(url, payload, token=token, timeout=timeout)
        
        # Extract contact ID from first result
        results = response.get('results', [])
        contact_id = None
        if results:
            contact_id = results[0].get('id')
        
        return (contact_id, response)
    except Exception as e:
        # Return error response
        return (None, {'error': str(e)})


def get_contact_by_id(token: str, contact_id: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Get full contact object by ID using GET /crm/v3/objects/contacts/{id}.
    
    Returns raw contact JSON.
    """
    url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    
    # Request useful properties for note formatting
    properties = [
        'email', 'firstname', 'lastname', 'phone', 'mobilephone', 'company',
        'website', 'lifecyclestage', 'createdate', 'hs_lastmodifieddate'
    ]
    
    params = {
        'properties': ','.join(properties)
    }
    
    return hubspot_get(url, params=params, token=token, timeout=timeout)


def build_new_contact_email_set(token: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[set, Dict[str, Any]]:
    """
    Build a fast index of NEW portal contact emails.
    
    Returns: (email_set, stats_dict) where stats has pages, contacts, emails_indexed
    """
    email_set = set()
    pages_fetched = 0
    contacts_total = 0
    emails_indexed = 0
    after = None
    
    print("Building NEW contact email index...", file=sys.stderr)
    
    while True:
        params = {
            'limit': 100,
            'properties': 'email'
        }
        
        if after is not None:
            params['after'] = after
        
        url = f"{BASE_URL}/crm/v3/objects/contacts"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        pages_fetched += 1
        results = response.get('results', [])
        
        if not results:
            break
        
        contacts_total += len(results)
        
        for contact in results:
            email_raw = contact.get('properties', {}).get('email')
            if email_raw and email_raw.strip():
                email_normalized = normalize_email(email_raw)
                if email_normalized:
                    email_set.add(email_normalized)
                    emails_indexed += 1
        
        # Progress update every 1000 contacts
        if contacts_total % 1000 == 0:
            print(f"  Progress: contacts={contacts_total} emails_indexed={emails_indexed}", file=sys.stderr)
        
        # Check for next page
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after = next_page.get('after')
        
        if not next_after:
            break
        
        after = next_after
    
    print(f"  Completed: pages={pages_fetched} contacts={contacts_total} emails_indexed={emails_indexed}", file=sys.stderr)
    
    return email_set, {
        'pages': pages_fetched,
        'contacts': contacts_total,
        'emails_indexed': emails_indexed
    }


# Constants for note formatting
NORMALIZE_STAGING_URLS = True

# Phone field synonyms
PHONE_FIELD_SYNONYMS = ["phone", "mobilephone", "phone_number", "phonenumber", "number", "tel", "telephone"]

# Company field synonyms
COMPANY_FIELD_SYNONYMS = ["company", "company_name"]


# Dedupe helper functions
def normalize_url_for_dedupe(url: str) -> str:
    """
    Normalize URL for deduplication: remove fragment, optionally normalize staging, strip trailing slash.
    """
    if not url:
        return url
    
    try:
        parsed = urlparse(url)
        # Remove fragment
        netloc = parsed.netloc.replace("staging.arrsys.com", "arrsys.com") if NORMALIZE_STAGING_URLS else parsed.netloc
        path = parsed.path.rstrip('/')  # Strip trailing slash
        normalized = urlunparse((
            parsed.scheme,
            netloc,
            path,
            parsed.params,
            parsed.query,
            ''  # Remove fragment
        ))
        return normalized
    except Exception:
        return url


def submitted_day_ms(submitted_at_ms: Optional[int], tz: str = "America/Toronto") -> str:
    """
    Convert submittedAt (epoch milliseconds) to YYYY-MM-DD date string in specified timezone.
    Returns "(unknown)" if invalid.
    """
    if not submitted_at_ms:
        return "(unknown)"
    
    try:
        submitted_dt = datetime.fromtimestamp(submitted_at_ms / 1000.0, tz=timezone.utc)
        if ZoneInfo:
            target_tz = ZoneInfo(tz)
            submitted_dt_local = submitted_dt.astimezone(target_tz)
            return submitted_dt_local.strftime("%Y-%m-%d")
        else:
            # Fallback: use UTC date
            return submitted_dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, TypeError):
        return "(unknown)"


def normalize_value(s: Any) -> str:
    """
    Normalize a value for deduplication: convert to string, strip, collapse whitespace.
    """
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    # Collapse whitespace
    s = ' '.join(s.split())
    return s


def build_canonical_fields(values: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Build canonical field dict from submission values list.
    
    Rules:
    - Normalize field names to lowercase
    - Normalize values (strip, collapse whitespace)
    - Ignore internal keys (objectTypeId)
    - Map phone synonyms to "phone"
    - Map firstname/lastname to "name" (combine)
    - If same field appears twice, keep last non-empty value
    - Apply phone/country splitting before canonicalization
    """
    canonical = {}
    raw_fields = {}
    
    # First pass: collect raw fields
    for item in values:
        if not isinstance(item, dict):
            continue
        
        name = item.get('name', '')
        value = item.get('value', '')
        
        # Skip internal keys
        if name.lower() in ['objecttypeid']:
            continue
        
        if name:
            raw_name = name.lower()
            raw_value = normalize_value(value) if value else ''
            # Last wins for same field name
            if raw_value or raw_name not in raw_fields:
                raw_fields[raw_name] = raw_value
    
    # Second pass: apply canonicalization rules
    
    # Email (normalize consistently with compute_dedupe_keys)
    if 'email' in raw_fields:
        email_norm = normalize_email(raw_fields['email'])
        canonical['email'] = email_norm if email_norm else raw_fields['email'].lower().strip()
    
    # Name (combine firstname + lastname)
    name_parts = []
    if 'firstname' in raw_fields:
        name_parts.append(raw_fields['firstname'])
    if 'lastname' in raw_fields:
        name_parts.append(raw_fields['lastname'])
    
    if name_parts:
        canonical['name'] = normalize_value(' '.join(name_parts))
    elif 'name' in raw_fields:
        canonical['name'] = raw_fields['name']
    
    # Phone (check synonyms, handle composite)
    phone_value = None
    for phone_key in PHONE_FIELD_SYNONYMS:
        if phone_key in raw_fields:
            phone_value = raw_fields[phone_key]
            break
    
    if phone_value:
        # Check if composite (country + phone)
        country_from_phone, phone_digits = split_country_and_phone(phone_value)
        if country_from_phone and phone_digits:
            # Split detected
            canonical['phone'] = phone_digits
            # Only add country if not already present
            if 'country' not in raw_fields:
                canonical['country'] = country_from_phone
        else:
            # Not composite, extract digits
            canonical['phone'] = extract_phone_digits(phone_value)
    
    # Country (prefer explicit field)
    if 'country' in raw_fields:
        canonical['country'] = raw_fields['country']
    
    # Company (check synonyms)
    for company_key in COMPANY_FIELD_SYNONYMS:
        if company_key in raw_fields:
            canonical['company'] = raw_fields[company_key]
            break
    
    # Remaining fields (exclude already processed)
    processed_keys = {'email', 'firstname', 'lastname', 'name', 'country', 'company'}
    processed_keys.update(PHONE_FIELD_SYNONYMS)
    processed_keys.update(COMPANY_FIELD_SYNONYMS)
    
    for raw_key, raw_value in raw_fields.items():
        if raw_key not in processed_keys and raw_value:
            canonical[raw_key] = raw_value
    
    return canonical


def compute_dedupe_keys(email: str, page_url: str, submitted_at_ms: Optional[int], 
                        canonical_fields: Dict[str, str]) -> Tuple[str, str]:
    """
    Compute two dedupe keys for a submission:
    
    Returns: (strict_hash, day_key)
    - strict_hash: SHA-256 hash of {email, page, fields}
    - day_key: "{email}|{page}|{date}"
    """
    # Normalize email
    email_norm = normalize_email(email) or email.lower().strip()
    
    # Normalize page URL
    page_norm = normalize_url_for_dedupe(page_url)
    
    # Get submitted date
    submitted_date = submitted_day_ms(submitted_at_ms)
    
    # Build strict signature payload (include submitted_date to reduce collisions)
    strict_payload = {
        "email": email_norm,
        "page": page_norm,
        "fields": canonical_fields,
        "submitted_date": submitted_date
    }
    
    # Serialize with sorted keys for stability
    strict_json = json.dumps(strict_payload, sort_keys=True, separators=(',', ':'))
    strict_bytes = strict_json.encode('utf-8')
    strict_hash = hashlib.sha256(strict_bytes).hexdigest()
    
    # Build day key
    day_key = f"{email_norm}|{page_norm}|{submitted_date}"
    
    return (strict_hash, day_key)


def normalize_url(url: str) -> str:
    """
    Normalize URL: remove fragment, optionally replace staging.arrsys.com with arrsys.com.
    """
    if not url:
        return url
    
    try:
        parsed = urlparse(url)
        # Remove fragment
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc.replace("staging.arrsys.com", "arrsys.com") if NORMALIZE_STAGING_URLS else parsed.netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            ''  # Remove fragment
        ))
        return normalized
    except Exception:
        return url


def extract_phone_digits(s: str) -> str:
    """
    Extract phone digits from string, keeping leading + if present.
    """
    if not s:
        return ""
    
    s = s.strip()
    has_plus = s.startswith('+')
    
    # Extract digits
    digits = ''.join(c for c in s if c.isdigit())
    
    if has_plus and digits:
        return '+' + digits
    return digits


def split_country_and_phone(s: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Split composite country+phone string into (country, phone).
    Returns (None, None) if not a composite.
    
    Handles patterns like "India (भारत)9099903323" or "India 99999999999"
    """
    if not s:
        return (None, None)
    
    s = s.strip()
    
    # Check if string contains both letters (including Unicode) and digits
    has_letters = any(c.isalpha() for c in s)
    has_digits = any(c.isdigit() for c in s)
    
    if not (has_letters and has_digits):
        return (None, None)
    
    # Extract phone digits (keep leading + if present)
    phone = extract_phone_digits(s)
    
    # Extract country: remove all digits and phone-related punctuation
    country = s
    # Remove digits and common phone characters
    for char in '0123456789+-()':
        country = country.replace(char, ' ')
    
    # Extract words: sequences of letters (including Unicode) and spaces
    words = []
    current_word = []
    for char in country:
        # Include letters (including Unicode) and spaces
        if char.isalpha() or char.isspace():
            if char.isalpha():
                current_word.append(char)
            elif current_word:  # Space after a word
                words.append(''.join(current_word))
                current_word = []
        elif current_word:  # Non-letter, non-space after a word
            words.append(''.join(current_word))
            current_word = []
    
    if current_word:
        words.append(''.join(current_word))
    
    country = ' '.join(words).strip()
    
    # Validate: phone should have at least 7 digits, country at least 2 chars
    phone_digits_only = phone.replace('+', '')
    if len(phone_digits_only) >= 7 and len(country) >= 2:
        return (country, phone)
    
    return (None, None)


def normalize_name(s: str) -> str:
    """
    Normalize name: collapse whitespace, title-case if all caps or all lowercase.
    """
    if not s:
        return s
    
    # Collapse multiple spaces
    s = ' '.join(s.split())
    
    # Check if all uppercase or all lowercase (and has at least 2 letters)
    letters_only = ''.join(c for c in s if c.isalpha())
    if len(letters_only) >= 2:
        if letters_only == letters_only.upper() or letters_only == letters_only.lower():
            # Title case each word
            words = s.split()
            title_words = []
            for w in words:
                if len(w) == 0:
                    continue
                elif len(w) == 1:
                    title_words.append(w.upper())
                else:
                    # First char upper, rest lower
                    title_words.append(w[0].upper() + w[1:].lower())
            return ' '.join(title_words)
    
    return s


def _extract_canonical_fields_from_submission(submission_obj: Dict[str, Any]) -> Tuple[Dict[str, str], str, str]:
    """
    Extract canonical fields from submission and compute normalized page URL and submitted date.
    
    Returns: (canonical_fields_dict, normalized_page_url, submitted_date_str)
    """
    # Page URL normalization
    page_url_raw = submission_obj.get('pageUrl', '')
    page_url_normalized = normalize_url(page_url_raw) if page_url_raw else ''
    
    # Submitted date
    submitted_at_ms = submission_obj.get('submittedAt')
    submitted_date_str = "(unknown)"
    if submitted_at_ms:
        try:
            # Convert epoch milliseconds to datetime
            submitted_dt = datetime.fromtimestamp(submitted_at_ms / 1000.0, tz=timezone.utc)
            # Convert to America/Toronto
            if ZoneInfo:
                toronto_tz = ZoneInfo("America/Toronto")
                submitted_dt_toronto = submitted_dt.astimezone(toronto_tz)
                submitted_date_str = submitted_dt_toronto.strftime("%Y-%m-%d")
            else:
                # Fallback: use UTC date
                submitted_date_str = submitted_dt.strftime("%Y-%m-%d")
        except (ValueError, OSError, TypeError):
            pass
    
    # Step A: Build raw_fields dict from submission.values
    values = submission_obj.get('values', [])
    raw_fields = {}
    
    if isinstance(values, list):
        for item in values:
            if isinstance(item, dict):
                name = item.get('name', '')
                value = item.get('value', '')
                if name:
                    raw_name = name.lower()
                    raw_value = str(value).strip() if value else ''
                    # First wins (keep existing behavior)
                    if raw_name not in raw_fields and raw_value:
                        raw_fields[raw_name] = raw_value
    elif isinstance(values, dict):
        for name, value in values.items():
            if name:
                raw_name = name.lower()
                raw_value = str(value).strip() if value else ''
                if raw_name not in raw_fields and raw_value:
                    raw_fields[raw_name] = raw_value
    
    # Step B: Canonicalization
    canonical = {}
    processed_raw_keys = set()
    
    # Email
    if 'email' in raw_fields:
        canonical['email'] = raw_fields['email']
        processed_raw_keys.add('email')
    
    # Name (combine firstname + lastname, or use name field)
    name_parts = []
    if 'firstname' in raw_fields:
        name_parts.append(raw_fields['firstname'])
        processed_raw_keys.add('firstname')
    if 'lastname' in raw_fields:
        name_parts.append(raw_fields['lastname'])
        processed_raw_keys.add('lastname')
    
    if name_parts:
        full_name = ' '.join(name_parts)
        canonical['name'] = normalize_name(full_name)
    elif 'name' in raw_fields:
        canonical['name'] = normalize_name(raw_fields['name'])
        processed_raw_keys.add('name')
    
    # Phone (check synonyms, handle composite)
    phone_value = None
    phone_raw_key = None
    for phone_key in PHONE_FIELD_SYNONYMS:
        if phone_key in raw_fields:
            phone_value = raw_fields[phone_key]
            phone_raw_key = phone_key
            processed_raw_keys.add(phone_key)
            break
    
    # Check if phone value is composite (country + phone)
    country_from_phone = None
    if phone_value:
        country_from_phone, phone_digits = split_country_and_phone(phone_value)
        if country_from_phone and phone_digits:
            # Split detected
            phone_value = phone_digits
            # Only use country_from_phone if no explicit country field
            if 'country' not in raw_fields:
                canonical['country'] = country_from_phone
        else:
            # Not composite, just extract digits
            phone_value = extract_phone_digits(phone_value)
    
    if phone_value:
        canonical['phone'] = phone_value
    
    # Country (prefer explicit field, else use from phone split)
    if 'country' in raw_fields:
        canonical['country'] = raw_fields['country']
        processed_raw_keys.add('country')
    elif country_from_phone and 'country' not in canonical:
        canonical['country'] = country_from_phone
    
    # Company (check synonyms)
    company_value = None
    for company_key in COMPANY_FIELD_SYNONYMS:
        if company_key in raw_fields:
            company_value = raw_fields[company_key]
            processed_raw_keys.add(company_key)
            break
    
    if company_value:
        canonical['company'] = company_value
    
    # Remaining fields
    remaining_fields = {}
    for raw_key, raw_value in raw_fields.items():
        if raw_key not in processed_raw_keys:
            # Title case label
            label = raw_key.replace('_', ' ').title()
            remaining_fields[label] = raw_value
    
    return (canonical, remaining_fields, page_url_normalized, submitted_date_str)


def submission_to_note_text(form_name: str, form_guid: str, submission_obj: Dict[str, Any], conversion_id: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
    """
    Convert a form submission to a plain text note body (for backward compatibility checks).
    
    Returns: (note_body_text, derived_dict)
    """
    canonical, remaining_fields, page_url_normalized, submitted_date_str = _extract_canonical_fields_from_submission(submission_obj)
    
    lines = []
    lines.append("Website form submission")
    lines.append("")
    
    if page_url_normalized:
        lines.append(f"Page: {page_url_normalized}")
    else:
        lines.append("Page: (unknown)")
    
    lines.append(f"Form: {form_name} ({form_guid})")
    lines.append(f"Submitted: {submitted_date_str}")
    lines.append("")
    lines.append("Responses:")
    
    # Build response lines in priority order
    response_lines = []
    
    # 1) Email
    if 'email' in canonical:
        response_lines.append(f"• Email: {canonical['email']}")
    
    # 2) Phone
    if 'phone' in canonical:
        response_lines.append(f"• Phone: {canonical['phone']}")
    
    # 3) Name
    if 'name' in canonical:
        response_lines.append(f"• Name: {canonical['name']}")
    
    # 4) Company
    if 'company' in canonical:
        response_lines.append(f"• Company: {canonical['company']}")
    
    # 5) Country
    if 'country' in canonical:
        response_lines.append(f"• Country: {canonical['country']}")
    
    # 6) Remaining fields (sorted alphabetically)
    remaining_sorted = sorted(remaining_fields.items(), key=lambda x: x[0].lower())
    for label, value in remaining_sorted:
        response_lines.append(f"• {label}: {value}")
    
    # Add "no additional fields" message if only email exists
    if len(response_lines) == 1 and 'email' in canonical:
        response_lines.append("• (No additional fields captured)")
    
    lines.extend(response_lines)
    
    # Add durable marker for duplicate detection (plain text format)
    if conversion_id:
        submission_key = make_submission_key(form_guid, conversion_id)
        lines.append("")
        lines.append(f"hs_form_submission_key={submission_key}")
    
    note_body = '\n'.join(lines)
    
    # Build derived dict
    derived = {
        'normalized_pageUrl': page_url_normalized,
        'submitted_date': submitted_date_str,
        'normalized_fields': canonical.copy()
    }
    
    return (note_body, derived)


def submission_to_note_html(form_name: str, form_guid: str, submission_obj: Dict[str, Any], conversion_id: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
    """
    Convert a form submission to an HTML note body string.
    
    Returns: (note_body_html, derived_dict) where derived_dict contains normalized fields.
    
    Format (HTML):
    <strong>Website form submission</strong><br><br>
    <strong>Page:</strong> https://...<br>
    <strong>Form:</strong> ...<br>
    <strong>Submitted:</strong> YYYY-MM-DD<br><br>
    <strong>Responses:</strong>
    <ul>
      <li><strong>Email:</strong> ...</li>
      ...
    </ul>
    <br><br>
    <!-- hs_form_submission: formGuid=<GUID> conversionId=<CID> -->
    """
    canonical, remaining_fields, page_url_normalized, submitted_date_str = _extract_canonical_fields_from_submission(submission_obj)
    
    # Build HTML parts
    html_parts = []
    
    # Title
    html_parts.append("<strong>Website form submission</strong><br><br>")
    
    # Page
    if page_url_normalized:
        html_parts.append(f"<strong>Page:</strong> {html_escape(page_url_normalized)}<br>")
    else:
        html_parts.append("<strong>Page:</strong> (unknown)<br>")
    
    # Form (escape form name and GUID)
    form_name_escaped = html_escape(form_name)
    form_guid_escaped = html_escape(form_guid)
    html_parts.append(f"<strong>Form:</strong> {form_name_escaped} ({form_guid_escaped})<br>")
    
    # Submitted date
    submitted_date_escaped = html_escape(submitted_date_str)
    html_parts.append(f"<strong>Submitted:</strong> {submitted_date_escaped}<br><br>")
    
    # Responses header
    html_parts.append("<strong>Responses:</strong>")
    html_parts.append("<ul>")
    
    # Build response items in priority order
    response_items = []
    
    # 1) Email
    if 'email' in canonical:
        email_escaped = html_escape(canonical['email'])
        response_items.append(f"<li><strong>Email:</strong> {email_escaped}</li>")
    
    # 2) Phone
    if 'phone' in canonical:
        phone_escaped = html_escape(canonical['phone'])
        response_items.append(f"<li><strong>Phone:</strong> {phone_escaped}</li>")
    
    # 3) Name
    if 'name' in canonical:
        name_escaped = html_escape(canonical['name'])
        response_items.append(f"<li><strong>Name:</strong> {name_escaped}</li>")
    
    # 4) Company
    if 'company' in canonical:
        company_escaped = html_escape(canonical['company'])
        response_items.append(f"<li><strong>Company:</strong> {company_escaped}</li>")
    
    # 5) Country
    if 'country' in canonical:
        country_escaped = html_escape(canonical['country'])
        response_items.append(f"<li><strong>Country:</strong> {country_escaped}</li>")
    
    # 6) Remaining fields (sorted alphabetically)
    remaining_sorted = sorted(remaining_fields.items(), key=lambda x: x[0].lower())
    for label, value in remaining_sorted:
        label_escaped = html_escape(label)
        value_escaped = html_escape(value)
        response_items.append(f"<li><strong>{label_escaped}:</strong> {value_escaped}</li>")
    
    # Add "no additional fields" message if only email exists
    if len(response_items) == 1 and 'email' in canonical:
        response_items.append("<li><strong>(No additional fields captured)</strong></li>")
    
    html_parts.extend(response_items)
    html_parts.append("</ul>")
    
    # Add durable marker for duplicate detection (visible-safe, survives HubSpot storage)
    if conversion_id:
        submission_key = make_submission_key(form_guid, conversion_id)
        # Use hidden span (font-size:0, color:transparent) instead of HTML comment
        marker = f'<br><br><span style="font-size:0; color:transparent;">hs_form_submission_key={submission_key}</span>'
        html_parts.append(marker)
    
    note_body_html = ''.join(html_parts)
    
    # Build derived dict
    derived = {
        'normalized_pageUrl': page_url_normalized,
        'submitted_date': submitted_date_str,
        'normalized_fields': canonical.copy()
    }
    
    return (note_body_html, derived)


def make_submission_key(form_guid: str, conversion_id: str) -> str:
    """Create submission key from formGuid and conversionId."""
    return f"{form_guid}:{conversion_id}"


def extract_marker_key(note_body: str) -> Optional[str]:
    """
    Extract submission_key from note body marker.
    
    Returns: submission_key (formGuid:conversionId) if marker found, None otherwise.
    
    Supports:
    - New durable marker: hs_form_submission_key=FORMGUID:CONVERSIONID (in hidden span or plain text)
    - Old marker format: hs_form_submission: formGuid=... conversionId=... (backward compatibility)
    """
    if not note_body:
        return None
    
    # Try new durable marker format first: hs_form_submission_key=FORMGUID:CONVERSIONID
    # Pattern matches UUIDs with hyphens: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    new_pattern = r'hs_form_submission_key=([0-9a-fA-F\-]+:[0-9a-fA-F\-]+)'
    match = re.search(new_pattern, note_body)
    if match:
        return match.group(1)  # Already in formGuid:conversionId format
    
    # Fallback: old marker format (backward compatibility)
    # Pattern for HTML comment: <!-- hs_form_submission: formGuid=... conversionId=... -->
    # Pattern for plain text: [hs_form_submission: formGuid=... conversionId=...]
    old_pattern = r'hs_form_submission:\s*formGuid=([^\s>]+)\s+conversionId=([^\s>]+)'
    match = re.search(old_pattern, note_body)
    if match:
        form_guid = match.group(1)
        conversion_id = match.group(2)
        return f"{form_guid}:{conversion_id}"
    
    return None


def extract_semantic_key(note_body: str) -> Optional[str]:
    """
    Extract semantic key from note body (fallback when marker missing).
    
    Returns: semantic_key as "{page}|{date}|{email}" or None if parsing fails.
    
    Supports both HTML and plain text formats.
    """
    if not note_body:
        return None
    
    page_url = None
    submitted_date = None
    email = None
    
    # Detect format
    is_html = '<strong>' in note_body or '<br>' in note_body or '<ul>' in note_body
    
    if is_html:
        # HTML format parsing
        # Extract Page: <strong>Page:</strong> URL<br>
        page_match = re.search(r'<strong>Page:</strong>\s*([^<]+)<br>', note_body)
        if page_match:
            page_url = page_match.group(1).strip()
            # Remove "(unknown)" if present
            if page_url == "(unknown)":
                page_url = None
        
        # Extract Submitted: <strong>Submitted:</strong> YYYY-MM-DD<br>
        date_match = re.search(r'<strong>Submitted:</strong>\s*([0-9]{4}-[0-9]{2}-[0-9]{2})', note_body)
        if date_match:
            submitted_date = date_match.group(1).strip()
        
        # Extract Email: <li><strong>Email:</strong> value</li>
        email_match = re.search(r'<li><strong>Email:</strong>\s*([^<]+)</li>', note_body)
        if email_match:
            email = email_match.group(1).strip()
    else:
        # Plain text format parsing
        lines = note_body.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith('Page:'):
                page_url = line[5:].strip()  # Remove "Page:"
                if page_url == "(unknown)":
                    page_url = None
            elif line.startswith('Submitted:'):
                date_match = re.search(r'Submitted:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})', line)
                if date_match:
                    submitted_date = date_match.group(1).strip()
            elif line.startswith('• Email:'):
                email = line[8:].strip()  # Remove "• Email:"
    
    # Normalize values
    if page_url:
        # Use same normalization as normalize_url_for_dedupe for consistency
        page_url = normalize_url_for_dedupe(page_url)
    
    if email:
        email = email.lower().strip()
    
    # Build semantic key if we have all three
    if page_url and submitted_date and email:
        return f"{page_url}|{submitted_date}|{email}"
    
    return None


def parse_date_to_toronto_midnight(date_str: str) -> Optional[int]:
    """
    Parse YYYY-MM-DD date string to epoch milliseconds at midnight America/Toronto.
    Returns None if parsing fails.
    """
    try:
        parts = date_str.split('-')
        if len(parts) != 3:
            return None
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
        
        if not ZoneInfo:
            print("Error: zoneinfo not available. Please use Python 3.9+ or install backports.zoneinfo", file=sys.stderr)
            return None
        
        # Create datetime at midnight in America/Toronto
        cutoff_local = datetime(year, month, day, 0, 0, 0, tzinfo=ZoneInfo("America/Toronto"))
        # Convert to UTC
        cutoff_utc = cutoff_local.astimezone(timezone.utc)
        # Convert to epoch milliseconds
        cutoff_ms = int(cutoff_utc.timestamp() * 1000)
        return cutoff_ms
    except (ValueError, OSError, TypeError) as e:
        print(f"Error parsing date {date_str}: {e}", file=sys.stderr)
        return None


def get_form_guid(form: Dict[str, Any]) -> Optional[str]:
    """
    Robust getter for form GUID from form object.
    Tries: id, guid, formGuid
    Validates format (must contain '-' for UUID format).
    """
    form_guid = form.get('id') or form.get('guid') or form.get('formGuid')
    
    if not form_guid:
        return None
    
    # Validate GUID format (should contain '-' for UUID)
    if '-' not in str(form_guid):
        return None
    
    return str(form_guid)


def find_first_submission_with_email_in_new(forms: List[Dict[str, Any]], new_email_set: set,
                                             old_token: str, restrict_form_guid: Optional[str] = None,
                                             max_scan: int = 5000, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[Dict[str, Any]], int]:
    """
    Find the first submission whose email exists in NEW portal contact email set.
    
    Returns: (target_email, formGuid, formName, submission_obj, scanned_count)
    or (None, None, None, None, scanned_count) if not found
    """
    scanned_count = 0
    
    # Ensure max_scan is positive
    if max_scan <= 0:
        max_scan = 5000
    
    print(f"Submission scan: max_scan={max_scan}", file=sys.stderr)
    
    # Filter forms if restrict_form_guid provided
    forms_to_check = forms
    if restrict_form_guid:
        forms_to_check = []
        for f in forms:
            f_guid = get_form_guid(f)
            if f_guid == restrict_form_guid:
                forms_to_check.append(f)
        if not forms_to_check:
            print(f"  Warning: No forms match restrict_form_guid={restrict_form_guid}", file=sys.stderr)
            return (None, None, None, None, 0)
    
    # Sort forms deterministically (by name, then guid for stability)
    forms_sorted = sorted(forms_to_check, key=lambda f: (f.get('name', ''), get_form_guid(f) or ''))
    
    print(f"Checking {len(forms_sorted)} forms for submissions...", file=sys.stderr)
    
    # Debug: print first form structure
    if forms_sorted:
        first_form = forms_sorted[0]
        first_form_keys = list(first_form.keys())
        first_form_guid = get_form_guid(first_form)
        first_form_name = first_form.get('name', 'Unknown')
        print(f"  First form debug: keys={first_form_keys}, guid={first_form_guid}, name={first_form_name}", file=sys.stderr)
    
    print("Scanning submissions to find an email present in NEW contacts...", file=sys.stderr)
    
    for form_idx, form in enumerate(forms_sorted):
        form_guid = get_form_guid(form)
        form_name = form.get('name', 'Unknown')
        
        if not form_guid:
            form_keys = list(form.keys())
            print(f"  Warning: skipping form missing guid/id. keys={form_keys}, name={form_name}", file=sys.stderr)
            continue
        
        print(f"  Checking form {form_idx + 1}/{len(forms_sorted)}: {form_name} ({form_guid})", file=sys.stderr)
        
        # Iterate submissions for this form
        yielded_any = False
        try:
            for submission in iter_form_submissions_old(form_guid, old_token, limit=50, timeout=timeout):
                yielded_any = True
                scanned_count += 1
                
                # Extract identifiers from submission
                ids = extract_identifiers(submission)
                submission_email = ids.get('email')
                
                # Check if submission email exists in NEW email set
                if submission_email:
                    submission_email_normalized = normalize_email(submission_email)
                    if submission_email_normalized and submission_email_normalized in new_email_set:
                        print(f"Found candidate email from submission that exists in NEW contacts: {submission_email_normalized}", file=sys.stderr)
                        return (submission_email_normalized, form_guid, form_name, submission, scanned_count)
                
                # Progress update every 500 scanned
                if scanned_count % 500 == 0:
                    print(f"  scanned={scanned_count}, current_form={form_name}", file=sys.stderr)
                
                # Check if we've exceeded max_scan
                if scanned_count >= max_scan:
                    print(f"  Reached max_scan={max_scan}, stopping", file=sys.stderr)
                    return (None, None, None, None, scanned_count)
            
            if not yielded_any:
                print(f"  Note: form {form_guid} ({form_name}) returned 0 submissions (ok for low volume).", file=sys.stderr)
            
            # Debug: if first form and scanned is still 0, print error
            if form_idx == 0 and scanned_count == 0:
                print(f"  DEBUG ERROR: First form {form_guid} returned 0 submissions. URL would be: {BASE_URL}/form-integrations/v1/submissions/forms/{form_guid}", file=sys.stderr)
                
        except Exception as e:
            # Continue to next form if this one fails
            print(f"  Warning: Error scanning form {form_guid} ({form_name}): {e}", file=sys.stderr)
            import traceback
            print(f"  Traceback: {traceback.format_exc()}", file=sys.stderr)
            continue
    
    return (None, None, None, None, scanned_count)


def find_one_submission_for_email_old(target_email: str, forms: List[Dict[str, Any]], 
                                       old_token: str, restrict_form_guid: Optional[str] = None,
                                       max_scan: int = 5000, timeout: int = DEFAULT_TIMEOUT) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]], int]:
    """
    Find a form submission in OLD portal that contains target_email.
    
    Iterates forms deterministically, streams submissions, stops at first match.
    
    Returns: (formGuid, formName, submission_obj, scanned_count) or (None, None, None, scanned_count) if not found
    """
    scanned_count = 0
    
    # Ensure max_scan is positive
    if max_scan <= 0:
        max_scan = 5000
    
    # Filter forms if restrict_form_guid provided
    forms_to_check = forms
    if restrict_form_guid:
        forms_to_check = []
        for f in forms:
            f_guid = get_form_guid(f)
            if f_guid == restrict_form_guid:
                forms_to_check.append(f)
        if not forms_to_check:
            return (None, None, None, 0)
    
    # Sort forms deterministically (by guid for stability)
    forms_sorted = sorted(forms_to_check, key=lambda f: (get_form_guid(f) or '', f.get('name', '')))
    
    for form in forms_sorted:
        form_guid = get_form_guid(form)
        form_name = form.get('name', 'Unknown')
        
        if not form_guid:
            form_keys = list(form.keys())
            print(f"  Warning: skipping form missing guid/id. keys={form_keys}, name={form_name}", file=sys.stderr)
            continue
        
        # Iterate submissions for this form
        yielded_any = False
        try:
            for submission in iter_form_submissions_old(form_guid, old_token, limit=50, timeout=timeout):
                yielded_any = True
                scanned_count += 1
                
                # Extract identifiers from submission
                ids = extract_identifiers(submission)
                submission_email = ids.get('email')
                
                # Compare normalized emails (both should already be normalized, but ensure)
                if submission_email:
                    submission_email_normalized = normalize_email(submission_email)
                    if submission_email_normalized == target_email:
                        return (form_guid, form_name, submission, scanned_count)
                
                # Check if we've exceeded max_scan
                if scanned_count >= max_scan:
                    return (None, None, None, scanned_count)
            
            if not yielded_any:
                print(f"  Note: form {form_guid} ({form_name}) returned 0 submissions.", file=sys.stderr)
                
        except Exception as e:
            # Continue to next form if this one fails
            print(f"  Warning: Error scanning form {form_guid} ({form_name}): {e}", file=sys.stderr)
            continue
    
    return (None, None, None, scanned_count)


def run_get_one(old_token: str, new_token: str, email: Optional[str], portal: str, 
                form_guid: Optional[str], max_scan: int, timeout: int):
    """
    Print raw HubSpot API response for a single contact that exists in both portals.
    Starts from form submissions to find an email that exists in NEW portal contacts.
    """
    print("\n" + "=" * 60, file=sys.stderr)
    print("GET-ONE CONTACT (RAW)", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Get portal details for verification
    old_details = get_portal_details(old_token, timeout=timeout)
    new_details = get_portal_details(new_token, timeout=timeout)
    
    old_portal_id = old_details.get('portalId') or old_details.get('portal_id') or 'unknown'
    new_portal_id = new_details.get('portalId') or new_details.get('portal_id') or 'unknown'
    
    print(f"OLD portalId: {old_portal_id}", file=sys.stderr)
    print(f"NEW portalId: {new_portal_id}", file=sys.stderr)
    print("", file=sys.stderr)
    
    # Ensure max_scan is positive
    if max_scan <= 0:
        max_scan = 5000
    
    # Fetch forms (needed for both modes)
    print("Fetching forms...", file=sys.stderr)
    live_forms = list_forms(archived=False, token=old_token, timeout=timeout)
    archived_forms = list_forms(archived=True, token=old_token, timeout=timeout)
    
    # De-duplicate by form id
    forms_dict = {}
    for form in live_forms + archived_forms:
        form_id = form.get('id')
        if form_id and form_id not in forms_dict:
            forms_dict[form_id] = form
    
    all_forms = list(forms_dict.values())
    
    if not all_forms:
        print("Error: No forms found in OLD portal", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(all_forms)} forms", file=sys.stderr)
    
    # Determine target email and submission
    target_email = None
    form_guid_found = None
    form_name_found = None
    submission_obj = None
    scanned_count = 0
    old_contact_id = None
    new_contact_id = None
    old_search_response = None
    new_search_response = None
    
    if email:
        # User provided email - normalize and find submission for it
        target_email = normalize_email(email)
        if not target_email:
            print(f"Error: Invalid email format: {email}", file=sys.stderr)
            sys.exit(1)
        
        print(f"Using provided email: {target_email}", file=sys.stderr)
        
        # Find submission for this email
        print(f"Searching for submission with email: {target_email}...", file=sys.stderr)
        if form_guid:
            print(f"Restricting search to form GUID: {form_guid}", file=sys.stderr)
        
        form_guid_found, form_name_found, submission_obj, scanned_count = find_one_submission_for_email_old(
            target_email=target_email,
            forms=all_forms,
            old_token=old_token,
            restrict_form_guid=form_guid,
            max_scan=max_scan,
            timeout=timeout
        )
        
        if not submission_obj:
            print(f"Email exists in NEW/OLD contacts but no submission found (scanned={scanned_count}).", file=sys.stderr)
            # Still continue to fetch contacts
        
        # Verify email exists in both portals
        print("Verifying email exists in both portals...", file=sys.stderr)
        old_contact_id, old_search_response = search_contact_id_by_email(old_token, target_email, timeout=timeout)
        new_contact_id, new_search_response = search_contact_id_by_email(new_token, target_email, timeout=timeout)
        
        if not old_contact_id:
            print(f"Error: Email {target_email} not found in OLD portal", file=sys.stderr)
            sys.exit(1)
        
        if not new_contact_id:
            print(f"Error: Email {target_email} not found in NEW portal", file=sys.stderr)
            sys.exit(1)
        
        print(f"Email verified in both portals", file=sys.stderr)
    else:
        # Auto mode: build NEW email set, then scan submissions
        print("Auto mode: finding submission with email in NEW contacts...", file=sys.stderr)
        
        # Build NEW contact email set
        new_email_set, new_stats = build_new_contact_email_set(new_token, timeout=timeout)
        
        if not new_email_set:
            print("Error: No contacts with emails found in NEW portal", file=sys.stderr)
            sys.exit(1)
        
        print(f"NEW portal email index: {new_stats['emails_indexed']} emails", file=sys.stderr)
        
        # Find first submission whose email is in NEW email set
        if form_guid:
            print(f"Restricting search to form GUID: {form_guid}", file=sys.stderr)
        
        target_email, form_guid_found, form_name_found, submission_obj, scanned_count = find_first_submission_with_email_in_new(
            forms=all_forms,
            new_email_set=new_email_set,
            old_token=old_token,
            restrict_form_guid=form_guid,
            max_scan=max_scan,
            timeout=timeout
        )
        
        if not target_email or not submission_obj:
            print(f"Error: No submission found with email present in NEW contacts (scanned={scanned_count})", file=sys.stderr)
            sys.exit(1)
        
        print(f"Selected email from submission: {target_email}", file=sys.stderr)
        
        # Fetch contacts for the chosen email
        print("Fetching contacts for selected email...", file=sys.stderr)
        old_contact_id, old_search_response = search_contact_id_by_email(old_token, target_email, timeout=timeout)
        new_contact_id, new_search_response = search_contact_id_by_email(new_token, target_email, timeout=timeout)
        
        if not old_contact_id:
            print(f"Warning: Email {target_email} not found in OLD portal contacts", file=sys.stderr)
        
        if not new_contact_id:
            print(f"Warning: Email {target_email} not found in NEW portal contacts (unexpected)", file=sys.stderr)
    
    # Print output (reordered: submission first, then NEW contact, then OLD contact)
    print("\n" + "=" * 60, file=sys.stderr)
    print(f"target_email: {target_email}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Print submission first (if found)
    print("\n" + "=" * 60, file=sys.stderr)
    print("OLD PORTAL FORM SUBMISSION (RAW)", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    if submission_obj:
        print(f"formGuid: {form_guid_found}", file=sys.stderr)
        print(f"formName: {form_name_found}", file=sys.stderr)
        print(f"matched_email: {target_email}", file=sys.stderr)
        
        # Print derived preview
        page_url = submission_obj.get('pageUrl', '')
        submitted_at = submission_obj.get('submittedAt', '')
        values = submission_obj.get('values', [])
        
        # Extract field names from values
        field_names = []
        if isinstance(values, list):
            for item in values:
                if isinstance(item, dict):
                    name = item.get('name', '')
                    if name:
                        field_names.append(name)
        elif isinstance(values, dict):
            field_names = list(values.keys())
        
        print(f"\nDerived preview:", file=sys.stderr)
        print(f"  pageUrl: {page_url}", file=sys.stderr)
        print(f"  submittedAt: {submitted_at}", file=sys.stderr)
        print(f"  field_names: {field_names}", file=sys.stderr)
        
        print("\nsubmission JSON:", file=sys.stderr)
        print(json.dumps(submission_obj, indent=2, sort_keys=True), file=sys.stdout)
    else:
        print(f"No form submission found for target_email (scanned={scanned_count}).", file=sys.stderr)
    
    # Print NEW portal data if requested
    if portal in ['new', 'both']:
        print("\n" + "=" * 60, file=sys.stderr)
        print("NEW PORTAL CONTACT (RAW)", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        
        if new_contact_id:
            print(f"new_contact_id: {new_contact_id}", file=sys.stderr)
            print("\nNEW search response JSON:", file=sys.stderr)
            print(json.dumps(new_search_response, indent=2, sort_keys=True), file=sys.stdout)
            
            new_contact = get_contact_by_id(new_token, new_contact_id, timeout=timeout)
            print("\nNEW contact GET response JSON:", file=sys.stderr)
            print(json.dumps(new_contact, indent=2, sort_keys=True), file=sys.stdout)
        else:
            print("Contact not found in NEW portal", file=sys.stderr)
    
    # Print OLD portal data if requested
    if portal in ['old', 'both']:
        print("\n" + "=" * 60, file=sys.stderr)
        print("OLD PORTAL CONTACT (RAW)", file=sys.stderr)
        print("=" * 60, file=sys.stderr)
        
        if old_contact_id:
            print(f"old_contact_id: {old_contact_id}", file=sys.stderr)
            print("\nOLD search response JSON:", file=sys.stderr)
            print(json.dumps(old_search_response, indent=2, sort_keys=True), file=sys.stdout)
            
            old_contact = get_contact_by_id(old_token, old_contact_id, timeout=timeout)
            print("\nOLD contact GET response JSON:", file=sys.stderr)
            print(json.dumps(old_contact, indent=2, sort_keys=True), file=sys.stdout)
        else:
            print("Contact not found in OLD portal", file=sys.stderr)


def load_created_note_keys(jsonl_path: str) -> set:
    """
    Load created note submission keys from JSONL file.
    
    Returns: set of submission_keys (formGuid:conversionId)
    
    Backward compatibility: If record has "submission_key", load it.
    If record has old format (key/day_key), ignore (or optionally derive if conversionId present).
    """
    submission_keys = set()
    
    if not os.path.exists(jsonl_path):
        return submission_keys
    
    try:
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    # New format: submission_key
                    if 'submission_key' in record:
                        submission_keys.add(record['submission_key'])
                    # Old format: try to derive if conversionId present
                    elif 'conversionId' in record and 'formGuid' in record:
                        conversion_id = record.get('conversionId', '')
                        form_guid = record.get('formGuid', '')
                        if conversion_id and form_guid:
                            submission_keys.add(f"{form_guid}:{conversion_id}")
                    # Otherwise ignore old records
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"Warning: Error loading {jsonl_path}: {e}", file=sys.stderr)
    
    return submission_keys


def append_created_note_key(jsonl_path: str, submission_key: str, form_guid: str, conversion_id: str,
                            email: str, page: str, submitted_date: str, contact_id: str, 
                            note_id: Optional[str] = None, note_body_hash: Optional[str] = None):
    """
    Append a created note key record to JSONL file.
    """
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
    
    record = {
        'submission_key': submission_key,
        'formGuid': form_guid,
        'conversionId': conversion_id,
        'email': email,
        'page': page,
        'submitted_date': submitted_date,
        'contactId': contact_id,
        'noteId': note_id or '',
        'note_body_hash': note_body_hash or '',
        'createdAt': datetime.now(timezone.utc).isoformat()
    }
    
    with open(jsonl_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')


def build_new_email_to_contact_id_map(token: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, str]:
    """
    Build a mapping of normalized email -> contact ID from NEW portal.
    
    Returns: dict mapping normalized_email -> contact_id
    """
    email_to_id = {}
    after = None
    
    print("Building NEW portal email->contactId index...", file=sys.stderr)
    
    while True:
        params = {
            'limit': 100,
            'properties': 'email'
        }
        
        if after is not None:
            params['after'] = after
        
        url = f"{BASE_URL}/crm/v3/objects/contacts"
        response = hubspot_get(url, params=params, token=token, timeout=timeout)
        
        results = response.get('results', [])
        if not results:
            break
        
        for contact in results:
            contact_id = contact.get('id')
            email_raw = contact.get('properties', {}).get('email')
            
            if email_raw and contact_id:
                email_normalized = normalize_email(email_raw)
                if email_normalized:
                    # Last wins if duplicate email (shouldn't happen, but handle it)
                    email_to_id[email_normalized] = contact_id
        
        # Progress update every 1000 contacts
        if len(email_to_id) % 1000 == 0 and len(email_to_id) > 0:
            print(f"  Progress: {len(email_to_id)} emails indexed", file=sys.stderr)
        
        # Check for next page
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after = next_page.get('after')
        
        if not next_after:
            break
        
        after = next_after
    
    print(f"  Completed: {len(email_to_id)} emails indexed", file=sys.stderr)
    return email_to_id


def create_note(token: str, note_body: str, hs_timestamp: Optional[str] = None, timeout: int = DEFAULT_TIMEOUT) -> Optional[str]:
    """
    Create a note object in HubSpot.
    
    Args:
        token: HubSpot API token
        note_body: The note body text (required)
        hs_timestamp: Timestamp in epoch milliseconds as string (required by some portals)
        timeout: Request timeout
    
    Returns: note_id or None if creation failed
    """
    url = f"{BASE_URL}/crm/v3/objects/notes"
    
    properties = {
        'hs_note_body': note_body
    }
    
    # Add timestamp if provided (required by many portals)
    if hs_timestamp:
        properties['hs_timestamp'] = hs_timestamp
    
    payload = {
        'properties': properties
    }
    
    # Custom POST handling to capture 400 error details and handle retries
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        request = Request(url, data=json.dumps(payload).encode('utf-8'))
        request.add_header('Authorization', f'Bearer {token}')
        request.add_header('Content-Type', 'application/json')
        
        try:
            response = urlopen(request, timeout=timeout)
            status = response.getcode()
            
            if status == 201:
                body_bytes = response.read()
                body_str = body_bytes.decode('utf-8') if body_bytes else ''
                json_data = json.loads(body_str) if body_str else {}
                note_id = json_data.get('id')
                return note_id
            else:
                print(f"Error creating note: Unexpected status {status}", file=sys.stderr)
                return None
                
        except HTTPError as e:
            status = e.code
            body_bytes = e.read() if hasattr(e, 'read') else b''
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            # Parse error response
            error_details = {}
            if body_str:
                try:
                    error_details = json.loads(body_str)
                except json.JSONDecodeError:
                    error_details = {'raw_body': body_str}
            
            # Print detailed error information (for all errors, including 400)
            print(f"Error creating NOTE. HTTP {status}", file=sys.stderr)
            print(f"  Response body: {json.dumps(error_details, indent=2)}", file=sys.stderr)
            
            # Redact token from payload for logging
            payload_safe = payload.copy()
            if 'properties' in payload_safe:
                payload_safe['properties'] = payload_safe['properties'].copy()
                # Truncate note_body if very long
                if 'hs_note_body' in payload_safe['properties']:
                    body_preview = payload_safe['properties']['hs_note_body']
                    if len(body_preview) > 200:
                        body_preview = body_preview[:200] + '...'
                    payload_safe['properties']['hs_note_body'] = body_preview
            
            print(f"  Request payload: {json.dumps(payload_safe, indent=2)}", file=sys.stderr)
            
            # Handle retries for rate limits and server errors
            if status == 429:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"  Rate limited (429). Waiting {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print("  Rate limit exceeded after retries.", file=sys.stderr)
                    return None
            elif 500 <= status < 600:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"  Server error ({status}). Retrying in {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"  Server error {status} after retries.", file=sys.stderr)
                    return None
            elif status == 401 or status == 403:
                print("  Forbidden/Unauthorized: verify private app has required scopes.", file=sys.stderr)
                return None
            else:
                # For 400 and other errors, don't retry but print details
                return None
                
        except Exception as e:
            print(f"Error creating note: {e}", file=sys.stderr)
            return None
    
    # Should not reach here, but handle just in case
    print("Error creating note: Max retries exceeded or unexpected error", file=sys.stderr)
    return None


def associate_note_to_contact(token: str, note_id: str, contact_id: str, timeout: int = DEFAULT_TIMEOUT) -> bool:
    """
    Associate a note to a contact in HubSpot.
    
    Retries on 429/5xx with exponential backoff.
    
    Returns: True if successful, False otherwise
    """
    url = f"{BASE_URL}/crm/v3/objects/notes/{note_id}/associations/contacts/{contact_id}/note_to_contact"
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # PUT request with empty body
            request_obj = Request(url, data=b'', method='PUT')
            request_obj.add_header('Authorization', f'Bearer {token}')
            request_obj.add_header('Content-Type', 'application/json')
            
            response = urlopen(request_obj, timeout=timeout)
            status = response.getcode()
            
            if status == 200 or status == 201:
                return True
            
            # Non-success status
            return False
            
        except HTTPError as e:
            status = e.code
            
            if status == 401 or status == 403:
                print(f"Error associating note {note_id} to contact {contact_id}: Forbidden/Unauthorized", file=sys.stderr)
                return False
            elif status == 429:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"Rate limited (429) associating note. Waiting {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"Error: Rate limit exceeded after retries for note {note_id}", file=sys.stderr)
                    return False
            elif 500 <= status < 600:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"Server error ({status}) associating note. Retrying in {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"Error: Server error after retries for note {note_id}", file=sys.stderr)
                    return False
            else:
                # Other 4xx errors
                print(f"Error associating note {note_id} to contact {contact_id}: HTTP {status}", file=sys.stderr)
                return False
                
        except Exception as e:
            print(f"Error associating note {note_id} to contact {contact_id}: {e}", file=sys.stderr)
            return False
    
    return False


def run_test_10(old_token: str, new_token: str, n_requested: int, max_scan: int, 
                since_date: Optional[str], timeout: int):
    """
    Generate reliability test set of N examples where email has:
    1) Form submission in OLD portal
    2) Matching contact in OLD portal
    3) Matching contact in NEW portal
    """
    print("\n" + "=" * 60, file=sys.stderr)
    print(f"TEST-{n_requested} GENERATOR", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Get portal details
    old_details = get_portal_details(old_token, timeout=timeout)
    new_details = get_portal_details(new_token, timeout=timeout)
    
    old_portal_id = old_details.get('portalId') or old_details.get('portal_id') or 'unknown'
    new_portal_id = new_details.get('portalId') or new_details.get('portal_id') or 'unknown'
    
    print(f"OLD portalId: {old_portal_id}", file=sys.stderr)
    print(f"NEW portalId: {new_portal_id}", file=sys.stderr)
    
    # Parse since_date if provided
    cutoff_ms = None
    if since_date:
        cutoff_ms = parse_date_to_toronto_midnight(since_date)
        if cutoff_ms:
            print(f"Filtering submissions >= {since_date} (Toronto midnight, epoch ms: {cutoff_ms})", file=sys.stderr)
        else:
            print(f"Warning: Could not parse --test-since-date {since_date}, ignoring", file=sys.stderr)
    
    # Step 0: Build NEW portal email index
    print("\nStep 0: Building NEW portal email index...", file=sys.stderr)
    new_email_set, new_stats = build_new_contact_email_set(new_token, timeout=timeout)
    
    if not new_email_set:
        print("Error: No contacts with emails found in NEW portal", file=sys.stderr)
        sys.exit(1)
    
    print(f"NEW portal email index: {new_stats['emails_indexed']} emails", file=sys.stderr)
    
    # Step 1: Iterate OLD portal forms and submissions
    print(f"\nStep 1: Scanning submissions (max_scan={max_scan}, target={n_requested} examples)...", file=sys.stderr)
    
    # Fetch forms
    print("Fetching forms...", file=sys.stderr)
    live_forms = list_forms(archived=False, token=old_token, timeout=timeout)
    archived_forms = list_forms(archived=True, token=old_token, timeout=timeout)
    
    # De-duplicate by form id
    forms_dict = {}
    for form in live_forms + archived_forms:
        form_id = form.get('id')
        if form_id and form_id not in forms_dict:
            forms_dict[form_id] = form
    
    all_forms = list(forms_dict.values())
    
    if not all_forms:
        print("Error: No forms found in OLD portal", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(all_forms)} forms", file=sys.stderr)
    
    # Collect test samples
    samples = []
    scanned_submissions = 0
    duplicates_skipped_strict = 0
    duplicates_skipped_same_day = 0
    seen_strict_hashes = set()
    seen_day_keys = set()
    unique_emails_collected = set()
    unique_pages_collected = set()
    
    # Sort forms deterministically
    forms_sorted = sorted(all_forms, key=lambda f: (get_form_guid(f) or '', f.get('name', '')))
    
    for form_idx, form in enumerate(forms_sorted):
        form_guid = get_form_guid(form)
        form_name = form.get('name', 'Unknown')
        
        if not form_guid:
            continue
        
        print(f"  Checking form {form_idx + 1}/{len(forms_sorted)}: {form_name} ({form_guid})", file=sys.stderr)
        
        try:
            for submission in iter_form_submissions_old(form_guid, old_token, limit=50, timeout=timeout):
                scanned_submissions += 1
                
                # Check max_scan limit
                if scanned_submissions >= max_scan:
                    print(f"\nReached max_scan={max_scan}, stopping", file=sys.stderr)
                    break
                
                # Check if we have enough samples
                if len(samples) >= n_requested:
                    break
                
                # Extract identifiers
                ids = extract_identifiers(submission)
                submission_email = ids.get('email')
                
                if not submission_email:
                    continue
                
                submission_email_normalized = normalize_email(submission_email)
                if not submission_email_normalized:
                    continue
                
                # Check if email in NEW email set
                if submission_email_normalized not in new_email_set:
                    continue
                
                # Check since_date filter
                submitted_at_ms = submission.get('submittedAt')
                if cutoff_ms:
                    if not submitted_at_ms or submitted_at_ms < cutoff_ms:
                        continue
                
                # Verify OLD contact exists
                old_contact_id, old_search_response = search_contact_id_by_email(old_token, submission_email_normalized, timeout=timeout)
                if not old_contact_id:
                    continue
                
                # Verify NEW contact exists
                new_contact_id, new_search_response = search_contact_id_by_email(new_token, submission_email_normalized, timeout=timeout)
                if not new_contact_id:
                    continue
                
                # Now that submission qualifies, check for duplicates
                # Build canonical fields for dedupe
                values = submission.get('values', [])
                canonical_fields = build_canonical_fields(values)
                
                # Compute dedupe keys
                page_url = submission.get('pageUrl', '')
                strict_hash, day_key = compute_dedupe_keys(
                    submission_email_normalized,
                    page_url,
                    submitted_at_ms,
                    canonical_fields
                )
                
                # Check for duplicates (strict or same-day)
                is_duplicate = False
                if strict_hash in seen_strict_hashes:
                    duplicates_skipped_strict += 1
                    is_duplicate = True
                elif day_key in seen_day_keys:
                    duplicates_skipped_same_day += 1
                    is_duplicate = True
                
                if is_duplicate:
                    continue
                
                # Mark keys as seen (first occurrence wins)
                seen_strict_hashes.add(strict_hash)
                seen_day_keys.add(day_key)
                
                # Build contact URLs
                old_contact_url = f"https://app.hubspot.com/contacts/{old_portal_id}/contact/{old_contact_id}"
                new_contact_url = f"https://app.hubspot.com/contacts/{new_portal_id}/contact/{new_contact_id}"
                
                # Generate note body
                # Extract conversionId for test-10 (may not be present in old submissions)
                conversion_id = submission.get('conversionId', '')
                note_body, derived = submission_to_note_html(form_name, form_guid, submission, conversion_id=conversion_id)
                
                # Get normalized page URL and submitted date for dedupe info
                normalized_page_url = normalize_url_for_dedupe(page_url)
                submitted_date = submitted_day_ms(submitted_at_ms)
                
                # Build sample record
                sample = {
                    'email': submission_email_normalized,
                    'form': {
                        'guid': form_guid,
                        'name': form_name
                    },
                    'submission': submission,  # Raw submission object
                    'matched_contacts': {
                        'old': {
                            'id': old_contact_id,
                            'url': old_contact_url
                        },
                        'new': {
                            'id': new_contact_id,
                            'url': new_contact_url
                        }
                    },
                    'note_body': note_body,
                    'derived': derived,
                    'dedupe': {
                        'strict_hash': strict_hash,
                        'day_key': day_key,
                        'normalized_pageUrl': normalized_page_url,
                        'submitted_date': submitted_date
                    }
                }
                
                samples.append(sample)
                unique_emails_collected.add(submission_email_normalized)
                unique_pages_collected.add(normalized_page_url)
                
                print(f"    Collected example {len(samples)}/{n_requested}: {submission_email_normalized}", file=sys.stderr)
                
                if len(samples) >= n_requested:
                    break
                
                # Progress update every 500 scanned
                if scanned_submissions % 500 == 0:
                    print(f"  scanned={scanned_submissions}, collected={len(samples)}, duplicates_strict={duplicates_skipped_strict}, duplicates_day={duplicates_skipped_same_day}, current_form={form_name}", file=sys.stderr)
            
            if len(samples) >= n_requested:
                break
            
            if scanned_submissions >= max_scan:
                break
                
        except Exception as e:
            print(f"  Warning: Error scanning form {form_guid} ({form_name}): {e}", file=sys.stderr)
            continue
    
    # Step 2: Write output JSON
    print(f"\nStep 2: Writing output...", file=sys.stderr)
    
    output = {
        'meta': {
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'oldPortalId': old_portal_id,
            'newPortalId': new_portal_id,
            'n_requested': n_requested,
            'n_returned': len(samples),
            'scanned_submissions': scanned_submissions,
            'duplicates_skipped_strict': duplicates_skipped_strict,
            'duplicates_skipped_same_day': duplicates_skipped_same_day,
            'unique_emails_collected': len(unique_emails_collected),
            'unique_pages_collected': len(unique_pages_collected),
            'since_date': since_date,
            'max_scan': max_scan
        },
        'samples': samples
    }
    
    json_path = './out/test_10_notes.json'
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print("\n" + "=" * 60, file=sys.stderr)
    print("SUMMARY", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Collected: {len(samples)}/{n_requested} examples", file=sys.stderr)
    print(f"Scanned submissions: {scanned_submissions}", file=sys.stderr)
    print(f"Duplicates skipped (strict): {duplicates_skipped_strict}", file=sys.stderr)
    print(f"Duplicates skipped (same-day): {duplicates_skipped_same_day}", file=sys.stderr)
    print(f"Unique emails: {len(unique_emails_collected)}", file=sys.stderr)
    print(f"Unique pages: {len(unique_pages_collected)}", file=sys.stderr)
    print(f"Output: {json_path}", file=sys.stderr)
    
    if len(samples) < n_requested and scanned_submissions < max_scan:
        print(f"\nWarning: Only found {len(samples)} examples but did not reach max_scan limit.", file=sys.stderr)
        print("This may indicate insufficient matching submissions.", file=sys.stderr)


# Constants for note creation
MAX_NOTES_TO_CHECK_PER_CONTACT = 500


def list_note_ids_for_contact(contact_id: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> List[str]:
    """
    Fetch note IDs associated to a contact in NEW portal.
    
    Returns: list of note IDs
    """
    note_ids = []
    after = None
    
    while True:
        params = {
            'limit': 500
        }
        
        if after is not None:
            params['after'] = after
        
        url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}/associations/notes"
        
        try:
            response = hubspot_get(url, params=params, token=token, timeout=timeout)
            
            results = response.get('results', [])
            if not results:
                break
            
            for result in results:
                note_id = result.get('id')
                if note_id:
                    note_ids.append(note_id)
            
            # Check for next page
            paging = response.get('paging', {})
            next_page = paging.get('next', {})
            next_after = next_page.get('after')
            
            if not next_after:
                break
            
            after = next_after
            
            # Performance guard: limit to MAX_NOTES_TO_CHECK_PER_CONTACT
            if len(note_ids) >= MAX_NOTES_TO_CHECK_PER_CONTACT:
                break
                
        except Exception as e:
            print(f"  Warning: Error fetching note IDs for contact {contact_id}: {e}", file=sys.stderr)
            break
    
    return note_ids


def batch_read_notes(note_ids: List[str], token: str, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, str]:
    """
    Batch read note bodies for given note IDs.
    
    Returns: dict mapping note_id -> hs_note_body
    """
    note_bodies = {}
    
    # Chunk inputs to max 100 per request
    chunk_size = 100
    
    for i in range(0, len(note_ids), chunk_size):
        chunk = note_ids[i:i + chunk_size]
        
        url = f"{BASE_URL}/crm/v3/objects/notes/batch/read"
        
        payload = {
            'properties': ['hs_note_body'],
            'inputs': [{'id': note_id} for note_id in chunk]
        }
        
        try:
            response = hubspot_post(url, payload, token=token, timeout=timeout)
            
            results = response.get('results', [])
            for result in results:
                note_id = result.get('id')
                properties = result.get('properties', {})
                note_body = properties.get('hs_note_body', '')
                if note_id and note_body:
                    note_bodies[note_id] = note_body
                    
        except Exception as e:
            print(f"  Warning: Error batch reading notes: {e}", file=sys.stderr)
            continue
    
    return note_bodies


def get_contact_existing_note_bodies(contact_id: str, token: str, 
                                     contact_cache: Dict[str, Dict[str, Any]], timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Get existing note data for a contact, using cache if available.
    
    Returns: dict with keys:
        'bodies': set of note body strings
        'marker_keys': set of submission_keys extracted from markers
        'semantic_keys': set of semantic keys (page|date|email)
    """
    # Check cache first
    if contact_id in contact_cache:
        return contact_cache[contact_id]
    
    # Fetch note IDs
    note_ids = list_note_ids_for_contact(contact_id, token, timeout=timeout)
    
    if not note_ids:
        empty_result = {
            'bodies': set(),
            'marker_keys': set(),
            'semantic_keys': set()
        }
        contact_cache[contact_id] = empty_result
        return empty_result
    
    # Warn if hitting limit
    if len(note_ids) >= MAX_NOTES_TO_CHECK_PER_CONTACT:
        print(f"  Warning: Contact {contact_id} has {len(note_ids)}+ notes, checking first {MAX_NOTES_TO_CHECK_PER_CONTACT}", file=sys.stderr)
        note_ids = note_ids[:MAX_NOTES_TO_CHECK_PER_CONTACT]
    
    # Batch read note bodies
    note_bodies_dict = batch_read_notes(note_ids, token, timeout=timeout)
    
    # Extract keys from note bodies
    note_bodies_set = set()
    marker_keys_set = set()
    semantic_keys_set = set()
    
    for note_body in note_bodies_dict.values():
        note_bodies_set.add(note_body)
        
        # Extract marker key
        marker_key = extract_marker_key(note_body)
        if marker_key:
            marker_keys_set.add(marker_key)
        else:
            # If no marker, try semantic key as fallback
            semantic_key = extract_semantic_key(note_body)
            if semantic_key:
                semantic_keys_set.add(semantic_key)
    
    result = {
        'bodies': note_bodies_set,
        'marker_keys': marker_keys_set,
        'semantic_keys': semantic_keys_set
    }
    
    # Cache it
    contact_cache[contact_id] = result
    
    return result


def run_create_notes(old_token: str, new_token: str, dry_run: bool, limit: int, 
                     since_date: Optional[str], resume: bool, max_scan: int,
                     forms_filter: Optional[str], timeout: int):
    """
    Create notes in NEW portal for form submissions from OLD portal.
    """
    print("\n" + "=" * 60, file=sys.stderr)
    print("CREATE NOTES FOR FORM SUBMISSIONS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    if dry_run:
        print("DRY-RUN MODE: No notes will be created", file=sys.stderr)
    else:
        print("PRODUCTION MODE: Notes will be created in NEW portal", file=sys.stderr)
    
    # Get portal details
    old_details = get_portal_details(old_token, timeout=timeout)
    new_details = get_portal_details(new_token, timeout=timeout)
    
    old_portal_id = old_details.get('portalId') or old_details.get('portal_id') or 'unknown'
    new_portal_id = new_details.get('portalId') or new_details.get('portal_id') or 'unknown'
    
    print(f"OLD portalId: {old_portal_id}", file=sys.stderr)
    print(f"NEW portalId: {new_portal_id}", file=sys.stderr)
    
    # Parse since_date if provided
    cutoff_ms = None
    if since_date:
        cutoff_ms = parse_date_to_toronto_midnight(since_date)
        if cutoff_ms:
            print(f"Filtering submissions >= {since_date} (Toronto midnight, epoch ms: {cutoff_ms})", file=sys.stderr)
        else:
            print(f"Warning: Could not parse --since-date {since_date}, ignoring", file=sys.stderr)
    
    # Parse forms filter if provided
    form_guids_filter = None
    if forms_filter:
        form_guids_filter = [guid.strip() for guid in forms_filter.split(',') if guid.strip()]
        print(f"Filtering to {len(form_guids_filter)} form(s): {form_guids_filter}", file=sys.stderr)
    
    # Load created note keys if resuming
    jsonl_path = './out/created_note_keys.jsonl'
    submission_keys_seen = set()
    
    if resume:
        print(f"\nLoading existing note keys from {jsonl_path}...", file=sys.stderr)
        submission_keys_seen = load_created_note_keys(jsonl_path)
        print(f"Loaded {len(submission_keys_seen)} submission keys", file=sys.stderr)
    else:
        # Still create the file if it doesn't exist (for appending)
        os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)
    
    # Step 1: Build NEW portal email->contactId index
    print("\nStep 1: Building NEW portal email->contactId index...", file=sys.stderr)
    new_email_to_id = build_new_email_to_contact_id_map(new_token, timeout=timeout)
    
    if not new_email_to_id:
        print("Error: No contacts with emails found in NEW portal", file=sys.stderr)
        sys.exit(1)
    
    print(f"Indexed {len(new_email_to_id)} emails", file=sys.stderr)
    
    # Step 2: Fetch forms from OLD portal
    print("\nStep 2: Fetching forms from OLD portal...", file=sys.stderr)
    live_forms = list_forms(archived=False, token=old_token, timeout=timeout)
    archived_forms = list_forms(archived=True, token=old_token, timeout=timeout)
    
    # De-duplicate by form id
    forms_dict = {}
    for form in live_forms + archived_forms:
        form_id = form.get('id')
        if form_id and form_id not in forms_dict:
            forms_dict[form_id] = form
    
    all_forms = list(forms_dict.values())
    
    # Filter by form GUIDs if provided
    if form_guids_filter:
        all_forms = [f for f in all_forms if get_form_guid(f) in form_guids_filter]
        if not all_forms:
            print(f"Error: No forms found matching provided GUIDs", file=sys.stderr)
            sys.exit(1)
    
    if not all_forms:
        print("Error: No forms found in OLD portal", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(all_forms)} forms to process", file=sys.stderr)
    
    # Step 3: Iterate submissions
    print(f"\nStep 3: Processing submissions (max_scan={max_scan if max_scan > 0 else 'unlimited'}, limit={limit if limit > 0 else 'unlimited'})...", file=sys.stderr)
    
    # Counters
    scanned_submissions = 0
    eligible_submissions = 0
    skipped_local_submission_key = 0
    skipped_existing_note_marker = 0
    skipped_existing_note_semantic = 0
    skipped_existing_note_exact = 0
    fetched_contact_note_sets = 0
    created_notes = 0
    errors = []
    
    # Per-contact note cache (bodies, marker_keys, semantic_keys)
    contact_existing_note_cache: Dict[str, Dict[str, Any]] = {}
    
    # Track first email with created note (for manual verification)
    first_created_email = None
    
    # Dry-run preview
    dry_run_preview = []
    
    # Sort forms deterministically
    forms_sorted = sorted(all_forms, key=lambda f: (get_form_guid(f) or '', f.get('name', '')))
    
    for form_idx, form in enumerate(forms_sorted):
        form_guid = get_form_guid(form)
        form_name = form.get('name', 'Unknown')
        
        if not form_guid:
            continue
        
        print(f"  Processing form {form_idx + 1}/{len(forms_sorted)}: {form_name} ({form_guid})", file=sys.stderr)
        
        try:
            for submission in iter_form_submissions_old(form_guid, old_token, limit=50, timeout=timeout):
                scanned_submissions += 1
                
                # Check max_scan limit
                if max_scan > 0 and scanned_submissions >= max_scan:
                    print(f"\nReached max_scan={max_scan}, stopping", file=sys.stderr)
                    break
                
                # Check limit
                if limit > 0 and created_notes >= limit:
                    print(f"\nReached limit={limit}, stopping", file=sys.stderr)
                    break
                
                # Extract identifiers
                ids = extract_identifiers(submission)
                submission_email = ids.get('email')
                
                if not submission_email:
                    continue
                
                submission_email_normalized = normalize_email(submission_email)
                if not submission_email_normalized:
                    continue
                
                # Check since_date filter
                submitted_at_ms = submission.get('submittedAt')
                if cutoff_ms:
                    if not submitted_at_ms or submitted_at_ms < cutoff_ms:
                        continue
                
                # Check if email exists in NEW portal
                contact_id = new_email_to_id.get(submission_email_normalized)
                if not contact_id:
                    continue
                
                eligible_submissions += 1
                
                # Extract conversionId from submission
                conversion_id = submission.get('conversionId', '')
                
                # Build submission_key (formGuid:conversionId)
                # If conversionId missing, fall back to derived hash
                if conversion_id:
                    submission_key = make_submission_key(form_guid, conversion_id)
                else:
                    # Fallback: derive from email+page+date+fields
                    values = submission.get('values', [])
                    canonical_fields = build_canonical_fields(values)
                    page_url = submission.get('pageUrl', '')
                    normalized_page_url = normalize_url_for_dedupe(page_url)
                    submitted_date = submitted_day_ms(submitted_at_ms)
                    
                    fallback_payload = {
                        "email": submission_email_normalized,
                        "page": normalized_page_url,
                        "submitted_date": submitted_date,
                        "fields": canonical_fields
                    }
                    fallback_json = json.dumps(fallback_payload, sort_keys=True, separators=(',', ':'))
                    fallback_hash = hashlib.sha256(fallback_json.encode('utf-8')).hexdigest()
                    submission_key = make_submission_key(form_guid, fallback_hash)
                    conversion_id = fallback_hash  # Use hash as conversionId for fallback
                
                # Get normalized page URL and submitted date (needed for semantic key)
                page_url = submission.get('pageUrl', '')
                normalized_page_url = normalize_url_for_dedupe(page_url)
                submitted_date = submitted_day_ms(submitted_at_ms)
                
                # Build semantic key (fallback for notes without markers)
                semantic_key = f"{normalized_page_url}|{submitted_date}|{submission_email_normalized}"
                
                # Check local idempotency (submission_key already processed)
                if submission_key in submission_keys_seen:
                    skipped_local_submission_key += 1
                    continue
                
                # Generate note bodies (HTML and text for duplicate checking)
                note_body_html, derived = submission_to_note_html(form_name, form_guid, submission, conversion_id=conversion_id)
                note_body_text, _ = submission_to_note_text(form_name, form_guid, submission, conversion_id=conversion_id)
                
                # Compute note body hash for audit (based on HTML version)
                note_body_hash = hashlib.sha256(note_body_html.encode('utf-8')).hexdigest()
                
                # Pre-check: Check if contact already has identical note
                # Track if this is first fetch for this contact (before cache is populated)
                is_first_fetch = contact_id not in contact_existing_note_cache
                
                existing_note_data = get_contact_existing_note_bodies(
                    contact_id, 
                    new_token, 
                    contact_existing_note_cache,
                    timeout=timeout
                )
                
                if is_first_fetch:
                    fetched_contact_note_sets += 1
                
                existing_marker_keys = existing_note_data.get('marker_keys', set())
                existing_semantic_keys = existing_note_data.get('semantic_keys', set())
                existing_note_bodies = existing_note_data.get('bodies', set())
                
                # Skip rule A: Marker key match (fastest, most reliable)
                if submission_key in existing_marker_keys:
                    skipped_existing_note_marker += 1
                    # Add to local set to skip in future runs
                    submission_keys_seen.add(submission_key)
                    continue
                
                # Skip rule B: Semantic key match (fallback for notes without markers)
                if semantic_key in existing_semantic_keys:
                    skipped_existing_note_semantic += 1
                    # Add to local set to skip in future runs
                    submission_keys_seen.add(submission_key)
                    continue
                
                # Skip rule C: Exact body match (check both HTML and text versions to avoid duplicates)
                # This handles cases where old notes exist in plain text format
                if note_body_html in existing_note_bodies or note_body_text in existing_note_bodies:
                    skipped_existing_note_exact += 1
                    # Add to local set to skip in future runs
                    submission_keys_seen.add(submission_key)
                    continue
                
                # Dry-run: collect preview
                if dry_run:
                    dry_run_preview.append({
                        'email': submission_email_normalized,
                        'contactId': contact_id,
                        'formGuid': form_guid,
                        'formName': form_name,
                        'submitted_date': submitted_date,
                        'page': normalized_page_url,
                        'submission_key': submission_key,
                        'conversionId': conversion_id,
                        'note_body': note_body_html,
                        'note_body_hash': note_body_hash
                    })
                    
                    # Mark as seen (for dry-run dedupe)
                    submission_keys_seen.add(submission_key)
                    # Add to cache (simulate creation)
                    if contact_id not in contact_existing_note_cache:
                        contact_existing_note_cache[contact_id] = {
                            'bodies': set(),
                            'marker_keys': set(),
                            'semantic_keys': set()
                        }
                    contact_existing_note_cache[contact_id]['bodies'].add(note_body_html)
                    contact_existing_note_cache[contact_id]['marker_keys'].add(submission_key)
                    contact_existing_note_cache[contact_id]['semantic_keys'].add(semantic_key)
                    created_notes += 1
                    
                    # Print preview every 25 notes
                    if created_notes % 25 == 0:
                        print(f"    Preview: {created_notes} notes (would create)", file=sys.stderr)
                
                else:
                    # Production: create note and associate
                    # Convert submittedAt (epoch ms) to string for hs_timestamp
                    hs_timestamp_str = str(submitted_at_ms) if submitted_at_ms else None
                    note_id = create_note(new_token, note_body_html, hs_timestamp=hs_timestamp_str, timeout=timeout)
                    
                    if not note_id:
                        errors.append({
                            'email': submission_email_normalized,
                            'contactId': contact_id,
                            'formGuid': form_guid,
                            'error': 'Failed to create note',
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                        continue
                    
                    # Associate note to contact
                    success = associate_note_to_contact(new_token, note_id, contact_id, timeout=timeout)
                    
                    if not success:
                        errors.append({
                            'email': submission_email_normalized,
                            'contactId': contact_id,
                            'formGuid': form_guid,
                            'noteId': note_id,
                            'error': 'Failed to associate note to contact',
                            'timestamp': datetime.now(timezone.utc).isoformat()
                        })
                        continue
                    
                    # Append to JSONL
                    append_created_note_key(
                        jsonl_path,
                        submission_key,
                        form_guid,
                        conversion_id,
                        submission_email_normalized,
                        normalized_page_url,
                        submitted_date,
                        contact_id,
                        note_id,
                        note_body_hash
                    )
                    
                    # Mark as seen
                    submission_keys_seen.add(submission_key)
                    # Add to cache (so subsequent submissions for same contact skip this)
                    if contact_id not in contact_existing_note_cache:
                        contact_existing_note_cache[contact_id] = {
                            'bodies': set(),
                            'marker_keys': set(),
                            'semantic_keys': set()
                        }
                    contact_existing_note_cache[contact_id]['bodies'].add(note_body_html)
                    contact_existing_note_cache[contact_id]['marker_keys'].add(submission_key)
                    contact_existing_note_cache[contact_id]['semantic_keys'].add(semantic_key)
                    created_notes += 1
                    
                    # Track first email for manual verification
                    if first_created_email is None:
                        first_created_email = submission_email_normalized
                
                # Progress update every 100 scanned
                if scanned_submissions % 100 == 0:
                    print(f"  scanned={scanned_submissions}, eligible={eligible_submissions}, created={created_notes}, skipped_exact={skipped_existing_note_exact}, skipped_marker={skipped_existing_note_marker}, skipped_semantic={skipped_existing_note_semantic}, skipped_local={skipped_local_submission_key}", file=sys.stderr)
                
                if limit > 0 and created_notes >= limit:
                    break
            
            if limit > 0 and created_notes >= limit:
                break
            
            if max_scan > 0 and scanned_submissions >= max_scan:
                break
                
        except Exception as e:
            print(f"  Warning: Error processing form {form_guid} ({form_name}): {e}", file=sys.stderr)
            continue
    
    # Step 4/5: Write output
    print(f"\nStep 4: Writing output...", file=sys.stderr)
    
    if dry_run:
        # Write dry-run preview JSON
        preview_path = './out/create_notes_dryrun.json'
        os.makedirs(os.path.dirname(preview_path), exist_ok=True)
        
        preview_output = {
            'meta': {
                'generatedAt': datetime.now(timezone.utc).isoformat(),
                'mode': 'dry-run',
                'scanned_submissions': scanned_submissions,
                'eligible_submissions': eligible_submissions,
                'would_create': created_notes,
                'skipped_local_submission_key': skipped_local_submission_key,
                'skipped_existing_note_marker': skipped_existing_note_marker,
                'skipped_existing_note_semantic': skipped_existing_note_semantic,
                'skipped_existing_note_exact': skipped_existing_note_exact,
                'fetched_contact_note_sets': fetched_contact_note_sets,
                'since_date': since_date,
                'max_scan': max_scan,
                'limit': limit
            },
            'preview': dry_run_preview[:100]  # First 100 for preview
        }
        
        with open(preview_path, 'w', encoding='utf-8') as f:
            json.dump(preview_output, f, indent=2, ensure_ascii=False)
        
        print(f"Dry-run preview written to: {preview_path}", file=sys.stderr)
    
    # Write errors if any
    if errors:
        errors_path = './out/create_notes_errors.jsonl'
        os.makedirs(os.path.dirname(errors_path), exist_ok=True)
        
        with open(errors_path, 'w', encoding='utf-8') as f:
            for error in errors:
                f.write(json.dumps(error, ensure_ascii=False) + '\n')
        
        print(f"Errors written to: {errors_path} ({len(errors)} errors)", file=sys.stderr)
    
    # Print summary
    print("\n" + "=" * 60, file=sys.stderr)
    print("SUMMARY", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Scanned submissions: {scanned_submissions}", file=sys.stderr)
    print(f"Eligible submissions: {eligible_submissions}", file=sys.stderr)
    if dry_run:
        print(f"Would create notes: {created_notes}", file=sys.stderr)
    else:
        print(f"Created notes: {created_notes}", file=sys.stderr)
    print(f"Skipped (local submission key): {skipped_local_submission_key}", file=sys.stderr)
    print(f"Skipped (existing note marker): {skipped_existing_note_marker}", file=sys.stderr)
    print(f"Skipped (existing note semantic): {skipped_existing_note_semantic}", file=sys.stderr)
    print(f"Skipped (existing note exact match): {skipped_existing_note_exact}", file=sys.stderr)
    print(f"Fetched note sets for contacts: {fetched_contact_note_sets}", file=sys.stderr)
    if errors:
        print(f"Errors: {len(errors)}", file=sys.stderr)
    if not dry_run:
        print(f"Note keys written to: {jsonl_path}", file=sys.stderr)
        if first_created_email:
            print(f"\nVerification: Check email {first_created_email} in NEW portal to verify note was created.", file=sys.stderr)


def run_db_difference(old_token: str, new_token: str, timeout: int):
    """
    Count contacts in OLD vs NEW portal and print the difference.
    Includes portal identity verification and dual counting methods.
    """
    print("\n" + "=" * 60, file=sys.stderr)
    print("CONTACT DB DIFFERENCE (VERIFIED)", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Get portal details
    print("\nFetching portal details...", file=sys.stderr)
    old_details = get_portal_details(old_token, timeout=timeout)
    new_details = get_portal_details(new_token, timeout=timeout)
    
    # Print portal details
    old_portal_id = old_details.get('portalId') or old_details.get('portal_id') or 'unknown'
    old_company = old_details.get('companyName') or old_details.get('company_name') or 'unknown'
    old_keys = old_details.get('keys', [])
    
    new_portal_id = new_details.get('portalId') or new_details.get('portal_id') or 'unknown'
    new_company = new_details.get('companyName') or new_details.get('company_name') or 'unknown'
    new_keys = new_details.get('keys', [])
    
    print(f"OLD portal: portalId={old_portal_id}, companyName={old_company}, keys={old_keys}", file=sys.stderr)
    print(f"NEW portal: portalId={new_portal_id}, companyName={new_company}, keys={new_keys}", file=sys.stderr)
    
    # Count contacts in OLD portal (list method)
    old_list_stats = count_contacts_in_portal(old_token, "OLD", timeout=timeout)
    
    # Count contacts in OLD portal (search total method)
    print(f"\nCounting contacts in OLD portal (search total)...", file=sys.stderr)
    old_search_stats = count_contacts_search_total(old_token, timeout=timeout)
    
    old_list_count = old_list_stats['contacts_count']
    old_search_total = old_search_stats.get('search_total')
    
    print(f"OLD: list_count={old_list_count}, search_total={old_search_total}", file=sys.stderr)
    
    # Check for mismatch
    old_mismatch = False
    if old_search_total is not None and old_list_count != old_search_total:
        old_mismatch = True
        print(f"\nWARNING: OLD portal list_count ({old_list_count}) != search_total ({old_search_total}).", file=sys.stderr)
        print(f"  List pagination may be incomplete or permissions differ.", file=sys.stderr)
    
    # Count contacts in NEW portal (list method)
    new_list_stats = count_contacts_in_portal(new_token, "NEW", timeout=timeout)
    
    # Count contacts in NEW portal (search total method)
    print(f"\nCounting contacts in NEW portal (search total)...", file=sys.stderr)
    new_search_stats = count_contacts_search_total(new_token, timeout=timeout)
    
    new_list_count = new_list_stats['contacts_count']
    new_search_total = new_search_stats.get('search_total')
    
    print(f"NEW: list_count={new_list_count}, search_total={new_search_total}", file=sys.stderr)
    
    # Check for mismatch
    new_mismatch = False
    if new_search_total is not None and new_list_count != new_search_total:
        new_mismatch = True
        print(f"\nWARNING: NEW portal list_count ({new_list_count}) != search_total ({new_search_total}).", file=sys.stderr)
        print(f"  List pagination may be incomplete or permissions differ.", file=sys.stderr)
    
    # Compute differences
    diff_signed_list = new_list_count - old_list_count
    diff_abs_list = abs(diff_signed_list)
    
    diff_signed_search = None
    diff_abs_search = None
    if old_search_total is not None and new_search_total is not None:
        diff_signed_search = new_search_total - old_search_total
        diff_abs_search = abs(diff_signed_search)
    
    # Print summary
    print("\n" + "=" * 60, file=sys.stderr)
    print("RESULTS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"OLD contacts: list={old_list_count} search_total={old_search_total}", file=sys.stderr)
    print(f"NEW contacts: list={new_list_count} search_total={new_search_total}", file=sys.stderr)
    print(f"Difference (NEW-OLD): list={diff_signed_list:+d} search={diff_signed_search if diff_signed_search is not None else 'N/A'}", file=sys.stderr)
    
    # Build email sets for overlap analysis
    print("\n" + "=" * 60, file=sys.stderr)
    print("EMAIL OVERLAP (OLD vs NEW)", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    old_email_data = fetch_contact_email_set(old_token, "OLD", timeout=timeout, track_contact_ids=True)
    new_email_data = fetch_contact_email_set(new_token, "NEW", timeout=timeout, track_contact_ids=False)
    
    old_email_set = old_email_data['_email_set']
    new_email_set = new_email_data['_email_set']
    old_email_to_contact_id = old_email_data.get('_email_to_contact_id', {})
    
    # Compute overlap
    old_emails_present_in_new = old_email_set & new_email_set
    old_emails_missing_in_new = old_email_set - new_email_set
    
    old_contacts_without_email = old_email_data['contacts_total'] - old_email_data['emails_with_value']
    
    # Calculate percentage
    old_unique_emails_count = len(old_email_set)
    percent_missing = 0.0
    if old_unique_emails_count > 0:
        percent_missing = (len(old_emails_missing_in_new) / old_unique_emails_count) * 100.0
    
    # Print email overlap summary
    print(f"\nOLD: contacts_total={old_email_data['contacts_total']}, contacts_with_email={old_email_data['emails_with_value']}, unique_emails={old_unique_emails_count}", file=sys.stderr)
    print(f"NEW: contacts_total={new_email_data['contacts_total']}, contacts_with_email={new_email_data['emails_with_value']}, unique_emails={len(new_email_set)}", file=sys.stderr)
    print(f"OLD emails present in NEW: {len(old_emails_present_in_new)}", file=sys.stderr)
    print(f"OLD emails missing in NEW: {len(old_emails_missing_in_new)}", file=sys.stderr)
    print(f"Percent OLD emails missing in NEW: {percent_missing:.2f}%", file=sys.stderr)
    if old_contacts_without_email > 0:
        print(f"OLD contacts without email (uncomparable): {old_contacts_without_email}", file=sys.stderr)
    
    # Write CSV of missing emails
    csv_path = './out/old_contacts_missing_in_new.csv'
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    
    missing_emails_sorted = sorted(old_emails_missing_in_new)
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['email', 'old_contact_id'])
        
        for email in missing_emails_sorted:
            contact_id = old_email_to_contact_id.get(email, '')
            writer.writerow([email, contact_id])
    
    print(f"\nCSV results written to: {csv_path}", file=sys.stderr)
    
    # Prepare samples for JSON (up to 50 each)
    missing_sample = list(missing_emails_sorted[:50])
    present_sample = sorted(list(old_emails_present_in_new))[:50]
    
    # Write JSON output
    output = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'old': {
            'portal_details': old_details,
            'list': old_list_stats,
            'search': old_search_stats
        },
        'new': {
            'portal_details': new_details,
            'list': new_list_stats,
            'search': new_search_stats
        },
        'difference': {
            'signed_new_minus_old_list': diff_signed_list,
            'absolute_list': diff_abs_list
        },
        'email_overlap': {
            'old_contacts_total': old_email_data['contacts_total'],
            'old_contacts_with_email': old_email_data['emails_with_value'],
            'old_unique_emails': old_unique_emails_count,
            'new_unique_emails': len(new_email_set),
            'old_emails_present_in_new': len(old_emails_present_in_new),
            'old_emails_missing_in_new': len(old_emails_missing_in_new),
            'percent_old_emails_missing_in_new': round(percent_missing, 2)
        },
        'samples': {
            'old_emails_missing_in_new_sample': missing_sample,
            'old_emails_present_in_new_sample': present_sample
        }
    }
    
    # Add search-based differences if both totals are available
    if diff_signed_search is not None:
        output['difference']['signed_new_minus_old_search'] = diff_signed_search
        output['difference']['absolute_search'] = diff_abs_search
    
    # Add warnings if mismatched
    if old_mismatch or new_mismatch:
        output['warnings'] = []
        if old_mismatch:
            output['warnings'].append(f"OLD portal: list_count ({old_list_count}) != search_total ({old_search_total})")
        if new_mismatch:
            output['warnings'].append(f"NEW portal: list_count ({new_list_count}) != search_total ({new_search_total})")
    
    json_path = './out/db_difference.json'
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\nJSON results written to: {json_path}", file=sys.stderr)


if __name__ == '__main__':
    main()
