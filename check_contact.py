#!/usr/bin/env python3
"""
Check Contact Duplicate Notes Tool.

Detects duplicate form-submission notes for a contact in the NEW HubSpot portal.
Supports both plain-text and HTML note formats.

Usage:
  python check_contact.py --duplicate-note --email "nc.haldun@gmail.com"
  python check_contact.py --duplicate-note --email "nc.haldun@gmail.com" --out ./out/nc_haldun_duplicates.json
"""

import argparse
import hashlib
import html
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

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
                token: str = None, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Make HTTP GET request to HubSpot API with retry logic.
    
    Retries on 429/5xx with exponential backoff.
    Returns JSON dict.
    """
    if params is None:
        params = {}
    
    # Build URL with params
    if params:
        query_str = urlencode(params)
        if '?' in url:
            url_final = f"{url}&{query_str}"
        else:
            url_final = f"{url}?{query_str}"
    else:
        url_final = url
    
    # Create request
    request = Request(url_final)
    request.add_header('Authorization', f'Bearer {token}')
    request.add_header('Content-Type', 'application/json')
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = urlopen(request, timeout=timeout)
            status = response.getcode()
            
            body_bytes = response.read()
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            body_dict = {}
            if body_str:
                try:
                    body_dict = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            return body_dict
            
        except HTTPError as e:
            status = e.code
            body_bytes = e.read() if hasattr(e, 'read') else b''
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            body_dict = {}
            if body_str:
                try:
                    body_dict = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            if status == 401:
                print("Error: Unauthorized (401). Verify ACCESS_TOKEN is valid.", file=sys.stderr)
                sys.exit(2)
            elif status == 403:
                print("Error: Forbidden (403). Verify ACCESS_TOKEN has required scopes.", file=sys.stderr)
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
                if body_dict and 'message' in body_dict:
                    error_msg += f": {body_dict['message']}"
                print(error_msg, file=sys.stderr)
                sys.exit(2)
                
        except URLError as e:
            print(f"Error: Network error: {e.reason}", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"Error: Unexpected error: {e}", file=sys.stderr)
            sys.exit(2)
    
    # Should not reach here
    print("Error: Max retries exceeded", file=sys.stderr)
    sys.exit(2)


def hubspot_post(url: str, data: Dict[str, Any], token: str = None, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    """
    Make HTTP POST request to HubSpot API with retry logic.
    
    Retries on 429/5xx with exponential backoff.
    Returns JSON dict.
    """
    # Create request
    request = Request(url, data=json.dumps(data).encode('utf-8'))
    request.add_header('Authorization', f'Bearer {token}')
    request.add_header('Content-Type', 'application/json')
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = urlopen(request, timeout=timeout)
            status = response.getcode()
            
            body_bytes = response.read()
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            body_dict = {}
            if body_str:
                try:
                    body_dict = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            return body_dict
            
        except HTTPError as e:
            status = e.code
            body_bytes = e.read() if hasattr(e, 'read') else b''
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            body_dict = {}
            if body_str:
                try:
                    body_dict = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            if status == 401:
                print("Error: Unauthorized (401). Verify ACCESS_TOKEN is valid.", file=sys.stderr)
                sys.exit(2)
            elif status == 403:
                print("Error: Forbidden (403). Verify ACCESS_TOKEN has required scopes.", file=sys.stderr)
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
                if body_dict and 'message' in body_dict:
                    error_msg += f": {body_dict['message']}"
                print(error_msg, file=sys.stderr)
                sys.exit(2)
                
        except URLError as e:
            print(f"Error: Network error: {e.reason}", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"Error: Unexpected error: {e}", file=sys.stderr)
            sys.exit(2)
    
    # Should not reach here
    print("Error: Max retries exceeded", file=sys.stderr)
    sys.exit(2)


def find_contact_by_email(email: str, token: str) -> Optional[Dict[str, Any]]:
    """Find contact by email in NEW portal. Returns contact dict or None."""
    url = f"{BASE_URL}/crm/v3/objects/contacts/search"
    
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email
                    }
                ]
            }
        ],
        "properties": ["email"],
        "limit": 1
    }
    
    response = hubspot_post(url, payload, token=token)
    results = response.get('results', [])
    
    if results:
        return results[0]
    return None


def get_all_note_ids_for_contact(contact_id: str, token: str) -> List[str]:
    """Get all note IDs associated with a contact, with pagination."""
    note_ids = []
    after = None
    
    while True:
        params = {'limit': '500'}
        if after:
            params['after'] = after
        
        url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}/associations/notes"
        response = hubspot_get(url, params=params, token=token)
        
        results = response.get('results', [])
        for result in results:
            if isinstance(result, dict):
                note_id = result.get('id', '')
            else:
                note_id = str(result)
            if note_id:
                note_ids.append(note_id)
        
        # Check for next page
        paging = response.get('paging', {})
        next_info = paging.get('next', {})
        after = next_info.get('after')
        
        if not after:
            break
    
    return note_ids


def batch_read_notes(note_ids: List[str], token: str) -> Dict[str, Dict[str, Any]]:
    """Batch read note bodies for note IDs. Returns dict mapping note_id -> note_data."""
    notes_dict = {}
    
    # Chunk into batches of 100
    chunk_size = 100
    for i in range(0, len(note_ids), chunk_size):
        chunk = note_ids[i:i + chunk_size]
        
        url = f"{BASE_URL}/crm/v3/objects/notes/batch/read"
        payload = {
            "properties": ["hs_note_body", "hs_timestamp", "hs_createdate"],
            "inputs": [{"id": note_id} for note_id in chunk]
        }
        
        response = hubspot_post(url, payload, token=token)
        results = response.get('results', [])
        
        for result in results:
            note_id = result.get('id', '')
            if note_id:
                notes_dict[note_id] = result
        
        # Log any missing IDs
        returned_ids = {r.get('id') for r in results}
        missing = [nid for nid in chunk if nid not in returned_ids]
        if missing:
            print(f"Warning: {len(missing)} note IDs not returned in batch read", file=sys.stderr)
    
    return notes_dict


def normalize_url_simple(url: str) -> str:
    """Minimal URL normalization: strip whitespace, trailing slash, fragment."""
    if not url:
        return url
    
    url = url.strip()
    
    # Strip fragment
    if '#' in url:
        url = url.split('#')[0]
    
    # Strip trailing slash (but keep root URLs like https://arrsys.com)
    if url.endswith('/') and url.count('/') > 3:
        url = url.rstrip('/')
    
    return url


def parse_plain_note(body: str) -> Optional[Dict[str, Any]]:
    """Parse plain text note format. Returns signature dict or None."""
    if not body or "Website form submission" not in body:
        return None
    
    lines = body.split('\n')
    signature = {
        'page_url': None,
        'submitted_date': None,
        'form_guid': None,
        'responses': {}
    }
    
    in_responses = False
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        if line.startswith('Page:'):
            page_part = line[5:].strip()  # Remove "Page:"
            if page_part and page_part != "(unknown)":
                signature['page_url'] = normalize_url_simple(page_part)
        
        elif line.startswith('Submitted:'):
            date_part = line[10:].strip()  # Remove "Submitted:"
            if date_part and date_part != "(unknown)":
                signature['submitted_date'] = date_part.strip()
        
        elif line.startswith('Form:'):
            form_part = line[5:].strip()  # Remove "Form:"
            # Extract GUID from parentheses: "Form Name (guid)"
            match = re.search(r'\(([^)]+)\)', form_part)
            if match:
                signature['form_guid'] = match.group(1).strip()
        
        elif line == 'Responses:':
            in_responses = True
            continue
        
        elif in_responses and line.startswith('•'):
            # Parse "• Label: Value"
            parts = line[1:].strip().split(':', 1)
            if len(parts) == 2:
                label = parts[0].strip().lower()
                value = parts[1].strip()
                if label and value and label != 'email':  # Skip email as per requirements
                    signature['responses'][label] = value
    
    # Only return if we have at least page_url or submitted_date
    if signature['page_url'] or signature['submitted_date']:
        return signature
    
    return None


def parse_html_note(body: str) -> Optional[Dict[str, Any]]:
    """Parse HTML note format. Returns signature dict or None."""
    if not body or "Website form submission" not in body:
        return None
    
    # Step 1: Unescape HTML entities first
    body = html.unescape(body)
    
    # Step 2: Extract <li>...</li> blocks BEFORE stripping all tags
    li_items = []
    li_pattern = r'<li[^>]*>(.*?)</li>'
    for match in re.finditer(li_pattern, body, re.DOTALL | re.IGNORECASE):
        li_content = match.group(1)
        # Remove tags inside this <li> only (like <strong>, etc.)
        li_text = re.sub(r'<[^>]+>', '', li_content)
        li_text = li_text.strip()
        if li_text:
            li_items.append(li_text)
    
    # Step 3: Convert <br> and <br/> to newlines for header parsing
    body_for_headers = re.sub(r'<br\s*/?>', '\n', body, flags=re.IGNORECASE)
    
    # Step 4: Remove all HTML tags for header extraction
    body_for_headers = re.sub(r'<[^>]+>', '', body_for_headers)
    
    # Step 5: Parse header fields (Page, Submitted, Form)
    header_lines = body_for_headers.split('\n')
    signature = {
        'page_url': None,
        'submitted_date': None,
        'form_guid': None,
        'responses': {}
    }
    
    for line in header_lines:
        line = line.strip()
        if not line:
            continue
        
        if line.startswith('Page:'):
            page_part = line[5:].strip()  # Remove "Page:"
            if page_part and page_part != "(unknown)":
                signature['page_url'] = normalize_url_simple(page_part)
        
        elif line.startswith('Submitted:'):
            date_part = line[10:].strip()  # Remove "Submitted:"
            if date_part and date_part != "(unknown)":
                signature['submitted_date'] = date_part.strip()
        
        elif line.startswith('Form:'):
            form_part = line[5:].strip()  # Remove "Form:"
            # Extract GUID from parentheses: "Form Name (guid)"
            match = re.search(r'\(([^)]+)\)', form_part)
            if match:
                signature['form_guid'] = match.group(1).strip()
    
    # Step 6: Parse response items from <li> blocks
    for li_text in li_items:
        # Split on the first ':' to get label/value
        if ':' in li_text:
            parts = li_text.split(':', 1)
            if len(parts) == 2:
                label = parts[0].strip().lower()
                value = parts[1].strip()
                
                # Skip if it's the "no additional fields" message
                if '(no additional fields captured)' in label:
                    continue
                
                # Skip email as per requirements
                if label and value and label != 'email':
                    signature['responses'][label] = value
    
    # Only return if we have at least page_url or submitted_date
    if signature['page_url'] or signature['submitted_date']:
        return signature
    
    return None


def detect_note_format(body: str) -> str:
    """Detect note format: 'plain', 'html', or 'unknown'."""
    if not body:
        return 'unknown'
    
    if '<strong>' in body or '<br>' in body or '<ul>' in body:
        return 'html'
    elif 'Website form submission' in body:
        return 'plain'
    else:
        return 'unknown'


def compute_signature_keys(signature: Dict[str, Any]) -> tuple:
    """Compute strict and day keys for a signature."""
    # Build responses dict without email, sorted
    responses_sorted = dict(sorted(signature['responses'].items()))
    
    # Strict key: page + submitted_date + responses
    strict_payload = {
        "page": signature.get('page_url', ''),
        "submitted": signature.get('submitted_date', ''),
        "responses": responses_sorted
    }
    strict_json = json.dumps(strict_payload, sort_keys=True, separators=(',', ':'))
    strict_key = hashlib.sha256(strict_json.encode('utf-8')).hexdigest()
    
    # Day key: page|submitted_date
    day_key = f"{signature.get('page_url', '')}|{signature.get('submitted_date', '')}"
    
    return (strict_key, day_key)


def main():
    parser = argparse.ArgumentParser(
        description='Check for duplicate form-submission notes for a contact',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--duplicate-note', action='store_true', required=True,
                       help='Flag to enable duplicate note checking')
    parser.add_argument('--email', type=str, required=True,
                       help='Contact email address to check')
    parser.add_argument('--out', type=str, help='Optional JSON output file path')
    
    args = parser.parse_args()
    
    if not args.duplicate_note:
        parser.print_help()
        sys.exit(1)
    
    # Load token
    token = get_access_token()
    
    # Step 1: Find contact
    print(f"Looking up contact: {args.email}", file=sys.stderr)
    contact = find_contact_by_email(args.email, token)
    
    if not contact:
        print(f"Error: Contact not found with email '{args.email}'", file=sys.stderr)
        sys.exit(1)
    
    contact_id = contact.get('id', '')
    contact_email = contact.get('properties', {}).get('email', args.email)
    
    print(f"Found contact: {contact_email} (ID: {contact_id})", file=sys.stderr)
    
    # Step 2: Get all note IDs
    print("Fetching note associations...", file=sys.stderr)
    note_ids = get_all_note_ids_for_contact(contact_id, token)
    total_note_ids = len(note_ids)
    print(f"Found {total_note_ids} note IDs", file=sys.stderr)
    
    if not note_ids:
        print("No notes found for this contact.")
        sys.exit(0)
    
    # Step 3: Batch read note bodies
    print("Reading note bodies...", file=sys.stderr)
    notes_dict = batch_read_notes(note_ids, token)
    
    # Step 4: Identify and parse candidate notes
    candidate_notes = []
    parse_failed_note_ids = []
    
    for note_id, note_data in notes_dict.items():
        body = note_data.get('properties', {}).get('hs_note_body', '')
        if not body or "Website form submission" not in body:
            continue
        
        format_type = detect_note_format(body)
        signature = None
        
        if format_type == 'plain':
            signature = parse_plain_note(body)
        elif format_type == 'html':
            signature = parse_html_note(body)
        
        if signature:
            # Compute keys
            strict_key, day_key = compute_signature_keys(signature)
            
            candidate_notes.append({
                'noteId': note_id,
                'format': format_type,
                'page_url': signature.get('page_url'),
                'submitted_date': signature.get('submitted_date'),
                'form_guid': signature.get('form_guid'),
                'responses': signature.get('responses', {}),
                'strict_key': strict_key,
                'day_key': day_key,
                'hs_timestamp': note_data.get('properties', {}).get('hs_timestamp'),
                'hs_createdate': note_data.get('properties', {}).get('hs_createdate')
            })
        else:
            parse_failed_note_ids.append(note_id)
    
    print(f"\nCandidate form submission notes: {len(candidate_notes)}", file=sys.stderr)
    print(f"Parsed successfully: {len(candidate_notes)}, parse_failed: {len(parse_failed_note_ids)}", file=sys.stderr)
    
    # Step 5: Detect duplicates
    # Group by strict_key
    strict_groups: Dict[str, List[Dict[str, Any]]] = {}
    for note in candidate_notes:
        strict_key = note['strict_key']
        if strict_key not in strict_groups:
            strict_groups[strict_key] = []
        strict_groups[strict_key].append(note)
    
    # Group by day_key
    day_groups: Dict[str, List[Dict[str, Any]]] = {}
    for note in candidate_notes:
        day_key = note['day_key']
        if day_key not in day_groups:
            day_groups[day_key] = []
        day_groups[day_key].append(note)
    
    # Find duplicates
    duplicates_strict = []
    for strict_key, notes in strict_groups.items():
        if len(notes) > 1:
            formats = list(set(n['format'] for n in notes))
            duplicates_strict.append({
                'strict_key': strict_key,
                'count': len(notes),
                'page_url': notes[0]['page_url'],
                'submitted_date': notes[0]['submitted_date'],
                'formats': formats,
                'noteIds': [n['noteId'] for n in notes]
            })
    
    duplicates_day = []
    for day_key, notes in day_groups.items():
        if len(notes) > 1:
            duplicates_day.append({
                'day_key': day_key,
                'count': len(notes),
                'page_url': notes[0]['page_url'],
                'submitted_date': notes[0]['submitted_date'],
                'noteIds': [n['noteId'] for n in notes]
            })
    
    # Step 6: Print report
    print("\n" + "=" * 60)
    print("DUPLICATE NOTE REPORT")
    print("=" * 60)
    print(f"Contact email: {contact_email}")
    print(f"Contact ID: {contact_id}")
    print(f"Total notes associated: {total_note_ids}")
    print(f"Candidate form submission notes: {len(candidate_notes)}")
    print(f"Parsed successfully: {len(candidate_notes)}, parse_failed: {len(parse_failed_note_ids)}")
    print()
    
    if duplicates_strict:
        print("EXACT DUPLICATES (same content):")
        print("-" * 60)
        for dup in duplicates_strict:
            print(f"  Count: {dup['count']}")
            print(f"  Page: {dup['page_url']}")
            print(f"  Submitted: {dup['submitted_date']}")
            print(f"  Formats: {', '.join(dup['formats'])}")
            print(f"  Note IDs: {', '.join(dup['noteIds'])}")
            print()
    else:
        print("No exact duplicates detected.")
        print()
    
    if duplicates_day:
        print("SAME-DAY DUPLICATES (same page + date):")
        print("-" * 60)
        for dup in duplicates_day:
            print(f"  Count: {dup['count']}")
            print(f"  Page: {dup['page_url']}")
            print(f"  Date: {dup['submitted_date']}")
            print(f"  Note IDs: {', '.join(dup['noteIds'])}")
            print()
    else:
        print("No same-day duplicates detected.")
        print()
    
    # Step 7: Write JSON output if requested
    if args.out:
        output_data = {
            'email': contact_email,
            'contactId': contact_id,
            'total_note_ids': total_note_ids,
            'candidate_notes': candidate_notes,
            'duplicates_strict': duplicates_strict,
            'duplicates_day': duplicates_day,
            'parse_failed_note_ids': parse_failed_note_ids
        }
        
        os.makedirs(os.path.dirname(args.out) if os.path.dirname(args.out) else '.', exist_ok=True)
        with open(args.out, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
        
        print(f"Output written to: {args.out}", file=sys.stderr)


if __name__ == '__main__':
    main()
