#!/usr/bin/env python3
"""
Verify Note Association Diagnostic Tool.

Performs two definitive checks for a given noteId/contactId in the NEW HubSpot portal:
1. Note exists (GET /crm/v3/objects/notes/{noteId})
2. Note is associated to contact (GET /crm/v3/objects/contacts/{contactId}/associations/notes)

Usage:
  python verify_note_association.py --note-id 309923788522 --contact-id 375933927155
  python verify_note_association.py --jsonl ./out/created_note_keys.jsonl --pick-email johnk@brandmd.com
"""

import argparse
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://api.hubapi.com"
DEFAULT_TIMEOUT = 30


def load_dotenv(path: str = '.env') -> dict:
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
    sys.exit(4)


def hubspot_get_diagnostic(url: str, params: dict = None, token: str = None, timeout: int = DEFAULT_TIMEOUT) -> tuple:
    """
    Make HTTP GET request to HubSpot API with diagnostic error handling.
    
    Returns: (success: bool, status: int, body_dict: dict, body_str: str)
    - success: True if 200, False otherwise
    - status: HTTP status code
    - body_dict: Parsed JSON dict (empty if parse fails)
    - body_str: Raw response body string
    
    Does NOT exit on errors - returns status/body for inspection.
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
            
            return (True, status, body_dict, body_str)
            
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
            
            # Handle retries for rate limits and server errors
            if status == 429:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"Rate limited (429). Waiting {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print("Error: Rate limit exceeded after retries.", file=sys.stderr)
                    return (False, status, body_dict, body_str)
            elif 500 <= status < 600:
                wait_time = 2.0 ** retry_count  # 1s, 2s, 4s, 8s
                if retry_count < max_retries - 1:
                    print(f"Server error ({status}). Retrying in {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"Error: Server error {status} after retries.", file=sys.stderr)
                    return (False, status, body_dict, body_str)
            else:
                # For 401/403/404/400/etc, return immediately (no retry)
                return (False, status, body_dict, body_str)
                
        except URLError as e:
            print(f"Error: Network error: {e.reason}", file=sys.stderr)
            return (False, 0, {}, str(e))
        except Exception as e:
            print(f"Error: Unexpected error: {e}", file=sys.stderr)
            return (False, 0, {}, str(e))
    
    # Should not reach here
    return (False, 0, {}, "Max retries exceeded")


def check_note_exists(note_id: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> tuple:
    """Check if note exists. Returns (success, status, body_dict, body_str)."""
    url = f"{BASE_URL}/crm/v3/objects/notes/{note_id}"
    params = {'properties': 'hs_note_body,hs_timestamp'}
    return hubspot_get_diagnostic(url, params=params, token=token, timeout=timeout)


def check_note_association(contact_id: str, note_id: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> tuple:
    """Check if note is associated to contact. Returns (success, status, body_dict, body_str, contains_note)."""
    url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}/associations/notes"
    params = {'limit': '500'}
    success, status, body_dict, body_str = hubspot_get_diagnostic(url, params=params, token=token, timeout=timeout)
    
    contains_note = False
    if success and body_dict:
        # Extract note IDs from results
        results = body_dict.get('results', [])
        returned_note_ids = []
        for result in results:
            # Results can be objects with 'id' or just strings
            if isinstance(result, dict):
                note_id_from_result = result.get('id', '')
            else:
                note_id_from_result = str(result)
            returned_note_ids.append(note_id_from_result)
            if note_id_from_result == note_id:
                contains_note = True
        
        # Store note IDs in body_dict for easier access
        body_dict['_returned_note_ids'] = returned_note_ids
    
    return (success, status, body_dict, body_str, contains_note)


def find_record_by_email(jsonl_path: str, email: str) -> dict:
    """Find first record in JSONL file matching email (case-insensitive)."""
    if not os.path.exists(jsonl_path):
        print(f"Error: JSONL file not found: {jsonl_path}", file=sys.stderr)
        sys.exit(4)
    
    email_lower = email.lower().strip()
    
    try:
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                    record_email = record.get('email', '').lower().strip()
                    if record_email == email_lower:
                        return record
                except json.JSONDecodeError as e:
                    print(f"Warning: Failed to parse line {line_num} in {jsonl_path}: {e}", file=sys.stderr)
                    continue
    except Exception as e:
        print(f"Error: Failed to read JSONL file: {e}", file=sys.stderr)
        sys.exit(4)
    
    print(f"Error: No record found with email '{email}' in {jsonl_path}", file=sys.stderr)
    sys.exit(4)


def main():
    parser = argparse.ArgumentParser(
        description='Verify note exists and is associated to contact in NEW HubSpot portal',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python verify_note_association.py --note-id 309923788522 --contact-id 375933927155
  python verify_note_association.py --jsonl ./out/created_note_keys.jsonl --pick-email johnk@brandmd.com
        """
    )
    
    # Direct mode
    parser.add_argument('--note-id', type=str, help='Note ID to verify')
    parser.add_argument('--contact-id', type=str, help='Contact ID to verify association')
    
    # Convenience mode
    parser.add_argument('--jsonl', type=str, help='JSONL file path (created_note_keys.jsonl)')
    parser.add_argument('--pick-email', type=str, help='Email to find in JSONL file')
    
    args = parser.parse_args()
    
    # Determine mode
    if args.jsonl and args.pick_email:
        # Convenience mode: find record by email
        record = find_record_by_email(args.jsonl, args.pick_email)
        note_id = record.get('noteId')
        contact_id = record.get('contactId')
        
        if not note_id or not contact_id:
            print(f"Error: Record found but missing noteId or contactId: {record}", file=sys.stderr)
            sys.exit(4)
        
        print(f"Found record for email: {record.get('email')}", file=sys.stderr)
        print(f"Using noteId: {note_id}, contactId: {contact_id}\n", file=sys.stderr)
        
    elif args.note_id and args.contact_id:
        # Direct mode
        note_id = args.note_id
        contact_id = args.contact_id
    else:
        parser.print_help()
        print("\nUsage hint:", file=sys.stderr)
        print("  Use --note-id and --contact-id for direct verification", file=sys.stderr)
        print("  OR use --jsonl and --pick-email to find a record automatically", file=sys.stderr)
        sys.exit(1)
    
    # Load token
    token = get_access_token()
    
    # Print header
    print("=" * 60)
    print("VERIFY NOTE + ASSOCIATION (NEW PORTAL)")
    print("=" * 60)
    print(f"noteId: {note_id}")
    print(f"contactId: {contact_id}")
    print()
    
    # [1] NOTE EXISTS CHECK
    print("[1] NOTE EXISTS CHECK")
    print("-" * 60)
    success, status, body_dict, body_str = check_note_exists(note_id, token)
    
    print(f"- HTTP status: {status}")
    
    if success:
        note_id_returned = body_dict.get('id', 'N/A')
        hs_timestamp = body_dict.get('properties', {}).get('hs_timestamp', 'N/A')
        hs_note_body = body_dict.get('properties', {}).get('hs_note_body', '')
        hs_note_body_preview = hs_note_body[:120] + '...' if len(hs_note_body) > 120 else hs_note_body
        
        print(f"- note id: {note_id_returned}")
        print(f"- hs_timestamp: {hs_timestamp}")
        print(f"- hs_note_body_preview: {hs_note_body_preview}")
        print()
        print("- Full response JSON:")
        print(json.dumps(body_dict, indent=2))
        print()
        
        note_exists = True
    else:
        print(f"- Error: Note not found or request failed")
        if body_str:
            print(f"- Response body: {body_str}")
        print()
        print("- Full response:")
        if body_dict:
            print(json.dumps(body_dict, indent=2))
        else:
            print(body_str)
        print()
        
        note_exists = False
        
        # Exit early if note doesn't exist
        if status == 404:
            print("Result: Note does not exist (404)")
            sys.exit(3)
        elif status == 401 or status == 403:
            print("Result: Authentication/Authorization error")
            sys.exit(4)
        else:
            print("Result: Note check failed")
            sys.exit(4)
    
    # [2] CONTACT -> NOTES ASSOCIATION CHECK
    print("[2] CONTACT -> NOTES ASSOCIATION CHECK")
    print("-" * 60)
    success, status, body_dict, body_str, contains_note = check_note_association(contact_id, note_id, token)
    
    print(f"- HTTP status: {status}")
    
    if success:
        returned_note_ids = body_dict.get('_returned_note_ids', [])
        returned_note_ids_count = len(returned_note_ids)
        
        print(f"- returned_note_ids_count: {returned_note_ids_count}")
        print(f"- contains_target_note: {contains_note}")
        
        if not contains_note and returned_note_ids_count > 0:
            print(f"- First 20 note IDs (for visibility):")
            for i, nid in enumerate(returned_note_ids[:20], 1):
                print(f"  {i}. {nid}")
            if returned_note_ids_count > 20:
                print(f"  ... and {returned_note_ids_count - 20} more")
        
        print()
        print("- Full response JSON:")
        print(json.dumps(body_dict, indent=2))
        print()
        
        if contains_note:
            print("Result: [SUCCESS] Note exists AND is associated to contact")
            sys.exit(0)
        else:
            print("Result: [FAIL] Note exists but is NOT associated to contact")
            sys.exit(2)
    else:
        print(f"- Error: Association check failed")
        if body_str:
            print(f"- Response body: {body_str}")
        print()
        print("- Full response:")
        if body_dict:
            print(json.dumps(body_dict, indent=2))
        else:
            print(body_str)
        print()
        
        if status == 401 or status == 403:
            print("Result: Authentication/Authorization error")
            sys.exit(4)
        else:
            print("Result: Association check failed")
            sys.exit(4)


if __name__ == '__main__':
    main()
