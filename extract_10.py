#!/usr/bin/env python3
"""
Extract and sample HubSpot conversation threads with full message history.

Usage examples:
  # Set OLD_ACCESS_TOKEN in .env file or environment:
  # OLD_ACCESS_TOKEN=pat-xxxxx (in .env file)
  # OR: export OLD_ACCESS_TOKEN="pat-xxxxx"
  
  python extract_10.py --pool-size 200 --sample-size 10 --out-dir out
  python extract_10.py --inbox-id 12345 --seed 7 --include-thread-details
  python extract_10.py --full-thread --scan-limit 800 --inbox-id 147959634
  python extract_10.py --full-thread --require-email-or-phone
  python extract_10.py --find-by-email afrys@thesugarart.com --print-curl --thread-status both --pick latest --out-dir out
"""

import argparse
import html
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Dict, List, Optional, Any, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen


BASE_URL = "https://api.hubapi.com"
DEFAULT_RATE_LIMIT_DELAY = 0.5  # seconds between API calls

# Request log for curl printing
_request_log: List[Dict[str, Any]] = []

# Required bot prompts that must appear in order for a "true chatbot conversation"
# Note: trailing punctuation removed for robust matching
REQUIRED_PROMPTS = [
    "what are you looking for",
    "what is your name",
    "what is a good email address to contact you with",
    "what is your country/region",
    "what is your good contact number to contact you with",
    "our team member will contact you shortly"
]


class HTMLStripper(HTMLParser):
    """HTML parser to extract text content."""
    
    def __init__(self):
        super().__init__()
        self.text = []
        self.in_script = False
        self.in_style = False
    
    def handle_starttag(self, tag, attrs):
        tag_lower = tag.lower()
        if tag_lower in ('script', 'style'):
            self.in_script = tag_lower == 'script'
            self.in_style = tag_lower == 'style'
        elif tag_lower in ('br', 'p', 'div', 'li'):
            self.text.append('\n')
    
    def handle_endtag(self, tag):
        tag_lower = tag.lower()
        if tag_lower in ('script', 'style'):
            self.in_script = False
            self.in_style = False
        elif tag_lower in ('p', 'div', 'li'):
            self.text.append('\n')
    
    def handle_data(self, data):
        if not (self.in_script or self.in_style):
            self.text.append(data)
    
    def get_text(self):
        return ''.join(self.text)


def strip_html(s: str) -> str:
    """Strip HTML tags and convert to plain text."""
    if not s:
        return ""
    
    # Convert block-level HTML tags to newlines before parsing
    s = re.sub(r'<br\s*/?>', '\n', s, flags=re.IGNORECASE)
    s = re.sub(r'</p>', '\n', s, flags=re.IGNORECASE)
    s = re.sub(r'</div>', '\n', s, flags=re.IGNORECASE)
    s = re.sub(r'<div>', '\n', s, flags=re.IGNORECASE)
    
    # Parse HTML
    parser = HTMLStripper()
    parser.feed(s)
    text = parser.get_text()
    
    # Decode HTML entities
    text = html.unescape(text)
    
    return text


def normalize(s: str) -> str:
    """Normalize text: lowercase, replace NBSP, collapse whitespace, trim."""
    if not s:
        return ""
    
    # Replace NBSP with space
    s = s.replace('\u00A0', ' ')
    s = s.replace('\u2000', ' ')
    s = s.replace('\u2001', ' ')
    s = s.replace('\u2002', ' ')
    s = s.replace('\u2003', ' ')
    s = s.replace('\u2004', ' ')
    s = s.replace('\u2005', ' ')
    s = s.replace('\u2006', ' ')
    s = s.replace('\u2007', ' ')
    s = s.replace('\u2008', ' ')
    s = s.replace('\u2009', ' ')
    s = s.replace('\u200A', ' ')
    s = s.replace('\u202F', ' ')
    s = s.replace('\u205F', ' ')
    
    # Lowercase
    s = s.lower()
    
    # Collapse whitespace within lines (spaces/tabs to single space)
    lines = []
    for line in s.split('\n'):
        line = re.sub(r'[\t ]+', ' ', line)
        lines.append(line)
    
    # Trim
    s = '\n'.join(lines).strip()
    
    # Collapse >2 blank lines to max 2
    s = re.sub(r'\n{3,}', '\n\n', s)
    
    return s


def normalize_for_match(text: str) -> str:
    """
    Normalize text for prompt matching.
    
    - lowercases
    - html-unescapes entities
    - replaces NBSP with space
    - strips HTML tags (convert <br>, </p>, </div>, <div> to newlines first)
    - collapses runs of whitespace to single spaces
    - trims
    - normalizes slash spacing: " / " or "/ " or " /" -> "/"
    - removes repeated punctuation spacing (optional)
    - converts newlines to spaces for single-line matching
    """
    if not text:
        return ""
    
    # Strip HTML tags first (convert block tags to newlines)
    text = strip_html(text)
    
    # Decode HTML entities
    text = html.unescape(text)
    
    # Replace NBSP and other unicode spaces with regular space
    text = text.replace('\u00A0', ' ')
    text = text.replace('\u2000', ' ')
    text = text.replace('\u2001', ' ')
    text = text.replace('\u2002', ' ')
    text = text.replace('\u2003', ' ')
    text = text.replace('\u202F', ' ')
    text = text.replace('\u205F', ' ')
    
    # Lowercase
    text = text.lower()
    
    # Normalize slash spacing: " / " or "/ " or " /" -> "/"
    text = re.sub(r'\s*/\s*', '/', text)
    
    # Collapse all whitespace (spaces, tabs, newlines) to single spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Trim
    text = text.strip()
    
    return text


def message_text(msg: Dict[str, Any]) -> str:
    """Extract text from message: prefer text, else richText, else ""."""
    text = msg.get("text", "")
    if text:
        return text
    
    rich_text = msg.get("richText", "")
    if rich_text:
        return rich_text
    
    return ""


class HTMLStripper(HTMLParser):
    """HTML parser to extract text content."""
    
    def __init__(self):
        super().__init__()
        self.text = []
        self.in_script = False
        self.in_style = False
    
    def handle_starttag(self, tag, attrs):
        tag_lower = tag.lower()
        if tag_lower in ('script', 'style'):
            self.in_script = tag_lower == 'script'
            self.in_style = tag_lower == 'style'
        elif tag_lower in ('br', 'p', 'div', 'li'):
            self.text.append('\n')
    
    def handle_endtag(self, tag):
        tag_lower = tag.lower()
        if tag_lower in ('script', 'style'):
            self.in_script = False
            self.in_style = False
        elif tag_lower in ('p', 'div', 'li'):
            self.text.append('\n')
    
    def handle_data(self, data):
        if not (self.in_script or self.in_style):
            self.text.append(data)
    
    def get_text(self):
        return ''.join(self.text)


def hubspot_get(path: str, params: Optional[Dict[str, Any]] = None, token: str = None) -> Dict[str, Any]:
    """
    Make a GET request to HubSpot API.
    
    Always uses OLD_ACCESS_TOKEN from get_old_access_token() (token parameter is ignored).
    
    Handles:
    - 401/403: print error and exit 2
    - 404: return {}
    - 429: read Retry-After, sleep, retry up to 5 times
    - 5xx: exponential backoff, retry up to 3 times
    """
    # Always use OLD_ACCESS_TOKEN
    token, _ = get_old_access_token()
    
    if params is None:
        params = {}
    
    # Build URL
    parsed = urlparse(BASE_URL + path)
    query_params = {}
    if parsed.query:
        # Parse existing query string
        for pair in parsed.query.split('&'):
            if pair and '=' in pair:
                key, value = pair.split('=', 1)
                query_params[key] = value
    query_params.update(params)
    
    query_string = urlencode(query_params)
    full_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        query_string,
        parsed.fragment
    ))
    
    # Create request
    req = Request(full_url)
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Accept', 'application/json')
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            with urlopen(req, timeout=30) as response:
                status = response.getcode()
                
                if status == 200:
                    body = response.read().decode('utf-8')
                    return json.loads(body) if body else {}
                elif status == 404:
                    return {}
                elif status == 401:
                    print("Error: Authentication failed (401). Check your access token.", file=sys.stderr)
                    sys.exit(2)
                elif status == 403:
                    print("Error: Access forbidden (403). Check your token permissions.", file=sys.stderr)
                    sys.exit(2)
                elif status == 429:
                    # Rate limited - check Retry-After header
                    retry_after = response.headers.get('Retry-After', '1')
                    try:
                        wait_time = float(retry_after)
                    except ValueError:
                        wait_time = 2.0 * (retry_count + 1)  # Fallback exponential
                    
                    if retry_count < max_retries - 1:
                        print(f"Rate limited (429). Waiting {wait_time:.1f}s before retry {retry_count + 1}/{max_retries}...", file=sys.stderr)
                        time.sleep(wait_time)
                        retry_count += 1
                        continue
                    else:
                        print("Error: Rate limit exceeded after retries.", file=sys.stderr)
                        sys.exit(2)
                elif 500 <= status < 600:
                    # Server error - exponential backoff
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    if retry_count < 2:  # Only retry 5xx up to 3 times
                        print(f"Server error ({status}). Retrying in {wait_time:.1f}s...", file=sys.stderr)
                        time.sleep(wait_time)
                        retry_count += 1
                        continue
                    else:
                        print(f"Error: Server error {status} after retries.", file=sys.stderr)
                        sys.exit(2)
                else:
                    print(f"Error: Unexpected status {status}", file=sys.stderr)
                    sys.exit(2)
                    
        except HTTPError as e:
            status = e.code
            if status == 401:
                print("Error: Authentication failed (401). Check your access token.", file=sys.stderr)
                sys.exit(2)
            elif status == 403:
                print("Error: Access forbidden (403). Check your token permissions.", file=sys.stderr)
                sys.exit(2)
            elif status == 404:
                return {}
            elif status == 429:
                retry_after = e.headers.get('Retry-After', '1')
                try:
                    wait_time = float(retry_after)
                except ValueError:
                    wait_time = 2.0 * (retry_count + 1)
                
                if retry_count < max_retries - 1:
                    print(f"Rate limited (429). Waiting {wait_time:.1f}s before retry {retry_count + 1}/{max_retries}...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print("Error: Rate limit exceeded after retries.", file=sys.stderr)
                    sys.exit(2)
            elif 500 <= status < 600:
                wait_time = (2 ** retry_count) + random.uniform(0, 1)
                if retry_count < 2:
                    print(f"Server error ({status}). Retrying in {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"Error: Server error {status} after retries.", file=sys.stderr)
                    sys.exit(2)
            else:
                print(f"Error: HTTP {status}: {e.reason}", file=sys.stderr)
                sys.exit(2)
        except URLError as e:
            print(f"Error: Network error: {e.reason}", file=sys.stderr)
            sys.exit(2)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON response: {e}", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"Error: Unexpected error: {e}", file=sys.stderr)
            sys.exit(2)
    
    return {}


def hubspot_request(method: str, path: str, params: Optional[Dict[str, Any]] = None, 
                    body: Optional[Dict[str, Any]] = None, token: str = None) -> Tuple[int, Dict[str, str], Dict[str, Any]]:
    """
    Make a request to HubSpot API and return (status_code, headers_dict, json_dict).
    
    Also logs the request for curl printing.
    
    Always uses OLD_ACCESS_TOKEN from get_old_access_token() (token parameter is ignored).
    
    Handles:
    - 401/403: print error and exit 2
    - 429: read Retry-After, sleep, retry up to 5 times
    - 5xx: exponential backoff, retry up to 3 times
    - Other 4xx: print error body and exit 2 (except 404 where we return empty dict)
    """
    # Always use OLD_ACCESS_TOKEN
    token, _ = get_old_access_token()
    
    if params is None:
        params = {}
    
    # Build URL
    parsed = urlparse(BASE_URL + path)
    query_params = {}
    if parsed.query:
        # Parse existing query string
        for pair in parsed.query.split('&'):
            if pair and '=' in pair:
                key, value = pair.split('=', 1)
                query_params[key] = value
    query_params.update(params)
    
    query_string = urlencode(query_params)
    full_url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        query_string,
        parsed.fragment
    ))
    
    # Create request
    req = Request(full_url)
    req.add_header('Authorization', f'Bearer {token}')
    req.add_header('Accept', 'application/json')
    
    if body is not None:
        req.add_header('Content-Type', 'application/json')
        req.data = json.dumps(body).encode('utf-8')
        req.method = method
    else:
        req.method = method
    
    # Log request
    _request_log.append({
        'method': method,
        'url': full_url,
        'body': body
    })
    
    max_retries = 5 if method == 'GET' else 3  # Fewer retries for POST
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            with urlopen(req, timeout=30) as response:
                status = response.getcode()
                headers = dict(response.headers)
                body_bytes = response.read()
                body_str = body_bytes.decode('utf-8') if body_bytes else ''
                
                # Parse JSON if present
                json_data = {}
                if body_str:
                    try:
                        json_data = json.loads(body_str) if body_str else {}
                    except json.JSONDecodeError:
                        pass
                
                if status == 200:
                    return (status, headers, json_data)
                elif status == 404:
                    return (status, headers, {})
                elif status == 401:
                    print("Error: Authentication failed (401). Check your access token.", file=sys.stderr)
                    sys.exit(2)
                elif status == 403:
                    print("Error: Access forbidden (403). Check your token permissions.", file=sys.stderr)
                    sys.exit(2)
                elif status == 429:
                    # Rate limited - check Retry-After header
                    retry_after = headers.get('Retry-After', '1')
                    try:
                        wait_time = float(retry_after)
                    except ValueError:
                        wait_time = 2.0 * (retry_count + 1)  # Fallback exponential
                    
                    if retry_count < max_retries - 1:
                        print(f"Rate limited (429). Waiting {wait_time:.1f}s before retry {retry_count + 1}/{max_retries}...", file=sys.stderr)
                        time.sleep(wait_time)
                        retry_count += 1
                        continue
                    else:
                        print("Error: Rate limit exceeded after retries.", file=sys.stderr)
                        sys.exit(2)
                elif 500 <= status < 600:
                    # Server error - exponential backoff
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    max_retries_5xx = 3
                    if retry_count < max_retries_5xx - 1:
                        print(f"Server error ({status}). Retrying in {wait_time:.1f}s...", file=sys.stderr)
                        time.sleep(wait_time)
                        retry_count += 1
                        continue
                    else:
                        print(f"Error: Server error {status} after retries.", file=sys.stderr)
                        sys.exit(2)
                else:
                    # Other 4xx or unexpected status
                    error_msg = f"Error: HTTP {status}"
                    if json_data and 'message' in json_data:
                        error_msg += f": {json_data['message']}"
                    if body_str:
                        error_msg += f"\nResponse body: {body_str[:500]}"
                    print(error_msg, file=sys.stderr)
                    sys.exit(2)
                    
        except HTTPError as e:
            status = e.code
            headers = dict(e.headers) if e.headers else {}
            body_bytes = e.read() if hasattr(e, 'read') else b''
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            json_data = {}
            if body_str:
                try:
                    json_data = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            if status == 401:
                print("Error: Authentication failed (401). Check your access token.", file=sys.stderr)
                sys.exit(2)
            elif status == 403:
                print("Error: Access forbidden (403). Check your token permissions.", file=sys.stderr)
                sys.exit(2)
            elif status == 404:
                return (status, headers, {})
            elif status == 429:
                retry_after = headers.get('Retry-After', '1')
                try:
                    wait_time = float(retry_after)
                except ValueError:
                    wait_time = 2.0 * (retry_count + 1)
                
                if retry_count < max_retries - 1:
                    print(f"Rate limited (429). Waiting {wait_time:.1f}s before retry {retry_count + 1}/{max_retries}...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print("Error: Rate limit exceeded after retries.", file=sys.stderr)
                    sys.exit(2)
            elif 500 <= status < 600:
                wait_time = (2 ** retry_count) + random.uniform(0, 1)
                max_retries_5xx = 3
                if retry_count < max_retries_5xx - 1:
                    print(f"Server error ({status}). Retrying in {wait_time:.1f}s...", file=sys.stderr)
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"Error: Server error {status} after retries.", file=sys.stderr)
                    sys.exit(2)
            else:
                error_msg = f"Error: HTTP {status}: {e.reason}"
                if json_data and 'message' in json_data:
                    error_msg += f": {json_data['message']}"
                if body_str:
                    error_msg += f"\nResponse body: {body_str[:500]}"
                print(error_msg, file=sys.stderr)
                sys.exit(2)
        except URLError as e:
            print(f"Error: Network error: {e.reason}", file=sys.stderr)
            sys.exit(2)
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON response: {e}", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"Error: Unexpected error: {e}", file=sys.stderr)
            sys.exit(2)
    
    return (500, {}, {})


def list_threads(pool_size: int, inbox_id: Optional[str] = None, archived: bool = False,
                 max_pages: int = 5, token: str = None) -> List[Dict[str, Any]]:
    """
    List conversation threads with paging.
    
    Returns list of thread objects up to pool_size or max_pages limit.
    """
    threads = []
    after = None
    page_count = 0
    
    print(f"Collecting thread pool (target: {pool_size} threads)...", file=sys.stderr)
    
    while len(threads) < pool_size and page_count < max_pages:
        params = {
            'limit': min(100, pool_size - len(threads)),  # API limit is typically 100
            'archived': str(archived).lower()
        }
        
        if inbox_id:
            params['inboxId'] = inbox_id
        
        if after:
            params['after'] = after
        
        response = hubspot_get('/conversations/v3/conversations/threads', params=params, token=token)
        
        # Rate limit delay between calls
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        
        if not response:
            break
        
        # Extract threads from response
        results = response.get('results', [])
        threads.extend(results)
        
        print(f"  Collected {len(threads)} threads so far...", file=sys.stderr)
        
        # Check for next page - normalize cursor to RAW (decoded) form
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        
        # Decode cursor to raw form (handle both encoded and raw cursors)
        after = unquote(next_after_encoded) if next_after_encoded else None
        
        if not after:
            break
        
        page_count += 1
    
    print(f"Collected {len(threads)} threads total.", file=sys.stderr)
    return threads


def list_threads_until_limit(scan_limit: int, inbox_id: Optional[str] = None, archived: bool = False,
                             token: str = None) -> List[Dict[str, Any]]:
    """
    List conversation threads with paging until scan_limit is reached.
    
    For --full-thread mode: keeps requesting pages until scanned == scan_limit or no more pages.
    Ignores pool_size as stopping condition.
    """
    threads = []
    after = None
    
    print(f"Collecting threads for scanning (target: {scan_limit} threads)...", file=sys.stderr)
    
    while len(threads) < scan_limit:
        params = {
            'limit': min(100, scan_limit - len(threads)),  # API limit is typically 100
            'archived': str(archived).lower()
        }
        
        if inbox_id:
            params['inboxId'] = inbox_id
        
        if after:
            params['after'] = after
        
        response = hubspot_get('/conversations/v3/conversations/threads', params=params, token=token)
        
        # Rate limit delay between calls
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        
        if not response:
            break
        
        # Extract threads from response
        results = response.get('results', [])
        threads.extend(results)
        
        print(f"  Collected {len(threads)} threads so far...", file=sys.stderr)
        
        # Check for next page - normalize cursor to RAW (decoded) form
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        
        # Decode cursor to raw form (handle both encoded and raw cursors)
        after = unquote(next_after_encoded) if next_after_encoded else None
        
        if not after:
            break
    
    # Trim to scan_limit if we got more
    threads = threads[:scan_limit]
    
    print(f"Collected {len(threads)} threads total (for scanning).", file=sys.stderr)
    return threads


def get_thread_messages(thread_id: str, token: str = None) -> Dict[str, Any]:
    """Get all messages for a thread (first page only, for backward compatibility)."""
    path = f'/conversations/v3/conversations/threads/{thread_id}/messages'
    response = hubspot_get(path, token=token)
    time.sleep(DEFAULT_RATE_LIMIT_DELAY)
    return response


def get_thread_messages_firstpage(thread_id: str, token: str = None) -> Dict[str, Any]:
    """Get first page of messages for a thread."""
    path = f'/conversations/v3/conversations/threads/{thread_id}/messages'
    response = hubspot_get(path, token=token)
    time.sleep(DEFAULT_RATE_LIMIT_DELAY)
    return response


def get_thread_messages_all(thread_id: str, token: str = None) -> Dict[str, Any]:
    """
    Get ALL messages for a thread, handling pagination.
    
    Uses limit=100 per page if supported.
    
    Returns a dict with:
    {
      "results": <all_results_combined>,
      "paging": <optional final paging>,
      "_pagesFetched": <int>
    }
    """
    all_results = []
    after = None
    pages_fetched = 0
    final_paging = None
    
    path = f'/conversations/v3/conversations/threads/{thread_id}/messages'
    
    while True:
        params = {'limit': 100}  # Use 100 per page
        if after:
            params['after'] = after
        
        response = hubspot_get(path, params=params, token=token)
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        pages_fetched += 1
        
        if not response:
            break
        
        # Extract results
        results = response.get('results', [])
        all_results.extend(results)
        
        # Check for next page - normalize cursor to RAW (decoded) form
        paging = response.get('paging', {})
        final_paging = paging
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        
        # Decode cursor to raw form (handle both encoded and raw cursors)
        after = unquote(next_after_encoded) if next_after_encoded else None
        
        if not after:
            break
    
    return {
        'results': all_results,
        'paging': final_paging,
        '_pagesFetched': pages_fetched
    }


def get_thread_details(thread_id: str, token: str = None) -> Dict[str, Any]:
    """Get detailed thread information."""
    path = f'/conversations/v3/conversations/threads/{thread_id}'
    response = hubspot_get(path, token=token)
    time.sleep(DEFAULT_RATE_LIMIT_DELAY)
    return response


def is_incoming_customer(msg: Dict[str, Any]) -> bool:
    """Check if message is from a customer (incoming)."""
    # True if direction is INCOMING
    if msg.get('direction') == 'INCOMING':
        return True
    
    # OR any sender.actorId startswith "V-"
    senders = msg.get('senders', [])
    for sender in senders:
        actor_id = sender.get('actorId', '')
        if actor_id.startswith('V-'):
            return True
    
    return False


def is_botlike_outgoing(msg: Dict[str, Any]) -> bool:
    """
    Check if message is a bot prompt candidate.
    
    Only considers messages of type in {"MESSAGE","WELCOME_MESSAGE"}.
    
    A message is "bot prompt candidate" if any of:
    - any sender.actorId startswith "B-"   (bot/chatflow identity)
    - OR msg.get("createdBy","") startswith "B-"
    - OR msg.get("type") == "WELCOME_MESSAGE"
    - OR (any sender.actorId startswith "S-" and msg.get("direction") == "OUTGOING")  (fallback)
    
    Do NOT treat B- as "Agent" here; treat it as bot.
    """
    msg_type = msg.get('type', '')
    
    # Only consider MESSAGE and WELCOME_MESSAGE types
    if msg_type not in {'MESSAGE', 'WELCOME_MESSAGE'}:
        return False
    
    # Get sender actor IDs
    senders = msg.get('senders', [])
    sender_actor_ids = [s.get('actorId', '') for s in senders]
    
    # Check if any actorId startswith "B-"
    for actor_id in sender_actor_ids:
        if actor_id and actor_id.startswith('B-'):
            return True
    
    # OR createdBy startswith "B-"
    created_by = msg.get('createdBy', '')
    if created_by and created_by.startswith('B-'):
        return True
    
    # OR type is WELCOME_MESSAGE
    if msg_type == 'WELCOME_MESSAGE':
        return True
    
    # OR (any actorId startswith "S-" and direction == "OUTGOING") - fallback
    if msg.get('direction') == 'OUTGOING':
        for actor_id in sender_actor_ids:
            if actor_id and actor_id.startswith('S-'):
                return True
    
    return False


def find_required_prompts(bot_prompt_lines: List[Tuple[int, str, str, str, str]], max_msgs_to_check: int = 60) -> Dict[str, Any]:
    """
    Find required prompts in bot_prompt_lines as an ordered subsequence.
    
    Args:
        bot_prompt_lines: List of (message_index, normalized_text, raw_text_preview, createdAt, raw_text) tuples
        max_msgs_to_check: Maximum message index to check (default 60)
    
    Returns:
        {
            "matched": bool,
            "matches": [
                {
                    "prompt": str,
                    "messageIndex": int,
                    "createdAt": str,
                    "text": str
                },
                ...
            ]
        }
    """
    if not bot_prompt_lines:
        return {"matched": False, "matches": []}
    
    # Filter to only messages within max_msgs_to_check
    filtered_lines = [(msg_idx, norm_text, preview, created_at, raw_text) 
                     for msg_idx, norm_text, preview, created_at, raw_text in bot_prompt_lines 
                     if msg_idx < max_msgs_to_check]
    
    if not filtered_lines:
        return {"matched": False, "matches": []}
    
    matches = []
    last_match_pos = -1
    
    # Find prompts in order
    for prompt_idx, prompt in enumerate(REQUIRED_PROMPTS):
        found = False
        
        # Search from after last match position
        search_start = last_match_pos + 1
        for i in range(search_start, len(filtered_lines)):
            msg_idx, norm_text, preview, created_at, raw_text = filtered_lines[i]
            
            # Check if prompt substring appears in normalized text
            if prompt in norm_text:
                matches.append({
                    "prompt": prompt,
                    "messageIndex": msg_idx,
                    "createdAt": created_at or "",
                    "text": raw_text[:100] if len(raw_text) > 100 else raw_text  # Short preview
                })
                last_match_pos = i
                found = True
                break
        
        if not found:
            return {"matched": False, "matches": matches}
    
    return {"matched": True, "matches": matches}


def extract_text(msg: Dict[str, Any]) -> str:
    """Extract text from message (prefer text, else richText, else "")."""
    text = msg.get('text', '')
    if text:
        return text
    
    rich_text = msg.get('richText', '')
    if rich_text:
        return rich_text
    
    return ""


def contains_email(text: str) -> bool:
    """Check if text contains an email address."""
    if not text:
        return False
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    return bool(re.search(email_pattern, text))


def contains_phone(text: str) -> bool:
    """Check if text contains a phone-like pattern (>= 10 digits)."""
    if not text:
        return False
    
    # Remove common separators and check for at least 10 digits
    digits_only = re.sub(r'[^\d+]', '', text)
    # Count digits (excluding + if at start)
    digit_count = len([c for c in digits_only if c.isdigit()])
    
    return digit_count >= 10


def score_chatbot_thread(messages: List[Dict[str, Any]], min_incoming: int = 4, 
                         min_outgoing: int = 4, require_email_or_phone: bool = False) -> Tuple[int, Dict[str, Any]]:
    """
    Score a thread to determine if it's a good chatbot conversation candidate.
    
    Returns (score, stats_dict).
    Base acceptance criteria must be met (score >= 0) to qualify.
    """
    incoming_count = 0
    outgoing_count = 0
    welcome_count = 0
    has_email = False
    has_phone = False
    total_messages = len(messages)
    
    # Filter out system events for counting/checking
    non_system_messages = [m for m in messages if m.get('type') not in {
        'THREAD_STATUS_CHANGE', 'ASSIGNMENT', 'NOTE'
    }]
    
    # Count incoming/outgoing
    for msg in non_system_messages:
        if is_incoming_customer(msg):
            incoming_count += 1
            # Check for email/phone in incoming messages
            text = extract_text(msg)
            if not has_email and contains_email(text):
                has_email = True
            if not has_phone and contains_phone(text):
                has_phone = True
        elif is_outgoing_botlike(msg):
            outgoing_count += 1
    
    # Count welcome messages
    for msg in messages:
        if msg.get('type') == 'WELCOME_MESSAGE':
            welcome_count += 1
    
    # Check base acceptance criteria
    if incoming_count < min_incoming:
        return (-1, {'reason': f'incoming_count {incoming_count} < min_incoming {min_incoming}'})
    if outgoing_count < min_outgoing:
        return (-1, {'reason': f'outgoing_count {outgoing_count} < min_outgoing {min_outgoing}'})
    if welcome_count < 1:
        return (-1, {'reason': 'welcome_count < 1'})
    if require_email_or_phone and not (has_email or has_phone):
        return (-1, {'reason': 'require_email_or_phone but no email or phone found'})
    
    # Calculate score
    score = 0
    score += incoming_count * 2
    score += outgoing_count * 2
    score += 5 if welcome_count >= 1 else 0
    score += 4 if has_email else 0
    score += 4 if has_phone else 0
    
    # Interleave bonus: check if first 2-3 non-system events look like: outgoing then incoming
    interleave_bonus = 0
    if len(non_system_messages) >= 2:
        first_non_system = non_system_messages[:3]
        if len(first_non_system) >= 2:
            first_is_outgoing = is_outgoing_botlike(first_non_system[0])
            second_is_incoming = is_incoming_customer(first_non_system[1])
            if first_is_outgoing and second_is_incoming:
                interleave_bonus = 2
    
    score += interleave_bonus
    
    # Length bonus
    length_bonus = min(10, total_messages // 2)
    score += length_bonus
    
    stats = {
        'incoming_count': incoming_count,
        'outgoing_count': outgoing_count,
        'welcome_count': welcome_count,
        'has_email': has_email,
        'has_phone': has_phone,
        'total_messages': total_messages,
        'interleave_bonus': interleave_bonus,
        'length_bonus': length_bonus,
        'score': score
    }
    
    return (score, stats)


def sample_threads(threads: List[Dict[str, Any]], sample_size: int, seed: Optional[int] = None) -> List[Dict[str, Any]]:
    """Randomly sample threads."""
    if seed is not None:
        random.seed(seed)
    
    if len(threads) <= sample_size:
        return threads
    
    return random.sample(threads, sample_size)


def find_full_chatbot_thread(threads: List[Dict[str, Any]], scan_limit: int,
                             max_msgs_to_check: int, include_thread_details: bool, token: str = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[List[Dict[str, Any]]]]:
    """
    Find a full chatbot conversation thread by matching required prompts in order.
    
    Returns (bundle_dict, match_info_dict, diagnostics_list) if found, (None, None, diagnostics_list) otherwise.
    diagnostics_list contains top 5 closest threads by matchedCount.
    """
    scanned_count = 0
    
    # Keywords to check in first page before fetching all pages
    keywords = ["looking for", "good email", "country", "contact number", "team member"]
    
    # Track diagnostics: closest threads by matchedCount
    diagnostics = []  # List of {"threadId", "inboxId", "channelAccountId", "matchedCount", "matchedPrompts", "firstBotPrompts"}
    
    print(f"Scanning threads for chatbot conversation with required prompts (limit: {scan_limit})...", file=sys.stderr)
    
    for i, thread_item in enumerate(threads[:scan_limit]):
        thread_id = thread_item.get('id', 'unknown')
        scanned_count += 1
        
        if scanned_count % 10 == 0:
            print(f"  Scanned {scanned_count}/{min(scan_limit, len(threads))} threads...", file=sys.stderr)
        
        # Fetch first page of messages to check for keywords
        try:
            first_page = get_thread_messages_firstpage(thread_id, token=token)
            
            if not first_page:
                continue
            
            first_page_results = first_page.get('results', [])
            
            # Quick keyword check: build normalized combined text from bot prompt candidates in first page
            first_page_normalized_text = ""
            for msg in first_page_results:
                if msg.get('type') in ('MESSAGE', 'WELCOME_MESSAGE') and is_botlike_outgoing(msg):
                    text = message_text(msg)
                    if text:
                        normalized = normalize_for_match(text)
                        if normalized:
                            first_page_normalized_text += " " + normalized
            
            # Check if any keywords appear in normalized text from bot prompts
            has_keyword = any(keyword.lower() in first_page_normalized_text for keyword in keywords)
            
            if not has_keyword:
                # No keywords found in first page bot prompts, skip fetching remaining pages
                continue
            
            # Fetch all messages
            all_messages_response = get_thread_messages_all(thread_id, token=token)
            all_messages = all_messages_response.get('results', [])
            
            if not all_messages:
                continue
            
            # Filter messages to only MESSAGE and WELCOME_MESSAGE types, sorted by createdAt
            filtered_messages = [
                m for m in all_messages 
                if m.get('type') in ('MESSAGE', 'WELCOME_MESSAGE')
            ]
            
            # Sort by createdAt ascending
            def get_sort_key(msg):
                dt = parse_iso_datetime(msg.get('createdAt'))
                if dt:
                    return (dt.timestamp(), msg.get('id', ''))
                return (0, msg.get('id', ''))
            
            filtered_messages = sorted(filtered_messages, key=get_sort_key)
            
            # Build bot_prompt_lines: list of tuples (message_index, normalized_text, raw_text_preview, createdAt, raw_text)
            # message_index is index in filtered_messages (MESSAGE/WELCOME_MESSAGE only)
            bot_prompt_lines = []
            
            for msg_idx, msg in enumerate(filtered_messages):
                if is_botlike_outgoing(msg):
                    text = message_text(msg)
                    if text:
                        normalized = normalize_for_match(text)
                        if normalized:
                            # Get raw cleaned text for preview (first 100 chars)
                            raw_cleaned = strip_html(text)
                            preview = raw_cleaned[:100] if len(raw_cleaned) > 100 else raw_cleaned
                            created_at = msg.get('createdAt', '')
                            bot_prompt_lines.append((msg_idx, normalized, preview, created_at, raw_cleaned))
            
            # Check if required prompts match (within max_msgs_to_check)
            match_result = find_required_prompts(bot_prompt_lines, max_msgs_to_check=max_msgs_to_check)
            
            # Track diagnostics: count matched prompts
            matched_count = len(match_result.get('matches', []))
            if matched_count > 0:
                diagnostics.append({
                    'threadId': thread_id,
                    'inboxId': thread_item.get('inboxId'),
                    'channelAccountId': thread_item.get('originalChannelAccountId') or thread_item.get('channelAccountId'),
                    'matchedCount': matched_count,
                    'matchedPrompts': [m.get('prompt') for m in match_result.get('matches', [])],
                    'firstBotPrompts': [preview for _, _, preview, _, _ in bot_prompt_lines[:3]] if bot_prompt_lines else []
                })
            
            if match_result['matched']:
                # Found qualifying thread!
                print(f"  Found matching thread {thread_id}!", file=sys.stderr)
                
                # Get thread details if requested
                thread_details = None
                if include_thread_details:
                    thread_details = get_thread_details(thread_id, token=token)
                
                # Create bundle
                messages_response = {
                    'results': all_messages,
                    'paging': all_messages_response.get('paging'),
                    '_pagesFetched': all_messages_response.get('_pagesFetched', 1)
                }
                bundle = create_bundle(thread_item, messages_response, thread_details)
                
                # Build match info with full match details (matches already have createdAt and text)
                match_info = {
                    'matched': True,
                    'matches': match_result.get('matches', []),
                    'scannedThreads': scanned_count,
                    'requiredPrompts': REQUIRED_PROMPTS
                }
                
                # Sort diagnostics by matchedCount descending, take top 5
                diagnostics_sorted = sorted(diagnostics, key=lambda x: x['matchedCount'], reverse=True)[:5]
                
                return (bundle, match_info, diagnostics_sorted)
        
        except Exception as e:
            print(f"  Warning: Error processing thread {thread_id}: {e}", file=sys.stderr)
            continue
    
    print(f"\nNo qualifying chatbot thread found after scanning {scanned_count} threads.", file=sys.stderr)
    print(f"  Required prompts: {REQUIRED_PROMPTS}", file=sys.stderr)
    
    # Sort diagnostics by matchedCount descending, take top 5
    diagnostics_sorted = sorted(diagnostics, key=lambda x: x['matchedCount'], reverse=True)[:5]
    
    return (None, None, diagnostics_sorted)


def format_speaker_label(msg: Dict[str, Any]) -> str:
    """
    Format speaker label from message senders and direction.
    
    Speaker mapping:
    - if any sender actorId startswith "V-" => Customer (V-xxxx)
    - else if any sender actorId startswith "B-" => Bot (B-xxxx) [include sender name if present]
    - else if any sender actorId startswith "S-" and type in {"WELCOME_MESSAGE","MESSAGE"} => Bot (S-hubspot)
    - else if msg.direction == "OUTGOING" => Agent
    - else System
    """
    senders = msg.get('senders', [])
    direction = msg.get('direction', '')
    msg_type = msg.get('type', '')
    
    if senders:
        first_sender = senders[0]
        actor_id = first_sender.get('actorId', '')
        
        # If any sender.actorId startswith "V-" => "Customer (V-xxxx)"
        if actor_id and actor_id.startswith('V-'):
            return f"Customer ({actor_id})"
        
        # Else if any sender.actorId startswith "B-" => "Bot (B-xxxx)" [include sender name if present]
        if actor_id and actor_id.startswith('B-'):
            name = first_sender.get('name', '')
            if name:
                return f"Bot ({name})"
            return f"Bot ({actor_id})"
        
        # Else if any sender.actorId startswith "S-" and type in {"WELCOME_MESSAGE","MESSAGE"} => "Bot (S-hubspot)"
        if actor_id and actor_id.startswith('S-') and msg_type in {'WELCOME_MESSAGE', 'MESSAGE'}:
            name = first_sender.get('name', '')
            if name:
                return f"Bot ({name})"
            return f"Bot (S-hubspot)"
    
    # Else if msg.direction == "OUTGOING" => "Agent"
    if direction == 'OUTGOING':
        return "Agent"
    
    # Else "System"
    return "System"


def parse_iso_datetime(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO8601 datetime string to datetime object."""
    if not dt_str:
        return None
    
    try:
        # Handle Z suffix
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return None


def format_datetime(dt: Optional[datetime]) -> str:
    """Format datetime to YYYY-MM-DD HH:MM:SS."""
    if dt is None:
        return "(time unknown)"
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def print_transcript(bundle: Dict[str, Any]) -> None:
    """Print a human-readable transcript of the conversation."""
    messages_response = bundle.get('messagesResponse', {})
    messages = messages_response.get('results', [])
    thread = bundle.get('thread', {})
    thread_id = bundle.get('threadId', 'unknown')
    
    # Filter to only MESSAGE and WELCOME_MESSAGE types
    transcript_messages = [
        m for m in messages 
        if m.get('type') in ('MESSAGE', 'WELCOME_MESSAGE')
    ]
    
    # Sort by createdAt ascending (stable)
    def get_sort_key(msg):
        dt = parse_iso_datetime(msg.get('createdAt'))
        if dt:
            return (dt.timestamp(), msg.get('id', ''))
        return (0, msg.get('id', ''))
    
    sorted_messages = sorted(transcript_messages, key=get_sort_key)
    
    for msg in sorted_messages:
        # Get timestamp
        created_at = parse_iso_datetime(msg.get('createdAt'))
        time_str = format_datetime(created_at)
        
        # Get speaker label
        speaker = format_speaker_label(msg)
        
        # Get cleaned text
        text = message_text(msg)
        if text:
            # Strip HTML and clean text (preserve newlines)
            cleaned = strip_html(text)
            # Remove excessive whitespace but keep intentional newlines
            lines = [line.strip() for line in cleaned.split('\n')]
            cleaned_text = '\n'.join(lines).strip()
            if not cleaned_text:
                cleaned_text = "(no message body)"
        else:
            cleaned_text = "(no message body)"
        
        # Print message: [YYYY-MM-DD HH:MM:SS] <Speaker>:
        print(f"[{time_str}] {speaker}:")
        print(cleaned_text)
        
        # Print attachments if any
        attachments = msg.get('attachments', [])
        if attachments:
            att_strs = []
            for att in attachments:
                name = att.get('name', 'unnamed')
                url = att.get('url', '')
                if url:
                    att_strs.append(f"{name} ({url})")
                else:
                    att_strs.append(name)
            print(f"Attachments: {', '.join(att_strs)}")
        
        print()  # Blank line between messages


def find_contact_id_by_email(email: str, token: str = None) -> Optional[str]:
    """Find contact ID by email address using CRM search API."""
    path = '/crm/v3/objects/contacts/search'
    body = {
        'filterGroups': [{
            'filters': [{
                'propertyName': 'email',
                'operator': 'EQ',
                'value': email
            }]
        }],
        'properties': ['email'],
        'limit': 1
    }
    
    status, headers, data = hubspot_request('POST', path, body=body, token=token)
    time.sleep(DEFAULT_RATE_LIMIT_DELAY)
    
    if status != 200 or not data:
        return None
    
    results = data.get('results', [])
    if len(results) == 0:
        return None
    
    # Return the contact ID
    contact = results[0]
    return contact.get('id')


def list_threads_by_contact_id(contact_id: str, thread_status: str = 'both', token: str = None) -> List[Dict[str, Any]]:
    """
    List threads by associatedContactId.
    
    HubSpot requires threadStatus when using associatedContactId.
    """
    all_threads = []
    max_pages = 10  # Safety cap
    
    statuses = []
    if thread_status in ('both', 'open'):
        statuses.append('OPEN')
    if thread_status in ('both', 'closed'):
        statuses.append('CLOSED')
    
    for status in statuses:
        threads = []
        after = None
        page_count = 0
        
        while page_count < max_pages:
            params = {
                'associatedContactId': contact_id,
                'threadStatus': status,
                'limit': 100
            }
            
            if after:
                params['after'] = after
            
            path = '/conversations/v3/conversations/threads'
            req_status, headers, response = hubspot_request('GET', path, params=params, token=token)
            time.sleep(DEFAULT_RATE_LIMIT_DELAY)
            page_count += 1
            
            if req_status != 200 or not response:
                break
            
            results = response.get('results', [])
            threads.extend(results)
            
            # Check for next page - normalize cursor to RAW (decoded) form
            paging = response.get('paging', {})
            next_page = paging.get('next', {})
            next_after_encoded = next_page.get('after')
            
            # Decode cursor to raw form (handle both encoded and raw cursors)
            after = unquote(next_after_encoded) if next_after_encoded else None
            
            if not after:
                break
        
        all_threads.extend(threads)
    
    return all_threads


def pick_thread(threads: List[Dict[str, Any]], pick_mode: str = 'latest') -> Optional[Dict[str, Any]]:
    """Pick thread by latestMessageTimestamp or createdAt based on pick_mode."""
    if not threads:
        return None
    
    def get_timestamp(thread):
        # Use latestMessageTimestamp if present, else createdAt
        timestamp = thread.get('latestMessageTimestamp') or thread.get('createdAt')
        if timestamp:
            dt = parse_iso_datetime(timestamp)
            if dt:
                return dt.timestamp()
        return 0
    
    if pick_mode == 'latest':
        return max(threads, key=get_timestamp)
    else:  # oldest
        return min(threads, key=get_timestamp)


def print_curl_commands() -> None:
    """Print curl commands for all logged requests."""
    print("\n===== API CALLS (curl) =====")
    
    for req in _request_log:
        method = req['method']
        url = req['url']
        body = req.get('body')
        
        # Extract path and params from URL
        parsed = urlparse(url)
        path = parsed.path
        query = parsed.query
        
        curl_parts = ['curl', '-s']
        
        if method == 'GET':
            curl_parts.extend(['-X', 'GET'])
            if query:
                full_url = f"{BASE_URL}{path}?{query}"
            else:
                full_url = f"{BASE_URL}{path}"
        else:  # POST
            curl_parts.extend(['-X', 'POST'])
            full_url = f"{BASE_URL}{path}"
        
        curl_parts.append(f'"{full_url}"')
        curl_parts.append('-H "Authorization: Bearer $OLD_ACCESS_TOKEN"')
        curl_parts.append('-H "Accept: application/json"')
        
        if body is not None:
            curl_parts.append('-H "Content-Type: application/json"')
            body_json = json.dumps(body)
            curl_parts.append(f"-d '{body_json}'")
        
        print(' '.join(curl_parts))


def save_found_thread_output(bundle: Dict[str, Any], out_dir: str, metadata: Dict[str, Any]) -> None:
    """Save found thread bundle to JSONL and pretty JSON files, plus metadata."""
    os.makedirs(out_dir, exist_ok=True)
    
    # Save JSONL (one bundle per line)
    jsonl_path = os.path.join(out_dir, 'found_thread.jsonl')
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(bundle) + '\n')
    
    print(f"\nSaved JSONL: {jsonl_path}", file=sys.stderr)
    
    # Save pretty JSON (single bundle)
    pretty_path = os.path.join(out_dir, 'found_thread.pretty.json')
    with open(pretty_path, 'w', encoding='utf-8') as f:
        json.dump(bundle, f, indent=2, ensure_ascii=False)
    
    print(f"Saved pretty JSON: {pretty_path}", file=sys.stderr)
    
    # Save metadata
    metadata_path = os.path.join(out_dir, 'metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    print(f"Saved metadata: {metadata_path}", file=sys.stderr)


def create_bundle(thread_item: Dict[str, Any], messages_response: Dict[str, Any],
                  thread_details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create a normalized bundle object."""
    thread_id = thread_item.get('id', 'unknown')
    
    bundle = {
        'threadId': thread_id,
        'thread': thread_details if thread_details else thread_item,
        'messagesResponse': messages_response,
        'fetchedAt': datetime.now(timezone.utc).isoformat()
    }
    
    return bundle


def load_dotenv(path: str = '.env') -> Dict[str, str]:
    """Load environment variables from a .env file."""
    env_vars = {}
    if not os.path.exists(path):
        return env_vars
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                # Parse KEY=value format
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Remove quotes if present
                    if (value.startswith('"') and value.endswith('"')) or \
                       (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    
                    env_vars[key] = value
    except Exception as e:
        print(f"Warning: Could not read .env file: {e}", file=sys.stderr)
    
    return env_vars


def get_old_access_token() -> Tuple[str, str]:
    """
    Get OLD_ACCESS_TOKEN from .env file or environment.
    
    Returns:
        (token: str, source: str) where source is either 'env_file' or 'environment'
    
    Exits with code 2 if token not found.
    """
    # Check OS environment first (precedence)
    token = os.environ.get('OLD_ACCESS_TOKEN')
    if token:
        return (token, 'environment')
    
    # Check .env file
    env_vars = load_dotenv('.env')
    token = env_vars.get('OLD_ACCESS_TOKEN')
    if token:
        return (token, 'env_file')
    
    # Not found
    print("Error: OLD_ACCESS_TOKEN not found in .env file or environment.", file=sys.stderr)
    print("Please set it in .env file (OLD_ACCESS_TOKEN=pat-...) or with: export OLD_ACCESS_TOKEN=\"pat-...\"", file=sys.stderr)
    sys.exit(2)


def save_outputs(bundles: List[Dict[str, Any]], out_dir: str, metadata: Dict[str, Any]) -> None:
    """Save bundles to JSONL and pretty JSON files, plus metadata."""
    os.makedirs(out_dir, exist_ok=True)
    
    # Save JSONL (one bundle per line)
    jsonl_path = os.path.join(out_dir, 'threads_10.jsonl')
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for bundle in bundles:
            f.write(json.dumps(bundle) + '\n')
    
    print(f"Saved JSONL: {jsonl_path}", file=sys.stderr)
    
    # Save pretty JSON (array of bundles)
    pretty_path = os.path.join(out_dir, 'threads_10.pretty.json')
    with open(pretty_path, 'w', encoding='utf-8') as f:
        json.dump(bundles, f, indent=2, ensure_ascii=False)
    
    print(f"Saved pretty JSON: {pretty_path}", file=sys.stderr)
    
    # Save metadata
    metadata_path = os.path.join(out_dir, 'metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    print(f"Saved metadata: {metadata_path}", file=sys.stderr)


def save_full_thread_output(bundle: Dict[str, Any], out_dir: str, metadata: Dict[str, Any]) -> None:
    """Save full-thread bundle to JSONL and pretty JSON files, plus metadata."""
    os.makedirs(out_dir, exist_ok=True)
    
    # Save JSONL (one bundle per line)
    jsonl_path = os.path.join(out_dir, 'full_thread.jsonl')
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps(bundle) + '\n')
    
    print(f"Saved JSONL: {jsonl_path}", file=sys.stderr)
    
    # Save pretty JSON (single bundle)
    pretty_path = os.path.join(out_dir, 'full_thread.pretty.json')
    with open(pretty_path, 'w', encoding='utf-8') as f:
        json.dump(bundle, f, indent=2, ensure_ascii=False)
    
    print(f"Saved pretty JSON: {pretty_path}", file=sys.stderr)
    
    # Save metadata
    metadata_path = os.path.join(out_dir, 'metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    print(f"Saved metadata: {metadata_path}", file=sys.stderr)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Extract and sample HubSpot conversation threads with message history'
    )
    parser.add_argument(
        '--token-env',
        type=str,
        help='[DEPRECATED] This flag is ignored. Script always uses OLD_ACCESS_TOKEN from .env file or environment.'
    )
    parser.add_argument(
        '--inbox-id',
        type=str,
        help='Optional inbox ID to filter threads'
    )
    parser.add_argument(
        '--archived',
        type=str,
        default='false',
        choices=['true', 'false'],
        help='Include archived threads (default: false)'
    )
    parser.add_argument(
        '--pool-size',
        type=int,
        default=100,
        help='Number of threads to collect before sampling (default: 100)'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=10,
        help='Number of threads to sample (default: 10)'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=5,
        help='Maximum pages to fetch when building pool (default: 5)'
    )
    parser.add_argument(
        '--out-dir',
        type=str,
        default='./out',
        help='Output directory for saved files (default: ./out)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        help='Random seed for deterministic sampling'
    )
    parser.add_argument(
        '--include-thread-details',
        action='store_true',
        help='Also fetch detailed thread info for each sampled thread'
    )
    parser.add_argument(
        '--print-mode',
        type=str,
        default='bundle',
        choices=['raw', 'bundle'],
        help='Print mode: raw (exact JSON) or bundle (normalized) (default: bundle)'
    )
    parser.add_argument(
        '--full-thread',
        action='store_true',
        help='Find and output one complete chatbot conversation thread'
    )
    parser.add_argument(
        '--scan-limit',
        type=int,
        default=400,
        help='Maximum number of threads to evaluate when using --full-thread (default: 400)'
    )
    parser.add_argument(
        '--max-msgs-to-check',
        type=int,
        default=60,
        help='Maximum number of MESSAGE/WELCOME_MESSAGE items to check for prompts (default: 60)'
    )
    parser.add_argument(
        '--min-incoming',
        type=int,
        default=4,
        help='Minimum INCOMING customer messages required for --full-thread (default: 4)'
    )
    parser.add_argument(
        '--min-outgoing',
        type=int,
        default=4,
        help='Minimum OUTGOING bot/agent messages required for --full-thread (default: 4)'
    )
    parser.add_argument(
        '--require-email-or-phone',
        action='store_true',
        help='Require at least one incoming message with email or phone pattern for --full-thread'
    )
    parser.add_argument(
        '--find-by-email',
        type=str,
        help='Find conversation thread by customer email address (OLD portal mode)'
    )
    parser.add_argument(
        '--thread-status',
        type=str,
        default='both',
        choices=['open', 'closed', 'both'],
        help='Thread status filter for --find-by-email (default: both)'
    )
    parser.add_argument(
        '--pick',
        type=str,
        default='latest',
        choices=['latest', 'oldest'],
        help='Pick latest or oldest thread when multiple found (default: latest)'
    )
    parser.add_argument(
        '--print-curl',
        action='store_true',
        help='Print curl commands for API calls (use with --find-by-email)'
    )
    
    args = parser.parse_args()
    
    # Get token (always OLD_ACCESS_TOKEN)
    token, source = get_old_access_token()
    if source == 'env_file':
        print("Using OLD_ACCESS_TOKEN from .env file", file=sys.stderr)
    else:
        print("Using OLD_ACCESS_TOKEN from environment", file=sys.stderr)
    
    # Parse archived flag
    archived = args.archived.lower() == 'true'
    
    # Clear request log at start
    _request_log.clear()
    
    # Handle --find-by-email mode (takes priority over other modes)
    if args.find_by_email:
        email = args.find_by_email
        thread_status = args.thread_status
        pick_mode = args.pick
        
        print("===== FIND BY EMAIL =====", file=sys.stderr)
        print(f"Email: {email}", file=sys.stderr)
        
        # Find contact ID by email
        contact_id = find_contact_id_by_email(email, token=token)
        
        threads = []
        
        if contact_id:
            print(f"ContactId: {contact_id}", file=sys.stderr)
            
            # List threads by associatedContactId
            threads = list_threads_by_contact_id(contact_id, thread_status=thread_status, token=token)
            print(f"Threads found: {len(threads)}", file=sys.stderr)
        
        if contact_id and threads:
            # Pick thread
            chosen_thread = pick_thread(threads, pick_mode=pick_mode)
            thread_id = chosen_thread.get('id', 'unknown')
            print(f"Chosen ThreadId: {thread_id}", file=sys.stderr)
            
            # Fetch full thread details
            thread_status_code, thread_headers, thread_details = hubspot_request(
                'GET',
                f'/conversations/v3/conversations/threads/{thread_id}',
                token=token
            )
            time.sleep(DEFAULT_RATE_LIMIT_DELAY)
            
            if thread_status_code != 200:
                print(f"Error: Failed to fetch thread details (status {thread_status_code})", file=sys.stderr)
                sys.exit(2)
            
            # Fetch ALL messages with pagination
            all_messages = []
            after = None
            pages_fetched = 0
            
            while True:
                params = {'limit': 100}
                if after:
                    params['after'] = after
                
                msg_status, msg_headers, messages_response = hubspot_request(
                    'GET',
                    f'/conversations/v3/conversations/threads/{thread_id}/messages',
                    params=params,
                    token=token
                )
                time.sleep(DEFAULT_RATE_LIMIT_DELAY)
                pages_fetched += 1
                
                if msg_status != 200 or not messages_response:
                    break
                
                results = messages_response.get('results', [])
                all_messages.extend(results)
                
                # Check for next page - normalize cursor to RAW (decoded) form
                paging = messages_response.get('paging', {})
                next_page = paging.get('next', {})
                next_after_encoded = next_page.get('after')
                
                # Decode cursor to raw form (handle both encoded and raw cursors)
                after = unquote(next_after_encoded) if next_after_encoded else None
                
                if not after:
                    break
            
            messages_response_aggregated = {
                'results': all_messages,
                '_pagesFetched': pages_fetched
            }
            
            # Create bundle
            bundle = {
                'email': email,
                'contactId': contact_id,
                'threadId': thread_id,
                'thread': thread_details,
                'messagesResponse': messages_response_aggregated,
                'fetchedAt': datetime.now(timezone.utc).isoformat()
            }
            
            # Print output
            print("\n===== FIND BY EMAIL =====")
            print(f"Email: {email}")
            print(f"ContactId: {contact_id}")
            print(f"Threads found: {len(threads)}")
            print(f"Chosen ThreadId: {thread_id}")
            
            if args.print_curl:
                print_curl_commands()
            
            print("\n===== THREAD (raw JSON) =====")
            print(json.dumps(thread_details, indent=2))
            
            print("\n===== MESSAGES (raw JSON) =====")
            print(json.dumps(messages_response_aggregated, indent=2))
            
            # Save to files
            metadata = {
                'mode': 'find-by-email',
                'email': email,
                'contactId': contact_id,
                'chosenThreadId': thread_id,
                'threadStatus': thread_status,
                'pickMode': pick_mode,
                'threadsFound': len(threads),
                'fetchedAt': datetime.now(timezone.utc).isoformat()
            }
            
            save_found_thread_output(bundle, args.out_dir, metadata)
            
            print(f"\nDone! Found and saved thread {thread_id} for email {email}.", file=sys.stderr)
            return
        
        # Fallback: contact not found OR no threads found by associatedContactId
        if not contact_id or not threads:
            if not contact_id:
                print("ContactId: (not found)", file=sys.stderr)
            else:
                print("No threads found by associatedContactId", file=sys.stderr)
            
            print("Falling back to scanning threads and searching message text for the email.", file=sys.stderr)
            
            if not args.inbox_id:
                print("Error: --inbox-id is required for fallback scanning.", file=sys.stderr)
                sys.exit(2)
            
            # Fallback: scan threads and search message text
            pool_size = max(args.pool_size, 500)
            scan_limit = args.scan_limit or 500
            
            threads = list_threads(
                pool_size=min(scan_limit, pool_size),
                inbox_id=args.inbox_id,
                archived=archived,
                max_pages=args.max_pages,
                token=token
            )
            
            found_thread_item = None
            email_lower = email.lower()
            
            for thread_item in threads[:scan_limit]:
                thread_id_check = thread_item.get('id', 'unknown')
                
                # Fetch first page of messages
                first_page = get_thread_messages_firstpage(thread_id_check, token=token)
                
                if not first_page:
                    continue
                
                first_page_results = first_page.get('results', [])
                
                # Check if email appears in any message text
                found = False
                for msg in first_page_results:
                    text = message_text(msg).lower()
                    if email_lower in text:
                        found = True
                        break
                
                if found:
                    found_thread_item = thread_item
                    break
            
            if found_thread_item:
                thread_id = found_thread_item.get('id', 'unknown')
                print(f"Found thread {thread_id} by scanning message text", file=sys.stderr)
                
                # Fetch full thread details
                thread_status_code, thread_headers, thread_details = hubspot_request(
                    'GET',
                    f'/conversations/v3/conversations/threads/{thread_id}',
                    token=token
                )
                time.sleep(DEFAULT_RATE_LIMIT_DELAY)
                
                if thread_status_code != 200:
                    thread_details = found_thread_item
                
                # Fetch ALL messages
                all_messages_response = get_thread_messages_all(thread_id, token=token)
                all_messages = all_messages_response.get('results', [])
                pages_fetched = all_messages_response.get('_pagesFetched', 1)
                
                messages_response_aggregated = {
                    'results': all_messages,
                    '_pagesFetched': pages_fetched
                }
                
                # Create bundle (use contact_id if available, else None)
                bundle = {
                    'email': email,
                    'contactId': contact_id,  # May be None
                    'threadId': thread_id,
                    'thread': thread_details,
                    'messagesResponse': messages_response_aggregated,
                    'fetchedAt': datetime.now(timezone.utc).isoformat()
                }
                
                # Print output
                print("\n===== FIND BY EMAIL =====")
                print(f"Email: {email}")
                print(f"ContactId: {contact_id or '(not found)'}")
                print(f"Threads found: 0 (found by scanning)")
                print(f"Chosen ThreadId: {thread_id}")
                
                if args.print_curl:
                    print_curl_commands()
                
                print("\n===== THREAD (raw JSON) =====")
                print(json.dumps(thread_details, indent=2))
                
                print("\n===== MESSAGES (raw JSON) =====")
                print(json.dumps(messages_response_aggregated, indent=2))
                
                # Save to files
                metadata = {
                    'mode': 'find-by-email',
                    'email': email,
                    'contactId': contact_id,
                    'chosenThreadId': thread_id,
                    'threadStatus': thread_status,
                    'pickMode': pick_mode,
                    'threadsFound': 0,
                    'foundByScanning': True,
                    'fetchedAt': datetime.now(timezone.utc).isoformat()
                }
                
                save_found_thread_output(bundle, args.out_dir, metadata)
                
                print(f"\nDone! Found and saved thread {thread_id} for email {email}.", file=sys.stderr)
                return
            else:
                contact_msg = f"Contact {'found' if contact_id else 'not found'}"
                print(f"Error: No threads found for email {email} ({contact_msg}).", file=sys.stderr)
                sys.exit(3)
    
    # Handle --full-thread mode
    if args.full_thread:
        # List threads for scanning (use scan_limit, page until we reach it)
        threads = list_threads_until_limit(
            scan_limit=args.scan_limit,
            inbox_id=args.inbox_id,
            archived=archived,
            token=token
        )
        
        if not threads:
            print("Error: No threads found.", file=sys.stderr)
            sys.exit(2)
        
        # Find full chatbot thread
        bundle, match_info, diagnostics = find_full_chatbot_thread(
            threads=threads,
            scan_limit=args.scan_limit,
            max_msgs_to_check=args.max_msgs_to_check,
            include_thread_details=args.include_thread_details,
            token=token
        )
        
        if not bundle:
            print(f"\nError: No qualifying chatbot thread found after scanning up to {args.scan_limit} threads.", file=sys.stderr)
            
            # Print top 5 closest threads by matchedCount
            if diagnostics:
                print(f"\nTop 5 closest threads by matched prompt count:")
                for diag in diagnostics:
                    print(f"  ThreadId: {diag.get('threadId')}, InboxId: {diag.get('inboxId')}, ChannelAccountId: {diag.get('channelAccountId')}")
                    print(f"    Matched: {diag.get('matchedCount')}/{len(REQUIRED_PROMPTS)} prompts: {diag.get('matchedPrompts', [])}")
                    if diag.get('firstBotPrompts'):
                        print(f"    First bot prompts: {diag['firstBotPrompts'][:3]}")
                
                # Save diagnostics
                diagnostics_path = os.path.join(args.out_dir, 'full_thread_diagnostics.pretty.json')
                os.makedirs(args.out_dir, exist_ok=True)
                with open(diagnostics_path, 'w', encoding='utf-8') as f:
                    json.dump(diagnostics, f, indent=2, ensure_ascii=False)
                print(f"\nSaved diagnostics to: {diagnostics_path}", file=sys.stderr)
            
            sys.exit(3)
        
        thread_id = bundle.get('threadId', 'unknown')
        thread = bundle.get('thread', {})
        
        # Print to stdout
        print("\n===== FULL CHATBOT THREAD FOUND: " + thread_id + " =====")
        
        # Print thread metadata summary
        print(f"inboxId: {thread.get('inboxId', 'Unknown')}")
        print(f"originalChannelAccountId: {thread.get('originalChannelAccountId') or thread.get('channelAccountId', 'Unknown')}")
        print(f"associatedContactId: {thread.get('associatedContactId', '(none)')}")
        
        # Print matched prompts positions
        if match_info and match_info.get('matched'):
            print(f"\nmatched prompts:")
            for match in match_info.get('matches', []):
                prompt = match.get('prompt', '')
                created_at = match.get('createdAt', '')
                text_preview = match.get('text', '')[:80]  # Short preview
                print(f"- {prompt} @ {created_at} : {text_preview}")
        
        # Print transcript
        print("\n===== TRANSCRIPT =====")
        print_transcript(bundle)
        
        # Extract pages fetched if available
        messages_response = bundle.get('messagesResponse', {})
        pages_fetched = messages_response.get('_pagesFetched', 1)
        
        # Save to files
        metadata = {
            'mode': 'full-thread',
            'requestParams': {
                'inboxId': args.inbox_id,
                'archived': archived,
                'scanLimit': args.scan_limit,
                'includeThreadDetails': args.include_thread_details
            },
            'selectedThreadId': thread_id,
            'scannedThreads': match_info.get('scannedThreads', 0) if match_info else 0,
            'requiredPrompts': REQUIRED_PROMPTS,
            'matches': match_info.get('matches', []) if match_info else [],
            'maxMsgsToCheck': args.max_msgs_to_check,
            'pagesFetched': pages_fetched,
            'fetchedAt': datetime.now(timezone.utc).isoformat()
        }
        
        save_full_thread_output(bundle, args.out_dir, metadata)
        
        print(f"\nDone! Found and saved full chatbot thread {thread_id}.", file=sys.stderr)
        return
    
    # Normal mode (existing behavior)
    # List threads to build pool
    threads = list_threads(
        pool_size=args.pool_size,
        inbox_id=args.inbox_id,
        archived=archived,
        max_pages=args.max_pages,
        token=token
    )
    
    if not threads:
        print("Error: No threads found.", file=sys.stderr)
        sys.exit(2)
    
    # Sample threads
    sampled_threads = sample_threads(threads, args.sample_size, seed=args.seed)
    
    print(f"\nSampled {len(sampled_threads)} threads. Fetching messages...\n", file=sys.stderr)
    
    # Fetch messages and details for each sampled thread
    bundles = []
    thread_ids = []
    
    for i, thread_item in enumerate(sampled_threads, 1):
        thread_id = thread_item.get('id', 'unknown')
        thread_ids.append(thread_id)
        
        print(f"Fetching thread {i}/{len(sampled_threads)}: {thread_id}...", file=sys.stderr)
        
        # Get messages
        messages_response = get_thread_messages(thread_id, token=token)
        
        # Get thread details if requested
        thread_details = None
        if args.include_thread_details:
            thread_details = get_thread_details(thread_id, token=token)
        
        # Create bundle
        bundle = create_bundle(thread_item, messages_response, thread_details)
        bundles.append(bundle)
        
        # Print to stdout
        print(f"\n===== THREAD {i}/{len(sampled_threads)}: {thread_id} =====")
        
        if args.print_mode == 'raw':
            # Print raw JSON responses
            print(json.dumps(thread_item, indent=2))
            print("\n--- Messages Response ---")
            print(json.dumps(messages_response, indent=2))
            if args.include_thread_details and thread_details:
                print("\n--- Thread Details ---")
                print(json.dumps(thread_details, indent=2))
        else:
            # Print bundle
            print(json.dumps(bundle, indent=2))
        
        print()  # Blank line between threads
    
    # Save to files
    metadata = {
        'requestParams': {
            'inboxId': args.inbox_id,
            'archived': archived,
            'poolSize': args.pool_size,
            'sampleSize': args.sample_size,
            'maxPages': args.max_pages,
            'includeThreadDetails': args.include_thread_details,
            'seed': args.seed
        },
        'counts': {
            'totalThreadsCollected': len(threads),
            'sampledThreads': len(sampled_threads)
        },
        'sampledThreadIds': thread_ids,
        'fetchedAt': datetime.now(timezone.utc).isoformat()
    }
    
    save_outputs(bundles, args.out_dir, metadata)
    
    print(f"\nDone! Processed {len(sampled_threads)} threads.", file=sys.stderr)


if __name__ == '__main__':
    main()
