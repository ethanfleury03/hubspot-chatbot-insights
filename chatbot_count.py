#!/usr/bin/env python3
"""
Count chatbot conversations in HubSpot OLD portal.

Usage examples:
  python chatbot_count.py --inbox-id 147959634 --channel-account-id 240442427 --fast
  python chatbot_count.py --scan-limit 20000 --progress-every 500 --json-out out/chatbot_count.json
  python chatbot_count.py --since 2024-01-01T00:00:00Z --until 2024-12-31T23:59:59Z --fast
  python chatbot_count.py --write-chatbot
  python chatbot_count.py --inbox-id 147959634 --channel-account-id 240442427 --write-chatbot --db out/chatbot.sqlite

Definition: A thread counts as a "chatbot conversation" if it contains ALL required bot prompts
as an ordered subsequence after normalization.
"""

import argparse
import html
import json
import os
import random
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

BASE_URL = "https://api.hubapi.com"

# Required bot prompts that must appear in order for a "true chatbot conversation"
# Note: trailing punctuation removed for robust matching
# This is the single source of truth for chatbot prompts
CHATBOT_PROMPTS_ORDERED = [
    "what are you looking for",
    "what is your name",
    "what is a good email address to contact you with",
    "what is your country/region",
    "what is your good contact number to contact you with"
]

# Total number of chatbot stages (derived from prompts list)
# This is the single source of truth for max stage
MAX_STAGE = len(CHATBOT_PROMPTS_ORDERED)

# Legacy alias for backward compatibility
REQUIRED_PROMPTS = CHATBOT_PROMPTS_ORDERED
TOTAL_STAGES = MAX_STAGE

# Keywords for prefiltering
PREFILTER_KEYWORDS = ["looking for", "good email", "country", "contact number", "team member"]

DEFAULT_RATE_LIMIT_DELAY = 0.5  # seconds between API calls


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


def get_old_access_token() -> Tuple[str, str]:
    """
    Get OLD_ACCESS_TOKEN from OS environment (priority) or .env file.
    
    Returns (token, source) where source is 'env' or 'env_file'.
    Exits with error if token not found.
    """
    # Check OS environment first
    token = os.environ.get('OLD_ACCESS_TOKEN')
    if token:
        return (token, 'env')
    
    # Check .env file
    env_vars = load_dotenv()
    token = env_vars.get('OLD_ACCESS_TOKEN')
    if token:
        return (token, 'env_file')
    
    print("Error: OLD_ACCESS_TOKEN not found in environment or .env file.", file=sys.stderr)
    sys.exit(2)


def hubspot_request(method: str, path: str, params: Optional[Dict[str, Any]] = None,
                    token: str = None) -> Tuple[int, Dict[str, str], Dict[str, Any]]:
    """
    Make HTTP request to HubSpot API using urllib.
    
    Returns (status_code, headers_dict, json_dict).
    Handles retries for 429 and 5xx errors.
    
    IMPORTANT: params should contain raw strings (never pre-encoded).
    urlencode() will handle encoding exactly once.
    """
    if params is None:
        params = {}
    
    # Build URL - params must be raw strings, not pre-encoded
    parsed = urlparse(BASE_URL + path)
    
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
    url = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        urlencode(query_params),  # Single encoding pass
        parsed.fragment
    ))
    
    # Create request
    request = Request(url)
    request.add_header('Authorization', f'Bearer {token}')
    request.add_header('Content-Type', 'application/json')
    
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = urlopen(request, timeout=30)
            status = response.getcode()
            headers = dict(response.headers)
            
            body_bytes = response.read()
            body_str = body_bytes.decode('utf-8') if body_bytes else ''
            
            json_data = {}
            if body_str:
                try:
                    json_data = json.loads(body_str)
                except json.JSONDecodeError:
                    pass
            
            return (status, headers, json_data)
            
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
            elif status == 429:
                retry_after = headers.get('Retry-After', '1')
                try:
                    wait_time = float(retry_after)
                except ValueError:
                    wait_time = 2.0 * (retry_count + 1)
                
                if retry_count < max_retries - 1:
                    print(f"Rate limited (429). Waiting {wait_time:.1f}s...", file=sys.stderr)
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


def list_threads_stream(inbox_id: Optional[str] = None, channel_account_id: Optional[str] = None,
                       max_pages: int = 200, since: Optional[str] = None,
                       until: Optional[str] = None, token: str = None):
    """
    Generator yielding thread items, with paging.
    
    Supports filtering by inbox_id, channel_account_id, since, until.
    Stops after max_pages or when no more pages.
    """
    after = None
    page_count = 0
    prev_page_sig = None
    seen_page_sigs = set()
    prev_last_id = None
    stop_reason = None
    
    while page_count < max_pages:
        params = {
            'limit': 100,
            'archived': 'false'
        }
        
        if inbox_id:
            params['inboxId'] = inbox_id
        
        # Add 'after' cursor only if not None (as raw string, never pre-encoded)
        if after is not None:
            params['after'] = after
        
        status, headers, response = hubspot_request(
            'GET',
            '/conversations/v3/conversations/threads',
            params=params,
            token=token
        )
        
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        
        if status != 200 or not response:
            break
        
        results = response.get('results', [])
        
        # Extract IDs for page signature (before filtering)
        ids = [r.get('id') for r in results if r.get('id') is not None]
        
        # Get next cursor
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        next_after_raw = unquote(next_after_encoded) if next_after_encoded else None
        
        # Build page signature: (first_id, last_id, count, next_after_raw)
        page_sig = (ids[0] if ids else None, ids[-1] if ids else None, len(ids), next_after_raw)
        
        # Natural end conditions
        if len(results) == 0:
            stop_reason = "empty_results"
            break
        
        if next_after_raw is None:
            stop_reason = "no_next_cursor"
            break
        
        # Cursor not advancing AND last item didn't change -> not moving forward
        if next_after_raw == after and (ids[-1] if ids else None) == prev_last_id:
            stop_reason = "cursor_not_advancing"
            break
        
        # Stuck page detection: same page signature seen again
        if page_sig in seen_page_sigs:
            # Save debug file
            debug_dir = 'out'
            os.makedirs(debug_dir, exist_ok=True)
            debug_path = os.path.join(debug_dir, 'paging_debug_last_response.json')
            try:
                with open(debug_path, 'w', encoding='utf-8') as f:
                    json.dump(response, f, indent=2, ensure_ascii=False)
            except Exception:
                pass
            
            first_id = ids[0] if ids else None
            last_id = ids[-1] if ids else None
            print(
                f"Pagination appears stuck: received the same page again. Treating as end-of-list.\n"
                f"after_raw={after!r} next_after_raw={next_after_raw!r} "
                f"first_id={first_id!r} last_id={last_id!r} count={len(ids)}",
                file=sys.stderr
            )
            stop_reason = "page_repeated"
            break
        
        # Track page signature and continue
        seen_page_sigs.add(page_sig)
        prev_page_sig = page_sig
        prev_last_id = ids[-1] if ids else prev_last_id
        after = next_after_raw  # Store only raw cursor
        page_count += 1
        
        # Apply filters that aren't supported by API and yield threads
        for thread in results:
            # Filter by channel_account_id
            if channel_account_id:
                thread_channel_id = thread.get('originalChannelAccountId') or thread.get('channelAccountId')
                if thread_channel_id != channel_account_id:
                    continue
            
            # Filter by since/until (createdAt)
            if since or until:
                created_at = thread.get('createdAt', '')
                if created_at:
                    try:
                        # Parse ISO8601 (handle Z suffix)
                        if created_at.endswith('Z'):
                            created_at_dt = datetime.fromisoformat(created_at[:-1] + '+00:00')
                        else:
                            created_at_dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        
                        if since:
                            since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
                            if created_at_dt < since_dt:
                                continue
                        
                        if until:
                            until_dt = datetime.fromisoformat(until.replace('Z', '+00:00'))
                            if created_at_dt > until_dt:
                                continue
                    except (ValueError, AttributeError):
                        # Skip if date parsing fails
                        continue
            
            # Passed all filters, yield thread
    
    # Print stop reason if we stopped
    if stop_reason:
        print(f"Thread paging stopped: reason={stop_reason}, scanned_pages={page_count}", file=sys.stderr)


def iter_threads_all(archived: bool, inbox_id: Optional[str] = None,
                     channel_account_id: Optional[str] = None, since: Optional[str] = None,
                     max_pages: int = 5000, scan_limit: int = 100000, token: str = None):
    """
    Stall-safe thread iterator using latestMessageTimestamp + latestMessageTimestampAfter.
    
    Generator yielding thread dicts with deduplication by threadId.
    Uses timestamp-based pagination with stall escape when cursor/page repeats.
    """
    after = None
    seen_thread_ids = set()
    seen_page_sigs = set()
    last_seen_lmts = None  # latestMessageTimestamp string of last yielded thread
    # Use since if provided, otherwise use a safe early date (2000-01-01)
    # API requires latestMessageTimestampAfter when sorting by latestMessageTimestamp
    lmts_after = since if since else "2000-01-01T00:00:00.000Z"
    pages = 0
    yielded_count = 0
    stop_reason = None
    last_request_params = None
    last_response = None
    last_page_info = None
    
    while pages < max_pages and yielded_count < scan_limit:
        params = {
            'limit': 500,
            'archived': 'true' if archived else 'false',
            'sort': 'latestMessageTimestamp',
            'latestMessageTimestampAfter': lmts_after,  # Required when sorting by latestMessageTimestamp
        }
        
        if inbox_id:
            params['inboxId'] = inbox_id
        
        if after is not None:
            params['after'] = after  # raw cursor (NOT pre-encoded)
        
        # Track last request params
        last_request_params = params.copy()
        
        status, headers, response = hubspot_request(
            'GET',
            '/conversations/v3/conversations/threads',
            params=params,
            token=token
        )
        
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        
        if status != 200 or not response:
            stop_reason = "http_error"
            last_response = response
            break
        
        results = response.get('results', [])
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        next_after_raw = unquote(next_after_encoded) if next_after_encoded else None
        
        # Extract IDs for page signature
        ids = [t.get('id') for t in results if t.get('id') is not None]
        first_id = ids[0] if ids else None
        last_id = ids[-1] if ids else None
        
        # Track last page info
        last_page_info = {
            'count': len(results),
            'firstId': first_id,
            'lastId': last_id,
            'nextAfterRaw': next_after_raw,
            'nextAfterEncoded': next_after_encoded
        }
        last_response = response
        
        # Build page signature: (lmts_after or None, after, first_id, last_id, len(ids), next_after_raw)
        # Use None if lmts_after is not set yet
        page_sig = (lmts_after if lmts_after else None, after, first_id, last_id, len(ids), next_after_raw)
        
        # Natural end condition
        if len(results) == 0:
            stop_reason = "no_results"
            break
        
        # Stuck page detection: same page signature seen again
        if page_sig in seen_page_sigs:
            # STALL ESCAPE: advance timestamp window
            if last_seen_lmts is None:
                stop_reason = "page_repeated_no_timestamp"
                break  # Nothing to advance
            
            # Advance latestMessageTimestampAfter by 1ms
            lmts_after = advance_timestamp_ms(last_seen_lmts, ms=1)
            after = None
            seen_page_sigs.clear()  # Clear to allow progress with new timestamp
            continue
        
        seen_page_sigs.add(page_sig)
        
        # Yield threads (already sorted by latestMessageTimestamp ascending)
        for thread in results:
            # Filter by channel_account_id
            if channel_account_id:
                thread_channel_id = thread.get('originalChannelAccountId') or thread.get('channelAccountId')
                if thread_channel_id != channel_account_id:
                    continue
            
            tid = thread.get('id')
            if not tid:
                continue
            
            # Deduplicate by threadId
            if tid in seen_thread_ids:
                continue
            
            seen_thread_ids.add(tid)
            last_seen_lmts = thread.get('latestMessageTimestamp') or last_seen_lmts
            yielded_count += 1
            
            if yielded_count >= scan_limit:
                stop_reason = "scan_limit_reached"
                # Still need to print diagnostics, so we'll break instead of return
                break
            
            yield thread
        
        # Normal paging advance
        if next_after_raw is not None and next_after_raw != after:
            after = next_after_raw
            pages += 1
            continue
        
        # STALL ESCAPE: cursor didn't advance, advance timestamp
        if next_after_raw is None or next_after_raw == after:
            if next_after_raw is None:
                stop_reason = "no_next_cursor"
                break
            elif next_after_raw == after:
                # Cursor repeated - try stall escape
                if last_seen_lmts is None:
                    stop_reason = "cursor_repeated_no_timestamp"
                    break
                
                # Advance latestMessageTimestampAfter by 1ms
                lmts_after = advance_timestamp_ms(last_seen_lmts, ms=1)
                after = None
                seen_page_sigs.clear()  # Clear to allow progress with new timestamp
                # Continue loop (don't increment pages since we're doing stall escape)
                continue
    
    # Check if we stopped due to scan limit or max pages (if stop_reason not already set)
    if stop_reason is None:
        if yielded_count >= scan_limit:
            stop_reason = "scan_limit_reached"
        elif pages >= max_pages:
            stop_reason = "max_pages_reached"
        else:
            stop_reason = "unknown"
    
    # Print diagnostic block
    mode_name = "archived" if archived else "live"
    print(f"\nTHREAD ENUMERATION STOP ({mode_name})", file=sys.stderr)
    print(f"- reason: {stop_reason}", file=sys.stderr)
    print(f"- pagesFetched: {pages}", file=sys.stderr)
    if last_request_params:
        # Format params for display (exclude token-related internal fields)
        display_params = {k: v for k, v in last_request_params.items()}
        print(f"- lastRequestParams: {json.dumps(display_params, indent=2)}", file=sys.stderr)
    if last_page_info:
        print(f"- lastPage: {json.dumps(last_page_info, indent=2)}", file=sys.stderr)
    
    # Save last response if not scan_limit_reached
    if stop_reason != "scan_limit_reached" and last_response:
        debug_dir = 'out'
        os.makedirs(debug_dir, exist_ok=True)
        debug_filename = f'last_threads_page_{mode_name}.json'
        debug_path = os.path.join(debug_dir, debug_filename)
        try:
            with open(debug_path, 'w', encoding='utf-8') as f:
                json.dump(last_response, f, indent=2, ensure_ascii=False)
            print(f"- saved last response to: {debug_path}", file=sys.stderr)
        except Exception as e:
            print(f"- warning: failed to save last response: {e}", file=sys.stderr)


def get_messages_efficiently(thread_id: str, messages_limit: int = 60, token: str = None) -> List[Dict[str, Any]]:
    """
    Fetch messages efficiently: only fetch enough to match prompts.
    
    Fetches first page, then continues fetching if needed until either:
    - collected enough MESSAGE/WELCOME_MESSAGE items (--messages-limit), or
    - no next page
    
    Returns filtered and sorted messages (only MESSAGE/WELCOME_MESSAGE types, sorted by createdAt).
    """
    all_results = []
    after = None
    
    while True:
        params = {'limit': 100}
        if after is not None:
            params['after'] = after
        
        status, headers, response = hubspot_request(
            'GET',
            f'/conversations/v3/conversations/threads/{thread_id}/messages',
            params=params,
            token=token
        )
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        
        if status != 200 or not response:
            break
        
        results = response.get('results', [])
        
        # Filter to MESSAGE/WELCOME_MESSAGE
        filtered = [m for m in results if m.get('type') in ('MESSAGE', 'WELCOME_MESSAGE')]
        all_results.extend(filtered)
        
        # Stop if we have enough MESSAGE/WELCOME_MESSAGE items
        if len(all_results) >= messages_limit:
            break
        
        # Check for next page
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        next_after_raw = unquote(next_after_encoded) if next_after_encoded else None
        
        if not next_after_raw:
            break
        
        after = next_after_raw
    
    # Sort all messages by createdAt across all pages
    def get_sort_key(msg):
        dt = parse_iso_datetime(msg.get('createdAt'))
        if dt:
            return (dt.timestamp(), msg.get('id', ''))
        return (0, msg.get('id', ''))
    
    all_results = sorted(all_results, key=get_sort_key)
    
    # Return only first messages_limit items
    return all_results[:messages_limit]


def get_messages_first_page(thread_id: str, token: str = None) -> Dict[str, Any]:
    """Get first page of messages for a thread."""
    status, headers, response = hubspot_request(
        'GET',
        f'/conversations/v3/conversations/threads/{thread_id}/messages',
        params={'limit': 100},
        token=token
    )
    time.sleep(DEFAULT_RATE_LIMIT_DELAY)
    
    if status == 200:
        return response
    return {}


def get_messages_all(thread_id: str, token: str = None) -> List[Dict[str, Any]]:
    """
    Fetch ALL pages of messages for a thread.
    
    IMPORTANT: 'after' cursor is passed as raw string (never pre-encoded).
    Returns list of messages, with _pagingStoppedReason added if paging stopped unexpectedly.
    """
    all_results = []
    after = None
    prev_page_sig = None
    seen_page_sigs = set()
    prev_last_id = None
    stop_reason = None
    page_count = 0
    
    while True:
        params = {'limit': 100}
        
        # Add 'after' cursor only if not None (as raw string, never pre-encoded)
        if after is not None:
            params['after'] = after
        
        status, headers, response = hubspot_request(
            'GET',
            f'/conversations/v3/conversations/threads/{thread_id}/messages',
            params=params,
            token=token
        )
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        page_count += 1
        
        if status != 200 or not response:
            break
        
        results = response.get('results', [])
        
        # Extract IDs for page signature
        ids = [r.get('id') for r in results if r.get('id') is not None]
        
        # Get next cursor
        paging = response.get('paging', {})
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        next_after_raw = unquote(next_after_encoded) if next_after_encoded else None
        
        # Build page signature: (first_msg_id, last_msg_id, count, next_after_raw)
        page_sig = (ids[0] if ids else None, ids[-1] if ids else None, len(ids), next_after_raw)
        
        # Natural end conditions
        if len(results) == 0:
            stop_reason = "empty_results"
            break
        
        if next_after_raw is None:
            stop_reason = "no_next_cursor"
            break
        
        # Cursor not advancing AND last item didn't change -> not moving forward
        if next_after_raw == after and (ids[-1] if ids else None) == prev_last_id:
            stop_reason = "cursor_not_advancing"
            break
        
        # Stuck page detection: same page signature seen again
        if page_sig in seen_page_sigs:
            # Save debug file
            debug_dir = 'out'
            os.makedirs(debug_dir, exist_ok=True)
            debug_path = os.path.join(debug_dir, f'messages_paging_debug_{thread_id}.json')
            try:
                with open(debug_path, 'w', encoding='utf-8') as f:
                    json.dump(response, f, indent=2, ensure_ascii=False)
            except Exception:
                pass
            
            first_id = ids[0] if ids else None
            last_id = ids[-1] if ids else None
            print(
                f"Message pagination appears stuck for thread {thread_id}: received the same page again. Treating as end-of-list.\n"
                f"after_raw={after!r} next_after_raw={next_after_raw!r} "
                f"first_id={first_id!r} last_id={last_id!r} count={len(ids)}",
                file=sys.stderr
            )
            stop_reason = "page_repeated"
            break
        
        # Add results
        all_results.extend(results)
        
        # Track page signature and continue
        seen_page_sigs.add(page_sig)
        prev_page_sig = page_sig
        prev_last_id = ids[-1] if ids else prev_last_id
        after = next_after_raw  # Store only raw cursor
    
    # Mark if paging stopped unexpectedly
    if stop_reason == "page_repeated":
        # Add metadata marker to results list (caller can check)
        # Since we return a list, we'll need to document this separately
        # For now, we just log it
        pass
    
    return all_results


def message_text(msg: Dict[str, Any]) -> str:
    """Extract text from message: prefer text, else richText, else ""."""
    text = msg.get("text", "")
    if text:
        return text
    
    rich_text = msg.get("richText", "")
    if rich_text:
        return rich_text
    
    return ""


def format_speaker_label_for_preview(msg: Dict[str, Any]) -> str:
    """
    Format speaker label for preview display.
    
    Rules:
    - If any sender.actorId startswith "V-" -> Customer
    - Else if any sender.actorId startswith "B-" -> Bot (include sender name if present)
    - Else if any sender.actorId startswith "S-" -> System
    - Else if msg.direction == "OUTGOING" -> Agent
    - Else -> Unknown
    """
    senders = msg.get('senders', [])
    sender_actor_ids = [s.get('actorId', '') for s in senders]
    
    # Check for V- (Customer)
    for actor_id in sender_actor_ids:
        if actor_id and actor_id.startswith('V-'):
            return 'Customer'
    
    # Check for B- (Bot)
    for actor_id in sender_actor_ids:
        if actor_id and actor_id.startswith('B-'):
            # Try to get sender name
            for sender in senders:
                if sender.get('actorId', '').startswith('B-'):
                    sender_name = sender.get('name') or sender.get('deliveryIdentifier', {}).get('value')
                    if sender_name:
                        return f'Bot ({sender_name})'
            return 'Bot'
    
    # Check for S- (System)
    for actor_id in sender_actor_ids:
        if actor_id and actor_id.startswith('S-'):
            return 'System'
    
    # Check direction
    if msg.get('direction') == 'OUTGOING':
        return 'Agent'
    
    return 'Unknown'


def strip_html_to_text(html_str: str) -> str:
    """Strip HTML tags and convert to plain text (reuse existing strip_html)."""
    return strip_html(html_str)


def clean_text_for_preview(text: str) -> str:
    """
    Clean text for single-line preview display.
    - Replace newlines with " / "
    - Trim and collapse whitespace
    """
    if not text:
        return ""
    
    # Strip HTML if needed (should already be done, but be safe)
    cleaned = strip_html_to_text(text) if '<' in text or '>' in text else text
    
    # Replace newlines with " / "
    cleaned = cleaned.replace('\n', ' / ').replace('\r', ' / ')
    
    # Collapse whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    # Trim
    cleaned = cleaned.strip()
    
    return cleaned


def format_datetime_for_preview(dt_str: Optional[str]) -> str:
    """Format ISO datetime string to YYYY-MM-DD HH:MM:SS for preview."""
    if not dt_str:
        return "N/A"
    
    dt = parse_iso_datetime(dt_str)
    if not dt:
        return dt_str
    
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def is_bot_prompt_candidate(msg: Dict[str, Any]) -> bool:
    """
    Check if message is a bot prompt candidate.
    
    Only considers messages of type in {"MESSAGE","WELCOME_MESSAGE"}.
    
    A message is "bot prompt candidate" if any of:
    - any sender.actorId startswith "B-"
    - OR msg.get("createdBy","") startswith "B-"
    - OR msg.get("type") == "WELCOME_MESSAGE"
    - OR (any sender.actorId startswith "S-" and msg.get("direction") == "OUTGOING")  (fallback)
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


def format_iso_datetime(dt: datetime) -> str:
    """Format datetime to ISO8601 with milliseconds and Z suffix."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    
    # Format with milliseconds
    iso_str = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]  # Remove last 3 microseconds to get milliseconds
    return iso_str + 'Z'


def advance_timestamp_ms(lmts_str: str, ms: int = 1) -> str:
    """Advance ISO8601 timestamp string by N milliseconds."""
    dt = parse_iso_datetime(lmts_str)
    if not dt:
        return lmts_str
    
    from datetime import timedelta
    dt_advanced = dt + timedelta(milliseconds=ms)
    return format_iso_datetime(dt_advanced)


def match_required_prompts(messages: List[Dict[str, Any]], messages_limit: int = 60) -> Tuple[bool, int, List[str], List[Dict[str, Any]]]:
    """
    Match REQUIRED_PROMPTS as ordered subsequence in messages.
    
    Args:
        messages: List of message dicts (should be filtered to MESSAGE/WELCOME_MESSAGE and sorted by createdAt)
        messages_limit: Only check first N messages
    
    Returns:
        (matched: bool, matched_count: int, missing: List[str], match_details: List[Dict])
        match_details contains: [{"prompt": str, "messageId": str, "createdAt": str, "textPreview": str}, ...]
    """
    # Filter to only MESSAGE and WELCOME_MESSAGE types, and limit
    filtered = [
        m for m in messages
        if m.get('type') in ('MESSAGE', 'WELCOME_MESSAGE')
    ][:messages_limit]
    
    # Sort by createdAt ascending
    def get_sort_key(msg):
        dt = parse_iso_datetime(msg.get('createdAt'))
        if dt:
            return (dt.timestamp(), msg.get('id', ''))
        return (0, msg.get('id', ''))
    
    filtered = sorted(filtered, key=get_sort_key)
    
    # Build normalized bot lines from bot prompt candidates, tracking message references
    bot_lines = []  # List of (normalized_text, message_dict)
    for msg in filtered:
        if is_bot_prompt_candidate(msg):
            text = message_text(msg)
            if text:
                normalized = normalize_for_match(text)
                if normalized:
                    bot_lines.append((normalized, msg))
    
    # Match prompts in order
    matched_count = 0
    last_match_pos = -1
    missing = []
    match_details = []
    
    for prompt in REQUIRED_PROMPTS:
        found = False
        
        # Search from after last match position
        search_start = last_match_pos + 1
        for i in range(search_start, len(bot_lines)):
            normalized_text, msg = bot_lines[i]
            if prompt in normalized_text:
                matched_count += 1
                last_match_pos = i
                found = True
                
                # Extract match details
                msg_id = msg.get('id', '')
                msg_created_at = msg.get('createdAt', '')
                msg_text = message_text(msg)
                text_preview = msg_text[:100] if len(msg_text) > 100 else msg_text
                
                match_details.append({
                    'prompt': prompt,
                    'messageId': msg_id,
                    'createdAt': msg_created_at,
                    'textPreview': text_preview
                })
                break
        
        if not found:
            missing.append(prompt)
    
    matched = matched_count == len(REQUIRED_PROMPTS)
    return (matched, matched_count, missing, match_details)


def normalize_text(s: str) -> str:
    """
    Normalize text for prompt matching.
    
    - lowercase
    - collapse whitespace
    - remove spaces around slashes (country/ region -> country/region)
    - strip trailing punctuation and extra whitespace
    """
    if not s:
        return ""
    
    # Strip HTML if present
    text = strip_html(s)
    
    # Decode HTML entities
    text = html.unescape(text)
    
    # Replace NBSP and other unicode spaces
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
    
    # Collapse all whitespace to single spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Strip trailing punctuation and whitespace
    text = re.sub(r'[?!.,:;]+$', '', text)
    text = text.strip()
    
    return text


def get_message_text(msg: Dict[str, Any]) -> str:
    """
    Get message text, preferring text field, fallback to richText stripped of tags.
    If both empty but attachments exist, return placeholder.
    """
    text = msg.get("text", "")
    if text:
        return text
    
    rich_text = msg.get("richText", "")
    if rich_text:
        return strip_html(rich_text)
    
    # Check for attachments
    attachments = msg.get("attachments", [])
    if attachments:
        return "[attachment]"
    
    return ""


def is_human_message(msg: Dict[str, Any]) -> bool:
    """
    Check if message is from a human visitor.
    
    Requirements:
    - direction == "INCOMING"
    - Has text/richText OR non-empty attachments
    - Sender/creator identifies visitor (V- prefix)
    """
    # Must be incoming
    if msg.get('direction') != 'INCOMING':
        return False
    
    # Must have content (text, richText, or attachments)
    text = get_message_text(msg)
    has_content = False
    if text and text != "[attachment]":
        has_content = True
    elif text == "[attachment]":
        # get_message_text returned "[attachment]" placeholder, so attachments exist
        has_content = True
    else:
        # No text, check if attachments exist
        attachments = msg.get("attachments", [])
        if attachments:
            has_content = True
    
    if not has_content:
        return False
    
    # Check sender actorIds
    senders = msg.get('senders', [])
    for sender in senders:
        actor_id = sender.get('actorId', '')
        if actor_id and actor_id.startswith('V-'):
            return True
    
    # Check createdBy
    created_by = msg.get('createdBy', '')
    if created_by and created_by.startswith('V-'):
        return True
    
    return False


def compute_chatbot_stage(messages: List[Dict[str, Any]]) -> Tuple[int, Dict[str, Any]]:
    """
    Compute chatbot stage (0-5) based on sequential prompt+human-reply logic.
    
    Stages 1-5: Require prompt exists AND human reply after it (sequential)
    
    Args:
        messages: List of message dicts (will be sorted by createdAt ascending)
    
    Returns:
        (stage: int, debug_info: dict)
        stage: 0-5 (0 = no prompts matched, 5 = all prompts matched)
        debug_info: dict with matched prompts and human replies
    """
    # Sort all messages by createdAt ascending
    def get_sort_key(msg):
        dt = parse_iso_datetime(msg.get('createdAt'))
        if dt:
            return (dt.timestamp(), msg.get('id', ''))
        return (0, msg.get('id', ''))
    
    sorted_messages = sorted(messages, key=get_sort_key)
    
    # Use CHATBOT_PROMPTS_ORDERED directly (5 stages)
    stage_prompts = CHATBOT_PROMPTS_ORDERED
    
    stage = 0
    last_prompt_index = -1
    matched_stages = []
    
    # Process stages 1-5 sequentially
    for stage_num in range(1, MAX_STAGE + 1):
        prompt_text = stage_prompts[stage_num - 1]
        prompt_found = False
        human_reply_found = False
        
        # Find prompt starting from after last matched position
        prompt_msg = None
        prompt_index = -1
        
        for i in range(last_prompt_index + 1, len(sorted_messages)):
            msg = sorted_messages[i]
            
            # Check if this is a bot prompt candidate
            if is_bot_prompt_candidate(msg):
                msg_text = get_message_text(msg)
                if msg_text:
                    normalized = normalize_text(msg_text)
                    if prompt_text in normalized:
                        prompt_found = True
                        prompt_msg = msg
                        prompt_index = i
                        break
        
        if not prompt_found:
            break  # Stop at first missing prompt
        
        # Find human reply after prompt
        for i in range(prompt_index + 1, len(sorted_messages)):
            msg = sorted_messages[i]
            if is_human_message(msg):
                human_reply_found = True
                break
        
        if not human_reply_found:
            break  # Stop if no human reply after prompt
        
        # Stage completed
        stage = stage_num
        last_prompt_index = prompt_index
        
        matched_stages.append({
            'stage': stage_num,
            'prompt': prompt_text,
            'promptMessageId': prompt_msg.get('id', '') if prompt_msg else '',
            'promptCreatedAt': prompt_msg.get('createdAt', '') if prompt_msg else '',
            'humanReplyFound': True
        })
    
    debug_info = {
        'matchedStages': matched_stages,
        'finalStage': stage
    }
    
    return (stage, debug_info)


def ensure_columns(conn: sqlite3.Connection, table_name: str, columns: Dict[str, str]) -> None:
    """
    Ensure columns exist in table, adding them if missing.
    
    Args:
        conn: SQLite connection
        table_name: Name of the table
        columns: Dict mapping column name to column declaration (e.g., "INTEGER NOT NULL DEFAULT 0")
    """
    # Get existing columns
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}
    
    # Add missing columns
    for col_name, col_decl in columns.items():
        if col_name not in existing_columns:
            try:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_decl}")
            except sqlite3.OperationalError as e:
                # Column might have been added concurrently, ignore
                if "duplicate column" not in str(e).lower():
                    raise


def compact_json(obj: Any) -> str:
    """Serialize object to compact JSON string (no extra whitespace)."""
    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'))


def init_db(db_path: str) -> sqlite3.Connection:
    """
    Initialize SQLite database with schema and indexes.
    
    Returns connection with WAL mode enabled.
    """
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    
    # Enable WAL mode for better concurrency
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    
    # Create table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS chatbot_threads (
            thread_id TEXT PRIMARY KEY,
            inbox_id TEXT,
            channel_id TEXT,
            channel_account_id TEXT,
            associated_contact_id TEXT,
            status TEXT,
            created_at TEXT,
            latest_message_timestamp TEXT,
            archived INTEGER,
            is_spam INTEGER,
            raw_thread_json TEXT,
            raw_messages_json TEXT,
            fetched_at TEXT,
            prompt_match_json TEXT
        )
    ''')
    
    # Create indexes
    conn.execute('CREATE INDEX IF NOT EXISTS idx_associated_contact_id ON chatbot_threads(associated_contact_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_channel_account_id ON chatbot_threads(channel_account_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_inbox_id ON chatbot_threads(inbox_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_latest_message_timestamp ON chatbot_threads(latest_message_timestamp)')
    
    # Check if old schema exists (has chatbot_completed_6)
    cursor = conn.execute("PRAGMA table_info(chatbot_threads)")
    existing_cols = {row[1]: row for row in cursor.fetchall()}
    
    # If old schema detected (has chatbot_completed_6), drop and recreate table
    if 'chatbot_completed_6' in existing_cols:
        print("Detected old schema (6 stages). Dropping and recreating chatbot_threads table...", file=sys.stderr)
        conn.execute('DROP TABLE IF EXISTS chatbot_threads')
        conn.execute('DROP INDEX IF EXISTS idx_channel_account_id')
        conn.execute('DROP INDEX IF EXISTS idx_inbox_id')
        conn.execute('DROP INDEX IF EXISTS idx_latest_message_timestamp')
        conn.commit()
        # Recreate table with new schema
        conn.execute('''
            CREATE TABLE IF NOT EXISTS chatbot_threads (
                thread_id TEXT PRIMARY KEY,
                inbox_id TEXT,
                channel_id TEXT,
                channel_account_id TEXT,
                associated_contact_id TEXT,
                status TEXT,
                created_at TEXT,
                latest_message_timestamp TEXT,
                archived INTEGER NOT NULL DEFAULT 0,
                is_spam INTEGER NOT NULL DEFAULT 0,
                raw_thread_json TEXT NOT NULL,
                raw_messages_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                prompt_match_json TEXT,
                chatbot_stage INTEGER NOT NULL DEFAULT 0,
                chatbot_completed_1 INTEGER NOT NULL DEFAULT 0,
                chatbot_completed_2 INTEGER NOT NULL DEFAULT 0,
                chatbot_completed_3 INTEGER NOT NULL DEFAULT 0,
                chatbot_completed_4 INTEGER NOT NULL DEFAULT 0,
                chatbot_completed_5 INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT
            )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_channel_account_id ON chatbot_threads(channel_account_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_inbox_id ON chatbot_threads(inbox_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_latest_message_timestamp ON chatbot_threads(latest_message_timestamp)')
        conn.commit()
        existing_cols = {}  # Reset for ensure_columns check below
    
    # Ensure new columns exist (schema migration for new installs)
    new_columns = {
        'chatbot_stage': 'INTEGER NOT NULL DEFAULT 0',
        'chatbot_completed_1': 'INTEGER NOT NULL DEFAULT 0',
        'chatbot_completed_2': 'INTEGER NOT NULL DEFAULT 0',
        'chatbot_completed_3': 'INTEGER NOT NULL DEFAULT 0',
        'chatbot_completed_4': 'INTEGER NOT NULL DEFAULT 0',
        'chatbot_completed_5': 'INTEGER NOT NULL DEFAULT 0',
        'updated_at': 'TEXT'  # Add updated_at if missing
    }
    ensure_columns(conn, 'chatbot_threads', new_columns)
    
    # Verify schema (debug log)
    cursor = conn.execute("PRAGMA table_info(chatbot_threads)")
    existing_cols = [row[1] for row in cursor.fetchall()]
    print(f"Database schema verified: {len(existing_cols)} columns in chatbot_threads", file=sys.stderr)
    # Verify chatbot_completed_6 does not exist
    if 'chatbot_completed_6' in existing_cols:
        print("ERROR: chatbot_completed_6 still exists in schema. Please delete the database file and rebuild.", file=sys.stderr)
        sys.exit(1)
    
    conn.commit()
    return conn


def upsert_chatbot_thread(conn: sqlite3.Connection, thread_details: Dict[str, Any],
                          messages_agg: Dict[str, Any], prompt_match_obj: Dict[str, Any],
                          chatbot_stage: int = 0) -> None:
    """
    Upsert a chatbot thread into the database.
    
    Args:
        conn: SQLite connection
        thread_details: Raw thread details from GET /threads/{id}
        messages_agg: Aggregated messages response with results, paging, _pagesFetched
        prompt_match_obj: Prompt match metadata
        chatbot_stage: Chatbot stage (0-5)
    """
    thread_id = thread_details.get('id', '')
    if not thread_id:
        return
    
    # Extract fields from thread_details
    inbox_id = thread_details.get('inboxId')
    channel_id = thread_details.get('originalChannelId') or thread_details.get('channelId')
    channel_account_id = thread_details.get('originalChannelAccountId') or thread_details.get('channelAccountId')
    associated_contact_id = thread_details.get('associatedContactId')
    status = thread_details.get('status')
    created_at = thread_details.get('createdAt')
    latest_message_timestamp = thread_details.get('latestMessageTimestamp')
    archived = 1 if thread_details.get('archived', False) else 0
    is_spam = 1 if thread_details.get('spam', False) else 0
    
    # Compute one-hot encoding for stage (5 stages total)
    c1, c2, c3, c4, c5 = 0, 0, 0, 0, 0
    if 1 <= chatbot_stage <= MAX_STAGE:
        if chatbot_stage == 1:
            c1 = 1
        elif chatbot_stage == 2:
            c2 = 1
        elif chatbot_stage == 3:
            c3 = 1
        elif chatbot_stage == 4:
            c4 = 1
        elif chatbot_stage == 5:
            c5 = 1
    
    # Serialize JSON
    raw_thread_json = compact_json(thread_details)
    raw_messages_json = compact_json(messages_agg)
    prompt_match_json = compact_json(prompt_match_obj)
    fetched_at = datetime.now(timezone.utc).isoformat()
    updated_at = fetched_at
    
    # Build column list and values tuple to prevent mismatches
    cols = [
        'thread_id',
        'inbox_id',
        'channel_id',
        'channel_account_id',
        'associated_contact_id',
        'status',
        'created_at',
        'latest_message_timestamp',
        'archived',
        'is_spam',
        'raw_thread_json',
        'raw_messages_json',
        'fetched_at',
        'prompt_match_json',
        'chatbot_stage',
        'chatbot_completed_1',
        'chatbot_completed_2',
        'chatbot_completed_3',
        'chatbot_completed_4',
        'chatbot_completed_5',
        'updated_at'
    ]
    
    values = [
        thread_id,
        inbox_id,
        channel_id,
        channel_account_id,
        associated_contact_id,
        status,
        created_at,
        latest_message_timestamp,
        archived,
        is_spam,
        raw_thread_json,
        raw_messages_json,
        fetched_at,
        prompt_match_json,
        chatbot_stage,
        c1,
        c2,
        c3,
        c4,
        c5,
        updated_at
    ]
    
    # Assertion to catch mismatches
    assert len(values) == len(cols), f"SQL mismatch: cols={len(cols)} values={len(values)}"
    
    # Build placeholders
    placeholders = ','.join(['?'] * len(cols))
    
    # Build UPDATE SET clause (all columns except thread_id)
    update_cols = [c for c in cols if c != 'thread_id']
    update_set = ','.join([f'{c}=excluded.{c}' for c in update_cols])
    
    # Build SQL
    sql = f'''INSERT INTO chatbot_threads ({','.join(cols)})
              VALUES ({placeholders})
              ON CONFLICT(thread_id) DO UPDATE SET {update_set}'''
    
    # Execute
    conn.execute(sql, values)


def load_one_for_stage(conn: sqlite3.Connection, stage: int, seed: int) -> Optional[Dict[str, Any]]:
    """
    Load one thread for a given stage from SQLite database.
    
    Args:
        conn: SQLite connection
        stage: Chatbot stage (1-5)
        seed: Random seed for deterministic selection
    
    Returns:
        Dict with row data or None if no rows found
    """
    # Query top 200 candidates ordered by latest_message_timestamp DESC
    # Prefer threads with associated_contact_id
    cursor = conn.execute('''
        SELECT
            thread_id,
            inbox_id,
            channel_account_id,
            associated_contact_id,
            status,
            created_at,
            latest_message_timestamp,
            archived,
            chatbot_stage,
            chatbot_completed_1,
            chatbot_completed_2,
            chatbot_completed_3,
            chatbot_completed_4,
            chatbot_completed_5,
            raw_thread_json,
            raw_messages_json,
            fetched_at
        FROM chatbot_threads
        WHERE chatbot_stage = ?
        ORDER BY 
            CASE WHEN associated_contact_id IS NOT NULL THEN 0 ELSE 1 END,
            latest_message_timestamp DESC
        LIMIT 200
    ''', (stage,))
    
    rows = cursor.fetchall()
    
    if not rows:
        return None
    
    # If only one row, use it
    if len(rows) == 1:
        row = rows[0]
    else:
        # Deterministic random selection from candidates
        rng = random.Random(seed)
        row = rng.choice(rows)
    
    # Convert row to dict
    return {
        'thread_id': row[0],
        'inbox_id': row[1],
        'channel_account_id': row[2],
        'associated_contact_id': row[3],
        'status': row[4],
        'created_at': row[5],
        'latest_message_timestamp': row[6],
        'archived': row[7],
        'chatbot_stage': row[8],
        'chatbot_completed_1': row[9],
        'chatbot_completed_2': row[10],
        'chatbot_completed_3': row[11],
        'chatbot_completed_4': row[12],
        'chatbot_completed_5': row[13],
        'raw_thread_json': row[14],
        'raw_messages_json': row[15],
        'fetched_at': row[16]
    }


def get_one_per_stage(db_path: str, out_dir: str, seed: int, pretty: bool, save: bool, no_truncate: bool) -> int:
    """
    Get one thread per stage (1-5) from SQLite and print/save bundles.
    
    Returns:
        Exit code (0 = success, 1 = some stages missing)
    """
    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}", file=sys.stderr)
        return 1
    
    conn = sqlite3.connect(db_path)
    
    print("\n" + "=" * 60, file=sys.stderr)
    print("GET ONE PER STAGE (from DB)", file=sys.stderr)
    print(f"db: {db_path}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    missing_stages = []
    bundles = []
    
    # Load one thread for each stage 1-5
    for stage in range(1, TOTAL_STAGES + 1):
        row_data = load_one_for_stage(conn, stage, seed)
        
        if not row_data:
            print(f"\nNo rows found for stage {stage}", file=sys.stderr)
            missing_stages.append(stage)
            continue
        
        # Parse JSON columns
        try:
            thread_obj = json.loads(row_data['raw_thread_json'])
            messages_obj = json.loads(row_data['raw_messages_json'])
        except json.JSONDecodeError as e:
            print(f"Error: Failed to parse JSON for stage {stage}: {e}", file=sys.stderr)
            missing_stages.append(stage)
            continue
        
        # Build one-hot dict
        one_hot = {
            'chatbot_completed_1': row_data['chatbot_completed_1'],
            'chatbot_completed_2': row_data['chatbot_completed_2'],
            'chatbot_completed_3': row_data['chatbot_completed_3'],
            'chatbot_completed_4': row_data['chatbot_completed_4'],
            'chatbot_completed_5': row_data['chatbot_completed_5']
        }
        
        # Build bundle
        bundle = {
            'threadId': row_data['thread_id'],
            'thread': thread_obj,
            'messagesResponse': messages_obj,
            'chatbotStage': row_data['chatbot_stage'],
            'oneHot': one_hot,
            'fetchedAt': row_data['fetched_at']
        }
        
        bundles.append((stage, row_data, bundle))
    
    conn.close()
    
    # Print bundles
    for stage, row_data, bundle in bundles:
        print(f"\n----- STAGE {stage}/{TOTAL_STAGES} -----", file=sys.stderr)
        print(f"thread_id: {row_data['thread_id']}", file=sys.stderr)
        print(f"associated_contact_id: {row_data['associated_contact_id'] or 'None'}", file=sys.stderr)
        print(f"latest_message_timestamp: {row_data['latest_message_timestamp']}", file=sys.stderr)
        print(f"inbox_id: {row_data['inbox_id']}", file=sys.stderr)
        print(f"channel_account_id: {row_data['channel_account_id']}", file=sys.stderr)
        print(f"status: {row_data['status']}", file=sys.stderr)
        
        # Check size for truncation (only for terminal output, not file saves)
        bundle_json_str = json.dumps(bundle, separators=(',', ':'))
        bundle_size_mb = len(bundle_json_str.encode('utf-8')) / (1024 * 1024)
        
        should_truncate_terminal = bundle_size_mb > 5 and not no_truncate and not pretty
        
        if should_truncate_terminal:
            # Print truncated bundle to terminal
            bundle_truncated = bundle.copy()
            bundle_truncated['messagesResponse'] = None
            save_note = " (use --save to write full bundle to file)" if save else ""
            print(f"\n(messagesResponse omitted from terminal; bundle >5MB{save_note})", file=sys.stderr)
            print(json.dumps(bundle_truncated, separators=(',', ':'), ensure_ascii=False))
        else:
            # Print full bundle to stdout
            if pretty:
                print(json.dumps(bundle, indent=2, ensure_ascii=False))
            else:
                print(json.dumps(bundle, separators=(',', ':'), ensure_ascii=False))
    
    # Save to files if requested
    if save:
        os.makedirs(out_dir, exist_ok=True)
        for stage, row_data, bundle in bundles:
            filename = os.path.join(out_dir, f'get_one_stage_{stage}.json')
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    if pretty:
                        json.dump(bundle, f, indent=2, ensure_ascii=False)
                    else:
                        json.dump(bundle, f, separators=(',', ':'), ensure_ascii=False)
                print(f"\nSaved stage {stage} bundle to: {filename}", file=sys.stderr)
            except Exception as e:
                print(f"Warning: Failed to save stage {stage} bundle: {e}", file=sys.stderr)
    
    # Print summary
    print(f"\nSummary: Found bundles for {len(bundles)}/{TOTAL_STAGES} stages", file=sys.stderr)
    if missing_stages:
        print(f"Missing stages: {missing_stages} (no data in database for these stages)", file=sys.stderr)
    
    # Return exit code 0 (missing stages are informational, not an error)
    return 0


def get_thread_details(thread_id: str, token: str = None) -> Dict[str, Any]:
    """Get detailed thread information."""
    status, headers, response = hubspot_request(
        'GET',
        f'/conversations/v3/conversations/threads/{thread_id}',
        params={},
        token=token
    )
    time.sleep(DEFAULT_RATE_LIMIT_DELAY)
    
    if status == 200:
        return response
    return {}


def get_messages_all_for_storage(thread_id: str, token: str = None) -> Dict[str, Any]:
    """
    Fetch ALL pages of messages for a thread, returning aggregated response.
    
    Returns dict with:
    {
      "results": [... all messages ...],
      "paging": <last paging if any>,
      "_pagesFetched": N
    }
    """
    all_results = []
    after = None
    pages_fetched = 0
    final_paging = None
    
    while True:
        params = {'limit': 100}
        if after is not None:
            params['after'] = after
        
        status, headers, response = hubspot_request(
            'GET',
            f'/conversations/v3/conversations/threads/{thread_id}/messages',
            params=params,
            token=token
        )
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        pages_fetched += 1
        
        if status != 200 or not response:
            break
        
        results = response.get('results', [])
        all_results.extend(results)
        
        # Track paging
        paging = response.get('paging', {})
        final_paging = paging
        
        # Check for next page
        next_page = paging.get('next', {})
        next_after_encoded = next_page.get('after')
        next_after_raw = unquote(next_after_encoded) if next_after_encoded else None
        
        if not next_after_raw:
            break
        
        after = next_after_raw
    
    return {
        'results': all_results,
        'paging': final_paging,
        '_pagesFetched': pages_fetched
    }


def analyze_mismatches(mismatch_data: List[Dict[str, Any]], output_path: str, samples_per_bucket: int,
                       total_threads: int, completed_count: int) -> None:
    """
    Analyze mismatch patterns and generate report.
    
    Args:
        mismatch_data: List of thread classification dicts (must include 'chatbotStage' field)
        output_path: Path to write JSON report
        samples_per_bucket: Number of samples to include per bucket
        total_threads: Total threads scanned (unused, derived from buckets)
        completed_count: Number of completed chatbot threads (unused, derived from buckets)
    """
    
    # Step 1: Build thread_stage mapping (unique by threadId)
    thread_stage = {}  # {threadId: stage}
    thread_data_map = {}  # {threadId: thread_data} for lookup
    
    for thread_data in mismatch_data:
        thread_id = thread_data.get('threadId', '')
        if not thread_id:
            continue
        
        # Use chatbotStage if available, else fallback to matchedCount
        stage = thread_data.get('chatbotStage', thread_data.get('matchedCount', 0))
        # Clamp stage to [0..MAX_STAGE]
        stage = max(0, min(MAX_STAGE, int(stage)))
        
        thread_stage[thread_id] = stage
        thread_data_map[thread_id] = thread_data
    
    # Step 2: Build buckets from thread_stage mapping
    buckets = {i: [] for i in range(MAX_STAGE + 1)}  # {0: [...], 1: [...], ..., MAX_STAGE: [...]}
    
    for thread_id, stage in thread_stage.items():
        buckets[stage].append(thread_data_map[thread_id])
    
    # Step 3: Derive ALL counts from bucket_counts
    bucket_counts = {i: len(buckets[i]) for i in range(MAX_STAGE + 1)}
    total_threads_derived = sum(bucket_counts.values())
    not_chatbot = bucket_counts[0]
    started = total_threads_derived - not_chatbot  # or sum(bucket_counts[i] for i in range(1, MAX_STAGE+1))
    completed = bucket_counts[MAX_STAGE]
    incomplete = started - completed
    
    # Step 4: Compute missing prompt analysis by bucket
    most_common_missing_any = {}  # Most common missing prompt (anywhere in list)
    most_common_first_missing = {}  # Most common first missing prompt (blocker)
    next_expected_prompt_by_bucket = {}  # Deterministic next expected prompt
    
    for bucket_level in range(1, MAX_STAGE):  # 1-4 (not 0 or MAX_STAGE)
        # Next expected prompt is deterministic
        next_expected_prompt_by_bucket[bucket_level] = CHATBOT_PROMPTS_ORDERED[bucket_level]
        
        # Compute first missing prompt for each thread in this bucket
        first_missing_list = []
        missing_prompts_list = []  # For "any missing" analysis
        
        for thread_data in buckets[bucket_level]:
            missing_prompts = thread_data.get('missingPrompts', [])
            missing_prompts_list.extend(missing_prompts)
            
            # Find first missing prompt (earliest stage that's missing)
            # missingPrompts is a list of prompt strings that are missing
            # We need to find the first one in CHATBOT_PROMPTS_ORDERED that appears in missingPrompts
            first_missing = None
            for prompt in CHATBOT_PROMPTS_ORDERED:
                if prompt in missing_prompts:
                    first_missing = prompt
                    break
            
            if first_missing:
                first_missing_list.append(first_missing)
        
        # Most common first missing prompt (the blocker)
        if first_missing_list:
            counter_first = Counter(first_missing_list)
            most_common_first = counter_first.most_common(1)[0]
            most_common_first_missing[bucket_level] = {
                'prompt': most_common_first[0],
                'count': most_common_first[1]
            }
        
        # Most common missing prompt (anywhere, not necessarily the blocker)
        if missing_prompts_list:
            counter_any = Counter(missing_prompts_list)
            most_common_any = counter_any.most_common(1)[0]
            most_common_missing_any[bucket_level] = {
                'prompt': most_common_any[0],
                'count': most_common_any[1]
            }
    
    # Build progress buckets structure for JSON
    progress_buckets = {}
    for level in range(MAX_STAGE + 1):  # 0-5
        bucket_threads = buckets[level]
        sample_threads = bucket_threads[:samples_per_bucket]
        
        samples = []
        for thread_data in sample_threads:
            samples.append({
                'threadId': thread_data.get('threadId', ''),
                'latestMessageTimestamp': thread_data.get('latestMessageTimestamp'),
                'inboxId': thread_data.get('inboxId'),
                'channelAccountId': thread_data.get('channelAccountId'),
                'associatedContactId': thread_data.get('associatedContactId')
            })
        
        progress_buckets[str(level)] = {
            'count': bucket_counts[level],
            'sampleThreadIds': [t.get('threadId', '') for t in sample_threads],
            'samples': samples
        }
    
    # Print analysis block (using derived counts)
    print("\n" + "=" * 60, file=sys.stderr)
    print("MISMATCH ANALYSIS", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Total threads: {total_threads_derived}", file=sys.stderr)
    print(f"Completed chatbot ({MAX_STAGE}/{MAX_STAGE}): {completed}", file=sys.stderr)
    print(f"Started chatbot (>={1}/{MAX_STAGE}): {started}", file=sys.stderr)
    print(f"Incomplete chatbot (1-{MAX_STAGE-1}/{MAX_STAGE}): {incomplete}", file=sys.stderr)
    print(f"Not chatbot (0/{MAX_STAGE}): {not_chatbot}", file=sys.stderr)
    
    print("\nProgress distribution:", file=sys.stderr)
    for level in range(MAX_STAGE, -1, -1):  # MAX_STAGE down to 0
        count = bucket_counts[level]
        if level == MAX_STAGE:
            print(f"  - {level}/{MAX_STAGE}: {count} (completed)", file=sys.stderr)
        elif level > 0:
            # Print next expected prompt and first missing prompt info
            info_parts = []
            if level in next_expected_prompt_by_bucket:
                info_parts.append(f'next expected: "{next_expected_prompt_by_bucket[level]}"')
            if level in most_common_first_missing:
                info_parts.append(f'most common blocker: "{most_common_first_missing[level]["prompt"]}" (N={most_common_first_missing[level]["count"]})')
            if level in most_common_missing_any:
                info_parts.append(f'any missing: "{most_common_missing_any[level]["prompt"]}" (N={most_common_missing_any[level]["count"]})')
            
            missing_info = " (" + "; ".join(info_parts) + ")" if info_parts else ""
            print(f"  - {level}/{MAX_STAGE}: {count}{missing_info}", file=sys.stderr)
        else:
            print(f"  - {level}/{MAX_STAGE}: {count} (not chatbot)", file=sys.stderr)
    
    # Print sample thread IDs for key buckets
    print("\nSample thread IDs:", file=sys.stderr)
    
    # MAX_STAGE/MAX_STAGE bucket (completed)
    if len(buckets[MAX_STAGE]) > 0:
        print(f"\n{MAX_STAGE}/{MAX_STAGE} bucket (first {min(samples_per_bucket, len(buckets[MAX_STAGE]))}):", file=sys.stderr)
        for thread_data in buckets[MAX_STAGE][:samples_per_bucket]:
            print(f"  {thread_data.get('threadId')} | "
                  f"latestMsg={thread_data.get('latestMessageTimestamp', 'N/A')} | "
                  f"inboxId={thread_data.get('inboxId', 'N/A')} | "
                  f"channelAccountId={thread_data.get('channelAccountId', 'N/A')} | "
                  f"contactId={thread_data.get('associatedContactId', 'N/A')}", file=sys.stderr)
    
    # 1/MAX_STAGE bucket
    if len(buckets[1]) > 0:
        print(f"\n1/{MAX_STAGE} bucket (first {min(samples_per_bucket, len(buckets[1]))}):", file=sys.stderr)
        for thread_data in buckets[1][:samples_per_bucket]:
            print(f"  {thread_data.get('threadId')} | "
                  f"latestMsg={thread_data.get('latestMessageTimestamp', 'N/A')} | "
                  f"inboxId={thread_data.get('inboxId', 'N/A')} | "
                  f"channelAccountId={thread_data.get('channelAccountId', 'N/A')} | "
                  f"contactId={thread_data.get('associatedContactId', 'N/A')}", file=sys.stderr)
    
    # 0/MAX_STAGE bucket
    if len(buckets[0]) > 0:
        print(f"\n0/{MAX_STAGE} bucket (first {min(samples_per_bucket, len(buckets[0]))}):", file=sys.stderr)
        for thread_data in buckets[0][:samples_per_bucket]:
            print(f"  {thread_data.get('threadId')} | "
                  f"latestMsg={thread_data.get('latestMessageTimestamp', 'N/A')} | "
                  f"inboxId={thread_data.get('inboxId', 'N/A')} | "
                  f"channelAccountId={thread_data.get('channelAccountId', 'N/A')} | "
                  f"contactId={thread_data.get('associatedContactId', 'N/A')}", file=sys.stderr)
    
    # Build JSON report (using derived counts)
    report = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'totals': {
            'threads': total_threads_derived,
            'completed': completed,
            'started': started,
            'incomplete': incomplete,
            'notChatbot': not_chatbot
        },
        'bucketCounts': bucket_counts,
        'progressBuckets': progress_buckets,
        'nextExpectedPromptByBucket': {str(k): v for k, v in next_expected_prompt_by_bucket.items()},
        'mostCommonFirstMissingPromptByBucket': {str(k): v for k, v in most_common_first_missing.items()},
        'mostCommonMissingPromptAnyByBucket': {str(k): v for k, v in most_common_missing_any.items()}
    }
    
    # Include perThread array if total threads <= 5000
    if total_threads_derived <= 5000:
        per_thread = []
        for thread_id, stage in thread_stage.items():
            thread_data = thread_data_map[thread_id]
            per_thread.append({
                'threadId': thread_id,
                'chatbotStage': stage,
                'matchedCount': thread_data.get('matchedCount', 0),
                'started': stage >= 1,
                'inboxId': thread_data.get('inboxId'),
                'channelAccountId': thread_data.get('channelAccountId'),
                'associatedContactId': thread_data.get('associatedContactId')
            })
        report['perThread'] = per_thread
    
    # Write JSON report
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\nMismatch analysis report written to: {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Failed to write mismatch report: {e}", file=sys.stderr)


def analyze_nonbot(per_thread_results: List[Dict[str, Any]], sample_fraction: float, max_lines: int,
                   seed: int, token: str, mismatch_out_path: Optional[str]) -> None:
    """
    Analyze non-bot conversations by sampling and printing previews.
    
    Args:
        per_thread_results: List of thread results with matchedCount for ALL scanned threads
        sample_fraction: Fraction of nonbot threads to sample (0.0-1.0)
        max_lines: Maximum message lines to print per thread
        seed: Random seed for reproducible sampling
        token: HubSpot API token
        mismatch_out_path: Path to mismatch report (if exists, write JSON here too)
    """
    import math
    from collections import Counter
    
    # Filter nonbot threads EXACTLY as: matchedCount == 0
    nonbot_threads = [t for t in per_thread_results if t.get("matchedCount", 0) == 0]
    
    nonbot_total = len(nonbot_threads)
    
    # Print sanity check
    print(f"\nNon-bot threads (matchedCount==0): {nonbot_total}", file=sys.stderr)
    
    if nonbot_total == 0:
        print("Unexpected: mismatch logic previously found non-bot threads. Check that per_thread_results is populated.", file=sys.stderr)
        return
    
    # Random sampling
    sample_count = math.ceil(nonbot_total * sample_fraction)
    rng = random.Random(seed)
    sampled_threads = rng.sample(nonbot_threads, min(sample_count, nonbot_total))
    
    # Sort by latestMessageTimestamp descending
    def get_sort_key(thread_data):
        lmts = thread_data.get('latestMessageTimestamp')
        dt = parse_iso_datetime(lmts) if lmts else None
        return dt.timestamp() if dt else 0
    
    sampled_threads.sort(key=get_sort_key, reverse=True)
    
    # Print header
    print("\n" + "=" * 60, file=sys.stderr)
    print(f"NON-BOT SAMPLE (count={len(sampled_threads)} of total_nonbot={nonbot_total})", file=sys.stderr)
    print(f"seed={seed} fraction={sample_fraction}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # JSON data for report
    json_samples = []
    
    # Process each sampled thread
    for idx, thread_data in enumerate(sampled_threads, 1):
        thread_id = thread_data.get('threadId', '')
        if not thread_id:
            continue
        
        print(f"\n----- NONBOT THREAD {idx}/{len(sampled_threads)} -----", file=sys.stderr)
        print(f"threadId: {thread_id}", file=sys.stderr)
        print(f"inboxId: {thread_data.get('inboxId', 'N/A')}", file=sys.stderr)
        print(f"channelAccountId: {thread_data.get('channelAccountId', 'N/A')}", file=sys.stderr)
        print(f"latestMessageTimestamp: {thread_data.get('latestMessageTimestamp', 'N/A')}", file=sys.stderr)
        print(f"associatedContactId: {thread_data.get('associatedContactId') or 'None'}", file=sys.stderr)
        
        # Fetch first page of messages
        status_code, headers, response = hubspot_request(
            'GET',
            f'/conversations/v3/conversations/threads/{thread_id}/messages',
            params={'limit': 100},
            token=token
        )
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        
        preview_lines = []
        type_counts = Counter()
        
        if status_code == 200 and response:
            results = response.get('results', [])
            
            # Count all message types from first page
            for msg in results:
                msg_type = msg.get('type', 'UNKNOWN')
                type_counts[msg_type] += 1
            
            # Filter to MESSAGE/WELCOME_MESSAGE and sort by createdAt
            message_messages = [
                m for m in results
                if m.get('type') in ('MESSAGE', 'WELCOME_MESSAGE')
            ]
            
            def get_msg_sort_key(msg):
                dt = parse_iso_datetime(msg.get('createdAt'))
                return dt.timestamp() if dt else 0
            
            message_messages.sort(key=get_msg_sort_key)
            
            # Take up to max_lines
            for msg in message_messages[:max_lines]:
                created_at = format_datetime_for_preview(msg.get('createdAt'))
                speaker = format_speaker_label_for_preview(msg)
                text = clean_text_for_preview(message_text(msg))
                
                preview_line = f"[{created_at}] {speaker}: {text}"
                preview_lines.append(preview_line)
                print(preview_line, file=sys.stderr)
        
        # Print type counts
        print(f"\nMessage type counts (first page only): {dict(type_counts)}", file=sys.stderr)
        
        # Store for JSON
        json_samples.append({
            'threadId': thread_id,
            'threadMeta': {
                'inboxId': thread_data.get('inboxId'),
                'channelAccountId': thread_data.get('channelAccountId'),
                'latestMessageTimestamp': thread_data.get('latestMessageTimestamp'),
                'associatedContactId': thread_data.get('associatedContactId')
            },
            'previewLines': preview_lines,
            'typeCountsFirstPage': dict(type_counts)
        })
    
    # Always write JSON report
    json_path = os.path.join('out', 'nonbot_sample_report.json')
    os.makedirs('out', exist_ok=True)
    try:
        report = {
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'seed': seed,
            'fraction': sample_fraction,
            'nonbotTotal': nonbot_total,
            'sampled': json_samples
        }
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\nNonbot sample JSON report written to: {json_path}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Failed to write nonbot JSON report: {e}", file=sys.stderr)


def get10_nonbot(per_thread_results: List[Dict[str, Any]], sample_n: int, max_lines: int,
                 seed: int, token: str, output_path: str) -> None:
    """
    Print N random non-bot threads (matchedCount==0) with short previews.
    
    Args:
        per_thread_results: List of thread results with matchedCount for ALL scanned threads
        sample_n: Number of threads to sample (default: 10)
        max_lines: Maximum message lines to print per thread
        seed: Random seed for reproducible sampling
        token: HubSpot API token
        output_path: Path to write JSON report
    """
    import math
    from collections import Counter
    
    # Filter nonbot threads EXACTLY as: matchedCount == 0
    nonbot_threads = [t for t in per_thread_results if t.get("matchedCount", 0) == 0]
    
    nonbot_total = len(nonbot_threads)
    
    # Print sanity check
    print(f"\nNon-bot threads (matchedCount==0): {nonbot_total}", file=sys.stderr)
    
    if nonbot_total == 0:
        print("No non-bot threads found.", file=sys.stderr)
        return
    
    # Random sampling
    actual_sample_n = min(sample_n, nonbot_total)
    rng = random.Random(seed)
    sampled_threads = rng.sample(nonbot_threads, actual_sample_n)
    
    # Sort by latestMessageTimestamp descending
    def get_sort_key(thread_data):
        lmts = thread_data.get('latestMessageTimestamp')
        dt = parse_iso_datetime(lmts) if lmts else None
        return dt.timestamp() if dt else 0
    
    sampled_threads.sort(key=get_sort_key, reverse=True)
    
    # Print header
    print("\n" + "=" * 60, file=sys.stderr)
    print(f"NON-BOT RANDOM SAMPLE (N={actual_sample_n} of total_nonbot={nonbot_total})", file=sys.stderr)
    print(f"seed={seed}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # JSON data for report
    json_samples = []
    
    # Process each sampled thread
    for idx, thread_data in enumerate(sampled_threads, 1):
        thread_id = thread_data.get('threadId', '')
        if not thread_id:
            continue
        
        print(f"\n----- NONBOT THREAD {idx}/{actual_sample_n} -----", file=sys.stderr)
        print(f"threadId: {thread_id}", file=sys.stderr)
        print(f"inboxId: {thread_data.get('inboxId', 'N/A')}", file=sys.stderr)
        print(f"originalChannelAccountId: {thread_data.get('originalChannelAccountId', 'N/A')}", file=sys.stderr)
        print(f"originalChannelId: {thread_data.get('originalChannelId', 'N/A')}", file=sys.stderr)
        print(f"status: {thread_data.get('status', 'N/A')}", file=sys.stderr)
        print(f"createdAt: {thread_data.get('createdAt', 'N/A')}", file=sys.stderr)
        print(f"latestMessageTimestamp: {thread_data.get('latestMessageTimestamp', 'N/A')}", file=sys.stderr)
        print(f"associatedContactId: {thread_data.get('associatedContactId') or 'None'}", file=sys.stderr)
        
        # Fetch first page of messages
        status_code, headers, response = hubspot_request(
            'GET',
            f'/conversations/v3/conversations/threads/{thread_id}/messages',
            params={'limit': 100},
            token=token
        )
        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
        
        preview_lines = []
        type_counts = Counter()
        
        print(f"\nPreview (up to {max_lines} lines):", file=sys.stderr)
        
        if status_code == 200 and response:
            results = response.get('results', [])
            
            # Count all message types from first page
            for msg in results:
                msg_type = msg.get('type', 'UNKNOWN')
                type_counts[msg_type] += 1
            
            # Filter to MESSAGE/WELCOME_MESSAGE and sort by createdAt
            message_messages = [
                m for m in results
                if m.get('type') in ('MESSAGE', 'WELCOME_MESSAGE')
            ]
            
            def get_msg_sort_key(msg):
                dt = parse_iso_datetime(msg.get('createdAt'))
                return dt.timestamp() if dt else 0
            
            message_messages.sort(key=get_msg_sort_key)
            
            # Take up to max_lines
            for msg in message_messages[:max_lines]:
                created_at = format_datetime_for_preview(msg.get('createdAt'))
                speaker = format_speaker_label_for_preview(msg)
                text = clean_text_for_preview(message_text(msg))
                
                preview_line = f"[{created_at}] {speaker}: {text}"
                preview_lines.append(preview_line)
                print(preview_line, file=sys.stderr)
        
        # Print type counts
        print(f"\nMessage type counts (first page only): {dict(type_counts)}", file=sys.stderr)
        
        # Store for JSON
        json_samples.append({
            'threadId': thread_id,
            'threadMeta': {
                'inboxId': thread_data.get('inboxId'),
                'originalChannelAccountId': thread_data.get('originalChannelAccountId'),
                'originalChannelId': thread_data.get('originalChannelId'),
                'status': thread_data.get('status'),
                'createdAt': thread_data.get('createdAt'),
                'latestMessageTimestamp': thread_data.get('latestMessageTimestamp'),
                'associatedContactId': thread_data.get('associatedContactId')
            },
            'previewLines': preview_lines,
            'typeCountsFirstPage': dict(type_counts)
        })
    
    # Write JSON report
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    try:
        report = {
            'generatedAt': datetime.now(timezone.utc).isoformat(),
            'seed': seed,
            'requestedN': sample_n,
            'sampledN': actual_sample_n,
            'nonbotTotal': nonbot_total,
            'sampled': json_samples
        }
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\nNon-bot sample JSON report written to: {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"Warning: Failed to write non-bot JSON report: {e}", file=sys.stderr)


def keyword_prefilter(first_page_messages: List[Dict[str, Any]]) -> bool:
    """
    Check if first page contains keywords in bot prompt candidates.
    
    Returns True if any keyword found, False otherwise.
    """
    normalized_text = ""
    for msg in first_page_messages:
        if msg.get('type') in ('MESSAGE', 'WELCOME_MESSAGE') and is_bot_prompt_candidate(msg):
            text = message_text(msg)
            if text:
                normalized = normalize_for_match(text)
                if normalized:
                    normalized_text += " " + normalized
    
    # Check if any keyword appears
    return any(keyword.lower() in normalized_text for keyword in PREFILTER_KEYWORDS)


def main():
    parser = argparse.ArgumentParser(
        description='Count chatbot conversations in HubSpot OLD portal',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--inbox-id',
        type=str,
        help='Optional: only scan threads in this inboxId'
    )
    parser.add_argument(
        '--channel-account-id',
        type=str,
        help='Optional: only scan threads whose originalChannelAccountId equals this'
    )
    parser.add_argument(
        '--scan-limit',
        type=int,
        default=100000,
        help='Maximum threads to evaluate (default: 100000)'
    )
    parser.add_argument(
        '--max-pages',
        type=int,
        default=5000,
        help='Hard cap on thread list pages (default: 5000)'
    )
    parser.add_argument(
        '--archived-mode',
        type=str,
        choices=['live', 'archived', 'both'],
        default='both',
        help='Which threads to scan: live, archived, or both (default: both)'
    )
    parser.add_argument(
        '--messages-limit',
        type=int,
        default=60,
        help='Only check first N MESSAGE/WELCOME_MESSAGE items (default: 60)'
    )
    parser.add_argument(
        '--fast',
        action='store_true',
        help='Fetch all message pages only if first page passes keyword prefilter'
    )
    parser.add_argument(
        '--json-out',
        type=str,
        help='Optional: write JSON report to this path'
    )
    parser.add_argument(
        '--progress-every',
        type=int,
        default=100,
        help='Print progress every N threads (default: 100)'
    )
    parser.add_argument(
        '--since',
        type=str,
        help='Optional: only count threads createdAt >= since (ISO8601)'
    )
    parser.add_argument(
        '--until',
        type=str,
        help='Optional: only count threads createdAt <= until (ISO8601)'
    )
    parser.add_argument(
        '--write-chatbot',
        action='store_true',
        help='Store completed chatbot conversations (stage=5) in SQLite database'
    )
    parser.add_argument(
        '--write-chatbot-all',
        action='store_true',
        help='Store all chatbot-started conversations (stage>=1) in SQLite database'
    )
    parser.add_argument(
        '--db',
        type=str,
        default='./out/chatbot_conversations.sqlite',
        help='SQLite database file path (default: ./out/chatbot_conversations.sqlite)'
    )
    parser.add_argument(
        '--commit-every',
        type=int,
        default=50,
        help='Commit database transaction every N inserts (default: 50)'
    )
    parser.add_argument(
        '--understand-mismatch',
        action='store_true',
        help='Analyze non-matched threads to understand mismatch patterns'
    )
    parser.add_argument(
        '--mismatch-out',
        type=str,
        default='./out/chatbot_mismatch_report.json',
        help='Output path for mismatch analysis JSON report (default: ./out/chatbot_mismatch_report.json)'
    )
    parser.add_argument(
        '--mismatch-samples',
        type=int,
        default=10,
        help='Number of thread IDs to sample per bucket for output (default: 10)'
    )
    parser.add_argument(
        '--understand-nonbot',
        action='store_true',
        help='Print random sample of non-bot conversations (matchedCount=0) with previews'
    )
    parser.add_argument(
        '--nonbot-sample-fraction',
        type=float,
        default=0.5,
        help='Fraction of nonbot threads to sample (default: 0.5)'
    )
    parser.add_argument(
        '--nonbot-max-lines',
        type=int,
        default=20,
        help='Maximum message lines to print per thread preview (default: 20)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducible sampling (default: 42)'
    )
    parser.add_argument(
        '--nonbot-only-channel-account-id',
        type=str,
        help='Optional: restrict nonbot sample to this channelAccountId'
    )
    parser.add_argument(
        '--get10-nonbot',
        action='store_true',
        help='Print 10 random non-bot threads (matchedCount==0) with short previews'
    )
    parser.add_argument(
        '--nonbot-n',
        type=int,
        default=10,
        help='Number of non-bot threads to sample (default: 10)'
    )
    parser.add_argument(
        '--nonbot-out',
        type=str,
        default='./out/nonbot_10_sample.json',
        help='Output path for non-bot sample JSON report (default: ./out/nonbot_10_sample.json)'
    )
    parser.add_argument(
        '--get-one',
        action='store_true',
        help='Print one raw API payload bundle per stage (1-5) from SQLite database'
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default='./out/chatbot_conversations.sqlite',
        help='SQLite database path for --get-one (default: ./out/chatbot_conversations.sqlite)'
    )
    parser.add_argument(
        '--out-dir',
        type=str,
        default='./out',
        help='Output directory for --get-one --save (default: ./out)'
    )
    parser.add_argument(
        '--pretty',
        action='store_true',
        help='Pretty-print JSON bundles (default: compact)'
    )
    parser.add_argument(
        '--save',
        action='store_true',
        help='Save bundles to files (get_one_stage_N.json)'
    )
    parser.add_argument(
        '--no-truncate',
        action='store_true',
        help='Always print full bundle even if large (default: truncate >5MB)'
    )
    
    args = parser.parse_args()
    
    # Handle --get-one mode (early exit, no API calls)
    if args.get_one:
        exit_code = get_one_per_stage(
            db_path=args.db_path,
            out_dir=args.out_dir,
            seed=args.seed,
            pretty=args.pretty,
            save=args.save,
            no_truncate=args.no_truncate
        )
        sys.exit(exit_code)
    
    # Get token
    token, source = get_old_access_token()
    if source == 'env_file':
        print("Using OLD_ACCESS_TOKEN from .env file", file=sys.stderr)
    else:
        print("Using OLD_ACCESS_TOKEN from environment", file=sys.stderr)
    
    # Print filters being applied
    filters = {}
    if args.inbox_id:
        filters['inboxId'] = args.inbox_id
    if args.channel_account_id:
        filters['channelAccountId'] = args.channel_account_id
    if args.since:
        filters['since'] = args.since
    if args.until:
        filters['until'] = args.until
    
    if filters:
        print(f"Filters applied: {filters}", file=sys.stderr)
    
    # Initialize database if --write-chatbot or --write-chatbot-all is enabled
    db_conn = None
    stored_count = 0
    started_count = 0  # Count of threads with stage >= 1
    failed_thread_ids = []
    if args.write_chatbot or args.write_chatbot_all:
        db_conn = init_db(args.db)
        print(f"Database initialized: {args.db}", file=sys.stderr)
    
    # Counters
    scanned_total = 0
    scanned_live = 0
    scanned_archived = 0
    matched = 0
    completed_count = 0  # Count of threads with chatbot_stage == MAX_STAGE (5/5)
    matched_with_contact = 0
    matched_without_contact = 0
    matched_live = 0
    matched_archived = 0
    matched_thread_ids = []
    near_misses = []  # threads with matched_count == 4 (near miss: 4/5 prompts matched)
    
    # Determine if we need mismatch/nonbot data
    want_classification = args.understand_mismatch or args.understand_nonbot or args.get10_nonbot
    
    # Mismatch analysis tracking (lightweight per-thread data)
    mismatch_data = []  # List of dicts with thread classification data (for --understand-mismatch)
    per_thread_results = []  # List of dicts with thread results for ALL scanned threads (for --understand-nonbot)
    
    # Determine archived mode
    archived_modes = []
    if args.archived_mode in ('live', 'both'):
        archived_modes.append(False)
    if args.archived_mode in ('archived', 'both'):
        archived_modes.append(True)
    
    # Scan threads (two passes: archived=false and archived=true)
    print(f"Scanning up to {args.scan_limit} threads using stall-safe enumeration...", file=sys.stderr)
    
    for archived in archived_modes:
        mode_name = "archived" if archived else "live"
        print(f"\nScanning {mode_name} threads...", file=sys.stderr)
        
        for thread in iter_threads_all(
            archived=archived,
            inbox_id=args.inbox_id,
            channel_account_id=args.channel_account_id,
            since=args.since,
            max_pages=args.max_pages,
            scan_limit=args.scan_limit,
            token=token
        ):
            if scanned_total >= args.scan_limit:
                break
            
            thread_id = thread.get('id', 'unknown')
            associated_contact_id = thread.get('associatedContactId')
            
            scanned_total += 1
            if archived:
                scanned_archived += 1
            else:
                scanned_live += 1
            
            # Progress reporting
            if scanned_total % args.progress_every == 0:
                rate = (matched / scanned_total * 100) if scanned_total > 0 else 0
                progress_line = f"  scanned={scanned_total}, completed={matched}, started={started_count}, rate={rate:.2f}%"
                if args.write_chatbot or args.write_chatbot_all:
                    progress_line += f", stored={stored_count}, db_path={args.db}"
                print(progress_line, file=sys.stderr)
            
            try:
                # Efficient message fetching: only fetch what we need
                all_messages = get_messages_efficiently(thread_id, messages_limit=args.messages_limit, token=token)
                
                # Match required prompts (even if no messages, matchedCount will be 0)
                if all_messages:
                    is_matched, matched_count, missing, match_details = match_required_prompts(all_messages, messages_limit=args.messages_limit)
                else:
                    # No messages means no prompts matched
                    is_matched = False
                    matched_count = 0
                    missing = REQUIRED_PROMPTS.copy()
                    match_details = []
                
                # Compute chatbot stage for storage (needed for both flags and started_count tracking)
                chatbot_stage = 0
                if all_messages:
                    chatbot_stage, stage_debug_info = compute_chatbot_stage(all_messages)
                # No messages means stage 0 (already set)
                
                # Track started threads (stage >= 1) - must be before continue
                if chatbot_stage >= 1:
                    started_count += 1
                
                # Track completed threads (stage == MAX_STAGE)
                if chatbot_stage == MAX_STAGE:
                    completed_count += 1
                
                # Populate per_thread_results for ALL scanned threads when any classification flag is enabled
                if want_classification:
                    # Fetch thread details if needed (for originalChannelId, status, createdAt)
                    thread_details_for_classification = None
                    if args.get10_nonbot or args.understand_nonbot:
                        thread_details_for_classification = get_thread_details(thread_id, token=token)
                        time.sleep(DEFAULT_RATE_LIMIT_DELAY)
                    
                    # Check if chatbot flow started (stage >= 1 means first prompt found AND human reply)
                    is_started = (chatbot_stage >= 1)
                    
                    per_thread_entry = {
                        'threadId': thread_id,
                        'matchedCount': matched_count,
                        'started': is_started,
                        'inboxId': thread.get('inboxId'),
                        'originalChannelAccountId': thread.get('originalChannelAccountId') or thread.get('channelAccountId'),
                        'originalChannelId': (thread_details_for_classification.get('originalChannelId') or thread_details_for_classification.get('channelId')) if thread_details_for_classification else None,
                        'status': thread_details_for_classification.get('status') if thread_details_for_classification else None,
                        'createdAt': thread.get('createdAt'),
                        'latestMessageTimestamp': thread.get('latestMessageTimestamp'),
                        'associatedContactId': associated_contact_id
                    }
                    per_thread_results.append(per_thread_entry)
                
                # Only process matched threads for counting/storage if messages exist
                # (Stage has already been computed above for started_count tracking)
                # Threads with no messages will have stage=0 and won't be stored anyway
                if not all_messages:
                    continue
                
                # Track mismatch analysis data if enabled (for --understand-mismatch)
                if args.understand_mismatch:
                    # Check if chatbot flow started (stage >= 1 means first prompt found AND human reply)
                    is_started = (chatbot_stage >= 1)
                    evidence_first_prompt = None
                    if is_started and stage_debug_info:
                        # Get evidence from stage debug info
                        matched_stages = stage_debug_info.get('matchedStages', [])
                        first_stage = next((s for s in matched_stages if s.get('stage') == 1), None)
                        if first_stage:
                            evidence_first_prompt = {
                                'messageId': first_stage.get('promptMessageId', ''),
                                'createdAt': first_stage.get('promptCreatedAt', ''),
                                'text': ''  # Can be populated if needed
                            }
                    
                    # Store lightweight classification data
                    thread_classification = {
                        'threadId': thread_id,
                        'matchedCount': matched_count,
                        'chatbotStage': chatbot_stage,  # Store stage for bucket derivation
                        'isCompleted': is_matched,
                        'isStarted': is_started,
                        'missingPrompts': missing,
                        'evidenceFirstPrompt': evidence_first_prompt,
                        'inboxId': thread.get('inboxId'),
                        'channelAccountId': thread.get('originalChannelAccountId') or thread.get('channelAccountId'),
                        'associatedContactId': associated_contact_id,
                        'createdAt': thread.get('createdAt'),
                        'latestMessageTimestamp': thread.get('latestMessageTimestamp')
                    }
                    mismatch_data.append(thread_classification)
                
                if is_matched:
                    matched += 1
                    if archived:
                        matched_archived += 1
                    else:
                        matched_live += 1
                    
                    # Track with/without contact
                    if associated_contact_id:
                        matched_with_contact += 1
                    else:
                        matched_without_contact += 1
                    
                    if len(matched_thread_ids) < 20:
                        matched_thread_ids.append(thread_id)
                
                # Store in database if flags are enabled
                should_store = False
                if args.write_chatbot and chatbot_stage == MAX_STAGE:
                    should_store = True
                elif args.write_chatbot_all and chatbot_stage >= 1:
                    should_store = True
                
                if should_store and db_conn:
                    try:
                        # Fetch thread details
                        thread_details = get_thread_details(thread_id, token=token)
                        if not thread_details or not thread_details.get('id'):
                            print(f"Warning: Failed to fetch thread details for {thread_id}", file=sys.stderr)
                            failed_thread_ids.append(thread_id)
                        else:
                            # Fetch all messages for storage
                            messages_agg = get_messages_all_for_storage(thread_id, token=token)
                            
                            # Build prompt match object
                            prompt_match_obj = {
                                'requiredPrompts': CHATBOT_PROMPTS_ORDERED,
                                'matches': match_details if all_messages else []
                            }
                            
                            # Store in database with stage
                            upsert_chatbot_thread(db_conn, thread_details, messages_agg, prompt_match_obj, chatbot_stage)
                            stored_count += 1
                            
                            # Commit periodically
                            if stored_count % args.commit_every == 0:
                                db_conn.commit()
                    except Exception as e:
                        print(f"Warning: Failed to store thread {thread_id} in database: {e}", file=sys.stderr)
                        failed_thread_ids.append(thread_id)
                
                # Track near misses (4/5 prompts matched)
                if matched_count == (MAX_STAGE - 1) and len(near_misses) < 20:
                    near_misses.append({
                        'threadId': thread_id,
                        'matchedCount': matched_count,
                        'missing': missing
                    })
            
            except Exception as e:
                print(f"Warning: Error processing thread {thread_id}: {e}", file=sys.stderr)
                continue
    
    # Final commit if database was used
    if (args.write_chatbot or args.write_chatbot_all) and db_conn:
        db_conn.commit()
        db_conn.close()
        print(f"\nDatabase closed: {args.db}", file=sys.stderr)
    
    # Save failed thread IDs if any
    if (args.write_chatbot or args.write_chatbot_all) and failed_thread_ids:
        failed_path = os.path.join('out', 'failed_threads.json')
        os.makedirs('out', exist_ok=True)
        try:
            with open(failed_path, 'w', encoding='utf-8') as f:
                json.dump({'failedThreadIds': failed_thread_ids}, f, indent=2, ensure_ascii=False)
            print(f"Failed thread IDs saved to: {failed_path}", file=sys.stderr)
        except Exception as e:
            print(f"Warning: Failed to save failed thread IDs: {e}", file=sys.stderr)
    
    # Print summary
    percentage = (matched / scanned_total * 100) if scanned_total > 0 else 0
    
    print("\n" + "=" * 60, file=sys.stderr)
    print("SUMMARY", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Total threads scanned: {scanned_total}", file=sys.stderr)
    if args.archived_mode == 'both':
        print(f"  - Live threads: {scanned_live}", file=sys.stderr)
        print(f"  - Archived threads: {scanned_archived}", file=sys.stderr)
    # For --write-chatbot-all, use stage-based counts; otherwise use matched counts
    if args.write_chatbot_all:
        print(f"Completed chatbot threads ({MAX_STAGE}/{MAX_STAGE}): {completed_count}", file=sys.stderr)
        print(f"Started chatbot threads (>={1}/{MAX_STAGE}): {started_count}", file=sys.stderr)
    else:
        print(f"Total chatbot threads found: {matched}", file=sys.stderr)
    
    if args.archived_mode == 'both':
        print(f"  - From live: {matched_live}", file=sys.stderr)
        print(f"  - From archived: {matched_archived}", file=sys.stderr)
    print(f"Chatbot threads with associatedContactId: {matched_with_contact}", file=sys.stderr)
    print(f"Chatbot threads missing associatedContactId: {matched_without_contact}", file=sys.stderr)
    print(f"Percentage: {percentage:.2f}%", file=sys.stderr)
    
    print("\nNote conversion estimate:", file=sys.stderr)
    print(f"  - Notes to create (minimum): {matched}", file=sys.stderr)
    print(f"  - Contacts to create/resolve (needs inference): {matched_without_contact}", file=sys.stderr)
    
    if args.write_chatbot or args.write_chatbot_all:
        print(f"\nDatabase storage:", file=sys.stderr)
        print(f"  - Total started chatbot threads (stage>={1}): {started_count}", file=sys.stderr)
        if args.write_chatbot:
            print(f"  - Stored (completed only, stage={MAX_STAGE}): {stored_count}", file=sys.stderr)
        elif args.write_chatbot_all:
            print(f"  - Stored (all started, stage>={1}): {stored_count}", file=sys.stderr)
        print(f"  - DB file path: {args.db}", file=sys.stderr)
        if failed_thread_ids:
            print(f"  - Failed to store: {len(failed_thread_ids)} threads", file=sys.stderr)
    
    if filters:
        print(f"\nFilters applied: {filters}", file=sys.stderr)
    
    # Print samples
    if matched_thread_ids:
        print(f"\nSample matched thread IDs (first {min(5, len(matched_thread_ids))}):", file=sys.stderr)
        for tid in matched_thread_ids[:5]:
            print(f"  {tid}", file=sys.stderr)
    
    if near_misses:
        print(f"\nNear misses ({MAX_STAGE - 1}/{MAX_STAGE} prompts matched, first {min(5, len(near_misses))}):", file=sys.stderr)
        for nm in near_misses[:5]:
            print(f"  {nm['threadId']}: missing {nm['missing']}", file=sys.stderr)
    
    # Mismatch analysis
    if args.understand_mismatch:
        analyze_mismatches(mismatch_data, args.mismatch_out, args.mismatch_samples, scanned_total, completed_count)
    
    # Nonbot analysis
    if args.understand_nonbot:
        analyze_nonbot(per_thread_results, args.nonbot_sample_fraction, args.nonbot_max_lines,
                      args.seed, token, args.mismatch_out)
    
    # Get10 nonbot sample
    if args.get10_nonbot:
        get10_nonbot(per_thread_results, args.nonbot_n, args.nonbot_max_lines,
                    args.seed, token, args.nonbot_out)
    
    # Write JSON report if requested
    if args.json_out:
        report = {
            'scanned': scanned_total,
            'scannedLive': scanned_live,
            'scannedArchived': scanned_archived,
            'matched': matched,
            'matchedLive': matched_live,
            'matchedArchived': matched_archived,
            'matchedWithContact': matched_with_contact,
            'matchedWithoutContact': matched_without_contact,
            'archivedMode': args.archived_mode,
            'filters': filters,
            'matchedThreadIdsSample': matched_thread_ids[:20],
            'nearMisses': near_misses[:20]
        }
        
        os.makedirs(os.path.dirname(args.json_out) if os.path.dirname(args.json_out) else '.', exist_ok=True)
        with open(args.json_out, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"\nJSON report written to: {args.json_out}", file=sys.stderr)


if __name__ == '__main__':
    main()
