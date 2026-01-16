#!/usr/bin/env python3
"""
Count chatbot conversations in HubSpot OLD portal.

Usage examples:
  python chatbot_count.py --inbox-id 147959634 --channel-account-id 240442427 --fast
  python chatbot_count.py --scan-limit 20000 --progress-every 500 --json-out out/chatbot_count.json
  python chatbot_count.py --since 2024-01-01T00:00:00Z --until 2024-12-31T23:59:59Z --fast

Definition: A thread counts as a "chatbot conversation" if it contains ALL required bot prompts
as an ordered subsequence after normalization.
"""

import argparse
import html
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

BASE_URL = "https://api.hubapi.com"

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
        next_after_encoded = response.get('paging', {}).get('next', {}).get('after')
        next_after_raw = unquote(next_after_encoded) if next_after_encoded else None
        
        # Extract IDs for page signature
        ids = [t.get('id') for t in results if t.get('id') is not None]
        first_id = ids[0] if ids else None
        last_id = ids[-1] if ids else None
        
        # Build page signature: (lmts_after or None, after, first_id, last_id, len(ids), next_after_raw)
        # Use None if lmts_after is not set yet
        page_sig = (lmts_after if lmts_after else None, after, first_id, last_id, len(ids), next_after_raw)
        
        # Natural end condition
        if len(results) == 0:
            break
        
        # Stuck page detection: same page signature seen again
        if page_sig in seen_page_sigs:
            # STALL ESCAPE: advance timestamp window
            if last_seen_lmts is None:
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
            
            if yielded_count > scan_limit:
                return
            
            yield thread
        
        # Normal paging advance
        if next_after_raw is not None and next_after_raw != after:
            after = next_after_raw
            pages += 1
            continue
        
        # STALL ESCAPE: cursor didn't advance, advance timestamp
        if next_after_raw is None or next_after_raw == after:
            if last_seen_lmts is None:
                break  # Nothing to advance
            
            # Advance latestMessageTimestampAfter by 1ms
            lmts_after = advance_timestamp_ms(last_seen_lmts, ms=1)
            after = None
            seen_page_sigs.clear()  # Clear to allow progress with new timestamp
            # Continue loop (don't increment pages since we're doing stall escape)


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


def match_required_prompts(messages: List[Dict[str, Any]], messages_limit: int = 60) -> Tuple[bool, int, List[str]]:
    """
    Match REQUIRED_PROMPTS as ordered subsequence in messages.
    
    Args:
        messages: List of message dicts (should be filtered to MESSAGE/WELCOME_MESSAGE and sorted by createdAt)
        messages_limit: Only check first N messages
    
    Returns:
        (matched: bool, matched_count: int, missing: List[str])
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
    
    # Build normalized bot lines from bot prompt candidates
    bot_lines = []
    for msg in filtered:
        if is_bot_prompt_candidate(msg):
            text = message_text(msg)
            if text:
                normalized = normalize_for_match(text)
                if normalized:
                    bot_lines.append(normalized)
    
    # Match prompts in order
    matched_count = 0
    last_match_pos = -1
    missing = []
    
    for prompt in REQUIRED_PROMPTS:
        found = False
        
        # Search from after last match position
        search_start = last_match_pos + 1
        for i in range(search_start, len(bot_lines)):
            line = bot_lines[i]
            if prompt in line:
                matched_count += 1
                last_match_pos = i
                found = True
                break
        
        if not found:
            missing.append(prompt)
    
    matched = matched_count == len(REQUIRED_PROMPTS)
    return (matched, matched_count, missing)


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
    
    args = parser.parse_args()
    
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
    
    # Counters
    scanned_total = 0
    scanned_live = 0
    scanned_archived = 0
    matched = 0
    matched_with_contact = 0
    matched_without_contact = 0
    matched_live = 0
    matched_archived = 0
    matched_thread_ids = []
    near_misses = []  # threads with matched_count == 5
    
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
                print(f"  scanned={scanned_total}, matched={matched}, rate={rate:.2f}%", file=sys.stderr)
            
            try:
                # Efficient message fetching: only fetch what we need
                all_messages = get_messages_efficiently(thread_id, messages_limit=args.messages_limit, token=token)
                
                if not all_messages:
                    continue
                
                # Match required prompts
                is_matched, matched_count, missing = match_required_prompts(all_messages, messages_limit=args.messages_limit)
                
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
                
                # Track near misses (5/6 prompts matched)
                if matched_count == 5 and len(near_misses) < 20:
                    near_misses.append({
                        'threadId': thread_id,
                        'matchedCount': matched_count,
                        'missing': missing
                    })
            
            except Exception as e:
                print(f"Warning: Error processing thread {thread_id}: {e}", file=sys.stderr)
                continue
    
    # Print summary
    percentage = (matched / scanned_total * 100) if scanned_total > 0 else 0
    
    print("\n" + "=" * 60, file=sys.stderr)
    print("SUMMARY", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Total threads scanned: {scanned_total}", file=sys.stderr)
    if args.archived_mode == 'both':
        print(f"  - Live threads: {scanned_live}", file=sys.stderr)
        print(f"  - Archived threads: {scanned_archived}", file=sys.stderr)
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
    
    if filters:
        print(f"\nFilters applied: {filters}", file=sys.stderr)
    
    # Print samples
    if matched_thread_ids:
        print(f"\nSample matched thread IDs (first {min(5, len(matched_thread_ids))}):", file=sys.stderr)
        for tid in matched_thread_ids[:5]:
            print(f"  {tid}", file=sys.stderr)
    
    if near_misses:
        print(f"\nNear misses (5/6 prompts matched, first {min(5, len(near_misses))}):", file=sys.stderr)
        for nm in near_misses[:5]:
            print(f"  {nm['threadId']}: missing {nm['missing']}", file=sys.stderr)
    
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
