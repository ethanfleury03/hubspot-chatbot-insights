#!/usr/bin/env python3
"""
Proof-of-concept script to fetch HubSpot conversation threads and messages.
"""

import os
import sys
import json
import time
import argparse
import re
from pathlib import Path
from datetime import datetime, timezone

import requests
import pandas as pd
from dotenv import load_dotenv


def load_hubspot_token():
    """Load HubSpot token from environment variables in priority order."""
    load_dotenv()
    
    token = None
    # Priority order: OLD_ACCESS_TOKEN first, then fallback to others
    for env_var in ['OLD_ACCESS_TOKEN', 'HUBSPOT_TOKEN', 'ACCESS_TOKEN', 'PRIVATE_APP_TOKEN']:
        token = os.getenv(env_var)
        if token:
            print(f"Found token from {env_var}")
            return token
    
    return None


def handle_api_error(response, operation, continue_on_error=False):
    """Handle API errors with appropriate messages.
    
    Args:
        response: The HTTP response object
        operation: Description of the operation being performed
        continue_on_error: If True, return False instead of exiting on non-fatal errors
    
    Returns:
        True if retry should happen (429), False otherwise
    """
    status_code = response.status_code
    
    if status_code == 401:
        print(f"ERROR: Authentication failed (401). Your token is invalid.")
        sys.exit(1)
    elif status_code == 403:
        print(f"ERROR: Forbidden (403). Missing required scopes (likely 'conversations.read').")
        sys.exit(1)
    elif status_code == 429:
        retry_after = response.headers.get('Retry-After', '60')
        try:
            wait_seconds = int(retry_after)
        except ValueError:
            wait_seconds = 60
        
        print(f"Rate limited (429). Waiting {wait_seconds} seconds before retry...")
        time.sleep(wait_seconds)
        return True  # Indicate retry should happen
    else:
        print(f"ERROR: {operation} failed with status {status_code}")
        print(f"Response: {response.text}")
        if continue_on_error:
            return False  # Don't exit, let caller handle it
        sys.exit(1)
    
    return False


def fetch_inboxes(token):
    """Fetch inboxes from HubSpot API."""
    url = "https://api.hubapi.com/conversations/v3/conversations/inboxes"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    max_retries = 2
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        
        should_retry = handle_api_error(response, "Fetching inboxes")
        if not should_retry:
            break
    
    sys.exit(1)


def fetch_threads(token, limit=1, archived=None, inbox_id=None, sort=None, latest_message_timestamp_after=None, after=None):
    """Fetch conversation threads from HubSpot API."""
    url = "https://api.hubapi.com/conversations/v3/conversations/threads"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params = {"limit": limit}
    
    if archived is not None:
        params["archived"] = archived
    if inbox_id is not None:
        params["inboxId"] = inbox_id
    
    # Add sorting parameters
    if sort == "latestMessageTimestamp":
        params["sort"] = "latestMessageTimestamp"
        if latest_message_timestamp_after:
            params["latestMessageTimestampAfter"] = latest_message_timestamp_after
        # Note: latestMessageTimestampAfter is required when sorting by latestMessageTimestamp
        # This is validated before calling this function
    
    # Add pagination parameter
    if after:
        params["after"] = after
    
    # Build query string for printing
    query_parts = [f"{k}={v}" for k, v in sorted(params.items())]
    query_string = "&".join(query_parts)
    full_url = f"{url}?{query_string}"
    print(f"  Query: {full_url}")
    
    max_retries = 2
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        
        should_retry = handle_api_error(response, "Fetching threads")
        if not should_retry:
            break
    
    sys.exit(1)


def fetch_messages(token, thread_id, use_cache=True):
    """Fetch messages for a specific thread from HubSpot API, with optional caching."""
    cache_path = Path(f"data/raw/messages/{thread_id}.json")
    
    # Check cache first
    if use_cache and cache_path.exists():
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  Warning: Failed to read cache for thread {thread_id}: {e}")
            # Fall through to fetch from API
    
    # Fetch from API
    url = f"https://api.hubapi.com/conversations/v3/conversations/threads/{thread_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    max_retries = 2
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            messages_data = response.json()
            
            # Save to cache
            if use_cache:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        json.dump(messages_data, f, indent=2, ensure_ascii=False)
                except IOError as e:
                    print(f"  Warning: Failed to save cache for thread {thread_id}: {e}")
            
            return messages_data
        
        should_retry = handle_api_error(response, f"Fetching messages for thread {thread_id}", continue_on_error=True)
        if not should_retry:
            break
    
    # Return None if fetch failed (don't exit, let caller handle it)
    return None


def fetch_me(token):
    """Fetch portal/user information from HubSpot API."""
    url = "https://api.hubapi.com/integrations/v1/me"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    max_retries = 2
    for attempt in range(max_retries):
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        
        should_retry = handle_api_error(response, "Fetching portal info")
        if not should_retry:
            break
    
    sys.exit(1)


def format_timestamp(timestamp_value):
    """Convert timestamp (millis int or ISO-8601 string) to readable datetime string."""
    try:
        # Handle ISO-8601 string format
        if isinstance(timestamp_value, str):
            # Try parsing ISO-8601 format
            dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        # Handle milliseconds timestamp
        elif isinstance(timestamp_value, (int, float)):
            dt = datetime.fromtimestamp(timestamp_value / 1000)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        pass
    
    return str(timestamp_value)


def parse_timestamp_for_sort(timestamp_value):
    """Parse timestamp for sorting (handles both millis and ISO-8601)."""
    try:
        if isinstance(timestamp_value, str):
            dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
            return dt.timestamp()
        elif isinstance(timestamp_value, (int, float)):
            return timestamp_value / 1000
    except (ValueError, TypeError, OSError):
        pass
    return 0


def print_transcript(messages_data, thread_id=None, show_thread_id=False):
    """Print a readable transcript of messages."""
    if not messages_data or 'results' not in messages_data:
        print("No messages found in response.")
        return
    
    messages = messages_data['results']
    
    # Sort messages by createdAt timestamp (handles both millis and ISO strings)
    sorted_messages = sorted(messages, key=lambda m: parse_timestamp_for_sort(m.get('createdAt', 0)))
    
    print("\n" + "="*80)
    if thread_id and show_thread_id:
        print(f"TRANSCRIPT - Thread ID: {thread_id}")
    else:
        print("TRANSCRIPT")
    print("="*80)
    
    for msg in sorted_messages:
        created_at = msg.get('createdAt', 0)
        direction = msg.get('direction')
        sender = msg.get('sender', {})
        msg_type = msg.get('type', '')
        
        # Determine sender label
        if direction == 'INBOUND':
            sender_label = sender.get('name', sender.get('email', 'Customer'))
        elif direction == 'OUTBOUND':
            sender_label = sender.get('name', sender.get('email', 'Agent'))
        elif direction:
            sender_label = f"{direction} ({sender.get('name', sender.get('email', 'Unknown'))})"
        else:
            # Fallback when direction is missing
            senders = msg.get('senders', [])
            if senders and len(senders) > 0:
                first_sender = senders[0] if isinstance(senders[0], dict) else {}
                sender_label = first_sender.get('name', first_sender.get('email', 'Unknown'))
            else:
                sender_label = msg_type if msg_type else 'Unknown'
        
        # Get message text
        text = msg.get('text', '')
        if not text:
            text = f"<non-text message>"
            if msg_type:
                text += f" (type: {msg_type})"
        
        timestamp_str = format_timestamp(created_at)
        thread_prefix = f"[Thread: {thread_id}] " if (thread_id and show_thread_id) else ""
        print(f"\n{thread_prefix}[{timestamp_str}] <{sender_label}>: {text}")
    
    print("\n" + "="*80)


def save_json(data, filepath):
    """Save JSON data to file, creating directories if needed."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved raw JSON to {filepath}")


def parse_timestamp_to_datetime(timestamp_value):
    """Convert timestamp (millis int or ISO-8601 string) to pandas datetime."""
    try:
        if isinstance(timestamp_value, str):
            # Handle ISO-8601 string format
            dt = datetime.fromisoformat(timestamp_value.replace('Z', '+00:00'))
            return pd.Timestamp(dt)
        elif isinstance(timestamp_value, (int, float)):
            # Handle milliseconds timestamp
            dt = datetime.fromtimestamp(timestamp_value / 1000)
            return pd.Timestamp(dt)
    except (ValueError, TypeError, OSError):
        pass
    
    return pd.NaT


def build_transcript_dataframe(messages_data, thread_id):
    """Build a pandas DataFrame from messages data."""
    if not messages_data or 'results' not in messages_data:
        return pd.DataFrame(columns=['thread_id', 'created_at', 'direction', 'sender', 'text', 'message_type'])
    
    messages = messages_data['results']
    
    # Sort messages by createdAt timestamp
    sorted_messages = sorted(messages, key=lambda m: parse_timestamp_for_sort(m.get('createdAt', 0)))
    
    rows = []
    for msg in sorted_messages:
        created_at = msg.get('createdAt', 0)
        direction = msg.get('direction', '')
        msg_type = msg.get('type', '')
        
        # Determine direction (best-effort)
        if direction:
            direction_val = direction
        elif msg_type:
            direction_val = msg_type
        else:
            direction_val = 'unknown'
        
        # Determine sender (best-effort)
        sender_val = ''
        sender = msg.get('sender', {})
        if sender:
            sender_val = sender.get('name', sender.get('email', ''))
        
        # If no sender from 'sender' field, try 'senders' array
        if not sender_val:
            senders = msg.get('senders', [])
            if senders and len(senders) > 0:
                first_sender = senders[0] if isinstance(senders[0], dict) else {}
                sender_val = first_sender.get('name', first_sender.get('email', ''))
        
        # Get text
        text = msg.get('text', '')
        if not text:
            text = '<non-text message>'
        
        # Parse timestamp to pandas datetime
        created_at_dt = parse_timestamp_to_datetime(created_at)
        
        rows.append({
            'thread_id': thread_id,
            'created_at': created_at_dt,
            'direction': direction_val,
            'sender': sender_val,
            'text': text,
            'message_type': msg_type if msg_type else ''
        })
    
    df = pd.DataFrame(rows)
    return df


def normalize_text(text):
    """Normalize text for comparison: strip, collapse whitespace, lowercase, remove trailing punctuation and quotes."""
    if not text or not isinstance(text, str):
        return ''
    
    # Convert to string, strip whitespace
    normalized = str(text).strip()
    
    # Collapse all internal whitespace (regex \s+) into single spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Lowercase
    normalized = normalized.lower()
    
    # Remove trailing punctuation: . , ! ? : ; and quotes (single and double)
    normalized = re.sub(r'[.,!?:;\"\']+$', '', normalized)
    
    # Trim again after removing punctuation
    normalized = normalized.strip()
    
    return normalized


def clean_text_for_analysis(text):
    """Clean text for analysis: strip and collapse whitespace, keep case as-is."""
    if not text or not isinstance(text, str):
        return ''
    
    # Convert to string, strip whitespace
    cleaned = str(text).strip()
    
    # Collapse all internal whitespace (regex \s+) into single spaces
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned


def normalize_text_for_analysis(text):
    """Normalize text for analysis: lowercase, remove punctuation, collapse whitespace."""
    if not text or not isinstance(text, str):
        return ''
    
    # Convert to string, strip whitespace
    normalized = str(text).strip()
    
    # Lowercase
    normalized = normalized.lower()
    
    # Remove punctuation (keep alphanumeric and spaces)
    normalized = re.sub(r'[^\w\s]', '', normalized)
    
    # Collapse all internal whitespace (regex \s+) into single spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Trim again
    normalized = normalized.strip()
    
    return normalized


def count_words(text):
    """Count words in text (split by whitespace)."""
    if not text or not isinstance(text, str):
        return 0
    return len(text.split())


def matches_question(text, question_text):
    """Check if message text matches the question using normalization."""
    if not text or not isinstance(text, str):
        return False
    return normalize_text(text) == normalize_text(question_text)


def is_system_message(text):
    """Check if text starts with the HubSpot onboarding/system block."""
    if not text or not isinstance(text, str):
        return False
    
    # Check if text starts with the specific onboarding message (case-insensitive)
    text_lower = text.lower().strip()
    return text_lower.startswith('you connected chat to your inbox')


def is_low_signal_answer(text):
    """Check if answer is a low-signal greeting like 'hi' or 'hello'."""
    if not text or not isinstance(text, str):
        return False
    
    # Clean and normalize for comparison
    text_cleaned = clean_text_for_analysis(text).lower().strip()
    
    # Check for common low-signal greetings
    low_signal_greetings = ['hi', 'hello', 'hey', 'heya']
    return text_cleaned in low_signal_greetings


def extract_answers(df_clean, question_text, debug_thread_id=None, debug_question_matches=None):
    """Extract visitor answers to the specified question from cleaned messages DataFrame.
    
    Returns:
        tuple: (df_answers_found, df_answers_missing, matched_questions) where:
            - df_answers_found: DataFrame with successful extractions
            - df_answers_missing: DataFrame with failed extractions and reasons
            - matched_questions: List of matched question rows for debugging
    """
    answers_found_rows = []
    answers_missing_rows = []
    matched_questions = []  # For debug output
    
    # Ensure created_at is datetime with UTC
    if 'created_at' in df_clean.columns:
        df_clean['created_at'] = pd.to_datetime(df_clean['created_at'], utc=True)
    
    # Sort by thread_id and created_at
    df_clean = df_clean.sort_values(['thread_id', 'created_at']).reset_index(drop=True)
    
    # Group by thread_id
    for thread_id, thread_df in df_clean.groupby('thread_id'):
        thread_df = thread_df.sort_values('created_at').reset_index(drop=True)
        thread_start_time = thread_df.iloc[0]['created_at'] if len(thread_df) > 0 else pd.NaT
        
        question_created_at = None
        answer_created_at = None
        answer_text = None
        question_row = None  # Store full question row for debug
        
        # Find first row that matches the question
        # Match based on normalized text and message_type in {"WELCOME_MESSAGE","MESSAGE"}
        question_found = False
        for idx, row in thread_df.iterrows():
            msg_type = str(row.get('message_type', '')).strip()
            text = row.get('text', '')
            
            if (msg_type in ['WELCOME_MESSAGE', 'MESSAGE'] and
                text and 
                text != '<non-text message>' and
                matches_question(text, question_text)):
                question_created_at = row['created_at']
                question_row = row
                question_found = True
                
                # Store for debug output
                if debug_question_matches is not None and len(matched_questions) < debug_question_matches:
                    matched_questions.append({
                        'thread_id': thread_id,
                        'created_at': question_created_at,
                        'direction': str(row.get('direction', '')).strip(),
                        'message_type': msg_type,
                        'text': text
                    })
                break
        
        # If question found, look for answer
        if question_found:
            # Find the question row index
            question_idx = None
            for idx, row in thread_df.iterrows():
                if row['created_at'] == question_created_at:
                    question_idx = idx
                    break
            
            if question_idx is not None:
                # Look for the first subsequent INCOMING message with non-empty text
                for answer_idx in range(question_idx + 1, len(thread_df)):
                    answer_row = thread_df.iloc[answer_idx]
                    answer_direction = str(answer_row.get('direction', '')).strip()
                    answer_msg_type = str(answer_row.get('message_type', '')).strip()
                    answer_text_val = answer_row.get('text', '')
                    
                    # Check if it's an incoming message
                    if (answer_direction == 'INCOMING' and 
                        answer_msg_type == 'MESSAGE' and
                        answer_text_val and 
                        isinstance(answer_text_val, str) and
                        answer_text_val.strip() and
                        answer_text_val != '<non-text message>' and
                        not is_system_message(answer_text_val)):
                        answer_created_at = answer_row['created_at']
                        answer_text = answer_text_val.strip()
                        break
            
            # If answer found, check if it's a low-signal answer
            if answer_text:
                # Clean the answer text (strip + collapse whitespace, preserve case)
                answer_text_cleaned = clean_text_for_analysis(answer_text)
                
                # Filter out low-signal greetings like "hi" or "hello"
                if is_low_signal_answer(answer_text_cleaned):
                    # Treat low-signal answers as if no answer was found
                    answers_missing_rows.append({
                        'thread_id': thread_id,
                        'status': 'answer_not_found',
                        'detail': 'question found but answer is low-signal greeting (hi/hello)',
                        'thread_start_time': thread_start_time
                    })
                else:
                    # Valid answer found
                    answer_text_norm = normalize_text_for_analysis(answer_text_cleaned)
                    answers_found_rows.append({
                        'thread_id': thread_id,
                        'question_created_at': question_created_at,
                        'answer_created_at': answer_created_at,
                        'answer_text': answer_text_cleaned,  # Store cleaned version
                        'answer_text_norm': answer_text_norm,  # Normalized version for analysis
                        'answer_len': len(answer_text_cleaned),
                        'answer_word_count': count_words(answer_text_cleaned)
                    })
            else:
                # Question found but no answer
                answers_missing_rows.append({
                    'thread_id': thread_id,
                    'status': 'answer_not_found',
                    'detail': 'question found but no subsequent INCOMING MESSAGE',
                    'thread_start_time': thread_start_time
                })
        else:
            # Question not found
            answers_missing_rows.append({
                'thread_id': thread_id,
                'status': 'question_not_found',
                'detail': 'no message_type WELCOME_MESSAGE or MESSAGE matching question text',
                'thread_start_time': thread_start_time
            })
    
    # Create DataFrames with proper columns even if empty
    if answers_found_rows:
        df_answers_found = pd.DataFrame(answers_found_rows)
    else:
        df_answers_found = pd.DataFrame(columns=['thread_id', 'question_created_at', 'answer_created_at', 
                                                  'answer_text', 'answer_text_norm', 'answer_len', 'answer_word_count'])
    
    if answers_missing_rows:
        df_answers_missing = pd.DataFrame(answers_missing_rows)
    else:
        df_answers_missing = pd.DataFrame(columns=['thread_id', 'status', 'detail', 'thread_start_time'])
    
    return df_answers_found, df_answers_missing, matched_questions


def fetch_all_threads(token, archived=None, inbox_id=None, sort=None, latest_message_timestamp_after=None):
    """Fetch all conversation threads from HubSpot API with pagination."""
    all_threads = []
    after = None
    page_num = 0
    previous_after = None  # Track previous cursor to detect loops
    
    # Create threads pages directory
    threads_pages_dir = Path("data/raw/threads_pages")
    threads_pages_dir.mkdir(parents=True, exist_ok=True)
    
    while True:
        page_num += 1
        print(f"\nFetching threads page {page_num}...")
        
        threads_data = fetch_threads(
            token, 
            limit=100,  # Use reasonable page size
            archived=archived,
            inbox_id=inbox_id,
            sort=sort,
            latest_message_timestamp_after=latest_message_timestamp_after,
            after=after
        )
        
        # Save page to cache
        page_path = threads_pages_dir / f"page_{page_num}.json"
        try:
            with open(page_path, 'w', encoding='utf-8') as f:
                json.dump(threads_data, f, indent=2, ensure_ascii=False)
        except IOError as e:
            print(f"  Warning: Failed to save page {page_num}: {e}")
        
        # Check if results list is empty
        results = threads_data.get("results", []) if threads_data else []
        if len(results) == 0:
            print("  No results returned; pagination complete.")
            break
        
        # Extract threads from this page
        all_threads.extend(results)
        print(f"  Found {len(results)} threads (total: {len(all_threads)})")
        
        # Check for next page
        paging = threads_data.get("paging", {}) if threads_data else {}
        next_info = paging.get("next", {})
        next_after = next_info.get("after") if isinstance(next_info, dict) else None
        
        if not next_after:
            print("  No paging.next.after; pagination complete.")
            break
        
        # Check for repeating cursor (infinite loop prevention)
        if next_after == after or next_after == previous_after:
            print(f"  Warning: Repeating cursor value detected ({next_after}); stopping pagination to prevent infinite loop.")
            break
        
        # Update cursors for next iteration
        previous_after = after
        after = next_after
    
    print(f"\nTotal threads fetched: {len(all_threads)}")
    return all_threads


def generate_answer_report(token, question_text, archived=None, inbox_id=None, sort=None, latest_message_timestamp_after=None):
    """Generate answer report by fetching all threads and extracting answers."""
    print("="*80)
    print("Answer Report Generation")
    print("="*80)
    
    # Fetch all threads with pagination
    print("\nFetching all threads...")
    all_threads = fetch_all_threads(
        token,
        archived=archived,
        inbox_id=inbox_id,
        sort=sort,
        latest_message_timestamp_after=latest_message_timestamp_after
    )
    
    if not all_threads:
        print("No threads found.")
        return
    
    # Process each thread
    print(f"\nProcessing {len(all_threads)} threads...")
    all_messages_dfs = []
    
    for idx, thread in enumerate(all_threads, 1):
        thread_id = thread.get('id')
        if not thread_id:
            continue
        
        # Progress update every 100 threads
        if idx % 100 == 0:
            print(f"  Processed {idx}/{len(all_threads)} threads...")
        
        # Fetch messages (with caching)
        messages_data = fetch_messages(token, thread_id, use_cache=True)
        
        if messages_data and 'results' in messages_data:
            # Build DataFrame for this thread
            df_thread = build_transcript_dataframe(messages_data, thread_id)
            all_messages_dfs.append(df_thread)
        else:
            print(f"  Warning: Failed to fetch messages for thread {thread_id}")
    
    if not all_messages_dfs:
        print("No messages found in any thread.")
        return
    
    # Combine all DataFrames
    print("\nCombining messages...")
    df = pd.concat(all_messages_dfs, ignore_index=True)
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
    df = df.sort_values(['thread_id', 'created_at']).reset_index(drop=True)
    
    # Extract answers
    print(f"\nExtracting answers to question: '{question_text}'")
    df_answers_found, df_answers_missing, _ = extract_answers(df, question_text)
    
    # Calculate statistics
    total_threads = len(all_threads)
    threads_with_question = len(df_answers_found) + len(df_answers_missing[df_answers_missing['status'] == 'answer_not_found'])
    threads_with_answer = len(df_answers_found)
    threads_question_missing = len(df_answers_missing[df_answers_missing['status'] == 'question_not_found'])
    threads_answer_missing = len(df_answers_missing[df_answers_missing['status'] == 'answer_not_found'])
    
    # Print summary
    print(f"\n{'='*80}")
    print("Answer Report Summary")
    print(f"{'='*80}")
    print(f"Threads scanned: {total_threads}")
    print(f"Answers found: {threads_with_answer}")
    print(f"Question not found: {threads_question_missing}")
    print(f"Answer not found: {threads_answer_missing}")
    
    # Save outputs
    output_dir = Path("data/out/answer_report")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save answers_found.csv
    answers_found_path = output_dir / "answers_found.csv"
    if len(df_answers_found) > 0:
        df_answers_found.to_csv(answers_found_path, index=False, encoding='utf-8')
        print(f"\nSaved: {answers_found_path} ({len(df_answers_found)} rows)")
    else:
        df_answers_found.to_csv(answers_found_path, index=False, encoding='utf-8')
        print(f"\nSaved: {answers_found_path} (0 rows)")
    
    # Save answers_missing.csv
    answers_missing_path = output_dir / "answers_missing.csv"
    if len(df_answers_missing) > 0:
        df_answers_missing.to_csv(answers_missing_path, index=False, encoding='utf-8')
        print(f"Saved: {answers_missing_path} ({len(df_answers_missing)} rows)")
    else:
        df_answers_missing.to_csv(answers_missing_path, index=False, encoding='utf-8')
        print(f"Saved: {answers_missing_path} (0 rows)")
    
    print(f"\nOutput directory: {output_dir.absolute()}")


def main():
    """Main execution function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch HubSpot conversation threads and messages')
    parser.add_argument('--amount', type=int, default=1, help='Number of conversations to fetch (default: 1)')
    parser.add_argument('--question', type=str, default='What are you looking for?', 
                       help='Question text to match for answer extraction (default: "What are you looking for?")')
    parser.add_argument('--answers-only', action='store_true', 
                       help='Only extract answers, skip transcript generation and CSV outputs')
    parser.add_argument('--include-missing', action='store_true',
                       help='Include combined answers file with missing threads (for debugging)')
    parser.add_argument('--debug-thread', type=str, default=None,
                       help='Debug: print detailed view for a specific thread ID')
    parser.add_argument('--debug-question-matches', type=int, default=10,
                       help='Debug: print first N matched question rows (default: 10)')
    parser.add_argument('--sort', type=str, default='id', choices=['id', 'latestMessageTimestamp'],
                       help='Sort threads by: "id" (default) or "latestMessageTimestamp"')
    parser.add_argument('--latest-after', type=str, default=None,
                       help='ISO datetime string for latestMessageTimestampAfter (e.g., "2023-01-01T00:00:00Z")')
    parser.add_argument('--recent-days', type=int, default=None,
                       help='Auto-compute latest-after as UTC now minus N days')
    parser.add_argument('--answerreport', action='store_true',
                       help='Generate answer report: fetch all threads, extract answers, export to CSV (implies --answers-only)')
    args = parser.parse_args()
    
    amount = args.amount
    question_text = args.question
    answers_only = args.answers_only or args.answerreport  # --answerreport implies --answers-only
    include_missing = args.include_missing
    debug_thread_id = args.debug_thread
    debug_question_matches = args.debug_question_matches
    sort = args.sort
    latest_after = args.latest_after
    recent_days = args.recent_days
    answerreport = args.answerreport
    
    if not answerreport and amount < 1:
        print("ERROR: --amount must be at least 1")
        sys.exit(1)
    
    # Handle latestMessageTimestampAfter parameter
    latest_message_timestamp_after = None
    if sort == "latestMessageTimestamp":
        # Prefer --latest-after over --recent-days if both provided
        if latest_after:
            # Validate ISO format
            try:
                pd.to_datetime(latest_after, utc=True)
                latest_message_timestamp_after = latest_after
            except (ValueError, TypeError):
                print("ERROR: --latest-after must be a valid ISO datetime string")
                print("Example format: 2023-01-01T00:00:00Z")
                sys.exit(1)
        elif recent_days is not None:
            if recent_days < 0:
                print("ERROR: --recent-days must be non-negative")
                sys.exit(1)
            # Compute latest-after as UTC now minus recent_days
            now_utc = datetime.now(timezone.utc)
            target_date = now_utc - pd.Timedelta(days=recent_days)
            latest_message_timestamp_after = target_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            print("ERROR: --sort latestMessageTimestamp requires --latest-after or --recent-days")
            print("Example: --sort latestMessageTimestamp --recent-days 365")
            print("Example: --sort latestMessageTimestamp --latest-after 2023-01-01T00:00:00Z")
            sys.exit(1)
    
    # Handle answer report mode
    if answerreport:
        # Load token
        token = load_hubspot_token()
        if not token:
            print("ERROR: HubSpot token not found in environment variables.")
            print("Please set one of: OLD_ACCESS_TOKEN, HUBSPOT_TOKEN, ACCESS_TOKEN, or PRIVATE_APP_TOKEN in your .env file.")
            sys.exit(1)
        
        # Generate answer report
        generate_answer_report(
            token,
            question_text,
            archived=None,  # Can be extended to support these flags
            inbox_id=None,
            sort=sort,
            latest_message_timestamp_after=latest_message_timestamp_after
        )
        return
    
    print("HubSpot Chatbot Insights - Proof of Concept")
    print("="*80)
    print(f"Fetching {amount} conversation(s)...")
    
    # Load token
    token = load_hubspot_token()
    if not token:
        print("ERROR: HubSpot token not found in environment variables.")
        print("Please set one of: OLD_ACCESS_TOKEN, HUBSPOT_TOKEN, ACCESS_TOKEN, or PRIVATE_APP_TOKEN in your .env file.")
        sys.exit(1)
    
    # Fetch portal info for verification
    print("\nFetching portal information for verification...")
    me_data = fetch_me(token)
    save_json(me_data, "data/raw/poc_me.json")
    
    # Print portal identifying information
    portal_id = me_data.get('portalId') or me_data.get('hubId')
    if portal_id:
        print(f"Portal ID / Hub ID: {portal_id}")
    
    # Print other identifying fields
    identifying_fields = ['user', 'appId', 'userId', 'hubDomain']
    for field in identifying_fields:
        if field in me_data:
            print(f"{field}: {me_data[field]}")
    
    # Fetch inboxes for diagnostics
    print("\nFetching inboxes for diagnostics...")
    inboxes_data = fetch_inboxes(token)
    save_json(inboxes_data, "data/raw/poc_inboxes.json")
    
    inboxes = []
    if inboxes_data and 'results' in inboxes_data:
        inboxes = inboxes_data['results']
    
    print(f"Inboxes returned: {len(inboxes)}")
    for inbox in inboxes:
        inbox_id = inbox.get('id', 'N/A')
        inbox_name = inbox.get('name', 'N/A')
        print(f"  - Inbox ID: {inbox_id}, Name: {inbox_name}")
    
    # Try to fetch threads with fallback logic
    threads_data = None
    threads = []
    
    # A) Try /threads?limit=amount (with sort params if specified)
    print(f"\nTrying: /threads?limit={amount}")
    threads_data = fetch_threads(token, limit=amount, sort=sort, latest_message_timestamp_after=latest_message_timestamp_after)
    if threads_data and 'results' in threads_data and len(threads_data['results']) > 0:
        threads = threads_data['results']
        print(f"✓ Found {len(threads)} thread(s) using default query")
    else:
        # B) Try /threads?limit=amount&archived=true
        print(f"\nTrying: /threads?limit={amount}&archived=true")
        threads_data = fetch_threads(token, limit=amount, archived=True, sort=sort, latest_message_timestamp_after=latest_message_timestamp_after)
        if threads_data and 'results' in threads_data and len(threads_data['results']) > 0:
            threads = threads_data['results']
            print(f"✓ Found {len(threads)} thread(s) in archived conversations")
        elif inboxes:
            # C) Try each inbox
            print(f"\nTrying inbox-specific queries...")
            for inbox in inboxes:
                inbox_id = inbox.get('id')
                if not inbox_id:
                    continue
                
                inbox_name = inbox.get('name', inbox_id)
                print(f"  Trying inbox: {inbox_name} (ID: {inbox_id})")
                
                # Try non-archived first
                threads_data = fetch_threads(token, limit=amount, inbox_id=inbox_id, sort=sort, latest_message_timestamp_after=latest_message_timestamp_after)
                if threads_data and 'results' in threads_data and len(threads_data['results']) > 0:
                    threads = threads_data['results']
                    print(f"  ✓ Found {len(threads)} thread(s) in inbox: {inbox_name}")
                    break
                
                # Try archived
                threads_data = fetch_threads(token, limit=amount, archived=True, inbox_id=inbox_id, sort=sort, latest_message_timestamp_after=latest_message_timestamp_after)
                if threads_data and 'results' in threads_data and len(threads_data['results']) > 0:
                    threads = threads_data['results']
                    print(f"  ✓ Found {len(threads)} thread(s) in archived conversations for inbox: {inbox_name}")
                    break
    
    # Check if we found any threads
    if not threads:
        print("\n" + "="*80)
        print("ERROR: No threads found even when including archived and filtering by inboxId.")
        print("="*80)
        print("\nSuggested next steps:")
        print("1. Confirm the token is for the correct HubSpot portal")
        print("2. Confirm conversations exist in Conversations > Inbox in the HubSpot UI")
        print("3. Confirm chats are not only stored as form submissions (form submissions")
        print("   are not accessible via the Conversations API)")
        print("4. Verify your private app has the 'conversations.read' scope enabled")
        sys.exit(1)
    
    # Limit to requested amount
    threads = threads[:amount]
    
    # Save raw threads JSON
    save_json(threads_data, "data/raw/poc_threads.json")
    
    # Process all threads and collect messages
    all_messages_dfs = []
    all_messages_combined = []
    thread_messages_map = {}  # Store messages per thread for transcript printing
    
    print(f"\nProcessing {len(threads)} thread(s)...")
    for idx, thread in enumerate(threads, 1):
        thread_id = thread.get('id')
        if not thread_id:
            print(f"  Thread {idx}: ERROR - Thread ID not found in response.")
            continue
        
        print(f"\n  Thread {idx}/{len(threads)}: {thread_id}")
        
        # Fetch messages for this thread
        print(f"  Fetching messages for thread {thread_id}...")
        messages_data = fetch_messages(token, thread_id)
        
        if messages_data and 'results' in messages_data:
            message_count = len(messages_data['results'])
            print(f"  Messages returned: {message_count}")
            
            # Build DataFrame for this thread
            df_thread = build_transcript_dataframe(messages_data, thread_id)
            all_messages_dfs.append(df_thread)
            
            # Store messages for this thread
            thread_messages_map[thread_id] = messages_data
            
            # Also keep raw messages for combined JSON
            all_messages_combined.extend(messages_data['results'])
        else:
            print(f"  No messages found for thread {thread_id}")
    
    # Save combined messages JSON (all threads)
    if all_messages_combined:
        combined_messages_data = {'results': all_messages_combined}
        save_json(combined_messages_data, "data/raw/poc_messages.json")
    
    # Combine all DataFrames
    if all_messages_dfs:
        df = pd.concat(all_messages_dfs, ignore_index=True)
        # Ensure created_at is datetime with UTC
        df['created_at'] = pd.to_datetime(df['created_at'], utc=True)
        # Sort by thread_id and created_at to ensure chronological order
        df = df.sort_values(['thread_id', 'created_at']).reset_index(drop=True)
        
        total_messages = len(df)
        print(f"\nTotal messages across all threads: {total_messages}")
        
        # Debug: Print thread details if requested
        if debug_thread_id:
            print(f"\n{'='*80}")
            print(f"DEBUG: Thread {debug_thread_id}")
            print(f"{'='*80}")
            thread_debug_df = df[df['thread_id'] == int(debug_thread_id)].copy()
            if len(thread_debug_df) > 0:
                print("\nPer-row view:")
                print("created_at | direction | message_type | text")
                print("-" * 80)
                for _, row in thread_debug_df.iterrows():
                    created_at_str = str(row['created_at'])
                    direction_str = str(row['direction'])
                    msg_type_str = str(row['message_type'])
                    text_str = str(row['text'])[:50] + ('...' if len(str(row['text'])) > 50 else '')
                    print(f"{created_at_str} | {direction_str} | {msg_type_str} | {text_str}")
            else:
                print(f"Thread {debug_thread_id} not found in DataFrame")
        
        # Extract answers to the question
        print(f"\nExtracting answers to question: '{question_text}'")
        df_answers_found, df_answers_missing, matched_questions = extract_answers(
            df, question_text, debug_thread_id=debug_thread_id, debug_question_matches=debug_question_matches
        )
        
        # Debug: Print question matches
        if matched_questions:
            print(f"\nFirst {len(matched_questions)} matched question rows:")
            print("thread_id | created_at | direction | message_type | text")
            print("-" * 100)
            for q in matched_questions:
                print(f"{q['thread_id']} | {q['created_at']} | {q['direction']} | {q['message_type']} | {q['text']}")
        
        # Debug: Print extraction results for debug thread
        if debug_thread_id:
            debug_thread_id_int = int(debug_thread_id)
            debug_answer_found = df_answers_found[df_answers_found['thread_id'] == debug_thread_id_int]
            debug_answer_missing = df_answers_missing[df_answers_missing['thread_id'] == debug_thread_id_int]
            
            # Find question row in the thread
            thread_debug_df = df[df['thread_id'] == debug_thread_id_int].copy()
            question_row_found = None
            for _, row in thread_debug_df.iterrows():
                msg_type = str(row.get('message_type', '')).strip()
                text = row.get('text', '')
                if (msg_type in ['WELCOME_MESSAGE', 'MESSAGE'] and
                    text and 
                    text != '<non-text message>' and
                    matches_question(text, question_text)):
                    question_row_found = row
                    break
            
            print(f"\n{'='*80}")
            print(f"DEBUG: Extraction Results for Thread {debug_thread_id}")
            print(f"{'='*80}")
            
            if question_row_found is not None:
                print(f"Detected question row:")
                print(f"  created_at: {question_row_found['created_at']}")
                print(f"  text: {question_row_found['text']}")
            else:
                print("Detected question row: not found")
            
            if len(debug_answer_found) > 0:
                row = debug_answer_found.iloc[0]
                print(f"\nExtracted answer row:")
                print(f"  created_at: {row['answer_created_at']}")
                print(f"  text: {row['answer_text']}")
            elif len(debug_answer_missing) > 0:
                row = debug_answer_missing.iloc[0]
                print(f"\nExtracted answer row: not found")
                print(f"  Status: {row['status']}")
                print(f"  Detail: {row['detail']}")
            else:
                print(f"\nExtracted answer row: not found (thread not processed)")
        
        # Calculate statistics
        total_threads = len(threads)
        threads_with_question = len(df_answers_found) + len(df_answers_missing[df_answers_missing['status'] == 'answer_not_found'])
        threads_with_answer = len(df_answers_found)
        threads_question_missing = len(df_answers_missing[df_answers_missing['status'] == 'question_not_found'])
        threads_answer_missing = len(df_answers_missing[df_answers_missing['status'] == 'answer_not_found'])
        
        # Print summary
        print(f"\n{'='*80}")
        print("Answer Extraction Summary")
        print(f"{'='*80}")
        print(f"Threads fetched: {total_threads}")
        print(f"Question found: {threads_with_question}")
        print(f"Answer found: {threads_with_answer}")
        print(f"Question missing: {total_threads - threads_with_question}")
        print(f"Answer missing: {threads_with_question - threads_with_answer}")
        
        # Print top 20 answers by frequency
        if threads_with_answer > 0:
            answer_counts = df_answers_found['answer_text'].value_counts().head(20)
            print(f"\nTop 20 answers by frequency:")
            for idx, (answer, count) in enumerate(answer_counts.items(), 1):
                print(f"  {idx:2d}. [{count:3d}x] {answer[:80]}{'...' if len(answer) > 80 else ''}")
        
        # Generate output filename based on question
        question_slug = question_text.lower().replace('?', '').replace('!', '').replace(' ', '_').replace("'", '').replace('"', '')
        # Clean up any remaining special characters
        question_slug = re.sub(r'[^a-z0-9_]', '', question_slug)
        
        # Save found answers DataFrame
        answers_found_csv_path = f"data/out/{question_slug}_answers_found.csv"
        Path(answers_found_csv_path).parent.mkdir(parents=True, exist_ok=True)
        if len(df_answers_found) > 0:
            df_answers_found.to_csv(answers_found_csv_path, index=False, encoding='utf-8')
            print(f"\nSaved found answers to {answers_found_csv_path} ({len(df_answers_found)} rows)")
        else:
            # Create empty file with headers
            df_answers_found.to_csv(answers_found_csv_path, index=False, encoding='utf-8')
            print(f"\nSaved found answers to {answers_found_csv_path} (0 rows)")
        
        # Save missing answers DataFrame
        answers_missing_csv_path = f"data/out/{question_slug}_answers_missing.csv"
        Path(answers_missing_csv_path).parent.mkdir(parents=True, exist_ok=True)
        if len(df_answers_missing) > 0:
            df_answers_missing.to_csv(answers_missing_csv_path, index=False, encoding='utf-8')
            print(f"Saved missing answers to {answers_missing_csv_path} ({len(df_answers_missing)} rows)")
        else:
            # Create empty file with headers
            df_answers_missing.to_csv(answers_missing_csv_path, index=False, encoding='utf-8')
            print(f"Saved missing answers to {answers_missing_csv_path} (0 rows)")
        
        # Save combined file only if --include-missing is set
        if include_missing:
            # Create combined DataFrame with all threads (sorted by thread_id)
            all_thread_ids = sorted(set(df_answers_found['thread_id'].tolist()) | set(df_answers_missing['thread_id'].tolist()))
            combined_rows = []
            
            for thread_id in all_thread_ids:
                found_row = df_answers_found[df_answers_found['thread_id'] == thread_id]
                if len(found_row) > 0:
                    row = found_row.iloc[0]
                    combined_rows.append({
                        'thread_id': thread_id,
                        'question_created_at': row['question_created_at'],
                        'answer_created_at': row['answer_created_at'],
                        'answer_text': row['answer_text']
                    })
                else:
                    # Thread is in missing list
                    combined_rows.append({
                        'thread_id': thread_id,
                        'question_created_at': pd.NaT,
                        'answer_created_at': pd.NaT,
                        'answer_text': None
                    })
            
            df_answers_combined = pd.DataFrame(combined_rows)
            answers_csv_path = f"data/out/{question_slug}_answers.csv"
            df_answers_combined.to_csv(answers_csv_path, index=False, encoding='utf-8')
            print(f"Saved combined answers to {answers_csv_path} ({len(df_answers_combined)} rows)")
        
        # Only generate transcripts and full CSV if not in answers-only mode
        if not answers_only:
            # Print transcript for each thread (if amount > 1, show thread IDs)
            show_thread_id = len(threads) > 1
            for idx, thread in enumerate(threads, 1):
                thread_id = thread.get('id')
                if thread_id and thread_id in thread_messages_map:
                    print_transcript(thread_messages_map[thread_id], thread_id=thread_id, show_thread_id=show_thread_id)
            
            print(f"\nDataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns")
            print("\nDataFrame head:")
            print(df.head())
            
            # Save DataFrame to CSV
            csv_path = "data/out/poc_transcript.csv"
            Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"\nSaved DataFrame to {csv_path}")
    else:
        print("\nERROR: No messages found in any thread.")
        sys.exit(1)
    
    print("\n✓ Proof of concept completed successfully!")


# Usage:
#   python init_poc.py [--amount N] [--question TEXT] [--answers-only] [--sort SORT] [--latest-after ISO] [--recent-days N]
#
# Examples:
#   python init_poc.py                           # Fetch 1 conversation (default)
#   python init_poc.py --amount 10               # Fetch 10 conversations
#   python init_poc.py --amount 10 --answers-only # Only extract answers, skip transcripts
#   python init_poc.py --question "What's your name?"  # Custom question
#   python init_poc.py --amount 50 --answers-only --sort latestMessageTimestamp --recent-days 365  # Pull 50 recent threads from last 365 days
#   python init_poc.py --amount 200 --answers-only --sort latestMessageTimestamp --latest-after 2023-01-01T00:00:00Z  # Pull threads since a fixed date
#
# This script will:
#   1. Load token from .env (prioritizes OLD_ACCESS_TOKEN)
#   2. Fetch and save portal info to data/raw/poc_me.json
#   3. Fetch and save inboxes to data/raw/poc_inboxes.json
#   4. Fetch N threads (with fallbacks) and save to data/raw/poc_threads.json
#   5. Fetch messages for all threads and save to data/raw/poc_messages.json
#   6. Extract answers to the specified question and save to data/out/how_can_we_help_you_answers.csv
#   7. (If not --answers-only) Print transcript(s) and create combined DataFrame
#   8. (If not --answers-only) Save DataFrame to data/out/poc_transcript.csv


if __name__ == "__main__":
    main()
