#!/usr/bin/env python3
"""
Convert HubSpot Conversations thread data into a formatted HubSpot Note body.

Usage examples:
  python conversation_to_note.py --poc
  python conversation_to_note.py --input sample.json
"""

import argparse
import html
import json
import os
import re
import sys
from datetime import datetime
from html.parser import HTMLParser
from textwrap import wrap
from typing import Dict, List, Optional, Any

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback for Python < 3.9
    ZoneInfo = None


class HTMLStripper(HTMLParser):
    """HTML parser to extract text content, handling links specially."""
    
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
        elif tag_lower == 'a':
            # For <a href="mailto:..."> or <a href="tel:...">, extract the link text
            # The link text will be in handle_data, so we just mark we're in a link
            pass
    
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


def strip_html_to_text(html_str: str) -> str:
    """Strip HTML tags and convert to plain text, preserving line breaks and handling entities."""
    if not html_str:
        return ""
    
    # Convert block-level HTML tags to newlines before parsing
    html_str = re.sub(r'<br\s*/?>', '\n', html_str, flags=re.IGNORECASE)
    html_str = re.sub(r'</p>', '\n', html_str, flags=re.IGNORECASE)
    html_str = re.sub(r'</div>', '\n', html_str, flags=re.IGNORECASE)
    
    # Handle mailto: and tel: links - extract the displayed text, not the URL
    # Pattern: <a href="mailto:email">text</a> -> text
    # Pattern: <a href="tel:phone">text</a> -> text
    html_str = re.sub(r'<a\s+href=["\'](mailto|tel):[^"\']*["\'][^>]*>([^<]+)</a>', r'\2', html_str, flags=re.IGNORECASE)
    
    # Parse HTML
    parser = HTMLStripper()
    parser.feed(html_str)
    text = parser.get_text()
    
    # Decode HTML entities (e.g. &#x27; -> ', &apos; -> ', &nbsp; -> space)
    text = html.unescape(text)
    
    # Normalize whitespace: collapse multiple spaces within a line, preserve newlines
    lines = []
    for line in text.split('\n'):
        # Collapse multiple spaces/tabs to single space, but preserve intentional spacing
        line = re.sub(r'[\t ]+', ' ', line)
        # Strip leading/trailing whitespace but keep the line if it has content
        line = line.strip()
        lines.append(line)
    
    # Join lines, preserving structure
    result = '\n'.join(lines)
    
    # Collapse >2 consecutive blank lines to max 2
    result = re.sub(r'\n{3,}', '\n\n', result)
    
    # Final strip
    return result.strip()


def parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO8601 datetime string to datetime object."""
    if not dt_str:
        return None
    
    # Common ISO8601 formats
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%dT%H:%M:%S%z',
        '%Y-%m-%d %H:%M:%S',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    
    # Try parsing with timezone info
    try:
        # Handle offset format like +00:00
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        pass
    
    return None


def format_dt(dt: Optional[datetime], tz: str) -> str:
    """Format datetime to string with timezone conversion."""
    if dt is None:
        return "(time unknown)"
    
    try:
        if ZoneInfo:
            # Convert to target timezone
            if dt.tzinfo is None:
                # Assume UTC if no timezone info
                dt = dt.replace(tzinfo=ZoneInfo('UTC'))
            
            target_tz = ZoneInfo(tz)
            dt_local = dt.astimezone(target_tz)
            return dt_local.strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            # Fallback: just format as-is
            return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        # If timezone conversion fails, return original format
        return dt.strftime('%Y-%m-%d %H:%M:%S')


def redact_text(text: str) -> str:
    """Redact email addresses and phone-like strings."""
    # Redact email addresses
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[REDACTED EMAIL]', text)
    
    # Redact phone numbers (various formats)
    phone_patterns = [
        r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',  # US format: 123-456-7890
        r'\b\(\d{3}\)\s?\d{3}[-.]?\d{4}\b',  # (123) 456-7890
        r'\b\+?\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}\b',  # International
    ]
    
    for pattern in phone_patterns:
        text = re.sub(pattern, '[REDACTED PHONE]', text)
    
    return text


def wrap_text(text: str, width: int) -> str:
    """Wrap text to specified width, preserving paragraph structure."""
    if not text:
        return ""
    
    lines = text.split('\n')
    wrapped_lines = []
    
    for line in lines:
        if len(line) <= width:
            wrapped_lines.append(line)
        else:
            # Wrap long lines
            wrapped = wrap(line, width=width, break_long_words=False, break_on_hyphens=False)
            wrapped_lines.extend(wrapped)
    
    return '\n'.join(wrapped_lines)


def normalize_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a single message from bundle format."""
    return {
        'id': msg.get('id', 'unknown'),
        'created_at': msg.get('createdAt'),
        'direction': msg.get('direction'),
        'type': msg.get('type', 'UNKNOWN'),
        'text': msg.get('text'),
        'rich_text': msg.get('richText'),
        'senders': msg.get('senders', []),
        'recipients': msg.get('recipients', []),
        'attachments': msg.get('attachments', [])
    }


def normalize_conversation(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize conversation data from various input shapes."""
    # Check if this is a bundle format (threadId, thread, messagesResponse)
    if 'threadId' in raw or ('thread' in raw and 'messagesResponse' in raw):
        # Bundle format
        thread_obj = raw.get('thread', {})
        messages_resp = raw.get('messagesResponse', {})
        
        normalized = {
            'thread_id': raw.get('threadId') or thread_obj.get('id', 'unknown'),
            'created_at': thread_obj.get('createdAt'),
            'updated_at': thread_obj.get('latestMessageTimestamp') or thread_obj.get('updatedAt'),
            'status': thread_obj.get('status'),
            'inbox_id': thread_obj.get('inboxId'),
            'associated_contact_id': thread_obj.get('associatedContactId'),
            'channel_id': thread_obj.get('originalChannelId') or thread_obj.get('channelId'),
            'channel_account_id': thread_obj.get('originalChannelAccountId') or thread_obj.get('channelAccountId'),
            'messages': []
        }
        
        # Normalize messages
        raw_messages = messages_resp.get('results', [])
        for msg in raw_messages:
            normalized['messages'].append(normalize_message(msg))
        
        return normalized
    
    # Legacy format (backward compatibility)
    normalized = {
        'thread_id': raw.get('id', 'unknown'),
        'created_at': raw.get('createdAt'),
        'updated_at': raw.get('updatedAt'),
        'status': raw.get('status'),
        'inbox_id': raw.get('inboxId'),
        'associated_contact_id': raw.get('associatedContactId'),
        'channel_id': raw.get('channelId'),
        'channel_account_id': raw.get('channelAccountId'),
        'channel': raw.get('channel', 'Unknown'),
        'subject': raw.get('subject'),
        'participants': [],
        'messages': []
    }
    
    # Extract participants (legacy format)
    if 'participants' in raw:
        normalized['participants'] = raw['participants']
    elif 'participants' not in raw:
        # Try to infer from messages
        seen = set()
        for msg in raw.get('messages', {}).get('results', raw.get('messages', [])):
            sender = msg.get('sender', {})
            role = sender.get('role', '')
            name = sender.get('name', '')
            email = sender.get('email', '')
            key = (role, name, email)
            if key not in seen:
                seen.add(key)
                normalized['participants'].append({
                    'role': role,
                    'name': name,
                    'email': email
                })
    
    # Extract messages (legacy format)
    if 'messages' in raw:
        if isinstance(raw['messages'], dict) and 'results' in raw['messages']:
            raw_messages = raw['messages']['results']
        elif isinstance(raw['messages'], list):
            raw_messages = raw['messages']
        else:
            raw_messages = []
        
        for msg in raw_messages:
            normalized['messages'].append({
                'id': msg.get('id', 'unknown'),
                'created_at': msg.get('createdAt'),
                'direction': msg.get('direction'),
                'type': msg.get('type', 'MESSAGE'),
                'text': msg.get('text'),
                'rich_text': msg.get('richText'),
                'senders': [msg.get('sender', {})] if 'sender' in msg else [],
                'recipients': msg.get('recipients', []),
                'attachments': msg.get('attachments', [])
            })
    
    return normalized


def infer_speaker_role(senders: List[Dict[str, Any]], direction: Optional[str] = None) -> str:
    """Infer speaker role from senders list and direction using actorId prefixes."""
    if not senders:
        # Fallback to direction
        if direction == "INCOMING":
            return "Customer"
        elif direction == "OUTGOING":
            return "Agent"
        else:
            return "System"
    
    # Get the first sender's actorId
    actor_id = senders[0].get('actorId', '')
    
    # Rule 1: If any sender.actorId startswith "V-" => role = "Customer"
    if actor_id.startswith("V-"):
        return "Customer"
    
    # Rule 2: Else if sender.actorId startswith "B-" or "A-" => role = "Agent"
    if actor_id.startswith("B-") or actor_id.startswith("A-"):
        return "Agent"
    
    # Rule 3: Else if sender.actorId startswith "S-" => role = "System"
    if actor_id.startswith("S-"):
        return "System"
    
    # Rule 4: Else if direction == "INCOMING" => "Customer"
    if direction == "INCOMING":
        return "Customer"
    
    # Rule 5: Else if direction == "OUTGOING" => "Agent"
    if direction == "OUTGOING":
        return "Agent"
    
    # Rule 6: Else => "System"
    return "System"


def format_speaker_label(senders: List[Dict[str, Any]], direction: Optional[str] = None) -> str:
    """Format speaker label from senders info and direction."""
    role = infer_speaker_role(senders, direction)
    
    # Get name from first sender if available
    name = None
    actor_id = None
    if senders:
        first_sender = senders[0]
        name = first_sender.get('name')
        actor_id = first_sender.get('actorId', '')
    
    # Prefer sender["name"] if present, otherwise show the actorId
    if name:
        return f"{role} ({name})"
    elif actor_id:
        return f"{role} ({actor_id})"
    else:
        return role


def format_note(conversation: Dict[str, Any], max_chars: int = 60000, wrap_width: int = 110,
                redact: bool = False, timezone: str = 'UTC') -> str:
    """Format conversation into HubSpot Note body."""
    parts = []
    
    # Header block
    parts.append("Conversation Transcript")
    parts.append(f"Thread: {conversation['thread_id']}")
    parts.append(f"Status: {conversation.get('status') or 'Unknown'}")
    
    inbox_id = conversation.get('inbox_id')
    parts.append(f"Inbox: {inbox_id or 'Unknown'}")
    
    associated_contact_id = conversation.get('associated_contact_id')
    parts.append(f"Associated contact ID: {associated_contact_id or '(none)'}")
    
    channel_id = conversation.get('channel_id') or 'Unknown'
    channel_account_id = conversation.get('channel_account_id') or 'Unknown'
    parts.append(f"Channel: {channel_id} / Account: {channel_account_id}")
    
    created_dt = parse_iso(conversation.get('created_at'))
    updated_dt = parse_iso(conversation.get('updated_at'))
    parts.append(f"Created: {format_dt(created_dt, timezone)}")
    parts.append(f"Updated: {format_dt(updated_dt, timezone)}")
    
    # Separate included and excluded messages
    all_messages = conversation.get('messages', [])
    included_types = {'MESSAGE', 'WELCOME_MESSAGE'}
    
    included_messages = []
    excluded_messages = []
    excluded_types = set()
    
    for msg in all_messages:
        msg_type = msg.get('type', 'UNKNOWN')
        if msg_type in included_types:
            included_messages.append(msg)
        else:
            excluded_messages.append(msg)
            excluded_types.add(msg_type)
    
    parts.append(f"Included messages: {len(included_messages)}")
    
    if excluded_types:
        excluded_types_str = ', '.join(sorted(excluded_types))
        parts.append(f"Omitted system events: {len(excluded_messages)} ({excluded_types_str})")
    
    # Separator
    parts.append('-' * 80)
    
    # Sort included messages by createdAt ascending (stable)
    def get_sort_key(msg):
        dt = parse_iso(msg.get('created_at'))
        if dt:
            return (dt.timestamp(), msg.get('id', ''))
        return (0, msg.get('id', ''))
    
    sorted_messages = sorted(included_messages, key=get_sort_key)
    
    transcript_parts = []
    for msg in sorted_messages:
        msg_dt = parse_iso(msg.get('created_at'))
        time_str = format_dt(msg_dt, timezone) if msg_dt else "(time unknown)"
        
        senders = msg.get('senders', [])
        direction = msg.get('direction')
        speaker_label = format_speaker_label(senders, direction)
        
        # Extract and clean message text
        # Prefer message["text"] if non-empty (after stripping), else use richText/rich_text
        text = msg.get('text') or ''
        if text:
            text = text.strip()
        
        if not text:
            rich_text = msg.get('rich_text') or msg.get('richText') or ''
            if rich_text:
                text = rich_text.strip()
        
        if not text:
            text = "(no message body)"
        else:
            text = strip_html_to_text(text)
            # Normalize trailing whitespace and edge cases
            text = text.strip()
            if not text:
                text = "(no message body)"
        
        if redact:
            text = redact_text(text)
        
        # Wrap text
        text = wrap_text(text, wrap_width)
        
        # Format message
        transcript_parts.append(f"[{time_str}] {speaker_label}:")
        transcript_parts.append(text)
        
        # Attachments
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
            transcript_parts.append(f"Attachments: {', '.join(att_strs)}")
        
        transcript_parts.append("")  # Blank line between messages
    
    transcript_text = '\n'.join(transcript_parts).rstrip()
    
    # Check length and truncate if needed
    header_text = '\n'.join(parts)
    full_text = f"{header_text}\n{transcript_text}\n\nEnd of transcript."
    
    if len(full_text) > max_chars:
        # Truncate transcript
        available_chars = max_chars - len(header_text) - len("\n\n[TRUNCATED ...]\n\nEnd of transcript.")
        truncated_transcript = transcript_text[:available_chars]
        
        # Try to truncate at a message boundary
        last_msg_end = truncated_transcript.rfind('\n\n')
        if last_msg_end > available_chars * 0.8:  # If we can find a boundary reasonably close
            truncated_transcript = truncated_transcript[:last_msg_end]
        
        full_text = f"{header_text}\n{truncated_transcript}\n\n[TRUNCATED - Note exceeds {max_chars} characters]\n\nEnd of transcript."
    
    return full_text


def load_conversation_from_file(path: str) -> Dict[str, Any]:
    """Load conversation JSON from file."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file: {e}", file=sys.stderr)
        sys.exit(2)
    except FileNotFoundError:
        print(f"Error: File not found: {path}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        sys.exit(2)


def get_poc_sample() -> Dict[str, Any]:
    """Load sample conversation from threads_10.pretty.json if present, else return built-in sample."""
    # Try to load from local file (check multiple possible locations)
    possible_paths = [
        './threads_10.pretty.json',
        './out/threads_10.pretty.json',
        'threads_10.pretty.json',
        'out/threads_10.pretty.json'
    ]
    sample_file = None
    for path in possible_paths:
        if os.path.exists(path):
            sample_file = path
            break
    
    if sample_file:
        try:
            with open(sample_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # If it's an array, pick the first item
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            elif isinstance(data, dict):
                return data
        except Exception:
            # If loading fails, fall back to embedded sample
            pass
    
    # Fall back to embedded sample (one thread from threads_10.pretty.json)
    return {
        "threadId": "3562432284",
        "thread": {
            "id": "3562432284",
            "createdAt": "2022-11-25T17:29:14.052Z",
            "status": "OPEN",
            "originalChannelId": "1000",
            "originalChannelAccountId": "80850280",
            "latestMessageTimestamp": "2022-11-25T17:29:14.142Z",
            "inboxId": "147959634",
            "associatedContactId": "340101"
        },
        "messagesResponse": {
            "results": [
                {
                    "id": "a5a6145f-5bf1-4d9c-b029-a1d133c25d8c",
                    "conversationsThreadId": "3562432284",
                    "createdAt": "2022-11-25T17:29:14.776Z",
                    "createdBy": "S-hubspot",
                    "senders": [{"actorId": "S-hubspot"}],
                    "recipients": [],
                    "newStatus": "OPEN",
                    "type": "THREAD_STATUS_CHANGE"
                },
                {
                    "id": "e1d9b8e855e8418392b75a3ba1d5deb6",
                    "conversationsThreadId": "3562432284",
                    "createdAt": "2022-11-25T17:29:14.142Z",
                    "createdBy": "V-340101",
                    "senders": [{"actorId": "V-340101"}],
                    "recipients": [],
                    "text": "What type of label is usually used on washer dryer drums for warnings",
                    "richText": "<div>What type of label is usually used on washer dryer drums for warnings</div>",
                    "attachments": [],
                    "direction": "INCOMING",
                    "type": "MESSAGE"
                },
                {
                    "id": "b45b722012494314b8f68e106aa96f4f",
                    "conversationsThreadId": "3562432284",
                    "createdAt": "2022-11-25T17:29:14.140Z",
                    "createdBy": "S-hubspot",
                    "senders": [{"actorId": "S-hubspot"}],
                    "recipients": [{"actorId": "V-340101"}],
                    "text": "Searching for the best solution?\nWe're here to help you",
                    "richText": "Searching for the best solution?\nWe're here to help you",
                    "type": "WELCOME_MESSAGE"
                }
            ]
        }
    }


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Convert HubSpot Conversations thread to formatted HubSpot Note body'
    )
    parser.add_argument(
        '--poc',
        action='store_true',
        help='Use sample conversation from threads_10.pretty.json (or built-in sample) and print raw bundle + formatted note'
    )
    parser.add_argument(
        '--input',
        type=str,
        help='Path to JSON file containing conversation/thread object or bundle'
    )
    parser.add_argument(
        '--max-chars',
        type=int,
        default=60000,
        help='Maximum characters for formatted note (default: 60000)'
    )
    parser.add_argument(
        '--wrap',
        type=int,
        default=110,
        help='Hard-wrap lines at this width (default: 110)'
    )
    parser.add_argument(
        '--redact',
        action='store_true',
        help='Redact email addresses and phone numbers'
    )
    parser.add_argument(
        '--timezone',
        type=str,
        default='UTC',
        help='Timezone for displaying timestamps (default: UTC)'
    )
    
    args = parser.parse_args()
    
    # Load conversation
    if args.poc:
        raw_conversation = get_poc_sample()
        
        # Print raw bundle first
        print("===== RAW BUNDLE (as pulled from API) =====")
        print(json.dumps(raw_conversation, indent=2))
        print()
        
    elif args.input:
        raw_conversation = load_conversation_from_file(args.input)
    else:
        parser.error("Either --poc or --input must be provided")
    
    # Normalize conversation
    try:
        conversation = normalize_conversation(raw_conversation)
    except Exception as e:
        print(f"Error: Failed to normalize conversation data: {e}", file=sys.stderr)
        sys.exit(2)
    
    # Format note
    try:
        note_body = format_note(
            conversation,
            max_chars=args.max_chars,
            wrap_width=args.wrap,
            redact=args.redact,
            timezone=args.timezone
        )
    except Exception as e:
        print(f"Error: Failed to format note: {e}", file=sys.stderr)
        sys.exit(2)
    
    # Print formatted note
    if args.poc:
        print("===== FORMATTED NOTE (paste into HubSpot Note) =====")
    print(note_body)
    sys.exit(0)


if __name__ == '__main__':
    main()