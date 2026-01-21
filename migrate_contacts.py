#!/usr/bin/env python3
"""
HubSpot Contact Migration Tool - Phase 1

Extracts all contacts and their properties from the OLD HubSpot account
and stores them in a local SQLite database for inspection.

Usage:
    python migrate_contacts.py --init
    python migrate_contacts.py --init --limit 100 --db ./out/test.sqlite
    python migrate_contacts.py --init --reset
"""

import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import requests
from dotenv import load_dotenv

# HubSpot API base URL
BASE_URL = "https://api.hubapi.com"

# User-Agent for requests
USER_AGENT = "HubSpot-Contact-Migration/1.0"

# Default database path
DEFAULT_DB_PATH = "./out/contacts_migration.sqlite"

# Schema version
SCHEMA_VERSION = 1

# Blacklisted property names (always exclude when creating contacts in NEW portal)
# These are system-managed properties that should not be copied
BLACKLISTED_PROPERTY_NAMES = {
    'createdate',
    'lastmodifieddate',
    'hs_object_id',
    'hs_all_contact_vids',
    'hs_analytics_first_timestamp',
    'hs_analytics_last_timestamp',
    'hs_analytics_first_visit_timestamp',
    'hs_analytics_last_visit_timestamp',
    'hs_latest_source_timestamp'
}

# Allowlisted property names (never exclude if they have non-empty values)
# These are core fields that are required or recommended for contact creation
ALLOWLISTED_PROPERTY_NAMES = {
    'email',  # Required
    'firstname',  # Recommended
    'lastname',  # Recommended
    'phone',  # Recommended
    'company'  # Recommended
}

# Blacklisted company property names (always exclude when creating companies in NEW portal)
# These are system-managed properties that should not be copied
BLACKLISTED_COMPANY_PROPERTY_NAMES = {
    'createdate',
    'lastmodifieddate',
    'hs_object_id',
    'lifecyclestage'  # Excluded to avoid triggering automation/segmentation on creation
}

# Allowlisted company property names (never exclude if they have non-empty values)
# These are core fields that are required or recommended for company creation
ALLOWLISTED_COMPANY_PROPERTY_NAMES = {
    'domain',  # Required
    'name'  # Required
}


def setup_logging(log_level: str = "INFO"):
    """Configure logging."""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_token() -> str:
    """
    Load OLD_ACCESS_TOKEN from environment variable or .env file.
    
    Exits with error if token not found.
    """
    load_dotenv()
    
    token = os.getenv('OLD_ACCESS_TOKEN')
    if token:
        return token
    
    print("ERROR: OLD_ACCESS_TOKEN not found in environment or .env file.", file=sys.stderr)
    print("Please set OLD_ACCESS_TOKEN in your environment or .env file.", file=sys.stderr)
    sys.exit(1)


def load_new_token() -> str:
    """
    Load ACCESS_TOKEN (NEW portal) from environment variable or .env file.
    
    Exits with error if token not found.
    """
    load_dotenv()
    
    token = os.getenv('ACCESS_TOKEN')
    if token:
        return token
    
    print("ERROR: ACCESS_TOKEN not found in environment or .env file.", file=sys.stderr)
    print("Please set ACCESS_TOKEN in your environment or .env file.", file=sys.stderr)
    sys.exit(1)


def make_request(method: str, url: str, token: str, **kwargs) -> requests.Response:
    """
    Make HTTP request to HubSpot API with retry logic.
    
    Handles:
    - 429 rate limits (with Retry-After header support)
    - 5xx server errors (with exponential backoff)
    - Max retries: ~8 per request
    
    Returns the response object.
    """
    headers = kwargs.pop('headers', {})
    headers['Authorization'] = f'Bearer {token}'
    headers['User-Agent'] = USER_AGENT
    headers.setdefault('Content-Type', 'application/json')
    
    max_retries = 8
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            
            # Success
            if response.status_code == 200:
                return response
            
            # Rate limit (429)
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                if retry_after:
                    try:
                        wait_seconds = int(retry_after)
                    except ValueError:
                        wait_seconds = 60
                else:
                    # Exponential backoff: 2^retry_count seconds
                    wait_seconds = min(2 ** retry_count, 60)
                
                logging.warning(f"Rate limited (429). Waiting {wait_seconds}s before retry {retry_count + 1}/{max_retries}...")
                time.sleep(wait_seconds)
                retry_count += 1
                continue
            
            # Server errors (5xx)
            if 500 <= response.status_code < 600:
                wait_seconds = min(2 ** retry_count, 60)
                logging.warning(f"Server error ({response.status_code}). Retrying in {wait_seconds}s ({retry_count + 1}/{max_retries})...")
                time.sleep(wait_seconds)
                retry_count += 1
                continue
            
            # Other errors - don't retry
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            if retry_count < max_retries - 1:
                wait_seconds = min(2 ** retry_count, 60)
                logging.warning(f"Request error: {e}. Retrying in {wait_seconds}s ({retry_count + 1}/{max_retries})...")
                time.sleep(wait_seconds)
                retry_count += 1
                continue
            else:
                raise
    
    # Should not reach here
    raise Exception(f"Max retries ({max_retries}) exceeded for {url}")


def create_database(db_path: str, reset: bool = False, reset_companies: bool = False, reset_associations: bool = False):
    """
    Create SQLite database with schema.
    
    Args:
        db_path: Path to SQLite database file
        reset: If True, drop existing contact tables before creating
        reset_companies: If True, drop existing company tables before creating
        reset_associations: If True, drop existing association tables before creating
    """
    db_dir = Path(db_path).parent
    db_dir.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    
    cursor = conn.cursor()
    
    if reset:
        logging.info("Dropping existing contact tables...")
        cursor.execute("DROP TABLE IF EXISTS contact_property_values_old")
        cursor.execute("DROP TABLE IF EXISTS contacts_old")
        cursor.execute("DROP TABLE IF EXISTS contact_properties_def")
        cursor.execute("DROP TABLE IF EXISTS meta")
        conn.commit()
    
    if reset_companies:
        logging.info("Dropping existing company tables...")
        cursor.execute("DROP TABLE IF EXISTS company_property_values_old")
        cursor.execute("DROP TABLE IF EXISTS companies_old")
        cursor.execute("DROP TABLE IF EXISTS company_properties_def")
        conn.commit()
    
    if reset_associations:
        logging.info("Dropping existing association tables...")
        cursor.execute("DROP TABLE IF EXISTS contact_company_associations_old")
        conn.commit()
    
    # Create meta table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    
    # Create contact_properties_def table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact_properties_def (
            name TEXT PRIMARY KEY,
            label TEXT,
            type TEXT,
            fieldType TEXT,
            groupName TEXT,
            description TEXT,
            options_json TEXT,
            hidden INTEGER,
            formField INTEGER,
            createdAt TEXT,
            updatedAt TEXT,
            raw_json TEXT
        )
    """)
    
    # Create contacts_old table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contacts_old (
            id TEXT PRIMARY KEY,
            email TEXT,
            createdAt TEXT,
            updatedAt TEXT,
            archived INTEGER,
            properties_json TEXT,
            raw_json TEXT
        )
    """)
    
    # Create contact_property_values_old table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact_property_values_old (
            contact_id TEXT,
            name TEXT,
            value TEXT,
            PRIMARY KEY (contact_id, name),
            FOREIGN KEY (contact_id) REFERENCES contacts_old(id) ON DELETE CASCADE
        )
    """)
    
    # Create company_properties_def table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS company_properties_def (
            name TEXT PRIMARY KEY,
            label TEXT,
            type TEXT,
            fieldType TEXT,
            groupName TEXT,
            description TEXT,
            options_json TEXT,
            hidden INTEGER,
            formField INTEGER,
            createdAt TEXT,
            updatedAt TEXT,
            raw_json TEXT
        )
    """)
    
    # Create companies_old table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies_old (
            id TEXT PRIMARY KEY,
            name TEXT,
            domain TEXT,
            createdAt TEXT,
            updatedAt TEXT,
            archived INTEGER,
            properties_json TEXT,
            raw_json TEXT
        )
    """)
    
    # Create company_property_values_old table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS company_property_values_old (
            company_id TEXT,
            name TEXT,
            value TEXT,
            PRIMARY KEY (company_id, name),
            FOREIGN KEY (company_id) REFERENCES companies_old(id) ON DELETE CASCADE
        )
    """)
    
    # Create contact_company_associations_old table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact_company_associations_old (
            contact_id TEXT,
            company_id TEXT,
            association_types_json TEXT,
            PRIMARY KEY (contact_id, company_id),
            FOREIGN KEY (contact_id) REFERENCES contacts_old(id) ON DELETE CASCADE,
            FOREIGN KEY (company_id) REFERENCES companies_old(id) ON DELETE CASCADE
        )
    """)
    
    # Create indexes for contacts
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_contacts_old_email 
        ON contacts_old(email)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_contact_values_old_name 
        ON contact_property_values_old(name)
    """)
    
    # Create indexes for companies
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_companies_old_domain 
        ON companies_old(domain)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_values_old_name 
        ON company_property_values_old(name)
    """)
    
    # Create indexes for associations
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_assoc_company_id 
        ON contact_company_associations_old(company_id)
    """)
    
    # Create company_id_map table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS company_id_map (
            old_company_id TEXT PRIMARY KEY,
            old_domain TEXT,
            new_company_id TEXT,
            status TEXT,
            error TEXT,
            created_at TEXT
        )
    """)
    
    # Create indexes for company_id_map
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_id_map_new_id 
        ON company_id_map(new_company_id)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_id_map_domain 
        ON company_id_map(old_domain)
    """)
    
    conn.commit()
    conn.close()
    
    logging.info(f"Database created/verified at: {db_path}")


def fetch_contact_properties(token: str) -> List[Dict[str, Any]]:
    """
    Fetch all contact property definitions from HubSpot.
    
    Returns list of property definition dictionaries.
    """
    url = f"{BASE_URL}/crm/v3/properties/contacts"
    
    logging.info("Fetching contact property definitions...")
    response = make_request('GET', url, token)
    response.raise_for_status()
    
    data = response.json()
    properties = data.get('results', [])
    
    logging.info(f"Found {len(properties)} contact properties")
    return properties


def store_property_definitions(conn: sqlite3.Connection, properties: List[Dict[str, Any]]):
    """Store contact property definitions in database."""
    cursor = conn.cursor()
    
    for prop in properties:
        name = prop.get('name', '')
        label = prop.get('label', '')
        prop_type = prop.get('type', '')
        field_type = prop.get('fieldType', '')
        group_name = prop.get('groupName', '')
        description = prop.get('description', '')
        options = prop.get('options', [])
        hidden = 1 if prop.get('hidden', False) else 0
        form_field = 1 if prop.get('formField', False) else 0
        created_at = prop.get('createdAt', '')
        updated_at = prop.get('updatedAt', '')
        raw_json_str = json.dumps(prop, ensure_ascii=False)
        options_json_str = json.dumps(options, ensure_ascii=False) if options else None
        
        cursor.execute("""
            INSERT OR REPLACE INTO contact_properties_def
            (name, label, type, fieldType, groupName, description, options_json,
             hidden, formField, createdAt, updatedAt, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, label, prop_type, field_type, group_name, description,
              options_json_str, hidden, form_field, created_at, updated_at, raw_json_str))
    
    conn.commit()
    logging.info(f"Stored {len(properties)} property definitions")


def fetch_all_contact_ids(token: str, limit: Optional[int] = None) -> List[str]:
    """
    Fetch all contact IDs from HubSpot with pagination.
    
    Args:
        token: HubSpot access token
        limit: Optional maximum number of contacts to fetch
    
    Returns:
        List of contact IDs
    """
    url = f"{BASE_URL}/crm/v3/objects/contacts"
    contact_ids = []
    after = None
    page_num = 0
    
    logging.info("Fetching contact IDs...")
    
    while True:
        page_num += 1
        params = {'limit': 100}
        if after:
            params['after'] = after
        
        response = make_request('GET', url, token, params=params)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        for result in results:
            contact_id = result.get('id')
            if contact_id:
                contact_ids.append(str(contact_id))
        
        logging.info(f"Page {page_num}: Found {len(results)} contacts (total: {len(contact_ids)})")
        
        # Check limit
        if limit and len(contact_ids) >= limit:
            contact_ids = contact_ids[:limit]
            logging.info(f"Reached limit of {limit} contacts")
            break
        
        # Check for next page
        paging = data.get('paging', {})
        next_info = paging.get('next', {})
        next_after = next_info.get('after') if isinstance(next_info, dict) else None
        
        if not next_after:
            break
        
        after = next_after
    
    logging.info(f"Total contact IDs fetched: {len(contact_ids)}")
    return contact_ids


def batch_read_contacts(token: str, contact_ids: List[str], property_names: List[str]) -> List[Dict[str, Any]]:
    """
    Batch read contacts with all properties.
    
    Args:
        token: HubSpot access token
        contact_ids: List of contact IDs to fetch
        property_names: List of property names to include
    
    Returns:
        List of contact objects
    """
    url = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"
    all_contacts = []
    
    # Chunk into batches of 100
    chunk_size = 100
    total_chunks = (len(contact_ids) + chunk_size - 1) // chunk_size
    
    for i in range(0, len(contact_ids), chunk_size):
        chunk = contact_ids[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        
        payload = {
            "properties": property_names,
            "inputs": [{"id": cid} for cid in chunk]
        }
        
        logging.info(f"Batch reading contacts: chunk {chunk_num}/{total_chunks} ({len(chunk)} contacts)...")
        
        response = make_request('POST', url, token, json=payload)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        all_contacts.extend(results)
        
        logging.info(f"  Retrieved {len(results)} contacts from batch")
    
    return all_contacts


def store_contact(conn: sqlite3.Connection, contact: Dict[str, Any]):
    """
    Store a single contact and its property values in the database.
    
    Args:
        conn: Database connection
        contact: Contact object from HubSpot API
    """
    cursor = conn.cursor()
    
    contact_id = str(contact.get('id', ''))
    properties = contact.get('properties', {})
    
    # Extract email from properties
    email = properties.get('email', '')
    
    # Extract timestamps
    created_at = contact.get('createdAt', '')
    updated_at = contact.get('updatedAt', '')
    archived = 1 if contact.get('archived', False) else 0
    
    # Store JSON
    properties_json_str = json.dumps(properties, ensure_ascii=False)
    raw_json_str = json.dumps(contact, ensure_ascii=False)
    
    # Insert contact
    cursor.execute("""
        INSERT OR REPLACE INTO contacts_old
        (id, email, createdAt, updatedAt, archived, properties_json, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (contact_id, email, created_at, updated_at, archived, properties_json_str, raw_json_str))
    
    # Insert property values
    for prop_name, prop_value in properties.items():
        # Convert value to string (JSON-serialize if not string)
        if prop_value is None:
            value_str = None
        elif isinstance(prop_value, str):
            value_str = prop_value
        else:
            value_str = json.dumps(prop_value, ensure_ascii=False)
        
        cursor.execute("""
            INSERT OR REPLACE INTO contact_property_values_old
            (contact_id, name, value)
            VALUES (?, ?, ?)
        """, (contact_id, prop_name, value_str))


def load_property_definitions(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """
    Load all property definitions from database into a dict keyed by property name.
    
    Returns:
        Dict mapping property name -> property definition (parsed from raw_json)
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name, raw_json FROM contact_properties_def")
    
    prop_defs = {}
    for row in cursor.fetchall():
        name, raw_json_str = row
        if raw_json_str:
            try:
                prop_defs[name] = json.loads(raw_json_str)
            except json.JSONDecodeError:
                # Skip invalid JSON
                continue
    
    return prop_defs


def lookup_contact_by_email(conn: sqlite3.Connection, email: str) -> Optional[tuple]:
    """
    Lookup contact by email (case-insensitive).
    
    Returns:
        Tuple of (contact_id, email, properties_json) or None if not found
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, email, properties_json FROM contacts_old WHERE LOWER(email)=LOWER(?)",
        (email,)
    )
    results = cursor.fetchall()
    
    if len(results) == 0:
        return None
    if len(results) > 1:
        # Multiple matches - return None to signal ambiguity
        return None
    
    return results[0]


def lookup_contact_by_id(conn: sqlite3.Connection, contact_id: str) -> Optional[tuple]:
    """
    Lookup contact by ID.
    
    Returns:
        Tuple of (contact_id, email, properties_json) or None if not found
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, email, properties_json FROM contacts_old WHERE id=?",
        (contact_id,)
    )
    result = cursor.fetchone()
    return result


def is_value_empty(value: Any) -> bool:
    """
    Check if a property value is considered empty.
    
    Excludes:
    - NULL/None
    - Empty string after trim
    - Literal "null" (case-insensitive)
    """
    if value is None:
        return True
    
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed:
            return True
        if trimmed.lower() == 'null':
            return True
    
    return False


def should_exclude_property(prop_name: str, prop_value: Any, prop_def: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Determine if a property should be excluded from migration.
    
    Returns:
        Tuple of (should_exclude: bool, reason: str)
    """
    # Check if value is empty (always exclude empty values, even allowlisted ones)
    if is_value_empty(prop_value):
        return (True, 'empty_value')
    
    # Allowlist override: if property is in allowlist and has non-empty value, include it
    # This overrides other exclusions (except empty value check above)
    if prop_name.lower() in ALLOWLISTED_PROPERTY_NAMES:
        return (False, '')
    
    # Check if name starts with system prefixes
    if prop_name.startswith('hs_') or prop_name.startswith('ip_'):
        return (True, 'system_hs_prefix' if prop_name.startswith('hs_') else 'system_ip_prefix')
    
    # Check blacklist
    if prop_name.lower() in BLACKLISTED_PROPERTY_NAMES:
        return (True, 'blacklisted_name')
    
    # Check property definition if available
    if prop_def:
        # Check modificationMetadata.readOnlyValue
        mod_meta = prop_def.get('modificationMetadata', {})
        if mod_meta.get('readOnlyValue') is True:
            return (True, 'read_only')
        
        # Check calculated
        if prop_def.get('calculated') is True:
            return (True, 'calculated')
    
    # Include this property
    return (False, '')


def filter_contact_properties(properties: Dict[str, Any], prop_defs: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, str], List[Tuple[str, str]]]:
    """
    Filter contact properties to only include those we will push to NEW portal.
    
    Args:
        properties: Contact properties dict from contacts_old.properties_json
        prop_defs: Property definitions dict keyed by property name
    
    Returns:
        Tuple of (included_properties: dict, excluded_list: list of (name, reason) tuples)
    """
    included = {}
    excluded = []
    
    # Sort property names for stable output
    sorted_prop_names = sorted(properties.keys())
    
    for prop_name in sorted_prop_names:
        prop_value = properties.get(prop_name)
        prop_def = prop_defs.get(prop_name)
        
        should_exclude, reason = should_exclude_property(prop_name, prop_value, prop_def)
        
        if should_exclude:
            excluded.append((prop_name, reason))
        else:
            # Include the property
            # Keep values as strings
            # If value is already a string, use it as-is (even if it looks like JSON)
            # If value is a complex type (dict/list), JSON-serialize it
            if isinstance(prop_value, str):
                included[prop_name] = prop_value
            elif isinstance(prop_value, (dict, list)):
                # Complex types: JSON-serialize to string
                included[prop_name] = json.dumps(prop_value, ensure_ascii=False)
            else:
                # Other types (numbers, booleans, etc.): convert to string
                included[prop_name] = str(prop_value) if prop_value is not None else ''
    
    return included, excluded


def fetch_company_properties(token: str) -> List[Dict[str, Any]]:
    """
    Fetch all company property definitions from HubSpot.
    
    Returns list of property definition dictionaries.
    """
    url = f"{BASE_URL}/crm/v3/properties/companies"
    
    logging.info("Fetching company property definitions...")
    response = make_request('GET', url, token)
    response.raise_for_status()
    
    data = response.json()
    properties = data.get('results', [])
    
    logging.info(f"Found {len(properties)} company properties")
    return properties


def store_company_property_definitions(conn: sqlite3.Connection, properties: List[Dict[str, Any]]):
    """Store company property definitions in database."""
    cursor = conn.cursor()
    
    for prop in properties:
        name = prop.get('name', '')
        label = prop.get('label', '')
        prop_type = prop.get('type', '')
        field_type = prop.get('fieldType', '')
        group_name = prop.get('groupName', '')
        description = prop.get('description', '')
        options = prop.get('options', [])
        hidden = 1 if prop.get('hidden', False) else 0
        form_field = 1 if prop.get('formField', False) else 0
        created_at = prop.get('createdAt', '')
        updated_at = prop.get('updatedAt', '')
        raw_json_str = json.dumps(prop, ensure_ascii=False)
        options_json_str = json.dumps(options, ensure_ascii=False) if options else None
        
        cursor.execute("""
            INSERT OR REPLACE INTO company_properties_def
            (name, label, type, fieldType, groupName, description, options_json,
             hidden, formField, createdAt, updatedAt, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, label, prop_type, field_type, group_name, description,
              options_json_str, hidden, form_field, created_at, updated_at, raw_json_str))
    
    conn.commit()
    logging.info(f"Stored {len(properties)} company property definitions")


def fetch_all_company_ids(token: str, limit: Optional[int] = None) -> List[str]:
    """
    Fetch all company IDs from HubSpot with pagination.
    
    Args:
        token: HubSpot access token
        limit: Optional maximum number of companies to fetch
    
    Returns:
        List of company IDs
    """
    url = f"{BASE_URL}/crm/v3/objects/companies"
    company_ids = []
    after = None
    page_num = 0
    
    logging.info("Fetching company IDs...")
    
    while True:
        page_num += 1
        params = {'limit': 100}
        if after:
            params['after'] = after
        
        response = make_request('GET', url, token, params=params)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        for result in results:
            company_id = result.get('id')
            if company_id:
                company_ids.append(str(company_id))
        
        logging.info(f"Page {page_num}: Found {len(results)} companies (total: {len(company_ids)})")
        
        # Check limit
        if limit and len(company_ids) >= limit:
            company_ids = company_ids[:limit]
            logging.info(f"Reached limit of {limit} companies")
            break
        
        # Check for next page
        paging = data.get('paging', {})
        next_info = paging.get('next', {})
        next_after = next_info.get('after') if isinstance(next_info, dict) else None
        
        if not next_after:
            break
        
        after = next_after
    
    logging.info(f"Total company IDs fetched: {len(company_ids)}")
    return company_ids


def batch_read_companies(token: str, company_ids: List[str], property_names: List[str]) -> List[Dict[str, Any]]:
    """
    Batch read companies with all properties.
    
    Args:
        token: HubSpot access token
        company_ids: List of company IDs to fetch
        property_names: List of property names to include
    
    Returns:
        List of company objects
    """
    url = f"{BASE_URL}/crm/v3/objects/companies/batch/read"
    all_companies = []
    
    # Chunk into batches of 100
    chunk_size = 100
    total_chunks = (len(company_ids) + chunk_size - 1) // chunk_size
    
    for i in range(0, len(company_ids), chunk_size):
        chunk = company_ids[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        
        payload = {
            "properties": property_names,
            "inputs": [{"id": cid} for cid in chunk]
        }
        
        logging.info(f"Batch reading companies: chunk {chunk_num}/{total_chunks} ({len(chunk)} companies)...")
        
        response = make_request('POST', url, token, json=payload)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        all_companies.extend(results)
        
        logging.info(f"  Retrieved {len(results)} companies from batch")
    
    return all_companies


def store_company(conn: sqlite3.Connection, company: Dict[str, Any]):
    """
    Store a single company and its property values in the database.
    
    Args:
        conn: Database connection
        company: Company object from HubSpot API
    """
    cursor = conn.cursor()
    
    company_id = str(company.get('id', ''))
    properties = company.get('properties', {})
    
    # Extract name and domain from properties
    name = properties.get('name', '')
    domain = properties.get('domain', '')
    
    # Extract timestamps
    created_at = company.get('createdAt', '')
    updated_at = company.get('updatedAt', '')
    archived = 1 if company.get('archived', False) else 0
    
    # Store JSON
    properties_json_str = json.dumps(properties, ensure_ascii=False)
    raw_json_str = json.dumps(company, ensure_ascii=False)
    
    # Insert company
    cursor.execute("""
        INSERT OR REPLACE INTO companies_old
        (id, name, domain, createdAt, updatedAt, archived, properties_json, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (company_id, name, domain, created_at, updated_at, archived, properties_json_str, raw_json_str))
    
    # Insert property values
    for prop_name, prop_value in properties.items():
        # Convert value to string (JSON-serialize if not string)
        if prop_value is None:
            value_str = None
        elif isinstance(prop_value, str):
            value_str = prop_value
        else:
            value_str = json.dumps(prop_value, ensure_ascii=False)
        
        cursor.execute("""
            INSERT OR REPLACE INTO company_property_values_old
            (company_id, name, value)
            VALUES (?, ?, ?)
        """, (company_id, prop_name, value_str))


def fetch_contact_company_associations(token: str, contact_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Batch fetch contact-company associations from HubSpot.
    
    Args:
        token: HubSpot access token
        contact_ids: List of contact IDs to fetch associations for
    
    Returns:
        List of association results from API
    """
    url = f"{BASE_URL}/crm/v4/associations/contacts/companies/batch/read"
    all_results = []
    
    # Chunk into batches of 1000
    chunk_size = 1000
    total_chunks = (len(contact_ids) + chunk_size - 1) // chunk_size
    
    for i in range(0, len(contact_ids), chunk_size):
        chunk = contact_ids[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        
        payload = {
            "inputs": [{"id": cid} for cid in chunk]
        }
        
        logging.info(f"Batch reading associations: chunk {chunk_num}/{total_chunks} ({len(chunk)} contacts)...")
        
        response = make_request('POST', url, token, json=payload)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        all_results.extend(results)
        
        logging.info(f"  Retrieved associations for {len(results)} contacts from batch")
    
    return all_results


def store_associations(conn: sqlite3.Connection, association_results: List[Dict[str, Any]]):
    """
    Store contact-company associations in database.
    
    Args:
        conn: Database connection
        association_results: List of association results from API
    """
    cursor = conn.cursor()
    
    stored_count = 0
    
    for result in association_results:
        from_obj = result.get('from', {})
        contact_id = str(from_obj.get('id', ''))
        
        if not contact_id:
            continue
        
        to_records = result.get('to', [])
        
        for to_record in to_records:
            company_id = str(to_record.get('toObjectId', ''))
            association_types = to_record.get('associationTypes', [])
            
            if not company_id:
                continue
            
            association_types_json = json.dumps(association_types, ensure_ascii=False)
            
            cursor.execute("""
                INSERT OR REPLACE INTO contact_company_associations_old
                (contact_id, company_id, association_types_json)
                VALUES (?, ?, ?)
            """, (contact_id, company_id, association_types_json))
            stored_count += 1
    
    conn.commit()
    logging.info(f"Stored {stored_count} associations")


def normalize_domain(domain: str) -> str:
    """
    Normalize domain for matching.
    
    Rules:
    - lowercase, trim
    - strip leading www.
    - remove trailing dot
    """
    if not domain:
        return ''
    
    normalized = domain.lower().strip()
    
    # Strip leading www.
    if normalized.startswith('www.'):
        normalized = normalized[4:]
    
    # Remove trailing dot
    if normalized.endswith('.'):
        normalized = normalized[:-1]
    
    return normalized


def normalize_name(name: str) -> str:
    """
    Normalize company name for matching.
    
    Rules:
    - lowercase, trim
    - collapse multiple spaces
    - remove trailing punctuation (commas/periods)
    """
    if not name:
        return ''
    
    normalized = name.lower().strip()
    
    # Collapse multiple spaces
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Remove trailing punctuation (commas, periods)
    normalized = re.sub(r'[.,]+$', '', normalized)
    
    return normalized.strip()


def build_new_company_cache(token: str, max_pages: Optional[int] = None) -> Tuple[Dict[str, List[Tuple[str, str, str]]], Dict[str, List[Tuple[str, str, str]]], int]:
    """
    Build in-memory cache of NEW portal companies.
    
    Args:
        token: HubSpot access token for NEW portal
        max_pages: Optional limit on number of pages to read
    
    Returns:
        Tuple of (new_by_domain dict, new_by_name dict, total_scanned count)
        Each dict maps normalized key -> list of (company_id, name, domain)
    """
    url = f"{BASE_URL}/crm/v3/objects/companies"
    new_by_domain: Dict[str, List[Tuple[str, str, str]]] = {}
    new_by_name: Dict[str, List[Tuple[str, str, str]]] = {}
    
    after = None
    page_num = 0
    total_scanned = 0
    
    logging.info("Building NEW company cache...")
    
    while True:
        page_num += 1
        
        # Check max_pages limit
        if max_pages and page_num > max_pages:
            logging.info(f"Reached max_pages limit ({max_pages})")
            break
        
        params = {
            'limit': 100,
            'properties': 'domain,name'
        }
        if after:
            params['after'] = after
        
        try:
            response = make_request('GET', url, token, params=params)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print("ERROR: Access forbidden (403). Missing required scope: crm.objects.companies.read", file=sys.stderr)
                print("Please ensure your ACCESS_TOKEN has the 'crm.objects.companies.read' scope.", file=sys.stderr)
                sys.exit(1)
            raise
        
        data = response.json()
        results = data.get('results', [])
        
        for result in results:
            # Skip archived companies
            if result.get('archived', False):
                continue
            
            company_id = str(result.get('id', ''))
            properties = result.get('properties', {})
            domain = properties.get('domain', '')
            name = properties.get('name', '')
            
            # Index by domain
            if domain:
                norm_domain = normalize_domain(domain)
                if norm_domain:
                    if norm_domain not in new_by_domain:
                        new_by_domain[norm_domain] = []
                    new_by_domain[norm_domain].append((company_id, name, domain))
            
            # Index by name
            if name:
                norm_name = normalize_name(name)
                if norm_name:
                    if norm_name not in new_by_name:
                        new_by_name[norm_name] = []
                    new_by_name[norm_name].append((company_id, name, domain))
            
            total_scanned += 1
        
        # Log progress every 10 pages
        if page_num % 10 == 0:
            logging.info(f"NEW cache: Page {page_num}, companies scanned: {total_scanned}")
        
        # Check for next page
        paging = data.get('paging', {})
        next_info = paging.get('next', {})
        next_after = next_info.get('after') if isinstance(next_info, dict) else None
        
        if not next_after:
            break
        
        after = next_after
    
    logging.info(f"NEW cache complete: {total_scanned} companies scanned, {len(new_by_domain)} unique domains, {len(new_by_name)} unique names")
    return new_by_domain, new_by_name, total_scanned


def run_plan_companies(db_path: str, export_path: Optional[str], include_name_fallback: bool, max_old: Optional[int], max_new_pages: Optional[int]):
    """
    Plan company migration by identifying which OLD companies are missing in NEW portal.
    
    Args:
        db_path: Path to SQLite database (required)
        export_path: Optional path to write CSV export
        include_name_fallback: If True, attempt name-based matching when domain missing
        max_old: Optional limit on OLD companies processed
        max_new_pages: Optional limit on NEW portal pages read
    """
    # Load NEW token (OLD comes from DB)
    new_token = load_new_token()
    
    # Check database exists
    if not Path(db_path).exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        print("Run --init-companies first to load OLD companies.", file=sys.stderr)
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Check companies_old table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='companies_old'")
        if not cursor.fetchone():
            print("ERROR: companies_old table not found.", file=sys.stderr)
            print("Run --init-companies first to load OLD companies.", file=sys.stderr)
            sys.exit(1)
        
        # Build NEW company cache
        logging.info("Fetching NEW portal companies...")
        new_by_domain, new_by_name, total_new_scanned = build_new_company_cache(new_token, max_pages=max_new_pages)
        
        # Read OLD companies from database
        logging.info("Reading OLD companies from database...")
        query = "SELECT id, domain, name FROM companies_old"
        if max_old:
            query += f" LIMIT {max_old}"
        
        cursor.execute(query)
        old_companies = cursor.fetchall()
        
        logging.info(f"Processing {len(old_companies)} OLD companies...")
        
        # Match OLD to NEW
        results = []
        outcome_counts = {
            'exact_match': 0,
            'ambiguous_domain': 0,
            'missing_in_new': 0,
            'fallback_name_match': 0,
            'ambiguous_name': 0,
            'missing_in_new_no_domain': 0
        }
        
        for old_id, old_domain, old_name in old_companies:
            old_domain = old_domain or ''
            old_name = old_name or ''
            
            status = None
            matched_new_ids = []
            matched_new_domains = []
            matched_new_names = []
            
            # Try domain match first
            if old_domain:
                norm_domain = normalize_domain(old_domain)
                if norm_domain and norm_domain in new_by_domain:
                    matches = new_by_domain[norm_domain]
                    if len(matches) == 1:
                        # Exact match
                        status = 'exact_match'
                        outcome_counts['exact_match'] += 1
                        matched_new_ids = [matches[0][0]]
                        matched_new_domains = [matches[0][2]]
                        matched_new_names = [matches[0][1]]
                    else:
                        # Ambiguous
                        status = 'ambiguous_domain'
                        outcome_counts['ambiguous_domain'] += 1
                        matched_new_ids = [m[0] for m in matches]
                        matched_new_domains = [m[2] for m in matches]
                        matched_new_names = [m[1] for m in matches]
                else:
                    # No domain match
                    status = 'missing_in_new'
                    outcome_counts['missing_in_new'] += 1
            else:
                # No domain, try name fallback if enabled
                if include_name_fallback and old_name:
                    norm_name = normalize_name(old_name)
                    if norm_name and norm_name in new_by_name:
                        matches = new_by_name[norm_name]
                        if len(matches) == 1:
                            status = 'fallback_name_match'
                            outcome_counts['fallback_name_match'] += 1
                            matched_new_ids = [matches[0][0]]
                            matched_new_domains = [matches[0][2]]
                            matched_new_names = [matches[0][1]]
                        else:
                            status = 'ambiguous_name'
                            outcome_counts['ambiguous_name'] += 1
                            matched_new_ids = [m[0] for m in matches]
                            matched_new_domains = [m[2] for m in matches]
                            matched_new_names = [m[1] for m in matches]
                    else:
                        status = 'missing_in_new_no_domain'
                        outcome_counts['missing_in_new_no_domain'] += 1
                else:
                    status = 'missing_in_new_no_domain'
                    outcome_counts['missing_in_new_no_domain'] += 1
            
            results.append({
                'old_company_id': old_id,
                'old_domain': old_domain,
                'old_name': old_name,
                'status': status,
                'matched_new_company_ids': matched_new_ids,
                'matched_new_domains': matched_new_domains,
                'matched_new_names': matched_new_names
            })
        
        # Print summary
        
        print("\n" + "=" * 80)
        print("COMPANY MIGRATION PLAN SUMMARY")
        print("=" * 80)
        print(f"total_old_companies_processed: {len(old_companies)}")
        print(f"new_companies_scanned: {total_new_scanned}")
        print()
        print("Outcome counts:")
        print(f"  exact_match: {outcome_counts['exact_match']}")
        print(f"  ambiguous_domain: {outcome_counts['ambiguous_domain']}")
        print(f"  missing_in_new: {outcome_counts['missing_in_new']}")
        if include_name_fallback:
            print(f"  fallback_name_match: {outcome_counts['fallback_name_match']}")
            print(f"  ambiguous_name: {outcome_counts['ambiguous_name']}")
        print(f"  missing_in_new_no_domain: {outcome_counts['missing_in_new_no_domain']}")
        print("=" * 80)
        
        # Print samples
        missing_samples = [r for r in results if r['status'] == 'missing_in_new'][:20]
        ambiguous_domain_samples = [r for r in results if r['status'] == 'ambiguous_domain'][:20]
        missing_no_domain_samples = [r for r in results if r['status'] == 'missing_in_new_no_domain'][:20]
        
        if missing_samples:
            print("\nMissing in NEW (first 20):")
            for r in missing_samples:
                print(f"  ID: {r['old_company_id']}, Domain: {r['old_domain']}, Name: {r['old_name']}")
        
        if ambiguous_domain_samples:
            print("\nAmbiguous domain matches (first 20):")
            for r in ambiguous_domain_samples:
                candidate_ids = ';'.join(r['matched_new_company_ids'])
                print(f"  ID: {r['old_company_id']}, Domain: {r['old_domain']}, Name: {r['old_name']}, Candidates: {candidate_ids}")
        
        if missing_no_domain_samples:
            print("\nMissing in NEW (no domain, first 20):")
            for r in missing_no_domain_samples:
                print(f"  ID: {r['old_company_id']}, Name: {r['old_name']}")
        
        # Export to CSV if requested
        if export_path:
            export_dir = Path(export_path).parent
            export_dir.mkdir(parents=True, exist_ok=True)
            
            with open(export_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'old_company_id',
                    'old_domain',
                    'old_name',
                    'status',
                    'matched_new_company_ids',
                    'matched_new_domains',
                    'matched_new_names'
                ])
                
                for r in results:
                    writer.writerow([
                        r['old_company_id'],
                        r['old_domain'],
                        r['old_name'],
                        r['status'],
                        ';'.join(r['matched_new_company_ids']),
                        ';'.join(r['matched_new_domains']),
                        ';'.join(r['matched_new_names'])
                    ])
            
            print(f"\nExported results to: {export_path}")
        
    finally:
        conn.close()


def count_companies_in_portal(token: str, portal_name: str) -> int:
    """
    Count all companies in a portal using pagination.
    
    Args:
        token: HubSpot access token
        portal_name: Name of portal for logging (e.g., "OLD" or "NEW")
    
    Returns:
        Total count of companies
    """
    url = f"{BASE_URL}/crm/v3/objects/companies"
    total_count = 0
    after = None
    page_num = 0
    
    logging.info(f"Counting companies in {portal_name} portal...")
    
    while True:
        page_num += 1
        params = {'limit': 100}
        if after:
            params['after'] = after
        
        response = make_request('GET', url, token, params=params)
        response.raise_for_status()
        
        data = response.json()
        results = data.get('results', [])
        
        # Count non-archived companies
        page_count = 0
        for result in results:
            if not result.get('archived', False):
                page_count += 1
        
        total_count += page_count
        
        # Log progress every 10 pages
        if page_num % 10 == 0:
            logging.info(f"{portal_name} portal: Page {page_num}, running total: {total_count} companies")
        
        # Check for next page
        paging = data.get('paging', {})
        next_info = paging.get('next', {})
        next_after = next_info.get('after') if isinstance(next_info, dict) else None
        
        if not next_after:
            break
        
        after = next_after
    
    logging.info(f"{portal_name} portal: Total companies counted: {total_count}")
    return total_count


def run_count_companies(db_path: Optional[str]):
    """
    Count companies in both OLD and NEW portals and print comparison.
    
    Args:
        db_path: Optional path to SQLite database (for local count context)
    """
    # Load both tokens
    old_token = load_token()
    new_token = load_new_token()
    
    # Count companies in OLD portal
    logging.info("Counting companies in OLD portal...")
    old_count = count_companies_in_portal(old_token, "OLD")
    
    # Count companies in NEW portal
    logging.info("Counting companies in NEW portal...")
    new_count = count_companies_in_portal(new_token, "NEW")
    
    # Calculate difference
    difference = old_count - new_count
    
    # Print summary
    print("\n" + "=" * 80)
    print("COMPANY COUNT SUMMARY")
    print("=" * 80)
    print(f"OLD portal companies: {old_count}")
    print(f"NEW portal companies: {new_count}")
    print(f"Difference (OLD - NEW): {difference}")
    print("=" * 80)
    
    # Optionally show local DB count if --db provided
    if db_path and Path(db_path).exists():
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Check if companies_old table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='companies_old'")
            if cursor.fetchone():
                cursor.execute("SELECT COUNT(*) FROM companies_old")
                local_count = cursor.fetchone()[0]
                
                print(f"\nlocal_db_companies_old: {local_count} from SQLite")
                
                if local_count != old_count:
                    print(f"WARNING: Local DB count ({local_count}) differs from OLD portal API count ({old_count})")
                    print("This may indicate a partial extract or data changes since extraction.")
            
            conn.close()
        except Exception as e:
            logging.warning(f"Failed to read local DB count: {e}")


def run_init_companies(db_path: str, reset: bool, limit: Optional[int]):
    """
    Run the --init-companies phase: extract companies from OLD account into SQLite.
    
    Args:
        db_path: Path to SQLite database
        reset: If True, clear company tables before repopulating
        limit: Optional maximum number of companies to fetch
    """
    token = load_token()
    
    # Create/verify database
    create_database(db_path, reset_companies=reset)
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    
    try:
        # Step 1: Fetch and store property definitions
        properties = fetch_company_properties(token)
        store_company_property_definitions(conn, properties)
        
        # Get property names for batch read
        property_names = [prop.get('name') for prop in properties if prop.get('name')]
        logging.info(f"Will fetch {len(property_names)} properties per company")
        
        # Step 2: Fetch all company IDs
        company_ids = fetch_all_company_ids(token, limit=limit)
        
        if not company_ids:
            logging.warning("No companies found")
            return
        
        # Step 3: Batch read companies with all properties
        logging.info("Fetching full company objects...")
        companies = batch_read_companies(token, company_ids, property_names)
        
        # Step 4: Store companies in database
        logging.info(f"Storing {len(companies)} companies in database...")
        stored_count = 0
        property_value_rows = 0
        start_time = time.time()
        
        # Use transactions: commit every 100 companies
        commit_interval = 100
        
        for idx, company in enumerate(companies, 1):
            store_company(conn, company)
            stored_count += 1
            
            # Count property values for this company
            company_properties = company.get('properties', {})
            property_value_rows += len(company_properties)
            
            # Commit periodically
            if idx % commit_interval == 0:
                conn.commit()
                elapsed = time.time() - start_time
                rate = stored_count / elapsed if elapsed > 0 else 0
                logging.info(f"Progress: {stored_count}/{len(companies)} companies stored ({rate:.1f} companies/sec)")
        
        # Final commit
        conn.commit()
        
        # Step 5: Update meta table
        cursor = conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()
        cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ('old_companies_loaded_at', now_iso))
        conn.commit()
        
        # Step 6: Print summary
        elapsed_total = time.time() - start_time
        rate_total = stored_count / elapsed_total if elapsed_total > 0 else 0
        
        print("\n" + "=" * 80)
        print("COMPANIES MIGRATION SUMMARY")
        print("=" * 80)
        print(f"Total properties saved: {len(properties)}")
        print(f"Total companies saved: {stored_count}")
        print(f"Total property-value rows saved: {property_value_rows}")
        print(f"Database path: {Path(db_path).absolute()}")
        print(f"Processing rate: {rate_total:.1f} companies/sec")
        print("=" * 80)
        
    finally:
        conn.close()


def run_init_associations(db_path: str, reset: bool):
    """
    Run the --init-associations phase: extract contact-company associations from OLD account into SQLite.
    
    Args:
        db_path: Path to SQLite database
        reset: If True, clear association tables before repopulating
    """
    token = load_token()
    
    # Check if contacts exist
    if not Path(db_path).exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    
    try:
        # Check if contacts_old table exists and has data
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM contacts_old")
        contact_count = cursor.fetchone()[0]
        
        if contact_count == 0:
            print("ERROR: No contacts found in database. Run --init first to load contacts.", file=sys.stderr)
            sys.exit(1)
        
        # Check if companies_old table exists (for FK constraint)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='companies_old'")
        if cursor.fetchone() is None:
            print("ERROR: companies_old table not found. Run --init-companies first to load companies.", file=sys.stderr)
            sys.exit(1)
        
        # Create/verify database schema
        create_database(db_path, reset_associations=reset)
        
        # Fetch all contact IDs
        cursor.execute("SELECT id FROM contacts_old")
        contact_ids = [row[0] for row in cursor.fetchall()]
        
        logging.info(f"Fetching associations for {len(contact_ids)} contacts...")
        
        # Fetch associations in batches
        association_results = fetch_contact_company_associations(token, contact_ids)
        
        # Store associations
        logging.info("Storing associations in database...")
        store_associations(conn, association_results)
        
        # Update meta table
        now_iso = datetime.now(timezone.utc).isoformat()
        cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ('old_contact_company_associations_loaded_at', now_iso))
        conn.commit()
        
        # Print summary
        cursor.execute("SELECT COUNT(*) FROM contact_company_associations_old")
        association_count = cursor.fetchone()[0]
        
        print("\n" + "=" * 80)
        print("ASSOCIATIONS MIGRATION SUMMARY")
        print("=" * 80)
        print(f"Contacts processed: {len(contact_ids)}")
        print(f"Total associations saved: {association_count}")
        print(f"Database path: {Path(db_path).absolute()}")
        print("=" * 80)
        
    finally:
        conn.close()


def load_company_property_definitions(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """
    Load all company property definitions from database into a dict keyed by property name.
    
    Returns:
        Dict mapping property name -> property definition (parsed from raw_json)
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name, raw_json FROM company_properties_def")
    
    prop_defs = {}
    for row in cursor.fetchall():
        name, raw_json_str = row
        if raw_json_str:
            try:
                prop_defs[name] = json.loads(raw_json_str)
            except json.JSONDecodeError:
                # Skip invalid JSON
                continue
    
    return prop_defs


def lookup_company_by_id(conn: sqlite3.Connection, company_id: str) -> Optional[tuple]:
    """
    Lookup company by ID.
    
    Returns:
        Tuple of (id, name, domain, properties_json) or None if not found
    """
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, name, domain, properties_json FROM companies_old WHERE id=?",
        (company_id,)
    )
    result = cursor.fetchone()
    return result


def lookup_company_by_domain(conn: sqlite3.Connection, domain: str) -> Optional[List[tuple]]:
    """
    Lookup companies by domain (normalized comparison).
    
    Args:
        conn: Database connection
        domain: Domain to search for (will be normalized)
    
    Returns:
        List of tuples (id, name, domain, properties_json) or None if not found
        Returns list to handle multiple matches
    """
    # Normalize the input domain
    norm_domain = normalize_domain(domain)
    
    cursor = conn.cursor()
    # Fetch all companies and normalize in Python for comparison
    cursor.execute("SELECT id, name, domain, properties_json FROM companies_old")
    all_companies = cursor.fetchall()
    
    matches = []
    for row in all_companies:
        company_id, name, db_domain, properties_json = row
        if db_domain:
            db_norm_domain = normalize_domain(db_domain)
            if db_norm_domain == norm_domain:
                matches.append(row)
    
    if len(matches) == 0:
        return None
    
    return matches


def should_exclude_company_property(prop_name: str, prop_value: Any, prop_def: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Determine if a company property should be excluded from migration.
    
    Returns:
        Tuple of (should_exclude: bool, reason: str)
    """
    # Check if value is empty (always exclude empty values, even allowlisted ones)
    if is_value_empty(prop_value):
        return (True, 'empty_value')
    
    # Allowlist override: if property is in allowlist and has non-empty value, include it
    # This overrides other exclusions (except empty value check above)
    if prop_name.lower() in ALLOWLISTED_COMPANY_PROPERTY_NAMES:
        return (False, '')
    
    # Check if name starts with system prefixes
    if prop_name.startswith('hs_') or prop_name.startswith('ip_'):
        return (True, 'system_hs_prefix' if prop_name.startswith('hs_') else 'system_ip_prefix')
    
    # Check blacklist
    if prop_name.lower() in BLACKLISTED_COMPANY_PROPERTY_NAMES:
        return (True, 'blacklisted_name')
    
    # Check property definition if available
    if prop_def:
        # Check modificationMetadata.readOnlyValue
        mod_meta = prop_def.get('modificationMetadata', {})
        if mod_meta.get('readOnlyValue') is True:
            return (True, 'read_only')
        
        # Check calculated
        if prop_def.get('calculated') is True:
            return (True, 'calculated')
    
    # Include this property
    return (False, '')


def filter_company_properties(properties: Dict[str, Any], prop_defs: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, str], List[Tuple[str, str]]]:
    """
    Filter company properties to only include those we will push to NEW portal.
    
    Args:
        properties: Company properties dict from companies_old.properties_json
        prop_defs: Property definitions dict keyed by property name
    
    Returns:
        Tuple of (included_properties: dict, excluded_list: list of (name, reason) tuples)
    """
    included = {}
    excluded = []
    
    # Sort property names for stable output
    sorted_prop_names = sorted(properties.keys())
    
    for prop_name in sorted_prop_names:
        prop_value = properties.get(prop_name)
        prop_def = prop_defs.get(prop_name)
        
        should_exclude, reason = should_exclude_company_property(prop_name, prop_value, prop_def)
        
        if should_exclude:
            excluded.append((prop_name, reason))
        else:
            # Include the property
            # Keep values as strings
            # If value is already a string, use it as-is (even if it looks like JSON)
            # If value is a complex type (dict/list), JSON-serialize it
            if isinstance(prop_value, str):
                value_to_include = prop_value
                
                # Normalize website property: add https:// prefix if missing and looks like a domain
                if prop_name == 'website' and value_to_include:
                    normalized_website = value_to_include.strip()
                    if normalized_website and not normalized_website.startswith('http://') and not normalized_website.startswith('https://'):
                        # Check if it looks like a domain (has at least one dot and no spaces)
                        if '.' in normalized_website and ' ' not in normalized_website:
                            value_to_include = f'https://{normalized_website}'
                
                included[prop_name] = value_to_include
            elif isinstance(prop_value, (dict, list)):
                # Complex types: JSON-serialize to string
                included[prop_name] = json.dumps(prop_value, ensure_ascii=False)
            else:
                # Other types (numbers, booleans, etc.): convert to string
                included[prop_name] = str(prop_value) if prop_value is not None else ''
    
    return included, excluded


def run_printone_company(db_path: str, company_domain: Optional[str], company_id: Optional[str]):
    """
    Print filtered property payload for one company.
    
    Args:
        db_path: Path to SQLite database
        company_domain: Company domain (optional)
        company_id: Company ID (optional)
    """
    # Validate inputs
    if not company_domain and not company_id:
        print("ERROR: --printone-company requires either --company-domain or --company-id", file=sys.stderr)
        sys.exit(1)
    
    if company_domain and company_id:
        print("ERROR: --printone-company requires either --company-domain or --company-id, not both", file=sys.stderr)
        sys.exit(1)
    
    # Connect to database
    if not Path(db_path).exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Lookup company
        if company_domain:
            matches = lookup_company_by_domain(conn, company_domain)
            if matches is None:
                print(f"ERROR: Company not found with domain '{company_domain}'", file=sys.stderr)
                sys.exit(1)
            if len(matches) > 1:
                print(f"ERROR: Multiple companies found with domain '{company_domain}':", file=sys.stderr)
                for match in matches:
                    print(f"  ID: {match[0]}, Domain: {match[2]}, Name: {match[1]}", file=sys.stderr)
                sys.exit(1)
            result = matches[0]
        else:
            result = lookup_company_by_id(conn, company_id)
            if result is None:
                print(f"ERROR: Company not found with ID '{company_id}'", file=sys.stderr)
                sys.exit(1)
        
        company_id_found, company_name, company_domain_found, properties_json_str = result
        
        # Parse properties
        try:
            properties = json.loads(properties_json_str)
        except json.JSONDecodeError:
            print(f"ERROR: Failed to parse properties_json for company {company_id_found}", file=sys.stderr)
            sys.exit(1)
        
        # Load property definitions
        prop_defs = load_company_property_definitions(conn)
        
        # Filter properties
        included_props, excluded_list = filter_company_properties(properties, prop_defs)
        
        # Print output
        print(f"COMPANY_DOMAIN: {company_domain_found or ''} COMPANY_ID: {company_id_found} COMPANY_NAME: {company_name or ''}")
        print()
        
        # Print JSON payload
        payload = {"properties": included_props}
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print()
        
        # Print summary
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"included_properties_count: {len(included_props)}")
        print(f"excluded_properties_count: {len(excluded_list)}")
        print()
        
        # Print first ~30 excluded properties
        print("Excluded properties (first 30):")
        for prop_name, reason in excluded_list[:30]:
            print(f"  - {prop_name}: {reason}")
        
        if len(excluded_list) > 30:
            print(f"  ... and {len(excluded_list) - 30} more")
        
    finally:
        conn.close()


def build_new_company_domain_cache(token: str) -> Dict[str, str]:
    """
    Build a simple cache mapping normalized domain -> NEW company ID.
    
    Args:
        token: HubSpot access token for NEW portal
    
    Returns:
        Dict mapping normalized domain -> company_id
    """
    url = f"{BASE_URL}/crm/v3/objects/companies"
    domain_to_id: Dict[str, str] = {}
    
    after = None
    page_num = 0
    total_scanned = 0
    
    logging.info("Building NEW company domain cache...")
    
    while True:
        page_num += 1
        params = {
            'limit': 100,
            'properties': 'domain,name'
        }
        if after:
            params['after'] = after
        
        try:
            response = make_request('GET', url, token, params=params)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print("ERROR: Access forbidden (403). Missing required scope: crm.objects.companies.read", file=sys.stderr)
                print("Please ensure your ACCESS_TOKEN has the 'crm.objects.companies.read' scope.", file=sys.stderr)
                sys.exit(1)
            raise
        
        data = response.json()
        results = data.get('results', [])
        
        for result in results:
            # Skip archived companies
            if result.get('archived', False):
                continue
            
            company_id = str(result.get('id', ''))
            properties = result.get('properties', {})
            domain = properties.get('domain', '')
            
            if domain:
                norm_domain = normalize_domain(domain)
                if norm_domain:
                    # If multiple companies share domain, keep first one (or could handle differently)
                    if norm_domain not in domain_to_id:
                        domain_to_id[norm_domain] = company_id
                    # Note: If multiple companies share domain, we'll handle during conflict resolution
            
            total_scanned += 1
        
        # Log progress every 10 pages
        if page_num % 10 == 0:
            logging.info(f"NEW domain cache: Page {page_num}, companies scanned: {total_scanned}")
        
        # Check for next page
        paging = data.get('paging', {})
        next_info = paging.get('next', {})
        next_after = next_info.get('after') if isinstance(next_info, dict) else None
        
        if not next_after:
            break
        
        after = next_after
    
    logging.info(f"NEW domain cache complete: {total_scanned} companies scanned, {len(domain_to_id)} unique domains")
    return domain_to_id


def search_company_by_domain(token: str, domain: str) -> List[str]:
    """
    Search for companies in NEW portal by domain.
    
    Args:
        token: HubSpot access token for NEW portal
        domain: Domain to search for (will be normalized)
    
    Returns:
        List of company IDs matching the domain
    """
    url = f"{BASE_URL}/crm/v3/objects/companies/search"
    norm_domain = normalize_domain(domain)
    
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "domain",
                        "operator": "EQ",
                        "value": norm_domain
                    }
                ]
            }
        ],
        "properties": ["domain", "name"],
        "limit": 10
    }
    
    try:
        response = make_request('POST', url, token, json=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print("ERROR: Access forbidden (403). Missing required scope: crm.objects.companies.read", file=sys.stderr)
            print("Please ensure your ACCESS_TOKEN has the 'crm.objects.companies.read' scope.", file=sys.stderr)
            sys.exit(1)
        raise
    
    data = response.json()
    results = data.get('results', [])
    
    company_ids = []
    for result in results:
        company_id = str(result.get('id', ''))
        if company_id:
            company_ids.append(company_id)
    
    return company_ids


def create_company_in_new(token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a company in NEW portal.
    
    Args:
        token: HubSpot access token for NEW portal
        payload: Company creation payload
    
    Returns:
        Response data from API
    """
    url = f"{BASE_URL}/crm/v3/objects/companies"
    
    try:
        response = make_request('POST', url, token, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        # Check for conflict (409) - company already exists
        if e.response.status_code == 409:
            raise Exception("CONFLICT") from e
        # Don't retry 400/403 - treat as fatal
        if e.response.status_code in (400, 403):
            error_data = {}
            try:
                error_data = e.response.json()
            except:
                pass
            error_msg = f"HTTP {e.response.status_code}"
            if error_data.get('message'):
                error_msg += f": {error_data['message']}"
            raise Exception(error_msg) from e
        # Re-raise other errors
        raise


def search_companies_by_created_date(token: str, created_after: str, created_before: Optional[str] = None, after: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Search for companies in NEW portal by created date.
    
    Args:
        token: HubSpot access token for NEW portal
        created_after: ISO8601 timestamp - only companies with createdate >= this
        created_before: Optional ISO8601 timestamp - only companies with createdate <= this
        after: Optional pagination cursor
    
    Returns:
        Tuple of (list of company results, next_after cursor or None)
    """
    url = f"{BASE_URL}/crm/v3/objects/companies/search"
    
    filters = [
        {
            "propertyName": "createdate",
            "operator": "GTE",
            "value": created_after
        }
    ]
    
    if created_before:
        filters.append({
            "propertyName": "createdate",
            "operator": "LTE",
            "value": created_before
        })
    
    payload = {
        "filterGroups": [
            {
                "filters": filters
            }
        ],
        "properties": ["name", "domain", "createdate"],
        "sorts": [
            {
                "propertyName": "createdate",
                "direction": "DESCENDING"
            }
        ],
        "limit": 100
    }
    
    if after:
        payload["after"] = after
    
    try:
        response = make_request('POST', url, token, json=payload)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print("ERROR: Access forbidden (403). Missing required scope: crm.objects.companies.read", file=sys.stderr)
            print("Please ensure your ACCESS_TOKEN has the 'crm.objects.companies.read' scope.", file=sys.stderr)
            sys.exit(1)
        raise
    
    data = response.json()
    results = data.get('results', [])
    
    # Get next page cursor
    paging = data.get('paging', {})
    next_info = paging.get('next', {})
    next_after = next_info.get('after') if isinstance(next_info, dict) else None
    
    return results, next_after


def update_company_name(token: str, company_id: str, name: str) -> Dict[str, Any]:
    """
    Update a company's name in NEW portal.
    
    Args:
        token: HubSpot access token for NEW portal
        company_id: Company ID to update
        name: New name value
    
    Returns:
        Response data from API
    """
    url = f"{BASE_URL}/crm/v3/objects/companies/{company_id}"
    payload = {
        "properties": {
            "name": name
        }
    }
    
    try:
        response = make_request('PATCH', url, token, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        # Don't retry 400/403 - treat as fatal
        if e.response.status_code in (400, 403):
            error_data = {}
            try:
                error_data = e.response.json()
            except:
                pass
            error_msg = f"HTTP {e.response.status_code}"
            if error_data.get('message'):
                error_msg += f": {error_data['message']}"
            raise Exception(error_msg) from e
        # Re-raise other errors (will be retried by make_request)
        raise


def run_fix_new_company_names(db_path: str, created_after: str, created_before: Optional[str], dry_run: bool, max_fix: Optional[int], continue_on_error: bool, export_fixed: Optional[str]):
    """
    Fix newly-created companies in NEW portal where name is missing by setting name = domain.
    
    Args:
        db_path: Path to SQLite database
        created_after: ISO8601 timestamp - only consider companies created >= this
        created_before: Optional ISO8601 timestamp - only consider companies created <= this
        dry_run: If True, don't update, just print what would be changed
        max_fix: Optional limit on number of updates
        continue_on_error: If True, continue on errors; otherwise stop on first error
        export_fixed: Optional path to write CSV of processed companies
    """
    # Load NEW token
    new_token = load_new_token()
    
    # Check database exists
    if not Path(db_path).exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    
    try:
        # Load allowed NEW company IDs from mapping table
        cursor = conn.cursor()
        cursor.execute("""
            SELECT new_company_id FROM company_id_map 
            WHERE status='created' AND new_company_id IS NOT NULL
        """)
        allowed_new_ids = {row[0] for row in cursor.fetchall()}
        
        logging.info(f"Found {len(allowed_new_ids)} companies in mapping table with status='created'")
        
        # Track statistics
        stats = {
            'scanned_in_window': 0,
            'matched_in_mapping': 0,
            'missing_name_and_fixable': 0,
            'updated_count': 0,
            'skipped_already_has_name': 0,
            'skipped_no_domain': 0,
            'skipped_not_in_mapping': 0,
            'failed_count': 0
        }
        
        processed_companies = []
        after = None
        page_num = 0
        
        logging.info(f"Searching NEW portal companies created after {created_after}...")
        
        while True:
            page_num += 1
            
            # Check max_fix limit
            if max_fix and stats['updated_count'] >= max_fix:
                logging.info(f"Reached max_fix limit ({max_fix})")
                break
            
            # Search companies
            results, next_after = search_companies_by_created_date(
                new_token, created_after, created_before, after=after
            )
            
            if not results:
                break
            
            stats['scanned_in_window'] += len(results)
            
            # Process each company
            for result in results:
                company_id = str(result.get('id', ''))
                properties = result.get('properties', {})
                createdate = properties.get('createdate', '')
                domain = properties.get('domain', '')
                name = properties.get('name', '')
                
                # Normalize name (trim)
                name_trimmed = name.strip() if name else ''
                
                # Check if company is in our mapping table
                if company_id not in allowed_new_ids:
                    stats['skipped_not_in_mapping'] += 1
                    processed_companies.append({
                        'company_id': company_id,
                        'createdate': createdate,
                        'domain': domain,
                        'old_name': name,
                        'new_name': None,
                        'action': 'skipped',
                        'error': 'not_in_mapping'
                    })
                    continue
                
                stats['matched_in_mapping'] += 1
                
                # Check if domain is empty
                if not domain or not domain.strip():
                    stats['skipped_no_domain'] += 1
                    processed_companies.append({
                        'company_id': company_id,
                        'createdate': createdate,
                        'domain': domain or '',
                        'old_name': name,
                        'new_name': None,
                        'action': 'skipped',
                        'error': 'no_domain'
                    })
                    continue
                
                # Check if name is already non-empty
                if name_trimmed:
                    stats['skipped_already_has_name'] += 1
                    processed_companies.append({
                        'company_id': company_id,
                        'createdate': createdate,
                        'domain': domain,
                        'old_name': name,
                        'new_name': None,
                        'action': 'skipped',
                        'error': 'already_has_name'
                    })
                    continue
                
                # This company needs fixing
                stats['missing_name_and_fixable'] += 1
                
                # Normalize domain for name
                normalized_domain = normalize_domain(domain)
                
                if dry_run:
                    processed_companies.append({
                        'company_id': company_id,
                        'createdate': createdate,
                        'domain': domain,
                        'old_name': name,
                        'new_name': normalized_domain,
                        'action': 'would_update',
                        'error': None
                    })
                else:
                    # Update company name
                    try:
                        update_company_name(new_token, company_id, normalized_domain)
                        stats['updated_count'] += 1
                        processed_companies.append({
                            'company_id': company_id,
                            'createdate': createdate,
                            'domain': domain,
                            'old_name': name,
                            'new_name': normalized_domain,
                            'action': 'updated',
                            'error': None
                        })
                        logging.info(f"Updated company {company_id}: name = {normalized_domain}")
                    except Exception as e:
                        error_msg = str(e)
                        stats['failed_count'] += 1
                        processed_companies.append({
                            'company_id': company_id,
                            'createdate': createdate,
                            'domain': domain,
                            'old_name': name,
                            'new_name': normalized_domain,
                            'action': 'failed',
                            'error': error_msg
                        })
                        logging.error(f"Failed to update company {company_id}: {error_msg}")
                        
                        if not continue_on_error:
                            print(f"ERROR: Failed to update company {company_id}: {error_msg}", file=sys.stderr)
                            sys.exit(1)
                
                # Check max_fix limit after update
                if max_fix and stats['updated_count'] >= max_fix:
                    break
            
            # Check for next page
            if not next_after:
                break
            
            after = next_after
        
        # Print summary
        print("\n" + "=" * 80)
        print("COMPANY NAME FIX SUMMARY")
        print("=" * 80)
        print(f"scanned_in_window: {stats['scanned_in_window']}")
        print(f"matched_in_mapping: {stats['matched_in_mapping']}")
        print(f"missing_name_and_fixable: {stats['missing_name_and_fixable']}")
        print(f"updated_count: {stats['updated_count']}")
        print(f"skipped_already_has_name: {stats['skipped_already_has_name']}")
        print(f"skipped_no_domain: {stats['skipped_no_domain']}")
        print(f"skipped_not_in_mapping: {stats['skipped_not_in_mapping']}")
        print(f"failed_count: {stats['failed_count']}")
        print("=" * 80)
        
        # Export CSV if requested
        if export_fixed:
            export_dir = Path(export_fixed).parent
            export_dir.mkdir(parents=True, exist_ok=True)
            
            with open(export_fixed, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'company_id',
                    'createdate',
                    'domain',
                    'old_name',
                    'new_name',
                    'action',
                    'error'
                ])
                
                for company in processed_companies:
                    writer.writerow([
                        company['company_id'],
                        company['createdate'] or '',
                        company['domain'] or '',
                        company['old_name'] or '',
                        company['new_name'] or '',
                        company['action'],
                        company['error'] or ''
                    ])
            
            print(f"\nExported results to: {export_fixed}")
        
    finally:
        conn.close()


def run_create_missing_companies(db_path: str, dry_run: bool, max_create: Optional[int], continue_on_error: bool, export_created: Optional[str]):
    """
    Create missing companies in NEW portal.
    
    Args:
        db_path: Path to SQLite database
        dry_run: If True, don't create, just print counts and samples
        max_create: Optional limit on number of companies to create
        continue_on_error: If True, continue on errors; otherwise stop on first error
        export_created: Optional path to write CSV of created mappings
    """
    # Load NEW token (OLD comes from DB)
    new_token = load_new_token()
    
    # Check database exists
    if not Path(db_path).exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        print("Run --init-companies first to load OLD companies.", file=sys.stderr)
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    
    try:
        # Ensure schema exists
        create_database(db_path, reset=False, reset_companies=False, reset_associations=False)
        
        # Check companies_old table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='companies_old'")
        if not cursor.fetchone():
            print("ERROR: companies_old table not found.", file=sys.stderr)
            print("Run --init-companies first to load OLD companies.", file=sys.stderr)
            sys.exit(1)
        
        # Build NEW company domain cache
        logging.info("Fetching NEW portal companies...")
        new_domain_cache = build_new_company_domain_cache(new_token)
        
        # Load company property definitions
        prop_defs = load_company_property_definitions(conn)
        
        # Read OLD companies from database
        logging.info("Reading OLD companies from database...")
        cursor.execute("SELECT id, domain, name, properties_json FROM companies_old")
        old_companies = cursor.fetchall()
        
        logging.info(f"Processing {len(old_companies)} OLD companies...")
        
        # Track statistics
        stats = {
            'processed': 0,
            'created': 0,
            'already_exists': 0,
            'failed': 0,
            'skipped_no_domain': 0,
            'skipped_already_mapped': 0
        }
        
        candidates_to_create = []
        created_mappings = []
        
        for old_id, old_domain, old_name, properties_json_str in old_companies:
            old_domain = old_domain or ''
            old_name = old_name or ''
            
            stats['processed'] += 1
            
            # Check if already mapped
            cursor.execute("SELECT status, new_company_id FROM company_id_map WHERE old_company_id=?", (old_id,))
            existing_mapping = cursor.fetchone()
            if existing_mapping:
                status, new_id = existing_mapping
                if status in ('created', 'already_exists'):
                    stats['skipped_already_mapped'] += 1
                    continue
            
            # Normalize domain
            if old_domain:
                norm_domain = normalize_domain(old_domain)
            else:
                norm_domain = ''
            
            # Check if domain exists in NEW cache
            if norm_domain and norm_domain in new_domain_cache:
                # Already exists in NEW
                new_company_id = new_domain_cache[norm_domain]
                now_iso = datetime.now(timezone.utc).isoformat()
                cursor.execute("""
                    INSERT OR REPLACE INTO company_id_map
                    (old_company_id, old_domain, new_company_id, status, error, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (old_id, old_domain, new_company_id, 'already_exists', None, now_iso))
                conn.commit()
                stats['already_exists'] += 1
                created_mappings.append({
                    'old_company_id': old_id,
                    'old_domain': old_domain,
                    'new_company_id': new_company_id,
                    'status': 'already_exists',
                    'error': None
                })
                continue
            
            # Candidate to create (if domain is non-empty)
            if not norm_domain:
                stats['skipped_no_domain'] += 1
                continue
            
            # Check max_create limit
            if max_create and stats['created'] + len(candidates_to_create) >= max_create:
                break
            
            # Parse properties and build payload
            try:
                properties = json.loads(properties_json_str)
            except json.JSONDecodeError as e:
                error_msg = f"Failed to parse properties_json: {str(e)}"
                now_iso = datetime.now(timezone.utc).isoformat()
                cursor.execute("""
                    INSERT OR REPLACE INTO company_id_map
                    (old_company_id, old_domain, new_company_id, status, error, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (old_id, old_domain, None, 'failed', error_msg, now_iso))
                conn.commit()
                stats['failed'] += 1
                created_mappings.append({
                    'old_company_id': old_id,
                    'old_domain': old_domain,
                    'new_company_id': None,
                    'status': 'failed',
                    'error': error_msg
                })
                if not continue_on_error:
                    print(f"ERROR: Failed to parse properties for company {old_id}: {error_msg}", file=sys.stderr)
                    sys.exit(1)
                continue
            
            # Filter properties (reuse printone-company logic)
            included_props, excluded_list = filter_company_properties(properties, prop_defs)
            
            # Build payload
            payload = {"properties": included_props}
            
            candidates_to_create.append({
                'old_id': old_id,
                'old_domain': old_domain,
                'old_name': old_name,
                'payload': payload
            })
        
        # In dry-run mode, print samples and exit
        if dry_run:
            print("\n" + "=" * 80)
            print("DRY RUN - COMPANY CREATION PLAN")
            print("=" * 80)
            print(f"total_old_companies: {len(old_companies)}")
            print(f"total_new_companies_scanned: {len(new_domain_cache)}")
            print(f"candidates_missing: {len(candidates_to_create)}")
            print(f"already_exists_count: {stats['already_exists']}")
            print(f"skipped_no_domain: {stats['skipped_no_domain']}")
            print(f"skipped_already_mapped: {stats['skipped_already_mapped']}")
            print()
            print("First 10 candidate companies to create:")
            for i, candidate in enumerate(candidates_to_create[:10], 1):
                print(f"\n{i}. OLD ID: {candidate['old_id']}, Domain: {candidate['old_domain']}, Name: {candidate['old_name']}")
                print("   Payload:")
                print(json.dumps(candidate['payload'], indent=4, ensure_ascii=False))
            print("=" * 80)
            return
        
        # Create companies
        logging.info(f"Creating {len(candidates_to_create)} companies in NEW portal...")
        start_time = time.time()
        
        for idx, candidate in enumerate(candidates_to_create, 1):
            old_id = candidate['old_id']
            old_domain = candidate['old_domain']
            payload = candidate['payload']
            
            try:
                # Create company
                response_data = create_company_in_new(new_token, payload)
                new_company_id = str(response_data.get('id', ''))
                
                # Record success
                now_iso = datetime.now(timezone.utc).isoformat()
                cursor.execute("""
                    INSERT OR REPLACE INTO company_id_map
                    (old_company_id, old_domain, new_company_id, status, error, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (old_id, old_domain, new_company_id, 'created', None, now_iso))
                conn.commit()
                
                stats['created'] += 1
                created_mappings.append({
                    'old_company_id': old_id,
                    'old_domain': old_domain,
                    'new_company_id': new_company_id,
                    'status': 'created',
                    'error': None
                })
                
            except Exception as e:
                error_msg = str(e)
                
                # Handle conflict - search for existing company
                if 'CONFLICT' in error_msg:
                    try:
                        existing_ids = search_company_by_domain(new_token, old_domain)
                        if len(existing_ids) == 1:
                            new_company_id = existing_ids[0]
                            now_iso = datetime.now(timezone.utc).isoformat()
                            cursor.execute("""
                                INSERT OR REPLACE INTO company_id_map
                                (old_company_id, old_domain, new_company_id, status, error, created_at)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (old_id, old_domain, new_company_id, 'already_exists', None, now_iso))
                            conn.commit()
                            stats['already_exists'] += 1
                            created_mappings.append({
                                'old_company_id': old_id,
                                'old_domain': old_domain,
                                'new_company_id': new_company_id,
                                'status': 'already_exists',
                                'error': None
                            })
                            continue
                        elif len(existing_ids) > 1:
                            error_msg = f"Domain conflict: multiple companies found ({len(existing_ids)} matches)"
                        else:
                            error_msg = "Domain conflict but no company found in search"
                    except Exception as search_error:
                        error_msg = f"Conflict occurred but search failed: {search_error}"
                
                # Record failure
                now_iso = datetime.now(timezone.utc).isoformat()
                cursor.execute("""
                    INSERT OR REPLACE INTO company_id_map
                    (old_company_id, old_domain, new_company_id, status, error, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (old_id, old_domain, None, 'failed', error_msg, now_iso))
                conn.commit()
                
                stats['failed'] += 1
                created_mappings.append({
                    'old_company_id': old_id,
                    'old_domain': old_domain,
                    'new_company_id': None,
                    'status': 'failed',
                    'error': error_msg
                })
                
                logging.error(f"Failed to create company {old_id} ({old_domain}): {error_msg}")
                
                if not continue_on_error:
                    print(f"ERROR: Failed to create company {old_id}: {error_msg}", file=sys.stderr)
                    sys.exit(1)
            
            # Log progress every 50 creates
            if idx % 50 == 0:
                elapsed = time.time() - start_time
                rate = stats['created'] / elapsed if elapsed > 0 else 0
                logging.info(f"Progress: processed={idx}, created={stats['created']}, already_exists={stats['already_exists']}, failed={stats['failed']}, rate={rate:.1f}/sec")
        
        # Final commit
        conn.commit()
        
        # Get total mapping rows
        cursor.execute("SELECT COUNT(*) FROM company_id_map")
        mapping_table_rows_total = cursor.fetchone()[0]
        
        # Print summary
        print("\n" + "=" * 80)
        print("COMPANY CREATION SUMMARY")
        print("=" * 80)
        print(f"total_old_companies: {len(old_companies)}")
        print(f"total_new_companies_scanned: {len(new_domain_cache)}")
        print(f"candidates_missing: {len(candidates_to_create)}")
        print(f"created_count: {stats['created']}")
        print(f"already_exists_count: {stats['already_exists']}")
        print(f"failed_count: {stats['failed']}")
        print(f"mapping_table_rows_total: {mapping_table_rows_total}")
        print(f"Database path: {Path(db_path).absolute()}")
        print("=" * 80)
        
        # Export CSV if requested
        if export_created:
            export_dir = Path(export_created).parent
            export_dir.mkdir(parents=True, exist_ok=True)
            
            with open(export_created, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'old_company_id',
                    'old_domain',
                    'new_company_id',
                    'status',
                    'error'
                ])
                
                for mapping in created_mappings:
                    writer.writerow([
                        mapping['old_company_id'],
                        mapping['old_domain'],
                        mapping['new_company_id'] or '',
                        mapping['status'],
                        mapping['error'] or ''
                    ])
            
            print(f"\nExported mappings to: {export_created}")
        
    finally:
        conn.close()


def run_print_one(db_path: str, email: Optional[str], contact_id: Optional[str], include_associations: bool = False):
    """
    Print filtered property payload for one contact.
    
    Args:
        db_path: Path to SQLite database
        email: Contact email (optional)
        contact_id: Contact ID (optional)
    """
    # Validate inputs
    if not email and not contact_id:
        print("ERROR: --print-one requires either --email or --contact-id", file=sys.stderr)
        sys.exit(1)
    
    if email and contact_id:
        print("ERROR: --print-one requires either --email or --contact-id, not both", file=sys.stderr)
        sys.exit(1)
    
    # Connect to database
    if not Path(db_path).exists():
        print(f"ERROR: Database not found: {db_path}", file=sys.stderr)
        sys.exit(1)
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Lookup contact
        if email:
            result = lookup_contact_by_email(conn, email)
            if result is None:
                # Check if it's ambiguous
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM contacts_old WHERE LOWER(email)=LOWER(?)",
                    (email,)
                )
                count = cursor.fetchone()[0]
                if count > 1:
                    print(f"ERROR: Multiple contacts found with email '{email}'", file=sys.stderr)
                    sys.exit(1)
                else:
                    print(f"ERROR: Contact not found with email '{email}'", file=sys.stderr)
                    sys.exit(1)
        else:
            result = lookup_contact_by_id(conn, contact_id)
            if result is None:
                print(f"ERROR: Contact not found with ID '{contact_id}'", file=sys.stderr)
                sys.exit(1)
        
        contact_id_found, email_found, properties_json_str = result
        
        # Parse properties
        try:
            properties = json.loads(properties_json_str)
        except json.JSONDecodeError:
            print(f"ERROR: Failed to parse properties_json for contact {contact_id_found}", file=sys.stderr)
            sys.exit(1)
        
        # Load property definitions
        prop_defs = load_property_definitions(conn)
        
        # Filter properties
        included_props, excluded_list = filter_contact_properties(properties, prop_defs)
        
        # Print output
        print(f"EMAIL: {email_found} CONTACT_ID: {contact_id_found}")
        print()
        
        # Print JSON payload
        payload = {"properties": included_props}
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print()
        
        # Print summary
        print("=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"included_properties_count: {len(included_props)}")
        print(f"excluded_properties_count: {len(excluded_list)}")
        print()
        
        # Print first ~30 excluded properties
        print("Excluded properties (first 30):")
        for prop_name, reason in excluded_list[:30]:
            print(f"  - {prop_name}: {reason}")
        
        if len(excluded_list) > 30:
            print(f"  ... and {len(excluded_list) - 30} more")
        
        # Handle associations if requested
        if include_associations:
            cursor = conn.cursor()
            
            # Query associations for this contact
            cursor.execute("""
                SELECT cca.company_id, cca.association_types_json,
                       co.name, co.domain
                FROM contact_company_associations_old cca
                LEFT JOIN companies_old co ON cca.company_id = co.id
                WHERE cca.contact_id = ?
            """, (contact_id_found,))
            
            associations_data = cursor.fetchall()
            
            # Build associations payload
            companies_list = []
            for row in associations_data:
                company_id, association_types_json, company_name, company_domain = row
                
                # Parse association types
                try:
                    association_types = json.loads(association_types_json) if association_types_json else []
                except json.JSONDecodeError:
                    association_types = []
                
                company_info = {
                    "old_company_id": company_id,
                    "association_types": association_types
                }
                
                if company_name:
                    company_info["company_name"] = company_name
                if company_domain:
                    company_info["company_domain"] = company_domain
                
                companies_list.append(company_info)
            
            # Print associations JSON
            if companies_list:
                print()
                associations_payload = {
                    "associations": {
                        "companies": companies_list
                    }
                }
                print(json.dumps(associations_payload, indent=2, ensure_ascii=False))
                print()
            
            # Update summary with association counts
            print("=" * 80)
            print("ASSOCIATIONS SUMMARY")
            print("=" * 80)
            print(f"associated_companies_count: {len(companies_list)}")
            print()
            
            # Print first ~20 company IDs/domains
            if companies_list:
                print("Associated companies (first 20):")
                for company_info in companies_list[:20]:
                    company_id = company_info.get("old_company_id", "")
                    domain = company_info.get("company_domain", "")
                    name = company_info.get("company_name", "")
                    
                    parts = []
                    if name:
                        parts.append(f"name={name}")
                    if domain:
                        parts.append(f"domain={domain}")
                    parts.append(f"id={company_id}")
                    
                    print(f"  - {', '.join(parts)}")
                
                if len(companies_list) > 20:
                    print(f"  ... and {len(companies_list) - 20} more")
        
    finally:
        conn.close()


def run_init(db_path: str, reset: bool, limit: Optional[int]):
    """
    Run the --init phase: extract contacts from OLD account into SQLite.
    
    Args:
        db_path: Path to SQLite database
        reset: If True, clear contact tables before repopulating
        limit: Optional maximum number of contacts to fetch
    """
    token = load_token()
    
    # Create/verify database
    create_database(db_path, reset=reset, reset_companies=False, reset_associations=False)
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    
    try:
        # Step 1: Fetch and store property definitions
        properties = fetch_contact_properties(token)
        store_property_definitions(conn, properties)
        
        # Get property names for batch read
        property_names = [prop.get('name') for prop in properties if prop.get('name')]
        logging.info(f"Will fetch {len(property_names)} properties per contact")
        
        # Step 2: Fetch all contact IDs
        contact_ids = fetch_all_contact_ids(token, limit=limit)
        
        if not contact_ids:
            logging.warning("No contacts found")
            return
        
        # Step 3: Batch read contacts with all properties
        logging.info("Fetching full contact objects...")
        contacts = batch_read_contacts(token, contact_ids, property_names)
        
        # Step 4: Store contacts in database
        logging.info(f"Storing {len(contacts)} contacts in database...")
        stored_count = 0
        property_value_rows = 0
        start_time = time.time()
        
        # Use transactions: commit every 100 contacts
        commit_interval = 100
        
        for idx, contact in enumerate(contacts, 1):
            store_contact(conn, contact)
            stored_count += 1
            
            # Count property values for this contact
            contact_properties = contact.get('properties', {})
            property_value_rows += len(contact_properties)
            
            # Commit periodically
            if idx % commit_interval == 0:
                conn.commit()
                elapsed = time.time() - start_time
                rate = stored_count / elapsed if elapsed > 0 else 0
                logging.info(f"Progress: {stored_count}/{len(contacts)} contacts stored ({rate:.1f} contacts/sec)")
        
        # Final commit
        conn.commit()
        
        # Step 5: Update meta table
        cursor = conn.cursor()
        now_iso = datetime.now(timezone.utc).isoformat()
        cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ('schema_version', str(SCHEMA_VERSION)))
        cursor.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", ('old_account_loaded_at', now_iso))
        conn.commit()
        
        # Step 6: Print summary
        elapsed_total = time.time() - start_time
        rate_total = stored_count / elapsed_total if elapsed_total > 0 else 0
        
        print("\n" + "=" * 80)
        print("MIGRATION SUMMARY")
        print("=" * 80)
        print(f"Total properties saved: {len(properties)}")
        print(f"Total contacts saved: {stored_count}")
        print(f"Total property-value rows saved: {property_value_rows}")
        print(f"Database path: {Path(db_path).absolute()}")
        print(f"Processing rate: {rate_total:.1f} contacts/sec")
        print("=" * 80)
        
    finally:
        conn.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='HubSpot Contact Migration Tool - Phase 1',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--init', action='store_true',
                           help='Run initialization: extract contacts from OLD account into SQLite')
    mode_group.add_argument('--init-companies', action='store_true',
                           help='Extract companies from OLD account into SQLite')
    mode_group.add_argument('--init-associations', action='store_true',
                           help='Extract contact-company associations from OLD account into SQLite')
    mode_group.add_argument('--print-one', action='store_true',
                           help='Print filtered property payload for one contact')
    mode_group.add_argument('--count-companies', action='store_true',
                           help='Count companies in both OLD and NEW portals and print comparison')
    mode_group.add_argument('--plan-companies', action='store_true',
                           help='Plan company migration by identifying missing OLD companies in NEW portal')
    mode_group.add_argument('--printone-company', action='store_true',
                           help='Print filtered property payload for one OLD company')
    mode_group.add_argument('--create-missing-companies', action='store_true',
                           help='Create missing companies in NEW portal and record ID mappings')
    mode_group.add_argument('--fix-new-company-names', action='store_true',
                           help='Fix newly-created companies in NEW portal where name is missing by setting name = domain')
    
    parser.add_argument('--db', type=str, default=DEFAULT_DB_PATH,
                       help=f'Path to SQLite database (default: {DEFAULT_DB_PATH}, required for --plan-companies, --printone-company, --create-missing-companies, and --fix-new-company-names, optional for --count-companies)')
    
    # Init mode arguments
    parser.add_argument('--reset', action='store_true',
                       help='Clear tables before repopulating (mode-specific: contacts for --init, companies for --init-companies, associations for --init-associations)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Maximum number of records to pull (for testing); default is no limit (--init and --init-companies modes only)')
    
    # Print-one mode arguments
    parser.add_argument('--email', type=str, default=None,
                       help='Contact email address (--print-one mode, case-insensitive)')
    parser.add_argument('--contact-id', type=str, default=None,
                       help='Contact ID (--print-one mode)')
    parser.add_argument('--include-associations', action='store_true',
                       help='Include company associations in output (--print-one mode only)')
    
    # Plan-companies mode arguments
    parser.add_argument('--export', type=str, default=None,
                       help='Write results to CSV at the given path (--plan-companies mode only)')
    parser.add_argument('--include-name-fallback', action='store_true',
                       help='Attempt name-based matching when domain is missing (--plan-companies mode only)')
    parser.add_argument('--max-old', type=int, default=None,
                       help='Limit number of OLD companies processed (--plan-companies mode only, for testing)')
    parser.add_argument('--max-new-pages', type=int, default=None,
                       help='Cap how many pages to read from NEW when building lookup cache (--plan-companies mode only)')
    
    # Printone-company mode arguments
    parser.add_argument('--company-domain', type=str, default=None,
                       help='Company domain (--printone-company mode, case-insensitive, normalized)')
    parser.add_argument('--company-id', type=str, default=None,
                       help='Company ID (--printone-company mode)')
    
    # Create-missing-companies mode arguments
    parser.add_argument('--dry-run', action='store_true',
                       help='Do not create/update in NEW; just print counts and samples (--create-missing-companies and --fix-new-company-names modes)')
    parser.add_argument('--max-create', type=int, default=None,
                       help='Cap number of companies to create (--create-missing-companies mode only, for testing)')
    parser.add_argument('--continue-on-error', action='store_true',
                       help='Continue even if a company fails (--create-missing-companies and --fix-new-company-names modes)')
    parser.add_argument('--export-created', type=str, default=None,
                       help='Write CSV of created mappings (--create-missing-companies mode only)')
    
    # Fix-new-company-names mode arguments
    parser.add_argument('--created-after', type=str, default=None,
                       help='ISO8601 timestamp - only consider NEW companies with createdate >= this (--fix-new-company-names mode, required)')
    parser.add_argument('--created-before', type=str, default=None,
                       help='ISO8601 timestamp - only consider NEW companies with createdate <= this (--fix-new-company-names mode, optional)')
    parser.add_argument('--max-fix', type=int, default=None,
                       help='Cap number of companies to update (--fix-new-company-names mode only, for testing)')
    parser.add_argument('--export-fixed', type=str, default=None,
                       help='Write CSV of processed companies (--fix-new-company-names mode only)')
    
    parser.add_argument('--log-level', type=str, default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level (default: INFO)')
    
    args = parser.parse_args()
    
    # Validate --include-associations
    if args.include_associations and not args.print_one:
        print("ERROR: --include-associations requires --print-one", file=sys.stderr)
        sys.exit(1)
    
    # Validate --count-companies requires both tokens
    if args.count_companies:
        load_dotenv()
        old_token = os.getenv('OLD_ACCESS_TOKEN')
        new_token = os.getenv('ACCESS_TOKEN')
        
        if not old_token:
            print("ERROR: --count-companies requires OLD_ACCESS_TOKEN", file=sys.stderr)
            sys.exit(1)
        if not new_token:
            print("ERROR: --count-companies requires ACCESS_TOKEN", file=sys.stderr)
            sys.exit(1)
    
    # Validate --plan-companies requires ACCESS_TOKEN (not OLD_ACCESS_TOKEN)
    if args.plan_companies:
        load_dotenv()
        new_token = os.getenv('ACCESS_TOKEN')
        
        if not new_token:
            print("ERROR: --plan-companies requires ACCESS_TOKEN", file=sys.stderr)
            sys.exit(1)
    
    # Validate --create-missing-companies requires ACCESS_TOKEN (not OLD_ACCESS_TOKEN)
    if args.create_missing_companies:
        load_dotenv()
        new_token = os.getenv('ACCESS_TOKEN')
        
        if not new_token:
            print("ERROR: --create-missing-companies requires ACCESS_TOKEN", file=sys.stderr)
            sys.exit(1)
    
    # Validate --fix-new-company-names requires ACCESS_TOKEN and --created-after
    if args.fix_new_company_names:
        load_dotenv()
        new_token = os.getenv('ACCESS_TOKEN')
        
        if not new_token:
            print("ERROR: --fix-new-company-names requires ACCESS_TOKEN", file=sys.stderr)
            sys.exit(1)
        
        if not args.created_after:
            print("ERROR: --fix-new-company-names requires --created-after", file=sys.stderr)
            sys.exit(1)
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Route to appropriate mode
    if args.init:
        run_init(args.db, args.reset, args.limit)
    elif args.init_companies:
        run_init_companies(args.db, args.reset, args.limit)
    elif args.init_associations:
        run_init_associations(args.db, args.reset)
    elif args.print_one:
        run_print_one(args.db, args.email, args.contact_id, args.include_associations)
    elif args.count_companies:
        # For --count-companies, db_path is optional (only used for local count context)
        run_count_companies(args.db)
    elif args.plan_companies:
        run_plan_companies(args.db, args.export, args.include_name_fallback, args.max_old, args.max_new_pages)
    elif args.printone_company:
        run_printone_company(args.db, args.company_domain, args.company_id)
    elif args.create_missing_companies:
        run_create_missing_companies(args.db, args.dry_run, args.max_create, args.continue_on_error, args.export_created)
    elif args.fix_new_company_names:
        run_fix_new_company_names(args.db, args.created_after, args.created_before, args.dry_run, args.max_fix, args.continue_on_error, args.export_fixed)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
