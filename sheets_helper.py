# -*- coding: utf-8 -*-
"""Google Sheets read/write helpers with caching."""
import time, gspread
from google.oauth2.service_account import Credentials
from config import *

_sheet_cache = {}
CACHE_TTL = 30
_gc = None; _gc_ts = 0; _GC_TTL = 600
_sh = None; _sh_ts = 0

def get_gc():
    global _gc, _gc_ts
    now = time.time()
    if _gc is None or (now - _gc_ts) > _GC_TTL:
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        _gc = gspread.authorize(creds); _gc_ts = now
    return _gc

def get_spreadsheet():
    global _sh, _sh_ts
    now = time.time()
    if _sh is None or (now - _sh_ts) > _GC_TTL:
        _sh = get_gc().open_by_key(SHEET_ID); _sh_ts = now
    return _sh

def get_sheet(tab):
    global _sh, _sh_ts
    sh = get_spreadsheet()
    try:
        return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows=2000, cols=26); _init_headers(ws, tab); return ws
    except Exception:
        _sh = None; _sh_ts = 0; sh = get_spreadsheet()
        try: return sh.worksheet(tab)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab, rows=2000, cols=26); _init_headers(ws, tab); return ws

def _get_headers_for(tab):
    return {TAB_BUDGETS:["id","country","quarter","total_budget","updated_at"], TAB_CHANNELS:["id","country","quarter","name","budget","sort_order","created_at"], TAB_ACTIVITIES:["id","channel_id","country","quarter","name","sort_order","created_at"], TAB_ENTRIES:ENTRY_HEADERS, TAB_MAPPING:MAPPING_HEADERS, TAB_VENDORS:VENDOR_HEADERS, TAB_USERS:USER_HEADERS, TAB_CATEGORIES:CATEGORY_HEADERS}.get(tab, None)

def safe_get_records(ws, tab=None):
    try:
        hdrs = _get_headers_for(tab) if tab else None
        return ws.get_all_records(expected_headers=hdrs) if hdrs else ws.get_all_records(numericise_ignore=['all'])
    except Exception:
        try: return ws.get_all_records(numericise_ignore=['all'])
        except Exception: return []

def get_records_cached(tab):
    now = time.time()
    if tab in _sheet_cache:
        ts, data = _sheet_cache[tab]
        if now - ts < CACHE_TTL: return data
    try:
        ws = get_sheet(tab)
        data = ws.get_all_records(expected_headers=_get_headers_for(tab))
    except Exception:
        try: data = ws.get_all_records(numericise_ignore=['all'])
        except Exception: data = []
    _sheet_cache[tab] = (now, data); return data

def invalidate_cache(tab):
    _sheet_cache.pop(tab, None)

def rows_for_cached(tab, **filters):
    rows = get_records_cached(tab)
    for k, v in filters.items():
        rows = [r for r in rows if str(r.get(k,"")) == str(v)]
    return rows

def rows_for(tab, **filters):
    try:
        ws = get_sheet(tab); hdrs = _get_headers_for(tab)
        rows = ws.get_all_records(expected_headers=hdrs) if hdrs else safe_get_records(ws)
    except Exception:
        try: rows = ws.get_all_records(numericise_ignore=['all'])
        except Exception: rows = []
    for k, v in filters.items():
        rows = [r for r in rows if str(r.get(k,"")) == str(v)]
    return rows

def _init_headers(ws, tab):
    hdrs = {TAB_BUDGETS:["id","country","quarter","total_budget","updated_at"], TAB_CHANNELS:["id","country","quarter","name","budget","sort_order","created_at"], TAB_ACTIVITIES:["id","channel_id","country","quarter","name","sort_order","created_at"], TAB_ENTRIES:ENTRY_HEADERS, TAB_MAPPING:MAPPING_HEADERS, TAB_VENDORS:VENDOR_HEADERS, TAB_USERS:USER_HEADERS, TAB_CATEGORIES:CATEGORY_HEADERS}
    if tab in hdrs:
        ws.append_row(hdrs[tab])
        ws.format("1:1", {"backgroundColor":{"red":0.11,"green":0.31,"blue":0.24},"textFormat":{"foregroundColor":{"red":1,"green":1,"blue":1},"bold":True}})

def ensure_entry_headers():
    try:
        ws = get_sheet(TAB_ENTRIES); row1 = ws.row_values(1)
        if not row1: return
        if "activity_id" in row1 and "activity_name" in row1: return
        ws.update('A1:X1', [ENTRY_HEADERS])
        ws.format("1:1", {"backgroundColor":{"red":0.11,"green":0.31,"blue":0.24},"textFormat":{"foregroundColor":{"red":1,"green":1,"blue":1},"bold":True}})
        invalidate_cache(TAB_ENTRIES)
    except Exception as e:
        print(f"[MIGRATION] Could not fix Entries headers: {e}")
