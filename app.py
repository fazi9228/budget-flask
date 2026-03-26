"""
APAC Marketing Budget Tracker — Flask + Google Sheets
Run:  python app.py
Open: http://localhost:5000
"""

import os, json, uuid, base64, io, csv
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, request, jsonify, session, redirect, send_file
from werkzeug.security import generate_password_hash, check_password_hash
import gspread
from google.oauth2.service_account import Credentials

# ── CONFIG ────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-in-production")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_CREDS", "credentials.json")
SHEET_ID             = os.environ.get("SHEET_ID", "13TMeZ3pqdUQr2WRMG5G70xchZKfmiPVOShZChqbUsv4")
ADMIN_MARKET         = "APAC"

MARKETS  = ["CN","HKG","ID","IN","MN","MY","PH","SG","TH","TW","VN","TW/SG/MY/MN"]
QUARTERS = ["Q1","Q2","Q3","Q4"]

# Invoice storage on disk (not in Google Sheets — avoids cell size limits)
INVOICE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "invoices")
os.makedirs(INVOICE_DIR, exist_ok=True)

def save_invoice_to_disk(data_url, entry_id, filename):
    """Save a base64 data URL to disk, return the stored filename."""
    try:
        header, b64 = data_url.split(",", 1)
        data = base64.b64decode(b64)
        # Sanitise filename
        safe_name = "".join(c for c in filename if c.isalnum() or c in "._- ")[:100]
        stored_name = f"{entry_id}_{uuid.uuid4().hex[:6]}_{safe_name}"
        path = os.path.join(INVOICE_DIR, stored_name)
        with open(path, "wb") as f:
            f.write(data)
        return stored_name
    except Exception:
        return None

def get_invoice_path(stored_name):
    """Get full path for a stored invoice."""
    path = os.path.join(INVOICE_DIR, stored_name)
    if os.path.exists(path):
        return path
    return None

TAB_BUDGETS    = "Budgets"
TAB_CHANNELS   = "Channels"
TAB_ACTIVITIES = "Activities"
TAB_ENTRIES    = "Entries"
TAB_MAPPING    = "ChannelMapping"
TAB_VENDORS    = "Vendors"

ENTRY_HEADERS = [
    "id","country","quarter","month","channel_id","channel_name",
    "activity_id","activity_name",
    "bu","finance_cat","marketing_cat","description",
    "planned","confirmed","actual",
    "jira","vendor","notes",
    "approved","invoice_names","invoice_data",
    "entered_by","created_at","updated_at"
]

MAPPING_HEADERS = ["channel_keyword","bu","finance_cat","marketing_cat","updated_by","updated_at"]
VENDOR_HEADERS  = ["id","name","country","added_by","created_at"]

DEFAULT_MAPPING = [
    ("performance",  "Marketing : Programmatic - 613000009XXX",          "PPC",                  "PPC / Search"),
    ("ppc",          "Marketing : Programmatic - 613000009XXX",          "PPC",                  "PPC / Search"),
    ("programmatic", "Marketing : Programmatic - 613000009XXX",          "Programmatic",         "Programmatic"),
    ("affiliate",    "Marketing : Affiliate - 613000002XXX",             "Affiliate",            "Affiliate- CPA & FF"),
    ("paid social",  "Marketing : Paid Social / YouTube - 613000004XXX", "Paid Social-Meta",     "Paid Social"),
    ("social",       "Marketing : Paid Social / YouTube - 613000004XXX", "Paid Social-Meta",     "Paid Social"),
    ("youtube",      "Marketing : Paid Social / YouTube - 613000004XXX", "Paid Social-Youtube",  "YouTube"),
    ("brand",        "Marketing : Local Brand - 613000024XXX",           "Campaigns/Promotions", "Brand / OOH"),
    ("event",        "Marketing : Local Brand - 613000024XXX",           "Event",                "Events & Sponsorship"),
    ("influencer",   "Marketing : Local Brand - 613000024XXX",           "Influencer/KOL",       "Influencer / KOL"),
    ("kol",          "Marketing : Local Brand - 613000024XXX",           "Influencer/KOL",       "Influencer / KOL"),
    ("premium",      "Marketing : Premium - 613000019XXX",               "Premium",              "Premium Partners"),
    ("partner",      "Marketing : Partners - 613000022XXX",              "Partner",              "Premium Partners"),
    ("raf",          "Marketing - Refer a friend - 613000003XXX",        "RAF",                  "Refer a Friend"),
    ("refer",        "Marketing - Refer a friend - 613000003XXX",        "RAF",                  "Refer a Friend"),
    ("mar tech",     "Marketing : Marketing technology",                  "Marketing Technology", "Technology"),
    ("technology",   "Marketing : Marketing technology",                  "Marketing Technology", "Technology"),
    ("seo",          "Marketing : Local Brand - 613000024XXX",           "Local SEO",            "SEO"),
    ("amf1",         "Marketing : Partners - 613000022XXX",              "AMF1 Activation",      "AMF1"),
]

# ── SHEETS HELPERS ────────────────────────────────────────────
# In-memory cache — reduces Sheets API calls significantly
import time
_sheet_cache = {}
CACHE_TTL = 30  # seconds

# Persistent gspread client + spreadsheet (avoids re-auth on every call)
_gc = None
_gc_ts = 0
_GC_TTL = 600  # re-auth every 10 minutes

def get_gc():
    global _gc, _gc_ts
    now = time.time()
    if _gc is None or (now - _gc_ts) > _GC_TTL:
        try:
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            _gc = gspread.authorize(creds)
            _gc_ts = now
        except Exception as e:
            _gc = None
            raise e
    return _gc

_sh = None
_sh_ts = 0

def get_spreadsheet():
    global _sh, _sh_ts
    now = time.time()
    if _sh is None or (now - _sh_ts) > _GC_TTL:
        try:
            gc = get_gc()
            _sh = gc.open_by_key(SHEET_ID)
            _sh_ts = now
        except Exception as e:
            _sh = None
            raise e
    return _sh

def get_sheet(tab):
    global _sh, _sh_ts
    sh = get_spreadsheet()
    try:
        return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows=2000, cols=26)
        _init_headers(ws, tab)
        return ws
    except Exception:
        # Stale spreadsheet object — force refresh and retry
        _sh = None
        _sh_ts = 0
        sh = get_spreadsheet()
        try:
            return sh.worksheet(tab)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab, rows=2000, cols=26)
            _init_headers(ws, tab)
            return ws

def get_records_cached(tab):
    """Get all records with caching. Mutations call invalidate_cache(tab)."""
    now = time.time()
    if tab in _sheet_cache:
        ts, data = _sheet_cache[tab]
        if now - ts < CACHE_TTL:
            return data
    try:
        ws = get_sheet(tab)
        # Use expected_headers to handle duplicate/empty column names in Google Sheets
        data = ws.get_all_records(expected_headers=_get_headers_for(tab))
    except Exception:
        try:
            # Fallback: try without expected_headers but with numericise_ignore
            data = ws.get_all_records(numericise_ignore=['all'])
        except Exception:
            data = []
    _sheet_cache[tab] = (now, data)
    return data

def _get_headers_for(tab):
    """Return expected headers for a tab, or None if unknown."""
    hdrs = {
        TAB_BUDGETS:    ["id","country","quarter","total_budget","updated_at"],
        TAB_CHANNELS:   ["id","country","quarter","name","budget","sort_order","created_at"],
        TAB_ACTIVITIES: ["id","channel_id","country","quarter","name","sort_order","created_at"],
        TAB_ENTRIES:    ENTRY_HEADERS,
        TAB_MAPPING:    MAPPING_HEADERS,
        TAB_VENDORS:    VENDOR_HEADERS,
    }
    return hdrs.get(tab, None)

def safe_get_records(ws, tab=None):
    """Get all records from a worksheet, handling duplicate headers gracefully."""
    try:
        hdrs = _get_headers_for(tab) if tab else None
        if hdrs:
            return ws.get_all_records(expected_headers=hdrs)
        return safe_get_records(ws)
    except Exception:
        try:
            return ws.get_all_records(numericise_ignore=['all'])
        except Exception:
            return []

def invalidate_cache(tab):
    _sheet_cache.pop(tab, None)

def rows_for_cached(tab, **filters):
    rows = get_records_cached(tab)
    for k, v in filters.items():
        rows = [r for r in rows if str(r.get(k,"")) == str(v)]
    return rows

def _init_headers(ws, tab):
    hdrs = {
        TAB_BUDGETS:    ["id","country","quarter","total_budget","updated_at"],
        TAB_CHANNELS:   ["id","country","quarter","name","budget","sort_order","created_at"],
        TAB_ACTIVITIES: ["id","channel_id","country","quarter","name","sort_order","created_at"],
        TAB_ENTRIES:    ENTRY_HEADERS,
        TAB_MAPPING:    MAPPING_HEADERS,
        TAB_VENDORS:    VENDOR_HEADERS,
    }
    if tab in hdrs:
        ws.append_row(hdrs[tab])
        ws.format("1:1", {
            "backgroundColor": {"red":0.11,"green":0.31,"blue":0.24},
            "textFormat": {"foregroundColor":{"red":1,"green":1,"blue":1},"bold":True}
        })

def _ensure_entry_headers():
    """Check if Entries sheet headers match expected. Fix if needed."""
    try:
        ws = get_sheet(TAB_ENTRIES)
        row1 = ws.row_values(1)
        if not row1:
            return

        # Check if activity_id column exists in headers
        if "activity_id" in row1 and "activity_name" in row1:
            return  # Already correct

        # The data rows were written with 24 columns (including activity_id/activity_name)
        # but the header row only has 22 columns (missing those two).
        # Fix: overwrite the header row with the correct 24 headers.
        # Data is already in the right positions — just the header labels are wrong/missing.
        ws.update('A1:X1', [ENTRY_HEADERS])
        ws.format("1:1", {
            "backgroundColor": {"red":0.11,"green":0.31,"blue":0.24},
            "textFormat": {"foregroundColor":{"red":1,"green":1,"blue":1},"bold":True}
        })
        print(f"[MIGRATION] Fixed Entries header row — added activity_id and activity_name columns")
        invalidate_cache(TAB_ENTRIES)
    except Exception as e:
        print(f"[MIGRATION] Could not fix Entries headers: {e}")

def rows_for(tab, **filters):
    try:
        ws = get_sheet(tab)
        hdrs = _get_headers_for(tab)
        rows = ws.get_all_records(expected_headers=hdrs) if hdrs else safe_get_records(ws)
    except Exception:
        try:
            rows = ws.get_all_records(numericise_ignore=['all'])
        except Exception:
            rows = []
    for k, v in filters.items():
        rows = [r for r in rows if str(r.get(k,"")) == str(v)]
    return rows

# ── AUTH DECORATORS ───────────────────────────────────────────
def require_login(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user"):
            return jsonify({"error":"Not authenticated"}), 401
        return f(*args, **kwargs)
    return decorated

def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get("user") != ADMIN_MARKET:
            return jsonify({"error":"Admin required"}), 403
        return f(*args, **kwargs)
    return decorated

def check_country_access(country):
    u = session.get("user")
    if u != ADMIN_MARKET and u != country:
        return False
    return True

# ── PAGES ─────────────────────────────────────────────────────
@app.route("/")
def index():
    if not session.get("user"):
        return render_template("login.html", markets=MARKETS)
    return render_template("app.html",
        user=session["user"],
        is_admin=(session["user"] == ADMIN_MARKET),
        markets=MARKETS, quarters=QUARTERS
    )

@app.route("/login", methods=["POST"])
def login():
    market = request.form.get("market","").strip()
    if market in MARKETS + [ADMIN_MARKET]:
        session["user"] = market
    return redirect("/")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# ── BUDGET API ────────────────────────────────────────────────
@app.route("/api/budget/<country>/<quarter>")
@require_login
def api_get_budget(country, quarter):
    if not check_country_access(country):
        return jsonify({"error":"Forbidden"}), 403
    try:
        brows = rows_for_cached(TAB_BUDGETS, country=country, quarter=quarter)
        total = float(brows[0]["total_budget"]) if brows else 0

        channels = sorted([
            {"id":r["id"],"name":r["name"],"budget":float(r["budget"] or 0),"sort_order":int(r.get("sort_order") or 0)}
            for r in rows_for_cached(TAB_CHANNELS, country=country, quarter=quarter)
        ], key=lambda x: x["sort_order"])

        # Attach activities to each channel
        all_acts = rows_for_cached(TAB_ACTIVITIES, country=country, quarter=quarter)
        for ch in channels:
            ch["activities"] = sorted([
                {"id":a["id"],"name":a["name"],"sort_order":int(a.get("sort_order") or 0)}
                for a in all_acts if str(a["channel_id"]) == str(ch["id"])
            ], key=lambda x: x["sort_order"])

        # Load mapping for auto-fill
        try:
            mrows = get_records_cached(TAB_MAPPING)
            if not mrows:
                _seed_mapping()
                invalidate_cache(TAB_MAPPING)
                mrows = get_records_cached(TAB_MAPPING)
            mapping = [{"channel_keyword":r["channel_keyword"],"bu":r["bu"],"finance_cat":r["finance_cat"],"marketing_cat":r["marketing_cat"]} for r in mrows if r.get("channel_keyword")]
        except Exception:
            mapping = [{"channel_keyword":kw,"bu":bu,"finance_cat":fc,"marketing_cat":mc} for kw,bu,fc,mc in DEFAULT_MAPPING]

        return jsonify({"total":total, "channels":channels, "mapping":mapping})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Budget load failed: {str(e)}"}), 500

def _seed_mapping():
    ws = get_sheet(TAB_MAPPING)
    now = datetime.utcnow().isoformat()
    for kw,bu,fc,mc in DEFAULT_MAPPING:
        ws.append_row([kw,bu,fc,mc,"system",now])

# ── ACTIVITIES API ────────────────────────────────────────────
@app.route("/api/activities", methods=["POST"])
@require_login
@require_admin
def api_add_activity():
    d = request.get_json()
    existing = rows_for(TAB_ACTIVITIES, channel_id=d["channel_id"])
    act_id = "act_" + str(uuid.uuid4())[:8]
    get_sheet(TAB_ACTIVITIES).append_row([
        act_id, d["channel_id"], d["country"], d["quarter"],
        d["name"], len(existing), datetime.utcnow().isoformat()
    ])
    invalidate_cache(TAB_ACTIVITIES)
    return jsonify({"id":act_id,"name":d["name"],"sort_order":len(existing)})

@app.route("/api/activities/<act_id>", methods=["PUT"])
@require_login
@require_admin
def api_update_activity(act_id):
    d = request.get_json()
    ws = get_sheet(TAB_ACTIVITIES)
    rows = safe_get_records(ws, TAB_ACTIVITIES)
    idx = next((i for i,r in enumerate(rows) if r["id"]==act_id), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    r = rows[idx]
    ws.update(f"A{idx+2}:G{idx+2}", [[act_id, r["channel_id"], r["country"], r["quarter"], d.get("name",r["name"]), r.get("sort_order",0), r.get("created_at","")]])
    invalidate_cache(TAB_ACTIVITIES)
    return jsonify({"ok":True})

@app.route("/api/activities/<act_id>", methods=["DELETE"])
@require_login
@require_admin
def api_delete_activity(act_id):
    ws = get_sheet(TAB_ACTIVITIES)
    rows = safe_get_records(ws, TAB_ACTIVITIES)
    idx = next((i for i,r in enumerate(rows) if r["id"]==act_id), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    ws.delete_rows(idx+2)
    invalidate_cache(TAB_ACTIVITIES)
    return jsonify({"ok":True})

# ── MAPPING API ──────────────────────────────────────────────
@app.route("/api/mapping")
@require_login
def api_get_mapping():
    try:
        mrows = get_records_cached(TAB_MAPPING)
        if not mrows:
            _seed_mapping()
            invalidate_cache(TAB_MAPPING)
            mrows = get_records_cached(TAB_MAPPING)
        mapping = [{"channel_keyword":r["channel_keyword"],"bu":r["bu"],"finance_cat":r["finance_cat"],"marketing_cat":r["marketing_cat"]} for r in mrows if r.get("channel_keyword")]
    except Exception:
        mapping = [{"channel_keyword":kw,"bu":bu,"finance_cat":fc,"marketing_cat":mc} for kw,bu,fc,mc in DEFAULT_MAPPING]
    return jsonify(mapping)

# ── MAPPING SAVE ──────────────────────────────────────────────
@app.route("/api/mapping", methods=["POST"])
@require_login
@require_admin
def api_save_mapping():
    mappings = request.get_json()
    ws = get_sheet(TAB_MAPPING)
    ws.clear()
    ws.append_row(MAPPING_HEADERS)
    ws.format("1:1", {"backgroundColor":{"red":0.11,"green":0.31,"blue":0.24},"textFormat":{"foregroundColor":{"red":1,"green":1,"blue":1},"bold":True}})
    now = datetime.utcnow().isoformat()
    for m in mappings:
        ws.append_row([m.get("channel_keyword",""), m.get("bu",""), m.get("finance_cat",""), m.get("marketing_cat",""), session["user"], now])
    invalidate_cache(TAB_MAPPING)
    return jsonify({"ok":True, "count":len(mappings)})

# ── VENDORS API ──────────────────────────────────────────────
@app.route("/api/vendors")
@require_login
def api_get_vendors():
    """Returns all vendors. Global vendors have country='GLOBAL', market-specific have country code."""
    try:
        vendors = get_records_cached(TAB_VENDORS)
        user = session.get("user","")
        # Everyone sees global + their own market vendors
        if user != ADMIN_MARKET:
            vendors = [v for v in vendors if v.get("country") in ("GLOBAL", user)]
        result = [{"id":v["id"],"name":v["name"],"country":v.get("country","GLOBAL")} for v in vendors]
        # Deduplicate by name (case-insensitive)
        seen = set()
        unique = []
        for v in result:
            key = v["name"].lower()
            if key not in seen:
                seen.add(key)
                unique.append(v)
        return jsonify(sorted(unique, key=lambda x: x["name"].lower()))
    except Exception:
        return jsonify([])

@app.route("/api/vendors", methods=["POST"])
@require_login
def api_add_vendor():
    d = request.get_json()
    name = d.get("name","").strip()
    if not name:
        return jsonify({"error":"Name required"}), 400
    # Admin can add global, market users add to their market
    user = session.get("user","")
    vendor_country = d.get("country", "GLOBAL" if user == ADMIN_MARKET else user)
    vid = "v_" + str(uuid.uuid4())[:8]
    now = datetime.utcnow().isoformat()
    get_sheet(TAB_VENDORS).append_row([vid, name, vendor_country, user, now])
    invalidate_cache(TAB_VENDORS)
    return jsonify({"id":vid, "name":name, "country":vendor_country})

@app.route("/api/vendors/<vid>", methods=["DELETE"])
@require_login
@require_admin
def api_delete_vendor(vid):
    ws = get_sheet(TAB_VENDORS)
    rows = safe_get_records(ws, TAB_VENDORS)
    idx = next((i for i,r in enumerate(rows) if r["id"]==vid), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    ws.delete_rows(idx+2)
    invalidate_cache(TAB_VENDORS)
    return jsonify({"ok":True})

@app.route("/api/vendors/import", methods=["POST"])
@require_login
@require_admin
def api_import_vendors():
    """Bulk import vendors from CSV/XLSX. Columns: Name, Country (optional, defaults to GLOBAL)"""
    if 'file' not in request.files:
        return jsonify({"error":"No file"}), 400
    f = request.files['file']
    fname = f.filename.lower()
    parsed = []
    try:
        if fname.endswith(('.xlsx','.xls')):
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(f.read()), data_only=True, read_only=True)
            ws_xl = wb.active
            all_rows = list(ws_xl.iter_rows(values_only=True))
            wb.close()
            first = [str(v or '').lower() for v in all_rows[0]] if all_rows else []
            start = 1 if any(k in ' '.join(first) for k in ['name','vendor']) else 0
            for row in all_rows[start:]:
                vals = [str(v).strip() if v is not None else '' for v in row]
                if vals and vals[0]: parsed.append(vals)
        else:
            content = f.read().decode('utf-8-sig')
            lines = [l.strip() for l in content.splitlines() if l.strip()]
            if not lines:
                return jsonify({"error": "Empty file"}), 400
            first = lines[0].lower()
            has_hdr = 'name' in first or 'vendor' in first
            data_lines = lines[1:] if has_hdr else lines
            import csv as _csv
            for row in _csv.reader(data_lines):
                if row and row[0].strip(): parsed.append(row)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error":f"Parse failed: {str(e)}"}), 400

    if not parsed:
        return jsonify({"error": "No valid rows found in file"}), 400

    try:
        ws = get_sheet(TAB_VENDORS)
        try:
            existing = [str(v.get("name","")).lower() for v in safe_get_records(ws, TAB_VENDORS)]
        except Exception:
            existing = []
        now = datetime.utcnow().isoformat()
        saved, skipped = 0, 0
        saved_rows = []
        for row in parsed:
            name = row[0].strip()
            vendor_country = row[1].strip().upper() if len(row)>1 and row[1].strip() else "GLOBAL"
            if name.lower() in existing:
                skipped += 1; continue
            vid = "v_" + str(uuid.uuid4())[:8]
            ws.append_row([vid, name, vendor_country, session["user"], now])
            existing.append(name.lower())
            saved += 1
            saved_rows.append({"name":name, "country":vendor_country})
        invalidate_cache(TAB_VENDORS)
        return jsonify({"ok":True, "saved":saved, "skipped":skipped, "rows":saved_rows})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Import failed: {str(e)}"}), 500

@app.route("/api/budget/<country>/<quarter>", methods=["POST"])
@require_login
@require_admin
def api_save_budget(country, quarter):
    total = float(request.get_json().get("total", 0))
    ws  = get_sheet(TAB_BUDGETS)
    rows = safe_get_records(ws, TAB_BUDGETS)
    now = datetime.utcnow().isoformat()
    idx = next((i for i,r in enumerate(rows) if r["country"]==country and r["quarter"]==quarter), None)
    if idx is not None:
        ws.update(f"A{idx+2}:E{idx+2}", [[rows[idx]["id"], country, quarter, total, now]])
    else:
        ws.append_row([str(uuid.uuid4())[:8], country, quarter, total, now])
    invalidate_cache(TAB_BUDGETS)
    return jsonify({"ok":True})

# ── CHANNELS API ──────────────────────────────────────────────
@app.route("/api/channels", methods=["POST"])
@require_login
@require_admin
def api_add_channel():
    d = request.get_json()
    existing = rows_for(TAB_CHANNELS, country=d["country"], quarter=d["quarter"])
    ch_id = "ch_" + str(uuid.uuid4())[:8]
    get_sheet(TAB_CHANNELS).append_row([
        ch_id, d["country"], d["quarter"], d["name"],
        float(d.get("budget",0)), len(existing), datetime.utcnow().isoformat()
    ])
    invalidate_cache(TAB_CHANNELS)
    return jsonify({"id":ch_id,"name":d["name"],"budget":float(d.get("budget",0)),"sort_order":len(existing)})

@app.route("/api/channels/<ch_id>", methods=["PUT"])
@require_login
@require_admin
def api_update_channel(ch_id):
    d = request.get_json()
    ws = get_sheet(TAB_CHANNELS)
    rows = safe_get_records(ws, TAB_CHANNELS)
    idx = next((i for i,r in enumerate(rows) if r["id"]==ch_id), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    r = rows[idx]
    ws.update(f"A{idx+2}:G{idx+2}", [[
        ch_id, r["country"], r["quarter"],
        d.get("name", r["name"]), float(d.get("budget", r["budget"])),
        r.get("sort_order",0), r.get("created_at","")
    ]])
    invalidate_cache(TAB_CHANNELS)
    return jsonify({"ok":True})

@app.route("/api/channels/<ch_id>", methods=["DELETE"])
@require_login
@require_admin
def api_delete_channel(ch_id):
    ws = get_sheet(TAB_CHANNELS)
    rows = safe_get_records(ws, TAB_CHANNELS)
    idx = next((i for i,r in enumerate(rows) if r["id"]==ch_id), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    ws.delete_rows(idx+2)
    invalidate_cache(TAB_CHANNELS)
    return jsonify({"ok":True})

# ── ENTRIES API ───────────────────────────────────────────────
@app.route("/api/entries/<country>/<quarter>")
@require_login
def api_get_entries(country, quarter):
    if not check_country_access(country):
        return jsonify({"error":"Forbidden"}), 403
    try:
        rows = rows_for_cached(TAB_ENTRIES, country=country, quarter=quarter)
        entries = []
        for r in rows:
            entries.append({
                "id": r["id"], "country": r["country"], "quarter": r["quarter"],
                "month": r["month"], "channel_id": r["channel_id"], "channel_name": r["channel_name"],
                "bu": r["bu"], "finance_cat": r["finance_cat"], "marketing_cat": r["marketing_cat"],
                "activity_id": r.get("activity_id",""), "activity_name": r.get("activity_name",""),
                "description": r["description"],
                "planned":   float(r["planned"]   or 0),
                "confirmed": float(r["confirmed"] or 0),
                "actual":    float(r["actual"]    or 0),
                "jira": r["jira"], "vendor": r["vendor"], "notes": r["notes"],
                "approved": str(r["approved"]).lower() == "true",
                "invoice_names": json.loads(r["invoice_names"]) if r.get("invoice_names") else [],
                "entered_by": r["entered_by"], "updated_at": r["updated_at"],
            })
        return jsonify(entries)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Entries load failed: {str(e)}"}), 500

@app.route("/api/entries", methods=["POST"])
@require_login
def api_add_entry():
    d = request.get_json()
    country = d.get("country","")
    if not check_country_access(country):
        return jsonify({"error":"Forbidden"}), 403
    try:
        entry_id = "e_" + str(uuid.uuid4())[:10]
        now = datetime.utcnow().isoformat()

        # Save invoice files to disk, store only filenames in Sheets
        inv_names = d.get("invoice_names", [])
        inv_data_urls = d.get("invoice_data", [])
        stored_files = []
        for i, name in enumerate(inv_names):
            if i < len(inv_data_urls) and inv_data_urls[i]:
                stored = save_invoice_to_disk(inv_data_urls[i], entry_id, name)
                if stored:
                    stored_files.append(stored)

        get_sheet(TAB_ENTRIES).append_row([
            entry_id, country,
            d.get("quarter",""), d.get("month",""),
            d.get("channel_id",""), d.get("channel_name",""),
            d.get("activity_id",""), d.get("activity_name",""),
            d.get("bu",""), d.get("finance_cat",""), d.get("marketing_cat",""),
            d.get("description",""),
            float(d.get("planned") or 0), float(d.get("confirmed") or 0), float(d.get("actual") or 0),
            d.get("jira",""), d.get("vendor",""), d.get("notes",""),
            str(d.get("approved", False)),
            json.dumps(inv_names),
            json.dumps(stored_files),
            session["user"], now, now
        ])
        invalidate_cache(TAB_ENTRIES)
        return jsonify({"id":entry_id,"ok":True})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Save failed: {str(e)}"}), 500

@app.route("/api/entries/<entry_id>", methods=["PUT"])
@require_login
def api_update_entry(entry_id):
    d = request.get_json()
    try:
        ws = get_sheet(TAB_ENTRIES)
        rows = safe_get_records(ws, TAB_ENTRIES)
        idx = next((i for i,r in enumerate(rows) if str(r.get("id",""))==str(entry_id)), None)
        if idx is None: return jsonify({"error":"Not found"}), 404
        r = rows[idx]
        if not check_country_access(str(r.get("country",""))):
            return jsonify({"error":"Forbidden"}), 403

        approved = d.get("approved", str(r.get("approved","")).lower()=="true")
        jira = d.get("jira", r.get("jira",""))

        inv_names_new = d.get("invoice_names", None)
        inv_data_new  = d.get("invoice_data", None)

        existing_names = json.loads(str(r.get("invoice_names") or "[]"))
        existing_files = json.loads(str(r.get("invoice_data")  or "[]"))

        if inv_names_new is not None:
            new_data_urls = inv_data_new if inv_data_new else []
            final_names = []
            final_files = []
            for name in inv_names_new:
                if name in existing_names:
                    old_idx = existing_names.index(name)
                    final_names.append(name)
                    final_files.append(existing_files[old_idx] if old_idx < len(existing_files) else "")
                    existing_names[old_idx] = None
                elif new_data_urls:
                    data_url = new_data_urls.pop(0)
                    stored = save_invoice_to_disk(data_url, entry_id, name)
                    final_names.append(name)
                    final_files.append(stored or "")
                else:
                    final_names.append(name)
                    final_files.append("")
            inv_names = json.dumps(final_names)
            inv_data  = json.dumps(final_files)
        else:
            inv_names = json.dumps(existing_names)
            inv_data  = json.dumps(existing_files)
        now = datetime.utcnow().isoformat()

        # Update all 24 columns (A through X)
        ws.update(f"A{idx+2}:X{idx+2}", [[
            entry_id, r.get("country",""),
            d.get("quarter", r.get("quarter","")), d.get("month", r.get("month","")),
            d.get("channel_id", r.get("channel_id","")), d.get("channel_name", r.get("channel_name","")),
            d.get("activity_id", r.get("activity_id","")), d.get("activity_name", r.get("activity_name","")),
            d.get("bu", r.get("bu","")), d.get("finance_cat", r.get("finance_cat","")),
            d.get("marketing_cat", r.get("marketing_cat","")),
            d.get("description", r.get("description","")),
            float(d.get("planned", r.get("planned",0)) or 0),
            float(d.get("confirmed", r.get("confirmed",0)) or 0),
            float(d.get("actual", r.get("actual",0)) or 0),
            jira, d.get("vendor", r.get("vendor","")), d.get("notes", r.get("notes","")),
            str(approved), inv_names, inv_data,
            r.get("entered_by",""), r.get("created_at",""), now
        ]])
        invalidate_cache(TAB_ENTRIES)
        return jsonify({"ok":True})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Update failed: {str(e)}"}), 500

@app.route("/api/entries/<entry_id>", methods=["DELETE"])
@require_login
def api_delete_entry(entry_id):
    user = session["user"]
    ws = get_sheet(TAB_ENTRIES)
    rows = safe_get_records(ws, TAB_ENTRIES)
    idx = next((i for i,r in enumerate(rows) if str(r.get("id",""))==str(entry_id)), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    if user != ADMIN_MARKET and str(rows[idx].get("country","")) != user:
        return jsonify({"error":"Forbidden"}), 403
    ws.delete_rows(idx+2)
    invalidate_cache(TAB_ENTRIES)
    return jsonify({"ok":True})

# ── TEMPLATE DOWNLOADS ────────────────────────────────────────
@app.route("/api/channel_template")
@require_login
def api_channel_template():
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['Country', 'Quarter', 'Channel Name', 'Budget (USD)'])
    w.writerow(['TH', 'Q1', 'Performance Marketing', 50000])
    w.writerow(['TH', 'Q1', 'Affiliate', 30000])
    w.writerow(['TH', 'Q1', 'Paid Social', 25000])
    w.writerow(['TH', 'Q1', 'Regional Marketing', 20000])
    w.writerow(['SG', 'Q1', 'Performance Marketing', 40000])
    w.writerow(['SG', 'Q1', 'Affiliate', 20000])
    w.writerow(['MY', 'Q2', 'Performance Marketing', 35000])
    w.writerow(['MY', 'Q2', 'Paid Social', 15000])
    return send_file(io.BytesIO(out.getvalue().encode('utf-8-sig')),
                     mimetype='text/csv', as_attachment=True,
                     download_name='channel_budget_template.csv')

@app.route("/api/budget_template")
@require_login
def api_budget_template():
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['Country', 'Quarter', 'Total Budget (USD)'])
    for country in ['TH','SG','MY','CN','HKG','ID','IN','VN','PH','TW','MN']:
        for q in ['Q1','Q2','Q3','Q4']:
            w.writerow([country, q, 0])
    return send_file(io.BytesIO(out.getvalue().encode('utf-8-sig')),
                     mimetype='text/csv', as_attachment=True,
                     download_name='country_budget_template.csv')

# ── INVOICE DOWNLOAD ──────────────────────────────────────────
@app.route("/api/invoice/<entry_id>/<int:inv_idx>")
@require_login
def api_invoice(entry_id, inv_idx):
    rows = safe_get_records(get_sheet(TAB_ENTRIES), TAB_ENTRIES)
    r = next((row for row in rows if row["id"]==entry_id), None)
    if not r: return "Not found", 404
    names = json.loads(r.get("invoice_names") or "[]")
    datas = json.loads(r.get("invoice_data")  or "[]")
    if inv_idx >= len(datas): return "Not found", 404
    name = names[inv_idx] if inv_idx < len(names) else f"invoice_{inv_idx}"
    stored = datas[inv_idx]

    # New format: stored filename on disk
    if stored and not stored.startswith("data:"):
        path = get_invoice_path(stored)
        if path:
            import mimetypes
            mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
            return send_file(path, mimetype=mime, as_attachment=True, download_name=name)
        return "File not found on disk", 404

    # Legacy format: base64 data URL stored in Sheets
    if stored and stored.startswith("data:"):
        try:
            header, b64 = stored.split(",", 1)
            mime = header.split(";")[0].replace("data:","")
            return send_file(io.BytesIO(base64.b64decode(b64)), mimetype=mime,
                             as_attachment=True, download_name=name)
        except Exception:
            return "Invalid invoice data", 500

    return "No file data", 404

# ── BULK UPLOAD: EXCEL PARSER ────────────────────────────────
@app.route("/api/parse_bulk", methods=["POST"])
@require_login
def api_parse_bulk():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    f = request.files['file']
    if not f.filename.lower().endswith(('.xlsx', '.xls')):
        return jsonify({"error": "Please upload an .xlsx or .xls file"}), 400
    try:
        import openpyxl, io as _io
        wb = openpyxl.load_workbook(_io.BytesIO(f.read()), data_only=True, read_only=True)
        ws = wb.active
        rows_out = []
        all_rows = list(ws.iter_rows(values_only=True))
        # Detect header
        start = 0
        if all_rows:
            first = [str(v or '').lower() for v in all_rows[0]]
            if any(k in ' '.join(first) for k in ['month','vendor','planned','campaign']):
                start = 1
        for row in all_rows[start:]:
            vals = [str(v).strip() if v is not None else '' for v in row]
            if not any(vals): continue
            def safe_float(s):
                try: return float(str(s).replace(',','').replace('$','').strip() or 0)
                except: return 0
            rows_out.append({
                'month':     vals[0] if len(vals)>0 else '',
                'campaign':  vals[1] if len(vals)>1 else '',
                'desc':      vals[2] if len(vals)>2 else '',
                'vendor':    vals[3] if len(vals)>3 else '',
                'planned':   safe_float(vals[4]) if len(vals)>4 else 0,
                'confirmed': safe_float(vals[5]) if len(vals)>5 else 0,
                'actual':    safe_float(vals[6]) if len(vals)>6 else 0,
                'jira':      vals[7] if len(vals)>7 else '',
                'notes':     vals[8] if len(vals)>8 else '',
            })
        wb.close()
        return jsonify({"rows": rows_out})
    except Exception as e:
        return jsonify({"error": f"Could not parse file: {str(e)}"}), 400

# ── BULK UPLOAD: ENTRY TEMPLATE ──────────────────────────────
@app.route("/api/bulk_template")
@require_login
def api_bulk_template():
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['Month','Activity','Description','Vendor',
                'Planned (USD)','Confirmed (USD)','Actual (USD)','JIRA Task','Notes'])
    w.writerow(['Jul 2025','Google Search Q1','Brand keywords','Google',5000,'',3200,'MKT-001',''])
    w.writerow(['Aug 2025','Meta Retargeting','Retargeting campaign','Meta',3000,'',2800,'MKT-002',''])
    w.writerow(['Sep 2025','Bing Display','Display network','Bing',2000,'',1500,'','Paused mid-month'])
    return send_file(
        io.BytesIO(out.getvalue().encode('utf-8-sig')),
        mimetype='text/csv', as_attachment=True,
        download_name='bulk_entry_template.csv'
    )

# ── BULK CHANNEL IMPORT ────────────────────────────────────────
@app.route("/api/import/channels", methods=["POST"])
@require_login
@require_admin
def api_import_channels():
    """
    Accepts CSV or XLSX with columns: Country, Quarter, Channel Name, Budget
    Creates channels (and budget records if missing) for each row.
    Returns the actual rows that were saved for preview.
    """
    if 'file' not in request.files:
        return jsonify({"error": "No file"}), 400
    f = request.files['file']
    fname = f.filename.lower()

    # Parse file into rows
    parsed_rows = []
    try:
        if fname.endswith(('.xlsx', '.xls')):
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(f.read()), data_only=True, read_only=True)
            ws = wb.active
            all_rows = list(ws.iter_rows(values_only=True))
            wb.close()
            if not all_rows:
                return jsonify({"error": "Empty file"}), 400
            first = [str(v or '').lower() for v in all_rows[0]]
            start = 1 if any(k in ' '.join(first) for k in ['country','quarter','channel','name']) else 0
            for row in all_rows[start:]:
                vals = [str(v).strip() if v is not None else '' for v in row]
                if any(vals):
                    parsed_rows.append(vals)
        else:
            content = f.read().decode('utf-8-sig')
            lines = [l.strip() for l in content.splitlines() if l.strip()]
            if not lines:
                return jsonify({"error": "Empty file"}), 400
            first = lines[0].lower()
            has_header = 'country' in first or 'quarter' in first or 'channel' in first
            data_lines = lines[1:] if has_header else lines
            import csv as _csv
            parsed_rows = list(_csv.reader(data_lines))
    except Exception as e:
        return jsonify({"error": f"Could not parse file: {str(e)}"}), 400

    saved, skipped = 0, 0
    saved_rows = []
    skipped_dups = 0
    skipped_invalid = 0
    ws_ch = get_sheet(TAB_CHANNELS)
    ws_bud = get_sheet(TAB_BUDGETS)
    existing_ch = safe_get_records(ws_ch, TAB_CHANNELS)
    existing_bud = safe_get_records(ws_bud, TAB_BUDGETS)
    now = datetime.utcnow().isoformat()

    for row in parsed_rows:
        if len(row) < 3:
            skipped += 1
            skipped_invalid += 1
            continue
        if len(row) >= 4:
            country_val = row[0].strip()
            quarter_val = row[1].strip().upper()
            name_val    = row[2].strip()
            try:
                budget_val = float(str(row[3]).replace(',','').replace('$','').strip() or 0)
            except:
                budget_val = 0
        else:
            country_val = request.form.get('country', '')
            quarter_val = row[0].strip().upper()
            name_val    = row[1].strip()
            try:
                budget_val = float(str(row[2]).replace(',','').replace('$','').strip() or 0)
            except:
                budget_val = 0

        if not country_val or not quarter_val or not name_val:
            skipped += 1
            skipped_invalid += 1
            continue

        if not quarter_val.startswith('Q'):
            quarter_val = 'Q' + quarter_val

        # Create budget record if it doesn't exist
        has_bud = any(r['country']==country_val and r['quarter']==quarter_val for r in existing_bud)
        if not has_bud:
            ws_bud.append_row([str(uuid.uuid4())[:8], country_val, quarter_val, 0, now])
            existing_bud.append({'country':country_val,'quarter':quarter_val,'total_budget':0})

        # Skip duplicate channel
        dup = any(r['country']==country_val and r['quarter']==quarter_val and str(r['name']).strip()==name_val for r in existing_ch)
        if dup:
            skipped += 1
            skipped_dups += 1
            continue

        sort_order = len([r for r in existing_ch if r['country']==country_val and r['quarter']==quarter_val])
        ch_id = "ch_" + str(uuid.uuid4())[:8]
        ws_ch.append_row([ch_id, country_val, quarter_val, name_val, budget_val, sort_order, now])
        existing_ch.append({'id':ch_id,'country':country_val,'quarter':quarter_val,'name':name_val,'budget':budget_val,'sort_order':sort_order})
        saved += 1
        saved_rows.append({"country":country_val, "quarter":quarter_val, "name":name_val, "budget":budget_val})

    invalidate_cache(TAB_CHANNELS)
    invalidate_cache(TAB_BUDGETS)
    return jsonify({
        "ok": True, "saved": saved, "skipped": skipped,
        "skipped_dups": skipped_dups, "skipped_invalid": skipped_invalid,
        "rows": saved_rows,
        "total_parsed": len(parsed_rows),
    })

# ── BULK BUDGET IMPORT ─────────────────────────────────────────
@app.route("/api/import/budgets", methods=["POST"])
@require_login
@require_admin
def api_import_budgets():
    """
    Accepts CSV or XLSX with columns: Country, Quarter, Total Budget
    Sets country-level total budgets in bulk.
    """
    if 'file' not in request.files:
        return jsonify({"error": "No file"}), 400
    f = request.files['file']
    fname = f.filename.lower()

    parsed_rows = []
    try:
        if fname.endswith(('.xlsx', '.xls')):
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(f.read()), data_only=True, read_only=True)
            ws_xl = wb.active
            all_rows = list(ws_xl.iter_rows(values_only=True))
            wb.close()
            if not all_rows:
                return jsonify({"error": "Empty file"}), 400
            first = [str(v or '').lower() for v in all_rows[0]]
            start = 1 if any(k in ' '.join(first) for k in ['country','quarter','budget']) else 0
            for row in all_rows[start:]:
                vals = [str(v).strip() if v is not None else '' for v in row]
                if any(vals):
                    parsed_rows.append(vals)
        else:
            content = f.read().decode('utf-8-sig')
            lines = [l.strip() for l in content.splitlines() if l.strip()]
            if not lines:
                return jsonify({"error": "Empty file"}), 400
            first = lines[0].lower()
            has_header = 'country' in first or 'quarter' in first or 'budget' in first
            data_lines = lines[1:] if has_header else lines
            import csv as _csv
            parsed_rows = list(_csv.reader(data_lines))
    except Exception as e:
        return jsonify({"error": f"Could not parse file: {str(e)}"}), 400

    ws = get_sheet(TAB_BUDGETS)
    existing = safe_get_records(ws, TAB_BUDGETS)
    now = datetime.utcnow().isoformat()
    saved, skipped = 0, 0
    saved_rows = []

    for row in parsed_rows:
        if len(row) < 3:
            skipped += 1
            continue
        country_val = row[0].strip()
        quarter_val = row[1].strip().upper()
        if not quarter_val.startswith('Q'):
            quarter_val = 'Q' + quarter_val
        try:
            total = float(str(row[2]).replace(',','').replace('$','').strip() or 0)
        except:
            skipped += 1
            continue
        if not country_val or not quarter_val:
            skipped += 1
            continue

        idx = next((i for i,r in enumerate(existing) if r['country']==country_val and r['quarter']==quarter_val), None)
        if idx is not None:
            ws.update(f"A{idx+2}:E{idx+2}", [[existing[idx]['id'], country_val, quarter_val, total, now]])
            existing[idx]['total_budget'] = total
        else:
            ws.append_row([str(uuid.uuid4())[:8], country_val, quarter_val, total, now])
            existing.append({'country':country_val,'quarter':quarter_val,'total_budget':total})
        saved += 1
        saved_rows.append({"country":country_val, "quarter":quarter_val, "total":total})

    invalidate_cache(TAB_BUDGETS)
    return jsonify({"ok": True, "saved": saved, "skipped": skipped, "rows": saved_rows})

# ── BUDGET SUMMARY (all markets) ─────────────────────────────
@app.route("/api/budget_summary")
@require_login
@require_admin
def api_budget_summary():
    """Returns all budget records with their channel allocation totals."""
    try:
        budgets = get_records_cached(TAB_BUDGETS)
        channels = get_records_cached(TAB_CHANNELS)
        result = []
        for b in budgets:
            co, q = str(b["country"]), str(b["quarter"])
            total = float(b.get("total_budget") or 0)
            ch_list = [c for c in channels if str(c["country"])==co and str(c["quarter"])==q]
            allocated = sum(float(c.get("budget") or 0) for c in ch_list)
            result.append({
                "country": co, "quarter": q,
                "total": total, "allocated": allocated,
                "channels": len(ch_list),
                "deviation": allocated - total,
            })
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ── RECONCILIATION ───────────────────────────────────────────
@app.route("/api/reconciliation/<quarter>")
@require_login
@require_admin
def api_reconciliation(quarter):
    """
    Full hierarchy reconciliation using rows_for_cached (same filtering as dashboard).
    """
    try:
        # Get all budgets for this quarter
        all_budgets = get_records_cached(TAB_BUDGETS)
        all_channels = get_records_cached(TAB_CHANNELS)
        all_activities = get_records_cached(TAB_ACTIVITIES)

        # Find all markets with budget or entries for this quarter
        q_budgets = [b for b in all_budgets if str(b.get("quarter",""))==quarter]
        markets_from_budgets = {str(b["country"]) for b in q_budgets}

        # Also check entries — use rows_for_cached which does str() comparison
        all_q_entries = rows_for_cached(TAB_ENTRIES, quarter=quarter)
        markets_from_entries = {str(e["country"]) for e in all_q_entries}

        all_markets = sorted(markets_from_budgets | markets_from_entries)

        result = []
        for mkt in all_markets:
            # Budget for this market+quarter
            brow = next((b for b in q_budgets if str(b["country"])==mkt), None)
            plan_budget = float(brow["total_budget"]) if brow else 0

            # Channels for this market+quarter
            mkt_channels = [c for c in all_channels if str(c.get("country",""))==mkt and str(c.get("quarter",""))==quarter]

            # Entries for this market+quarter — use rows_for_cached
            mkt_entries = rows_for_cached(TAB_ENTRIES, country=mkt, quarter=quarter)

            # Build channel hierarchy
            ch_data = []
            assigned_entry_ids = set()
            for ch in sorted(mkt_channels, key=lambda c: int(c.get("sort_order") or 0)):
                ch_id_str = str(ch["id"])
                ch_entries = [e for e in mkt_entries if str(e.get("channel_id",""))==ch_id_str]
                ch_acts = [a for a in all_activities if str(a.get("channel_id",""))==ch_id_str and str(a.get("country",""))==mkt and str(a.get("quarter",""))==quarter]

                act_data = []
                act_entry_ids = set()
                for act in sorted(ch_acts, key=lambda a: int(a.get("sort_order") or 0)):
                    act_id_str = str(act["id"])
                    a_entries = [e for e in ch_entries if str(e.get("activity_id",""))==act_id_str]
                    a_items = []
                    for e in a_entries:
                        act_entry_ids.add(str(e["id"]))
                        assigned_entry_ids.add(str(e["id"]))
                        a_items.append(_entry_summary(e))
                    act_data.append({
                        "id": act["id"], "name": act["name"],
                        "planned": sum(i["planned"] for i in a_items),
                        "confirmed": sum(i["confirmed"] for i in a_items),
                        "actual": sum(i["actual"] for i in a_items),
                        "entries": len(a_items), "items": a_items,
                    })

                unassigned = [e for e in ch_entries if str(e["id"]) not in act_entry_ids]
                unassigned_items = []
                for e in unassigned:
                    assigned_entry_ids.add(str(e["id"]))
                    unassigned_items.append(_entry_summary(e))

                ch_pln = sum(float(e.get("planned") or 0) for e in ch_entries)
                ch_con = sum(float(e.get("confirmed") or 0) for e in ch_entries)
                ch_act_val = sum(float(e.get("actual") or 0) for e in ch_entries)

                ch_data.append({
                    "id": ch["id"], "name": ch["name"],
                    "budget": float(ch.get("budget") or 0),
                    "planned": ch_pln, "confirmed": ch_con, "actual": ch_act_val,
                    "entries": len(ch_entries),
                    "activities": act_data,
                    "unassigned": unassigned_items,
                })

            orphan_entries = [e for e in mkt_entries if str(e["id"]) not in assigned_entry_ids]
            orphan_items = [_entry_summary(e) for e in orphan_entries]

            sum_planned   = sum(float(e.get("planned") or 0) for e in mkt_entries)
            sum_confirmed = sum(float(e.get("confirmed") or 0) for e in mkt_entries)
            sum_actual    = sum(float(e.get("actual") or 0) for e in mkt_entries)

            no_actual  = sum(1 for e in mkt_entries if float(e.get("planned") or 0) > 0 and float(e.get("actual") or 0) == 0)
            no_jira    = sum(1 for e in mkt_entries if not e.get("jira"))
            no_invoice = sum(1 for e in mkt_entries if float(e.get("actual") or 0) > 0 and not str(e.get("invoice_names","[]")).strip("[]"))

            result.append({
                "country": mkt, "plan_budget": plan_budget,
                "ch_allocated": sum(float(c.get("budget") or 0) for c in mkt_channels),
                "sum_planned": sum_planned, "sum_confirmed": sum_confirmed, "sum_actual": sum_actual,
                "entries": len(mkt_entries), "channels_data": ch_data,
                "orphan_entries": orphan_items,
                "var_plan_vs_actual": sum_actual - sum_planned,
                "flags": {"no_actual":no_actual,"no_jira":no_jira,"no_invoice":no_invoice},
            })
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def _entry_summary(e):
    return {
        "id": e["id"], "month": e.get("month",""), "description": e.get("description",""),
        "vendor": e.get("vendor",""), "activity_name": e.get("activity_name",""),
        "planned": float(e.get("planned") or 0), "confirmed": float(e.get("confirmed") or 0),
        "actual": float(e.get("actual") or 0), "jira": e.get("jira",""),
        "approved": str(e.get("approved","")).lower()=="true",
    }

# ── DEBUG: raw sheet data viewer ─────────────────────────────
@app.route("/api/debug/entries")
@require_login
@require_admin
def api_debug_entries():
    """Shows raw entry data from Google Sheets for debugging."""
    try:
        ws = get_sheet(TAB_ENTRIES)
        raw = safe_get_records(ws, TAB_ENTRIES)
        # Show first 10 entries with all fields and types
        debug = []
        for r in raw[:20]:
            debug.append({k: {"val": str(v), "type": type(v).__name__} for k, v in r.items()})
        # Also show what rows_for_cached returns for TH/Q4
        cached = rows_for_cached(TAB_ENTRIES, country="TH", quarter="Q4")
        return jsonify({
            "total_raw": len(raw),
            "sample_raw": debug,
            "cached_TH_Q4": len(cached),
            "cached_sample": [{k: str(v) for k,v in e.items()} for e in cached[:5]],
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ── CAMPAIGNS (cross-country activity view) ──────────────────
@app.route("/api/campaigns")
@require_login
@require_admin
def api_campaigns():
    """
    Groups entries by activity_name across all countries.
    Returns campaign-level rollups with per-country breakdown.
    """
    try:
        entries = get_records_cached(TAB_ENTRIES)
        q_filter = request.args.get("quarter", "")

        if q_filter:
            entries = [e for e in entries if str(e.get("quarter","")) == q_filter]

        # Group by activity_name (campaign)
        from collections import defaultdict
        campaigns = defaultdict(lambda: {"countries": defaultdict(lambda: {
            "planned":0,"confirmed":0,"actual":0,"entries":0,"channel":"",
        })})

        for e in entries:
            act_name = e.get("activity_name") or e.get("description") or "(no campaign)"
            co = e["country"]
            c = campaigns[act_name]["countries"][co]
            c["planned"]   += float(e.get("planned")   or 0)
            c["confirmed"] += float(e.get("confirmed") or 0)
            c["actual"]    += float(e.get("actual")    or 0)
            c["entries"]   += 1
            if not c["channel"]:
                c["channel"] = e.get("channel_name", "")

        result = []
        for name, data in campaigns.items():
            countries = []
            tot_pln = tot_con = tot_act = tot_ent = 0
            for co, vals in sorted(data["countries"].items()):
                countries.append({"country":co, **vals})
                tot_pln += vals["planned"]
                tot_con += vals["confirmed"]
                tot_act += vals["actual"]
                tot_ent += vals["entries"]
            result.append({
                "campaign": name,
                "countries": countries,
                "total_planned": tot_pln,
                "total_confirmed": tot_con,
                "total_actual": tot_act,
                "total_entries": tot_ent,
                "market_count": len(countries),
                "variance": tot_act - tot_pln,
            })

        # Sort by total planned descending
        result.sort(key=lambda x: x["total_planned"], reverse=True)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ── CSV EXPORT ────────────────────────────────────────────────
@app.route("/api/export")
@require_login
def api_export():
    user = session["user"]
    rows = safe_get_records(get_sheet(TAB_ENTRIES), TAB_ENTRIES)
    if user != ADMIN_MARKET:
        rows = [r for r in rows if r["country"]==user]
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Country","Quarter","Month","Channel","Activity",
                "BU","Finance Category","Marketing Category","Description",
                "Planned (USD)","Confirmed (USD)","Actual (USD)",
                "JIRA","Vendor","Notes","Approved","Invoices","Entered By","Updated At"])
    for r in rows:
        inv_count = len(json.loads(r.get("invoice_names") or "[]"))
        w.writerow([
            r["country"], r["quarter"], r["month"], r["channel_name"],
            r.get("activity_name",""),
            r["bu"], r["finance_cat"], r["marketing_cat"], r["description"],
            r["planned"], r["confirmed"], r["actual"],
            r["jira"], r["vendor"], r["notes"],
            "Yes" if str(r["approved"])=="True" else "No",
            f"{inv_count} file(s)", r["entered_by"], r["updated_at"]
        ])
    fname = f"APAC_Budget_{'ALL' if user==ADMIN_MARKET else user}_{datetime.now().strftime('%Y-%m-%d')}.csv"
    return send_file(io.BytesIO(out.getvalue().encode("utf-8-sig")),
                     mimetype="text/csv", as_attachment=True, download_name=fname)

# ── ADMIN OVERVIEW ────────────────────────────────────────────
@app.route("/api/admin/overview")
@require_login
@require_admin
def api_admin_overview():
    budgets = safe_get_records(get_sheet(TAB_BUDGETS), TAB_BUDGETS)
    entries = safe_get_records(get_sheet(TAB_ENTRIES), TAB_ENTRIES)
    result = []
    for b in budgets:
        ces = [e for e in entries if str(e["country"])==str(b["country"]) and str(e["quarter"])==str(b["quarter"])]
        result.append({
            "country": b["country"], "quarter": b["quarter"],
            "total": float(b["total_budget"] or 0),
            "planned": sum(float(e.get("planned") or 0) for e in ces),
            "actual":  sum(float(e.get("actual")  or 0) for e in ces),
            "entries": len(ces),
        })
    return jsonify(result)

# ── XLSX EXPORT (Finance APAC format) ─────────────────────────
@app.route("/api/export/xlsx")
@require_login
def api_export_xlsx():
    from export_xlsx import build_finance_export
    import tempfile

    user = session["user"]

    # Pull data from Sheets
    all_entries  = safe_get_records(get_sheet(TAB_ENTRIES), TAB_ENTRIES)
    all_channels = safe_get_records(get_sheet(TAB_CHANNELS), TAB_CHANNELS)
    all_budgets  = safe_get_records(get_sheet(TAB_BUDGETS), TAB_BUDGETS)

    if user != ADMIN_MARKET:
        all_entries  = [e for e in all_entries if str(e.get("country","")) == user]
        all_channels = [c for c in all_channels if str(c.get("country","")) == user]
        all_budgets  = [b for b in all_budgets  if str(b.get("country","")) == user]

    # Generate
    tmp = tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False)
    tmp.close()

    try:
        build_finance_export(tmp.name, all_entries, all_channels, all_budgets)
    except Exception as e:
        import traceback; traceback.print_exc()
        os.unlink(tmp.name)
        return jsonify({"error": f"Export failed: {str(e)}"}), 500

    fname = f"APAC_Marketing_FY26_{'ALL' if user==ADMIN_MARKET else user}_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    response = send_file(tmp.name, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True, download_name=fname)

    @response.call_on_close
    def cleanup():
        try: os.unlink(tmp.name)
        except: pass

    return response

if __name__ == "__main__":
    # Auto-fix missing columns on startup
    try:
        _ensure_entry_headers()
    except Exception as e:
        print(f"[STARTUP] Migration check failed: {e}")
    app.run(debug=True, port=5000)