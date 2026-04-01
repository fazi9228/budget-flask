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
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-in-production")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_CREDS", "credentials.json")
SHEET_ID             = os.environ.get("SHEET_ID", "13TMeZ3pqdUQr2WRMG5G70xchZKfmiPVOShZChqbUsv4")
PM_SHEET_ID          = os.environ.get("PM_SHEET_ID", "1_gqrbmEvmVYu3_Bu5IrJa2zKE9DWcZZQEmMvYnfuR4I")
ADMIN_MARKET         = "APAC"

MARKETS  = ["CN","HKG","ID","IN","MN","MY","PH","SG","TH","TW","VN","APAC"]
QUARTERS = ["Q1","Q2","Q3","Q4"]

# Invoice storage on disk
INVOICE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "invoices")
os.makedirs(INVOICE_DIR, exist_ok=True)

def save_invoice_to_disk(data_url, entry_id, filename):
    try:
        header, b64 = data_url.split(",", 1)
        data = base64.b64decode(b64)
        safe_name = "".join(c for c in filename if c.isalnum() or c in "._- ")[:100]
        stored_name = f"{entry_id}_{uuid.uuid4().hex[:6]}_{safe_name}"
        path = os.path.join(INVOICE_DIR, stored_name)
        with open(path, "wb") as f:
            f.write(data)
        return stored_name
    except Exception:
        return None

def get_invoice_path(stored_name):
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
TAB_USERS      = "Users"
TAB_CATEGORIES = "Categories"

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
USER_HEADERS    = ["username","password_hash","display_name","role","markets","created_at"]
CATEGORY_HEADERS = ["id","type","value","sort_order","created_at"]

# Default users
DEFAULT_USERS = [
    ("pepper", "APAC@123", "Pepper (Admin)", "admin", "ALL"),
    ("affiliate", "Affiliate@123", "Affiliate Manager", "editor", "ALL"),
    ("performance", "Performance@123", "Performance Marketing", "editor", "ALL"),
    ("campaigns", "Campaigns@123", "Campaign Manager", "editor", "ALL"),
    ("th_sales", "TH@123", "Thailand Sales", "country", "TH"),
    ("sg_sales", "SG@123", "Singapore Sales", "country", "SG"),
    ("cn_sales", "CN@123", "China Sales", "country", "CN"),
    ("hkg_sales", "HKG@123", "Hong Kong Sales", "country", "HKG"),
    ("id_sales", "ID@123", "Indonesia Sales", "country", "ID"),
    ("in_sales", "IN@123", "India Sales", "country", "IN"),
    ("my_sales", "MY@123", "Malaysia Sales", "country", "MY"),
    ("vn_sales", "VN@123", "Vietnam Sales", "country", "VN"),
    ("tw_sales", "TW@123", "Taiwan Sales", "country", "TW"),
    ("ph_sales", "PH@123", "Philippines Sales", "country", "PH"),
    ("mn_sales", "MN@123", "Mongolia Sales", "country", "MN"),
]

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

# Default BU / Finance / Marketing categories (seeded to Categories tab)
DEFAULT_BU_LIST = [
    "Marketing : Programmatic - 613000009XXX",
    "Marketing : Affiliate - 613000002XXX",
    "Marketing : Paid Social / YouTube - 613000004XXX",
    "Marketing : Local Brand - 613000024XXX",
    "Marketing : Premium - 613000019XXX",
    "Marketing : Partners - 613000022XXX",
    "Marketing : Marketing technology",
    "Marketing - Refer a friend - 613000003XXX",
    "AMF1",
]
DEFAULT_FIN_CATS = [
    "Affiliate","PPC","PMAX","Programmatic","Bing","Paid Social-Meta",
    "Paid Social-Twitter","Paid Social-Youtube","Paid Social-Douyin",
    "Paid Social-Weibo","Paid Social-Rednote","Paid Social-Wechat",
    "Paid Social-KOC","Baidu-Display","Baidu-PPC","Event",
    "Local Direct Deals","Influencer/KOL","Local SEO",
    "Campaigns/Promotions","Partner","Premium","RAF",
    "AMF1 Race Tickets","AMF1 Activation","Marketing Technology",
    "Education (Internal)",
]
DEFAULT_MKT_CATS = [
    "Affiliate- CPA & FF","PPC / Search","Programmatic","Paid Social",
    "YouTube","Display","Influencer / KOL","Events & Sponsorship",
    "Premium Partners","SEO","Email / CRM","Brand / OOH","Content",
    "Technology","AMF1","Refer a Friend","Other",
]

# PM data → tracker country mapping
PM_COUNTRY_MAP = {
    'TH':'TH','SG':'SG','CN':'CN','HK':'HKG',
    'ID':'ID','IN':'IN','MY':'MY','VN':'VN',
    'TW':'TW','PH':'PH','MN':'MN',
}

# PM channel group → tracker category mapping
PM_CHANNEL_MAP = {
    'Affiliates':       {'channel_name':'Affiliate','bu':'Marketing : Affiliate - 613000002XXX','finance_cat':'Affiliate','marketing_cat':'Affiliate- CPA & FF'},
    'Meta':             {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta','marketing_cat':'Paid Social'},
    'TikTok':           {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta','marketing_cat':'Paid Social'},
    'Bing':             {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'Bing','marketing_cat':'PPC / Search'},
    'AdRoll':           {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'Programmatic','marketing_cat':'Programmatic'},
    'RedNote':          {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Rednote','marketing_cat':'Paid Social'},
    'BiliBili':         {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta','marketing_cat':'Paid Social'},
    'TradingView':      {'channel_name':'Partner-Marketing Support','bu':'Marketing : Partners - 613000022XXX','finance_cat':'Partner','marketing_cat':'Premium Partners'},
    'Apple Search Ads': {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC','marketing_cat':'PPC / Search'},
    'IB':               {'channel_name':'Affiliate','bu':'Marketing : Affiliate - 613000002XXX','finance_cat':'Affiliate','marketing_cat':'Affiliate- CPA & FF'},
    'Others':           {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC','marketing_cat':'PPC / Search'},
}

# FY26 month → quarter mapping
MONTH_TO_QUARTER = {
    7:'Q1',8:'Q1',9:'Q1',10:'Q2',11:'Q2',12:'Q2',
    1:'Q3',2:'Q3',3:'Q3',4:'Q4',5:'Q4',6:'Q4',
}
MONTH_KEY_MAP = {
    (2025,7):'2025-07',(2025,8):'2025-08',(2025,9):'2025-09',
    (2025,10):'2025-10',(2025,11):'2025-11',(2025,12):'2025-12',
    (2026,1):'2026-01',(2026,2):'2026-02',(2026,3):'2026-03',
    (2026,4):'2026-04',(2026,5):'2026-05',(2026,6):'2026-06',
}

# ── SHEETS HELPERS ────────────────────────────────────────────
import time
_sheet_cache = {}
CACHE_TTL = 30

_gc = None
_gc_ts = 0
_GC_TTL = 600

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
    now = time.time()
    if tab in _sheet_cache:
        ts, data = _sheet_cache[tab]
        if now - ts < CACHE_TTL:
            return data
    try:
        ws = get_sheet(tab)
        data = ws.get_all_records(expected_headers=_get_headers_for(tab))
    except Exception:
        try:
            data = ws.get_all_records(numericise_ignore=['all'])
        except Exception:
            data = []
    _sheet_cache[tab] = (now, data)
    return data

def _get_headers_for(tab):
    hdrs = {
        TAB_BUDGETS:    ["id","country","quarter","total_budget","updated_at"],
        TAB_CHANNELS:   ["id","country","quarter","name","budget","sort_order","created_at"],
        TAB_ACTIVITIES: ["id","channel_id","country","quarter","name","sort_order","created_at"],
        TAB_ENTRIES:    ENTRY_HEADERS,
        TAB_MAPPING:    MAPPING_HEADERS,
        TAB_VENDORS:    VENDOR_HEADERS,
        TAB_USERS:      USER_HEADERS,
        TAB_CATEGORIES: CATEGORY_HEADERS,
    }
    return hdrs.get(tab, None)

def safe_get_records(ws, tab=None):
    try:
        hdrs = _get_headers_for(tab) if tab else None
        if hdrs:
            return ws.get_all_records(expected_headers=hdrs)
        return ws.get_all_records(numericise_ignore=['all'])
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
        TAB_USERS:      USER_HEADERS,
        TAB_CATEGORIES: CATEGORY_HEADERS,
    }
    if tab in hdrs:
        ws.append_row(hdrs[tab])
        ws.format("1:1", {
            "backgroundColor": {"red":0.11,"green":0.31,"blue":0.24},
            "textFormat": {"foregroundColor":{"red":1,"green":1,"blue":1},"bold":True}
        })

def _ensure_entry_headers():
    try:
        ws = get_sheet(TAB_ENTRIES)
        row1 = ws.row_values(1)
        if not row1:
            return
        if "activity_id" in row1 and "activity_name" in row1:
            return
        ws.update('A1:X1', [ENTRY_HEADERS])
        ws.format("1:1", {
            "backgroundColor": {"red":0.11,"green":0.31,"blue":0.24},
            "textFormat": {"foregroundColor":{"red":1,"green":1,"blue":1},"bold":True}
        })
        print(f"[MIGRATION] Fixed Entries header row")
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

# ── AUTH ──────────────────────────────────────────────────────
def _seed_users():
    ws = get_sheet(TAB_USERS)
    existing = safe_get_records(ws, TAB_USERS)
    if existing:
        return
    now = datetime.utcnow().isoformat()
    for uname, pwd, display, role, markets in DEFAULT_USERS:
        ws.append_row([uname, generate_password_hash(pwd), display, role, markets, now])
    print(f"[SEED] Created {len(DEFAULT_USERS)} default users")

def _seed_categories():
    """Seed default BU/finance/marketing categories if Categories tab is empty."""
    ws = get_sheet(TAB_CATEGORIES)
    existing = safe_get_records(ws, TAB_CATEGORIES)
    if existing:
        return
    now = datetime.utcnow().isoformat()
    i = 0
    for val in DEFAULT_BU_LIST:
        ws.append_row([f"cat_{i}", "bu", val, i, now]); i+=1
    for val in DEFAULT_FIN_CATS:
        ws.append_row([f"cat_{i}", "finance", val, i, now]); i+=1
    for val in DEFAULT_MKT_CATS:
        ws.append_row([f"cat_{i}", "marketing", val, i, now]); i+=1
    print(f"[SEED] Created {i} default categories")

def _get_user(username):
    users = safe_get_records(get_sheet(TAB_USERS), TAB_USERS)
    return next((u for u in users if str(u.get("username","")).lower()==username.lower()), None)

def _get_all_users():
    return safe_get_records(get_sheet(TAB_USERS), TAB_USERS)

def _current_user():
    uname = session.get("username")
    if not uname: return None
    return _get_user(uname)

def require_login(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("username"):
            return jsonify({"error":"Not authenticated"}), 401
        return f(*args, **kwargs)
    return decorated

def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get("role") != "admin":
            return jsonify({"error":"Admin required"}), 403
        return f(*args, **kwargs)
    return decorated

def check_country_access(country):
    role = session.get("role","")
    if role == "admin" or role == "editor":
        return True
    markets = session.get("markets","")
    if markets == "ALL":
        return True
    allowed = [m.strip() for m in markets.split(",")]
    return country in allowed

# ── PAGES ─────────────────────────────────────────────────────
@app.route("/")
def index():
    if not session.get("username"):
        users = _get_all_users()
        user_list = [{"username":u.get("username",""), "display_name":u.get("display_name",u.get("username",""))} for u in users]
        return render_template("login.html", users=user_list, markets=MARKETS)

    role = session.get("role","country")
    user_markets = session.get("markets","")
    is_admin = role == "admin"
    is_editor = role == "editor"

    if role == "country" and user_markets != "ALL":
        visible_markets = [m.strip() for m in user_markets.split(",")]
    else:
        visible_markets = MARKETS

    return render_template("app.html",
        user=session.get("display_name", session["username"]),
        username=session["username"],
        is_admin=is_admin,
        is_editor=is_editor,
        role=role,
        markets=visible_markets, quarters=QUARTERS
    )

@app.route("/login", methods=["POST"])
def login():
    username = request.form.get("username","").strip()
    password = request.form.get("password","").strip()

    if not username or not password:
        users = _get_all_users()
        user_list = [{"username":u.get("username",""), "display_name":u.get("display_name","")} for u in users]
        return render_template("login.html", users=user_list, markets=MARKETS, error="Enter username and password")

    user = _get_user(username)
    if not user or not check_password_hash(str(user.get("password_hash","")), password):
        users = _get_all_users()
        user_list = [{"username":u.get("username",""), "display_name":u.get("display_name","")} for u in users]
        return render_template("login.html", users=user_list, markets=MARKETS, error="Invalid password")

    session["username"] = user["username"]
    session["display_name"] = user.get("display_name", user["username"])
    session["role"] = user.get("role", "country")
    session["markets"] = user.get("markets", "ALL")
    session["user"] = "APAC" if user.get("role") == "admin" else user.get("markets","").split(",")[0]
    return redirect("/")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")

# ── CATEGORIES API ────────────────────────────────────────────
@app.route("/api/categories")
@require_login
def api_get_categories():
    """Return all categories grouped by type (bu, finance, marketing)."""
    try:
        rows = get_records_cached(TAB_CATEGORIES)
        if not rows:
            _seed_categories()
            invalidate_cache(TAB_CATEGORIES)
            rows = get_records_cached(TAB_CATEGORIES)
        result = {"bu":[], "finance":[], "marketing":[]}
        for r in rows:
            t = r.get("type","")
            if t in result:
                result[t].append({"id":r["id"], "value":r["value"], "sort_order":int(r.get("sort_order") or 0)})
        for t in result:
            result[t].sort(key=lambda x: x["sort_order"])
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"bu":DEFAULT_BU_LIST, "finance":DEFAULT_FIN_CATS, "marketing":DEFAULT_MKT_CATS})

@app.route("/api/categories", methods=["POST"])
@require_login
@require_admin
def api_add_category():
    d = request.get_json()
    cat_type = d.get("type","")  # bu, finance, marketing
    value = d.get("value","").strip()
    if not cat_type or not value:
        return jsonify({"error":"Type and value required"}), 400
    if cat_type not in ("bu","finance","marketing"):
        return jsonify({"error":"Invalid type"}), 400
    # Check duplicate
    rows = get_records_cached(TAB_CATEGORIES)
    if any(r.get("type")==cat_type and str(r.get("value","")).lower()==value.lower() for r in rows):
        return jsonify({"error":"Category already exists"}), 400
    cat_id = f"cat_{uuid.uuid4().hex[:8]}"
    sort = len([r for r in rows if r.get("type")==cat_type])
    now = datetime.utcnow().isoformat()
    get_sheet(TAB_CATEGORIES).append_row([cat_id, cat_type, value, sort, now])
    invalidate_cache(TAB_CATEGORIES)
    return jsonify({"id":cat_id, "type":cat_type, "value":value, "sort_order":sort})

@app.route("/api/categories/<cat_id>", methods=["PUT"])
@require_login
@require_admin
def api_update_category(cat_id):
    d = request.get_json()
    ws = get_sheet(TAB_CATEGORIES)
    rows = safe_get_records(ws, TAB_CATEGORIES)
    idx = next((i for i,r in enumerate(rows) if str(r.get("id",""))==cat_id), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    r = rows[idx]
    ws.update(f"A{idx+2}:E{idx+2}", [[cat_id, r["type"], d.get("value",r["value"]), r.get("sort_order",0), r.get("created_at","")]])
    invalidate_cache(TAB_CATEGORIES)
    return jsonify({"ok":True})

@app.route("/api/categories/<cat_id>", methods=["DELETE"])
@require_login
@require_admin
def api_delete_category(cat_id):
    ws = get_sheet(TAB_CATEGORIES)
    rows = safe_get_records(ws, TAB_CATEGORIES)
    idx = next((i for i,r in enumerate(rows) if str(r.get("id",""))==cat_id), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    ws.delete_rows(idx+2)
    invalidate_cache(TAB_CATEGORIES)
    return jsonify({"ok":True})

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

        all_acts = rows_for_cached(TAB_ACTIVITIES, country=country, quarter=quarter)
        for ch in channels:
            ch["activities"] = sorted([
                {"id":a["id"],"name":a["name"],"sort_order":int(a.get("sort_order") or 0)}
                for a in all_acts if str(a["channel_id"]) == str(ch["id"])
            ], key=lambda x: x["sort_order"])

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
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Budget load failed: {str(e)}"}), 500

def _seed_mapping():
    ws = get_sheet(TAB_MAPPING)
    now = datetime.utcnow().isoformat()
    for kw,bu,fc,mc in DEFAULT_MAPPING:
        ws.append_row([kw,bu,fc,mc,"system",now])

# ── ACTIVITIES API ────────────────────────────────────────────
@app.route("/api/activities", methods=["POST"])
@require_login
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
    try:
        vendors = get_records_cached(TAB_VENDORS)
        user = session.get("user","")
        if user != ADMIN_MARKET:
            vendors = [v for v in vendors if v.get("country") in ("GLOBAL", user)]
        result = [{"id":v["id"],"name":v["name"],"country":v.get("country","GLOBAL")} for v in vendors]
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
        return jsonify({"error": f"Import failed: {str(e)}"}), 500

# ── USERS API ────────────────────────────────────────────────
@app.route("/api/users")
@require_login
@require_admin
def api_get_users():
    users = _get_all_users()
    return jsonify([{
        "username": u.get("username",""),
        "display_name": u.get("display_name",""),
        "role": u.get("role","country"),
        "markets": u.get("markets",""),
    } for u in users])

@app.route("/api/users", methods=["POST"])
@require_login
@require_admin
def api_add_user():
    d = request.get_json()
    uname = d.get("username","").strip().lower()
    pwd = d.get("password","").strip()
    display = d.get("display_name","").strip() or uname
    role = d.get("role","country")
    markets = d.get("markets","ALL")
    if not uname or not pwd:
        return jsonify({"error":"Username and password required"}), 400
    if _get_user(uname):
        return jsonify({"error":"Username already exists"}), 400
    now = datetime.utcnow().isoformat()
    get_sheet(TAB_USERS).append_row([uname, generate_password_hash(pwd), display, role, markets, now])
    invalidate_cache(TAB_USERS)
    return jsonify({"ok":True, "username":uname})

@app.route("/api/users/<username>", methods=["PUT"])
@require_login
@require_admin
def api_update_user(username):
    d = request.get_json()
    ws = get_sheet(TAB_USERS)
    rows = safe_get_records(ws, TAB_USERS)
    idx = next((i for i,r in enumerate(rows) if str(r.get("username","")).lower()==username.lower()), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    r = rows[idx]
    new_pwd = d.get("password","").strip()
    pwd_hash = generate_password_hash(new_pwd) if new_pwd else r.get("password_hash","")
    ws.update(f"A{idx+2}:F{idx+2}", [[
        r["username"], pwd_hash,
        d.get("display_name", r.get("display_name","")),
        d.get("role", r.get("role","country")),
        d.get("markets", r.get("markets","")),
        r.get("created_at",""),
    ]])
    invalidate_cache(TAB_USERS)
    return jsonify({"ok":True})

@app.route("/api/users/<username>", methods=["DELETE"])
@require_login
@require_admin
def api_delete_user(username):
    if username.lower() == session.get("username","").lower():
        return jsonify({"error":"Cannot delete yourself"}), 400
    ws = get_sheet(TAB_USERS)
    rows = safe_get_records(ws, TAB_USERS)
    idx = next((i for i,r in enumerate(rows) if str(r.get("username","")).lower()==username.lower()), None)
    if idx is None: return jsonify({"error":"Not found"}), 404
    ws.delete_rows(idx+2)
    invalidate_cache(TAB_USERS)
    return jsonify({"ok":True})

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
        import traceback; traceback.print_exc()
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
            session.get("username", session.get("user","")), now, now
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

    if stored and not stored.startswith("data:"):
        path = get_invoice_path(stored)
        if path:
            import mimetypes
            mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
            return send_file(path, mimetype=mime, as_attachment=True, download_name=name)
        return "File not found on disk", 404

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

@app.route("/api/bulk_template")
@require_login
def api_bulk_template():
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['Month','Activity','Description','Vendor','Planned (USD)','Confirmed (USD)','Actual (USD)','JIRA Task','Notes'])
    w.writerow(['Jul 2025','Google Search Q1','Brand keywords','Google',5000,'',3200,'MKT-001',''])
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
    if 'file' not in request.files:
        return jsonify({"error": "No file"}), 400
    f = request.files['file']
    fname = f.filename.lower()
    parsed_rows = []
    try:
        if fname.endswith(('.xlsx', '.xls')):
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(f.read()), data_only=True, read_only=True)
            ws = wb.active
            all_rows = list(ws.iter_rows(values_only=True))
            wb.close()
            if not all_rows: return jsonify({"error": "Empty file"}), 400
            first = [str(v or '').lower() for v in all_rows[0]]
            start = 1 if any(k in ' '.join(first) for k in ['country','quarter','channel','name']) else 0
            for row in all_rows[start:]:
                vals = [str(v).strip() if v is not None else '' for v in row]
                if any(vals): parsed_rows.append(vals)
        else:
            content = f.read().decode('utf-8-sig')
            lines = [l.strip() for l in content.splitlines() if l.strip()]
            if not lines: return jsonify({"error": "Empty file"}), 400
            first = lines[0].lower()
            has_header = 'country' in first or 'quarter' in first or 'channel' in first
            data_lines = lines[1:] if has_header else lines
            import csv as _csv
            parsed_rows = list(_csv.reader(data_lines))
    except Exception as e:
        return jsonify({"error": f"Could not parse file: {str(e)}"}), 400

    saved, skipped, skipped_dups, skipped_invalid = 0, 0, 0, 0
    saved_rows = []
    ws_ch = get_sheet(TAB_CHANNELS)
    ws_bud = get_sheet(TAB_BUDGETS)
    existing_ch = safe_get_records(ws_ch, TAB_CHANNELS)
    existing_bud = safe_get_records(ws_bud, TAB_BUDGETS)
    now = datetime.utcnow().isoformat()

    for row in parsed_rows:
        if len(row) < 3:
            skipped += 1; skipped_invalid += 1; continue
        if len(row) >= 4:
            country_val, quarter_val, name_val = row[0].strip(), row[1].strip().upper(), row[2].strip()
            try: budget_val = float(str(row[3]).replace(',','').replace('$','').strip() or 0)
            except: budget_val = 0
        else:
            country_val = request.form.get('country', '')
            quarter_val, name_val = row[0].strip().upper(), row[1].strip()
            try: budget_val = float(str(row[2]).replace(',','').replace('$','').strip() or 0)
            except: budget_val = 0

        if not country_val or not quarter_val or not name_val:
            skipped += 1; skipped_invalid += 1; continue
        if not quarter_val.startswith('Q'):
            quarter_val = 'Q' + quarter_val

        has_bud = any(r['country']==country_val and r['quarter']==quarter_val for r in existing_bud)
        if not has_bud:
            ws_bud.append_row([str(uuid.uuid4())[:8], country_val, quarter_val, 0, now])
            existing_bud.append({'country':country_val,'quarter':quarter_val,'total_budget':0})

        dup = any(r['country']==country_val and r['quarter']==quarter_val and str(r['name']).strip()==name_val for r in existing_ch)
        if dup:
            skipped += 1; skipped_dups += 1; continue

        sort_order = len([r for r in existing_ch if r['country']==country_val and r['quarter']==quarter_val])
        ch_id = "ch_" + str(uuid.uuid4())[:8]
        ws_ch.append_row([ch_id, country_val, quarter_val, name_val, budget_val, sort_order, now])
        existing_ch.append({'id':ch_id,'country':country_val,'quarter':quarter_val,'name':name_val,'budget':budget_val,'sort_order':sort_order})
        saved += 1
        saved_rows.append({"country":country_val, "quarter":quarter_val, "name":name_val, "budget":budget_val})

    invalidate_cache(TAB_CHANNELS)
    invalidate_cache(TAB_BUDGETS)
    return jsonify({"ok":True, "saved":saved, "skipped":skipped, "skipped_dups":skipped_dups, "skipped_invalid":skipped_invalid, "rows":saved_rows, "total_parsed":len(parsed_rows)})

# ── BULK BUDGET IMPORT ─────────────────────────────────────────
@app.route("/api/import/budgets", methods=["POST"])
@require_login
@require_admin
def api_import_budgets():
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
            if not all_rows: return jsonify({"error": "Empty file"}), 400
            first = [str(v or '').lower() for v in all_rows[0]]
            start = 1 if any(k in ' '.join(first) for k in ['country','quarter','budget']) else 0
            for row in all_rows[start:]:
                vals = [str(v).strip() if v is not None else '' for v in row]
                if any(vals): parsed_rows.append(vals)
        else:
            content = f.read().decode('utf-8-sig')
            lines = [l.strip() for l in content.splitlines() if l.strip()]
            if not lines: return jsonify({"error": "Empty file"}), 400
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
        if len(row) < 3: skipped += 1; continue
        country_val = row[0].strip()
        quarter_val = row[1].strip().upper()
        if not quarter_val.startswith('Q'): quarter_val = 'Q' + quarter_val
        try: total = float(str(row[2]).replace(',','').replace('$','').strip() or 0)
        except: skipped += 1; continue
        if not country_val or not quarter_val: skipped += 1; continue

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

# ── BUDGET SUMMARY ─────────────────────────────────────────────
@app.route("/api/budget_summary")
@require_login
@require_admin
def api_budget_summary():
    try:
        budgets = get_records_cached(TAB_BUDGETS)
        channels = get_records_cached(TAB_CHANNELS)
        result = []
        for b in budgets:
            co, q = str(b["country"]), str(b["quarter"])
            total = float(b.get("total_budget") or 0)
            ch_list = [c for c in channels if str(c["country"])==co and str(c["quarter"])==q]
            allocated = sum(float(c.get("budget") or 0) for c in ch_list)
            result.append({"country":co,"quarter":q,"total":total,"allocated":allocated,"channels":len(ch_list),"deviation":allocated-total})
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── RECONCILIATION ───────────────────────────────────────────
@app.route("/api/reconciliation/<quarter>")
@require_login
def api_reconciliation(quarter):
    if session.get("role") not in ("admin", "editor"):
        return jsonify({"error":"Access denied"}), 403
    try:
        all_budgets = get_records_cached(TAB_BUDGETS)
        all_channels = get_records_cached(TAB_CHANNELS)
        all_activities = get_records_cached(TAB_ACTIVITIES)

        q_budgets = [b for b in all_budgets if str(b.get("quarter",""))==quarter]
        markets_from_budgets = {str(b["country"]) for b in q_budgets}

        all_q_entries = rows_for_cached(TAB_ENTRIES, quarter=quarter)
        markets_from_entries = {str(e["country"]) for e in all_q_entries}

        all_markets = sorted(markets_from_budgets | markets_from_entries)

        result = []
        for mkt in all_markets:
            brow = next((b for b in q_budgets if str(b["country"])==mkt), None)
            plan_budget = float(brow["total_budget"]) if brow else 0

            mkt_channels = [c for c in all_channels if str(c.get("country",""))==mkt and str(c.get("quarter",""))==quarter]
            mkt_entries = rows_for_cached(TAB_ENTRIES, country=mkt, quarter=quarter)

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

# ── ANALYTICS API ─────────────────────────────────────────────
@app.route("/api/analytics")
@require_login
def api_analytics():
    """Comprehensive analytics data for the dashboard."""
    try:
        q_filter = request.args.get("quarter", "")
        all_entries = get_records_cached(TAB_ENTRIES)
        all_budgets = get_records_cached(TAB_BUDGETS)
        all_channels = get_records_cached(TAB_CHANNELS)

        if q_filter:
            all_entries = [e for e in all_entries if str(e.get("quarter",""))==q_filter]
            all_budgets = [b for b in all_budgets if str(b.get("quarter",""))==q_filter]
            all_channels = [c for c in all_channels if str(c.get("quarter",""))==q_filter]

        co_filter = request.args.get("country", "")
        if co_filter:
            all_entries = [e for e in all_entries if str(e.get("country",""))==co_filter]
            all_budgets = [b for b in all_budgets if str(b.get("country",""))==co_filter]
            all_channels = [c for c in all_channels if str(c.get("country",""))==co_filter]

        # 1. Summary totals
        total_budget = sum(float(b.get("total_budget") or 0) for b in all_budgets)
        total_planned = sum(float(e.get("planned") or 0) for e in all_entries)
        total_confirmed = sum(float(e.get("confirmed") or 0) for e in all_entries)
        total_actual = sum(float(e.get("actual") or 0) for e in all_entries)
        total_entries = len(all_entries)

        # 2. By country
        by_country = defaultdict(lambda: {"budget":0,"planned":0,"confirmed":0,"actual":0,"entries":0})
        for b in all_budgets:
            by_country[str(b["country"])]["budget"] += float(b.get("total_budget") or 0)
        for e in all_entries:
            co = str(e.get("country",""))
            by_country[co]["planned"] += float(e.get("planned") or 0)
            by_country[co]["confirmed"] += float(e.get("confirmed") or 0)
            by_country[co]["actual"] += float(e.get("actual") or 0)
            by_country[co]["entries"] += 1
        country_data = [{"country":k, **v} for k,v in sorted(by_country.items())]

        # 3. By channel
        by_channel = defaultdict(lambda: {"planned":0,"confirmed":0,"actual":0,"entries":0,"budget":0})
        ch_budget_map = {}
        for c in all_channels:
            key = str(c.get("name",""))
            ch_budget_map[key] = ch_budget_map.get(key, 0) + float(c.get("budget") or 0)
        for e in all_entries:
            ch = str(e.get("channel_name","")) or str(e.get("finance_cat","")) or "Other"
            by_channel[ch]["planned"] += float(e.get("planned") or 0)
            by_channel[ch]["confirmed"] += float(e.get("confirmed") or 0)
            by_channel[ch]["actual"] += float(e.get("actual") or 0)
            by_channel[ch]["entries"] += 1
        for ch_name, bud in ch_budget_map.items():
            by_channel[ch_name]["budget"] = bud
        channel_data = [{"channel":k, **v} for k,v in sorted(by_channel.items(), key=lambda x: -x[1]["actual"])]

        # 4. By month (time series)
        by_month = defaultdict(lambda: {"planned":0,"confirmed":0,"actual":0,"entries":0})
        for e in all_entries:
            mo = str(e.get("month",""))
            if mo:
                by_month[mo]["planned"] += float(e.get("planned") or 0)
                by_month[mo]["confirmed"] += float(e.get("confirmed") or 0)
                by_month[mo]["actual"] += float(e.get("actual") or 0)
                by_month[mo]["entries"] += 1
        month_data = [{"month":k, **v} for k,v in sorted(by_month.items())]

        # 5. By marketing category
        by_mkt_cat = defaultdict(lambda: {"planned":0,"actual":0,"entries":0})
        for e in all_entries:
            mc = str(e.get("marketing_cat","")) or "Other"
            by_mkt_cat[mc]["planned"] += float(e.get("planned") or 0)
            by_mkt_cat[mc]["actual"] += float(e.get("actual") or 0)
            by_mkt_cat[mc]["entries"] += 1
        mkt_cat_data = [{"category":k, **v} for k,v in sorted(by_mkt_cat.items(), key=lambda x: -x[1]["actual"])]

        # 6. By finance category
        by_fin_cat = defaultdict(lambda: {"planned":0,"actual":0,"entries":0})
        for e in all_entries:
            fc = str(e.get("finance_cat","")) or "Other"
            by_fin_cat[fc]["planned"] += float(e.get("planned") or 0)
            by_fin_cat[fc]["actual"] += float(e.get("actual") or 0)
            by_fin_cat[fc]["entries"] += 1
        fin_cat_data = [{"category":k, **v} for k,v in sorted(by_fin_cat.items(), key=lambda x: -x[1]["actual"])]

        # 7. Variance analysis (top over/under entries)
        variance_entries = []
        for e in all_entries:
            pln = float(e.get("planned") or 0)
            act = float(e.get("actual") or 0)
            if pln > 0 or act > 0:
                variance_entries.append({
                    "id": e["id"], "country": e["country"], "channel": e.get("channel_name",""),
                    "description": e.get("description","") or e.get("activity_name",""),
                    "planned": pln, "actual": act, "variance": act - pln,
                    "variance_pct": ((act - pln) / pln * 100) if pln > 0 else 0,
                })
        variance_entries.sort(key=lambda x: abs(x["variance"]), reverse=True)

        # 8. Completion metrics
        with_actual = sum(1 for e in all_entries if float(e.get("actual") or 0) > 0)
        with_jira = sum(1 for e in all_entries if e.get("jira"))
        approved = sum(1 for e in all_entries if str(e.get("approved","")).lower()=="true")

        # 9. Budget matrix: marketing_cat × country — 3 values each (budget, planned, actual)
        MATRIX_COUNTRIES = ["HKG","CN","TW","TH","VN","SG","MY","MN","IN","APAC","ID","PH"]

        # Build channel→marketing_cat mapping from entries AND from channel name patterns
        ch_mkt_map = {}  # channel_id → marketing_cat
        for e in all_entries:
            cid = str(e.get("channel_id",""))
            mc = str(e.get("marketing_cat",""))
            if cid and mc:
                ch_mkt_map[cid] = mc

        # Also try to map unmapped channels using PM_CHANNEL_MAP name patterns
        for c in all_channels:
            cid = str(c.get("id",""))
            if cid not in ch_mkt_map:
                cname = str(c.get("name","")).strip()
                # Try matching channel name to PM_CHANNEL_MAP values
                for pm_key, pm_val in PM_CHANNEL_MAP.items():
                    if pm_val.get("channel_name","") == cname:
                        ch_mkt_map[cid] = pm_val.get("marketing_cat","Other")
                        break

        # Aggregate budgets by country + marketing_cat from channels
        budget_by_co_mc = defaultdict(lambda: defaultdict(float))
        unallocated_budget = defaultdict(float)  # country → unallocated budget
        for c in all_channels:
            co = str(c.get("country",""))
            cid = str(c.get("id",""))
            mc = ch_mkt_map.get(cid, "")
            bud = float(c.get("budget") or 0)
            if mc and co:
                budget_by_co_mc[mc][co] += bud
            elif co:
                unallocated_budget[co] += bud

        # Country-level total budgets from Budgets tab (the real total)
        country_total_budgets = defaultdict(float)
        for b in all_budgets:
            co = str(b.get("country",""))
            country_total_budgets[co] += float(b.get("total_budget") or 0)

        # Aggregate planned + actual from entries
        planned_by_co_mc = defaultdict(lambda: defaultdict(float))
        actual_by_co_mc = defaultdict(lambda: defaultdict(float))
        all_mkt_cats_set = set()
        for e in all_entries:
            mc = str(e.get("marketing_cat","")) or "Other"
            co = str(e.get("country",""))
            planned_by_co_mc[mc][co] += float(e.get("planned") or 0)
            actual_by_co_mc[mc][co] += float(e.get("actual") or 0)
            all_mkt_cats_set.add(mc)

        # Also include categories that have budget but no entries yet
        for mc in budget_by_co_mc:
            all_mkt_cats_set.add(mc)

        # Build matrix rows
        budget_matrix = []
        for mc in sorted(all_mkt_cats_set):
            row = {"category": mc}
            t_bud, t_pln, t_act = 0, 0, 0
            for co in MATRIX_COUNTRIES:
                b = budget_by_co_mc[mc].get(co, 0)
                p = planned_by_co_mc[mc].get(co, 0)
                a = actual_by_co_mc[mc].get(co, 0)
                row[co+"_bud"] = b
                row[co+"_pln"] = p
                row[co+"_act"] = a
                t_bud += b; t_pln += p; t_act += a
            row["total_bud"] = t_bud
            row["total_pln"] = t_pln
            row["total_act"] = t_act
            if t_bud > 0 or t_pln > 0 or t_act > 0:
                budget_matrix.append(row)

        # Totals row — use country_total_budgets for budget column (from Budgets tab)
        totals_row = {"category": "Total"}
        gt_bud, gt_pln, gt_act = 0, 0, 0
        for co in MATRIX_COUNTRIES:
            cb = country_total_budgets.get(co, 0)  # Real budget from Budgets tab
            cp = sum(planned_by_co_mc[mc].get(co, 0) for mc in all_mkt_cats_set)
            ca = sum(actual_by_co_mc[mc].get(co, 0) for mc in all_mkt_cats_set)
            totals_row[co+"_bud"] = cb
            totals_row[co+"_pln"] = cp
            totals_row[co+"_act"] = ca
            gt_bud += cb; gt_pln += cp; gt_act += ca
        totals_row["total_bud"] = gt_bud
        totals_row["total_pln"] = gt_pln
        totals_row["total_act"] = gt_act
        budget_matrix.append(totals_row)

        return jsonify({
            "summary": {
                "total_budget": total_budget, "total_planned": total_planned,
                "total_confirmed": total_confirmed, "total_actual": total_actual,
                "total_entries": total_entries, "variance": total_actual - total_planned,
                "budget_utilization": (total_actual / total_budget * 100) if total_budget > 0 else 0,
            },
            "by_country": country_data,
            "by_channel": channel_data,
            "by_month": month_data,
            "by_marketing_cat": mkt_cat_data,
            "by_finance_cat": fin_cat_data,
            "top_variances": variance_entries[:20],
            "completion": {
                "total": total_entries,
                "with_actual": with_actual,
                "with_jira": with_jira,
                "approved": approved,
            },
            "budget_matrix": budget_matrix,
            "matrix_countries": MATRIX_COUNTRIES,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ── PM DATA SYNC ─────────────────────────────────────────────
@app.route("/api/pm/preview")
@require_login
@require_admin
def api_pm_preview():
    """Reads PM Google Sheet and returns aggregated spend by country/channel_group/month."""
    try:
        gc = get_gc()
        pm_sh = gc.open_by_key(PM_SHEET_ID)
        pm_ws = pm_sh.worksheet("Ad_Performance")
        all_data = pm_ws.get_all_records()

        q_filter = request.args.get("quarter", "")

        # Debug: sample first few rows
        _debug_samples = []
        _debug_skip_reasons = defaultdict(int)

        # Aggregate: country + channel_group + month → spend
        agg = defaultdict(lambda: {"spend":0, "impressions":0, "clicks":0, "ql":0, "ft":0, "rows":0})
        for i, row in enumerate(all_data):
            pm_country = str(row.get("Country","")).strip()
            tracker_country = PM_COUNTRY_MAP.get(pm_country)
            if not tracker_country:
                _debug_skip_reasons["non_apac_country"] += 1
                continue  # Skip non-APAC countries

            date_str = str(row.get("Date","")).strip()
            # Capture sample for debug
            if i < 5:
                _debug_samples.append({"row": i+2, "Date": date_str, "Country": pm_country, "Channel_Group": str(row.get("Channel_Group","")), "Spend": str(row.get("Spend (AUD)",""))})

            # Parse date: handle MM/DD/YYYY, M/D/YYYY, YYYY-MM-DD, or other formats
            month_num = None
            year = None
            try:
                if "/" in date_str:
                    parts = date_str.split("/")
                    if len(parts) == 3:
                        month_num = int(parts[0])
                        year = int(parts[2])
                elif "-" in date_str:
                    parts = date_str.split("-")
                    if len(parts) == 3:
                        year = int(parts[0])
                        month_num = int(parts[1])
                else:
                    # Try parsing as a date string
                    from datetime import datetime as _dt
                    try:
                        dt = _dt.strptime(date_str, "%d %b %Y")
                        month_num, year = dt.month, dt.year
                    except ValueError:
                        try:
                            dt = _dt.strptime(date_str, "%b %d, %Y")
                            month_num, year = dt.month, dt.year
                        except ValueError:
                            pass
            except (ValueError, IndexError):
                pass

            if not month_num or not year:
                _debug_skip_reasons["date_parse_fail"] += 1
                continue

            quarter = MONTH_TO_QUARTER.get(month_num)
            if not quarter:
                _debug_skip_reasons["no_quarter_match"] += 1
                continue
            if q_filter and quarter != q_filter:
                _debug_skip_reasons["quarter_filtered"] += 1
                continue

            month_key = MONTH_KEY_MAP.get((year, month_num), "")
            if not month_key:
                _debug_skip_reasons["no_month_key"] += 1
                continue

            channel_group = str(row.get("Channel_Group","")).strip()
            if not channel_group or channel_group == "Organic" or channel_group == "IB":
                _debug_skip_reasons["organic_ib_skip"] += 1
                continue  # Skip organic/IB (not paid)

            key = f"{tracker_country}|{channel_group}|{month_key}|{quarter}"
            spend = 0
            try:
                spend = float(str(row.get("Spend (AUD)","0")).replace(",","") or 0)
            except (ValueError, TypeError):
                pass
            ql = 0
            try:
                ql = float(str(row.get("QL","0")).replace(",","") or 0)
            except (ValueError, TypeError):
                pass
            ft = 0
            try:
                ft = float(str(row.get("FT","0")).replace(",","") or 0)
            except (ValueError, TypeError):
                pass

            agg[key]["spend"] += spend
            agg[key]["ql"] += ql
            agg[key]["ft"] += ft
            agg[key]["rows"] += 1

        # Build preview rows
        preview = []
        for key, vals in sorted(agg.items()):
            parts = key.split("|")
            country, channel_group, month_key, quarter = parts[0], parts[1], parts[2], parts[3]
            mapping = PM_CHANNEL_MAP.get(channel_group, PM_CHANNEL_MAP.get("Others", {}))
            preview.append({
                "country": country,
                "channel_group": channel_group,
                "month": month_key,
                "quarter": quarter,
                "spend": round(vals["spend"], 2),
                "ql": round(vals["ql"]),
                "ft": round(vals["ft"]),
                "rows": vals["rows"],
                "mapped_channel": mapping.get("channel_name", "Performance Marketing (PM)"),
                "mapped_bu": mapping.get("bu", ""),
                "mapped_finance_cat": mapping.get("finance_cat", ""),
                "mapped_marketing_cat": mapping.get("marketing_cat", ""),
            })

        return jsonify({
            "total_rows": len(all_data),
            "apac_rows": sum(v["rows"] for v in agg.values()),
            "total_spend": round(sum(v["spend"] for v in agg.values()), 2),
            "preview": preview,
            "debug": {
                "sample_rows": _debug_samples,
                "skip_reasons": dict(_debug_skip_reasons),
                "columns_found": list(all_data[0].keys()) if all_data else [],
                "quarter_filter": q_filter,
            },
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"PM Sheet read failed: {str(e)}"}), 500

@app.route("/api/pm/sync", methods=["POST"])
@require_login
@require_admin
def api_pm_sync():
    """Sync selected PM rows into the budget tracker as entries.
    Rows with a group_tag are grouped: one activity per group+country+month,
    with each channel as a separate sub-entry under that activity.
    Rows without a group_tag sync as individual entries.
    """
    rows_to_sync = request.get_json()
    if not rows_to_sync:
        return jsonify({"error": "No rows provided"}), 400

    ws_entries = get_sheet(TAB_ENTRIES)
    ws_channels = get_sheet(TAB_CHANNELS)
    ws_activities = get_sheet(TAB_ACTIVITIES)
    existing_channels = safe_get_records(ws_channels, TAB_CHANNELS)
    existing_activities = safe_get_records(ws_activities, TAB_ACTIVITIES)
    now = datetime.utcnow().isoformat()
    saved, skipped, channels_created, activities_created = 0, 0, 0, 0

    # Helper: find or create channel
    def ensure_channel(country, quarter, mapped_channel):
        nonlocal channels_created
        ch = next((c for c in existing_channels
                    if str(c["country"])==country and str(c["quarter"])==quarter
                    and str(c["name"]).strip()==mapped_channel), None)
        if ch:
            return str(ch["id"])
        ch_id = "ch_" + str(uuid.uuid4())[:8]
        sort_order = len([c for c in existing_channels if str(c["country"])==country and str(c["quarter"])==quarter])
        ws_channels.append_row([ch_id, country, quarter, mapped_channel, 0, sort_order, now])
        existing_channels.append({"id":ch_id,"country":country,"quarter":quarter,"name":mapped_channel,"budget":0,"sort_order":sort_order})
        channels_created += 1
        return ch_id

    # Helper: find or create activity
    def ensure_activity(channel_id, country, quarter, activity_name):
        nonlocal activities_created
        act = next((a for a in existing_activities
                     if str(a.get("channel_id",""))==channel_id
                     and str(a.get("country",""))==country
                     and str(a.get("quarter",""))==quarter
                     and str(a.get("name","")).strip()==activity_name), None)
        if act:
            return str(act["id"])
        act_id = "act_" + str(uuid.uuid4())[:8]
        sort_order = len([a for a in existing_activities if str(a.get("channel_id",""))==channel_id])
        ws_activities.append_row([act_id, channel_id, country, quarter, activity_name, sort_order, now])
        existing_activities.append({"id":act_id,"channel_id":channel_id,"country":country,"quarter":quarter,"name":activity_name,"sort_order":sort_order})
        activities_created += 1
        return act_id

    # Separate grouped vs ungrouped rows
    grouped = defaultdict(list)  # key: "group_tag|country|quarter|month" → [rows]
    ungrouped = []

    for row in rows_to_sync:
        group_tag = str(row.get("group_tag","")).strip()
        if group_tag:
            key = f"{group_tag}|{row.get('country','')}|{row.get('quarter','')}|{row.get('month','')}"
            grouped[key].append(row)
        else:
            ungrouped.append(row)

    # Process grouped rows: create one activity per group, each channel as a sub-entry
    for group_key, group_rows in grouped.items():
        parts = group_key.split("|")
        group_tag, country, quarter, month = parts[0], parts[1], parts[2], parts[3]

        # Use "Performance Marketing (PM)" as the parent channel for grouped entries
        # (or the most common mapped_channel in the group)
        channel_name = "Performance Marketing (PM)"
        channel_id = ensure_channel(country, quarter, channel_name)

        # Create activity with the group tag as name
        activity_id = ensure_activity(channel_id, country, quarter, group_tag)

        # Each row in the group becomes a sub-entry under this activity
        for row in group_rows:
            spend = float(row.get("spend", 0))
            if spend <= 0:
                skipped += 1
                continue

            channel_group = row.get("channel_group", "")
            mapped_bu = row.get("mapped_bu", "")
            mapped_finance_cat = row.get("mapped_finance_cat", "")
            mapped_marketing_cat = row.get("mapped_marketing_cat", "")

            entry_id = "pm_" + str(uuid.uuid4())[:10]
            description = f"{channel_group}: ${round(spend,2):,.0f} AUD"

            ws_entries.append_row([
                entry_id, country, quarter, month,
                channel_id, channel_name,
                activity_id, group_tag,  # activity_id, activity_name
                mapped_bu, mapped_finance_cat, mapped_marketing_cat,
                description,
                round(spend, 2),  # planned
                0,  # confirmed
                round(spend, 2),  # actual
                "", channel_group, f"PM Sync [{group_tag}] {channel_group} | {now[:10]}",
                "False",
                "[]", "[]",
                session.get("username", "pm_sync"), now, now,
            ])
            saved += 1

    # Process ungrouped rows: each row = individual entry (original behaviour)
    for row in ungrouped:
        country = row.get("country", "")
        quarter = row.get("quarter", "")
        month = row.get("month", "")
        spend = float(row.get("spend", 0))
        channel_group = row.get("channel_group", "")
        mapped_channel = row.get("mapped_channel", "Performance Marketing (PM)")
        mapped_bu = row.get("mapped_bu", "")
        mapped_finance_cat = row.get("mapped_finance_cat", "")
        mapped_marketing_cat = row.get("mapped_marketing_cat", "")

        if spend <= 0:
            skipped += 1
            continue

        channel_id = ensure_channel(country, quarter, mapped_channel)

        entry_id = "pm_" + str(uuid.uuid4())[:10]
        description = f"PM Sync: {channel_group}"

        ws_entries.append_row([
            entry_id, country, quarter, month,
            channel_id, mapped_channel,
            "", "",  # activity_id, activity_name
            mapped_bu, mapped_finance_cat, mapped_marketing_cat,
            description,
            round(spend, 2),  # planned = spend
            0,  # confirmed
            round(spend, 2),  # actual = spend
            "", channel_group, f"Auto-synced from PM Sheet on {now[:10]}",
            "False",
            "[]", "[]",
            session.get("username", "pm_sync"), now, now,
        ])
        saved += 1

    invalidate_cache(TAB_ENTRIES)
    invalidate_cache(TAB_CHANNELS)
    invalidate_cache(TAB_ACTIVITIES)
    return jsonify({"ok": True, "saved": saved, "skipped": skipped,
                    "channels_created": channels_created, "activities_created": activities_created})

# Month key → short label mapping for activity names
_MONTH_SHORT = {
    "2025-07":"Jul 25","2025-08":"Aug 25","2025-09":"Sep 25",
    "2025-10":"Oct 25","2025-11":"Nov 25","2025-12":"Dec 25",
    "2026-01":"Jan 26","2026-02":"Feb 26","2026-03":"Mar 26",
    "2026-04":"Apr 26","2026-05":"May 26","2026-06":"Jun 26",
}

@app.route("/api/pm/auto_sync")
@require_login
@require_admin
def api_pm_auto_sync():
    """Background auto-sync: reads PM sheet, creates/updates entries.
    Activity naming: {COUNTRY} FY26 {QUARTER} - {Channel} - {Mon YY}
    Updates existing entries (matched by country+channel_group+month).
    Never overwrites with $0 if existing > 0.
    """
    try:
        gc = get_gc()
        pm_sh = gc.open_by_key(PM_SHEET_ID)
        pm_ws = pm_sh.worksheet("Ad_Performance")
        all_pm_data = pm_ws.get_all_records()

        # Aggregate PM data: country+channel_group+month → spend/ql/ft
        agg = defaultdict(lambda: {"spend":0, "ql":0, "ft":0, "rows":0})
        for row in all_pm_data:
            pm_country = str(row.get("Country","")).strip()
            tracker_country = PM_COUNTRY_MAP.get(pm_country)
            if not tracker_country:
                continue

            date_str = str(row.get("Date","")).strip()
            month_num, year = None, None
            try:
                if "/" in date_str:
                    parts = date_str.split("/")
                    if len(parts)==3: month_num, year = int(parts[0]), int(parts[2])
                elif "-" in date_str:
                    parts = date_str.split("-")
                    if len(parts)==3: year, month_num = int(parts[0]), int(parts[1])
            except (ValueError, IndexError):
                pass
            if not month_num or not year:
                continue

            quarter = MONTH_TO_QUARTER.get(month_num)
            month_key = MONTH_KEY_MAP.get((year, month_num), "")
            if not quarter or not month_key:
                continue

            channel_group = str(row.get("Channel_Group","")).strip()
            if not channel_group or channel_group in ("Organic", "IB"):
                continue

            key = f"{tracker_country}|{channel_group}|{month_key}|{quarter}"
            spend = 0
            try: spend = float(str(row.get("Spend (AUD)","0")).replace(",","") or 0)
            except: pass
            ql = 0
            try: ql = float(str(row.get("QL","0")).replace(",","") or 0)
            except: pass
            ft = 0
            try: ft = float(str(row.get("FT","0")).replace(",","") or 0)
            except: pass

            agg[key]["spend"] += spend
            agg[key]["ql"] += ql
            agg[key]["ft"] += ft
            agg[key]["rows"] += 1

        if not agg:
            return jsonify({"ok": True, "message": "No APAC data found", "synced": 0, "updated": 0, "skipped": 0})

        # Load existing tracker data
        ws_entries = get_sheet(TAB_ENTRIES)
        ws_channels = get_sheet(TAB_CHANNELS)
        ws_activities = get_sheet(TAB_ACTIVITIES)
        existing_entries = safe_get_records(ws_entries, TAB_ENTRIES)
        existing_channels = safe_get_records(ws_channels, TAB_CHANNELS)
        existing_activities = safe_get_records(ws_activities, TAB_ACTIVITIES)
        now = datetime.utcnow().isoformat()

        # Only sync countries that have a budget configured
        all_budgets_list = get_records_cached(TAB_BUDGETS)
        budget_countries = set(str(b.get("country","")) for b in all_budgets_list if float(b.get("total_budget") or 0) > 0)
        # Also include countries in MARKETS as valid
        valid_countries = budget_countries | set(MARKETS)

        # Build lookup for existing PM entries: country|channel_group|month → entry row index + data
        existing_pm = {}
        for idx, e in enumerate(existing_entries):
            eid = str(e.get("id",""))
            # Only match PM-synced entries (id starts with pm_)
            if not eid.startswith("pm_"):
                continue
            co = str(e.get("country",""))
            mo = str(e.get("month",""))
            # Try vendor field first (channel_group stored here)
            vendor = str(e.get("vendor","")).strip()
            # Also check description for "PM Sync: {channel_group}" or "{channel_group}: $..."
            desc = str(e.get("description","")).strip()
            channel_group_from_desc = ""
            if desc.startswith("PM Sync: "):
                channel_group_from_desc = desc[9:].strip()
            elif ": $" in desc:
                channel_group_from_desc = desc.split(": $")[0].strip()

            # Use vendor if it matches a known PM channel, otherwise use description
            known_channels = set(PM_CHANNEL_MAP.keys())
            cg = ""
            if vendor in known_channels:
                cg = vendor
            elif channel_group_from_desc in known_channels:
                cg = channel_group_from_desc
            elif vendor:
                cg = vendor  # fallback to vendor even if not in known list

            if cg and co and mo:
                ekey = f"{co}|{cg}|{mo}"
                existing_pm[ekey] = {"idx": idx, "entry": e}

        synced, updated, skipped, channels_created, activities_created = 0, 0, 0, 0, 0

        for agg_key, vals in sorted(agg.items()):
            parts = agg_key.split("|")
            country, channel_group, month_key, quarter = parts[0], parts[1], parts[2], parts[3]
            spend = round(vals["spend"], 2)

            # Skip countries not in the tracker
            if country not in valid_countries:
                skipped += 1
                continue

            mapping = PM_CHANNEL_MAP.get(channel_group, PM_CHANNEL_MAP.get("Others", {}))
            mapped_channel = mapping.get("channel_name", "Performance Marketing (PM)")
            mapped_bu = mapping.get("bu", "")
            mapped_finance_cat = mapping.get("finance_cat", "")
            mapped_marketing_cat = mapping.get("marketing_cat", "")

            # Activity name: CN FY26 Q3 - RedNote - Jan 26
            month_short = _MONTH_SHORT.get(month_key, month_key)
            activity_name = f"{country} FY26 {quarter} - {channel_group} - {month_short}"

            # Check if entry already exists
            lookup_key = f"{country}|{channel_group}|{month_key}"
            existing = existing_pm.get(lookup_key)

            if existing:
                # UPDATE existing entry
                old_spend = float(existing["entry"].get("actual") or 0)

                # Zero-protection: don't overwrite with 0 if existing > 0
                if spend <= 0 and old_spend > 0:
                    skipped += 1
                    continue

                # Find the row in the sheet (row index is 0-based in records, +2 for header+1-based)
                sheet_row = existing["idx"] + 2

                # Ensure channel + activity exist for backfilling
                ch = next((c for c in existing_channels
                            if str(c["country"])==country and str(c["quarter"])==quarter
                            and str(c["name"]).strip()==mapped_channel), None)
                if ch:
                    channel_id = str(ch["id"])
                else:
                    channel_id = "ch_" + str(uuid.uuid4())[:8]
                    sort_order = len([c for c in existing_channels if str(c["country"])==country and str(c["quarter"])==quarter])
                    ws_channels.append_row([channel_id, country, quarter, mapped_channel, 0, sort_order, now])
                    existing_channels.append({"id":channel_id,"country":country,"quarter":quarter,"name":mapped_channel,"budget":0,"sort_order":sort_order})
                    channels_created += 1

                act = next((a for a in existing_activities
                             if str(a.get("channel_id",""))==channel_id
                             and str(a.get("country",""))==country
                             and str(a.get("quarter",""))==quarter
                             and str(a.get("name","")).strip()==activity_name), None)
                if act:
                    activity_id = str(act["id"])
                else:
                    activity_id = "act_" + str(uuid.uuid4())[:8]
                    sort_order = len([a for a in existing_activities if str(a.get("channel_id",""))==channel_id])
                    ws_activities.append_row([activity_id, channel_id, country, quarter, activity_name, sort_order, now])
                    existing_activities.append({"id":activity_id,"channel_id":channel_id,"country":country,"quarter":quarter,"name":activity_name,"sort_order":sort_order})
                    activities_created += 1

                # Check if anything needs updating
                needs_update = False
                old_act_id = str(existing["entry"].get("activity_id","")).strip()
                spend_changed = abs(spend - old_spend) >= 0.01

                if not old_act_id or spend_changed:
                    needs_update = True

                if not needs_update:
                    skipped += 1
                    continue

                try:
                    old_notes = str(existing["entry"].get("notes",""))
                    note_parts = []
                    if spend_changed:
                        note_parts.append(f"${old_spend:.0f}->${spend:.0f}")
                    if not old_act_id:
                        note_parts.append(f"assigned to {activity_name}")
                    new_notes = f"Updated {' | '.join(note_parts)} on {now[:10]} | {old_notes}"[:500]

                    if spend_changed:
                        ws_entries.update_cell(sheet_row, 13, spend)   # planned
                        ws_entries.update_cell(sheet_row, 15, spend)   # actual
                    # Always backfill activity assignment
                    ws_entries.update_cell(sheet_row, 5, channel_id)     # channel_id
                    ws_entries.update_cell(sheet_row, 6, mapped_channel) # channel_name
                    ws_entries.update_cell(sheet_row, 7, activity_id)    # activity_id
                    ws_entries.update_cell(sheet_row, 8, activity_name)  # activity_name
                    ws_entries.update_cell(sheet_row, 18, new_notes)     # notes
                    ws_entries.update_cell(sheet_row, 24, now)           # updated_at
                    updated += 1
                except Exception as ue:
                    print(f"Auto-sync update error row {sheet_row}: {ue}")
                    skipped += 1
                continue

            # NEW entry — create channel + activity if needed
            if spend <= 0:
                skipped += 1
                continue

            # Find or create channel
            ch = next((c for c in existing_channels
                        if str(c["country"])==country and str(c["quarter"])==quarter
                        and str(c["name"]).strip()==mapped_channel), None)
            if ch:
                channel_id = str(ch["id"])
            else:
                channel_id = "ch_" + str(uuid.uuid4())[:8]
                sort_order = len([c for c in existing_channels if str(c["country"])==country and str(c["quarter"])==quarter])
                ws_channels.append_row([channel_id, country, quarter, mapped_channel, 0, sort_order, now])
                existing_channels.append({"id":channel_id,"country":country,"quarter":quarter,"name":mapped_channel,"budget":0,"sort_order":sort_order})
                channels_created += 1

            # Find or create activity
            act = next((a for a in existing_activities
                         if str(a.get("channel_id",""))==channel_id
                         and str(a.get("country",""))==country
                         and str(a.get("quarter",""))==quarter
                         and str(a.get("name","")).strip()==activity_name), None)
            if act:
                activity_id = str(act["id"])
            else:
                activity_id = "act_" + str(uuid.uuid4())[:8]
                sort_order = len([a for a in existing_activities if str(a.get("channel_id",""))==channel_id])
                ws_activities.append_row([activity_id, channel_id, country, quarter, activity_name, sort_order, now])
                existing_activities.append({"id":activity_id,"channel_id":channel_id,"country":country,"quarter":quarter,"name":activity_name,"sort_order":sort_order})
                activities_created += 1

            entry_id = "pm_" + str(uuid.uuid4())[:10]
            description = f"{channel_group}: ${spend:,.0f} AUD"

            ws_entries.append_row([
                entry_id, country, quarter, month_key,
                channel_id, mapped_channel,
                activity_id, activity_name,
                mapped_bu, mapped_finance_cat, mapped_marketing_cat,
                description,
                spend,  # planned
                0,      # confirmed
                spend,  # actual
                "", channel_group, f"Auto-synced {now[:10]}",
                "False",
                "[]", "[]",
                session.get("username", "pm_auto"), now, now,
            ])
            # Track for dedup
            existing_pm[lookup_key] = {"idx": len(existing_entries), "entry": {"actual": spend, "vendor": channel_group}}
            existing_entries.append({})  # placeholder
            synced += 1

        invalidate_cache(TAB_ENTRIES)
        invalidate_cache(TAB_CHANNELS)
        invalidate_cache(TAB_ACTIVITIES)
        return jsonify({
            "ok": True, "synced": synced, "updated": updated, "skipped": skipped,
            "channels_created": channels_created, "activities_created": activities_created,
            "total_pm_rows": len(all_pm_data),
            "total_agg_rows": len(agg),
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Auto-sync failed: {str(e)}"}), 500
@app.route("/api/campaigns")
@require_login
@require_admin
def api_campaigns():
    try:
        entries = get_records_cached(TAB_ENTRIES)
        q_filter = request.args.get("quarter", "")
        if q_filter:
            entries = [e for e in entries if str(e.get("quarter","")) == q_filter]

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

# ── XLSX EXPORT ───────────────────────────────────────────────
@app.route("/api/export/xlsx")
@require_login
def api_export_xlsx():
    from export_xlsx import build_finance_export
    import tempfile

    user = session["user"]
    all_entries  = safe_get_records(get_sheet(TAB_ENTRIES), TAB_ENTRIES)
    all_channels = safe_get_records(get_sheet(TAB_CHANNELS), TAB_CHANNELS)
    all_budgets  = safe_get_records(get_sheet(TAB_BUDGETS), TAB_BUDGETS)

    if user != ADMIN_MARKET:
        all_entries  = [e for e in all_entries if str(e.get("country","")) == user]
        all_channels = [c for c in all_channels if str(c.get("country","")) == user]
        all_budgets  = [b for b in all_budgets  if str(b.get("country","")) == user]

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
    try:
        _ensure_entry_headers()
    except Exception as e:
        print(f"[STARTUP] Migration check failed: {e}")
    try:
        _seed_users()
    except Exception as e:
        print(f"[STARTUP] User seed failed: {e}")
    try:
        _seed_categories()
    except Exception as e:
        print(f"[STARTUP] Category seed failed: {e}")
    app.run(debug=True, port=5000)