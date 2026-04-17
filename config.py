# -*- coding: utf-8 -*-
"""
Configuration constants, channel mappings, and normalisation helpers.

DATA ACCESS: this module is storage-agnostic. No direct Sheets/DB calls.
Used by: app.py, api_pm.py, api_uploads.py, migrate_channels.py, sheets_helper.py, export_xlsx.py
"""
import os

SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-in-production")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_CREDS", "credentials.json")
SHEET_ID = os.environ.get("SHEET_ID", "13TMeZ3pqdUQr2WRMG5G70xchZKfmiPVOShZChqbUsv4")
ADMIN_MARKET = "APAC"

# BigQuery
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "gen-lang-client-0602500310")
BQ_DATASET = os.environ.get("BQ_DATASET", "pepperstone_apac")
BQ_TABLE = os.environ.get("BQ_TABLE", "ad_performance")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "asia-southeast1")

MARKETS = ["CN","HKG","ID","IN","MN","MY","PH","SG","TH","TW","VN","APAC"]
QUARTERS = ["Q1","Q2","Q3","Q4"]

# Tab names
TAB_BUDGETS = "Budgets"
TAB_CHANNELS = "Channels"
TAB_ACTIVITIES = "Activities"
TAB_ENTRIES = "Entries"
TAB_MAPPING = "ChannelMapping"
TAB_VENDORS = "Vendors"
TAB_USERS = "Users"
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
VENDOR_HEADERS = ["id","name","country","added_by","created_at"]
USER_HEADERS = ["username","password_hash","display_name","role","markets","created_at"]
CATEGORY_HEADERS = ["id","type","value","sort_order","created_at"]

# ═══════════════════════════════════════════════════════════════════
#  UMBRELLA CHANNEL STRUCTURE
# ═══════════════════════════════════════════════════════════════════
# The tracker has exactly TWO auto-populated channels:
#   1. "Performance Marketing"  — umbrella for all Paid Social + PM + LDD
#   2. "Affiliate - CPA & FF"   — standalone, for BQ Affiliates rows
#
# PM sync MAY NOT create new channels. If a market lacks these two
# channels, sync skips the market with a clear reason.
# ═══════════════════════════════════════════════════════════════════

PM_UMBRELLA_CHANNEL = "Performance Marketing"
AFFILIATE_CHANNEL = "Affiliate - CPA & FF"

# Activities under Performance Marketing (10 from BQ + 1 manual for LDD)
PM_ACTIVITIES = ["Meta","TikTok","Douyin","RedNote","BiliBili",
                 "Bing","AdRoll","Apple Search Ads","TradingView","Others","LDD"]

# Default users (unchanged)
DEFAULT_USERS = [
    ("pepper", "APAC@123", "Pepper (Admin)", "admin", "ALL"),
    ("affiliate", "Affiliate@123", "Affiliate Manager", "editor", "ALL"),
    ("performance", "Performance@123", "Performance Marketing", "editor", "ALL"),
    ("campaigns", "Campaigns@123", "Campaign Manager", "editor", "ALL"),
    ("th_sales", "TH@123", "Thailand Sales", "country", "TH"),
    ("cn_sales", "CN@123", "China Sales", "country", "CN"),
    ("hkg_sales", "HKG@123", "Hong Kong Sales", "country", "HKG"),
    ("vn_sales", "VN@123", "Vietnam Sales", "country", "VN"),
    ("roapac_sales", "ROAPAC@123", "Rest of APAC Sales", "country", "ID,IN,MY,SG,MN,PH,TW"),
]

# Channel-name keyword mapping for auto-populating BU/Finance/Marketing categories
# Used both for manual entries and Line-Item CSV upload when those fields are blank
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

# PM BigQuery -> Tracker country mapping
PM_COUNTRY_MAP = {
    'TH':'TH','SG':'SG','CN':'CN','HK':'HKG',
    'ID':'ID','IN':'IN','MY':'MY','VN':'VN',
    'TW':'TW','PH':'PH','MN':'MN',
}

# ═══════════════════════════════════════════════════════════════════
#  PM CHANNEL MAP — rewritten for umbrella structure
#  Every BQ Channel_Group maps to:
#    channel_name:  which tracker CHANNEL it lands under (must already exist)
#    activity_name: which ACTIVITY under that channel (auto-created if missing)
#    bu / finance_cat / marketing_cat: default classification tags
# ═══════════════════════════════════════════════════════════════════
PM_CHANNEL_MAP = {
    # Paid Social cluster → Performance Marketing / "<ChannelGroup>" activity
    'Meta':     {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'Meta',    'bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta',    'marketing_cat':'Paid Social'},
    'TikTok':   {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'TikTok',  'bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta',    'marketing_cat':'Paid Social'},
    'Douyin':   {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'Douyin',  'bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Douyin',  'marketing_cat':'Paid Social'},
    'RedNote':  {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'RedNote', 'bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Rednote', 'marketing_cat':'Paid Social'},
    'BiliBili': {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'BiliBili','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta',    'marketing_cat':'Paid Social'},
    # PM cluster → Performance Marketing / "<ChannelGroup>" activity
    'Bing':             {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'Bing',            'bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'Bing',        'marketing_cat':'PPC / Search'},
    'AdRoll':           {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'AdRoll',          'bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'Programmatic','marketing_cat':'Programmatic'},
    'Apple Search Ads': {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'Apple Search Ads','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC',         'marketing_cat':'PPC / Search'},
    'TradingView':      {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'TradingView',     'bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC',         'marketing_cat':'PPC / Search'},
    'Others':           {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'Others',          'bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC',         'marketing_cat':'PPC / Search'},
    # Affiliate cluster → Affiliate - CPA & FF channel
    'Affiliates': {'channel_name':AFFILIATE_CHANNEL,'activity_name':'Affiliate','bu':'Marketing : Affiliate - 613000002XXX','finance_cat':'Affiliate','marketing_cat':'Affiliate- CPA & FF'},
}

# Fallback when a Channel_Group doesn't normalise to anything known
PM_DEFAULT_MAPPING = {'channel_name':PM_UMBRELLA_CHANNEL,'activity_name':'Others',
                      'bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC','marketing_cat':'PPC / Search'}

# ═══════════════════════════════════════════════════════════════════
#  NORMALISATION — "Meta HK", "meta-agency", "FB" → "Meta"
# ═══════════════════════════════════════════════════════════════════
# Prefix match: any Channel_Group starting with one of these collapses to it.
# Matches in order, longest-first (so "Apple Search Ads" beats "Apple").
CHANNEL_GROUP_PREFIXES = [
    'Apple Search Ads',   # must come before 'Apple' (we don't have 'Apple' but belt-and-braces)
    'TradingView',
    'BiliBili',
    'Affiliates',
    'AdRoll',
    'RedNote',
    'Douyin',
    'TikTok',
    'Others',
    'Meta',
    'Bing',
]

# Exact-word aliases (case-insensitive) for common variants in uploaded files
CHANNEL_GROUP_ALIAS_MAP = {
    'FB':'Meta', 'FACEBOOK':'Meta', 'INSTAGRAM':'Meta', 'IG':'Meta',
    'APPLE':'Apple Search Ads', 'ASA':'Apple Search Ads',
    'AFFILIATE':'Affiliates', 'AFF':'Affiliates',
    'TRADING VIEW':'TradingView', 'TRADINGVIEW':'TradingView', 'TV':'TradingView',
    'ADROLL':'AdRoll',
    'REDNOTE':'RedNote', 'RED NOTE':'RedNote', 'XIAOHONGSHU':'RedNote',
    'BILIBILI':'BiliBili', 'BILI':'BiliBili', 'BILI BILI':'BiliBili',
    'TIK TOK':'TikTok', 'TIKTOK':'TikTok',
    'DOUYIN':'Douyin',
    'BING':'Bing', 'MICROSOFT ADS':'Bing', 'MICROSOFT':'Bing',
    'META':'Meta',
}

# Country code aliases for uploaded files
COUNTRY_ALIAS_MAP = {
    'HK':'HKG', 'HONGKONG':'HKG', 'HONG KONG':'HKG', 'HK SAR':'HKG',
    'THAILAND':'TH',
    'VIETNAM':'VN', 'VIET NAM':'VN',
    'TAIWAN':'TW',
    'SINGAPORE':'SG',
    'INDONESIA':'ID',
    'INDIA':'IN',
    'PHILIPPINES':'PH', 'PHILLIPINES':'PH',
    'MONGOLIA':'MN',
    'MALAYSIA':'MY',
    'CHINA':'CN', 'PRC':'CN', 'MAINLAND CHINA':'CN',
}


def normalise_channel_group(raw):
    """Collapse any Channel_Group variant to its canonical PM_CHANNEL_MAP key.
    Examples:  'Meta HK' → 'Meta',  'FB' → 'Meta',  'meta-agency' → 'Meta',
               'Apple Search Ads' → 'Apple Search Ads',  'ASA' → 'Apple Search Ads',
               'Affiliates - Global' → 'Affiliates'
    Returns canonical key if known, else the cleaned-up raw string."""
    s = str(raw or '').strip()
    if not s:
        return s
    # 1. exact match against known keys
    if s in PM_CHANNEL_MAP:
        return s
    # 2. alias table (case/whitespace-insensitive)
    up = ' '.join(s.upper().split())
    if up in CHANNEL_GROUP_ALIAS_MAP:
        return CHANNEL_GROUP_ALIAS_MAP[up]
    # 3. prefix match — longest-first ordering enforced by list order
    low = s.lower()
    for canon in CHANNEL_GROUP_PREFIXES:
        cl = canon.lower()
        # match "meta" as whole word prefix, followed by space/dash/_ or EOL
        if low == cl or low.startswith(cl + ' ') or low.startswith(cl + '-') or low.startswith(cl + '_') or low.startswith(cl + '/'):
            return canon
    # 4. last resort: return as-is (caller will use PM_DEFAULT_MAPPING)
    return s


def normalise_country(raw):
    """Map common country-name variants to tracker codes. Returns upper-cased input
    if no alias matches (caller should validate against MARKETS list)."""
    s = str(raw or '').strip().upper()
    if not s:
        return s
    if s in (m.upper() for m in MARKETS):
        return s if s != 'HKG' else 'HKG'  # already canonical
    # direct alias
    if s in COUNTRY_ALIAS_MAP:
        return COUNTRY_ALIAS_MAP[s]
    # normalised whitespace
    compact = ' '.join(s.split())
    if compact in COUNTRY_ALIAS_MAP:
        return COUNTRY_ALIAS_MAP[compact]
    return s


# FY26 month -> quarter
MONTH_TO_QUARTER = {7:'Q1',8:'Q1',9:'Q1',10:'Q2',11:'Q2',12:'Q2',1:'Q3',2:'Q3',3:'Q3',4:'Q4',5:'Q4',6:'Q4'}
MONTH_KEY_MAP = {
    (2025,7):'2025-07',(2025,8):'2025-08',(2025,9):'2025-09',
    (2025,10):'2025-10',(2025,11):'2025-11',(2025,12):'2025-12',
    (2026,1):'2026-01',(2026,2):'2026-02',(2026,3):'2026-03',
    (2026,4):'2026-04',(2026,5):'2026-05',(2026,6):'2026-06',
}
MONTH_SHORT = {
    "2025-07":"Jul 25","2025-08":"Aug 25","2025-09":"Sep 25",
    "2025-10":"Oct 25","2025-11":"Nov 25","2025-12":"Dec 25",
    "2026-01":"Jan 26","2026-02":"Feb 26","2026-03":"Mar 26",
    "2026-04":"Apr 26","2026-05":"May 26","2026-06":"Jun 26",
}
VALID_MONTH_KEYS = set(MONTH_SHORT.keys())


def month_to_quarter(month_key):
    """'2025-07' -> 'Q1'. Returns empty string if malformed."""
    try:
        parts = str(month_key).strip().split('-')
        m = int(parts[1])
        return MONTH_TO_QUARTER.get(m, '')
    except Exception:
        return ''
