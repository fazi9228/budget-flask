# -*- coding: utf-8 -*-
"""Configuration constants and channel mappings."""
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

# Default users
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

# PM Channel_Group -> Tracker channel mapping
# Paid Social: Meta, TikTok, Douyin, RedNote, BiliBili
# Performance Marketing (PM): Bing, AdRoll, Apple Search Ads, TradingView, Others
# Affiliate: Affiliates
# Skip: Organic, IB
PM_CHANNEL_MAP = {
    'Affiliates':       {'channel_name':'Affiliate','bu':'Marketing : Affiliate - 613000002XXX','finance_cat':'Affiliate','marketing_cat':'Affiliate- CPA & FF'},
    'Meta':             {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta','marketing_cat':'Paid Social'},
    'TikTok':           {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta','marketing_cat':'Paid Social'},
    'Douyin':           {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Douyin','marketing_cat':'Paid Social'},
    'RedNote':          {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Rednote','marketing_cat':'Paid Social'},
    'BiliBili':         {'channel_name':'Paid Social','bu':'Marketing : Paid Social / YouTube - 613000004XXX','finance_cat':'Paid Social-Meta','marketing_cat':'Paid Social'},
    'Bing':             {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'Bing','marketing_cat':'PPC / Search'},
    'AdRoll':           {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'Programmatic','marketing_cat':'Programmatic'},
    'Apple Search Ads': {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC','marketing_cat':'PPC / Search'},
    'TradingView':      {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC','marketing_cat':'PPC / Search'},
    'Others':           {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC','marketing_cat':'PPC / Search'},
}
PM_DEFAULT_MAPPING = {'channel_name':'Performance Marketing (PM)','bu':'Marketing : Programmatic - 613000009XXX','finance_cat':'PPC','marketing_cat':'PPC / Search'}

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
