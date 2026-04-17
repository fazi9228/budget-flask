# -*- coding: utf-8 -*-
"""Upload endpoints: Planned (BQ-shaped) and Line-Item (full manual).
Also: bulk delete and CSV template downloads.

DATA ACCESS (for Postgres port):
  reads:  get_sheet, safe_get_records, get_records_cached, rows_for_cached
  writes: ws.append_row, ws.update, ws.delete_rows, invalidate_cache
See PORTING.md.
"""
import io, csv, uuid, json
from datetime import datetime
from flask import Blueprint, request, jsonify, session, send_file

from config import *
from sheets_helper import (get_sheet, safe_get_records, get_records_cached,
                           rows_for_cached, invalidate_cache)
from auth import require_login, require_admin

bp = Blueprint('uploads', __name__)

# ═══════════════════════════════════════════════════════════════════
#  RBAC helpers
# ═══════════════════════════════════════════════════════════════════
def _allowed_markets_for_session():
    """Return a set of market codes the current user can write to,
    or None meaning 'all markets allowed'."""
    role = session.get("role", "")
    markets = session.get("markets", "")
    if role == "admin":
        return None  # everything
    if markets == "ALL":
        return None  # editors with ALL
    return set(m.strip() for m in markets.split(",") if m.strip())


def _filter_rbac(rows, country_key="country"):
    """Split rows into (allowed, rejected) based on session RBAC."""
    allowed_markets = _allowed_markets_for_session()
    if allowed_markets is None:
        return rows, []
    kept, rejected = [], []
    for r in rows:
        co = str(r.get(country_key, "") or "").strip()
        if co in allowed_markets:
            kept.append(r)
        else:
            r["_reject_reason"] = f"no access to market '{co}'"
            rejected.append(r)
    return kept, rejected


# ═══════════════════════════════════════════════════════════════════
#  File parsing
# ═══════════════════════════════════════════════════════════════════
def _parse_upload(file_storage):
    """Parse CSV or XLSX into a list of dicts keyed by lowercased header.
    Returns (list_of_dicts, headers_seen)."""
    fname = (file_storage.filename or "").lower()
    rows = []
    headers = []
    if fname.endswith(('.xlsx', '.xls')):
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(file_storage.read()), data_only=True, read_only=True)
        ws = wb.active
        all_rows = list(ws.iter_rows(values_only=True))
        wb.close()
        if not all_rows:
            return [], []
        headers = [str(v or '').strip() for v in all_rows[0]]
        for raw in all_rows[1:]:
            if not any(str(v or '').strip() for v in raw):
                continue
            d = {}
            for i, h in enumerate(headers):
                val = raw[i] if i < len(raw) else None
                d[h.lower()] = ('' if val is None else str(val).strip())
            rows.append(d)
    else:
        text = file_storage.read().decode('utf-8-sig', errors='replace')
        reader = csv.reader(io.StringIO(text))
        all_rows = list(reader)
        if not all_rows:
            return [], []
        headers = [str(v or '').strip() for v in all_rows[0]]
        for raw in all_rows[1:]:
            if not any(str(v or '').strip() for v in raw):
                continue
            d = {}
            for i, h in enumerate(headers):
                val = raw[i] if i < len(raw) else ''
                d[h.lower()] = str(val or '').strip()
            rows.append(d)
    return rows, headers


def _num(val):
    """Parse a loosely-formatted currency/number string. Returns 0.0 on failure."""
    s = str(val or '').replace(',', '').replace('$', '').replace(' ', '').strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _pick(d, *keys):
    """Return first non-empty value for any of the given header keys (case-insensitive)."""
    for k in keys:
        v = d.get(k.lower(), '')
        if v:
            return v
    return ''


def _normalise_quarter(q, month_key=''):
    """Accept 'Q1'/'q1'/'1' — or derive from month_key if blank."""
    s = str(q or '').strip().upper()
    if s and not s.startswith('Q'):
        s = 'Q' + s
    if s in QUARTERS:
        return s
    # derive from month
    derived = month_to_quarter(month_key)
    return derived or ''


def _apply_keyword_mapping(channel_name):
    """Given a channel name, return (bu, finance_cat, marketing_cat) by keyword match.
    Mirrors the UI's applyMapping() so line-item uploads auto-populate blank cats."""
    if not channel_name:
        return '', '', ''
    low = str(channel_name).lower()
    best, best_len = None, 0
    for kw, bu, fc, mc in DEFAULT_MAPPING:
        if kw in low and len(kw) > best_len:
            best, best_len = (bu, fc, mc), len(kw)
    return best if best else ('', '', '')


# ═══════════════════════════════════════════════════════════════════
#  ENDPOINT 1: Planned Upload (BQ-shaped, 4 columns)
#  Country, Month, Channel_Group, Planned
#  Joins on (country, month, channel_group → channel+activity via PM_CHANNEL_MAP)
# ═══════════════════════════════════════════════════════════════════
@bp.route("/api/upload/planned", methods=["POST"])
@require_login
def upload_planned():
    if 'file' not in request.files:
        return jsonify({"error":"No file uploaded"}), 400
    try:
        parsed, headers = _parse_upload(request.files['file'])
    except Exception as e:
        return jsonify({"error": f"Could not parse file: {e}"}), 400
    if not parsed:
        return jsonify({"error":"File is empty"}), 400

    # Pre-normalise every row
    norm_rows = []
    rejected = []
    for idx, d in enumerate(parsed, start=2):  # start=2 for human-readable row numbers
        co_raw = _pick(d, 'country', 'market', 'region')
        mo_raw = _pick(d, 'month', 'period', 'month_key')
        cg_raw = _pick(d, 'channel_group', 'channel group', 'campaign type', 'campaign_type', 'channel')
        pln_raw = _pick(d, 'planned', 'planned budget', 'planned (aud)', 'budget', 'gross budget', 'gross_budget')
        co = normalise_country(co_raw)
        cg = normalise_channel_group(cg_raw)
        mo = str(mo_raw).strip()
        # Accept various month formats: 2025-07, 2025/07, Jul-2025, Jul 2025, 07-2025
        mo_std = _standardise_month(mo)
        planned = _num(pln_raw)
        if not co:
            rejected.append({"row":idx,"reason":"missing country","raw":dict(d)}); continue
        if co not in MARKETS:
            rejected.append({"row":idx,"reason":f"unknown country '{co_raw}' (normalised to '{co}')","raw":dict(d)}); continue
        if not mo_std or mo_std not in VALID_MONTH_KEYS:
            rejected.append({"row":idx,"reason":f"invalid month '{mo_raw}' (expected YYYY-MM in FY26 range)","raw":dict(d)}); continue
        if not cg:
            rejected.append({"row":idx,"reason":"missing channel_group","raw":dict(d)}); continue
        if cg not in PM_CHANNEL_MAP:
            rejected.append({"row":idx,"reason":f"unknown channel_group '{cg_raw}' (normalised to '{cg}')","raw":dict(d)}); continue
        if planned <= 0:
            rejected.append({"row":idx,"reason":f"planned must be > 0 (got {pln_raw})","raw":dict(d)}); continue
        quarter = month_to_quarter(mo_std)
        norm_rows.append({
            "country": co, "month": mo_std, "quarter": quarter,
            "channel_group": cg, "planned": planned, "_src_row": idx
        })

    # RBAC filter
    allowed, rbac_rejected = _filter_rbac(norm_rows)
    for r in rbac_rejected:
        rejected.append({"row":r["_src_row"],"reason":r.get("_reject_reason","RBAC"),"raw":{"country":r["country"]}})

    if not allowed:
        return jsonify({"ok":True, "saved":0, "overwrote":0, "created":0,
                       "channels_created":0, "activities_created":0,
                       "rejected":rejected[:100], "rejected_count":len(rejected)})

    # Load state
    ws_entries = get_sheet(TAB_ENTRIES)
    ws_channels = get_sheet(TAB_CHANNELS)
    ws_activities = get_sheet(TAB_ACTIVITIES)
    ws_budgets = get_sheet(TAB_BUDGETS)
    existing_entries = safe_get_records(ws_entries, TAB_ENTRIES)
    existing_channels = safe_get_records(ws_channels, TAB_CHANNELS)
    existing_activities = safe_get_records(ws_activities, TAB_ACTIVITIES)
    existing_budgets = safe_get_records(ws_budgets, TAB_BUDGETS)
    now = datetime.utcnow().isoformat()
    username = session.get("username", "planned_upload")

    channels_created = 0
    activities_created = 0
    saved = 0
    overwrote = 0
    created = 0

    def ensure_budget(country, quarter):
        if any(str(b.get("country",""))==country and str(b.get("quarter",""))==quarter for b in existing_budgets):
            return
        ws_budgets.append_row([str(uuid.uuid4())[:8], country, quarter, 0, now])
        existing_budgets.append({"country":country, "quarter":quarter, "total_budget":0})

    def ensure_channel(country, quarter, channel_name):
        nonlocal channels_created
        for c in existing_channels:
            if (str(c.get("country","")) == country
                and str(c.get("quarter","")) == quarter
                and str(c.get("name","")).strip() == channel_name):
                return str(c["id"])
        cid = "ch_" + str(uuid.uuid4())[:8]
        so = len([c for c in existing_channels if str(c.get("country",""))==country and str(c.get("quarter",""))==quarter])
        ws_channels.append_row([cid, country, quarter, channel_name, 0, so, now])
        existing_channels.append({"id":cid, "country":country, "quarter":quarter,
                                  "name":channel_name, "budget":0, "sort_order":so})
        channels_created += 1
        ensure_budget(country, quarter)  # also seed a $0 budget row so market appears in lists
        return cid

    def ensure_activity(channel_id, country, quarter, activity_name):
        nonlocal activities_created
        for a in existing_activities:
            if (str(a.get("channel_id","")) == channel_id
                and str(a.get("country","")) == country
                and str(a.get("quarter","")) == quarter
                and str(a.get("name","")).strip() == activity_name):
                return str(a["id"])
        aid = "act_" + str(uuid.uuid4())[:8]
        so = len([a for a in existing_activities if str(a.get("channel_id",""))==channel_id])
        ws_activities.append_row([aid, channel_id, country, quarter, activity_name, so, now])
        existing_activities.append({"id":aid, "channel_id":channel_id, "country":country,
                                    "quarter":quarter, "name":activity_name, "sort_order":so})
        activities_created += 1
        return aid

    # Entry lookup by (country, channel_id, activity_id, month)
    entry_lookup = {}
    for idx, e in enumerate(existing_entries):
        key = "{}|{}|{}|{}".format(
            str(e.get("country","")), str(e.get("channel_id","")),
            str(e.get("activity_id","")), str(e.get("month","")))
        entry_lookup.setdefault(key, []).append((idx, e))

    for r in allowed:
        mapping = PM_CHANNEL_MAP[r["channel_group"]]
        mapped_channel = mapping["channel_name"]
        mapped_activity = mapping["activity_name"]
        channel_id = ensure_channel(r["country"], r["quarter"], mapped_channel)
        activity_id = ensure_activity(channel_id, r["country"], r["quarter"], mapped_activity)
        planned = round(r["planned"], 2)

        lookup_key = f"{r['country']}|{channel_id}|{activity_id}|{r['month']}"
        matches = entry_lookup.get(lookup_key, [])
        if matches:
            # Overwrite planned on the first match (should only be one, pm_ entries are unique per key)
            idx, e = matches[0]
            sheet_row = idx + 2
            ws_entries.update(f"A{sheet_row}:X{sheet_row}", [[
                str(e.get("id","")), str(e.get("country","")), str(e.get("quarter","")), str(e.get("month","")),
                channel_id, mapped_channel, activity_id, mapped_activity,
                str(e.get("bu","")) or mapping["bu"],
                str(e.get("finance_cat","")) or mapping["finance_cat"],
                str(e.get("marketing_cat","")) or mapping["marketing_cat"],
                str(e.get("description","")),
                planned,                                # NEW planned (overwritten)
                float(e.get("confirmed") or 0),
                float(e.get("actual") or 0),
                str(e.get("jira","")), str(e.get("vendor","")), str(e.get("notes","")),
                str(e.get("approved","False")),
                str(e.get("invoice_names","[]")), str(e.get("invoice_data","[]")),
                str(e.get("entered_by","")), str(e.get("created_at","")), now
            ]])
            overwrote += 1
            saved += 1
        else:
            # Create planned-only entry (PM sync will later fill actual if/when data arrives)
            entry_id = "pln_" + str(uuid.uuid4())[:10]
            ws_entries.append_row([
                entry_id, r["country"], r["quarter"], r["month"],
                channel_id, mapped_channel, activity_id, mapped_activity,
                mapping["bu"], mapping["finance_cat"], mapping["marketing_cat"],
                f"Planned upload: {r['channel_group']}",
                planned, 0, 0,
                "", mapped_activity, f"Planned uploaded {now[:10]}",
                "False", "[]", "[]",
                username, now, now
            ])
            existing_entries.append({"id":entry_id, "country":r["country"],
                                     "channel_id":channel_id, "activity_id":activity_id,
                                     "month":r["month"], "planned":planned})
            entry_lookup.setdefault(lookup_key, []).append((len(existing_entries)-1, existing_entries[-1]))
            created += 1
            saved += 1

    invalidate_cache(TAB_ENTRIES)
    invalidate_cache(TAB_CHANNELS)
    invalidate_cache(TAB_ACTIVITIES)
    invalidate_cache(TAB_BUDGETS)
    return jsonify({
        "ok":True, "saved":saved, "overwrote":overwrote, "created":created,
        "channels_created":channels_created, "activities_created":activities_created,
        "rejected":rejected[:100], "rejected_count":len(rejected),
    })


def _standardise_month(raw):
    """Accept '2025-07', '2025/07', 'Jul-2025', 'Jul 2025', '07-2025', '7/2025'.
    Return canonical 'YYYY-MM' or '' if unparseable."""
    s = str(raw or '').strip()
    if not s:
        return ''
    # direct YYYY-MM
    if len(s) == 7 and s[4] in '-/' and s[:4].isdigit() and s[5:].isdigit():
        return f"{s[:4]}-{s[5:].zfill(2)}"
    # MM-YYYY or MM/YYYY
    parts = s.replace('/', '-').split('-')
    if len(parts) == 2:
        a, b = parts[0].strip(), parts[1].strip()
        if a.isdigit() and b.isdigit() and len(b) == 4:  # MM-YYYY
            return f"{b}-{a.zfill(2)}"
        if a.isdigit() and b.isdigit() and len(a) == 4:  # YYYY-MM already
            return f"{a}-{b.zfill(2)}"
        # month name + year
        months = {'jan':'01','feb':'02','mar':'03','apr':'04','may':'05','jun':'06',
                  'jul':'07','aug':'08','sep':'09','sept':'09','oct':'10','nov':'11','dec':'12'}
        if b.isdigit() and len(b) == 4 and a[:3].lower() in months:
            return f"{b}-{months[a[:3].lower()]}"
        if a.isdigit() and len(a) == 4 and b[:3].lower() in months:
            return f"{a}-{months[b[:3].lower()]}"
    # "Jul 2025" with space
    sp = s.split()
    if len(sp) == 2:
        months = {'jan':'01','feb':'02','mar':'03','apr':'04','may':'05','jun':'06',
                  'jul':'07','aug':'08','sep':'09','sept':'09','oct':'10','nov':'11','dec':'12'}
        if sp[1].isdigit() and len(sp[1])==4 and sp[0][:3].lower() in months:
            return f"{sp[1]}-{months[sp[0][:3].lower()]}"
    return ''


# ═══════════════════════════════════════════════════════════════════
#  ENDPOINT 2: Line-Item Upload (full 18-column manual)
#  Country, Quarter, Month, Channel, Activity, BU, Finance Category,
#  Marketing Category, Description, Planned, Confirmed, Actual,
#  JIRA, Vendor, Notes, Approved
#  (Invoices skipped — binary, UI-only)
#
#  Dedup key: country+month+channel+activity+vendor+description (overwrites).
#  Minimum required: Country, Month, Channel, Planned OR Actual.
#  Blank categories auto-populated via DEFAULT_MAPPING keyword matcher.
#  Approved=True overwrites allowed but flagged in report.
# ═══════════════════════════════════════════════════════════════════
@bp.route("/api/upload/entries", methods=["POST"])
@require_login
def upload_entries():
    if 'file' not in request.files:
        return jsonify({"error":"No file uploaded"}), 400
    try:
        parsed, headers = _parse_upload(request.files['file'])
    except Exception as e:
        return jsonify({"error": f"Could not parse file: {e}"}), 400
    if not parsed:
        return jsonify({"error":"File is empty"}), 400

    norm_rows, rejected = [], []
    for idx, d in enumerate(parsed, start=2):
        co_raw = _pick(d, 'country', 'market')
        mo_raw = _pick(d, 'month', 'period')
        q_raw = _pick(d, 'quarter', 'q')
        ch_name = _pick(d, 'channel', 'channel name', 'channel_name')
        act_name = _pick(d, 'activity', 'activity name', 'activity_name')
        bu = _pick(d, 'bu', 'bu / finance account', 'finance account', 'account')
        fin_cat = _pick(d, 'finance category', 'finance_cat', 'finance cat', 'finance_category')
        mkt_cat = _pick(d, 'marketing category', 'marketing_cat', 'marketing cat', 'marketing_category')
        desc = _pick(d, 'description', 'desc', 'note')
        pln = _num(_pick(d, 'planned', 'planned (aud)', 'planned budget', 'budget'))
        con = _num(_pick(d, 'confirmed', 'confirmed (aud)'))
        act_val = _num(_pick(d, 'actual', 'actual (aud)', 'spend', 'spend up to date'))
        jira = _pick(d, 'jira', 'jira task', 'ticket')
        vendor = _pick(d, 'vendor', 'supplier')
        notes = _pick(d, 'notes', 'comments', 'remark')
        approved_raw = _pick(d, 'approved', 'approval')
        approved = str(approved_raw).strip().lower() in ('true','yes','y','1','approved','✓')

        co = normalise_country(co_raw)
        mo_std = _standardise_month(mo_raw)
        quarter = _normalise_quarter(q_raw, mo_std)

        # Required field checks
        if not co:
            rejected.append({"row":idx,"reason":"missing country","raw":dict(d)}); continue
        if co not in MARKETS:
            rejected.append({"row":idx,"reason":f"unknown country '{co_raw}'","raw":dict(d)}); continue
        if not mo_std or mo_std not in VALID_MONTH_KEYS:
            rejected.append({"row":idx,"reason":f"invalid month '{mo_raw}'","raw":dict(d)}); continue
        if not quarter:
            rejected.append({"row":idx,"reason":f"cannot determine quarter from month '{mo_std}'","raw":dict(d)}); continue
        if not ch_name:
            rejected.append({"row":idx,"reason":"missing channel","raw":dict(d)}); continue
        if pln <= 0 and act_val <= 0 and con <= 0:
            rejected.append({"row":idx,"reason":"no planned/confirmed/actual value","raw":dict(d)}); continue

        # Auto-populate categories if blank
        if not bu or not fin_cat or not mkt_cat:
            auto_bu, auto_fc, auto_mc = _apply_keyword_mapping(ch_name)
            bu = bu or auto_bu
            fin_cat = fin_cat or auto_fc
            mkt_cat = mkt_cat or auto_mc

        norm_rows.append({
            "country":co, "quarter":quarter, "month":mo_std,
            "channel_name":ch_name, "activity_name":act_name,
            "bu":bu, "finance_cat":fin_cat, "marketing_cat":mkt_cat,
            "description":desc,
            "planned":pln, "confirmed":con, "actual":act_val,
            "jira":jira, "vendor":vendor, "notes":notes, "approved":approved,
            "_src_row":idx,
        })

    # RBAC
    allowed, rbac_rej = _filter_rbac(norm_rows)
    for r in rbac_rej:
        rejected.append({"row":r["_src_row"],"reason":r.get("_reject_reason","RBAC"),"raw":{"country":r["country"]}})

    if not allowed:
        return jsonify({"ok":True, "saved":0, "overwrote":0, "created":0,
                       "channels_created":0, "activities_created":0,
                       "approved_overwrites":[],
                       "rejected":rejected[:100], "rejected_count":len(rejected)})

    ws_entries = get_sheet(TAB_ENTRIES)
    ws_channels = get_sheet(TAB_CHANNELS)
    ws_activities = get_sheet(TAB_ACTIVITIES)
    existing_entries = safe_get_records(ws_entries, TAB_ENTRIES)
    existing_channels = safe_get_records(ws_channels, TAB_CHANNELS)
    existing_activities = safe_get_records(ws_activities, TAB_ACTIVITIES)
    now = datetime.utcnow().isoformat()
    username = session.get("username", "line_upload")

    # Channel lookup — line-item upload REQUIRES channel to already exist for that market+quarter
    def find_channel(country, quarter, channel_name):
        for c in existing_channels:
            if (str(c.get("country","")) == country
                and str(c.get("quarter","")) == quarter
                and str(c.get("name","")).strip().lower() == channel_name.strip().lower()):
                return str(c["id"]), str(c.get("name",""))
        return None, None

    activities_created = 0
    def ensure_activity(channel_id, country, quarter, activity_name):
        nonlocal activities_created
        if not activity_name:
            return ""
        for a in existing_activities:
            if (str(a.get("channel_id","")) == channel_id
                and str(a.get("country","")) == country
                and str(a.get("quarter","")) == quarter
                and str(a.get("name","")).strip().lower() == activity_name.strip().lower()):
                return str(a["id"])
        aid = "act_" + str(uuid.uuid4())[:8]
        so = len([a for a in existing_activities if str(a.get("channel_id",""))==channel_id])
        ws_activities.append_row([aid, channel_id, country, quarter, activity_name, so, now])
        existing_activities.append({"id":aid,"channel_id":channel_id,"country":country,
                                    "quarter":quarter,"name":activity_name,"sort_order":so})
        activities_created += 1
        return aid

    # Build dedup index: country|month|channel_id|activity_id|vendor_lower|desc_lower
    def dedup_key(country, month, ch_id, act_id, vendor, description):
        return "|".join([country, month, ch_id, act_id or "",
                         (vendor or "").strip().lower(),
                         (description or "").strip().lower()])
    dedup_idx = {}
    for idx, e in enumerate(existing_entries):
        k = dedup_key(str(e.get("country","")), str(e.get("month","")),
                      str(e.get("channel_id","")), str(e.get("activity_id","")),
                      str(e.get("vendor","")), str(e.get("description","")))
        dedup_idx[k] = (idx, e)

    saved, overwrote, created = 0, 0, 0
    approved_overwrites = []

    for r in allowed:
        ch_id, ch_canonical = find_channel(r["country"], r["quarter"], r["channel_name"])
        if not ch_id:
            rejected.append({
                "row": r["_src_row"],
                "reason": f"channel '{r['channel_name']}' does not exist for {r['country']}/{r['quarter']} — admin must create it first",
                "raw": {"country":r["country"],"quarter":r["quarter"],"channel":r["channel_name"]}
            })
            continue
        act_id = ensure_activity(ch_id, r["country"], r["quarter"], r["activity_name"])
        key = dedup_key(r["country"], r["month"], ch_id, act_id, r["vendor"], r["description"])
        match = dedup_idx.get(key)

        if match:
            idx, e = match
            sheet_row = idx + 2
            was_approved = str(e.get("approved","")).strip().lower() == "true"
            if was_approved:
                approved_overwrites.append({
                    "row": r["_src_row"],
                    "entry_id": str(e.get("id","")),
                    "country": r["country"], "month": r["month"],
                    "channel": ch_canonical, "activity": r["activity_name"],
                    "vendor": r["vendor"], "description": r["description"],
                })
            ws_entries.update(f"A{sheet_row}:X{sheet_row}", [[
                str(e.get("id","")), r["country"], r["quarter"], r["month"],
                ch_id, ch_canonical, act_id, r["activity_name"],
                r["bu"] or str(e.get("bu","")),
                r["finance_cat"] or str(e.get("finance_cat","")),
                r["marketing_cat"] or str(e.get("marketing_cat","")),
                r["description"] or str(e.get("description","")),
                r["planned"] if r["planned"]>0 else float(e.get("planned") or 0),
                r["confirmed"] if r["confirmed"]>0 else float(e.get("confirmed") or 0),
                r["actual"] if r["actual"]>0 else float(e.get("actual") or 0),
                r["jira"] or str(e.get("jira","")),
                r["vendor"] or str(e.get("vendor","")),
                r["notes"] or str(e.get("notes","")),
                str(r["approved"]) if r["approved"] else str(e.get("approved","False")),
                str(e.get("invoice_names","[]")), str(e.get("invoice_data","[]")),
                str(e.get("entered_by","")) or username, str(e.get("created_at","")) or now, now
            ]])
            overwrote += 1
            saved += 1
        else:
            entry_id = "li_" + str(uuid.uuid4())[:10]
            ws_entries.append_row([
                entry_id, r["country"], r["quarter"], r["month"],
                ch_id, ch_canonical, act_id, r["activity_name"],
                r["bu"], r["finance_cat"], r["marketing_cat"],
                r["description"],
                r["planned"], r["confirmed"], r["actual"],
                r["jira"], r["vendor"], r["notes"],
                str(r["approved"]),
                "[]", "[]",
                username, now, now
            ])
            existing_entries.append({"id":entry_id, "country":r["country"], "month":r["month"],
                                     "channel_id":ch_id, "activity_id":act_id,
                                     "vendor":r["vendor"], "description":r["description"]})
            dedup_idx[key] = (len(existing_entries)-1, existing_entries[-1])
            created += 1
            saved += 1

    invalidate_cache(TAB_ENTRIES)
    invalidate_cache(TAB_CHANNELS)
    invalidate_cache(TAB_ACTIVITIES)
    return jsonify({
        "ok":True, "saved":saved, "overwrote":overwrote, "created":created,
        "channels_created":0,  # line-item upload does not create channels
        "activities_created":activities_created,
        "approved_overwrites":approved_overwrites,  # flagged for admin review
        "rejected":rejected[:100], "rejected_count":len(rejected),
    })


# ═══════════════════════════════════════════════════════════════════
#  BULK DELETE (admin only)
#  Filter by country+quarter + optional channel + optional month
#  Two-phase: POST /api/bulk_delete/preview then POST /api/bulk_delete/commit
# ═══════════════════════════════════════════════════════════════════
def _filter_entries_for_bulk(d):
    country = d.get("country","").strip()
    quarter = d.get("quarter","").strip()
    channel_name = d.get("channel","").strip()
    activity_name = d.get("activity","").strip()   # NEW: optional activity filter
    month = d.get("month","").strip()
    if not country or not quarter:
        return None, "country and quarter are required"
    rows = safe_get_records(get_sheet(TAB_ENTRIES), TAB_ENTRIES)
    out = []
    for idx, e in enumerate(rows):
        if str(e.get("country","")) != country: continue
        if str(e.get("quarter","")) != quarter: continue
        if channel_name and str(e.get("channel_name","")).strip().lower() != channel_name.lower(): continue
        if activity_name and str(e.get("activity_name","")).strip().lower() != activity_name.lower(): continue
        if month and str(e.get("month","")) != month: continue
        out.append((idx, e))
    return out, None


@bp.route("/api/bulk_delete/preview", methods=["POST"])
@require_login
@require_admin
def bulk_delete_preview():
    d = request.get_json() or {}
    matches, err = _filter_entries_for_bulk(d)
    if err:
        return jsonify({"error":err}), 400
    total_planned = sum(float(e.get("planned") or 0) for _, e in matches)
    total_actual = sum(float(e.get("actual") or 0) for _, e in matches)
    # Return all matching rows (checkbox UI needs to show every one)
    # Cap at 500 as a safety valve — if you need to delete more than that,
    # use a narrower filter.
    MAX_PREVIEW = 500
    sample = [{
        "id": str(e.get("id","")),
        "month": str(e.get("month","")),
        "channel": str(e.get("channel_name","")),
        "activity": str(e.get("activity_name","")),
        "vendor": str(e.get("vendor","")),
        "description": str(e.get("description","")),
        "planned": float(e.get("planned") or 0),
        "actual": float(e.get("actual") or 0),
        "approved": str(e.get("approved","")).lower() == "true",
    } for _, e in matches[:MAX_PREVIEW]]
    return jsonify({
        "ok":True,
        "count": len(matches),
        "truncated": len(matches) > MAX_PREVIEW,
        "total_planned": round(total_planned, 2),
        "total_actual": round(total_actual, 2),
        "sample": sample,
    })


@bp.route("/api/bulk_delete/commit", methods=["POST"])
@require_login
@require_admin
def bulk_delete_commit():
    """Delete entries by explicit ID list (checkbox-selected in UI).
    Request body: {ids: [...], confirmation: "DELETE N SELECTED"}
    """
    d = request.get_json() or {}
    confirmation = str(d.get("confirmation","")).strip()
    ids_to_delete = [str(x).strip() for x in (d.get("ids") or []) if str(x).strip()]
    if not ids_to_delete:
        return jsonify({"error":"No entries selected. Tick the checkboxes for rows you want to delete."}), 400

    expected = f"DELETE {len(ids_to_delete)} SELECTED"
    if confirmation != expected:
        return jsonify({"error":f"Confirmation mismatch. Type exactly: {expected}"}), 400

    # Resolve IDs to sheet row indices
    ws = get_sheet(TAB_ENTRIES)
    rows = safe_get_records(ws, TAB_ENTRIES)
    id_set = set(ids_to_delete)
    to_delete = []
    for idx, e in enumerate(rows):
        if str(e.get("id","")) in id_set:
            to_delete.append((idx, str(e.get("id",""))))

    if not to_delete:
        return jsonify({"error":"None of the provided IDs were found in the sheet (maybe already deleted)."}), 400

    # Delete bottom-up so indices stay valid
    to_delete.sort(key=lambda t: t[0], reverse=True)
    deleted_ids = []
    for idx, eid in to_delete:
        try:
            ws.delete_rows(idx + 2)
            deleted_ids.append(eid)
        except Exception as ex:
            print(f"[bulk_delete] failed row {idx+2} (id={eid}): {ex}")
    invalidate_cache(TAB_ENTRIES)
    return jsonify({
        "ok": True,
        "deleted": len(deleted_ids),
        "deleted_ids": deleted_ids[:200],
        "requested": len(ids_to_delete),
        "not_found": len(ids_to_delete) - len(deleted_ids),
    })


# ═══════════════════════════════════════════════════════════════════
#  CSV TEMPLATE DOWNLOADS
# ═══════════════════════════════════════════════════════════════════
@bp.route("/api/template/planned")
@require_login
def template_planned():
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Country","Month","Channel_Group","Planned"])
    w.writerow(["TH","2025-07","Meta","22000"])
    w.writerow(["TH","2025-07","Bing","10000"])
    w.writerow(["TH","2025-07","Affiliates","8000"])
    w.writerow(["HKG","2025-07","Meta","12000"])
    w.writerow(["HKG","2025-07","Apple Search Ads","5000"])
    return send_file(io.BytesIO(buf.getvalue().encode("utf-8-sig")),
                     mimetype="text/csv", as_attachment=True,
                     download_name="planned_template.csv")


@bp.route("/api/template/entries")
@require_login
def template_entries():
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Country","Quarter","Month","Channel","Activity","BU","Finance Category",
                "Marketing Category","Description","Planned","Confirmed","Actual",
                "JIRA","Vendor","Notes","Approved"])
    w.writerow(["TH","Q2","2025-10","Campaign/Promotions","Black Friday","",
                "Campaigns/Promotions","Brand / OOH","Facebook ads + OOH",
                "5000","0","4823","APAC-123","Meta Platforms","Ran 2 weeks","False"])
    w.writerow(["VN","Q2","2025-11","Events","Trader Meetup HCMC","",
                "Event","Events & Sponsorship","Venue + catering",
                "8000","0","0","APAC-145","Sheraton","","False"])
    w.writerow(["TH","Q1","2025-08","Performance Marketing","Meta","","","",
                "Override: Meta agency extra spend",
                "0","0","500","","","Extra spend outside BQ tracking","False"])
    return send_file(io.BytesIO(buf.getvalue().encode("utf-8-sig")),
                     mimetype="text/csv", as_attachment=True,
                     download_name="line_items_template.csv")


@bp.route("/api/template/budgets")
@require_login
def template_budgets():
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Country","Quarter","Total Budget (AUD)"])
    w.writerow(["TH","Q1","150000"])
    w.writerow(["TH","Q2","150000"])
    w.writerow(["HKG","Q1","180000"])
    return send_file(io.BytesIO(buf.getvalue().encode("utf-8-sig")),
                     mimetype="text/csv", as_attachment=True,
                     download_name="budgets_template.csv")


@bp.route("/api/template/channels")
@require_login
def template_channels():
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Country","Quarter","Channel Name","Budget (AUD)"])
    w.writerow(["TH","Q1","Performance Marketing","80000"])
    w.writerow(["TH","Q1","Affiliate - CPA & FF","10000"])
    w.writerow(["TH","Q1","Campaign/Promotions","15000"])
    w.writerow(["TH","Q1","Events","20000"])
    return send_file(io.BytesIO(buf.getvalue().encode("utf-8-sig")),
                     mimetype="text/csv", as_attachment=True,
                     download_name="channels_template.csv")
