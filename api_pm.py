# -*- coding: utf-8 -*-
"""BigQuery PM data sync - preview, manual sync, auto sync."""
import uuid, time
from datetime import datetime
from collections import defaultdict
from flask import Blueprint, request, jsonify, session
from google.oauth2.service_account import Credentials
from google.cloud import bigquery

from config import *
from sheets_helper import get_sheet, safe_get_records, invalidate_cache, get_records_cached
from auth import require_login, require_admin

bp = Blueprint('pm', __name__)

def _get_bq_client():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return bigquery.Client(project=BQ_PROJECT_ID, credentials=creds, location=BQ_LOCATION)

def _bq_fetch_pm_data(q_filter=None):
    """Query BigQuery for PM data, aggregated by country+channel_group+month."""
    client = _get_bq_client()
    table_ref = f"`{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`"
    apac_countries = "','".join(PM_COUNTRY_MAP.keys())
    query = f"""
    SELECT Country, Channel_Group,
        EXTRACT(YEAR FROM Date) AS yr, EXTRACT(MONTH FROM Date) AS mo,
        SUM(IFNULL(Spend_AUD, 0)) AS total_spend,
        SUM(IFNULL(QL, 0)) AS total_ql,
        SUM(IFNULL(FT, 0)) AS total_ft,
        COUNT(*) AS row_count
    FROM {table_ref}
    WHERE Country IN ('{apac_countries}')
      AND Channel_Group IS NOT NULL
      AND Channel_Group NOT IN ('Organic', 'IB', '')
    GROUP BY Country, Channel_Group, yr, mo
    ORDER BY Country, Channel_Group, yr, mo
    """
    results = list(client.query(query).result())
    agg_rows = []; total_raw_rows = 0
    for row in results:
        tracker_country = PM_COUNTRY_MAP.get(str(row.Country or '').strip())
        if not tracker_country: continue
        yr, mo = int(row.yr or 0), int(row.mo or 0)
        if not yr or not mo: continue
        quarter = MONTH_TO_QUARTER.get(mo)
        month_key = MONTH_KEY_MAP.get((yr, mo), "")
        if not quarter or not month_key: continue
        if q_filter and quarter != q_filter: continue
        rc = int(row.row_count or 0); total_raw_rows += rc
        agg_rows.append({"country": tracker_country, "channel_group": str(row.Channel_Group or '').strip(), "month_key": month_key, "quarter": quarter, "spend": round(float(row.total_spend or 0), 2), "ql": round(float(row.total_ql or 0)), "ft": round(float(row.total_ft or 0)), "rows": rc})
    return agg_rows, total_raw_rows

@bp.route("/api/pm/preview")
@require_login
@require_admin
def preview():
    try:
        q_filter = request.args.get("quarter", "")
        agg_rows, total_raw_rows = _bq_fetch_pm_data(q_filter or None)
        preview = []
        for r in sorted(agg_rows, key=lambda x: (x["country"], x["channel_group"], x["month_key"])):
            mapping = PM_CHANNEL_MAP.get(r["channel_group"], PM_DEFAULT_MAPPING)
            preview.append({"country": r["country"], "channel_group": r["channel_group"], "month": r["month_key"], "quarter": r["quarter"], "spend": r["spend"], "ql": r["ql"], "ft": r["ft"], "rows": r["rows"], "mapped_channel": mapping.get("channel_name","Performance Marketing (PM)"), "mapped_bu": mapping.get("bu",""), "mapped_finance_cat": mapping.get("finance_cat",""), "mapped_marketing_cat": mapping.get("marketing_cat","")})
        return jsonify({"total_rows": total_raw_rows, "apac_rows": sum(r["rows"] for r in agg_rows), "total_spend": round(sum(r["spend"] for r in agg_rows), 2), "preview": preview, "debug": {"source": "BigQuery", "project": BQ_PROJECT_ID, "table": f"{BQ_DATASET}.{BQ_TABLE}", "quarter_filter": q_filter}})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"BigQuery read failed: {str(e)}"}), 500

@bp.route("/api/pm/sync", methods=["POST"])
@require_login
@require_admin
def sync():
    rows_to_sync = request.get_json()
    if not rows_to_sync: return jsonify({"error": "No rows provided"}), 400
    ws_entries = get_sheet(TAB_ENTRIES); ws_channels = get_sheet(TAB_CHANNELS); ws_activities = get_sheet(TAB_ACTIVITIES)
    existing_channels = safe_get_records(ws_channels, TAB_CHANNELS); existing_activities = safe_get_records(ws_activities, TAB_ACTIVITIES)
    now = datetime.utcnow().isoformat()
    saved, skipped, channels_created, activities_created = 0, 0, 0, 0
    def ensure_channel(country, quarter, mapped_channel):
        nonlocal channels_created
        ch = next((c for c in existing_channels if str(c["country"])==country and str(c["quarter"])==quarter and str(c["name"]).strip()==mapped_channel), None)
        if ch: return str(ch["id"])
        ch_id = "ch_" + str(uuid.uuid4())[:8]; sort_order = len([c for c in existing_channels if str(c["country"])==country and str(c["quarter"])==quarter])
        ws_channels.append_row([ch_id, country, quarter, mapped_channel, 0, sort_order, now])
        existing_channels.append({"id":ch_id,"country":country,"quarter":quarter,"name":mapped_channel,"budget":0,"sort_order":sort_order}); channels_created += 1
        return ch_id
    for row in rows_to_sync:
        country, quarter, month = row.get("country",""), row.get("quarter",""), row.get("month","")
        spend = float(row.get("spend",0)); channel_group = row.get("channel_group","")
        mapped_channel = row.get("mapped_channel","Performance Marketing (PM)")
        if spend <= 0: skipped += 1; continue
        channel_id = ensure_channel(country, quarter, mapped_channel)
        entry_id = "pm_" + str(uuid.uuid4())[:10]
        ws_entries.append_row([entry_id, country, quarter, month, channel_id, mapped_channel, "", "", row.get("mapped_bu",""), row.get("mapped_finance_cat",""), row.get("mapped_marketing_cat",""), f"PM Sync: {channel_group}", 0, 0, round(spend,2), "", mapped_channel, f"Synced from BigQuery {now[:10]}", "False", "[]", "[]", session.get("username","pm_sync"), now, now])
        saved += 1
    invalidate_cache(TAB_ENTRIES); invalidate_cache(TAB_CHANNELS); invalidate_cache(TAB_ACTIVITIES)
    return jsonify({"ok":True, "saved":saved, "skipped":skipped, "channels_created":channels_created, "activities_created":activities_created})

@bp.route("/api/pm/auto_sync")
@require_login
@require_admin
def auto_sync():
    try:
        agg_rows, total_raw_rows = _bq_fetch_pm_data()
        if not agg_rows:
            return jsonify({"ok":True, "message":"No APAC data in BigQuery", "synced":0, "updated":0, "skipped":0})
        agg = {}
        for r in agg_rows:
            key = f"{r['country']}|{r['channel_group']}|{r['month_key']}|{r['quarter']}"
            if key in agg: agg[key]["spend"]+=r["spend"]; agg[key]["rows"]+=r["rows"]
            else: agg[key] = dict(r)
        ws_entries = get_sheet(TAB_ENTRIES); ws_channels = get_sheet(TAB_CHANNELS); ws_activities = get_sheet(TAB_ACTIVITIES)
        existing_entries = safe_get_records(ws_entries, TAB_ENTRIES)
        existing_channels = safe_get_records(ws_channels, TAB_CHANNELS)
        existing_activities = safe_get_records(ws_activities, TAB_ACTIVITIES)
        now = datetime.utcnow().isoformat()
        valid_countries = set(str(b.get("country","")) for b in get_records_cached(TAB_BUDGETS) if float(b.get("total_budget") or 0)>0) | set(MARKETS)
        # Build lookup for existing pm_ entries
        known_channels = set(PM_CHANNEL_MAP.keys()); existing_pm = {}
        for idx, e in enumerate(existing_entries):
            eid = str(e.get("id","")); 
            if not eid.startswith("pm_"): continue
            co, mo = str(e.get("country","")), str(e.get("month",""))
            vendor, desc, act_name = str(e.get("vendor","")).strip(), str(e.get("description","")).strip(), str(e.get("activity_name","")).strip()
            cg = ""
            if vendor in known_channels: cg = vendor
            elif ": $" in desc: cg_c = desc.split(": $")[0].strip(); cg = cg_c if cg_c in known_channels else ""
            elif desc.startswith("PM Sync: "): cg_c = desc[9:].strip(); cg = cg_c if cg_c in known_channels else ""
            if not cg and " - " in act_name:
                parts = act_name.split(" - ")
                if len(parts)>=2: cg_c = parts[1].strip(); cg = cg_c if cg_c in known_channels else ""
            if cg and co and mo: existing_pm[f"{co}|{cg}|{mo}"] = {"idx":idx,"entry":e}
        synced, updated, skipped, channels_created, activities_created, _write_count = 0, 0, 0, 0, 0, 0
        for agg_key, vals in sorted(agg.items()):
            parts = agg_key.split("|"); country, channel_group, month_key, quarter = parts[0], parts[1], parts[2], parts[3]
            spend = round(vals["spend"], 2)
            if country not in valid_countries: skipped+=1; continue
            # Skip zero spend early — don't create channels/activities for $0
            lookup_key = f"{country}|{channel_group}|{month_key}"
            existing = existing_pm.get(lookup_key)
            if spend <= 0 and not existing: skipped+=1; continue
            if spend <= 0 and existing and float(existing["entry"].get("actual") or 0) > 0: skipped+=1; continue
            mapping = PM_CHANNEL_MAP.get(channel_group, PM_DEFAULT_MAPPING)
            mapped_channel = mapping["channel_name"]; mapped_bu = mapping["bu"]; mapped_finance_cat = mapping["finance_cat"]; mapped_marketing_cat = mapping["marketing_cat"]
            month_short = MONTH_SHORT.get(month_key, month_key)
            activity_name = f"{country} FY26 {quarter} - {channel_group} - {month_short}"
            # Find/create channel (only if we have spend)
            ch = next((c for c in existing_channels if str(c["country"])==country and str(c["quarter"])==quarter and str(c["name"]).strip()==mapped_channel), None)
            if ch: channel_id = str(ch["id"])
            else:
                channel_id = "ch_"+str(uuid.uuid4())[:8]; so = len([c for c in existing_channels if str(c["country"])==country and str(c["quarter"])==quarter])
                ws_channels.append_row([channel_id,country,quarter,mapped_channel,0,so,now]); existing_channels.append({"id":channel_id,"country":country,"quarter":quarter,"name":mapped_channel,"budget":0,"sort_order":so}); channels_created+=1
            # Find/create activity (only if we have spend)
            act = next((a for a in existing_activities if str(a.get("channel_id",""))==channel_id and str(a.get("country",""))==country and str(a.get("quarter",""))==quarter and str(a.get("name","")).strip()==activity_name), None)
            if act: activity_id = str(act["id"])
            else:
                activity_id = "act_"+str(uuid.uuid4())[:8]; so = len([a for a in existing_activities if str(a.get("channel_id",""))==channel_id])
                ws_activities.append_row([activity_id,channel_id,country,quarter,activity_name,so,now]); existing_activities.append({"id":activity_id,"channel_id":channel_id,"country":country,"quarter":quarter,"name":activity_name,"sort_order":so}); activities_created+=1
            if existing:
                old_spend = float(existing["entry"].get("actual") or 0)
                old_planned = float(existing["entry"].get("planned") or 0)
                if spend<=0 and old_spend>0: skipped+=1; continue
                sheet_row = existing["idx"]+2
                e = existing["entry"]
                try:
                    # Build full row update in one API call (avoids quota limits)
                    # Columns: A=id, B=country, C=quarter, D=month, E=channel_id, F=channel_name,
                    #          G=activity_id, H=activity_name, I=bu, J=finance_cat, K=marketing_cat,
                    #          L=description, M=planned(0), N=confirmed, O=actual, P=jira, Q=vendor,
                    #          R=notes, S=approved, T=invoice_names, U=invoice_data, V=entered_by, W=created_at, X=updated_at
                    new_actual = spend if abs(spend-old_spend)>=0.01 else old_spend
                    ws_entries.update(f"A{sheet_row}:X{sheet_row}", [[
                        str(e.get("id","")), str(e.get("country","")), str(e.get("quarter","")), str(e.get("month","")),
                        channel_id, mapped_channel, activity_id, activity_name,
                        str(e.get("bu","")) or mapped_bu, str(e.get("finance_cat","")) or mapped_finance_cat,
                        str(e.get("marketing_cat","")) or mapped_marketing_cat,
                        str(e.get("description","")),
                        0,  # planned - always zero for pm_ entries
                        float(e.get("confirmed") or 0),
                        new_actual,
                        str(e.get("jira","")), mapped_channel, str(e.get("notes","")),
                        str(e.get("approved","False")), str(e.get("invoice_names","[]")), str(e.get("invoice_data","[]")),
                        str(e.get("entered_by","")), str(e.get("created_at","")), now
                    ]])
                    updated+=1; _write_count+=1
                    if _write_count % 25 == 0: time.sleep(1)  # throttle to stay under API quota
                except Exception as ue: print(f"Auto-sync error row {sheet_row}: {ue}"); skipped+=1
                continue
            if spend<=0: skipped+=1; continue
            entry_id = "pm_"+str(uuid.uuid4())[:10]
            ws_entries.append_row([entry_id,country,quarter,month_key,channel_id,mapped_channel,activity_id,activity_name,mapped_bu,mapped_finance_cat,mapped_marketing_cat,f"{channel_group}: ${spend:,.0f} AUD",0,0,spend,"",mapped_channel,f"Auto-synced {now[:10]}","False","[]","[]",session.get("username","pm_auto"),now,now])
            existing_pm[lookup_key] = {"idx":len(existing_entries),"entry":{"actual":spend}}; existing_entries.append({}); synced+=1
            _write_count+=1
            if _write_count % 25 == 0: time.sleep(1)
        invalidate_cache(TAB_ENTRIES); invalidate_cache(TAB_CHANNELS); invalidate_cache(TAB_ACTIVITIES)
        return jsonify({"ok":True, "synced":synced, "updated":updated, "skipped":skipped, "channels_created":channels_created, "activities_created":activities_created, "total_pm_rows":total_raw_rows, "total_agg_rows":len(agg)})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Auto-sync failed: {str(e)}"}), 500