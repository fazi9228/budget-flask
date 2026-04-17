# -*- coding: utf-8 -*-
"""BigQuery PM data sync — preview, manual sync, auto-sync.

DATA ACCESS (for Postgres port):
  reads:  get_sheet, safe_get_records, get_records_cached, rows_for
  writes: ws.append_row, ws.update, invalidate_cache
See PORTING.md for the abstraction surface.

KEY INVARIANTS:
  1. Sync NEVER creates tracker channels. If a market lacks the required
     umbrella channel (Performance Marketing or Affiliate - CPA & FF),
     sync skips its rows with a clear reason.
  2. Activities under the umbrella are normalised to canonical names
     (e.g. "Meta HK" in BQ → "Meta" activity).
  3. Planned is never overwritten — sync only updates actual.
"""
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
    """Query BigQuery. Returns (agg_rows, total_raw_rows).
    Each agg_row has: country, channel_group (NORMALISED), month_key, quarter, spend, ql, ft, rows."""
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
    total_raw_rows = 0
    # RE-AGGREGATE after normalisation — "Meta HK" and "Meta Agency" collapse to "Meta"
    bucket = defaultdict(lambda: {"spend":0.0, "ql":0, "ft":0, "rows":0})
    for row in results:
        tracker_country = PM_COUNTRY_MAP.get(str(row.Country or '').strip())
        if not tracker_country:
            continue
        yr, mo = int(row.yr or 0), int(row.mo or 0)
        if not yr or not mo:
            continue
        quarter = MONTH_TO_QUARTER.get(mo)
        month_key = MONTH_KEY_MAP.get((yr, mo), "")
        if not quarter or not month_key:
            continue
        if q_filter and quarter != q_filter:
            continue
        raw_cg = str(row.Channel_Group or '').strip()
        cg_norm = normalise_channel_group(raw_cg)
        rc = int(row.row_count or 0)
        total_raw_rows += rc
        k = (tracker_country, cg_norm, month_key, quarter)
        bucket[k]["spend"] += float(row.total_spend or 0)
        bucket[k]["ql"]    += int(row.total_ql or 0)
        bucket[k]["ft"]    += int(row.total_ft or 0)
        bucket[k]["rows"]  += rc
    agg_rows = []
    for (country, cg_norm, month_key, quarter), v in bucket.items():
        agg_rows.append({
            "country": country,
            "channel_group": cg_norm,
            "month_key": month_key,
            "quarter": quarter,
            "spend": round(v["spend"], 2),
            "ql": round(v["ql"]),
            "ft": round(v["ft"]),
            "rows": v["rows"],
        })
    return agg_rows, total_raw_rows


@bp.route("/api/pm/preview")
@require_login
@require_admin
def preview():
    try:
        q_filter = request.args.get("quarter", "")
        agg_rows, total_raw_rows = _bq_fetch_pm_data(q_filter or None)
        preview_rows = []
        for r in sorted(agg_rows, key=lambda x: (x["country"], x["channel_group"], x["month_key"])):
            mapping = PM_CHANNEL_MAP.get(r["channel_group"], PM_DEFAULT_MAPPING)
            preview_rows.append({
                "country": r["country"],
                "channel_group": r["channel_group"],
                "month": r["month_key"],
                "quarter": r["quarter"],
                "spend": r["spend"],
                "ql": r["ql"],
                "ft": r["ft"],
                "rows": r["rows"],
                "mapped_channel":  mapping.get("channel_name", PM_UMBRELLA_CHANNEL),
                "mapped_activity": mapping.get("activity_name", r["channel_group"]),
                "mapped_bu":       mapping.get("bu", ""),
                "mapped_finance_cat":   mapping.get("finance_cat", ""),
                "mapped_marketing_cat": mapping.get("marketing_cat", ""),
            })
        return jsonify({
            "total_rows": total_raw_rows,
            "apac_rows": sum(r["rows"] for r in agg_rows),
            "total_spend": round(sum(r["spend"] for r in agg_rows), 2),
            "preview": preview_rows,
            "debug": {"source":"BigQuery","project":BQ_PROJECT_ID,
                      "table":f"{BQ_DATASET}.{BQ_TABLE}","quarter_filter":q_filter},
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"BigQuery read failed: {str(e)}"}), 500


def _find_channel(existing_channels, country, quarter, channel_name):
    """Return the channel dict for an EXISTING channel, or None.
    Sync never creates channels — this is a pure lookup."""
    for c in existing_channels:
        if (str(c.get("country","")) == country
            and str(c.get("quarter","")) == quarter
            and str(c.get("name","")).strip() == channel_name):
            return c
    return None


def _ensure_activity(ws_activities, existing_activities, channel_id, country, quarter, activity_name, now):
    """Find-or-create an activity under a channel. Idempotent.
    Returns (activity_id, created_flag)."""
    for a in existing_activities:
        if (str(a.get("channel_id","")) == channel_id
            and str(a.get("country","")) == country
            and str(a.get("quarter","")) == quarter
            and str(a.get("name","")).strip() == activity_name):
            return str(a["id"]), False
    aid = "act_" + str(uuid.uuid4())[:8]
    so = len([a for a in existing_activities if str(a.get("channel_id","")) == channel_id])
    ws_activities.append_row([aid, channel_id, country, quarter, activity_name, so, now])
    existing_activities.append({
        "id": aid, "channel_id": channel_id, "country": country,
        "quarter": quarter, "name": activity_name, "sort_order": so
    })
    return aid, True


@bp.route("/api/pm/sync", methods=["POST"])
@require_login
@require_admin
def sync():
    """Manual sync for rows the admin has explicitly selected in the UI."""
    rows_to_sync = request.get_json()
    if not rows_to_sync:
        return jsonify({"error": "No rows provided"}), 400
    ws_entries = get_sheet(TAB_ENTRIES)
    ws_channels = get_sheet(TAB_CHANNELS)
    ws_activities = get_sheet(TAB_ACTIVITIES)
    existing_channels = safe_get_records(ws_channels, TAB_CHANNELS)
    existing_activities = safe_get_records(ws_activities, TAB_ACTIVITIES)
    now = datetime.utcnow().isoformat()
    saved, skipped, activities_created, skip_reasons = 0, 0, 0, []

    for row in rows_to_sync:
        country = row.get("country","")
        quarter = row.get("quarter","")
        month = row.get("month","")
        spend = float(row.get("spend", 0))
        channel_group = row.get("channel_group","")
        if spend <= 0:
            skipped += 1
            continue
        mapping = PM_CHANNEL_MAP.get(channel_group, PM_DEFAULT_MAPPING)
        mapped_channel = mapping["channel_name"]
        mapped_activity = mapping["activity_name"]

        ch = _find_channel(existing_channels, country, quarter, mapped_channel)
        if not ch:
            skipped += 1
            skip_reasons.append(f"{country}/{quarter}: channel '{mapped_channel}' not configured — create it in Config")
            continue
        channel_id = str(ch["id"])
        activity_id, act_created = _ensure_activity(
            ws_activities, existing_activities, channel_id, country, quarter, mapped_activity, now)
        if act_created:
            activities_created += 1

        entry_id = "pm_" + str(uuid.uuid4())[:10]
        ws_entries.append_row([
            entry_id, country, quarter, month, channel_id, mapped_channel,
            activity_id, mapped_activity,
            mapping.get("bu",""), mapping.get("finance_cat",""), mapping.get("marketing_cat",""),
            f"PM Sync: {channel_group}",
            0, 0, round(spend, 2),
            "", mapped_activity, f"Synced from BigQuery {now[:10]}",
            "False", "[]", "[]",
            session.get("username","pm_sync"), now, now
        ])
        saved += 1

    invalidate_cache(TAB_ENTRIES)
    invalidate_cache(TAB_CHANNELS)
    invalidate_cache(TAB_ACTIVITIES)
    return jsonify({"ok":True, "saved":saved, "skipped":skipped,
                    "channels_created":0,   # sync never creates channels
                    "activities_created":activities_created,
                    "skip_reasons":skip_reasons[:50]})


@bp.route("/api/pm/auto_sync")
@require_login
@require_admin
def auto_sync():
    """Full auto-sync of all BQ rows to tracker.
    - Never creates channels (skips with reason if missing).
    - Updates only `actual` on existing pm_ entries; never touches planned.
    - Creates new pm_ entries for country+activity+month combos that don't exist yet."""
    try:
        agg_rows, total_raw_rows = _bq_fetch_pm_data()
        if not agg_rows:
            return jsonify({"ok":True, "message":"No APAC data in BigQuery",
                           "synced":0, "updated":0, "skipped":0,
                           "channels_created":0, "activities_created":0,
                           "skip_reasons":[], "total_pm_rows":0, "total_agg_rows":0})

        ws_entries = get_sheet(TAB_ENTRIES)
        ws_channels = get_sheet(TAB_CHANNELS)
        ws_activities = get_sheet(TAB_ACTIVITIES)
        existing_entries = safe_get_records(ws_entries, TAB_ENTRIES)
        existing_channels = safe_get_records(ws_channels, TAB_CHANNELS)
        existing_activities = safe_get_records(ws_activities, TAB_ACTIVITIES)
        now = datetime.utcnow().isoformat()

        valid_countries = set(str(b.get("country","")) for b in get_records_cached(TAB_BUDGETS)
                              if float(b.get("total_budget") or 0) > 0) | set(MARKETS)

        # Build lookup for existing pm_ entries by country+channel_id+activity_id+month.
        # Post-migration, pm_ entries have correct channel_id/activity_id pointing to
        # the umbrella channel, so we match on IDs directly — much more robust than
        # the old string-parsing approach.
        existing_pm = {}
        for idx, e in enumerate(existing_entries):
            eid = str(e.get("id",""))
            if not eid.startswith("pm_"):
                continue
            co = str(e.get("country",""))
            mo = str(e.get("month",""))
            ch_id = str(e.get("channel_id",""))
            act_id = str(e.get("activity_id",""))
            if co and mo and ch_id and act_id:
                existing_pm[f"{co}|{ch_id}|{act_id}|{mo}"] = {"idx":idx, "entry":e}

        synced, updated, skipped = 0, 0, 0
        activities_created = 0
        skip_reasons = []
        _write_count = 0

        for r in sorted(agg_rows, key=lambda x: (x["country"], x["channel_group"], x["month_key"])):
            country = r["country"]
            channel_group = r["channel_group"]
            month_key = r["month_key"]
            quarter = r["quarter"]
            spend = round(r["spend"], 2)

            if country not in valid_countries:
                skipped += 1
                continue
            if spend <= 0:
                skipped += 1
                continue

            mapping = PM_CHANNEL_MAP.get(channel_group, PM_DEFAULT_MAPPING)
            mapped_channel = mapping["channel_name"]
            mapped_activity = mapping["activity_name"]
            mapped_bu = mapping["bu"]
            mapped_finance_cat = mapping["finance_cat"]
            mapped_marketing_cat = mapping["marketing_cat"]

            # HARD RULE: never create channels
            ch = _find_channel(existing_channels, country, quarter, mapped_channel)
            if not ch:
                skipped += 1
                reason = f"{country}/{quarter}: '{mapped_channel}' channel not set up"
                if reason not in skip_reasons:
                    skip_reasons.append(reason)
                continue
            channel_id = str(ch["id"])

            activity_id, act_created = _ensure_activity(
                ws_activities, existing_activities, channel_id, country, quarter, mapped_activity, now)
            if act_created:
                activities_created += 1

            lookup_key = f"{country}|{channel_id}|{activity_id}|{month_key}"
            existing = existing_pm.get(lookup_key)

            if existing:
                e = existing["entry"]
                old_spend = float(e.get("actual") or 0)
                if abs(spend - old_spend) < 0.01:
                    skipped += 1
                    continue
                sheet_row = existing["idx"] + 2
                try:
                    # Columns A..X (24 cols) — full rewrite of the row
                    ws_entries.update(f"A{sheet_row}:X{sheet_row}", [[
                        str(e.get("id","")), str(e.get("country","")), str(e.get("quarter","")), str(e.get("month","")),
                        channel_id, mapped_channel, activity_id, mapped_activity,
                        str(e.get("bu","")) or mapped_bu,
                        str(e.get("finance_cat","")) or mapped_finance_cat,
                        str(e.get("marketing_cat","")) or mapped_marketing_cat,
                        str(e.get("description","")) or f"{channel_group}: ${spend:,.0f} AUD",
                        0,                                     # planned — always zero for pm_ entries
                        float(e.get("confirmed") or 0),        # confirmed — untouched
                        spend,                                 # actual — updated
                        str(e.get("jira","")), mapped_activity, str(e.get("notes","")),
                        str(e.get("approved","False")),
                        str(e.get("invoice_names","[]")), str(e.get("invoice_data","[]")),
                        str(e.get("entered_by","")), str(e.get("created_at","")), now
                    ]])
                    updated += 1
                    _write_count += 1
                    if _write_count % 25 == 0:
                        time.sleep(1)  # throttle to stay under API quota
                except Exception as ue:
                    print(f"Auto-sync error row {sheet_row}: {ue}")
                    skipped += 1
                continue

            # Create new pm_ entry
            entry_id = "pm_" + str(uuid.uuid4())[:10]
            ws_entries.append_row([
                entry_id, country, quarter, month_key, channel_id, mapped_channel,
                activity_id, mapped_activity,
                mapped_bu, mapped_finance_cat, mapped_marketing_cat,
                f"{channel_group}: ${spend:,.0f} AUD",
                0, 0, spend,
                "", mapped_activity, f"Auto-synced {now[:10]}",
                "False", "[]", "[]",
                session.get("username","pm_auto"), now, now
            ])
            existing_pm[lookup_key] = {"idx": len(existing_entries), "entry": {"actual": spend}}
            existing_entries.append({})
            synced += 1
            _write_count += 1
            if _write_count % 25 == 0:
                time.sleep(1)

        invalidate_cache(TAB_ENTRIES)
        invalidate_cache(TAB_CHANNELS)
        invalidate_cache(TAB_ACTIVITIES)
        return jsonify({
            "ok":True, "synced":synced, "updated":updated, "skipped":skipped,
            "channels_created":0,              # invariant: sync never creates channels
            "activities_created":activities_created,
            "total_pm_rows":total_raw_rows, "total_agg_rows":len(agg_rows),
            "skip_reasons":skip_reasons[:50],
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Auto-sync failed: {str(e)}"}), 500


@bp.route("/api/pm/readiness")
@require_login
@require_admin
def readiness():
    """Check which markets have the required umbrella channels set up.
    Useful to call before running a sync."""
    try:
        channels = get_records_cached(TAB_CHANNELS)
        markets = [m for m in MARKETS if m != ADMIN_MARKET]
        required = [PM_UMBRELLA_CHANNEL, AFFILIATE_CHANNEL]
        missing = []
        for mkt in markets:
            for q in QUARTERS:
                for req in required:
                    has = any(
                        str(c.get("country","")) == mkt
                        and str(c.get("quarter","")) == q
                        and str(c.get("name","")).strip() == req
                        for c in channels
                    )
                    if not has:
                        missing.append({"country":mkt, "quarter":q, "channel":req})
        return jsonify({"ok":True, "missing":missing, "markets_checked":markets})
    except Exception as e:
        return jsonify({"error":str(e)}), 500
