# -*- coding: utf-8 -*-
"""
DIAGNOSTIC (read-only): find structural problems that cause amount inflation
in the analytics page for PM channels.

Specifically catches:
  1. Duplicate pm_ entries — same (country, activity_name, month) appearing more
     than once. Sum of `actual` here is the inflation amount.
  2. Inconsistent finance_cat for the same activity_name — same vendor tagged
     under multiple finance_cats (e.g. Douyin under both PPC and Paid Social-Douyin).
  3. pm_ entries whose activity_name is in PM_CHANNEL_MAP but whose finance_cat
     does NOT match the canonical mapping — these are mis-classified entries.
  4. pm_ entries whose channel_id no longer exists in the Channels sheet
     (orphans from a migration).

Reads only — never writes. Safe to run anywhere.

For the equivalent in-app diagnostic against the live backend (Postgres
in prod, Sheets locally), use the admin endpoint:
  GET /api/admin/pm_diagnose[?country=&month=&activity=]

Usage (CLI, against local Sheets):
  python diagnose_pm_duplicates.py
"""
from collections import defaultdict, Counter

from config import TAB_ENTRIES, TAB_CHANNELS, TAB_ACTIVITIES, PM_CHANNEL_MAP
from sheets_helper import get_sheet, safe_get_records


def main():
    print("\n" + "=" * 70)
    print(" PM DUPLICATE / MIS-CLASSIFICATION DIAGNOSTIC")
    print("=" * 70)

    entries = safe_get_records(get_sheet(TAB_ENTRIES), TAB_ENTRIES)
    channels = safe_get_records(get_sheet(TAB_CHANNELS), TAB_CHANNELS)
    activities = safe_get_records(get_sheet(TAB_ACTIVITIES), TAB_ACTIVITIES)

    pm_entries = [e for e in entries if str(e.get("id", "")).startswith("pm_")]
    other_entries = [e for e in entries if not str(e.get("id", "")).startswith("pm_")]
    channel_ids = {str(c.get("id", "")) for c in channels}
    activity_ids = {str(a.get("id", "")) for a in activities}

    print(f"\nTotal entries:    {len(entries)}")
    print(f"  pm_ entries:    {len(pm_entries)}")
    print(f"  manual / other: {len(other_entries)}")
    print(f"Channels in sheet: {len(channels)}")
    print(f"Activities in sheet: {len(activity_ids)}")

    # ── 1. Duplicate pm_ entries by (country, activity_name, month) ────────
    print("\n" + "-" * 70)
    print(" 1. DUPLICATE pm_ ENTRIES")
    print("    (same country + activity_name + month → should be 1, not >1)")
    print("-" * 70)

    bucket = defaultdict(list)
    for e in pm_entries:
        key = (str(e.get("country", "")), str(e.get("activity_name", "")), str(e.get("month", "")))
        bucket[key].append(e)

    dups = {k: v for k, v in bucket.items() if len(v) > 1}
    if not dups:
        print("\n  None found.")
    else:
        total_inflation = 0.0
        for (co, act, mo), rows in sorted(dups.items()):
            actuals = [float(r.get("actual") or 0) for r in rows]
            inflation = sum(actuals) - max(actuals)  # excess beyond the largest
            total_inflation += inflation
            print(f"\n  {co} / {act} / {mo}  — {len(rows)} duplicate rows, "
                  f"actuals={actuals}, INFLATION ${inflation:,.2f}")
            for r in rows:
                print(f"    id={r.get('id','')[:14]:14}  "
                      f"ch_id={r.get('channel_id','')[:14]:14}  "
                      f"act_id={r.get('activity_id','')[:14]:14}  "
                      f"finance_cat={r.get('finance_cat','')}")
        print(f"\n  >>> TOTAL DUPLICATE-INDUCED INFLATION: ${total_inflation:,.2f}")

    # ── 2. Inconsistent finance_cat for same activity_name ────────────────
    print("\n" + "-" * 70)
    print(" 2. INCONSISTENT finance_cat FOR SAME activity_name")
    print("    (e.g. Douyin tagged as both 'PPC' and 'Paid Social-Douyin')")
    print("-" * 70)

    by_act = defaultdict(Counter)
    for e in pm_entries:
        act = str(e.get("activity_name", ""))
        fc = str(e.get("finance_cat", ""))
        if act and fc:
            by_act[act][fc] += 1

    inconsistent = {act: cnt for act, cnt in by_act.items() if len(cnt) > 1}
    if not inconsistent:
        print("\n  None found.")
    else:
        for act, cnt in sorted(inconsistent.items()):
            canonical = PM_CHANNEL_MAP.get(act, {}).get("finance_cat", "<not in map>")
            print(f"\n  Activity '{act}'  — {dict(cnt)}")
            print(f"    Canonical (PM_CHANNEL_MAP): '{canonical}'")

    # ── 3. pm_ entries whose finance_cat doesn't match the canonical map ─
    print("\n" + "-" * 70)
    print(" 3. pm_ ENTRIES WITH STALE finance_cat (vs current PM_CHANNEL_MAP)")
    print("-" * 70)

    stale_fc = []
    for e in pm_entries:
        act = str(e.get("activity_name", ""))
        if act not in PM_CHANNEL_MAP:
            continue
        canonical = PM_CHANNEL_MAP[act]
        for field in ("bu", "finance_cat", "marketing_cat"):
            cur = str(e.get(field, ""))
            want = canonical[field]
            if cur and cur != want:
                stale_fc.append((e, field, cur, want))
                break  # one mismatch per row is enough to flag it

    if not stale_fc:
        print("\n  None found.")
    else:
        print(f"\n  Found {len(stale_fc)} mis-classified pm_ entries:")
        for e, field, cur, want in stale_fc[:30]:
            print(f"    {e.get('country','')}/{e.get('month','')}  "
                  f"act={e.get('activity_name',''):16}  "
                  f"{field}: '{cur}' -> '{want}'")
        if len(stale_fc) > 30:
            print(f"    ... and {len(stale_fc) - 30} more")

    # ── 4. pm_ entries pointing to non-existent channel_id / activity_id ─
    print("\n" + "-" * 70)
    print(" 4. pm_ ENTRIES POINTING TO MISSING channel_id / activity_id")
    print("-" * 70)

    orphans_ch = [e for e in pm_entries if str(e.get("channel_id", "")) not in channel_ids]
    orphans_act = [e for e in pm_entries if str(e.get("activity_id", "")) not in activity_ids]
    print(f"\n  pm_ entries with missing channel_id: {len(orphans_ch)}")
    print(f"  pm_ entries with missing activity_id: {len(orphans_act)}")
    for e in orphans_ch[:10]:
        print(f"    {e.get('country','')}/{e.get('quarter','')}/{e.get('month','')}  "
              f"act={e.get('activity_name',''):16}  ch_id={e.get('channel_id','')}")
    if len(orphans_ch) > 10:
        print(f"    ... and {len(orphans_ch) - 10} more")

    # ── 5. Per-activity totals: pm_ vs manual breakdown ──────────────────
    # This is what tells you whether the tracker matches BQ. For each
    # activity (Meta, Douyin, AdRoll, ...), show:
    #   - pm_ actual total: should match BQ for that activity
    #   - manual entries' planned + actual: legitimate human-entered numbers
    # If tracker shows AdRoll = $54k but BQ shows $38k, this breakdown
    # tells you if $16k is duplicate pm_ rows (a bug) or manual entries
    # (legitimate budget/correction).
    print("\n" + "-" * 70)
    print(" 5. PER-ACTIVITY TOTALS (pm_ actuals vs manual planned/actual)")
    print("    Compare 'pm_ actual' against BigQuery. Manual rows are extra.")
    print("-" * 70)

    per_act = defaultdict(lambda: {
        "pm_actual": 0.0, "pm_count": 0,
        "manual_planned": 0.0, "manual_actual": 0.0, "manual_count": 0,
    })
    for e in entries:
        eid = str(e.get("id", ""))
        # group by (channel_name, activity_name) so PM umbrella activities
        # are clearly separate from non-PM channels with the same activity
        key = (str(e.get("channel_name", "")), str(e.get("activity_name", "")))
        if eid.startswith("pm_"):
            per_act[key]["pm_actual"] += float(e.get("actual") or 0)
            per_act[key]["pm_count"] += 1
        else:
            per_act[key]["manual_planned"] += float(e.get("planned") or 0)
            per_act[key]["manual_actual"] += float(e.get("actual") or 0)
            per_act[key]["manual_count"] += 1

    rows = sorted(per_act.items(), key=lambda kv: -(kv[1]["pm_actual"] + kv[1]["manual_actual"]))
    print(f"\n  {'channel / activity':40}  {'pm_act':>12} {'man_pln':>10} {'man_act':>10}  pm# / man#")
    for (ch, act), v in rows:
        if v["pm_count"] == 0 and v["manual_count"] == 0:
            continue
        label = f"{ch[:24] or '(blank)'} / {act[:14] or '(blank)'}"
        print(f"  {label:40}  ${v['pm_actual']:>10,.0f}  ${v['manual_planned']:>8,.0f}  "
              f"${v['manual_actual']:>8,.0f}  {v['pm_count']:>3} / {v['manual_count']:>3}")

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(" SUMMARY")
    print("=" * 70)
    print(f"  Duplicate pm_ entry groups:        {len(dups)}")
    print(f"  Activities with mixed finance_cat: {len(inconsistent)}")
    print(f"  pm_ entries with stale mapping:    {len(stale_fc)}")
    print(f"  pm_ entries with missing ch_id:    {len(orphans_ch)}")
    print(f"  pm_ entries with missing act_id:   {len(orphans_act)}")
    print()


if __name__ == "__main__":
    main()
