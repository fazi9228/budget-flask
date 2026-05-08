# -*- coding: utf-8 -*-
"""
HISTORICAL CORRECTION: re-classify stale pm_ entries.

For every pm_ entry whose activity_name is in PM_CHANNEL_MAP, force its
bu / finance_cat / marketing_cat to match the canonical mapping. Fixes
the "Douyin tagged as PPC" / "TA Media tagged as Programmatic" type drift.

Reads only on dry-run. Writes only with --commit.

Usage:
  python reclassify_pm_entries.py            # dry-run, prints what would change
  python reclassify_pm_entries.py --commit   # apply
"""
import argparse
import time
from datetime import datetime

from config import TAB_ENTRIES, PM_CHANNEL_MAP
from sheets_helper import get_sheet, safe_get_records, invalidate_cache

THROTTLE = 1.1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()
    dry_run = not args.commit

    print("\n" + "=" * 70)
    print(f" RECLASSIFY pm_ ENTRIES {'(DRY-RUN)' if dry_run else '(COMMIT)'}")
    print("=" * 70)

    ws = get_sheet(TAB_ENTRIES)
    entries = safe_get_records(ws, TAB_ENTRIES)

    targets = []  # list of (sheet_row, entry, changes_dict)
    for i, e in enumerate(entries):
        eid = str(e.get("id", ""))
        if not eid.startswith("pm_"):
            continue
        act = str(e.get("activity_name", ""))
        if act not in PM_CHANNEL_MAP:
            continue
        canonical = PM_CHANNEL_MAP[act]
        changes = {}
        for field in ("bu", "finance_cat", "marketing_cat"):
            cur = str(e.get(field, ""))
            want = canonical[field]
            if cur != want:
                changes[field] = (cur, want)
        if changes:
            targets.append((i + 2, e, changes))

    print(f"\nTotal entries scanned: {len(entries)}")
    print(f"pm_ entries needing re-classification: {len(targets)}")

    if not targets:
        print("\n  Nothing to fix.\n")
        return

    print("\nRows that will change:")
    for sheet_row, e, changes in targets[:30]:
        diffs = "; ".join(f"{f}: '{c}' -> '{w}'" for f, (c, w) in changes.items())
        print(f"  row {sheet_row:4d}  {e.get('country',''):3} {e.get('month',''):8}  "
              f"act={e.get('activity_name',''):16}  {diffs}")
    if len(targets) > 30:
        print(f"  ... and {len(targets) - 30} more")

    if dry_run:
        print(f"\nRun with --commit to update {len(targets)} rows.\n")
        return

    print(f"\nUpdating {len(targets)} rows...")
    now = datetime.utcnow().isoformat()
    updated = 0
    for sheet_row, e, changes in targets:
        canonical = PM_CHANNEL_MAP[str(e.get("activity_name", ""))]
        try:
            # Cols I (bu=col9), J (finance_cat=col10), K (marketing_cat=col11)
            # plus updated_at at col X (col 24)
            ws.update(f"I{sheet_row}:K{sheet_row}", [[
                canonical["bu"], canonical["finance_cat"], canonical["marketing_cat"]
            ]])
            ws.update(f"X{sheet_row}", [[now]])
            updated += 1
            if updated % 10 == 0:
                print(f"  ...{updated}/{len(targets)}")
            time.sleep(THROTTLE)
        except Exception as ex:
            print(f"  ERROR row {sheet_row}: {ex}")

    invalidate_cache(TAB_ENTRIES)
    print(f"\nUpdated {updated}/{len(targets)} rows.\n")


if __name__ == "__main__":
    main()
