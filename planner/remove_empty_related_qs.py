#!/usr/bin/env python3
"""Remove connections with empty related_qs from multi_query_plan.json files."""

import json
import glob
import os

PLAN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "mutated_multi")

total_files = 0
total_removed = 0

for filepath in sorted(glob.glob(os.path.join(PLAN_DIR, "*", "*", "multi_query_plan.json"))):
    with open(filepath, 'r') as f:
        data = json.load(f)

    original_count = len(data["connections"])
    data["connections"] = [c for c in data["connections"] if c.get("related_qs")]
    new_count = len(data["connections"])
    removed = original_count - new_count

    if removed > 0:
        with open(filepath, 'w') as f:
            json.dump(data, f, separators=(', ', ': '))
            f.write('\n')
        total_removed += removed
        total_files += 1
        print(f"  {os.path.relpath(filepath)}: removed {removed} empty-related_qs connections")

print(f"\nDone: {total_files} files modified, {total_removed} connections removed")
