import json
from pathlib import Path
from collections import Counter

file = Path('data/bot/telemetry/runs/20260423_164243_110332/analysis/symbol_analysis.jsonl')

stats = {
    'total_raw_hits': 0,
    'total_candidates': 0,
    'total_delivered': 0,
    'by_reason': Counter(),
    'symbols_with_hits': 0
}

with open(file) as f:
    for line in f:
        data = json.loads(line)
        funnel = data.get('funnel', {})
        raw_hits = funnel.get('raw_hits', 0)
        candidates = data.get('candidates', 0)
        delivered = data.get('delivered', 0)
        
        stats['total_raw_hits'] += raw_hits
        stats['total_candidates'] += candidates
        stats['total_delivered'] += delivered
        
        if raw_hits > 0:
            stats['symbols_with_hits'] += 1
            
        # Check why rejected
        if raw_hits > 0 and candidates == 0:
            # Parse rejections - count may be dict or int
            for stage, count in funnel.items():
                if 'rejects' in stage and isinstance(count, int) and count > 0:
                    stats['by_reason'][stage] += count

print(f"Symbols with raw hits: {stats['symbols_with_hits']}")
print(f"Total raw hits: {stats['total_raw_hits']}")
print(f"Total candidates: {stats['total_candidates']}")
print(f"Total delivered: {stats['total_delivered']}")
print(f"\nRejection reasons (when raw_hits > 0 but candidates = 0):")
for reason, count in stats['by_reason'].most_common():
    print(f"  {reason}: {count}")
