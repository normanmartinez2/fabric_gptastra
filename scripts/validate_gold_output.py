"""Independent validation of downloaded deployed Gold and Silver Parquet."""
import json
import sys
from collections import Counter
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '.validation-deps'))
import pyarrow as pa
import pyarrow.parquet as pq
OUT = ROOT / 'artifacts/gold/deployment'
def read(folder):
    files = sorted(folder.glob('*.parquet'))
    assert files, str(folder)
    return pa.concat_tables([pq.read_table(f) for f in files])
silver = read(OUT / 'silver-after')
prior = read(ROOT / 'artifacts/gold/silver')
gold = read(OUT / 'gold')
assert gold.column_names == ['city', 'booking_count']
assert gold.schema.field('city').type == pa.string()
assert gold.schema.field('booking_count').type == pa.int64()
assert silver.schema.equals(prior.schema, check_metadata=False)
assert Counter(tuple(r.values()) for r in silver.to_pylist()) == Counter(tuple(r.values()) for r in prior.to_pylist()), 'Silver content changed'
rows = silver.to_pylist()
eligible = [r for r in rows if r['city'] is not None and r['city'].strip()]
expected = Counter(r['city'] for r in eligible)
actual = gold.to_pylist()
assert len(actual) == len({r['city'] for r in actual})
assert all(r['city'] is not None and r['city'].strip() and r['booking_count'] > 0 for r in actual)
assert dict(expected) == {r['city']: r['booking_count'] for r in actual}
total = sum(r['booking_count'] for r in actual)
assert len(rows) == len(eligible) == total == 1000
assert len(actual) == 50
report = dict(status='Succeeded', silver_rows=len(rows), eligible_rows=len(eligible),
    excluded_rows=len(rows)-len(eligible), gold_rows=len(actual), booking_count_sum=total,
    schema=[dict(name=f.name, type=str(f.type)) for f in gold.schema],
    silver_content_unchanged=True, all_city_aggregates_match=True)
(OUT / 'gold-validation.json').write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps(report, indent=2))
