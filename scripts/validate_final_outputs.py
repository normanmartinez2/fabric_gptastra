"""Independently validate downloaded Silver Parquet against downloaded live Bronze."""
import collections
import csv
import datetime
import decimal
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '.validation-deps'))
import pyarrow as pa
import pyarrow.parquet as pq

OUT = ROOT / 'artifacts/final-validation'
cfg = json.loads((ROOT / 'config/dev.json').read_text())
names = ['booking_id', 'passenger_id', 'flight_id', 'airport_id', 'amount', 'booking_date',
         'passenger_name', 'passenger_gender', 'passenger_nationality', 'airport_name', 'city', 'airport_country']
expected_schema = pa.schema([(n, pa.decimal128(18, 2) if n == 'amount' else pa.date32()
                             if n == 'booking_date' else pa.string()) for n in names])
files = sorted((OUT / 'silver').glob('*.parquet'))
assert files, 'No Parquet output files'
tables = [pq.read_table(p) for p in files]
assert all(t.schema.equals(expected_schema, check_metadata=False) for t in tables), 'Silver schema mismatch'
actual = pa.concat_tables(tables)
data = {}
audit = {'input_counts': {}, 'bronze_unchanged': {}, 'silver_path': cfg['paths']['silver'],
         'schema': {f.name: str(f.type) for f in expected_schema}, 'parquet_files': [f.name for f in files]}
for name in ('bookings', 'passengers', 'airports'):
    path = OUT / 'bronze' / (name + '.csv')
    with path.open(encoding='utf-8-sig', newline='') as f:
        data[name] = list(csv.DictReader(f))
    audit['input_counts'][name] = len(data[name])
    audit['bronze_unchanged'][name] = path.read_bytes() == (ROOT / 'artifacts/silver/deployment/bronze-before' / (name + '.csv')).read_bytes()
    audit.setdefault('bronze_sha256', {})[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    assert audit['bronze_unchanged'][name], name + ': Bronze changed'
passengers = {r['passenger_id']: r for r in data['passengers']}
airports = {r['airport_id']: r for r in data['airports']}
assert len(passengers) == len(data['passengers']) and len(airports) == len(data['airports'])
assert len({r['booking_id'] for r in data['bookings']}) == len(data['bookings'])
expected = []
for b in data['bookings']:
    p, a = passengers[b['passenger_id']], airports[b['airport_id']]
    expected.append((b['booking_id'], b['passenger_id'], b['flight_id'], b['airport_id'],
        decimal.Decimal(b['amount']), datetime.date.fromisoformat(b['booking_date']),
        p['name'], p['gender'], p['nationality'], a['airport_name'], a['city'], a['country']))
rows = [tuple(r[n] for n in names) for r in actual.to_pylist()]
assert collections.Counter(rows) == collections.Counter(expected), 'Joined Silver content mismatch'
assert actual.num_rows == len(data['bookings']) == 1000, 'Unexpected Silver count'
audit.update(silver_rows=actual.num_rows, schema_matches=True, full_join_content_matches=True, status='Passed')
(OUT / 'silver-validation.json').write_text(json.dumps(audit, indent=2) + '\n')
print(json.dumps(audit, indent=2))

# Counts above are regression expectations for this source snapshot, not business rules.
assert audit['input_counts'] == {'bookings': 1000, 'passengers': 200, 'airports': 50}
def parquet_rows(folder):
    parts = sorted(folder.glob('*.parquet'))
    assert parts
    return pa.concat_tables([pq.read_table(f) for f in parts])
prior_silver = parquet_rows(ROOT / 'artifacts/gold/deployment/silver-after')
assert collections.Counter(tuple(r[n] for n in names) for r in prior_silver.to_pylist()) == collections.Counter(rows)
gold = parquet_rows(OUT / 'gold')
assert gold.schema.equals(pa.schema([pa.field('city', pa.string()), pa.field('booking_count', pa.int64(), nullable=False)]), check_metadata=False)
eligible = [r for r in actual.to_pylist() if r['city'] is not None and r['city'].strip()]
expected_groups = collections.Counter(r['city'] for r in eligible)
gold_rows = gold.to_pylist()
assert len(gold_rows) == len({r['city'] for r in gold_rows})
assert {r['city']: r['booking_count'] for r in gold_rows} == dict(expected_groups)
prior_gold = parquet_rows(ROOT / 'artifacts/gold/deployment/gold')
assert collections.Counter(tuple(r.values()) for r in prior_gold.to_pylist()) == collections.Counter(tuple(r.values()) for r in gold_rows)
total = sum(r['booking_count'] for r in gold_rows)
assert gold.num_rows == 50 and total == len(eligible) == 1000
audit.update(gold_rows=gold.num_rows, gold_schema={'city':'STRING','booking_count':'BIGINT'},
    gold_path=cfg['paths']['gold'], booking_count_sum=total, eligible_silver_rows=len(eligible),
    excluded_silver_rows=actual.num_rows-len(eligible), deterministic_silver=True, deterministic_gold=True,
    duplicate_silver_booking_ids=actual.num_rows-len({r['booking_id'] for r in actual.to_pylist()}),
    duplicate_gold_cities=gold.num_rows-len(expected_groups))
(OUT / 'data-validation.json').write_text(json.dumps(audit, indent=2)+'\n')
print(json.dumps(audit, indent=2))

