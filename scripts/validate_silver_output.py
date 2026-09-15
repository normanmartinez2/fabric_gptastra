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

OUT = ROOT / 'artifacts/silver/deployment'
cfg = json.loads((ROOT / 'config/dev.json').read_text())
names = ['booking_id', 'passenger_id', 'flight_id', 'airport_id', 'amount', 'booking_date',
         'passenger_name', 'passenger_gender', 'passenger_nationality', 'airport_name', 'city', 'airport_country']
expected_schema = pa.schema([(n, pa.decimal128(18, 2) if n == 'amount' else pa.date32()
                             if n == 'booking_date' else pa.string()) for n in names])
files = sorted((OUT / 'parquet').glob('*.parquet'))
assert files, 'No Parquet output files'
tables = [pq.read_table(p) for p in files]
assert all(t.schema.equals(expected_schema, check_metadata=False) for t in tables), 'Silver schema mismatch'
actual = pa.concat_tables(tables)
data = {}
audit = {'input_counts': {}, 'bronze_unchanged': {}, 'silver_path': cfg['paths']['silver'],
         'schema': {f.name: str(f.type) for f in expected_schema}, 'parquet_files': [f.name for f in files]}
for name in ('bookings', 'passengers', 'airports'):
    path = OUT / 'bronze-after' / (name + '.csv')
    with path.open(encoding='utf-8-sig', newline='') as f:
        data[name] = list(csv.DictReader(f))
    audit['input_counts'][name] = len(data[name])
    audit['bronze_unchanged'][name] = path.read_bytes() == (OUT / 'bronze-before' / (name + '.csv')).read_bytes()
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
