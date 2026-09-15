"""Read local Bronze downloads, profile them, and build notebook artifacts. No Fabric writes."""
import ast
import collections
import csv
import datetime
import decimal
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/silver'
cfg = json.loads((ROOT / 'config/dev.json').read_text())
discovery = json.loads((ROOT / 'artifacts/bronze/discovery.json').read_text())
assert cfg['environment'] == 'dev'
assert cfg['workspace_id'] == discovery['workspace']['id'] == discovery['lakehouse']['workspaceId']
assert cfg['lakehouse_name'] == discovery['lakehouse']['displayName']
data, report = {}, {}
for name in ('bookings', 'passengers', 'airports'):
    path = OUT / 'bronze' / (name + '.csv')
    with path.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, strict=True)
        rows = list(reader)
        headers = reader.fieldnames
    assert rows and len(headers) == len(set(headers))
    assert all(None not in r and all(v is not None for v in r.values()) for r in rows)
    data[name] = rows
    keys = [k for k in headers if k.endswith('_id')]
    report[name] = {
        'path': cfg['paths']['bronze_' + name], 'rows': len(rows), 'columns': headers,
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
        'null_or_blank': {k: sum(not r[k].strip() for r in rows) for k in headers},
        'key_whitespace': {k: sum(r[k] != r[k].strip() for r in rows) for k in keys},
        'duplicate_excess': {k: len(rows) - len(set(r[k] for r in rows)) for k in keys},
        'logical_schema': {k: 'decimal(18,2)' if k == 'amount' else 'date (yyyy-MM-dd)'
                           if k == 'booking_date' else 'string' for k in headers},
    }
b = data['bookings']
amounts = [decimal.Decimal(r['amount']) for r in b]
dates = [datetime.date.fromisoformat(r['booking_date']) for r in b]
report['types'] = {'amount_min': str(min(amounts)), 'amount_max': str(max(amounts)),
    'amount_max_scale': max(-x.as_tuple().exponent for x in amounts),
    'date_min': str(min(dates)), 'date_max': str(max(dates))}
report['joins'] = {}
for key, dim in [('passenger_id', 'passengers'), ('airport_id', 'airports')]:
    counts = collections.Counter(r[key] for r in data[dim])
    report['joins'][key] = {'unmatched_bookings': sum(r[key] not in counts for r in b),
        'dimension_max_multiplicity': max(counts.values()),
        'expected_join_rows': sum(counts[r[key]] for r in b)}
assert all(x['unmatched_bookings'] == 0 and x['dimension_max_multiplicity'] == 1
           for x in report['joins'].values())
report['expected_silver_rows'] = len(b)
report['silver_executed'] = False
(OUT / 'bronze-profile.json').write_text(json.dumps(report, indent=2) + '\n')

def cell(source, tags=None):
    ast.parse(source)
    return {'cell_type': 'code', 'execution_count': None, 'outputs': [],
            'metadata': {'tags': tags} if tags else {}, 'source': source.splitlines(True)}

snapshot = 'CONFIG_SNAPSHOT = ' + repr(cfg) + '\nEXPECTED_LAKEHOUSE_ID = ' + repr(discovery['lakehouse']['id']) + '\n'
params = '# Generated from config/dev.json. Regenerate when configuration changes.\nconfig_json = ' + repr(json.dumps(cfg)) + '\n'
body = (OUT / 'notebook-body.py').read_text()
nb = {'nbformat': 4, 'nbformat_minor': 5, 'metadata': {
    'kernel_info': {'name': 'synapse_pyspark'},
    'language_info': {'name': 'python'}},
    'cells': [{'cell_type': 'markdown', 'metadata': {}, 'source': [
        '# Silver bookings enrichment\n',
        'Local design only. Attach the existing target Lakehouse as default before future execution.\n',
        'Strict many-to-one joins; no automatic deduplication or orphan dropping. City comes from airports.\n']},
        cell(params, ['parameters']), cell(snapshot), cell(body)]}
for i, c in enumerate(nb['cells']):
    c['id'] = 'silver-cell-' + str(i)
(OUT / 'nb_silver_bookings_enriched.ipynb').write_text(json.dumps(nb, indent=2) + '\n')
print(json.dumps({'input_counts': {n: len(r) for n, r in data.items()},
    'expected_silver_rows': len(b), 'notebook_code_syntax': 'passed', 'executed_silver': False}, indent=2))
