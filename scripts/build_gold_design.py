"""Build local-only Gold Notebook from reviewed config and inspected Silver schema."""
import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/gold'
cfg = json.loads((ROOT / 'config/dev.json').read_text())
discovery = json.loads((ROOT / 'artifacts/silver/deployment/discovery.json').read_text())
profile = json.loads((OUT / 'silver-profile.json').read_text())
assert cfg['environment'] == 'dev'
assert cfg['workspace_id'] == discovery['workspace']['id'] == discovery['lakehouse']['workspaceId']
assert cfg['lakehouse_name'] == discovery['lakehouse']['displayName']
assert profile['schema'].get('city') == 'string'
assert [n for n in profile['schema'] if 'city' in n.lower().split('_')] == ['city']

def code(text, tags=None):
    ast.parse(text)
    return {'cell_type': 'code', 'execution_count': None, 'outputs': [],
            'metadata': {'tags': tags} if tags else {}, 'source': text.splitlines(True)}

nb = {'nbformat': 4, 'nbformat_minor': 5, 'metadata': {
    'kernel_info': {'name': 'synapse_pyspark'}, 'language_info': {'name': 'python'},
    'dependencies': {'lakehouse': {'default_lakehouse': discovery['lakehouse']['id'],
        'default_lakehouse_name': cfg['lakehouse_name'], 'default_lakehouse_workspace_id': cfg['workspace_id']}}},
    'cells': [{'cell_type': 'markdown', 'metadata': {}, 'source': [
        '# Gold bookings by city\n',
        'Local design only. Exclude null/Unicode-whitespace-only cities; preserve other labels exactly.\n',
        'City lineage: airports.city through the validated Silver projection.\n']},
        code('config_json = ' + repr(json.dumps(cfg)) + '\n', ['parameters']),
        code('CONFIG_SNAPSHOT = ' + repr(cfg) + '\nEXPECTED_LAKEHOUSE_ID = ' + repr(discovery['lakehouse']['id']) + '\n'),
        code((OUT / 'notebook-body.py').read_text())]}
for i, c in enumerate(nb['cells']):
    c['id'] = f'gold-cell-{i}'
(OUT / 'nb_gold_bookings_by_city.ipynb').write_text(json.dumps(nb, indent=2) + '\n')
print('Gold Notebook generated; syntax verified; no cells executed.')
