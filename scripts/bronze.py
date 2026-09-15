"""Generate Bronze definitions and validate downloaded CSVs; Fabric execution uses MCP."""
import argparse
import csv
import hashlib
import io
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/bronze'

def inspect(path):
    data = path.read_bytes()
    reader = csv.DictReader(io.StringIO(data.decode('utf-8-sig'), newline=''))
    rows = list(reader)
    assert rows and None not in reader.fieldnames
    assert all(None not in r and all(v is not None for v in r.values()) for r in rows)
    return {'rows': len(rows), 'headers': reader.fieldnames, 'bytes': len(data),
            'sha256': hashlib.sha256(data).hexdigest(),
            'schema': {k: 'integer' if all(r[k].isdigit() for r in rows) else 'string (CSV)' for k in reader.fieldnames}}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--validate-target', action='store_true')
    args = parser.parse_args()
    cfg = json.loads((ROOT / 'config/dev.json').read_text())
    state = json.loads((OUT / 'discovery.json').read_text())
    ws, lh, conn = state['workspace'], state['lakehouse'], state['connection']
    assert cfg['environment'] == 'dev'
    assert ws['id'] == cfg['workspace_id'] and ws['displayName'] == cfg['workspace_name']
    assert ws['type'] == 'Personal' and lh['workspaceId'] == ws['id']
    assert lh['displayName'] == cfg['lakehouse_name']
    assert conn['connectionDetails']['type'] == 'HttpServer'
    assert conn['credentialDetails']['credentialType'] == 'Anonymous'
    report = {}
    definition = json.loads((OUT / 'pipeline.before.json').read_text())
    activities = definition['properties'].setdefault('activities', [])
    for name, url in cfg['sources'].items():
        source = inspect(OUT / 'source' / (name + '.csv'))
        keys = {'airports': ['airport_id'], 'passengers': ['passenger_id'], 'bookings': ['passenger_id', 'airport_id']}[name]
        assert set(keys) <= set(source['headers'])
        report[name] = {'source': source, 'join_keys_present': True}
        base = conn['connectionDetails']['path'].rstrip('/') + '/'
        assert url.startswith(base)
        path = cfg['paths']['bronze_' + name]
        assert path.startswith(cfg['paths']['bronze'].rstrip('/') + '/')
        folder, filename = path.removeprefix('Files/').rsplit('/', 1)
        activity = {'name': 'Copy_' + name + '_to_bronze', 'type': 'Copy', 'dependsOn': [],
            'policy': {'timeout': '0.01:00:00', 'retry': 2, 'retryIntervalInSeconds': 30},
            'typeProperties': {'source': {'type': 'BinarySource',
                'storeSettings': {'type': 'HttpReadSettings', 'requestMethod': 'GET'},
                'datasetSettings': {'type': 'Binary', 'typeProperties': {'location': {
                    'type': 'HttpServerLocation', 'relativeUrl': url[len(base):]}},
                    'externalReferences': {'connection': conn['id']}}},
                'sink': {'type': 'BinarySink', 'storeSettings': {'type': 'LakehouseWriteSettings'},
                    'datasetSettings': {'type': 'Binary', 'typeProperties': {'location': {
                        'type': 'LakehouseLocation', 'folderPath': folder, 'fileName': filename}},
                        'linkedService': {'name': lh['displayName'], 'properties': {'type': 'Lakehouse',
                            'typeProperties': {'workspaceId': ws['id'], 'artifactId': lh['id'], 'rootFolder': 'Files'}}}}},
                'enableStaging': False}}
        positions = [i for i, a in enumerate(activities) if a['name'] == activity['name']]
        assert len(positions) <= 1
        if positions:
            activities[positions[0]] = activity
        else:
            activities.append(activity)
        if args.validate_target:
            target = inspect(OUT / 'target' / filename)
            report[name]['target'] = target
            report[name]['byte_identical'] = source['sha256'] == target['sha256']
            assert source == target, f'{name}: source/target mismatch'
    (OUT / 'pipeline.intended.json').write_text(json.dumps(definition, indent=2) + '\n')
    (OUT / ('validation.json' if args.validate_target else 'source-validation.json')).write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))

if __name__ == '__main__':
    main()
