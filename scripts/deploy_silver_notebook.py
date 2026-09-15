"""Scoped Notebook REST fallback. Pipeline operations stay in Data Factory MCP."""
import argparse
import ast
import base64
import json
import subprocess
import time
import urllib.request
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/silver/deployment'
OUT.mkdir(exist_ok=True)
cfg = json.loads((ROOT / 'config/dev.json').read_text())
known = json.loads((ROOT / 'artifacts/bronze/discovery.json').read_text())
assert cfg['environment'] == 'dev'
assert cfg['workspace_id'] == known['workspace']['id'] == '36ef6570-f84b-4d04-8131-7f403f4a3f04'
assert cfg['lakehouse_name'] == known['lakehouse']['displayName']
workspace = cfg['workspace_id']
name = 'nb_silver_bookings_enriched'
token = json.loads(subprocess.run(['az.cmd', 'account', 'get-access-token', '--resource',
    'https://api.fabric.microsoft.com', '--output', 'json'], capture_output=True, text=True, check=True).stdout)['accessToken']

def request(method, path, body=None):
    url = path if path.startswith('https://') else 'https://api.fabric.microsoft.com/v1/' + path
    assert url.lower().startswith('https://api.fabric.microsoft.com/v1/'), 'Unexpected operation URL: ' + url
    req = urllib.request.Request(url, method=method, data=None if body is None else json.dumps(body).encode(),
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as response:
        raw = response.read()
        result = json.loads(raw) if raw else {}
        if response.status != 202:
            return result
        operation_id = response.headers.get('x-ms-operation-id')
        if not operation_id:
            operation_id = urllib.parse.urlparse(response.headers['Location']).path.rsplit('/', 1)[-1]
        import uuid
        operation = 'https://api.fabric.microsoft.com/v1/operations/' + str(uuid.UUID(operation_id))
    for _ in range(60):
        time.sleep(10)
        status = request('GET', operation)
        print(json.dumps({'operation_status': status.get('status')}), flush=True)
        if status.get('status') == 'Succeeded':
            return request('GET', operation.rstrip('/') + '/result')
        if status.get('status') in ('Failed', 'Cancelled'):
            raise RuntimeError(status)
    raise TimeoutError(operation)

def save(filename, obj):
    (OUT / filename).write_text(json.dumps(obj, indent=2) + '\n', encoding='utf-8')

def list_notebooks():
    page = request('GET', f'workspaces/{workspace}/notebooks')
    items = page.get('value', [])
    while page.get('continuationUri'):
        page = request('GET', page['continuationUri'])
        items.extend(page.get('value', []))
    return items

def definition(item_id):
    result = request('POST', f'workspaces/{workspace}/notebooks/{item_id}/getDefinition?format=ipynb')
    part = next(p for p in result['definition']['parts'] if p['path'].endswith('.ipynb'))
    return json.loads(base64.b64decode(part['payload']))

mode = argparse.ArgumentParser()
mode.add_argument('action', choices=['discover', 'deploy', 'readback', 'sessions'])
action = mode.parse_args().action
ws = request('GET', f'workspaces/{workspace}')
assert ws['id'] == workspace and ws['displayName'] == cfg['workspace_name']
lakehouse = request('GET', f'workspaces/{workspace}/lakehouses/{known["lakehouse"]["id"]}')
assert lakehouse['displayName'] == cfg['lakehouse_name']
notebooks = list_notebooks()
matches = [n for n in notebooks if n['displayName'] == name]
assert len(matches) <= 1
save('discovery.json', {'workspace': ws, 'lakehouse': lakehouse, 'notebooks': notebooks})
if action == 'discover':
    print(json.dumps({'workspace': ws['displayName'], 'lakehouse': lakehouse['displayName'], 'matching_notebooks': matches}))
elif action == 'sessions':
    assert len(matches) == 1
    sessions = request('GET', f'workspaces/{workspace}/notebooks/{matches[0]["id"]}/livySessions')
    save('notebook.sessions.json', sessions)
    print(json.dumps(sessions))
elif action == 'deploy':
    nb = json.loads((ROOT / 'artifacts/silver/nb_silver_bookings_enriched.ipynb').read_text())
    for c in nb['cells']:
        if c['cell_type'] == 'code':
            ast.parse(''.join(c['source']))
    # Only deployment metadata is added; approved cell source remains unchanged.
    nb['metadata']['dependencies'] = {'lakehouse': {
        'default_lakehouse': lakehouse['id'], 'default_lakehouse_name': cfg['lakehouse_name'],
        'default_lakehouse_workspace_id': workspace}}
    save('notebook.intended.ipynb', nb)
    payload = {'definition': {'format': 'ipynb', 'parts': [{'path': 'notebook-content.ipynb',
        'payloadType': 'InlineBase64', 'payload': base64.b64encode(json.dumps(nb).encode()).decode()}]}}
    if matches:
        item_id = matches[0]['id']
        save('notebook.before.ipynb', definition(item_id))
        result = request('POST', f'workspaces/{workspace}/notebooks/{item_id}/updateDefinition', payload)
    else:
        result = request('POST', f'workspaces/{workspace}/notebooks', {'displayName': name, **payload})
    matches = [n for n in list_notebooks() if n['displayName'] == name]
    assert len(matches) == 1
    save('notebook.item.json', matches[0])
    print(json.dumps(matches[0]))
if action in ('deploy', 'readback'):
    assert len(matches) == 1
    nb = definition(matches[0]['id'])
    save('notebook.deployed.ipynb', nb)
    original = json.loads((ROOT / 'artifacts/silver/nb_silver_bookings_enriched.ipynb').read_text())
    code = lambda n: [''.join(c['source']).strip() for c in n['cells'] if c['cell_type'] == 'code']
    assert code(nb) == code(original), 'Deployed code differs from approved artifact'
    binding = nb['metadata']['dependencies']['lakehouse']
    assert binding['default_lakehouse'] == lakehouse['id']
    assert binding['default_lakehouse_workspace_id'] == workspace
    save('notebook-verification.json', {'code_matches_approved': True, 'lakehouse_binding': binding})
    print('Verified deployed code and default Lakehouse binding.')
