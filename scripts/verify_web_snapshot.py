"""Verify a generated Pages payload without calling any financial data source."""
import gzip,json,re,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from backend.transmission import COMPARISONS
from backend.web_export import PRIVATE_KEYS
root=Path(sys.argv[1] if len(sys.argv)>1 else 'web-dist');folder=root/'snapshot'
manifest=json.loads((folder/'manifest.json').read_text(encoding='utf8'));routes=manifest['routes'];loaded={}
def read(file):
 if file not in loaded:
  path=folder/file;assert path.parent.resolve()==folder.resolve()
  blob=path.read_bytes();raw=gzip.decompress(blob) if file.endswith('.gz') else blob
  loaded[file]=json.loads(raw)
 return loaded[file]
def walk(value):
 if isinstance(value,dict):
  assert not (set(k.lower() for k in value)&PRIVATE_KEYS)
  for part in value.values():walk(part)
 elif isinstance(value,list):
  for part in value:walk(part)
 elif isinstance(value,str):assert not re.search(r'(?<![A-Za-z])[A-Za-z]:[\\/]',value)
for file in set(routes.values())|{manifest['transmission']}:
 walk(read(file))
for period in (20,60,120,252):
 for pair in COMPARISONS:
  for window in (20,60,120):
   analysis=read(routes[f"/api/transmission?pair={pair['id']}&period={period}&window={window}"])
   assert analysis['pair_id']==pair['id'] and analysis['window']==(12 if pair['frequency']=='monthly' else window)
brief=read(routes['/api/briefs/latest'])['brief']
assert brief['model_version']=='daily-digest-2' and len(brief['topics'])==3 and brief['uses_ai'] is False
assert all(topic['evidence'] and topic['limitations'] and topic['next_check'] for topic in brief['topics'])
assert read(routes['/api/schedule'])['network_enabled'] is False
assert not any(route.startswith(('/api/events','/api/notifications','/api/health','/api/history')) for route in routes)
assert (root/'index.html').is_file() and (root/'.nojekyll').is_file()
assert (root/'licenses/inventory.json').is_file()
print(json.dumps({'routes':len(routes),'series':manifest['series_count'],'topics':len(brief['topics']),
 'rules':len(brief['rule_notifications']),'merged_signals':len(brief['notifications']),
 'bytes':sum(p.stat().st_size for p in root.rglob('*') if p.is_file()),'private_routes':False,'result':'passed'}))
