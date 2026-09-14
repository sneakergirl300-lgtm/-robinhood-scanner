"""Two deterministic runs with state round trip; no live-chain success implied."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import analyzer as a
import test_analyzer as fixtures
from test_analyzer import candidate, FakeRPC, NOW, TOKEN
s=a.fresh_state()
first=a.run([candidate()],s,{'coverage_status':'PARTIAL'},{},FakeRPC(),now=NOW)
a.save('verification_state.json',s)
s=a.load('verification_state.json',{})
fixtures.NOW=NOW+900
second=a.run([candidate()],s,{'coverage_status':'PARTIAL'},{},FakeRPC(head=11000),now=NOW+900)
assert s['tokens'][TOKEN]['attempts']==2
assert s['tokens'][TOKEN]['streams']['transfers']['cursor']==11000
assert s['tokens'][TOKEN]['streams']['transfers']['gaps']
assert first['alerts']==second['alerts']==[]
a.save('verification.json',{'kind':'OFFLINE_WORKFLOW_INTEGRATION','first':first,'second':second,
                           'historical_runner_replay':'UNRESOLVED: historical data unavailable'})
print('Two-run checkpoint and gap integration passed')
