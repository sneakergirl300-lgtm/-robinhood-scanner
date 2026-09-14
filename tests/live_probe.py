"""Read-only live compatibility probe: 8 requests / 30 seconds, never sends alerts."""
import signal
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import analyzer as a
rpc=a.RPC(seconds=30,calls=8)
s=a.fresh_state()
def deadline(signum,frame):raise a.StopRun('PROBE_DEADLINE')
signal.signal(signal.SIGALRM,deadline)
signal.alarm(30)
try:
    out=a.run(a.load('candidates.json',[]),s,a.load('state.json',{}),a.load('risk_evidence.json',{}),rpc)
finally:
    signal.alarm(0)
a.save('live_probe.json',out)
print({k:v for k,v in out.items() if k!='tokens'})
# Successful process execution does not imply resolved chain data. Artifact reports both.
assert out['rpc_calls']<=8
assert out['alerts']==[]
