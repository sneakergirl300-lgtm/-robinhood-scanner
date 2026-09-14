import copy
import json
import time
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch
import analyzer as a
TOKEN='0x'+'1'*40
NOW=int(time.time())
def candidate(address=TOKEN, block=1000):
    return dict(address=address,first_seen_block=block,last_seen_block=block,
                collected_at_unix=NOW,classification='NEW_LAUNCH',observations=[])
class FakeRPC:
    def __init__(self,limit=48,head=2000):
        self.calls=0; self.limit=limit; self.head=head; self.started=time.monotonic(); self.requests=[]
    def __call__(self,method,params):
        if self.calls>=self.limit: raise a.StopRun('RPC_OR_TIME_BUDGET')
        self.calls+=1;self.requests.append((method,copy.deepcopy(params)))
        if method=='eth_getBlockByNumber': return {'number':hex(self.head),'timestamp':hex(NOW)}
        if method=='eth_getLogs': return []
        if method=='eth_call': return '0x'+f'{18 if params[0]["data"]==a.SELECTORS["decimals"] else 0:064x}'
        if method=='eth_getTransactionReceipt': return {'logs':[]}
        raise AssertionError(method)
class AnalyzerTests(unittest.TestCase):
    def state(self,cs=None):
        s=a.fresh_state();a.ingest(s,cs or [candidate()],NOW);return s
    def test_cursor_failure(self):
        s={'cursor':1000,'gaps':[[1,10]]};old=copy.deepcopy(s)
        def fail(*args): raise a.Deferred('failure')
        with self.assertRaises(a.Deferred): a.sample_stream(fail,s,{},2000,1000)
        self.assertEqual(s,old)
    def test_tail_gap_repair_resume(self):
        s={};r=FakeRPC();x=a.sample_stream(r,s,{'address':TOKEN},2000,1000)
        self.assertEqual((x['from_block'],x['to_block']),(1501,2000))
        self.assertEqual(s,{'cursor':2000,'gaps':[[1000,1500]]})
        a.sample_stream(r,s,{},2100,1000,repair=True)
        self.assertEqual(s,{'cursor':2000,'gaps':[[1500,1500]]})
        a.sample_stream(r,s,{},2100,1000)
        self.assertEqual(r.requests[-1][1][0]['fromBlock'],hex(2001))
        count=r.calls;self.assertIsNone(a.sample_stream(r,s,{},2100,1000));self.assertEqual(r.calls,count)
    def test_targeted_finalized_logs(self):
        s=self.state();r=FakeRPC();out=a.run([candidate()],s,{}, {},r,now=NOW)
        self.assertEqual(r.requests[0],('eth_getBlockByNumber',['finalized',False]))
        for method,params in r.requests:
            if method=='eth_getLogs':
                f=params[0];self.assertEqual(f['address'],TOKEN)
                self.assertLessEqual(int(f['toBlock'],16)-int(f['fromBlock'],16)+1,500)
        self.assertEqual(out['alerts'],[])
    def test_queue_rotation_and_followup_reservation(self):
        s=self.state([candidate('0x'+f'{i:040x}',i+1000) for i in range(1,30)])
        first=a.select_hot(s,NOW)
        for t in first:t.update(attempts=1,last_attempt=NOW,next_due=NOW+900)
        second=a.select_hot(s,NOW)
        self.assertFalse({t['address'] for t in first}&{t['address'] for t in second})
        for t in first:t['next_due']=NOW
        self.assertEqual(sum(bool(t['attempts']) for t in a.select_hot(s,NOW)),4)
    def test_cold_reactivation(self):
        c=candidate();c['collected_at_unix']=NOW-86400;s=self.state([c])
        self.assertEqual(a.select_hot(s,NOW),[])
        c['last_seen_block']+=1;a.ingest(s,[c],NOW)
        self.assertEqual(len(a.select_hot(s,NOW)),1)
    def test_large_universe_bounded_selection(self):
        s=self.state([candidate('0x'+f'{i:040x}') for i in range(1,10026)])
        self.assertEqual(len(s['tokens']),10025);self.assertEqual(len(a.select_hot(s,NOW)),8)
    def test_generic_owner_control_reject(self):
        t=self.state()['tokens'][TOKEN];t['metadata']['owner_share']='0.25'
        self.assertEqual(a.structural_gate(t,{})['status'],'REJECT')
    def test_synth_report_hold(self):
        risks=json.loads(Path('risk_evidence.json').read_text());synth=next(iter(risks));s=self.state([candidate(synth)])
        out=a.run([candidate(synth)],s,{},risks,FakeRPC(),now=NOW)
        self.assertEqual(s['tokens'][synth]['status'],'REJECT');self.assertEqual(out['selected'],0)
        self.assertIn('not independently',risks[synth]['source'])
    def test_unresolved_gate_and_mc(self):
        s=self.state();row=a.report_token(s['tokens'][TOKEN],s,NOW,{})
        self.assertEqual(row['status'],'UNRESOLVED');self.assertEqual(row['market_cap_status'],'MC UNRESOLVED')
        self.assertIsNone(row['actionable_alert']);self.assertIsNone(row['holders'])
        self.assertEqual(row['discovered_at'],NOW)
        self.assertEqual(row['discovery_sources'],[])
    def test_executed_price_uses_amounts(self):
        pool={'currency0':TOKEN,'currency1':a.USDG};swap={'amount0_raw':10*10**18,'amount1_raw':-20*10**6,'sqrt_price_x96':1}
        self.assertEqual(a.Decimal(a.executed_quote(pool,swap,TOKEN,18,6)),2)
        swap['amount1_raw']*=-1;self.assertIsNone(a.executed_quote(pool,swap,TOKEN,18,6))
    def test_stale_and_conflicting_price(self):
        s=self.state();t=s['tokens'][TOKEN];t['metadata']['decimals']=18;s['quotes']['decimals']=6;t['latest_swaps']={}
        for p,amount in [('p',10**6),('q',2*10**6)]:
            t['pools'][p]={'currency0':TOKEN,'currency1':a.USDG}
            t['latest_swaps'][p]={'amount0_raw':10**18,'amount1_raw':-amount,'timestamp':NOW,'tx':'0x1','block':100}
        row=a.report_token(t,s,NOW,{})
        self.assertTrue(row['valuation_conflict']);self.assertIsNone(row['executed_price'])
        for swap in t['latest_swaps'].values():swap['timestamp']=NOW-301
        self.assertIsNone(a.report_token(t,s,NOW,{})['executed_price'])
    def test_429_no_retry(self):
        r=a.RPC();err=urllib.error.HTTPError(a.RPC_URL,429,'limit',{},None)
        with patch('urllib.request.urlopen',side_effect=err) as open_:
            for _ in range(2):
                with self.assertRaises(a.StopRun):r('eth_blockNumber',[])
            self.assertEqual(open_.call_count,1);self.assertEqual(r.calls,1)
    def test_budget_and_deadline(self):
        r=a.RPC(calls=1)
        with patch('urllib.request.urlopen',side_effect=OSError('offline')) as open_:
            with self.assertRaises(a.Deferred):r('eth_blockNumber',[])
            with self.assertRaises(a.StopRun):r('eth_blockNumber',[])
            self.assertEqual(open_.call_count,1)
        r=a.RPC();r.deadline=time.monotonic()-1
        with self.assertRaises(a.StopRun):r('eth_blockNumber',[])
        self.assertEqual(r.calls,0)
    def test_budget_partial_persistence(self):
        s=self.state();out=a.run([candidate()],s,{'coverage_status':'PARTIAL'}, {},FakeRPC(limit=1),now=NOW)
        self.assertEqual(out['stop_reason'],'RPC_OR_TIME_BUDGET');self.assertEqual(out['collector_coverage'],'PARTIAL')
        self.assertEqual(out['rpc_calls'],1);self.assertEqual(s['tokens'][TOKEN]['attempts'],1)
    def test_runner_identities_not_in_production(self):
        fixtures=json.loads(Path('tests/historical_cases.json').read_text())
        production=''.join(Path(p).read_text() for p in ['analyzer.py','collector.py','risk_evidence.json'])
        for address in fixtures['runners'].values():self.assertNotIn(address,production)
    def test_corrupt_state_not_reset(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'state.json';p.write_text('{')
            with self.assertRaises(json.JSONDecodeError):a.load(p,{})
if __name__=='__main__':unittest.main()
