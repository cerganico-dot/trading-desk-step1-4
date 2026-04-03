from fastapi.testclient import TestClient

from app import app, _sim_engine
from engine.models import InstrumentQuote
from engine.signal_filter import FilterConfig, SignalFilter


def test_health():
    client = TestClient(app)
    r = client.get('/health')
    assert r.status_code == 200
    assert r.json()['ok'] is True


def test_api_state_has_new_steps():
    client = TestClient(app)
    r = client.get('/api/state')
    assert r.status_code == 200
    data = r.json()
    assert 'paper_events' in data
    assert 'alerts_sent' in data
    assert 'opportunity_log' in data
    assert 'zscore_history' in data


def test_signal_filter_blocks_when_edge_below_cost():
    filt = SignalFilter(FilterConfig(z_entry_threshold=1.5, min_volume=0, max_spread_bps=1000, roundtrip_cost_bps=30))
    q = InstrumentQuote(symbol='A', bid=99.9, ask=100.1, last=100, volume=1000, ts=__import__('datetime').datetime.utcnow())
    signals = filt.build_signals([('A','B')], {'A/B':1.0}, {'A/B':1.6}, {'A':q, 'B':q},)
    assert signals[0].signal != 'NO TRADE'
    assert signals[0].eligible is False
    assert signals[0].reject_reason == 'Edge no supera costos'


def test_sim_has_histories():
    desk = _sim_engine.build_state()
    assert desk.zscore_history['AL30/GD30']
    assert desk.opportunity_log
