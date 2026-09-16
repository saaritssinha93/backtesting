"""V13-v10: unchanged V9 entries, bounded target/stop research and native replay."""
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v9_backtest as v9

DEFAULT_SOURCE = Path('C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v9/run_20260913')
DEFAULT_OUTPUT = DEFAULT_SOURCE.parent.parent / 'v13_corrected_v10/run_20260913'


@dataclass(frozen=True)
class V10Config:
    initial_stop_pct: float = 1.5
    first_target_pct: float = 1.075
    runner_target_pct: float = 2.0
    cost_bps: float = 5.0
    partial_pct: float = .1
    runner_stop: str = 'BREAKEVEN'

    def validate(self) -> None:
        if not np.isfinite(self.initial_stop_pct) or not 0 < self.initial_stop_pct <= 1.5:
            raise ValueError('Initial SL must be positive and no greater than 1.5%')
        if not all(np.isfinite(x) for x in (self.first_target_pct,self.runner_target_pct)):
            raise ValueError('Targets must be finite')
        if not 0 < self.first_target_pct <= self.runner_target_pct <= 2.0:
            raise ValueError('Targets must satisfy 0 < first target <= runner target <= 2%')
        if not np.isfinite(self.partial_pct) or not 0 < self.partial_pct <= 1:
            raise ValueError('Partial fraction must be in (0,1]')
        if self.partial_pct < 1 and self.first_target_pct == self.runner_target_pct:
            raise ValueError('Distinct targets required for a partial exit')
        if self.partial_pct == 1 and self.first_target_pct != self.runner_target_pct:
            raise ValueError('Single exit must use equal first and final targets')
        if self.runner_stop not in ('BREAKEVEN','INITIAL'):
            raise ValueError('Runner stop must be BREAKEVEN or INITIAL')
        if not np.isfinite(self.cost_bps) or self.cost_bps < 0:
            raise ValueError('Costs must be finite and nonnegative')

    def exit_spec(self):
        self.validate()
        return v9.v5.ExitSpec(self.initial_stop_pct,self.first_target_pct,self.partial_pct,self.runner_target_pct,self.runner_stop,None)


def simulate(orders: pd.DataFrame, paths: dict, cfg: V10Config) -> pd.DataFrame:
    """Native BE replay plus explicit original-stop and full-exit alternatives.

    The legacy simulator always moves to BE regardless of ExitSpec.runner_stop;
    INITIAL therefore requires a real alternate replay, not a metadata change.
    """
    trades = v9.v5.simulate_scaleout(orders,paths,cfg.exit_spec(),cost_bps=cfg.cost_bps,
                                    max_entry_delay_minutes=v9.v5.MAX_ENTRY_DELAY_MINUTES)
    if cfg.runner_stop == 'BREAKEVEN' and cfg.partial_pct < 1:
        return trades
    for row in trades.loc[trades.filled.eq(True)].itertuples(index=True):
        p = paths[int(row.sid)]
        start,entry = int(row.entry_path_index),float(row.entry_price)
        long = row.side == 'LONG'
        direction = 1 if long else -1
        stop = entry*(1-direction*cfg.initial_stop_pct/100)
        t1 = entry*(1+direction*cfg.first_target_pct/100)
        target = entry*(1+direction*cfg.runner_target_pct/100)
        end = len(p['close'])-1
        def hit(level,begin,favorable):
            values = p['high'] if long==favorable else p['low']
            crossed = values[begin:]>=level if long==favorable else values[begin:]<=level
            found=np.flatnonzero(crossed)
            return begin+int(found[0]) if len(found) else len(values)
        si,ti=hit(stop,start,False),hit(t1,start,True)
        ambiguous=si==ti and si<=end
        first_hit=runner_hit=stopped=gap=False
        gap_bps=0.
        if min(si,ti)>end:
            index,price,reason=end,float(p['close'][end]),'TIME_EXIT_1515_NO_T1'
            gross=direction*(price/entry-1)*100
        elif si<=ti:
            index=si
            price,gap,gap_bps=v9.v5._adverse_stop_fill(p,index,stop,long,activation_index=start)
            gross=direction*(price/entry-1)*100
            reason,stopped='FULL_STOP',True
        elif cfg.partial_pct==1:
            index,price,gross=ti,t1,cfg.first_target_pct
            reason,first_hit,runner_hit='FULL_TARGET',True,True
        else:
            first_hit=True
            runner_stop=entry if cfg.runner_stop=='BREAKEVEN' else stop
            si,ri=hit(runner_stop,ti,False),hit(target,ti,True)
            ambiguous |= si==ri and si<=end
            if min(si,ri)>end:
                index,price,reason=end,float(p['close'][end]),'T1_THEN_TIME_EXIT_1515'
                runner_gross=direction*(price/entry-1)*100
            elif si<=ri:
                index=si
                price,gap,gap_bps=v9.v5._adverse_stop_fill(p,index,runner_stop,long,
                    activation_index=ti if cfg.runner_stop=='BREAKEVEN' else start)
                runner_gross=direction*(price/entry-1)*100
                reason='T1_THEN_BREAKEVEN' if cfg.runner_stop=='BREAKEVEN' else 'T1_THEN_INITIAL_STOP'
                stopped=cfg.runner_stop=='INITIAL'
            else:
                index,price,runner_gross=ri,target,cfg.runner_target_pct
                reason,runner_hit='RUNNER_TARGET',True
            gross=cfg.partial_pct*cfg.first_target_pct+(1-cfg.partial_pct)*runner_gross
        timestamp=pd.Timestamp(int(p['timestamp_ns'][index]),tz='UTC').tz_convert(v9.v5.common.IST)
        mfe,mae=v9.v5._excursions(p,entry,start,index,long)
        fields=dict(exit_price=price,exit_ts=timestamp,exit_path_index=index,
            holding_minutes=(timestamp-row.entry_ts).total_seconds()/60,
            gross_return_pct=gross,net_return_pct=gross-cfg.cost_bps/100,exit_reason=reason,
            first_target_hit=first_hit,runner_target_hit=runner_hit,target_hit=first_hit,
            stop_hit=stopped,same_bar_ambiguous=ambiguous,exit_gap_through=gap,exit_gap_bps=gap_bps,
            mfe_pct=mfe,mae_pct=mae)
        for field,value in fields.items():
            trades.at[row.Index,field]=value
    return trades


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_source(source: Path = DEFAULT_SOURCE) -> dict:
    """Read and verify the completed V9 dataset without rebuilding it."""
    folder = source / 'dataset'
    manifest = json.loads((folder / 'dataset_manifest.json').read_text(encoding='utf-8'))
    for record in manifest['sources']:
        path = Path(record['path'])
        if path.is_file() != record['exists'] or (record['exists'] and sha(path) != record['sha256']):
            raise RuntimeError(f'Frozen V9 source drift: {path}')
    for name,checksum in manifest['output_sha256'].items():
        if sha(folder / name) != checksum:
            raise RuntimeError(f'Frozen V9 artifact drift: {name}')
    cfg = v9.V9Config(**json.loads((source / 'frozen_config.json').read_text(encoding='utf-8')))
    cfg.validate()
    signals = pd.read_parquet(folder / 'signals.parquet')
    orders = v9.select_orders(signals,cfg)
    paths = {}
    with np.load(folder / 'paths.npz',allow_pickle=False) as archive:
        needed = set(orders.sid.astype(int))
        for name in archive.files:
            sid,field = name.split('_',1)
            if int(sid) in needed:
                paths.setdefault(int(sid),{})[field] = archive[name]
    v9.validate_configuration()
    v9.validate_paths(orders,paths)
    return dict(orders=orders,paths=paths,days=[date.fromisoformat(d) for d in manifest['days']],
                v9_config=cfg,manifest=manifest,source=source)


def evaluate_orders(orders: pd.DataFrame, paths: dict, cfg: V10Config,
                    base: v9.V9Config | None = None, *, validate_paths: bool = True):
    """Every changed exit receives a fresh chronological V6 portfolio allocation."""
    base = base or v9.V9Config()
    base.validate()
    cfg.validate()
    if validate_paths:
        v9.validate_paths(orders,paths)
    trades = simulate(orders,paths,cfg)
    for column,default in [('filled',False),('entry_ts',pd.NaT),('exit_ts',pd.NaT),
                           ('gross_return_pct',np.nan),('net_return_pct',np.nan),('cost_pct',np.nan),
                           ('initial_stop_pct',cfg.initial_stop_pct)]:
        if column not in trades:
            trades[column] = default
    trades = v9.v5.apply_fixed_capital_model(trades,base.capital_per_entry_rupees,base.leverage_factor)
    trades['v10_initial_stop_pct'] = cfg.initial_stop_pct
    trades['v10_first_target_pct'] = cfg.first_target_pct
    trades['v10_runner_target_pct'] = cfg.runner_target_pct
    ledger,summary = v9.v6.apply_portfolio_constraints(trades,base.portfolio_config())
    summary.update(v10_config=asdict(cfg),v9_entry_config=asdict(base),
        evidence='PREVIOUSLY_SEEN_HISTORY_TARGET_STOP_RESEARCH',
        target_limit_pct=2.,stop_limit_pct=1.5,
        stop_limit_note='Configured stop distance; adverse gaps can produce a realized loss beyond that distance.')
    return trades,ledger,summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir',type=Path,default=DEFAULT_SOURCE)
    parser.add_argument('--output-dir',type=Path,default=DEFAULT_OUTPUT / 'cli_replay')
    parser.add_argument('--config-json',type=Path)
    args = parser.parse_args()
    balanced = DEFAULT_OUTPUT / 'balanced' / 'frozen_config.json'
    config_path = args.config_json or (balanced if balanced.is_file() else DEFAULT_OUTPUT / 'frozen_config.json')
    if not config_path.is_file():
        parser.error('Run fno_v13_v10_research.py first or provide --config-json; no untested default is silently selected.')
    cfg = V10Config(**json.loads(config_path.read_text(encoding='utf-8')))
    dataset = load_source(args.source_dir)
    selected,ledger,summary = evaluate_orders(dataset['orders'],dataset['paths'],cfg,dataset['v9_config'])
    args.output_dir.mkdir(parents=True,exist_ok=True)
    selected.to_csv(args.output_dir / 'selected_trades.csv',index=False)
    ledger.to_csv(args.output_dir / 'portfolio_trades.csv',index=False)
    (args.output_dir / 'summary.json').write_text(json.dumps(summary,indent=2,default=str),encoding='utf-8')
    print(json.dumps(summary,indent=2,default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
