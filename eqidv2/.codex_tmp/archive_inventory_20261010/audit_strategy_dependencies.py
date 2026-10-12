"""Read-only static audit; never imports trading modules or reads credentials."""
import ast
import json
import re
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
seeds = set(ROOT.glob('fno_v13_v10_g*.py'))
seeds.update(ROOT.glob('*v13_v10_g*.py'))
seeds.update((ROOT / 'research').glob('g2*.py'))
seeds.update((ROOT / 'research').glob('g3*.py'))
seeds.update((ROOT / 'tools').glob('*v13_v10_g*.py'))
seeds.update((ROOT / 'tools').glob('v13_*shadow*.py'))
seeds.update((ROOT / 'tools').glob('v13_*research.py'))
seeds.update((ROOT / 'bat').glob('*v13_v10_g*'))
seeds.update((ROOT / 'bat').glob('*v13_shadow*'))
seeds.update((ROOT / 'bat').glob('*v13_strategy_research*'))
seeds.add(ROOT / 'bat/run_backtesting_result_v11_1600.bat')
# fno_live_profile.py:20 selects this compatibility branch dynamically.
seeds.add(ROOT / 'fno_v6_live_config.py')
live_review_seeds = [
    'eqidv2_live_combined_analyser_csv_v16_5min.py',
    'eqidv2_signal_engine_v16_5min.py',
    'eqidv2_live_combined_analyser_csv_id_5min_v7_persistent.py',
    'eqidv2_signal_discovery_v7_5min_id_persistent.py',
    'eqidv2_entry_engine_1min_v5_id.py',
    'avwap_trade_execution_PAPER_TRADE_FALSE_v15.py',
    'avwap_trade_execution_PAPER_TRADE_TRUE_v15.py',
    'avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py',
    'avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.py',
    'avwap_live_signal_generator.py',
    'eqidv2_live_combined_analyser_csv_v7_sweep_long.py',
    'eqidv2_live_combined_analyser_csv_v7_sweep_short.py',
    'final_setup_conf_v11_working.py',
    'filtered_fno_MIS_v2.py',
    'filtered_stocks_MIS_v2.py',
    'trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal.py',
]
seeds.update(ROOT / p for p in live_review_seeds)

def rel(p): return p.relative_to(ROOT).as_posix()
edges = []
errors = []
refs = []
protected = set()
queue = deque(sorted(seeds))

def local_modules(name):
    if not name: return []
    stem = ROOT.joinpath(*name.split('.'))
    result = []
    for p in (stem.with_suffix('.py'), stem / '__init__.py'):
        if p.is_file(): result.append(p)
    # Importing a package submodule also executes package __init__.
    parent = stem.parent
    while parent != ROOT and ROOT in parent.parents:
        if (parent / '__init__.py').is_file(): result.append(parent / '__init__.py')
        parent = parent.parent
    return result

def add_edge(source, target, line, kind):
    if not target.is_file(): return
    try: target.relative_to(ROOT)
    except ValueError: return
    edge = {'source': rel(source), 'target': rel(target), 'line': line, 'kind': kind}
    if edge not in edges: edges.append(edge)
    if target not in protected: queue.append(target)

def string_reference(source, value, line):
    if not value or len(value) > 500 or '\n' in value: return
    # Keep path-only evidence, never arbitrary text or secret values.
    if (re.match(r'^[A-Za-z]:[\\/]', value)
        or re.match(r'^(?:strategy_research|outputs|configs|docs|research|reports|logs|stocks|bat|tools|data|fno_oi)[\\/]', value)):
        refs.append({'source': rel(source), 'line': line, 'path_literal': value})
    if re.fullmatch(r'[\w./\\-]+\.(?:py|bat|ps1|json|csv|parquet|html|css|js|toml|yaml|yml|txt)', value, re.I):
        normalized = value.replace('\\', '/')
        for base in (ROOT, source.parent):
            candidate = base / normalized
            if candidate.is_file():
                if candidate.suffix.lower() in ('.py', '.bat', '.ps1'):
                    add_edge(source, candidate, line, 'literal source reference')
                else:
                    edges.append({'source': rel(source), 'target': rel(candidate), 'line': line, 'kind': 'literal artifact reference'})

while queue:
    source = queue.popleft().resolve()
    if source in protected: continue
    protected.add(source)
    # Do not open any secret-bearing token/credential artifact.
    if source.suffix.lower() not in ('.py', '.bat', '.ps1'): continue
    text = source.read_text(encoding='utf-8-sig', errors='replace')
    if source.suffix == '.py':
        try: tree = ast.parse(text)
        except SyntaxError as exc:
            errors.append({'source': rel(source), 'line': exc.lineno, 'error': str(exc)})
            continue
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import): names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                prefix = ''
                if node.level:
                    parts = source.relative_to(ROOT).parent.parts
                    prefix = '.'.join(parts[:len(parts)-node.level+1])
                module = '.'.join(x for x in (prefix, node.module) if x)
                names = [module] + [module + '.' + a.name for a in node.names if a.name != '*']
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'import_module' and node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                names = [node.args[0].value]
            for name in names:
                for target in local_modules(name): add_edge(source, target, node.lineno, 'static import')
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                string_reference(source, node.value, node.lineno)
    else:
        for n, line in enumerate(text.splitlines(), 1):
            for match in re.finditer(r'([\w.-]+\.(?:py|bat|ps1))\b', line, re.I):
                for base in (ROOT, ROOT / 'bat', ROOT / 'tools', source.parent):
                    add_edge(source, base / match.group(1), n, 'launcher literal reference')

artifact_refs = sorted({e['target'] for e in edges if e['kind'] == 'literal artifact reference'})
result = {
    'scope': 'Conservative static dependency closure of all root G/G2/G3 sources, their research and backtest/live/paper launchers. Does not prove unused files safe to archive.',
    'method': 'Python AST imports, constant dynamic imports, literal source/artifact references, launcher filenames; no strategy module imported or executed and no secret artifact contents read.',
    'entrypoints_and_family_seeds': sorted(rel(p) for p in seeds if p.is_file()),
    'protected_relative_files': sorted({rel(p) for p in protected} | set(artifact_refs)),
    'protected_relative_dirs': ['docs/v13_v10_g_web', 'logs'],
    'dependency_edges': sorted(edges, key=lambda e: (e['source'], e['line'], e['target'])),
    'shared_data_path_literals': sorted(refs, key=lambda e: (e['source'], e['line'])),
    'parse_errors': errors,
    'uncertainty': ['Dynamic environment-controlled imports and externally configured data paths require conservative protection.', 'Family seeds include optional research and audit sources intentionally because user protects files related to G/G2/G3.', 'Runtime external data is not inside the workspace archive inventory.']
}
result['critical_findings'] = [
    {'finding': 'Canonical G live and paper entrypoint runs old-named V5 engine; real-order coordinator runs old-named V6 engine.', 'evidence': ['fno_v13_v10_g_live.py:9', 'fno_v13_v10_g_live_kite_session.py:15', 'fno_v5_live.py:41', 'fno_v6_live_kite_session.py:571']},
    {'finding': 'G backtest inherits F -> E -> D/C/B/A -> V10 -> V9 -> corrected V5/V3/V2 and V6 portfolio/OI backtest utilities. These are functional source dependencies, despite old version names.', 'evidence': ['fno_v13_v10_g_backtest.py:21', 'fno_v13_v10_f_backtest.py:13', 'fno_v13_v10_b_backtest.py:11'], 'see': 'dependency_edges'},
    {'finding': 'Active G requires hash-verified frozen configuration and selected ledger from external strategy_research/v13_corrected_v10_g/run_20260914_opportunity_expansion; import executes configuration loading.', 'evidence': ['fno_v13_v10_g_live_config.py:39', 'fno_v13_v10_g_live_config.py:42', 'fno_v13_v10_g_live_config.py:90']},
    {'finding': 'G2 requires original G immutable full-history bundle, G config, and optionally daily G replays. G3 default verifies frozen archived evidence rather than recomputing new dates.', 'evidence': ['fno_v13_v10_g_2_backtest.py:35', 'fno_v13_v10_g_2_extend_results.py:44', 'fno_v13_v10_g_3_backtest.py:12', 'research/g3_freeze.py:22']},
    {'finding': 'Old V6 namespace still owns G live arm and kill switch; preserving just v13_v10_g_live would omit safety/control state.', 'evidence': ['fno_v13_v10_g_live_config.py:35', 'fno_v5_live.py:128', 'fno_v6_live_kite_session.py:51']},
    {'finding': 'Repository token cache and market calendar are required shared data. All api_key*.txt/access_token*.txt artifacts must remain protected; only their names were checked.', 'evidence': ['fno_oi_hybrid_data.py:39', 'fno_oi_common.py:358', 'fno_oi_common.py:359', 'fno_oi_common.py:861']},
    {'finding': 'Daily backtest launcher also runs prospective-shadow finalizer and research-report refresh; old-named V11 launcher forwards to G.', 'evidence': ['bat/run_backtesting_result_v13_v10_g_1620.bat:16', 'bat/run_backtesting_result_v13_v10_g_1620.bat:17', 'bat/run_backtesting_result_v11_1600.bat:3']}
]
result['manually_resolved_external_protected_paths'] = [
    {'path': 'C:/TradingData/eqidv2/fno_oi', 'coverage': 'Shared acquisition data, instrument masters/universe, contract registry, raw futures/equity/options, ready markers, features/rankings, strategy history and live/paper order state.', 'evidence': ['fno_oi_common.py:27', 'fno_options_atm_fetch_5min.py:53']},
    {'path': 'C:/TradingData/eqidv2/fno_oi/v6_live', 'coverage': 'G live control namespace: live_arm.json and kill_switch.json', 'evidence': ['fno_v13_v10_g_live_config.py:35', 'fno_v5_live.py:128']},
    {'path': 'C:/TradingData/eqidv2/fno_oi/v13_v10_g_live', 'coverage': 'G scanner, confirmation, signals, PAPER and LIVE orders, consolidation, evidence and broker export state.', 'evidence': ['fno_v5_live.py:120', 'fno_v13_v10_g_live_config.py:31']},
    {'path': 'C:/TradingData/eqidv2/fno_oi/v13_v10_g_options_paper', 'coverage': 'G options paper orders, consolidation and bar audit.', 'evidence': ['fno_v13_v10_g_options_paper.py:56']},
    {'path': 'C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1', 'coverage': 'Default G3 archived source/data/result bundle.', 'evidence': ['research/g3_freeze.py:22', 'research/g3_freeze.py:23']},
    {'path': 'C:/TradingData/eqidv2/backtesting_result_v13_v10_g', 'coverage': 'G dated immutable backtest runs/results consumed by G2/G3 and dashboards.', 'evidence': ['fno_v13_v10_g_2_extend_results.py:44']},
    {'path': 'C:/TradingData/eqidv2/backtesting_result_v11/latest', 'coverage': 'Data-readiness verification compatibility location still used by G prerequisite helpers.', 'evidence': ['data_for_backtesting_verify.py:31', 'wait_for_data_backtesting_ready.py:25']},
    {'path': 'C:/TradingData/eqidv2/stocks_indicators_5min_eq_live', 'coverage': 'Active G launcher overrides default 5m path to this directory.', 'evidence': ['bat/run_fno_v13_v10_g_scanner_5min.bat:10']},
    {'path': 'C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2', 'coverage': 'Default/historical 5m price input.', 'evidence': ['eqidv2_runtime_paths.py:9', 'fno_oi_hybrid_data.py:25']},
    {'path': 'C:/TradingData/eqidv2/stocks_indicators_1min_eq', 'coverage': 'Default 1m equity history and backtest input.', 'evidence': ['eqidv2_runtime_paths.py:15', 'fno_oi_hybrid_data.py:31']},
    {'path': 'C:/TradingData/eqidv2/observability', 'coverage': 'Logs, metrics, journals, reconciliation, raw observations and shadow evidence.', 'evidence': ['fno_v5_live.py:210', 'fno_oi_common.py:245']},
    {'path': 'C:/TradingData/eqidv2/v13_v10_g_strategy_research', 'coverage': 'Prospective shadow and strategy research outputs.', 'evidence': ['tools/v13_strategy_research.py:25', 'tools/v13_prospective_shadow.py:25']},
    {'path': 'C:/TradingData/eqidv2/v13_v10_g_execution_research', 'coverage': 'Path-aware execution research outputs.', 'evidence': ['tools/v13_execution_research.py:26']}
]
result['protected_relative_files'] = sorted(set(result['protected_relative_files']) | {rel(p) for glob in ('api_key*.txt', 'access_token*.txt') for p in ROOT.glob(glob)})
result['instructions_audit'] = 'No AGENTS.md found in workspace traversal or ancestor chain.'
result['newly_discovered_live_dependency_seeds'] = live_review_seeds
result['critical_findings'].extend([
    {'finding': 'V16 live analyser imports V17C no-NF live -> V17B -> V17p/V17n and their predecessor cascade. V16 signal engine imports V17f. The complete static import chain is now protected; V17 filenames cannot be discarded as a family.', 'evidence': ['eqidv2_live_combined_analyser_csv_v16_5min.py:62', 'avwap_combined_runner_v17C_noNF_live_5min.py:104', 'avwap_combined_runner_v17B_live_5min.py:79', 'eqidv2_signal_engine_v16_5min.py:89']},
    {'finding': 'Old V15/V16 backtest runners also provide core live and PAPER executor logic.', 'evidence': ['avwap_trade_execution_PAPER_TRADE_FALSE_v15.py:55', 'avwap_trade_execution_PAPER_TRADE_TRUE_v15.py:53', 'avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py:69', 'avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.py:62']},
    {'finding': 'ID V7 live scan and signal discovery depend on old ID V2/V6/V7 backtest modules; V6 imports v17D_exit_resolver. All are now protected with their source dependency closure.', 'evidence': ['avwap_5min_ID_v7_live_scan.py:41', 'avwap_5min_ID_v7_live_scan.py:42', 'avwap_5min_ID_v7_candidate_scan.py:22', 'avwap_5min_ID_v7_candidate_scan.py:24', 'avwap_5min_ID_v6_backtesting.py:28']},
    {'finding': 'The generic setup loader dynamically loads configured modules or .py paths; live/paper wrappers configure final_setup_conf_v11_working, which also imports final_setup_conf.', 'evidence': ['eqidv2_setup_conf_loader.py:54', 'eqidv2_setup_conf_loader.py:60', 'bat/run_conf_live_executor.bat:8', 'bat/run_conf_paper_executor.bat:10', 'final_setup_conf_v11_working.py:1434']},
    {'finding': 'Live-minimal data feed dynamically chooses filtered_fno_MIS_v2 or filtered_stocks_MIS_v2; both sources and imports are now protected.', 'evidence': ['trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal.py:351', 'trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_live_minimal.py:353']},
    {'finding': 'ID V7 signal-discovery live gate reads an accepted-rules CSV from an old-named outputs_ID_v8_5min_research_restore directory. Output folders can be input dependencies.', 'evidence': ['eqidv2_signal_discovery_v7_5min_id_persistent.py:196', 'eqidv2_signal_discovery_v7_5min_id_persistent.py:903', 'bat/run_eqidv2_signal_discovery_v7_5min_id_persistent.bat:57']},
    {'finding': 'G2 unused-slot research discovers all root fno*.py files for provenance hashes. Options backtest discovers all G options Python files for code hashes. These are provenance scans, not global strategy execution/discovery.', 'evidence': ['fno_v13_v10_g_2_unused_slots_research.py:301', 'fno_v13_v10_g_2_unused_slots_research.py:423', 'fno_v13_v10_g_options_backtest.py:382']},
])
(OUT / 'strategy_dependencies.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps({'seed_count': len(seeds), 'protected_count': len(protected), 'artifact_count':len(artifact_refs), 'edges':len(edges), 'path_literals': len(refs), 'errors':errors, 'output': str(OUT / 'strategy_dependencies.json')}))
