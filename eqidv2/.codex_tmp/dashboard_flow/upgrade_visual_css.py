from pathlib import Path
import re

path = Path('dashboard_flow.css')
css = path.read_text(encoding='utf-8-sig')
# Meaningful captions should never inherit the original 6–9px type scale.
css = re.sub(r'(font-size:)\s*([6-9])px', r'\g<1>10px', css)
css = re.sub(r'(font:)\s*([6-9])px', r'\g<1>10px', css)
css += r'''

/* Readable, compact surfaces. Theme values also drive the SVG chart. */
:root {
  color-scheme:light;
  --bg:#f3f6f4; --surface:#fff; --surface-soft:#f6f9f7; --surface-raised:#edf3ef;
  --ink:#183b32; --muted:#596e63; --line:#dce6df; --line-strong:#a6bcb0;
  --green:#15835f; --green-dark:#087553; --mint:#eaf6ee; --rose:#a54661; --pink:#fceef2; --blue:#385f9b;
  --chart-bg:#f8fbf9; --chart-grid:#dce7df; --chart-zero:#91ad9e; --chart-spine:#e9f2ec;
  --chart-green:#15946b; --chart-rose:#bf667f; --selected-green:#d7f0e3; --selected-rose:#f6e1e7;
  --card-green-start:#f8fcf9; --card-green-end:#eaf6ef; --card-green-line:#c8e1d2;
  --card-rose-start:#fffbfc; --card-rose-end:#fbeef2; --card-rose-line:#e8ccd5;
  --notice-bg:#fffaeb; --notice-ink:#786020; --notice-line:#e6d7a6;
  --family-g3:#087553; --family-g2:#385f9b; --family-g:#a54661;
}
body[data-theme="dark"] {
  color-scheme:dark;
  --bg:#10141b; --surface:#171e28; --surface-soft:#1b2530; --surface-raised:#23313d;
  --ink:#edf2f7; --muted:#a7b9b0; --line:#304139; --line-strong:#597566;
  --green:#6edbb0; --green-dark:#6edbb0; --mint:#193b30; --rose:#eda0b5; --pink:#3d2834; --blue:#9fbaf1;
  --chart-bg:#17241f; --chart-grid:#30473a; --chart-zero:#718f7d; --chart-spine:#22392d;
  --chart-green:#64d6a8; --chart-rose:#e396ad; --selected-green:#284e3e; --selected-rose:#533548;
  --card-green-start:#203a2e; --card-green-end:#1b3027; --card-green-line:#365747;
  --card-rose-start:#3a2932; --card-rose-end:#30232c; --card-rose-line:#614252;
  --notice-bg:#332e20; --notice-ink:#e5cf90; --notice-line:#665939;
  --family-g3:#6edbb0; --family-g2:#9fbaf1; --family-g:#eda0b5;
}
button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible,[tabindex]:focus-visible{outline:2px solid var(--green);outline-offset:3px}
body[data-theme="dark"] button:hover:not(:disabled){filter:brightness(1.12)}
.app-header{height:66px;padding:0 28px;gap:20px;background:var(--surface)}
.brand{font-size:23px}.brand small{font-size:10px;letter-spacing:.5px;color:var(--muted)}
.view-tabs{margin-left:2vw;gap:20px}.view-tabs button{color:var(--muted)}
.header-end{gap:8px}.historical-badge{font-size:11px;color:var(--muted);background:var(--surface-soft);border-color:var(--line)}
.icon-btn{height:34px;width:34px;background:var(--surface)}
#flow-theme,#logs-link{display:inline-flex;align-items:center;justify-content:center;min-height:34px;width:auto;padding:7px 10px;border:1px solid var(--line);border-radius:6px;background:var(--surface);font-size:12px;white-space:nowrap;color:var(--ink)}
#logs-link{color:var(--green-dark)}
.avatar{background:var(--surface-raised);border-color:var(--surface);color:var(--muted)}
main{padding:18px 28px;min-width:0}.workspace-heading{margin-bottom:15px;gap:24px}.workspace-heading>div:first-child{min-width:0}
.eyebrow{font-size:10px;letter-spacing:1.1px;color:var(--muted)}
h1{font-size:30px;margin:7px 0 4px}.workspace-heading p{margin-top:6px;line-height:1.5}
.run-control{width:355px;max-width:100%;flex-shrink:0}.run-control label{font-size:10px;letter-spacing:.6px;color:var(--muted)}
select{background:var(--surface);border-color:var(--line-strong);font-size:12px;padding:9px 10px}
.run-control>span{font-size:11px;color:var(--muted);line-height:1.4}
.family-tabs{display:flex;gap:4px;background:var(--surface-raised);padding:3px;border:1px solid var(--line);border-radius:7px}
.family-tabs button{flex:1;min-height:30px;border:0;border-radius:5px;background:none;font-size:12px;font-weight:600;color:var(--muted)}
.family-tabs button.active{background:var(--surface);color:var(--green-dark);box-shadow:0 1px 4px #173f2520}
.run-control .archive-toggle{display:flex;align-items:center;gap:6px;font:11px var(--sans);letter-spacing:0;color:var(--muted)}
.archive-toggle input{margin:0;accent-color:var(--green)}
.metrics{background:var(--surface);padding:15px 0;margin-bottom:11px}.metrics>div{padding:0 22px;min-width:0}
.metrics>div>span{color:var(--muted);letter-spacing:.6px}.metrics small{font-size:9px;background:var(--surface-soft);color:var(--muted)}
.metrics strong{font-size:27px;margin:6px 0 4px}.metrics em{font-size:11px;color:var(--muted);line-height:1.4}
.scope-bar{display:flex;align-items:center;gap:8px 16px;flex-wrap:wrap;margin:0 0 13px;font-size:12px;color:var(--muted)}
.scope-bar label{display:flex;align-items:center;gap:8px;color:var(--ink)}.scope-bar select{font-size:11px;padding:6px 8px}
#scope-description{line-height:1.5}#filter-summary{margin-left:auto;font-size:11px;color:var(--muted);line-height:1.5}
.notice{background:var(--notice-bg);color:var(--notice-ink);border-color:var(--notice-line)}
.flow-workspace,.ledger-view,.compare-view{background:var(--surface);border:1px solid var(--line);border-radius:10px;overflow:hidden;min-width:0}
.workspace-toolbar{padding:13px 18px;gap:14px}.workspace-title strong{font-size:14px}
.subtle-tag{border-color:var(--line);color:var(--muted)}.toolbar-controls{min-width:0}.toolbar-controls select{font-size:11px}
.search{background:var(--surface-soft);border-color:var(--line-strong);min-width:0}.search>span,.search input::placeholder,.search kbd{color:var(--muted)}
.search input{font-size:12px;min-width:0}.text-btn{font-size:12px;color:var(--muted)}
.breadth-row{padding:11px 18px;background:var(--surface-soft);font-size:11px}.breadth-row strong{font-size:12px}
.live-dot{box-shadow:0 0 0 3px var(--mint)}.breadth-track{background:var(--surface-raised)}.breadth-track .up{background:var(--chart-green)}.breadth-track .down{background:var(--chart-rose)}
.flow-body{grid-template-columns:170px minmax(0,1fr) 290px;min-height:520px}
.ranking-panel{padding:17px 14px;background:var(--surface);min-width:0}.panel-heading,.panel-heading span:last-child{color:var(--muted);font-size:10px;letter-spacing:.3px}
.panel-caption{color:var(--muted);font-size:11px;margin:8px 0 20px}
.setup-label{font-size:11px;color:var(--ink)}.setup-label b{max-width:80px}.setup-label span{font-size:10px}.setup-track{background:var(--surface-raised)}.setup-track i{background:var(--chart-green)}.setup-row.loss .setup-track i{background:var(--chart-rose)}
.ranking-bottom{border-color:var(--line);padding-top:18px;color:var(--muted)}.ranking-bottom .mini-icon{background:var(--mint);color:var(--green-dark)}.ranking-bottom strong{color:var(--ink);font-size:12px}.ranking-bottom p{font-size:11px;line-height:1.6}
.chart-panel{background:var(--chart-bg)}.chart-topline{padding:12px 15px;font-size:11px;color:var(--muted)}
.range-control{background:var(--surface-raised);flex-shrink:0}.range-control button{color:var(--muted);font-size:10px}.range-control .active{background:var(--surface);color:var(--green-dark)}
.distribution{height:43px;flex-shrink:0;border-color:var(--line)}.dist-axis{background:var(--line-strong)}.dist-zero{background:var(--muted)}.dist-dot{border-color:var(--chart-bg);background:var(--chart-green)}.dist-dot.loss{background:var(--chart-rose)}.dist-label{color:var(--muted);font-size:10px;letter-spacing:.2px}
.flow-chart{height:395px;overflow:hidden}.flow-chart>svg{min-height:365px}.chart-footnote{font-size:10px;color:var(--muted);padding:11px 16px}.chart-footnote>span:last-child>span{color:var(--green-dark)}
.stock-panel{background:var(--surface);padding:17px 13px 12px}.stock-sort{color:var(--muted);font-size:11px}
.outcome-group-label{font-size:10px;opacity:1;letter-spacing:.4px}.stock-group-grid{grid-auto-rows:minmax(131px,auto);max-height:279px;scrollbar-color:var(--line-strong) transparent}.stock-outcome-group:only-child .stock-group-grid{max-height:552px}
.stock-card{border-color:var(--card-green-line);background:linear-gradient(155deg,var(--card-green-start),var(--card-green-end))}.stock-card:hover,.stock-card.selected{border-color:var(--green)}
.stock-card.loss{background:linear-gradient(155deg,var(--card-rose-start),var(--card-rose-end));border-color:var(--card-rose-line)}.stock-card.loss:hover{border-color:var(--rose)}
.stock-card .card-symbol,.stock-card.loss .card-symbol{font-size:12px;color:var(--ink)}.stock-card .card-rank{font-size:10px;color:var(--muted);border-color:var(--line-strong)}
.stock-card .card-value{font-size:20px;color:var(--green-dark)}.stock-card.loss .card-value{color:var(--rose)}
.stock-card .card-meta,.stock-card.loss .card-meta,.card-foot,.stock-card.loss .card-foot{font-size:10px;color:var(--muted);letter-spacing:0}
.card-chart{opacity:1}.card-chart path[stroke]{stroke:var(--chart-green)}.card-chart path:not([stroke]){fill:var(--chart-green);opacity:.13}.loss .card-chart path[stroke]{stroke:var(--chart-rose)}.loss .card-chart path:not([stroke]){fill:var(--chart-rose)}
.replay-bar{background:var(--surface);padding:12px 18px}.replay-button{background:var(--mint);border-color:var(--card-green-line);color:var(--green-dark);height:32px;width:32px;font-size:12px}.replay-label{width:125px}.replay-label strong{font-size:12px}.replay-label span,.replay-bar>span{color:var(--muted);font-size:10px}.replay-bar input{accent-color:var(--green)}.replay-bar>.text-btn{font-size:11px}
.app-footer{color:var(--muted);font-size:10px;line-height:1.6;padding-top:15px}.app-footer>span{flex-wrap:wrap}.app-footer .text-btn{font-size:10px}.divider{color:var(--line-strong)}
.loading-state,.empty-state{color:var(--muted);font-size:12px;line-height:1.6}.loading-orbit{border-color:var(--line);border-top-color:var(--green)}
.export-btn{background:var(--mint);border-color:var(--card-green-line);color:var(--green-dark);font-size:12px;white-space:nowrap}
.table-wrap{max-width:100%}th{font-size:11px;color:var(--muted);background:var(--surface-soft)}td{font-size:12px;color:var(--ink)}th,td{padding:13px 18px;border-color:var(--line)}td:first-child{font-size:11px;color:var(--muted)}td button{color:var(--green-dark);font-size:12px}.side-badge{border-color:var(--line-strong);color:var(--muted);font-size:10px}.table-footer{font-size:12px;color:var(--muted)}
dialog{border-color:var(--line-strong);background:var(--surface)}dialog::backdrop{background:#10221c75}.dialog-head{padding:22px 25px 17px}.dialog-head p{font-size:11px;color:var(--muted);line-height:1.6}.detail-metrics span{font-size:11px;color:var(--muted)}.detail-chart{height:220px}.detail-table th,.detail-table td{font-size:11px}.dialog-foot{font-size:11px;color:var(--muted);line-height:1.5}.about-content{color:var(--muted)}.skip-link{background:var(--surface)}
/* G-family comparison. */
.compare-header{display:flex;justify-content:space-between;align-items:center;gap:20px;padding:20px 22px;border-bottom:1px solid var(--line)}.compare-header h2{font-size:22px;margin:0 0 5px}.compare-header p{font-size:12px;color:var(--muted);margin:0;line-height:1.6}.compare-controls{display:flex;align-items:center;gap:12px;flex-shrink:0}.compare-controls label{display:flex;align-items:center;gap:8px;font-size:12px;color:var(--muted)}
.compare-status{padding:12px 22px;color:var(--muted);font-size:12px;line-height:1.6}.compare-coverage{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;padding:18px 22px}.compare-family-card{background:var(--surface-soft);border:1px solid var(--line);border-top:3px solid var(--family-color,var(--green));border-radius:7px;padding:13px 15px;min-width:0}.compare-family-label{font-size:14px;font-weight:600;color:var(--family-color,var(--ink));margin-bottom:7px}.compare-family-date{font:11px var(--mono);color:var(--ink);line-height:1.7}.compare-family-meta{font-size:12px;color:var(--muted);line-height:1.7;margin-top:3px}.compare-notice{padding:12px 15px;margin:0 22px 16px;border:1px solid var(--notice-line);background:var(--notice-bg);color:var(--notice-ink);border-radius:6px;font-size:12px;line-height:1.6}.compare-exclusions{margin:0 22px 18px;font-size:12px;color:var(--muted);line-height:1.7}.compare-exclusions summary{cursor:pointer;color:var(--ink)}.compare-exclusions ul{padding-left:20px}
.compare-panel{padding:18px 22px;border-top:1px solid var(--line);min-width:0}.compare-panel h3{font-size:15px;font-weight:600;margin:0 0 6px}.compare-panel p{font-size:12px;color:var(--muted);line-height:1.6;margin:0 0 12px}.compare-chart{width:100%;min-width:0;overflow:auto;background:var(--chart-bg);border:1px solid var(--line);border-radius:7px}.compare-chart svg{display:block;width:100%}.compare-axis-label{fill:var(--muted);font-size:12px}.compare-grid-line{stroke:var(--chart-grid)}.compare-zero-line{stroke:var(--chart-zero)}.compare-legend{display:flex;flex-wrap:wrap;gap:16px;margin:12px 0 2px;font-size:12px}.compare-legend-item{display:flex;align-items:center;gap:7px;color:var(--ink)}.compare-legend-item:before{content:"";width:17px;height:3px;border-radius:2px;background:var(--family-color,var(--green))}.compare-table td,.compare-table th{padding:12px 15px}.compare-table th:first-child{min-width:140px}
@media(min-width:1600px){.flow-body{grid-template-columns:190px minmax(0,1fr) 338px;min-height:557px}.flow-chart{height:430px}.metrics{padding:16px 0}main{padding:22px 36px}}
@media(max-width:1250px){.app-header{padding:0 22px;gap:15px}.view-tabs{margin-left:0;gap:15px}.header-end{gap:7px}.historical-badge,.avatar{display:none}main{padding:18px 22px}.metrics>div{padding:0 17px}.metrics strong{font-size:25px}.metrics small{display:none}.flow-body{grid-template-columns:156px minmax(0,1fr) 258px}.stock-card .card-value{font-size:18px}.stock-card .card-symbol{font-size:11px}.ranking-panel{padding:17px 12px}.setup-label{font-size:10px}.setup-label b{max-width:72px}.brand{font-size:22px}}
@media(max-width:980px){.app-header{height:auto;flex-wrap:wrap;padding-top:12px;gap:12px}.view-tabs{order:3;width:100%;height:38px;gap:25px;margin:0}.header-end{margin-left:auto}.flow-body{grid-template-columns:155px minmax(0,1fr)}.stock-panel{padding:16px}.stock-card .card-symbol{font-size:12px}.stock-card .card-value{font-size:21px}.metrics{grid-template-columns:repeat(3,minmax(0,1fr));padding:0}.metrics>div{padding:14px 17px;border-bottom:1px solid var(--line)}.metrics>div:nth-child(3){border-right:0}.metrics>div:nth-child(n+4){border-bottom:0}.metrics .sessions-metric{display:block}.metrics>div:last-child{border-right:0}.run-control{width:310px}.flow-chart{height:380px}.compare-header{align-items:flex-start;flex-direction:column}.compare-controls{width:100%;justify-content:space-between}.compare-coverage{gap:9px}.compare-family-card{padding:11px}}
@media(max-width:640px){.app-header{padding:12px 14px 0;gap:12px}.brand{font-size:22px}.brand small{font-size:10px;letter-spacing:0}.header-end{gap:6px;flex-wrap:wrap;margin-left:0}.view-tabs{position:static;gap:19px;height:38px;overflow:auto}.view-tabs button{font-size:12px}main{padding:17px 13px}.workspace-heading{margin-bottom:14px}h1{font-size:28px}.eyebrow{font-size:10px}.workspace-heading p{font-size:12px}.run-control{display:flex;margin-top:16px;width:100%;gap:7px}.run-control label{font-size:11px}.run-control select{font-size:12px;padding:9px}.run-control>span{font-size:10px}.metrics{grid-template-columns:1fr 1fr;margin-bottom:10px;padding:0}.metrics>div{padding:14px;border:0;border-bottom:1px solid var(--line)}.metrics>div:nth-child(odd){border-right:1px solid var(--line)}.metrics>div:nth-child(4){border-bottom:1px solid var(--line)}.metrics>div:last-child{grid-column:1/-1;border:0}.metrics strong{font-size:24px}.metrics>div>span{font-size:10px}.metrics em{font-size:11px}.scope-bar{gap:7px}.scope-bar label{width:100%;justify-content:space-between}#scope-description,#filter-summary{font-size:11px;margin-left:0;width:100%}.workspace-toolbar{padding:13px 12px;gap:12px}.search input{font-size:11px}.toolbar-controls select{max-width:119px;padding:7px 5px}.breadth-row{font-size:11px}.breadth-counts{gap:5px;font-size:10px}.breadth-counts b{margin-left:3px}.panel-caption{font-size:11px;margin:5px 0 14px}.setup-rankings{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}.setup-label{font-size:11px}.setup-label b{max-width:100px}.chart-topline{padding:12px 10px;font-size:11px;align-items:flex-start}.range-control button{font-size:10px}.flow-chart{height:370px;overflow:auto}.flow-chart>svg{min-width:540px;min-height:365px}.chart-footnote{font-size:10px;flex-wrap:wrap;padding:10px}.stock-group-grid{grid-auto-rows:minmax(145px,auto);max-height:309px}.stock-card .card-symbol{font-size:12px}.stock-card .card-value{font-size:22px}.replay-bar{padding:12px;gap:9px}.replay-label{width:106px}.replay-label strong{font-size:12px}.app-footer,.app-footer .text-btn{font-size:10px}.detail-metrics span,.dialog-head p{font-size:11px}.detail-chart{padding:0 8px}.table-footer{font-size:11px}.compare-header{padding:17px 14px;gap:14px}.compare-controls{align-items:flex-start;flex-wrap:wrap;gap:9px}.compare-controls label{flex-wrap:wrap}.compare-coverage{grid-template-columns:1fr;padding:14px;gap:9px}.compare-family-card{padding:12px 14px}.compare-notice,.compare-exclusions{margin-left:14px;margin-right:14px}.compare-panel{padding:17px 14px}.compare-status{padding:12px 14px}}
'''
path.write_text(css, encoding='utf-8')
