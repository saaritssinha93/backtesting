(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const state = {data:null, range:'all', side:'all', query:'', setup:null, selected:null, reverse:false, frame:0, dates:[], stocks:[], scopedTrades:[], page:0, timer:null, request:0, scope:'filtered', view:'flow'};
  const money = (value, decimals=0) => value == null || !Number.isFinite(value) ? '—' : `${value < 0 ? '−' : ''}₹${Math.abs(value).toLocaleString('en-IN',{maximumFractionDigits:decimals,minimumFractionDigits:decimals})}`;
  const shortMoney = value => value == null ? '—' : `${value < 0 ? '−' : '+'}₹${Math.abs(value)>=100000 ? (Math.abs(value)/100000).toFixed(2)+'L' : Math.abs(value)>=1000 ? (Math.abs(value)/1000).toFixed(1)+'k' : Math.abs(value).toFixed(0)}`;
  const num = value => value == null ? '—' : Number(value).toLocaleString('en-IN');
  const dayLabel = value => value ? new Date(value+'T12:00:00').toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric'}) : '—';
  const cls = value => value < 0 ? 'negative' : value > 0 ? 'positive' : '';
  function el(tag, className, text) {const n=document.createElement(tag);if(className)n.className=className;if(text!=null)n.textContent=text;return n;}
  function empty(target, message) {const n=el('div','empty-state',message);target.replaceChildren(n);}
  function api(path) {const url=new URL(path,location.origin);if(FLOW_API_TOKEN)url.searchParams.set('token',FLOW_API_TOKEN);return url.pathname+url.search;}
  $('logs-link').href=api('/');document.querySelector('.brand').href=api('/dashboard-flow');
  const themeKey='eqidv2_dashboard_theme';
  function applyTheme(theme) {
    const value=theme==='dark'?'dark':'light';document.body.dataset.theme=value;
    $('flow-theme').textContent=value==='dark'?'Light':'Dark';$('flow-theme').setAttribute('aria-label','Switch to '+(value==='dark'?'light':'dark')+' theme');
    if(state.data){renderFlow();const stock=state.stocks.find(s=>s.symbol===state.selected);if(stock&&$('stock-dialog').open)FlowChart.render($('detail-chart'),[stock],state.dates.slice(0,state.frame),{compact:true,selected:stock.symbol});}
  }
  try{applyTheme(localStorage.getItem(themeKey));}catch{applyTheme('light');}
  $('flow-theme').onclick=()=>{const theme=document.body.dataset.theme==='dark'?'light':'dark';try{localStorage.setItem(themeKey,theme);}catch{}applyTheme(theme);};
  window.addEventListener('storage',event=>{if(event.key===themeKey)applyTheme(event.newValue);});
  function renderRunControls() {
    const data=state.data, selected=data.selected_run;
    const families=[...new Set(data.runs.map(run=>run.strategy))];
    $('family-tabs').replaceChildren(...families.map(family=>{
      const button=el('button',family===selected?.strategy?'active':'',FlowCore.shortStrategy(family));button.type='button';
      button.setAttribute('aria-pressed',String(family===selected?.strategy));button.setAttribute('aria-label','Show '+FlowCore.shortStrategy(family)+' backtests');
      button.onclick=()=>{$('show-archives').checked=false;load(data.runs.find(run=>run.strategy===family).id);};return button;
    }));
    const familyRuns=data.runs.filter(run=>run.strategy===selected?.strategy);
    const visible=$('show-archives').checked?familyRuns:familyRuns.filter((run,index)=>index===0||run.id===selected?.id);
    $('run-select').replaceChildren(...visible.map(run=>{const index=familyRuns.indexOf(run);const option=el('option','',FlowCore.runLabel(run,index===0,index));option.value=run.id;option.title=FlowCore.runLabel(run,index===0,index);return option;}));
    if(selected)$('run-select').value=selected.id;
    if(!data.runs.length)$('run-select').append(el('option','','No saved runs'));
  }
  function renderComparison(force=false) {if(state.data)return FlowComparison.render($('compare-view'),{runs:state.data.runs,api,force});}
  function setView(view) {
    stopReplay();state.view=view;
    $('flow-view').hidden=view!=='flow';$('ledger-view').hidden=view!=='ledger';$('compare-view').hidden=view!=='compare';
    ['summary-metrics','scope-bar','run-controls'].forEach(id=>$(id).hidden=view==='compare');
    document.querySelectorAll('[data-view]').forEach(button=>{const active=button.dataset.view===view;button.classList.toggle('active',active);button.setAttribute('aria-pressed',String(active));});
    if(view==='compare')renderComparison();
  }
  function stopReplay() {if(state.timer)clearInterval(state.timer);state.timer=null;$('replay-toggle').textContent='▶';$('replay-toggle').setAttribute('aria-label','Play historical sessions');}
  function setNotice(message) {$('notice').textContent=message;$('notice').hidden=!message;}
  async function load(run) {
    stopReplay(); const request=++state.request; $('refresh').disabled=true; $('run-select').disabled=true;
    setNotice(''); $('flow-view').setAttribute('aria-busy','true');
    const controller=new AbortController();const timeout=setTimeout(()=>controller.abort(),20000);
    try {
      const response=await fetch(api('/api/dashboard-flow'+(run?'?run='+encodeURIComponent(run):'')),{cache:'no-store',signal:controller.signal});
      if(!response.ok)throw new Error(response.status===401?'Your dashboard session has expired. Reopen Dashboard Flow from its launcher.':'The saved run could not be loaded. Refresh to try again.');
      const data=await response.json();if(request!==state.request)return;if(data.error)throw new Error(data.error);
      state.data=data;state.selected=null;state.setup=null;state.frame=0;state.page=0;
      renderRunControls();renderFlow(true);setNotice((data.warnings||[]).join(' '));
      if(state.view==='compare')await renderComparison(true);
    } catch(error) {if(request!==state.request)return;setNotice(error.name==='AbortError'?'Reading the saved results timed out. Please refresh.':error.message);if(state.data?.selected_run)$('run-select').value=state.data.selected_run.id;if(!state.data){empty($('flow-chart'),'Data is unavailable. Use Refresh to retry.');empty($('stock-cards'),'Waiting for saved results.');}}
    finally {clearTimeout(timeout);if(request===state.request){$('refresh').disabled=false;$('run-select').disabled=false;$('flow-view').removeAttribute('aria-busy');}}
  }
  function renderMetrics() {
    if(!state.data)return;
    const r=state.data.selected_run,dates=state.dates.slice(0,state.frame);
    const filtered=state.scope==='filtered';
    const s=filtered?FlowCore.summarize(state.scopedTrades,dates,FlowCore.hasCompleteLedger(state.data)):state.data.summary;
    $('metric-net').textContent=money(s.net_pnl);$('metric-net').className=cls(s.net_pnl);
    $('metric-net-note').textContent=`${num(s.trades)} trades across ${num(s.sessions)} sessions`;
    $('metric-win').textContent=s.win_rate_pct==null?'—':s.win_rate_pct.toFixed(1)+'%';
    $('metric-win-note').textContent=`${num(s.wins)} wins / ${num(s.losses)} losses`;
    $('metric-pf').textContent=s.profit_factor==null?'—':s.profit_factor.toFixed(2)+'×';
    $('metric-dd').textContent=money(s.max_drawdown);$('metric-sessions').textContent=`${num(s.sessions)} / ${num(s.trades)}`;
    $('metric-cost').textContent=money(s.cost)+' total costs';$('period').textContent=`${dayLabel(state.data.summary.period_start)} — ${dayLabel(state.data.summary.period_end)} · ${num(state.data.summary.sessions)} recorded sessions`;
    $('source-label').textContent=r?r.source_label:'No saved results';
    const filters=[];if(state.query)filters.push('Search: '+state.query);if(state.side!=='all')filters.push(state.side==='LONG'?'Long only':'Short only');if(state.setup)filters.push('Setup: '+state.setup);if(state.range!=='all')filters.push('Last '+state.range+' sessions');if(state.frame<state.dates.length)filters.push('Replay to '+dayLabel(dates.at(-1)));
    $('filter-summary').textContent=filters.join(' · ');
    $('scope-description').textContent=filtered?`${num(s.trades)} trades · ${num(s.sessions)} sessions${!FlowCore.hasCompleteLedger(state.data)?' · Incomplete ledger: totals unavailable':''}`:'Full recorded run · map filters do not change this summary or ledger';
    $('clear-global-filters').hidden=!filters.length;
    $('summary-metrics').setAttribute('aria-label',filtered?'Filtered selection performance':'Entire run performance');
  }
  function aggregate(trades, dates) {
    const rows=new Map();
    trades.forEach(t=>{
      const symbol=t.symbol||'UNSPECIFIED';if(!rows.has(symbol))rows.set(symbol,{symbol,net_pnl:0,trades:0,wins:0,days:new Map(),sides:new Set(),entries:[],complete:true});
      const s=rows.get(symbol);s.trades++;s.entries.push(t);s.sides.add(t.side);if(t.net_pnl==null)s.complete=false;else{s.net_pnl+=t.net_pnl;s.wins+=t.net_pnl>0?1:0;s.days.set(t.date,(s.days.get(t.date)||0)+t.net_pnl);}
    });
    return [...rows.values()].map(s=>{let total=0;return {...s,net_pnl:s.complete?s.net_pnl:null,side:s.sides.size===1?[...s.sides][0]:'MIXED',points:dates.map(d=>{total+=s.days.get(d)||0;return s.complete?total:null;})};}).sort((a,b)=>(b.net_pnl??-Infinity)-(a.net_pnl??-Infinity)||a.symbol.localeCompare(b.symbol));
  }
  function filteredBase() {
    const daily=state.data.daily;const windowDays=state.range==='all'?daily:daily.slice(-Number(state.range));
    const dates=windowDays.map(d=>d.date);const set=new Set(dates);
    return {dates,trades:state.data.trades.filter(t=>set.has(t.date)&&(state.side==='all'||t.side===state.side)&&(!state.query||[t.symbol,t.setup,t.side,t.date].join(' ').toLowerCase().includes(state.query)))};
  }
  function renderFlow(reset=false) {
    if(!state.data)return;
    const base=filteredBase();state.dates=base.dates;
    if(reset||!state.frame)state.frame=base.dates.length;state.frame=Math.min(state.frame,base.dates.length);
    const dates=base.dates.slice(0,state.frame);const visibleDates=new Set(dates);const replayTrades=base.trades.filter(t=>visibleDates.has(t.date));
    state.scopedTrades=replayTrades.filter(t=>!state.setup||(t.setup||'Unclassified')===state.setup);
    state.stocks=aggregate(state.scopedTrades,dates);
    if(!state.stocks.some(s=>s.symbol===state.selected))state.selected=null;
    renderRankings(replayTrades);renderDistribution();renderCards();
    const winners=state.stocks.filter(s=>s.net_pnl>0).length,losers=state.stocks.filter(s=>s.net_pnl<0).length,flat=state.stocks.length-winners-losers;
    $('stock-count').textContent=state.stocks.length+(state.stocks.length===1?' stock':' stocks');$('positive-count').textContent=winners;$('negative-count').textContent=losers;$('flat-count').textContent=flat?flat+' FLAT / N.A.':'';
    const up=el('i','up'),down=el('i','down');up.style.flex=winners;down.style.flex=losers;$('breadth-track').replaceChildren(up,down);
    $('card-count').textContent=state.stocks.length+' STOCKS';
    $('chart-caption').textContent=state.setup?state.setup+' · cumulative net P&L':'Cumulative net P&L by stock';
    if(state.stocks.length)FlowChart.render($('flow-chart'),state.stocks,dates,{selected:state.selected,onSelect:selectStock});
    else empty($('flow-chart'),state.data.trades.length?'No executed trades match this selection. Try Reset or advance the replay.':'No executed trade ledger is available for this run. Choose Entire run to see saved daily totals.');
    $('replay-slider').max=Math.max(1,base.dates.length);$('replay-slider').value=state.frame||1;$('replay-slider').disabled=!base.dates.length;
    $('replay-toggle').disabled=base.dates.length<2;$('replay-date').textContent=dayLabel(dates.at(-1));$('replay-progress').textContent=state.frame+' / '+base.dates.length;
    $('replay-slider').setAttribute('aria-valuetext',dayLabel(dates.at(-1)));$('clear-setup').hidden=!state.setup;
    renderMetrics();renderLedger();
  }
  function renderRankings(trades) {
    const groups=new Map();trades.forEach(t=>{const name=t.setup||'Unclassified';const row=groups.get(name)||{name,pnl:0,complete:true};if(t.net_pnl==null)row.complete=false;else row.pnl+=t.net_pnl;groups.set(name,row);});
    const sorted=[...groups.values()].sort((a,b)=>b.pnl-a.pnl);const max=Math.max(1,...sorted.map(s=>Math.abs(s.pnl)));
    const nodes=sorted.map(row=>{const button=el('button','setup-row'+(row.pnl<0?' loss':'')+(row.name===state.setup?' active':''));button.title=row.name+' · '+money(row.complete?row.pnl:null);button.setAttribute('aria-pressed',String(row.name===state.setup));
      const label=el('span','setup-label'),name=el('b','',row.name),value=el('span',cls(row.pnl),shortMoney(row.complete?row.pnl:null));label.append(name,value);
      const track=el('span','setup-track'),bar=el('i');bar.style.width=Math.max(2,Math.abs(row.pnl)/max*100)+'%';track.append(bar);button.append(label,track);
      button.addEventListener('click',()=>{state.setup=state.setup===row.name?null:row.name;state.selected=null;renderFlow();});return button;});
    $('setup-rankings').replaceChildren(...nodes);if(!nodes.length)empty($('setup-rankings'),'No setups in view');
  }
  function renderDistribution() {
    const container=$('distribution');container.replaceChildren(el('span','dist-axis'),el('span','dist-label','STOCK DISPERSION'),el('span','dist-label right','NET P&L →'));
    const known=state.stocks.filter(s=>s.net_pnl!=null);const min=Math.min(0,...known.map(s=>s.net_pnl)),max=Math.max(1,...known.map(s=>s.net_pnl));
    const zero=el('i','dist-zero');zero.style.left=(-min/(max-min)*94+3)+'%';container.append(zero);
    known.forEach((s,i)=>{const dot=el('button','dist-dot'+(s.net_pnl<0?' loss':''));dot.style.left=((s.net_pnl-min)/(max-min)*94+3)+'%';dot.style.top=17+(i%3)*5+'px';dot.title=s.symbol+' · '+money(s.net_pnl);dot.setAttribute('aria-label',dot.title);dot.onclick=()=>selectStock(s.symbol);container.append(dot);});
  }
  function sparkline(points,negative) {
    const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');svg.setAttribute('viewBox','0 0 120 32');svg.setAttribute('preserveAspectRatio','none');svg.setAttribute('aria-hidden','true');svg.classList.add('card-chart');
    const known=points.filter(v=>v!=null);if(!known.length)return svg;
    const min=Math.min(0,...known),max=Math.max(1,...known),span=max-min||1;
    const coords=points.map((p,i)=>p==null?null:[i/Math.max(1,points.length-1)*120,29-(p-min)/span*26]);
    const line=document.createElementNS(ns,'path');let d='';let pen=false;coords.forEach(p=>{if(!p){pen=false;return;}d+=(pen?'L':'M')+p.join(',');pen=true;});
    const area=document.createElementNS(ns,'path');if(coords.every(Boolean)){area.setAttribute('d',d+'L120,32L0,32Z');area.setAttribute('fill',negative?'#e9bdc955':'#8bceaa44');svg.append(area);}
    line.setAttribute('d',d);line.setAttribute('fill','none');line.setAttribute('stroke',negative?'#c58295':'#6db489');line.setAttribute('stroke-width','1.3');svg.append(line);return svg;
  }
  function renderCards() {
    const rows=state.reverse?[...state.stocks].reverse():state.stocks;
    const cards=rows.map(s=>{const card=el('button','stock-card'+(s.net_pnl<0?' loss':'')+(s.symbol===state.selected?' selected':''));card.setAttribute('aria-label',`${s.symbol}, ${money(s.net_pnl)}, ${s.trades} trades. Open details.`);
      card.append(el('span','card-symbol',s.symbol),el('span','card-value',shortMoney(s.net_pnl)),el('span','card-rank',String(state.stocks.indexOf(s)+1).padStart(2,'0')),el('span','card-meta',s.side+' · '+(s.net_pnl>0?'NET PROFIT':s.net_pnl<0?'NET LOSS':s.net_pnl==null?'INCOMPLETE':'FLAT')));
      const profile=el('span','profile');s.points.slice(-10).forEach(p=>{const b=el('i');const max=Math.max(1,...s.points.filter(v=>v!=null).map(Math.abs));b.style.width=Math.max(4,Math.abs(p||0)/max*100)+'%';profile.append(b);});card.append(profile,sparkline(s.points,s.net_pnl<0));
      const foot=el('span','card-foot');foot.append(el('span','',s.trades+' TRADES'),el('span','',s.complete?Math.round(s.wins/s.trades*100)+'% WIN':'N/A'));card.append(foot);card.onclick=()=>selectStock(s.symbol);return card;});
    const positive=el('section','stock-outcome-group'),negative=el('section','stock-outcome-group');
    const profitGrid=el('div','stock-group-grid'),lossGrid=el('div','stock-group-grid');
    cards.forEach((card,i)=>(rows[i].net_pnl<0?lossGrid:profitGrid).append(card));
    positive.append(el('div','outcome-group-label positive','PROFITABLE / FLAT · '+profitGrid.children.length),profitGrid);
    negative.append(el('div','outcome-group-label negative','LOSING · '+lossGrid.children.length),lossGrid);
    const groups=[positive,negative].filter(g=>g.lastChild.children.length);if(state.reverse)groups.reverse();
    $('stock-cards').replaceChildren(...groups);if(!cards.length)empty($('stock-cards'),'No stocks in this selection');
  }
  function selectStock(symbol) {
    stopReplay();state.selected=symbol;renderCards();const s=state.stocks.find(row=>row.symbol===symbol);if(!s)return;
    FlowChart.render($('flow-chart'),state.stocks,state.dates.slice(0,state.frame),{selected:symbol,onSelect:selectStock});
    $('detail-title').textContent=s.symbol;$('detail-subtitle').textContent=`${state.data.selected_run.strategy} / ${s.side} / ${s.trades} EXECUTED TRADES`;
    const metrics=[['NET P&L',money(s.net_pnl),cls(s.net_pnl)],['WIN RATE',s.complete?Math.round(s.wins/s.trades*100)+'%':'—',''],['EXECUTED TRADES',num(s.trades),''],['AVG. NET / TRADE',money(s.net_pnl==null?null:s.net_pnl/s.trades),'']];
    $('detail-metrics').replaceChildren(...metrics.map(([label,value,c])=>{const n=el('div');n.append(el('span','',label),el('strong',c,value));return n;}));
    if(!$('stock-dialog').open)$('stock-dialog').showModal();
    FlowChart.render($('detail-chart'),[s],state.dates.slice(0,state.frame),{compact:true,selected:symbol});
    $('detail-trades').replaceChildren(...[...s.entries].reverse().map(t=>{const row=el('tr');row.append(el('td','',dayLabel(t.date)),el('td','',t.side),el('td','',t.setup||'—'),el('td','',money(t.entry_price,2)+' → '+money(t.exit_price,2)),el('td','numeric '+cls(t.net_pnl),money(t.net_pnl,2)));return row;}));
  }
  function ledgerRows() {return (state.scope==='filtered'?state.scopedTrades:(state.data?.trades||[])).slice().reverse();}
  function renderLedger() {
    const rows=ledgerRows(),size=20;const pages=Math.max(1,Math.ceil(rows.length/size));state.page=Math.min(state.page,pages-1);const slice=rows.slice(state.page*size,(state.page+1)*size);
    $('ledger-count').textContent=rows.length+' TRADES';$('ledger-body').replaceChildren(...slice.map(t=>{const row=el('tr');row.append(el('td','',dayLabel(t.date)),el('td','',t.symbol||'—'));const side=el('td');side.append(el('span','side-badge',t.side||'—'));row.append(side,el('td','',t.setup||'—'),el('td','',money(t.entry_price,2)),el('td','',money(t.exit_price,2)),el('td','',t.exit_reason||'—'),el('td','numeric '+cls(t.net_pnl),money(t.net_pnl,2)));return row;}));
    if(!slice.length){const row=el('tr'),cell=el('td','','No executed trades match this search.');cell.colSpan=8;row.append(cell);$('ledger-body').append(row);}
    $('ledger-pagination').textContent=rows.length?`${state.page*size+1}–${Math.min((state.page+1)*size,rows.length)} of ${rows.length} executions`:'0 executions';$('page-prev').disabled=state.page===0;$('page-next').disabled=state.page>=pages-1;
  }
  function resetFilters() {stopReplay();state.query='';state.side='all';state.setup=null;state.selected=null;state.range='all';state.page=0;$('stock-search').value='';$('ledger-search').value='';$('side-filter').value='all';document.querySelectorAll('[data-range]').forEach(b=>{b.classList.toggle('active',b.dataset.range==='all');b.setAttribute('aria-pressed',String(b.dataset.range==='all'));});renderFlow(true);}
  $('run-select').addEventListener('change',e=>load(e.target.value));$('refresh').onclick=()=>load(state.data?.selected_run?.id);
  $('show-archives').onchange=()=>{const latest=state.data?.runs.find(run=>run.strategy===state.data.selected_run?.strategy);if(!$('show-archives').checked&&latest?.id!==state.data?.selected_run?.id)load(latest.id);else if(state.data)renderRunControls();};
  $('summary-scope').onchange=e=>{state.scope=e.target.value;state.page=0;renderMetrics();renderLedger();};$('clear-global-filters').onclick=resetFilters;
  $('stock-search').addEventListener('input',e=>{state.query=e.target.value.trim().toLowerCase();$('ledger-search').value=e.target.value;state.selected=null;state.page=0;renderFlow();});
  $('side-filter').onchange=e=>{state.side=e.target.value;state.selected=null;renderFlow();};$('reset-filters').onclick=resetFilters;
  $('clear-setup').onclick=()=>{state.setup=null;renderFlow();};$('sort-stocks').onclick=()=>{state.reverse=!state.reverse;$('sort-stocks').textContent=state.reverse?'↑':'↓';renderCards();};
  document.querySelectorAll('[data-range]').forEach(button=>button.onclick=()=>{stopReplay();state.range=button.dataset.range;document.querySelectorAll('[data-range]').forEach(b=>{b.classList.toggle('active',b===button);b.setAttribute('aria-pressed',String(b===button));});renderFlow(true);});
  document.querySelectorAll('[data-view]').forEach(button=>button.onclick=()=>setView(button.dataset.view));
  $('replay-toggle').onclick=()=>{if(state.timer){stopReplay();return;}if(state.frame>=state.dates.length)state.frame=1;renderFlow();$('replay-toggle').textContent='Ⅱ';$('replay-toggle').setAttribute('aria-label','Pause historical sessions');state.timer=setInterval(()=>{if(state.frame>=state.dates.length){stopReplay();return;}state.frame++;renderFlow();},850);};
  $('replay-slider').oninput=e=>{stopReplay();state.frame=Number(e.target.value);renderFlow();};$('replay-end').onclick=()=>{stopReplay();state.frame=state.dates.length;renderFlow();};
  $('close-dialog').onclick=()=>$('stock-dialog').close();$('close-about').onclick=()=>$('about-dialog').close();$('about').onclick=()=>$('about-dialog').showModal();
  [$('stock-dialog'),$('about-dialog')].forEach(dialog=>dialog.addEventListener('click',event=>{const r=dialog.getBoundingClientRect();if(event.target===dialog&&(event.clientX<r.left||event.clientX>r.right||event.clientY<r.top||event.clientY>r.bottom))dialog.close();}));
  $('ledger-search').oninput=e=>{state.scope='filtered';$('summary-scope').value='filtered';state.query=e.target.value.trim().toLowerCase();$('stock-search').value=e.target.value;state.page=0;state.selected=null;renderFlow();};$('page-prev').onclick=()=>{state.page--;renderLedger();};$('page-next').onclick=()=>{state.page++;renderLedger();};
  $('export-csv').onclick=()=>{const keys=['date','symbol','side','setup','entry_price','exit_price','net_pnl','gross_pnl','cost','exit_reason'];const safe=value=>{let s=String(value??'');if(typeof value==='string'&&/^[=+\-@\t\r]/.test(s))s="'"+s;return '"'+s.replaceAll('"','""')+'"';};const csv=[keys.join(','),...ledgerRows().map(row=>keys.map(key=>safe(row[key])).join(','))].join('\r\n');const blob=new Blob(['\uFEFF'+csv],{type:'text/csv;charset=utf-8'});const url=URL.createObjectURL(blob),link=el('a');link.href=url;link.download='dashboard-flow-trades.csv';link.click();setTimeout(()=>URL.revokeObjectURL(url),5000);};
  document.addEventListener('keydown',event=>{if(event.key==='/'&&state.view!=='compare'&&!['INPUT','SELECT','TEXTAREA'].includes(document.activeElement.tagName)&&!$('stock-dialog').open&&!$('about-dialog').open){event.preventDefault();if(state.view==='ledger')$('ledger-search').focus();else $('stock-search').focus();}});
  document.addEventListener('visibilitychange',()=>{if(document.hidden)stopReplay();});
  load();
})();
