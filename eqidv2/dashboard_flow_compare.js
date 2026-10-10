/* Compare recorded strategy histories without manufacturing missing sessions. */
((host) => {
  'use strict';
  const FAMILIES = [
    {strategy:'V13-V10-G-3',label:'G-3',color:'var(--family-g3, #087553)'},
    {strategy:'V13-V10-G-2',label:'G-2',color:'var(--family-g2, #385f9b)'},
    {strategy:'V13-V10-G',label:'G',color:'var(--family-g, #a54661)'},
  ];
  const finite = value => typeof value === 'number' && Number.isFinite(value);
  const money = value => !finite(value) ? '—' : `${value < 0 ? '−' : ''}₹${Math.abs(value).toLocaleString('en-IN',{maximumFractionDigits:0})}`;
  const dateLabel = date => date ? new Date(date+'T12:00:00').toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric'}) : 'Unavailable';
  const validDate = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value+'T12:00:00Z')) && new Date(value+'T12:00:00Z').toISOString().slice(0,10) === value;
  const signedClass = value => finite(value) ? value < 0 ? 'negative' : value > 0 ? 'positive' : '' : '';
  const sum = (rows, field) => {
    if(!rows.length||!rows.every(row=>finite(row[field])))return null;
    const total=rows.reduce((value,row)=>value+row[field],0);return finite(total)?total:null;
  };

  function summarize(rows) {
    const net = sum(rows,'net_pnl'), trades = sum(rows,'trades'), wins = sum(rows,'wins');
    let total=0, peak=0, drawdown=0, complete=true;
    const points=rows.map(row=>{
      if(!finite(row.net_pnl))complete=false;
      if(!complete)return {date:row.date,value:null};
      total+=row.net_pnl;if(!finite(total)){complete=false;return {date:row.date,value:null};}
      peak=Math.max(peak,total);drawdown=Math.max(drawdown,peak-total);
      return {date:row.date,value:total};
    });
    const monthly=new Map();
    rows.forEach(row=>{const month=row.date.slice(0,7);if(!monthly.has(month))monthly.set(month,[]);monthly.get(month).push(row);});
    return {sessions:rows.length,net_pnl:net,gross_pnl:sum(rows,'gross_pnl'),cost:sum(rows,'cost'),trades,wins,
      win_rate_pct:finite(trades)&&trades>0&&finite(wins)?wins/trades*100:null,
      max_drawdown:rows.length&&complete?drawdown:null,points,
      monthly:[...monthly].map(([month,entries])=>({month,net_pnl:sum(entries,'net_pnl'),sessions:entries.length})),
      incomplete:rows.some(row=>['net_pnl','gross_pnl','cost','trades','wins'].some(field=>!finite(row[field])))};
  }

  function prepare(entries, mode='common') {
    const families=entries.map(entry=>{
      const raw=entry.data?.daily;
      const rows=Array.isArray(raw)?raw.filter(row=>validDate(row.date)).slice().sort((a,b)=>a.date.localeCompare(b.date)):[];
      const dates=rows.map(row=>row.date);
      const error=entry.error || (!Array.isArray(raw)?'Saved daily results are unavailable.':rows.length!==raw.length?'The session calendar contains an invalid date.':new Set(dates).size!==dates.length?'The session calendar contains duplicate dates.':!rows.length?'No recorded sessions are available.':null);
      return {...entry,rows:error?[]:rows,dates:error?[]:dates,error};
    });
    const union=[...new Set(families.flatMap(family=>family.dates))].sort();
    const allAvailable=families.length===FAMILIES.length&&families.every(family=>!family.error);
    const common=allAvailable?union.filter(date=>families.every(family=>family.dates.includes(date))):[];
    const selectedDates=new Set(common);
    return {mode,union,common,allAvailable,
      families:families.map(family=>{
        const rows=mode==='common'?family.rows.filter(row=>selectedDates.has(row.date)):family.rows;
        return {...family,selected:rows,missing:union.filter(date=>!family.dates.includes(date)),
          excluded:family.dates.filter(date=>!selectedDates.has(date)),summary:summarize(rows)};
      })};
  }

  function latestRuns(runs) {return FAMILIES.map(family=>({...family,run:runs.find(run=>run.strategy===family.strategy)||null}));}
  function element(tag,className,text) {const node=document.createElement(tag);if(className)node.className=className;if(text!=null)node.textContent=text;return node;}
  function svgElement(tag,attrs,text) {const node=document.createElementNS('http://www.w3.org/2000/svg',tag);Object.entries(attrs||{}).forEach(([name,value])=>node.setAttribute(name,value));if(text!=null)node.textContent=text;return node;}
  function legend(families) {const node=element('div','compare-legend');families.forEach(family=>{const item=element('span','compare-legend-item',family.label);item.style.setProperty('--family-color',family.color);node.append(item);});return node;}

  function chart(model) {
    const container=element('div','compare-chart');
    const dates=model.mode==='common'?model.common:model.union;
    const allPoints=model.families.flatMap(family=>family.summary.points).filter(point=>finite(point.value));
    if(!dates.length||!allPoints.length){container.append(element('div','empty-state','A comparison chart requires recorded session results.'));return container;}
    const width=Math.max(640,Math.min(1800,Math.round((host.document?.getElementById('compare-view')?.clientWidth||1050)-40)));
    const height=320,left=92,right=28,top=22,bottom=48,plotWidth=width-left-right,plotHeight=height-top-bottom;
    const low=Math.min(0,...allPoints.map(point=>point.value)),high=Math.max(0,...allPoints.map(point=>point.value));
    const padding=(high-low)*.08||100;
    const min=low-padding,max=high+padding;
    const y=value=>top+(max-value)/(max-min)*plotHeight;
    const x=index=>left+(index+1)/dates.length*plotWidth;
    const svg=svgElement('svg',{viewBox:`0 0 ${width} ${height}`,role:'img','aria-label':`Cumulative net profit and loss in rupees, ${model.mode==='common'?'common recorded sessions':'each version’s full recorded history'}`});
    svg.style.width='100%';svg.style.minWidth='640px';svg.style.height='320px';
    svg.append(svgElement('title',{},'Cumulative net P&L by strategy version'));
    svg.append(svgElement('desc',{},'Each line starts from zero and sums saved daily net results. Missing sessions and unknown values are not filled.'));
    for(let tick=0;tick<=4;tick++){
      const value=min+(max-min)*tick/4,py=y(value);
      svg.append(svgElement('line',{x1:left,y1:py,x2:width-right,y2:py,class:'compare-grid-line'}),svgElement('text',{x:left-12,y:py+4,'text-anchor':'end',class:'compare-axis-label'},money(value)));
    }
    svg.append(svgElement('line',{x1:left,y1:y(0),x2:width-right,y2:y(0),class:'compare-zero-line'}));
    const dateTicks=[...new Set([0,Math.floor((dates.length-1)/2),dates.length-1])];
    dateTicks.forEach(index=>svg.append(svgElement('text',{x:x(index),y:height-15,'text-anchor':index===dates.length-1?'end':index===0?'start':'middle',class:'compare-axis-label'},dateLabel(dates[index]))));
    model.families.forEach(family=>{
      const points=new Map(family.summary.points.map(point=>[point.date,point.value]));
      let path='',connected=false;
      dates.forEach((date,index)=>{
        const value=points.get(date);
        if(!finite(value)){connected=false;return;}
        if(index===0)path=`M${left},${y(0)}L${x(index)},${y(value)}`;
        else path+=`${connected?'L':'M'}${x(index)},${y(value)}`;
        connected=true;
        const dot=svgElement('circle',{cx:x(index),cy:y(value),r:3,fill:family.color,'fill-opacity':'.65'});
        dot.append(svgElement('title',{},`${family.label} · ${dateLabel(date)} · ${money(value)} cumulative net`));svg.append(dot);
      });
      const line=svgElement('path',{d:path,fill:'none',stroke:family.color,'stroke-width':2.5,'stroke-linejoin':'round','stroke-linecap':'round'});
      line.append(svgElement('title',{},`${family.label}: ${money(family.summary.net_pnl)} recorded net P&L`));svg.append(line);
    });
    container.append(svg);return container;
  }

  function metricTable(model) {
    const wrap=element('div','table-wrap'),table=element('table','compare-table'),head=element('thead'),headRow=element('tr');
    const metrics=[['net_pnl','Net P&L',money],['gross_pnl','Gross P&L',money],['cost','Costs',money],['trades','Trades',value=>finite(value)?value.toLocaleString('en-IN'):'—'],['win_rate_pct','Win rate',value=>finite(value)?value.toFixed(1)+'%':'—'],['max_drawdown','Day-end drawdown',money]];
    ['Version','Sessions',...metrics.map(metric=>metric[1])].forEach((label,index)=>headRow.append(element('th',index?'numeric':'',label)));head.append(headRow);table.append(head);
    const body=element('tbody');model.families.forEach(family=>{
      const row=element('tr'),name=element('td'),badge=element('span','compare-legend-item',family.label);badge.style.setProperty('--family-color',family.color);name.append(badge);row.append(name,element('td','numeric',family.error||(model.mode==='common'&&!model.allAvailable)?'—':String(family.summary.sessions)));
      metrics.forEach(([field,,format])=>row.append(element('td','numeric '+(field==='net_pnl'?signedClass(family.summary[field]):''),format(family.summary[field]))));body.append(row);
    });table.append(body);wrap.append(table);return wrap;
  }

  function monthlyTable(model) {
    const months=[...new Set(model.families.flatMap(family=>family.summary.monthly.map(month=>month.month)))].sort();
    if(!months.length)return element('div','empty-state','Monthly results will appear when recorded sessions are available.');
    const wrap=element('div','table-wrap'),table=element('table','compare-table'),head=element('thead'),tr=element('tr');tr.append(element('th','','Month'));
    model.families.forEach(family=>tr.append(element('th','numeric',family.label+' net P&L')));head.append(tr);table.append(head);
    const body=element('tbody');months.forEach(month=>{
      const row=element('tr');row.append(element('td','',new Date(month+'-01T12:00:00').toLocaleDateString('en-GB',{month:'long',year:'numeric'})));
      model.families.forEach(family=>{const result=family.summary.monthly.find(item=>item.month===month);const cell=element('td','numeric '+signedClass(result?.net_pnl),money(result?.net_pnl));cell.title=result?`${result.sessions} recorded sessions`:'No recorded sessions for this month';row.append(cell);});body.append(row);
    });table.append(body);wrap.append(table);return wrap;
  }

  function draw(container,state) {
    const model=prepare(state.entries,state.mode);
    const header=element('div','compare-header'),heading=element('div');heading.append(element('h2','','Compare versions'),element('p','','Latest saved G-3, G-2 and G results, with the calendar made explicit.'));
    const controls=element('div','compare-controls'),label=element('label','','Session scope'),select=element('select');select.id='comparison-mode';label.htmlFor=select.id;
    [['common','Common recorded sessions'],['full','Full recorded history']].forEach(([value,text])=>{const option=element('option','',text);option.value=value;select.append(option);});select.value=state.mode;
    select.addEventListener('change',()=>{state.mode=select.value;draw(container,state);container.querySelector('#comparison-mode')?.focus();});
    const refresh=element('button','text-btn','Refresh comparison');refresh.type='button';refresh.onclick=()=>render(container,{runs:state.runs,api:state.api,force:true});controls.append(label,select,refresh);header.append(heading,controls);
    const coverage=element('div','compare-coverage');model.families.forEach(family=>{
      const card=element('div','compare-family-card');card.style.setProperty('--family-color',family.color);card.append(element('strong','compare-family-label',family.label+' · Latest'));
      if(family.error)card.append(element('span','compare-family-date','Unavailable'),element('p','compare-family-meta',family.error));
      else {card.append(element('span','compare-family-date','Through '+dateLabel(family.dates.at(-1))),element('p','compare-family-meta',`${family.dates.length} recorded sessions · ${dateLabel(family.dates[0])} onward`));const name=element('span','compare-family-meta',family.run?.kind==='daily'?'Daily replay':family.run?.kind==='full_history'?'Full backtest':'Backtest');name.title=family.run?.run_name||'';card.append(name);}
      coverage.append(card);
    });
    const notice=element('div','compare-notice');notice.setAttribute('role','status');
    if(!model.allAvailable&&state.mode==='common')notice.textContent='Common-session comparison is unavailable until all three versions have valid recorded calendars. Refresh to retry, or inspect available full histories.';
    else if(state.mode==='common')notice.textContent=model.common.length?`${model.common.length} sessions shared by all three versions · ${dateLabel(model.common[0])} – ${dateLabel(model.common.at(-1))}. Metrics and curves use only these dates and begin from zero.`:'These versions have no recorded sessions in common. Use Full recorded history to inspect each available version.';
    else notice.textContent='Each version uses its own recorded calendar. Totals may cover different sessions; lines break at missing dates. Missing dates are never treated as zero-profit days.';
    const exclusions=element('details','compare-exclusions'),excludedCount=model.union.filter(date=>!model.common.includes(date)).length;
    exclusions.append(element('summary','',model.allAvailable?`${excludedCount} date${excludedCount===1?'':'s'} outside the shared calendar · inspect coverage`:'Inspect available session coverage'));
    const list=element('ul');model.families.forEach(family=>{
      const missing=family.error?'calendar unavailable':family.missing.length?`not recorded: ${family.missing.map(dateLabel).join(', ')}`:'records every date in the combined calendar';
      const excluded=model.allAvailable?`; ${family.excluded.length} of its recorded sessions excluded in common mode`:'';
      list.append(element('li','',`${family.label}: ${missing}${excluded}.`));
    });exclusions.append(list);
    const summary=element('section','compare-panel');summary.append(element('h3','','Performance by version'),element('p','','All figures are in INR. Drawdown uses daily closing net P&L and a starting balance of zero.'),metricTable(model));
    const curve=element('section','compare-panel');curve.append(element('h3','','Cumulative net P&L'),legend(model.families),chart(model));
    const monthly=element('section','compare-panel');monthly.append(element('h3','','Monthly net results'),element('p','',state.mode==='common'?'Each column uses the same recorded sessions within each month.':'Each column includes only that version’s recorded sessions within the month.'),monthlyTable(model));
    const notes=element('p','compare-status','Comparison always uses the latest saved run in each version, independently of the Flow map’s run selector and filters. — means unavailable; it is not zero.');
    if(model.families.some(family=>family.summary.incomplete))notes.append(document.createTextNode(' Some recorded fields are unknown: affected totals are unavailable and cumulative lines stop at the first unknown net result.'));
    const warnings=model.families.flatMap(family=>(family.data?.warnings||[]).map(warning=>`${family.label}: ${warning}`));
    if(warnings.length)notes.append(document.createTextNode(' '+warnings.join(' ')));
    container.replaceChildren(header,coverage,notice,exclusions,summary,curve,monthly,notes);
    container.removeAttribute('aria-busy');
  }

  const states=new WeakMap();
  async function render(container,{runs,api,force=false}) {
    if(!container||typeof api!=='function')return;
    const selected=latestRuns(Array.isArray(runs)?runs:[]);
    const key=JSON.stringify(selected.map(family=>[family.strategy,family.run?.id||null]));
    let state=states.get(container);
    if(state&&state.key===key&&!force&&state.api===api&&state.entries){draw(container,state);return;}
    if(state?.controller)state.controller.abort();
    const controller=new AbortController();
    state={key,mode:state?.mode||'common',runs,api,controller,entries:null};states.set(container,state);
    container.setAttribute('aria-busy','true');const loading=element('div','empty-state','Reading the latest three saved histories…');loading.setAttribute('role','status');container.replaceChildren(loading);
    const timeout=setTimeout(()=>controller.abort(),20000);
    try {
      const entries=await Promise.all(selected.map(async family=>{
        if(!family.run)return {...family,error:'No saved run is available for this version.'};
        try {
          const response=await fetch(api('/api/dashboard-flow?run='+encodeURIComponent(family.run.id)),{cache:'no-store',signal:controller.signal});
          if(!response.ok)throw new Error(response.status===401?'Your dashboard session has expired. Reopen the dashboard.':'This saved history could not be read. Refresh to retry.');
          const data=await response.json();if(data.error)throw new Error(data.error);
          if(data.selected_run?.id!==family.run.id)throw new Error('The returned run does not match the requested version.');
          return {...family,data};
        } catch(error) {return {...family,error:error.name==='AbortError'?'Reading this history timed out. Refresh to retry.':error.message};}
      }));
      if(states.get(container)!==state)return;
      state.entries=entries;draw(container,state);
    } finally {clearTimeout(timeout);if(states.get(container)===state)container.removeAttribute('aria-busy');}
  }

  host.FlowComparison={render};
  if(typeof module!=='undefined'&&module.exports)module.exports={summarize,prepare,latestRuns};
})(typeof window!=='undefined'?window:globalThis);
