import fs from 'node:fs/promises';
import path from 'node:path';
import { Workbook } from '@oai/artifact-tool';

const folder=path.resolve(import.meta.dirname,'..');
const source=JSON.parse(await fs.readFile(path.join(folder,'g3_reference.json'),'utf8'));
const encode=value=>{
  const text=value==null?'':String(value);
  return /[",\r\n]/.test(text)?'"'+text.replaceAll('"','""')+'"':text;
};
for(const name of ['daywise_all.csv','october_daywise_all.csv']){
  const target=path.join(folder,name);
  const workbook=await Workbook.fromCSV(await fs.readFile(target,'utf8'),{sheetName:'Comparison'});
  const sheet=workbook.worksheets.getItem('Comparison');
  const original=sheet.getUsedRange().values;
  if(original[0].includes(source.column))throw new Error('G3 column already present');
  const values=[[source.column],...original.slice(1).map(row=>{
    if(!Object.hasOwn(source.daywise_net_rs,row[0]))throw new Error('Missing verified G3 date '+row[0]);
    return [source.daywise_net_rs[row[0]]];
  })];
  sheet.getRangeByIndexes(0,original[0].length,original.length,1).values=values;
  workbook.recalculate();
  const result=sheet.getUsedRange().values;
  if(JSON.stringify(result.map(row=>row.slice(0,original[0].length)))!==JSON.stringify(original))throw new Error('Existing comparison changed');
  console.log((await workbook.inspect({kind:'region',sheetId:'Comparison',range:'H1:I4',maxChars:1600,tableMaxRows:4,tableMaxCols:2})).ndjson);
  await fs.writeFile(target,result.map(row=>row.map(encode).join(',')).join('\n')+'\n','utf8');
  // Rendering formats are a preview only: the requested deliverables stay CSV.
  if(name==='october_daywise_all.csv'){
    sheet.getUsedRange().format.columnWidth=22;
    sheet.getRange('A1:I1').format.wrapText=true;
    sheet.getRange('A1:I1').format.rowHeight=44;
    sheet.getRange('A1:I1').format.fill='#EAF2F8';
    sheet.getRange('I2:I17').format.numberFormat='#,##0.00;[Red](#,##0.00)';
    const preview=await workbook.render({sheetName:'Comparison',range:'A1:I17',scale:1.4,format:'png'});
    await fs.writeFile(path.join(folder,'.artifact_g3','comparison_preview.png'),new Uint8Array(await preview.arrayBuffer()));
  }
  console.log('ADDED VERIFIED G3 COLUMN',name,'rows',original.length-1);
}
