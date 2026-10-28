import fs from 'node:fs/promises';
import path from 'node:path';
import { Workbook } from '@oai/artifact-tool';

const out = path.resolve(import.meta.dirname, '..');
const tables = JSON.parse(await fs.readFile(path.join(out, '.build', 'tables.json'), 'utf8'));
const quote = value => {
  const text = value == null ? '' : String(value);
  return /[",\r\n]/.test(text) ? '"' + text.replaceAll('"', '""') + '"' : text;
};

for (const [filename, rows] of Object.entries(tables)) {
  if (!rows.length) throw new Error('Empty table: ' + filename);
  const fields = Object.keys(rows[0]);
  const matrix = [fields, ...rows.map((row, i) => {
    if (JSON.stringify(Object.keys(row)) !== JSON.stringify(fields)) {
      throw new Error('Inconsistent schema: ' + filename + ' row ' + i);
    }
    return fields.map(field => row[field]);
  })];
  const workbook = Workbook.create();
  const sheet = workbook.worksheets.add('Slot results');
  sheet.getRangeByIndexes(0, 0, matrix.length, fields.length).values = matrix;
  workbook.recalculate();
  const actual = sheet.getUsedRange().values;
  if (JSON.stringify(actual) !== JSON.stringify(matrix)) throw new Error('Cell values changed: ' + filename);
  if (filename === 'slot_time_overall.csv') {
    const inspected = await workbook.inspect({kind:'region', sheetId:'Slot results',
      range:'A1:J5', tableMaxRows:5, tableMaxCols:10, maxChars:1800});
    console.log(inspected.ndjson);
    sheet.getRange('A1:J1').format.fill = '#EAF2F8';
    sheet.getRange('A1:J1').format.wrapText = true;
    sheet.getRange('A1:J1').format.rowHeight = 48;
    sheet.getRange('A1:J15').format.columnWidth = 19;
    const preview = await workbook.render({sheetName:'Slot results', range:'A1:J15', scale:1.25, format:'png'});
    await fs.writeFile(path.join(out,'.build','slot_preview.png'),new Uint8Array(await preview.arrayBuffer()));
  }
  const csv = actual.map(row => row.map(quote).join(',')).join('\n') + '\n';
  await fs.writeFile(path.join(out, filename), csv, 'utf8');
  console.log(filename, rows.length, 'data rows');
}
