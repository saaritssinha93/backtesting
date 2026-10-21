import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const baseDir = path.dirname(fileURLToPath(import.meta.url));
const data = JSON.parse(await fs.readFile(path.join(baseDir, "mapping_data.json"), "utf8"));
const workbook = Workbook.create();

const navy = "#17365D";
const blue = "#1F4E78";
const lightBlue = "#D9EAF7";
const paleBlue = "#EDF4FA";
const green = "#E2F0D9";
const amber = "#FFF2CC";
const dark = "#1F2937";
const grey = "#64748B";
const border = "#CBD5E1";

function titleBlock(sheet, titleRange, subtitleRange, title, subtitle) {
  sheet.mergeCells(titleRange);
  sheet.getRange(titleRange.split(":")[0]).values = [[title]];
  sheet.getRange(titleRange).format.fill = navy;
  sheet.getRange(titleRange).format.font = { bold: true, color: "#FFFFFF", size: 18 };
  sheet.getRange(titleRange).format.verticalAlignment = "center";
  sheet.getRange(titleRange).format.rowHeight = 30;

  sheet.mergeCells(subtitleRange);
  sheet.getRange(subtitleRange.split(":")[0]).values = [[subtitle]];
  sheet.getRange(subtitleRange).format.fill = paleBlue;
  sheet.getRange(subtitleRange).format.font = { color: dark, italic: true, size: 10 };
  sheet.getRange(subtitleRange).format.wrapText = true;
  sheet.getRange(subtitleRange).format.verticalAlignment = "center";
  sheet.getRange(subtitleRange).format.rowHeight = 28;
}

function styleHeader(range) {
  range.format.fill = blue;
  range.format.font = { bold: true, color: "#FFFFFF", size: 10 };
  range.format.verticalAlignment = "center";
  range.format.wrapText = true;
  range.format.rowHeight = 30;
  range.format.borders = { preset: "all", style: "thin", color: border };
}

function styleNote(sheet, rangeAddress, text, fill = "#F8FAFC") {
  sheet.mergeCells(rangeAddress);
  const cell = rangeAddress.split(":")[0];
  sheet.getRange(cell).values = [[text]];
  const range = sheet.getRange(rangeAddress);
  range.format.fill = fill;
  range.format.font = { color: grey, size: 9 };
  range.format.wrapText = true;
  range.format.verticalAlignment = "center";
  range.format.rowHeight = 24;
}

// Main stock-to-index mapping.
const mapping = workbook.worksheets.add("Stock mapping");
mapping.showGridLines = false;
mapping.tabColor = blue;
titleBlock(
  mapping,
  "A2:K2",
  "A3:K3",
  "NSE F&O stock-to-index mapping",
  `Current stock-futures universe retrieved ${data.summary.retrieved_on}; index memberships use the published NSE Indices constituent files.`
);

mapping.getRange("A5:K5").format.fill = lightBlue;
mapping.getRange("A5:K5").format.borders = { preset: "all", style: "thin", color: border };
mapping.getRange("A5:K5").format.verticalAlignment = "center";
mapping.getRange("A5:K5").format.rowHeight = 26;
mapping.getRange("A5").values = [["F&O stocks"]];
mapping.getRange("B5").formulas = [["=COUNTA(A10:A222)"]];
mapping.getRange("D5").values = [["Primary mapped"]];
mapping.getRange("E5").formulas = [["=COUNTIF(D10:D222,\"<>Unmapped\")"]];
mapping.getRange("G5").values = [["Specialised mapped"]];
mapping.getRange("H5").formulas = [["=COUNTIF(K10:K222,\"Broad + specialised\")"]];
mapping.getRange("J5").values = [["Membership rows"]];
mapping.getRange("K5").values = [[data.summary.membership_rows]];
for (const cell of ["A5", "D5", "G5", "J5"]) {
  mapping.getRange(cell).format.font = { bold: true, color: dark, size: 10 };
}
for (const cell of ["B5", "E5", "H5", "K5"]) {
  mapping.getRange(cell).format.font = { bold: true, color: navy, size: 15 };
  mapping.getRange(cell).format.horizontalAlignment = "center";
  mapping.getRange(cell).setNumberFormat("0");
}

styleNote(
  mapping,
  "A6:K6",
  `Primary size buckets — Nifty 50: ${data.summary.primary_counts["Nifty 50"]}; Next 50: ${data.summary.primary_counts["Nifty Next 50"]}; Midcap 150: ${data.summary.primary_counts["Nifty Midcap 150"]}; Smallcap 250: ${data.summary.primary_counts["Nifty Smallcap 250"]}; Microcap 250: ${data.summary.primary_counts["Nifty Microcap 250"]}.`,
  "#F1F5F9"
);
styleNote(
  mapping,
  "A7:K7",
  "For historical backtests, use effective-dated index membership. This workbook is a current snapshot and must not be projected backward. Thematic and strategy coverage is limited to the published files listed on the Sources sheet.",
  amber
);

const mappingHeaders = [
  "Symbol",
  "Company",
  "ISIN",
  "Primary size index",
  "Broad-market indices",
  "Sectoral indices",
  "Thematic indices",
  "Strategy / factor indices",
  "All mapped indices",
  "Index count",
  "Mapping status",
];
const mappingRows = data.stocks.map((row) => [
  row.symbol,
  row.company,
  row.isin,
  row.primary_size_index,
  row.broad_indices,
  row.sectoral_indices,
  row.thematic_indices,
  row.strategy_indices,
  row.all_mapped_indices,
  row.index_count,
  row.mapping_status,
]);
mapping.getRange("A9:K9").values = [mappingHeaders];
styleHeader(mapping.getRange("A9:K9"));
mapping.getRange(`A10:K${9 + mappingRows.length}`).values = mappingRows;
mapping.getRange(`A10:K${9 + mappingRows.length}`).format.font = { color: dark, size: 9 };
mapping.getRange(`A10:K${9 + mappingRows.length}`).format.verticalAlignment = "top";
mapping.getRange(`E10:I${9 + mappingRows.length}`).format.wrapText = true;
mapping.getRange(`J10:J${9 + mappingRows.length}`).setNumberFormat("0");
mapping.getRange(`K10:K${9 + mappingRows.length}`).format.fill = green;
const broadOnlyIndex = data.stocks.findIndex((row) => row.mapping_status === "Broad only");
if (broadOnlyIndex >= 0) {
  mapping.getRange(`K${10 + broadOnlyIndex}`).format.fill = amber;
}
const mappingTable = mapping.tables.add(`A9:K${9 + mappingRows.length}`, true, "StockIndexMappingTable");
mappingTable.style = "TableStyleMedium2";
mappingTable.showFilterButton = true;
mapping.freezePanes.freezeRows(9);
mapping.freezePanes.freezeColumns(3);

const mappingWidths = [14, 32, 18, 21, 42, 40, 48, 48, 70, 12, 22];
for (let i = 0; i < mappingWidths.length; i += 1) {
  mapping.getRangeByIndexes(0, i, 1, 1).format.columnWidth = mappingWidths[i];
}

// Long-form membership audit table.
const detail = workbook.worksheets.add("Membership detail");
detail.showGridLines = false;
detail.tabColor = "#2F75B5";
titleBlock(
  detail,
  "A2:G2",
  "A3:G3",
  "Membership detail",
  "One row per F&O stock × index membership, suitable for filtering, joins, and audit work."
);
styleNote(detail, "A5:G5", "The source pages state that displayed composition is as of the last trading day of the previous month.");
styleNote(detail, "A6:G6", "A stock may appear in many overlapping indices; Family separates Broad, Sectoral, Thematic, and Strategy memberships.", "#F1F5F9");
styleNote(detail, "A7:G7", "Source URL is the exact published constituent file used for the membership row.", amber);

const detailHeaders = ["Retrieved on", "Symbol", "Company", "ISIN", "Index", "Family", "Source URL"];
const detailRows = data.memberships.map((row) => [
  row.retrieved_on,
  row.symbol,
  row.company,
  row.isin,
  row.index,
  row.family,
  row.source_url,
]);
detail.getRange("A9:G9").values = [detailHeaders];
styleHeader(detail.getRange("A9:G9"));
detail.getRange(`A10:G${9 + detailRows.length}`).values = detailRows;
detail.getRange(`A10:G${9 + detailRows.length}`).format.font = { color: dark, size: 9 };
detail.getRange(`A10:G${9 + detailRows.length}`).format.verticalAlignment = "top";
detail.getRange(`E10:G${9 + detailRows.length}`).format.wrapText = true;
const detailTable = detail.tables.add(`A9:G${9 + detailRows.length}`, true, "MembershipDetailTable");
detailTable.style = "TableStyleMedium2";
detailTable.showFilterButton = true;
detail.freezePanes.freezeRows(9);
detail.freezePanes.freezeColumns(2);
const detailWidths = [15, 14, 34, 18, 44, 16, 72];
for (let i = 0; i < detailWidths.length; i += 1) {
  detail.getRangeByIndexes(0, i, 1, 1).format.columnWidth = detailWidths[i];
}

// Source catalogue and methodology notes.
const sources = workbook.worksheets.add("Sources & notes");
sources.showGridLines = false;
sources.tabColor = "#70AD47";
titleBlock(
  sources,
  "A2:I2",
  "A3:I3",
  "Sources and coverage notes",
  `${data.summary.sources_fetched} of ${data.summary.source_count} constituent files were fetched successfully on ${data.summary.retrieved_on}.`
);
styleNote(sources, "A5:I5", "Universe: 213 NSE stock-futures underlyings from the current near-month universe; six index futures are excluded.");
styleNote(sources, "A6:I6", "Coverage: all 22 current broad-market files and all 34 current sectoral files listed here; 34 thematic and 30 strategy/factor files are included as a selected public-file set.", "#F1F5F9");
styleNote(sources, "A7:I7", "Snapshot caution: index membership changes. Use effective-dated archives for historical research and licensed data or written consent for production refreshes.", amber);

const sourceHeaders = [
  "Family",
  "Index",
  "Coverage scope",
  "Status",
  "Constituents",
  "F&O overlap",
  "Retrieved on",
  "Source URL",
  "Error",
];
const sourceRows = data.sources.map((row) => [
  row.family,
  row.index,
  row.scope,
  row.status,
  row.constituents,
  row.fno_overlap,
  row.retrieved_on,
  row.source_url,
  row.error,
]);
sources.getRange("A9:I9").values = [sourceHeaders];
styleHeader(sources.getRange("A9:I9"));
sources.getRange(`A10:I${9 + sourceRows.length}`).values = sourceRows;
sources.getRange(`A10:I${9 + sourceRows.length}`).format.font = { color: dark, size: 9 };
sources.getRange(`A10:I${9 + sourceRows.length}`).format.verticalAlignment = "top";
sources.getRange(`C10:D${9 + sourceRows.length}`).format.wrapText = true;
sources.getRange(`H10:I${9 + sourceRows.length}`).format.wrapText = true;
sources.getRange(`E10:F${9 + sourceRows.length}`).setNumberFormat("0");
const sourceTable = sources.tables.add(`A9:I${9 + sourceRows.length}`, true, "IndexSourceTable");
sourceTable.style = "TableStyleMedium2";
sourceTable.showFilterButton = true;
sources.freezePanes.freezeRows(9);
sources.freezePanes.freezeColumns(2);
const sourceWidths = [14, 42, 34, 14, 14, 13, 15, 74, 42];
for (let i = 0; i < sourceWidths.length; i += 1) {
  sources.getRangeByIndexes(0, i, 1, 1).format.columnWidth = sourceWidths[i];
}

workbook.recalculate();

const mainCheck = await workbook.inspect({
  kind: "region",
  sheetId: "Stock mapping",
  range: "A1:K16",
  maxChars: 6000,
  tableMaxRows: 16,
  tableMaxCols: 11,
});
console.log(mainCheck.ndjson);
const detailCheck = await workbook.inspect({
  kind: "region",
  sheetId: "Membership detail",
  range: "A1:G15",
  maxChars: 4000,
  tableMaxRows: 15,
  tableMaxCols: 7,
});
console.log(detailCheck.ndjson);
const sourceCheck = await workbook.inspect({
  kind: "region",
  sheetId: "Sources & notes",
  range: "A1:I18",
  maxChars: 4500,
  tableMaxRows: 18,
  tableMaxCols: 9,
});
console.log(sourceCheck.ndjson);
const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: { useRegex: true, maxResults: 300 },
  summary: "final formula error scan",
});
console.log(errors.ndjson);

for (const [sheetName, range, fileName] of [
  ["Stock mapping", "A1:K28", "preview_stock_mapping.png"],
  ["Membership detail", "A1:G30", "preview_membership_detail.png"],
  ["Sources & notes", "A1:I30", "preview_sources_notes.png"],
]) {
  const preview = await workbook.render({ sheetName, range, scale: 1, format: "png" });
  await fs.writeFile(path.join(baseDir, fileName), new Uint8Array(await preview.arrayBuffer()));
}

const outputPath = path.join(baseDir, "nse_fno_stock_index_mapping_2026-10-21.xlsx");
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);
console.log(JSON.stringify({ outputPath, sheets: ["Stock mapping", "Membership detail", "Sources & notes"] }));
