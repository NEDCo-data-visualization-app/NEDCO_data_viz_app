import { urlWithFilters } from './url.js';

export function downloadTableAsCSV(tableEl, filename) {
  const rows = tableEl.querySelectorAll('tr');
  const csv = [];
  rows.forEach(row => {
    const cells = row.querySelectorAll('th, td');
    const line = Array.from(cells).map(cell =>
      '"' + cell.innerText.replace(/"/g, '""') + '"'
    ).join(',');
    csv.push(line);
  });

  const blob = new Blob([csv.join('\n')], { type: 'text/csv' });
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = filename || 'table_export.csv';
  document.body.appendChild(link);
  link.click();
  URL.revokeObjectURL(link.href);
  document.body.removeChild(link);
}

export function downloadChart(chartId, filename) {
  const canvas = document.getElementById(chartId);
  if (!canvas) return;
  const link = document.createElement('a');
  link.href = canvas.toDataURL('image/png', 1.0);
  link.download = filename || (chartId + '.png');
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

export function downloadCurrentTable(filename) {
  const container = document.querySelector('.table-responsive');
  if (!container) return;
  const table = container.querySelector('table');
  if (!table) return;
  if (!table.id) table.id = 'dataTable';
  downloadTableAsCSV(table, filename || 'table_export.csv');
}

export function downloadFilteredCSV() {
  const url = urlWithFilters('/download-csv');
  window.location.assign(url);
}
