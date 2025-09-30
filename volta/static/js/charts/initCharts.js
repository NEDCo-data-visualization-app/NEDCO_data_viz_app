import { drawLine } from './drawLine.js';
import { drawPie } from './drawPie.js';
import { drawBar } from './drawBar.js';
import { urlWithFilters, updateUrlQuery } from '../utils/url.js';
import { fetchJson } from '../utils/fetchJson.js';


// debounce helper
function debounce(fn, ms) {
  let t;
  return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

// sync dropdown button text
function updateMetricDropdownText() {
  const dropdownBtn = document.getElementById('metricDropdown');
  const checkboxes = document.querySelectorAll('.metric-checkbox');
  if (!dropdownBtn || !checkboxes.length) return;

  const checked = Array.from(checkboxes).filter(cb => cb.checked);
  if (checked.length === 0 && checkboxes[0]) checkboxes[0].checked = true;

  const labels = Array.from(checkboxes)
    .filter(cb => cb.checked)
    .map(cb => cb.nextElementSibling ? cb.nextElementSibling.textContent : cb.value);

  dropdownBtn.textContent = labels.join(', ');
}

export function initCharts() {
  const freqSelect = document.getElementById('freqSelect');
  const lineEl     = document.getElementById('lineChart');
  const barEl      = document.getElementById('barChart');
  const pieRow     = document.getElementById('pieChartsRow');
  const checkboxes = document.querySelectorAll('.metric-checkbox');

  if (!checkboxes.length || !freqSelect || !lineEl) return;

  // enforce max 2 metrics + sync text
  checkboxes.forEach(cb => {
    cb.addEventListener('change', () => {
      const checked = Array.from(checkboxes).filter(c => c.checked);
      if (checked.length > 2) {
        cb.checked = false;
        alert('You can select at most 2 metrics.');
      }
      updateMetricDropdownText();
    });
  });

  const refresh = debounce(async () => {
    const checkedBoxes = Array.from(checkboxes).filter(cb => cb.checked);
    if (checkedBoxes.length > 2) {
      checkedBoxes.slice(2).forEach(cb => cb.checked = false);
      alert('You can select at most 2 metrics.');
      updateMetricDropdownText();
      return;
    }

    const metrics = checkedBoxes.map(cb => cb.value);
    const freq    = freqSelect.value;
    const splitBy = document.querySelector('[data-splitby]:checked')?.value || null;

    updateUrlQuery(metrics.join(','), freq, splitBy);
    if (!metrics.length) return;

    // Line chart
    const series = await fetchJson(urlWithFilters('/chart-data', {
      metric: metrics.join(','),
      freq,
      split_by: splitBy
    }));
    drawLine(series, lineEl);

    // Pie charts
    if (pieRow) {
      pieRow.innerHTML = '';
      for (let i = 0; i < metrics.length; i++) {
        const chartId  = i === 0 ? 'pieChart' : 'pieChart2';
        const colClass = metrics.length === 1 ? 'col-12' : 'col-12 col-md-6';
        const chartData = await fetchJson(urlWithFilters('/pie-data', { metric: metrics[i] }));
        if (!chartData) continue;

        const metricLabel = chartData.metric_label || metrics[i];

        const col = document.createElement('div');
        col.className = colClass;
        col.innerHTML = `
          <div class="card border-0 shadow-sm mb-3">
            <div class="card-body">
              <h6 class="mb-1">Composition by segment${metrics.length > 1 ? ' (' + metricLabel + ')' : ''}</h6>
              <div class="text-muted small mb-2">Shares are based on the <strong>sum</strong> of the selected metric over current filters.</div>
              <div class="chart-box"><canvas id="${chartId}"></canvas></div>
              <button class="btn btn-outline-secondary btn-sm mt-2"
                      onclick="downloadChart('${chartId}','composition_${metricLabel}.png')">
                Download Chart
              </button>
            </div>
          </div>
        `;
        pieRow.appendChild(col);
        drawPie(chartData, document.getElementById(chartId));
      }
    }

    // Bar chart
    if (barEl && metrics.length) {
      const barSeriesList = await Promise.all(
        metrics.map(metric => fetchJson(urlWithFilters('/bar-data', { metric })))
      );
      drawBar(barSeriesList.filter(Boolean), barEl);
    }
  }, 100);

  checkboxes.forEach(cb => cb.addEventListener('change', refresh));
  freqSelect.addEventListener('change', refresh);
  document.querySelectorAll('[data-splitby]').forEach(cb => cb.addEventListener('change', refresh));

  updateMetricDropdownText();
  refresh();
}
