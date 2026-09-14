// Overview charts: one metric at a time (radio pills), three views of it.
import { drawLineTotals } from './drawLine.js';
import { drawPie } from './drawPie.js';
import { drawBar } from './drawBar.js';
import { applyChartTheme, setLoading } from './theme.js';
import { urlWithFilters, updateUrlQuery } from '../utils/url.js';
import { fetchJson } from '../utils/fetchJson.js';
import { debounce } from '../utils/debounce.js';

const FREQ_WORD = { D: 'day', W: 'week', M: 'month' };

export function initCharts() {
  const radios = document.querySelectorAll('.metric-checkbox');
  const freqSelect = document.getElementById('freqSelect');
  const lineEl = document.getElementById('lineChartTotal');
  const barEl = document.getElementById('barChart');
  const pieEl = document.getElementById('pieChart');
  if (!radios.length || !lineEl) return;

  applyChartTheme();

  const metricHidden = document.getElementById('metricHidden');
  const freqHidden = document.getElementById('freqHidden');
  const metricLabels = document.querySelectorAll('[data-chart-metric-label]');
  const freqLabels = document.querySelectorAll('[data-chart-freq-label]');
  let token = 0;

  const refresh = debounce(async () => {
    const current = ++token;
    const checked = document.querySelector('.metric-checkbox:checked') || radios[0];
    if (!checked) return;
    const metric = checked.value;
    const labelEl = document.querySelector(`label[for="${checked.id}"]`);
    const label = labelEl ? labelEl.textContent.trim() : metric;
    const freq = (freqSelect && freqSelect.value) || 'M';

    metricLabels.forEach((el) => { el.textContent = label; });
    freqLabels.forEach((el) => { el.textContent = FREQ_WORD[freq] || 'period'; });
    if (metricHidden) metricHidden.value = metric;
    if (freqHidden) freqHidden.value = freq;
    updateUrlQuery(metric, freq, null);

    setLoading([lineEl, barEl, pieEl], true);
    const [seriesData, barData, pieData] = await Promise.all([
      fetchJson(urlWithFilters('/chart-data', { metric, freq, agg: 'sum' })),
      barEl ? fetchJson(urlWithFilters('/bar-data', { metric })) : null,
      pieEl ? fetchJson(urlWithFilters('/pie-data', { metric })) : null,
    ]);
    if (current !== token) return;

    drawLineTotals(seriesData, lineEl, { freq });
    if (barEl) drawBar(barData, barEl);
    if (pieEl) drawPie(pieData, pieEl);
    setLoading([lineEl, barEl, pieEl], false);
  }, 80);

  radios.forEach((r) => r.addEventListener('change', refresh));
  if (freqSelect) freqSelect.addEventListener('change', refresh);
  refresh();
}
