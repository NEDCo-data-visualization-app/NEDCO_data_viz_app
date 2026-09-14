// Account page: this meter's monthly energy against the peer median, and
// monthly amount paid. Data is embedded in the page (no extra request).
import { palette, withAlpha, valueAxis, categoryAxis, replaceChart, setEmptyState, formatFull, applyChartTheme } from '../charts/theme.js';

export function initAccountCharts() {
  const dataEl = document.getElementById('accountSeries');
  const energyEl = document.getElementById('accountEnergyChart');
  const paidEl = document.getElementById('accountPaidChart');
  if (!dataEl || !energyEl || !window.Chart) return;

  let series;
  try { series = JSON.parse(dataEl.textContent); } catch { return; }
  applyChartTheme();

  const labels = series.labels || [];
  if (!labels.length) {
    setEmptyState(energyEl, 'No purchases in this period.');
    if (paidEl) setEmptyState(paidEl, 'No purchases in this period.');
    return;
  }

  const datasets = [
    {
      label: 'This meter',
      data: series.kwh,
      borderColor: palette.blue,
      backgroundColor: withAlpha(palette.blue, 0.08),
      pointBackgroundColor: palette.blue,
      fill: true,
      tension: 0.25,
    },
  ];
  if ((series.peer_kwh || []).some((v) => v != null)) {
    datasets.push({
      label: 'Median similar customer',
      data: series.peer_kwh,
      borderColor: palette.red,
      pointBackgroundColor: palette.red,
      borderDash: [6, 4],
      fill: false,
      tension: 0.25,
      spanGaps: true,
    });
  }

  replaceChart(energyEl, {
    type: 'line',
    data: { labels, datasets },
    options: {
      interaction: { mode: 'index', intersect: false },
      scales: { x: categoryAxis(null), y: valueAxis('kWh') },
      plugins: {
        legend: { display: datasets.length > 1 },
        tooltip: { callbacks: { label: (item) => ` ${item.dataset.label}: ${formatFull(item.parsed.y)} kWh` } },
      },
    },
  });

  if (paidEl) {
    replaceChart(paidEl, {
      type: 'bar',
      data: {
        labels,
        datasets: [{ label: 'Amount paid', data: series.paid, backgroundColor: palette.blue, maxBarThickness: 28 }],
      },
      options: {
        scales: { x: categoryAxis(null, { maxTicksLimit: 8 }), y: valueAxis('GH₵') },
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (item) => ` GH₵ ${formatFull(item.parsed.y)}` } },
        },
      },
    });
  }
}
