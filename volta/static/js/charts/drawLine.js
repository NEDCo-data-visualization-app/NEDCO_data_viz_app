// Single-series line chart of totals over time.
import { palette, withAlpha, valueAxis, categoryAxis, replaceChart, setEmptyState, formatFull } from './theme.js';

const FREQ_LABEL = { D: 'day', W: 'week', M: 'month' };

export function drawLineTotals(seriesDict, canvasEl, { freq = 'M' } = {}) {
  if (!window.Chart || !canvasEl) return;

  const labels = seriesDict?.labels || [];
  const metrics = Object.keys(seriesDict?.values || {});
  if (!labels.length || !metrics.length) {
    setEmptyState(canvasEl, 'No data for the current filters.');
    return;
  }
  setEmptyState(canvasEl, null);

  const metric = metrics[0];
  const label = seriesDict.metric_labels?.[metric] || metric;
  const data = seriesDict.values[metric];

  replaceChart(canvasEl, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label,
          data,
          borderColor: palette.blue,
          backgroundColor: withAlpha(palette.blue, 0.08),
          pointBackgroundColor: palette.blue,
          fill: true,
          tension: 0.25,
          spanGaps: true,
        },
      ],
    },
    options: {
      interaction: { mode: 'index', intersect: false },
      scales: {
        x: categoryAxis(null),
        y: valueAxis(label),
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (items) => `${items[0].label} (${FREQ_LABEL[freq] || 'period'})`,
            label: (item) => ` ${label}: ${formatFull(item.parsed.y)}`,
          },
        },
      },
    },
  });
}

// Kept for compatibility with older callers.
export const drawLine = drawLineTotals;
