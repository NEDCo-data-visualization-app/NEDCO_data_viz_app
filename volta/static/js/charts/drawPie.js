// Doughnut: share of one metric by account type (few segments, direct percentage labels).
import { series as seriesColors, inkOn, palette, replaceChart, setEmptyState, formatFull } from './theme.js';

export function drawPie(data, canvasEl) {
  if (!window.Chart || !canvasEl) return;

  const labels = Array.isArray(data?.labels) ? data.labels : [];
  const values = Array.isArray(data?.values) ? data.values : [];
  if (!labels.length || !values.length || !values.some((v) => v > 0)) {
    setEmptyState(canvasEl, 'No composition data for the current filters.');
    return;
  }
  setEmptyState(canvasEl, null);

  const total = values.reduce((a, b) => a + (b || 0), 0);
  const colors = labels.map((_, i) => seriesColors[Math.min(i, seriesColors.length - 1)]);
  const label = data.metric_label || 'Share';

  replaceChart(canvasEl, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [
        {
          label,
          data: values,
          backgroundColor: colors,
          hoverBackgroundColor: colors,
          borderColor: palette.surface,
          borderWidth: 2, // the 2px surface gap between segments
        },
      ],
    },
    options: {
      cutout: '62%',
      plugins: {
        legend: { display: true, position: 'bottom' },
        tooltip: {
          callbacks: {
            label: (item) => {
              const v = item.parsed;
              const pct = total ? (100 * v) / total : 0;
              return ` ${formatFull(v)} (${pct.toFixed(1)}%)`;
            },
          },
        },
        datalabels: {
          display: (ctx) => total > 0 && (ctx.dataset.data[ctx.dataIndex] / total) >= 0.06,
          formatter: (value) => `${total ? ((100 * value) / total).toFixed(0) : 0}%`,
          color: (ctx) => inkOn[colors[ctx.dataIndex]] || '#ffffff',
          font: { weight: '600', size: 12 },
        },
      },
    },
    plugins: window.ChartDataLabels ? [window.ChartDataLabels] : [],
  });
}
