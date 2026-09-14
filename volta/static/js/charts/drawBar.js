// Horizontal bars: one metric by district, largest first.
import { palette, valueAxis, categoryAxis, replaceChart, setEmptyState, formatFull } from './theme.js';

export function drawBar(seriesList, canvasEl) {
  if (!window.Chart || !canvasEl) return;

  const series = Array.isArray(seriesList) ? seriesList[0] : null;
  if (!series || !series.labels?.length) {
    setEmptyState(canvasEl, 'No district data for the current filters.');
    return;
  }
  setEmptyState(canvasEl, null);

  const label = series.metric_label || 'Total';
  const horizontal = series.labels.length > 6;

  replaceChart(canvasEl, {
    type: 'bar',
    data: {
      labels: series.labels,
      datasets: [
        {
          label,
          data: series.values,
          backgroundColor: palette.blue,
          hoverBackgroundColor: palette.sky,
          maxBarThickness: 24,
          categoryPercentage: 0.7,
          barPercentage: 0.9,
          borderRadius: horizontal ? { topRight: 4, bottomRight: 4 } : { topLeft: 4, topRight: 4 },
          borderSkipped: horizontal ? 'left' : 'bottom',
        },
      ],
    },
    options: {
      indexAxis: horizontal ? 'y' : 'x',
      scales: horizontal
        ? { x: valueAxis(label), y: categoryAxis(null, { autoSkip: false, maxTicksLimit: 50 }) }
        : { x: categoryAxis(null, { autoSkip: false, maxTicksLimit: 50 }), y: valueAxis(label) },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (item) => ` ${label}: ${formatFull(horizontal ? item.parsed.x : item.parsed.y)}`,
          },
        },
      },
    },
  });
}
