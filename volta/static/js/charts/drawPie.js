// Renders the composition doughnut chart.
// Expects: { labels: [...], values: [...], metric_label: '...' }
export function drawPie(series, canvasEl) {
  if (!window.Chart || !canvasEl) return;

  const labels = Array.isArray(series?.labels) ? series.labels : [];
  const values = Array.isArray(series?.values) ? series.values : [];
  const label  = series?.metric_label || 'Metric';

  if (!labels.length || !values.length) {
    canvasEl.parentElement.innerHTML =
      '<div class="text-muted text-center py-4">No composition data for current filters.</div>';
    return;
  }

  const ctx = canvasEl.getContext('2d');
  if (ctx._chart) ctx._chart.destroy();

  ctx._chart = new Chart(ctx, {
    type: 'doughnut',
    data: { labels, datasets: [{ label, data: values }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'bottom' },
        tooltip: { callbacks: { label: (tt) => `${tt.label}: ${tt.formattedValue}` } },
        datalabels: {
          formatter: (value, context) => {
            const total = context.chart.data.datasets[0].data
              .reduce((a, b) => a + b, 0);
            const percent = total ? (value / total) * 100 : 0;
            return percent.toFixed(2) + '%';
          },
          color: '#fff',
          font: { weight: 'normal' }
        }
      },
      cutout: '55%'
    },
    // Requires ChartDataLabels loaded globally via <script> in your HTML
    plugins: [window.ChartDataLabels || {}]
  });
}
