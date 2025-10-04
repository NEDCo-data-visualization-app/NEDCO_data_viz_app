// Renders grouped bar chart for city or other segment comparisons.
// Expects seriesList like: [{ labels: [...], values: [...], metric_label: '...' }, ...]
export function drawBar(seriesList, canvasEl) {
  if (!window.Chart || !canvasEl) return;

  const ctx = canvasEl.getContext('2d');
  if (ctx._chart) ctx._chart.destroy();

  if (!seriesList || !seriesList.length) {
    canvasEl.parentElement.innerHTML =
      '<div class="text-muted text-center py-4">No city data for current filters.</div>';
    return;
  }

  const labelsSet = new Set();
  seriesList.forEach(series => (series.labels || []).forEach(l => labelsSet.add(l)));
  const labels = Array.from(labelsSet);

  const baseColors = ['#36A2EB', '#FF6384', '#4BC0C0', '#9966FF', '#FF9F40'];

  const datasets = seriesList.map((series, i) => {
    const data = labels.map(l => {
      const idx = (series.labels || []).indexOf(l);
      return idx >= 0 ? series.values[idx] : 0;
    });
    return {
      label: series.metric_label || `Series ${i + 1}`,
      data,
      backgroundColor: baseColors[i % baseColors.length],
      yAxisID: 'y'
    };
  });

  const scales = {
    x: {
      title: { display: true, text: 'City' },
      ticks: { maxRotation: 45, minRotation: 0 }
    },
    y: {
        type: 'linear',
        position: 'left',
        beginAtZero: true,
        ticks: { maxRotation: 0 }
      }
  };

  ctx._chart = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales,
      plugins: {
        legend: { display: true },
        tooltip: {
          callbacks: {
            label: (tt) => `${tt.dataset.label} (${tt.label}): ${tt.formattedValue}`
          }
        }
      }
    }
  });
}
