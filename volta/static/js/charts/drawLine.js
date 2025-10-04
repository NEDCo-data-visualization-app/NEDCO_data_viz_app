// Renders the main time-series line chart.
// Accepts either:
//  - { labels: [...], series: [{label, values, color?}, ...] }  // split-by format
//  - { labels: [...], values: {metric: [...]}, metric_labels: {...} } // legacy
export function drawLine(seriesDict, canvasEl) {
  if (!window.Chart || !canvasEl) return;

  const labels = seriesDict?.labels || [];
  if (!labels.length) {
    canvasEl.parentElement.innerHTML =
      '<div class="text-muted text-center py-4">No time-series data for current filters.</div>';
    return;
  }

  const ctx = canvasEl.getContext('2d');
  if (ctx._chart) ctx._chart.destroy();

  let datasets = [];

  // New split-by format
  if (Array.isArray(seriesDict?.series)) {
    datasets = seriesDict.series.map((s, i) => {
      const color = s.color || `hsl(${i * 47}, 70%, 50%)`;
      return {
        label: s.label,
        data: s.values,
        borderColor: color,
        backgroundColor: color,
        yAxisID: 'y',
        tension: 0.45,
        fill: false,
        pointRadius: 2
      };
    });
  }

  // Legacy format (two metrics max)
  if (!datasets.length && seriesDict?.values) {
    const metrics = Object.keys(seriesDict.values || {});
    datasets = metrics.map((metric, i) => {
      let color;
      if (i===0){
        color = '#36A2EB'
      } else if (i === 1){
        color ='#FF6384';
      } else {
       color = '#4BC0C0'
      }
      const yAxis = i === 0 ? 'y' : 'y1';
      return {
        label: seriesDict.metric_labels?.[metric] || metric,
        data: seriesDict.values[metric],
        borderColor: color,
        backgroundColor: color,
        yAxisID: yAxis,
        tension: 0.45,
        fill: false,
        pointRadius: 2
      };
    });
  }

  const scales = {
    x: {
      type: 'category',
      position: 'bottom',
      title: { display: true, text: 'Charge Date' },
      ticks: { autoSkip: true, maxTicksLimit: 12 }
    },
    y: {
        type: 'linear',
        position: 'left',
        title: { display: true, text: datasets[0]?.label || '' }
      }
  };

  // Only add right axis if legacy two-metric mode is used
  if (!Array.isArray(seriesDict?.series) && datasets.length > 1) {
    scales.y1 = {
      type: 'linear',
      position: 'right',
      title: { display: true, text: datasets[1]?.label || '' },
      grid: { drawOnChartArea: false }
    };
  }

  ctx._chart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales,
      plugins: {
        legend: { display: true },
        tooltip: { mode: 'index', intersect: false }
      }
    }
  });
}
