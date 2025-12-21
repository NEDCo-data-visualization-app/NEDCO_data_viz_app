// Chart rendering helpers for Volta dashboard
// Works with both split-by series and legacy values format
// Supports any number of metrics

function generateColor(i) {
  return `hsl(${i * 47 % 360}, 70%, 50%)`;
}

// Default colors for legacy two-metric charts
const LEGACY_DEFAULT_COLORS = ['#36A2EB', '#FF6384', '#4BC0C0'];

// ---------- Line chart (legacy + split-by) ----------
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

  // Split-by series format
  if (Array.isArray(seriesDict?.series)) {
    datasets = seriesDict.series.map((s, i) => ({
      label: s.label,
      data: s.values,
      borderColor: s.color || generateColor(i),
      backgroundColor: s.color || generateColor(i),
      yAxisID: 'y',
      tension: 0.45,
      fill: false,
      pointRadius: 2
    }));
  }

  // Legacy format (apply fixed default colors)
  if (!datasets.length && seriesDict?.values) {
    const metrics = Object.keys(seriesDict.values || {});
    datasets = metrics.map((metric, i) => ({
      label: seriesDict.metric_labels?.[metric] || metric,
      data: seriesDict.values[metric],
      borderColor: LEGACY_DEFAULT_COLORS[i] || generateColor(i),
      backgroundColor: LEGACY_DEFAULT_COLORS[i] || generateColor(i),
      yAxisID: i === 0 ? 'y' : 'y1',
      tension: 0.45,
      fill: false,
      pointRadius: 2
    }));
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
      title: { display: true, text: datasets[0]?.label || 'Value' }
    }
  };

  // Only add right axis if legacy two-metric mode is used
  if (!Array.isArray(seriesDict?.series) && datasets.length > 1) {
    const secondLabel = datasets[1]?.label || '';
    scales.y1 = {
      type: 'linear',
      position: 'right',
      title: { display: true, text: secondLabel },
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

// ---------- Line totals chart (totals per bucket) ----------
export function drawLineTotals(seriesDict, canvasEl) {
  if (!window.Chart || !canvasEl) return;

  const labels = seriesDict?.labels || [];
  if (!labels.length) {
    canvasEl.parentElement.innerHTML =
      '<div class="text-muted text-center py-4">No total data for current filters.</div>';
    return;
  }

  const ctx = canvasEl.getContext('2d');
  if (ctx._chart) ctx._chart.destroy();

  let datasets = [];

  // Split-by series format
  if (Array.isArray(seriesDict?.series)) {
    datasets = seriesDict.series.map((s, i) => ({
      label: s.label,
      data: s.values,
      borderColor: s.color || generateColor(i),
      backgroundColor: s.color || generateColor(i),
      yAxisID: 'y',
      tension: 0.45,
      fill: false,
      pointRadius: 2
    }));
  }

  // Legacy format
  if (!datasets.length && seriesDict?.values) {
    const metrics = Object.keys(seriesDict.values || {});
    datasets = metrics.map((metric, i) => ({
      label: seriesDict.metric_labels?.[metric] || metric,
      data: seriesDict.values[metric],
      borderColor: LEGACY_DEFAULT_COLORS[i] || generateColor(i),
      backgroundColor: LEGACY_DEFAULT_COLORS[i] || generateColor(i),
      yAxisID: i === 0 ? 'y' : 'y1',
      tension: 0.45,
      fill: false,
      pointRadius: 2
    }));
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
      title: { display: true, text: datasets[0]?.label || 'Total' }
    }
  };

  if (!Array.isArray(seriesDict?.series) && datasets.length > 1) {
    const secondLabel = datasets[1]?.label || '';
    scales.y1 = {
      type: 'linear',
      position: 'right',
      title: { display: true, text: secondLabel },
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
