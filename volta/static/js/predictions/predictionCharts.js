const chartInstances = {};
const chartModes = {
  kwh: 'combined',
  payments: 'combined'
};
let storedData = null;

function destroyChart(key) {
  const chart = chartInstances[key];
  if (chart && typeof chart.destroy === 'function') {
    chart.destroy();
  }
  delete chartInstances[key];
}

function togglePlaceholder(key, show) {
  const el = document.querySelector(`[data-chart-placeholder="${key}"]`);
  if (!el) return;
  if (show) {
    el.classList.remove('d-none');
  } else {
    el.classList.add('d-none');
  }
}

function formatMonthLabel(isoMonth) {
  if (!isoMonth) return '';
  const date = new Date(isoMonth);
  if (Number.isNaN(date.getTime())) return isoMonth;
  return date.toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short'
  });
}

function buildRecordMap(rows) {
  const map = new Map();
  if (!Array.isArray(rows)) return map;
  rows.forEach((row) => {
    if (!row || !row.month) return;
    map.set(row.month, row);
  });
  return map;
}

function collectMonths(data) {
  const set = new Set();
  data.forEach((row) => {
    if (row && row.month) {
      set.add(row.month);
    }
  });
  return Array.from(set).sort();
}

function hasValues(datasets) {
  return datasets.some((dataset) =>
    Array.isArray(dataset.data) && dataset.data.some((value) => Number.isFinite(value))
  );
}

function mapMetricValues(labels, recordMap, metric) {
  return labels.map((month) => {
    const record = recordMap.get(month);
    if (!record) return null;
    const value = record[metric];
    const numeric = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  });
}

function filterEmptyDatasets(datasets) {
  return datasets.filter((dataset) =>
    Array.isArray(dataset.data) && dataset.data.some((value) => Number.isFinite(value))
  );
}

function applyChart(key, config) {
  const canvas = document.querySelector(`[data-chart-canvas="${key}"]`);
  if (!canvas || typeof window.Chart === 'undefined') {
    return;
  }

  const context = canvas.getContext('2d');
  let chart = chartInstances[key];
  if (chart) {
    chart.data.labels = config.labels;
    chart.data.datasets = config.datasets;
    chart.options.scales = config.scales;
    chart.update();
    return;
  }

  chart = new Chart(context, {
    type: 'line',
    data: {
      labels: config.labels,
      datasets: config.datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: config.scales,
      plugins: {
        legend: { display: true },
        tooltip: { mode: 'index', intersect: false }
      }
    }
  });
  chartInstances[key] = chart;
}

function buildKwhConfig(data, mode) {
  const forecastRows = Array.isArray(data?.forecast) ? data.forecast : [];
  const historicalRows = Array.isArray(data?.historical) ? data.historical : [];

  if (!forecastRows.length) {
    return null;
  }

  const forecastMap = buildRecordMap(forecastRows);
  const historicalMap = buildRecordMap(historicalRows);

  let monthKeys;
  if (mode === 'combined') {
    monthKeys = Array.from(new Set([...collectMonths(historicalRows), ...collectMonths(forecastRows)])).sort();
  } else {
    monthKeys = collectMonths(forecastRows);
  }

  const labels = monthKeys.map(formatMonthLabel);

  const datasets = [];

  if (mode === 'combined') {
    const historicalData = mapMetricValues(monthKeys, historicalMap, 'kwh');
    datasets.push({
      label: 'kWh (historical)',
      data: historicalData,
      borderColor: '#0d6efd',
      backgroundColor: '#0d6efd',
      borderWidth: 2,
      tension: 0.3,
      pointRadius: 2,
      spanGaps: false,
      fill: false
    });
  }

  const forecastData = mapMetricValues(monthKeys, forecastMap, 'kwh');
  datasets.push({
    label: mode === 'combined' ? 'kWh (forecast)' : 'kWh forecast',
    data: forecastData,
    borderColor: '#0d6efd',
    backgroundColor: '#0d6efd',
    borderWidth: 2,
    borderDash: [6, 3],
    tension: 0.3,
    pointRadius: 2,
    spanGaps: false,
    fill: false
  });

  const filtered = filterEmptyDatasets(datasets);
  if (!filtered.length) {
    return null;
  }

  return {
    labels,
    datasets: filtered,
    scales: {
      x: {
        title: { display: true, text: 'Month' },
        ticks: { maxTicksLimit: 12 }
      },
      y: {
        title: { display: true, text: 'kWh' },
        beginAtZero: true
      }
    }
  };
}

function buildPaymentsConfig(data, mode) {
  const forecastRows = Array.isArray(data?.forecast) ? data.forecast : [];
  const historicalRows = Array.isArray(data?.historical) ? data.historical : [];

  if (!forecastRows.length) {
    return null;
  }

  const forecastMap = buildRecordMap(forecastRows);
  const historicalMap = buildRecordMap(historicalRows);

  let monthKeys;
  if (mode === 'combined') {
    monthKeys = Array.from(new Set([...collectMonths(historicalRows), ...collectMonths(forecastRows)])).sort();
  } else {
    monthKeys = collectMonths(forecastRows);
  }

  const labels = monthKeys.map(formatMonthLabel);

  const datasets = [];

  if (mode === 'combined') {
    const paymoneyHist = mapMetricValues(monthKeys, historicalMap, 'paymoney');
    datasets.push({
      label: 'Paymoney (historical)',
      data: paymoneyHist,
      borderColor: '#0d6efd',
      backgroundColor: '#0d6efd',
      borderWidth: 2,
      tension: 0.3,
      pointRadius: 2,
      spanGaps: false,
      fill: false,
      yAxisID: 'paymoney'
    });

    const ghcHist = mapMetricValues(monthKeys, historicalMap, 'ghc');
    datasets.push({
      label: 'Cash Received (historical)',
      data: ghcHist,
      borderColor: '#198754',
      backgroundColor: '#198754',
      borderWidth: 2,
      tension: 0.3,
      pointRadius: 2,
      spanGaps: false,
      fill: false,
      yAxisID: 'ghc'
    });
  }

  const paymoneyForecast = mapMetricValues(monthKeys, forecastMap, 'paymoney');
  datasets.push({
    label: mode === 'combined' ? 'Paymoney (forecast)' : 'Paymoney forecast',
    data: paymoneyForecast,
    borderColor: '#0d6efd',
    backgroundColor: '#0d6efd',
    borderWidth: 2,
    borderDash: [6, 3],
    tension: 0.3,
    pointRadius: 2,
    spanGaps: false,
    fill: false,
    yAxisID: 'paymoney'
  });

  const ghcForecast = mapMetricValues(monthKeys, forecastMap, 'ghc');
  datasets.push({
    label: mode === 'combined' ? 'Cash Received (forecast)' : 'Cash Received forecast',
    data: ghcForecast,
    borderColor: '#198754',
    backgroundColor: '#198754',
    borderWidth: 2,
    borderDash: [6, 3],
    tension: 0.3,
    pointRadius: 2,
    spanGaps: false,
    fill: false,
    yAxisID: 'ghc'
  });

  const filtered = filterEmptyDatasets(datasets);
  if (!filtered.length) {
    return null;
  }

  return {
    labels,
    datasets: filtered,
    scales: {
      x: {
        title: { display: true, text: 'Month' },
        ticks: { maxTicksLimit: 12 }
      },
      paymoney: {
        type: 'linear',
        position: 'left',
        title: { display: true, text: 'Paymoney (GHC)' },
        beginAtZero: true
      },
      ghc: {
        type: 'linear',
        position: 'right',
        title: { display: true, text: 'Cash Received (GHC)' },
        grid: { drawOnChartArea: false },
        beginAtZero: true
      }
    }
  };
}

function renderChart(key) {
  const mode = chartModes[key] || 'combined';
  const data = storedData;

  if (!data || !Array.isArray(data.forecast) || !data.forecast.length) {
    destroyChart(key);
    togglePlaceholder(key, true);
    return;
  }

  const config = key === 'kwh' ? buildKwhConfig(data, mode) : buildPaymentsConfig(data, mode);

  if (!config || !config.labels.length || !hasValues(config.datasets)) {
    destroyChart(key);
    togglePlaceholder(key, true);
    return;
  }

  togglePlaceholder(key, false);
  applyChart(key, config);
}

export function initPredictionCharts() {
  document.querySelectorAll('[data-chart-mode-toggle]').forEach((group) => {
    const chartKey = group.getAttribute('data-chart-mode-toggle');
    if (!chartKey) return;

    group.querySelectorAll('input[type="radio"][data-chart-mode-option]').forEach((input) => {
      if (input.checked) {
        chartModes[chartKey] = input.value;
      }
      input.addEventListener('change', () => {
        if (!input.checked) return;
        chartModes[chartKey] = input.value;
        renderChart(chartKey);
      });
    });
  });

  renderChart('kwh');
  renderChart('payments');
}

export function updatePredictionCharts(nextData) {
  if (!nextData || !Array.isArray(nextData.forecast) || !nextData.forecast.length) {
    storedData = null;
    destroyChart('kwh');
    destroyChart('payments');
    togglePlaceholder('kwh', true);
    togglePlaceholder('payments', true);
    return;
  }

  storedData = {
    historical: Array.isArray(nextData.historical) ? nextData.historical : [],
    forecast: Array.isArray(nextData.forecast) ? nextData.forecast : []
  };

  renderChart('kwh');
  renderChart('payments');
}