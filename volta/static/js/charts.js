(function () {
  // Optional: nicer line defaults
  if (window.Chart) {
    Chart.defaults.datasets.line.tension = 0.45;
    Chart.defaults.datasets.line.cubicInterpolationMode = 'monotone';
    Chart.defaults.datasets.line.borderWidth = 2;
    Chart.defaults.elements.point.radius = 2;
  }

  function urlWithFilters(path, extra) {
    const qs = new URLSearchParams(window.location.search);
    for (const [k, v] of Object.entries(extra || {})) {
      if (v != null && v !== '') qs.set(k, v);
    }
    return `${path}?${qs.toString()}`;
  }

  function updateUrlQuery(metric, freq) {
    const qs = new URLSearchParams(window.location.search);
    if (metric) qs.set('metric', metric); else qs.delete('metric');
    if (freq)   qs.set('freq',  freq);    else qs.delete('freq');
    history.replaceState(null, '', `${location.pathname}?${qs.toString()}`);
  }

  async function fetchJson(url) {
    try {
      const resp = await fetch(url, { cache: 'no-store' });
      if (!resp.ok) return null;
      return await resp.json();
    } catch {
      return null;
    }
  }

  function drawLine(seriesDict, canvasEl) {
    if (!window.Chart || !canvasEl) return;

    const metrics = Object.keys(seriesDict.values || {});
    const labels = seriesDict.labels || [];

    if (!labels.length) {
      canvasEl.parentElement.innerHTML = '<div class="text-muted text-center py-4">No time-series data for current filters.</div>';
      return;
    }

    const ctx = canvasEl.getContext('2d');
    if (ctx._chart) ctx._chart.destroy();

    const datasets = metrics.map((metric, i) => {
      const color = i === 0 ? '#36A2EB' : '#FF6384';
      const yAxis = i === 0 ? 'y' : 'y1';
      return {
        label: seriesDict.metric_labels[metric] || metric,
        data: seriesDict.values[metric],
        borderColor: color,
        backgroundColor: color,
        yAxisID: yAxis,
        tension: 0.45,
        fill: false,
        pointRadius: 2
      };
    });

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
    if (datasets.length > 1) {
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
        scales: scales,
        plugins: { legend: { display: true }, tooltip: { mode: 'index', intersect: false } }
      }
    });
  }


  function drawPie(series, canvasEl) {
    if (!window.Chart || !canvasEl) return;
    const labels = Array.isArray(series?.labels) ? series.labels : [];
    const values = Array.isArray(series?.values) ? series.values : [];
    const label  = series?.metric_label || 'Metric';

    if (!labels.length || !values.length) {
      canvasEl.parentElement.innerHTML = '<div class="text-muted text-center py-4">No composition data for current filters.</div>';
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
              const total = context.chart.data.datasets[0].data.reduce((a, b) => a + b, 0);
              const percent = (value / total) * 100;
              return percent.toFixed(2) + '%';
            },
            color: '#fff',
            font: { weight: 'normal' }
          }
        },
        cutout: '55%'
      },
      plugins: [ChartDataLabels]
    });
  }

  // Bar chart (sum by city)
  function drawBar(seriesList, canvasEl) {
    if (!window.Chart || !canvasEl) return;
    const ctx = canvasEl.getContext('2d');
    if (ctx._chart) ctx._chart.destroy();

    if (!seriesList || !seriesList.length) {
      canvasEl.parentElement.innerHTML = '<div class="text-muted text-center py-4">No city data for current filters.</div>';
      return;
    }

    const labelsSet = new Set();
    seriesList.forEach(series => series.labels.forEach(l => labelsSet.add(l)));
    const labels = Array.from(labelsSet);

    const colors = ['#36A2EB', '#FF6384'];
    const datasets = seriesList.map((series, i) => {
      const yAxis = i === 0 ? 'y' : 'y1';
      const data = labels.map(l => {
        const idx = series.labels.indexOf(l);
        return idx >= 0 ? series.values[idx] : 0;
      });
      return {
        label: series.metric_label,
        data,
        backgroundColor: colors[i],
        yAxisID: yAxis
      };
    });

    const scales = {
      x: {
        title: { display: true, text: 'City'},
        ticks: { maxRotation: 45, minRotation: 0 } 
      },
      y: {
        type: 'linear',
        position: 'left',
        beginAtZero: true,
        title: { display: true, text: datasets[0]?.label || ''},
        ticks: { maxRotation: 0 }
      }
    };

    if (datasets.length > 1) {
      scales.y1 = {
        type: 'linear',
        position: 'right',
        beginAtZero: true,
        grid: { drawOnChartArea: false },
        title: { display: true, text: datasets[1]?.label || '' },
        ticks: { maxRotation: 0 }
      };
    }

    ctx._chart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: scales,
        plugins: {
          legend: { display: true },
          tooltip: { callbacks: { label: (tt) => `${tt.dataset.label} (${tt.label}): ${tt.formattedValue}` } }
        }
      }
    });
  }

  // Simple debounce
  function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }

  function initCharts() {
    const freqSelect = document.getElementById('freqSelect');
    const lineEl    = document.getElementById('lineChart');
    const barEl     = document.getElementById('barChart');
    const pieRow    = document.getElementById('pieChartsRow'); 
    const checkboxes = document.querySelectorAll('.metric-checkbox');

    if (!checkboxes.length || !freqSelect || !lineEl) return;

    checkboxes.forEach(cb => {
    cb.addEventListener('change', () => {
      const checked = Array.from(checkboxes).filter(c => c.checked);
      if (checked.length > 2) {
        cb.checked = false;
        alert("You can select at most 2 metrics.");
      }
      updateDropdownText(); 
    });
  });

    const refresh = debounce(async () => {
      const checkedBoxes = Array.from(checkboxes).filter(cb => cb.checked);
      if (checkedBoxes.length > 2) {
        checkedBoxes.slice(2).forEach(cb => cb.checked = false);
        alert("You can select at most 2 metrics.");
        updateDropdownText(); 
        return;
      }
      const metrics = Array.from(checkboxes)
        .filter(cb => cb.checked)
        .map(cb => cb.value);

      const freq = freqSelect.value;

      updateUrlQuery(metrics.join(','), freq);
      if (!metrics.length) return;

      const series = await fetchJson(urlWithFilters('/chart-data', { metric: metrics.join(','), freq }));
      drawLine(series, lineEl);

      if (pieRow) {
        pieRow.innerHTML = ''; 
        for (let i = 0; i < metrics.length; i++) {
          const chartId = i === 0 ? 'pieChart' : 'pieChart2';
          const colClass = metrics.length === 1 ? 'col-12' : 'col-12 col-md-6';
          const chartData = await fetchJson(urlWithFilters('/pie-data', { metric: metrics[i] }));
          if (!chartData) continue;

          const metricLabel = chartData.metric_label || metrics[i];

          const col = document.createElement('div');
          col.className = colClass;
          col.innerHTML = `
            <div class="card border-0 shadow-sm mb-3">
              <div class="card-body">
                <h6 class="mb-1">Composition by segment${metrics.length > 1 ? ' (' + metricLabel + ')' : ''}</h6>
                <div class="text-muted small mb-2">Shares are based on the <strong>sum</strong> of the selected metric over current filters.</div>
                <div class="chart-box"><canvas id="${chartId}"></canvas></div>
                <button class="btn btn-outline-secondary btn-sm mt-2"
                        onclick="downloadChart('${chartId}','composition_${metricLabel}.png')">
                  Download Chart
                </button>
              </div>
            </div>
          `;
          pieRow.appendChild(col);
          drawPie(chartData, document.getElementById(chartId));
        }
      }
      if (barEl && metrics.length) {
        const barSeriesList = await Promise.all(
          metrics.map(metric => fetchJson(urlWithFilters('/bar-data', { metric })))
        );
        drawBar(barSeriesList.filter(Boolean), barEl);
      }
    }, 100);

    checkboxes.forEach(cb => cb.addEventListener('change', refresh));
    freqSelect.addEventListener('change', refresh);

    const btn = document.getElementById('refreshCharts');
    if (btn) btn.addEventListener('click', refresh);

    refresh(); 
  }


  // ---- NEW: generic live search / reorder with "pin selected" --------------
  function initFilterSearch() {
    const inputs = document.querySelectorAll('.filter-search-input[data-filter-target]');
    if (!inputs.length) return;

    function scoreItem(text, q) {
      if (!q) return 2;                 // neutral if empty
      if (text.startsWith(q)) return 0; // best: starts with
      if (text.includes(q))  return 1;  // then: contains
      return 2;                         // otherwise: last
    }

    function reorder(input, list) {
      if (!list) return;
      const q = input.value.trim().toLowerCase();
      const items = Array.from(list.querySelectorAll('.filter-item'));
      if (!items.length) return;

      const selected = [];
      const unselected = [];
      for (const el of items) {
        const cb = el.querySelector('input[type="checkbox"]');
        if (cb && cb.checked) selected.push(el);
        else unselected.push(el);
      }

      const textFor = (el) => {
        const label = el.querySelector('.label-text');
        return (label ? label.textContent : el.textContent || '').trim().toLowerCase();
      };

      unselected.sort((a, b) => {
        const ta = textFor(a);
        const tb = textFor(b);
        const sa = scoreItem(ta, q);
        const sb = scoreItem(tb, q);
        if (sa !== sb) return sa - sb;
        return ta.localeCompare(tb);
      });

      for (const el of [...selected, ...unselected]) {
        list.appendChild(el);
      }
    }

    inputs.forEach((input) => {
      const selector = input.getAttribute('data-filter-target');
      if (!selector) return;
      const list = document.querySelector(selector);
      if (!list) return;

      const run = () => reorder(input, list);
      input.addEventListener('input', run);
      list.addEventListener('change', run);
      run();
    });
  }
  // --------------------------------------------------------------------------

  // ------- NEW: Export helpers (exposed globally for inline onclick) --------
  function downloadTableAsCSV(tableEl, filename) {
    const rows = tableEl.querySelectorAll('tr');
    const csv = [];
    rows.forEach(row => {
      const cells = row.querySelectorAll('th, td');
      const line = Array.from(cells).map(cell =>
        '"' + cell.innerText.replace(/"/g, '""') + '"'
      ).join(',');
      csv.push(line);
    });

    const blob = new Blob([csv.join('\n')], { type: 'text/csv' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename || 'table_export.csv';
    document.body.appendChild(link);
    link.click();
    URL.revokeObjectURL(link.href);
    document.body.removeChild(link);
  }

  // Expose for buttons in index.html:
  window.downloadChart = function (chartId, filename) {
    const canvas = document.getElementById(chartId);
    if (!canvas) return;
    const link = document.createElement('a');
    link.href = canvas.toDataURL('image/png', 1.0);
    link.download = filename || (chartId + '.png');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  window.downloadCurrentTable = function (filename) {
    const container = document.querySelector('.table-responsive');
    if (!container) return;
    const table = container.querySelector('table');
    if (!table) return;
    if (!table.id) table.id = 'dataTable';
    downloadTableAsCSV(table, filename || 'table_export.csv');
  };

  // NEW: download the entire filtered dataset from the server
  window.downloadFilteredCSV = function () {
    const url = urlWithFilters('/download-csv');
    window.location.assign(url);
  };
  // --------------------------------------------------------------------------

  document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    initFilterSearch(); // NEW
    const metricSelect = document.getElementById('metricSelect');
    if (metricSelect) {
      metricSelect.addEventListener('change', function() {
        const selected = Array.from(this.selectedOptions);
        if (selected.length > 2) {
          selected.slice(2).forEach(opt => opt.selected = false);
          alert("You can select at most 2 metrics for the chart.");
        }
      });
    }

  });
})();
