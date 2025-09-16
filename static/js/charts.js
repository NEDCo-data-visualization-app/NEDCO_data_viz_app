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

  function drawLine(series, canvasEl) {
    if (!window.Chart || !canvasEl) return;
    const labels = Array.isArray(series?.labels) ? series.labels : [];
    const values = Array.isArray(series?.values) ? series.values : [];
    const label  = series?.metric_label || 'Metric';

    if (!labels.length || !values.length) {
      canvasEl.parentElement.innerHTML = '<div class="text-muted text-center py-4">No time-series data for current filters.</div>';
      return;
    }
    const ctx = canvasEl.getContext('2d');
    if (ctx._chart) ctx._chart.destroy();

    ctx._chart = new Chart(ctx, {
      type: 'line',
      data: { labels, datasets: [{ label, data: values }] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: { x: { ticks: { autoSkip: true, maxTicksLimit: 12 } } },
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
      data: {
        labels,
        datasets: [{ label, data: values }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' },
          tooltip: { callbacks: { label: (tt) => `${tt.label}: ${tt.formattedValue}` } }
        },
        cutout: '55%'
      }
    });
  }

  // Bar chart (sum by city)
  function drawBar(series, canvasEl) {
    if (!window.Chart || !canvasEl) return;
    const labels = Array.isArray(series?.labels) ? series.labels : [];
    const values = Array.isArray(series?.values) ? series.values : [];
    const label  = series?.metric_label || 'Metric';

    if (!labels.length || !values.length) {
      canvasEl.parentElement.innerHTML = '<div class="text-muted text-center py-4">No city data for current filters.</div>';
      return;
    }
    const ctx = canvasEl.getContext('2d');
    if (ctx._chart) ctx._chart.destroy();

    ctx._chart = new Chart(ctx, {
      type: 'bar',
      data: {
        labels,
        datasets: [{ label, data: values }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { ticks: { autoSkip: false, maxRotation: 60, minRotation: 30 } },
          y: { beginAtZero: true }
        },
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: (tt) => `${tt.label}: ${tt.formattedValue}` } }
        }
      }
    });
  }

  // Simple debounce
  function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }

  function initCharts() {
    const metricSelect = document.getElementById('metricSelect');
    const freqSelect   = document.getElementById('freqSelect');
    const lineEl       = document.getElementById('lineChart');
    const pieEl        = document.getElementById('pieChart');
    const barEl        = document.getElementById('barChart');

    if (!metricSelect || !freqSelect || !lineEl) return;

    const refresh = debounce(async () => {
      const metric = metricSelect.value;
      const freq   = freqSelect.value; // D | W | M
      updateUrlQuery(metric, freq);

      // Time series
      const tsUrl = urlWithFilters('/chart-data', { metric, freq });
      const ts    = await fetchJson(tsUrl);
      if (ts) drawLine(ts, lineEl);

      // Composition donut
      if (pieEl) {
        const pieUrl = urlWithFilters('/pie-data', { metric });
        const pie    = await fetchJson(pieUrl);
        if (pie) drawPie(pie, pieEl);
      }

      // Bar chart
      if (barEl) {
        const barUrl = urlWithFilters('/bar-data', { metric });
        const bar    = await fetchJson(barUrl);
        if (bar) drawBar(bar, barEl);
      }
    }, 100);

    metricSelect.addEventListener('change', refresh);
    freqSelect.addEventListener('change', refresh);
    const btn = document.getElementById('refreshCharts');
    if (btn) btn.addEventListener('click', refresh);

    // First render
    refresh();
  }

  // ---- NEW: meterid live search / reorder ----------------------------------
  function initMeteridSearch() {
    const input = document.getElementById('meteridSearch');
    const list  = document.getElementById('meteridList');
    if (!input || !list) return;

    function scoreItem(text, q) {
      if (!q) return 2;               // neutral if empty
      if (text.startsWith(q)) return 0; // best: starts with
      if (text.includes(q))  return 1;  // then: contains
      return 2;                         // otherwise: last
    }

    const reorder = () => {
      const q = input.value.trim().toLowerCase();
      const items = Array.from(list.querySelectorAll('.meterid-item'));
      items.sort((a, b) => {
        const ta = a.querySelector('.label-text').textContent.toLowerCase();
        const tb = b.querySelector('.label-text').textContent.toLowerCase();
        const sa = scoreItem(ta, q);
        const sb = scoreItem(tb, q);
        if (sa !== sb) return sa - sb;
        return ta.localeCompare(tb);
      });
      items.forEach(el => list.appendChild(el));
    };

    input.addEventListener('input', reorder);
  }
  // --------------------------------------------------------------------------

  document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    initMeteridSearch(); // NEW
  });
})();
