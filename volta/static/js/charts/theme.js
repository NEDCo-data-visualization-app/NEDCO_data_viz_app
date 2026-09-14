// Chart theme shared by every chart: NEDCo palette, quiet chrome, number formats.
export const palette = {
  navy: '#0b2545',
  blue: '#0060a8',
  red: '#c8391f',
  sky: '#0098d8',
  gold: '#e0a000',
  ink: '#0b0b0b',
  ink2: '#52514e',
  muted: '#898781',
  grid: '#e1e0d9',
  axis: '#c3c2b7',
  surface: '#ffffff',
};

// Categorical slots in fixed order (validated for colour-vision safety on white).
export const series = [palette.blue, palette.red, palette.sky, palette.gold];

// Ink to use on top of a series fill (labels inside donut segments).
export const inkOn = { [palette.blue]: '#ffffff', [palette.red]: '#ffffff', [palette.sky]: '#0b0b0b', [palette.gold]: '#0b0b0b' };

export function withAlpha(hex, alpha) {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
}

const compact = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 });
const full = new Intl.NumberFormat('en', { maximumFractionDigits: 2 });
export const formatCompact = (v) => (Number.isFinite(v) ? compact.format(v) : '');
export const formatFull = (v) => (Number.isFinite(v) ? full.format(v) : '');

let applied = false;
export function applyChartTheme() {
  if (!window.Chart || applied) return;
  applied = true;
  const d = Chart.defaults;
  d.font.family = "system-ui, -apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif";
  d.font.size = 12;
  d.color = palette.ink2;
  d.borderColor = palette.grid;
  d.responsive = true;
  d.maintainAspectRatio = false;
  d.plugins.legend.position = 'bottom';
  d.plugins.legend.labels.usePointStyle = true;
  d.plugins.legend.labels.boxWidth = 8;
  d.plugins.legend.labels.boxHeight = 8;
  d.plugins.tooltip.backgroundColor = palette.navy;
  d.plugins.tooltip.titleColor = '#ffffff';
  d.plugins.tooltip.bodyColor = '#ffffff';
  d.plugins.tooltip.padding = 10;
  d.plugins.tooltip.cornerRadius = 6;
  d.plugins.tooltip.displayColors = true;
  d.plugins.tooltip.boxPadding = 4;
  d.elements.line.borderWidth = 2;
  d.elements.line.borderJoinStyle = 'round';
  d.elements.line.borderCapStyle = 'round';
  d.elements.point.radius = 0;
  d.elements.point.hoverRadius = 5;
  d.elements.point.hitRadius = 12;
  d.elements.point.borderWidth = 2;
  d.elements.point.borderColor = palette.surface;
  d.elements.bar.borderRadius = { topLeft: 4, topRight: 4 };
  d.elements.bar.borderSkipped = 'bottom';
  // The datalabels plugin is registered globally; keep it off unless a chart opts in.
  if (d.plugins.datalabels) d.plugins.datalabels.display = false;
}

export const hairlineGrid = { color: palette.grid, drawTicks: false, lineWidth: 1 };

export function valueAxis(title) {
  return {
    beginAtZero: true,
    grid: hairlineGrid,
    border: { display: false },
    ticks: { callback: (v) => formatCompact(v), maxTicksLimit: 6, color: palette.muted, padding: 6 },
    title: title ? { display: true, text: title, color: palette.ink2 } : { display: false },
  };
}

export function categoryAxis(title, extra = {}) {
  return {
    grid: { display: false },
    border: { color: palette.axis },
    ticks: { color: palette.muted, autoSkip: true, maxTicksLimit: 12, maxRotation: 0, ...extra },
    title: title ? { display: true, text: title, color: palette.ink2 } : { display: false },
  };
}

/** Replace any chart on the canvas; returns the new chart. */
export function replaceChart(canvasEl, config) {
  const existing = Chart.getChart(canvasEl);
  if (existing) existing.destroy();
  return new Chart(canvasEl.getContext('2d'), config);
}

/** Show a message in place of the chart (or clear it) without removing the canvas. */
export function setEmptyState(canvasEl, message) {
  const box = canvasEl.closest('.chart-box') || canvasEl.parentElement;
  let note = box.querySelector('.chart-empty');
  if (message) {
    const existing = Chart.getChart(canvasEl);
    if (existing) existing.destroy();
    if (!note) {
      note = document.createElement('div');
      note.className = 'chart-empty';
      box.appendChild(note);
    }
    note.textContent = message;
    canvasEl.hidden = true;
  } else {
    if (note) note.remove();
    canvasEl.hidden = false;
  }
}

export function setLoading(elements, isLoading) {
  elements.forEach((el) => {
    const box = el && (el.closest('.chart-box') || el);
    if (box) box.classList.toggle('is-loading', isLoading);
  });
}
