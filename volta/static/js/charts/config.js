// ---------- Chart.js defaults ----------
export function setupChartDefaults() {
  if (window.Chart) {
    Chart.defaults.datasets.line.tension = 0.45;
    Chart.defaults.datasets.line.cubicInterpolationMode = 'monotone';
    Chart.defaults.datasets.line.borderWidth = 2;
    Chart.defaults.elements.point.radius = 2;
  }
}
