// "Last 12 months" style shortcuts in the filter bar: set both dates and
// apply immediately.
import { setDateRange } from './liveFiltersController.js';

export function initPeriodPresets() {
  const buttons = document.querySelectorAll('.period-presets [data-period-start]');
  if (!buttons.length) return;
  buttons.forEach((btn) => {
    btn.addEventListener('click', () => {
      buttons.forEach((b) => b.classList.remove('active'));
      btn.classList.add('active');
      setDateRange(btn.dataset.periodStart || '', btn.dataset.periodEnd || '', { submit: true });
    });
  });
}
