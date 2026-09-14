// Shows "All" / "3 selected" / the single value on each filter dropdown button.
import { onFilterStateChange } from './liveFiltersController.js';

export function initFilterSummary() {
  const buttons = document.querySelectorAll('[data-filter-summary-for]');
  if (!buttons.length) return;

  onFilterStateChange((snapshot) => {
    buttons.forEach((button) => {
      const name = button.dataset.filterSummaryFor;
      const set = snapshot.selections.get(name);
      const values = set ? Array.from(set) : [];
      if (!values.length) button.textContent = 'All';
      else if (values.length === 1) button.textContent = values[0];
      else button.textContent = `${values.length} selected`;
      button.classList.toggle('fw-semibold', values.length > 0);
    });
  });
}
