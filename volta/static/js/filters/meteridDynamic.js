// Search-as-you-type meter list on the dashboard. Results respect the other
// active filters (dates, location, account type).
import { debounce } from '../utils/debounce.js';
import {
  escapeHtml,
  getFilterStateSnapshot,
  onFilterStateChange,
  registerFilterRenderer,
  setFilterOptions,
} from './liveFiltersController.js';

const LIMIT = 200;

function setsDiffer(a = new Set(), b = new Set()) {
  if (a.size !== b.size) return true;
  for (const value of a) if (!b.has(value)) return true;
  return false;
}

function relevantSelectionsChanged(prev, next) {
  if (prev.startDate !== next.startDate || prev.endDate !== next.endDate) return true;
  const keys = new Set([...prev.selections.keys(), ...next.selections.keys()]);
  for (const key of keys) {
    if (key === 'meterid') continue;
    if (setsDiffer(prev.selections.get(key), next.selections.get(key))) return true;
  }
  return false;
}

export function initMeteridDynamic() {
  const input = document.getElementById('meteridSearch');
  const list = document.getElementById('meteridList');
  if (!input || !list) return;

  const endpoint = list.dataset.optionsEndpoint || '/options/meterid';
  let latestSnapshot = getFilterStateSnapshot();
  let controller = null;
  let currentQuery = '';

  const render = (items, selectedSet) => {
    const selection = new Set(Array.from(selectedSet || [], (v) => String(v)));
    const merged = Array.from(new Set([...selection, ...(items || []).map(String)]));

    if (!merged.length) {
      list.innerHTML = '<div class="text-muted small px-2 py-1">No meter IDs</div>';
      return;
    }
    list.innerHTML = merged.map((value) => {
      const safeValue = escapeHtml(value);
      return `
        <label class="filter-item meterid-item">
          <input type="checkbox" name="meterid" value="${safeValue}"${selection.has(value) ? ' checked' : ''}>
          <span class="label-text">${safeValue}</span>
        </label>
      `;
    }).join('');
  };

  registerFilterRenderer('meterid', (_container, context) => render(context.options, context.selected));

  const buildPayload = (query) => {
    const payload = {
      q: query || '',
      limit: LIMIT,
      start_date: latestSnapshot.startDate || '',
      end_date: latestSnapshot.endDate || '',
      selections: {},
    };
    latestSnapshot.selections.forEach((set, key) => {
      if (key !== 'meterid') payload.selections[key] = Array.from(set);
    });
    return payload;
  };

  const fetchOptions = async (query) => {
    currentQuery = query || '';
    if (controller) controller.abort();
    controller = new AbortController();
    list.innerHTML = '<div class="text-muted small px-2 py-1">Loading…</div>';

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildPayload(currentQuery)),
        signal: controller.signal,
      });
      if (!response.ok) throw new Error(`Failed to load meter IDs: ${response.status}`);
      const data = await response.json();
      const items = Array.isArray(data) ? data : [];
      setFilterOptions('meterid', items, { render: false });
      render(items, latestSnapshot.selections.get('meterid') || new Set());
    } catch (err) {
      if (err.name === 'AbortError') return;
      console.error('Failed to refresh meterid options', err);
      list.innerHTML = '<div class="text-muted small px-2 py-1">Unable to load meter IDs</div>';
    } finally {
      controller = null;
    }
  };

  input.addEventListener('input', debounce(() => fetchOptions(input.value.trim()), 250));

  onFilterStateChange((snapshot) => {
    const shouldUpdate = relevantSelectionsChanged(latestSnapshot, snapshot);
    latestSnapshot = snapshot;
    if (shouldUpdate) fetchOptions(currentQuery);
  });

  fetchOptions('');
}
