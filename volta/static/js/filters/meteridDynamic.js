import {
  escapeHtml,
  getFilterStateSnapshot,
  onFilterStateChange,
  registerFilterRenderer,
  setFilterOptions,
} from './liveFiltersController.js';

const LIMIT = 200;

function debounce(fn, delay = 250) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

function setsDiffer(a = new Set(), b = new Set()) {
  if (a.size !== b.size) return true;
  for (const value of a) {
    if (!b.has(value)) return true;
  }
  return false;
}

function relevantSelectionsChanged(prev, next) {
  if (prev.startDate !== next.startDate || prev.endDate !== next.endDate) {
    return true;
  }

  const keys = new Set([
    ...Array.from(prev.selections.keys()),
    ...Array.from(next.selections.keys()),
  ]);

  for (const key of keys) {
    if (key === 'meterid') continue;
    const prevSet = prev.selections.get(key) || new Set();
    const nextSet = next.selections.get(key) || new Set();
    if (setsDiffer(prevSet, nextSet)) return true;
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
    const selection =
      selectedSet instanceof Set
        ? new Set(Array.from(selectedSet, (value) => String(value)))
        : new Set();
    const merged = Array.from(
      new Set([
        ...Array.from(selection),
        ...(items || []).map((item) => String(item)),
      ])
    );

    if (!merged.length) {
      list.innerHTML = '<div class="text-muted small px-2 py-1">No meter IDs</div>';
      return;
    }

    const html = merged
      .map((value) => {
        const safeValue = escapeHtml(value);
        const checked = selection.has(value) ? ' checked' : '';
        return `
          <label class="filter-item meterid-item">
            <input type="checkbox" name="meterid" value="${safeValue}"${checked}>
            <span class="label-text">${safeValue}</span>
          </label>
        `;
      })
      .join('');

    list.innerHTML = html;
  };

  registerFilterRenderer('meterid', (_container, context) => {
    render(context.options, context.selected);
  });

  const buildPayload = (query) => {
    const payload = {
      q: query || '',
      limit: LIMIT,
      start_date: latestSnapshot.startDate || '',
      end_date: latestSnapshot.endDate || '',
      selections: {},
    };

    latestSnapshot.selections.forEach((set, key) => {
      if (key === 'meterid') return;
      payload.selections[key] = Array.from(set);
    });

    return payload;
  };

  const fetchOptions = (query) => {
    currentQuery = query || '';
    if (controller) {
      controller.abort();
    }
    controller = new AbortController();
    list.innerHTML = '<div class="text-muted small px-2 py-1">Loading…</div>';

    return fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildPayload(currentQuery)),
      signal: controller.signal,
    })
      .then((response) => {
        if (!response.ok) throw new Error(`Failed to load meter IDs: ${response.status}`);
        return response.json();
      })
      .then((data) => {
        const items = Array.isArray(data) ? data : [];
        setFilterOptions('meterid', items, { render: false });
        render(items, latestSnapshot.selections.get('meterid') || new Set());
      })
      .catch((err) => {
        if (err.name === 'AbortError') return;
        console.error('Failed to refresh meterid options', err);
        list.innerHTML = '<div class="text-muted small px-2 py-1">Unable to load meter IDs</div>';
      })
      .finally(() => {
        controller = null;
      });
  };

  const debouncedSearch = debounce((value) => {
    fetchOptions((value || '').trim());
  }, 250);

  input.addEventListener('input', () => {
    debouncedSearch(input.value);
  });

  onFilterStateChange((snapshot) => {
    const shouldUpdate = relevantSelectionsChanged(latestSnapshot, snapshot);
    latestSnapshot = snapshot;
    if (shouldUpdate) {
      fetchOptions(currentQuery);
    }
  });

  fetchOptions('');
}