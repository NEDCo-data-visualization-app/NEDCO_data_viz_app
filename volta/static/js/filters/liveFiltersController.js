const state = {
  startDate: '',
  endDate: '',
  selections: new Map(),
};

const listeners = new Set();
const renderers = new Map();
const listContainers = new Map();
const latestOptions = new Map();

let formEl = null;
let optionsEndpoint = '';
let fetchController = null;
let hiddenSyncContainer = null;

// facets to request from the server for options refresh (e.g., ["loc","res_mapped"])
let facetNames = [];

function debounce(fn, delay = 350) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function ensureSelectionSet(name) {
  if (!state.selections.has(name)) {
    state.selections.set(name, new Set());
  }
  return state.selections.get(name);
}

function parseJsonList(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch (err) {
    console.warn('Failed to parse data-initial-selected', err);
    return [];
  }
}

function buildSnapshot() {
  return {
    startDate: state.startDate,
    endDate: state.endDate,
    selections: new Map(
      Array.from(state.selections.entries(), ([key, set]) => [key, new Set(set)])
    ),
  };
}

function emitState() {
  const snapshot = buildSnapshot();
  listeners.forEach((cb) => {
    try {
      cb(snapshot);
    } catch (err) {
      console.error('Live filter listener error', err);
    }
  });
}

function defaultRenderer(container, { name, options, selected }) {
  const selectedSet = selected instanceof Set ? selected : new Set(selected || []);
  const merged = Array.from(new Set([...(selectedSet || new Set()), ...(options || [])]));

  if (!merged.length) {
    container.innerHTML = '<div class="text-muted small px-2 py-1">No options available</div>';
    return;
  }

  const html = merged
    .map((value) => {
      const safeValue = escapeHtml(value);
      const isChecked = selectedSet.has(value);
      const checkedAttr = isChecked ? ' checked' : '';
      return `
        <label class="filter-item">
          <input type="checkbox" name="${escapeHtml(name)}" value="${safeValue}"${checkedAttr}>
          <span class="label-text">${safeValue}</span>
        </label>
      `;
    })
    .join('');

  container.innerHTML = html;
}

export function getFilterStateSnapshot() {
  return buildSnapshot();
}

export function onFilterStateChange(cb) {
  if (typeof cb !== 'function') return () => {};
  listeners.add(cb);
  cb(buildSnapshot());
  return () => listeners.delete(cb);
}

export function registerFilterRenderer(name, renderer) {
  if (!name || typeof renderer !== 'function') return;
  renderers.set(name, renderer);
  const container = listContainers.get(name);
  if (container) {
    const options = latestOptions.get(name) || [];
    const selected = state.selections.get(name) || new Set();
    renderer(container, { name, options, selected });
  }
}

export function setFilterOptions(name, options, { render = true } = {}) {
  if (!name) return;
  const normalized = Array.from(new Set((options || []).map((item) => String(item))));
  latestOptions.set(name, normalized);
  const container = listContainers.get(name);
  if (!container) return;
  if (!render) return;
  const renderer = renderers.get(name) || defaultRenderer;
  renderer(container, {
    name,
    options: normalized,
    selected: state.selections.get(name) || new Set(),
  });
}

function buildRequestPayload() {
  const selections = {};
  state.selections.forEach((set, key) => {
    selections[key] = Array.from(set);
  });

  return {
    start_date: state.startDate || '',
    end_date: state.endDate || '',
    selections,
  };
}

const debouncedFetch = debounce(() => {
  if (!optionsEndpoint) return;

  if (fetchController) {
    fetchController.abort();
  }
  fetchController = new AbortController();

  const payload = { ...buildRequestPayload(), facets: facetNames };

  fetch(optionsEndpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
    signal: fetchController.signal,
  })
    .then((response) => {
      if (!response.ok) throw new Error(`Failed to load filter options: ${response.status}`);
      return response.json();
    })
    .then((data) => {
      const options = data && typeof data === 'object' ? data.options || {} : {};
      Object.entries(options).forEach(([name, values]) => {
        const container = listContainers.get(name);
        if (!container) return;
        const skipRender = container.dataset.dynamicList === 'true';
        setFilterOptions(name, values || [], { render: !skipRender });
      });
    })
    .catch((err) => {
      if (err.name === 'AbortError') return;
      console.error('Failed to refresh filter options', err);
    })
    .finally(() => {
      fetchController = null;
    });
}, 350);

function scheduleRefresh() {
  if (!optionsEndpoint) return;
  debouncedFetch();
}

function handleCheckboxChange(event) {
  const input = event.target;
  if (!(input instanceof HTMLInputElement) || input.type !== 'checkbox') return;
  const { name, value } = input;
  if (!name) return;

  const set = ensureSelectionSet(name);
  const strValue = String(value);
  const hadValue = set.has(strValue);
  if (input.checked && !hadValue) {
    set.add(strValue);
  } else if (!input.checked && hadValue) {
    set.delete(strValue);
  } else {
    return;
  }

  emitState();
  scheduleRefresh();
}

function bindDateInput(input) {
  const apply = () => {
    const name = input.name;
    const value = input.value || '';
    if (name === 'start_date') {
      if (state.startDate === value) return;
      state.startDate = value;
    } else if (name === 'end_date') {
      if (state.endDate === value) return;
      state.endDate = value;
    } else {
      return;
    }
    emitState();
    scheduleRefresh();
  };

  const handler = debounce(apply, 200);
  input.addEventListener('input', () => handler());
  input.addEventListener('change', () => handler());
}

export function initLiveFilters() {
  formEl = document.querySelector('[data-filters-form]');
  if (!formEl) return;

  optionsEndpoint = formEl.dataset.optionsEndpoint || '';

  // Hidden container to hold serialized selections on submit
  hiddenSyncContainer = document.createElement('div');
  hiddenSyncContainer.setAttribute('data-sync-hidden', 'true');
  hiddenSyncContainer.style.display = 'none';
  formEl.appendChild(hiddenSyncContainer);

  const dateInputs = formEl.querySelectorAll('input[type="date"][name]');
  dateInputs.forEach((input) => {
    if (input.name === 'start_date') {
      state.startDate = input.value || '';
    } else if (input.name === 'end_date') {
      state.endDate = input.value || '';
    }
    bindDateInput(input);
  });

  const filterContainers = formEl.querySelectorAll('[data-filter-list][data-filter-name]');
  const facetSet = new Set();
  filterContainers.forEach((container) => {
    const name = container.getAttribute('data-filter-name');
    if (!name) return;

    listContainers.set(name, container);

    const initialSelected = parseJsonList(container.getAttribute('data-initial-selected'));
    const set = ensureSelectionSet(name);
    initialSelected.forEach((value) => set.add(value));

    const checkboxes = container.querySelectorAll('input[type="checkbox"]');
    const options = new Set();
    checkboxes.forEach((checkbox) => {
      options.add(String(checkbox.value));
      if (checkbox.checked) {
        set.add(String(checkbox.value));
      }
    });

    if (options.size > 0) {
      latestOptions.set(name, Array.from(options));
    }

    // Collect facet names to request from the server for options.
    // Skip meterid here—it has a dedicated dynamic endpoint.
    if (name.toLowerCase() !== 'meterid') {
      facetSet.add(name);
    }
  });
  facetNames = Array.from(facetSet);

  formEl.addEventListener('change', handleCheckboxChange);

  emitState();

  // Ensure authoritative state is submitted even if dynamic lists re-render
  formEl.addEventListener('submit', () => {
    // Keep the visible date inputs in sync with state
    const startInput = formEl.querySelector('input[name="start_date"]');
    const endInput = formEl.querySelector('input[name="end_date"]');
    if (startInput) startInput.value = state.startDate || '';
    if (endInput) endInput.value = state.endDate || '';

    // Rebuild hidden inputs fresh each submit (for selections like meterid)
    hiddenSyncContainer.innerHTML = '';
    state.selections.forEach((set, key) => {
      for (const val of set) {
        const i = document.createElement('input');
        i.type = 'hidden';
        i.name = key; // e.g., "meterid"
        i.value = String(val);
        hiddenSyncContainer.appendChild(i);
      }
    });
  });
}

export function refreshFilterOptionsNow() {
  if (!optionsEndpoint) return;
  if (fetchController) {
    fetchController.abort();
  }
  debouncedFetch();
}

export { escapeHtml };
