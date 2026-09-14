// Keeps the dashboard filter state in sync with the DOM, refreshes facet
// options from the server as the user narrows the selection, and submits the
// form with the chosen metrics / granularity.
import { debounce } from '../utils/debounce.js';

const state = {
  startDate: '',
  endDate: '',
  selections: new Map(),
  metrics: new Set(),
  freq: 'M',
};

const listeners = new Set();
const renderers = new Map();
const listContainers = new Map();
const latestOptions = new Map();

let formEl = null;
let optionsEndpoint = '';
let fetchController = null;
let hiddenSyncContainer = null;
let facetNames = [];

export function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function ensureSelectionSet(name) {
  if (!state.selections.has(name)) state.selections.set(name, new Set());
  return state.selections.get(name);
}

function parseJsonList(value) {
  if (!value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map((item) => String(item)) : [];
  } catch {
    return [];
  }
}

function buildSnapshot() {
  return {
    startDate: state.startDate,
    endDate: state.endDate,
    selections: new Map(Array.from(state.selections.entries(), ([key, set]) => [key, new Set(set)])),
    metrics: new Set(state.metrics),
    freq: state.freq,
  };
}

function emitState() {
  const snapshot = buildSnapshot();
  listeners.forEach((cb) => {
    try { cb(snapshot); } catch (err) { console.error('Live filter listener error', err); }
  });
}

// ---------------- Submit ----------------
function handleSubmit() {
  if (!formEl) return;

  const startInput = formEl.querySelector('input[name="start_date"]');
  const endInput = formEl.querySelector('input[name="end_date"]');
  if (startInput) startInput.value = state.startDate || '';
  if (endInput) endInput.value = state.endDate || '';

  // Checkboxes that are not currently rendered (e.g. a meter that scrolled out
  // of the search results) still need to be submitted, so mirror the state
  // into hidden inputs and disable the live checkboxes to avoid duplicates.
  hiddenSyncContainer.innerHTML = '';
  formEl.querySelectorAll('[data-filter-list] input[type="checkbox"]').forEach((cb) => { cb.disabled = true; });
  state.selections.forEach((set, key) => {
    set.forEach((val) => {
      const input = document.createElement('input');
      input.type = 'hidden';
      input.name = key;
      input.value = String(val);
      hiddenSyncContainer.appendChild(input);
    });
  });

  const metricHidden = document.getElementById('metricHidden');
  const freqHidden = document.getElementById('freqHidden');
  if (metricHidden && state.metrics.size) metricHidden.value = Array.from(state.metrics).join(',');
  if (freqHidden) freqHidden.value = state.freq || 'M';

  formEl.submit();
}

// ---------------- Rendering ----------------
function defaultRenderer(container, { name, options, selected }) {
  const selectedSet = selected instanceof Set ? selected : new Set(selected || []);
  const merged = Array.from(new Set([...selectedSet, ...(options || [])]));

  if (!merged.length) {
    container.innerHTML = '<div class="text-muted small px-2 py-1">No options available</div>';
    return;
  }

  container.innerHTML = merged.map((value) => {
    const safeValue = escapeHtml(value);
    return `
      <label class="filter-item">
        <input type="checkbox" name="${escapeHtml(name)}" value="${safeValue}"${selectedSet.has(value) ? ' checked' : ''}>
        <span class="label-text">${safeValue}</span>
      </label>
    `;
  }).join('');
}

export function getFilterStateSnapshot() { return buildSnapshot(); }

// Set both dates programmatically (period shortcuts) and optionally apply.
export function setDateRange(start, end, { submit = false } = {}) {
  state.startDate = start || '';
  state.endDate = end || '';
  if (formEl) {
    const startInput = formEl.querySelector('input[name="start_date"]');
    const endInput = formEl.querySelector('input[name="end_date"]');
    if (startInput) startInput.value = state.startDate;
    if (endInput) endInput.value = state.endDate;
  }
  emitState();
  if (submit) handleSubmit();
  else scheduleRefresh();
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
    renderer(container, { name, options: latestOptions.get(name) || [], selected: state.selections.get(name) || new Set() });
  }
}

export function setFilterOptions(name, options, { render = true } = {}) {
  if (!name) return;
  const normalized = Array.from(new Set((options || []).map(String)));
  latestOptions.set(name, normalized);

  const container = listContainers.get(name);
  if (container && render) {
    const renderer = renderers.get(name) || defaultRenderer;
    renderer(container, { name, options: normalized, selected: state.selections.get(name) || new Set() });
  }
}

// ---------------- Server refresh ----------------
function buildRequestPayload() {
  const selections = {};
  state.selections.forEach((set, key) => { selections[key] = Array.from(set); });
  return {
    start_date: state.startDate || '',
    end_date: state.endDate || '',
    selections,
    facets: facetNames,
  };
}

async function fetchOptions() {
  if (!optionsEndpoint || !facetNames.length) return;
  if (fetchController) fetchController.abort();
  fetchController = new AbortController();

  try {
    const response = await fetch(optionsEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildRequestPayload()),
      signal: fetchController.signal,
    });
    if (!response.ok) return;
    const data = await response.json();
    const options = (data && data.options) || {};
    facetNames.forEach((name) => {
      if (Array.isArray(options[name])) setFilterOptions(name, options[name]);
    });
  } catch (err) {
    if (err.name !== 'AbortError') console.error('Failed to refresh filter options', err);
  } finally {
    fetchController = null;
  }
}

const scheduleRefresh = debounce(fetchOptions, 350);

export function refreshFilterOptionsNow() { fetchOptions(); }

// ---------------- DOM bindings ----------------
function handleCheckboxChange(event) {
  const input = event.target;
  if (!(input instanceof HTMLInputElement) || input.type !== 'checkbox' || !input.name) return;
  if (!listContainers.has(input.name)) return;

  const set = ensureSelectionSet(input.name);
  if (input.checked) set.add(String(input.value));
  else set.delete(String(input.value));

  emitState();
  scheduleRefresh();
}

function bindDateInput(input) {
  const apply = debounce(() => {
    const value = input.value || '';
    if (input.name === 'start_date') state.startDate = value;
    if (input.name === 'end_date') state.endDate = value;
    emitState();
    scheduleRefresh();
  }, 200);
  input.addEventListener('input', apply);
  input.addEventListener('change', apply);
}

export function initLiveFilters() {
  formEl = document.querySelector('[data-filters-form]');
  if (!formEl) return;

  optionsEndpoint = formEl.dataset.optionsEndpoint || '';

  hiddenSyncContainer = document.createElement('div');
  hiddenSyncContainer.hidden = true;
  formEl.appendChild(hiddenSyncContainer);

  formEl.querySelectorAll('input[type="date"]').forEach((input) => {
    if (input.name === 'start_date') state.startDate = input.value || '';
    if (input.name === 'end_date') state.endDate = input.value || '';
    bindDateInput(input);
  });

  formEl.querySelectorAll('[data-filter-list][data-filter-name]').forEach((container) => {
    const name = container.dataset.filterName;
    if (!name) return;
    listContainers.set(name, container);

    const set = ensureSelectionSet(name);
    parseJsonList(container.dataset.initialSelected).forEach((v) => set.add(v));
    container.querySelectorAll('input[type="checkbox"]:checked').forEach((cb) => set.add(cb.value));

    if (name.toLowerCase() !== 'meterid') facetNames.push(name);
  });

  formEl.addEventListener('change', handleCheckboxChange);

  // Metric / granularity live in the charts card, outside the form.
  const metricHidden = document.getElementById('metricHidden');
  const freqHidden = document.getElementById('freqHidden');
  const syncMetrics = () => {
    state.metrics.clear();
    document.querySelectorAll('.metric-checkbox:checked').forEach((cb) => state.metrics.add(cb.value));
    if (metricHidden && state.metrics.size) metricHidden.value = Array.from(state.metrics).join(',');
  };
  document.querySelectorAll('.metric-checkbox').forEach((cb) => cb.addEventListener('change', syncMetrics));
  syncMetrics();

  const freqSelect = document.getElementById('freqSelect');
  if (freqSelect) {
    state.freq = freqSelect.value || 'M';
    freqSelect.addEventListener('change', () => {
      state.freq = freqSelect.value;
      if (freqHidden) freqHidden.value = state.freq;
    });
  }

  formEl.addEventListener('submit', (e) => {
    e.preventDefault();
    handleSubmit();
  });

  emitState();
}
