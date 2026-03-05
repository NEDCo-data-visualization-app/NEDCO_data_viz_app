const state = {
  startDate: '',
  endDate: '',
  selections: new Map(),
  metrics: new Set(),  // Changed: store multiple metrics as a Set
  freq: null,
};

// Initialize metrics and freq from page
document.querySelectorAll('.metric-checkbox:checked').forEach(cb => {
  state.metrics.add(cb.value);
});
if (state.metrics.size === 0 && typeof defaultMetric !== 'undefined') {
  state.metrics.add(defaultMetric);
}
state.freq = document.getElementById('freqSelect')?.value || 'M';

const listeners = new Set();
const renderers = new Map();
const listContainers = new Map();
const latestOptions = new Map();

let formEl = null;
let optionsEndpoint = '';
let fetchController = null;
let hiddenSyncContainer = null;

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

// ---------------- Submit ----------------
function handleSubmit() {
  if (!formEl) return;

  const metricHidden = document.getElementById('metricHidden');
  const freqHidden   = document.getElementById('freqHidden');

  const startInput = formEl.querySelector('input[name="start_date"]');
  const endInput   = formEl.querySelector('input[name="end_date"]');
  if (startInput) startInput.value = state.startDate || '';
  if (endInput)   endInput.value   = state.endDate   || '';

  if (hiddenSyncContainer) hiddenSyncContainer.innerHTML = '';
  state.selections.forEach((set, key) => {
    for (const val of set) {
      const i = document.createElement('input');
      i.type  = 'hidden';
      i.name  = key;
      i.value = String(val);
      hiddenSyncContainer.appendChild(i);
    }
  });

  // Sync metric (comma-separated if multiple)
  const metricValue = state.metrics.size > 0 
    ? Array.from(state.metrics).join(',')
    : (typeof defaultMetric !== 'undefined' ? defaultMetric : 'ocd_energy');
    
  if (metricHidden) {
    metricHidden.value = metricValue;
    console.log('[SUBMIT] Setting metricHidden.value to:', metricHidden.value);
    console.log('[SUBMIT] metricHidden.name:', metricHidden.name);
  } else {
    console.error('[SUBMIT] metricHidden element not found!');
  }
  
  // Sync freq
  const freqValue = state.freq || 'M';
  if (freqHidden) {
    freqHidden.value = freqValue;
    console.log('[SUBMIT] Setting freqHidden.value to:', freqHidden.value);
    console.log('[SUBMIT] freqHidden.name:', freqHidden.name);
  } else {
    console.error('[SUBMIT] freqHidden element not found!');
  }

  // DEBUG: Log all form data before submit
  const formData = new FormData(formEl);
  console.log('[SUBMIT] Complete form data:');
  for (let [key, value] of formData.entries()) {
    console.log(`  ${key}: ${value}`);
  }
  
  console.log('[SUBMIT] State before submit:', {
    metrics: Array.from(state.metrics),
    freq: state.freq,
    selections: Object.fromEntries(
      Array.from(state.selections.entries()).map(([k, v]) => [k, Array.from(v)])
    )
  });

  formEl.submit();
}

// ---------------- Snapshot / State ----------------
function buildSnapshot() {
  return {
    startDate: state.startDate,
    endDate: state.endDate,
    selections: new Map(
      Array.from(state.selections.entries(), ([key, set]) => [key, new Set(set)])
    ),
    metrics: new Set(state.metrics),
    freq: state.freq,
  };
}

function emitState() {
  const snapshot = buildSnapshot();
  listeners.forEach(cb => {
    try { cb(snapshot); }
    catch (err) { console.error('Live filter listener error', err); }
  });
}

// ---------------- Renderer ----------------
function defaultRenderer(container, { name, options, selected }) {
  const selectedSet = selected instanceof Set ? selected : new Set(selected || []);
  const merged = Array.from(new Set([...(selectedSet || new Set()), ...(options || [])]));

  if (!merged.length) {
    container.innerHTML = '<div class="text-muted small px-2 py-1">No options available</div>';
    return;
  }

  container.innerHTML = merged.map(value => {
    const safeValue = escapeHtml(value);
    const isChecked = selectedSet.has(value);
    return `
      <label class="filter-item">
        <input type="checkbox" name="${escapeHtml(name)}" value="${safeValue}"${isChecked ? ' checked' : ''}>
        <span class="label-text">${safeValue}</span>
      </label>
    `;
  }).join('');
}

export function getFilterStateSnapshot() { return buildSnapshot(); }

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

// ---------------- Payload + Fetch ----------------
function buildRequestPayload() {
  const selections = {};
  state.selections.forEach((set, key) => { selections[key] = Array.from(set); });
  return {
    start_date: state.startDate || '',
    end_date: state.endDate || '',
    selections,
    metric: Array.from(state.metrics).join(',') || (typeof defaultMetric !== 'undefined' ? defaultMetric : ''),
    freq: state.freq || 'M',
  };
}

const debouncedFetch = debounce(() => {
  
}, 350);

function scheduleRefresh() { if (optionsEndpoint) debouncedFetch(); }

function handleCheckboxChange(event) {
  const input = event.target;
  if (!(input instanceof HTMLInputElement) || input.type !== 'checkbox') return;
  const { name, value } = input;
  if (!name) return;

  const set = ensureSelectionSet(name);
  const strValue = String(value);
  if (input.checked) set.add(strValue);
  else set.delete(strValue);

  emitState();
  scheduleRefresh();
}

function bindDateInput(input) {
  const apply = () => {
    const value = input.value || '';
    if (input.name === 'start_date' && state.startDate !== value) state.startDate = value;
    if (input.name === 'end_date' && state.endDate !== value) state.endDate = value;
    emitState(); scheduleRefresh();
  };
  const handler = debounce(apply, 200);
  input.addEventListener('input', handler);
  input.addEventListener('change', handler);
}

// 🔑 Set options + restore metric/freq after render
export function setFilterOptions(name, options, { render = true } = {}) {
  console.log("[DEBUG] setFilterOptions called", { name, options });
  if (!name) return;

  const normalized = Array.from(new Set(options.map(String)));
  latestOptions.set(name, normalized);

  const container = listContainers.get(name);
  if (container && render) {
    const renderer = renderers.get(name) || defaultRenderer;
    renderer(container, { name, options: normalized, selected: state.selections.get(name) || new Set() });

    // 🔹 Restore metric + freq immediately after rendering
    restoreMetricAndFreq();
  }

  console.log("[DEBUG] setFilterOptions completed for", name);
}

// 🔹 Restore metric/freq consistently
function restoreMetricAndFreq() {
  console.log('[RESTORE] Restoring metrics:', Array.from(state.metrics), 'freq:', state.freq);
  
  // Metric checkboxes - restore all selected metrics
  document.querySelectorAll('.metric-checkbox').forEach(cb => {
    cb.checked = state.metrics.has(cb.value);
  });

  // Frequency dropdown
  const freqSelect = document.getElementById('freqSelect');
  if (freqSelect && state.freq) {
    const hasOption = Array.from(freqSelect.options).some(opt => opt.value === state.freq);
    if (hasOption) {
      freqSelect.value = state.freq;
      console.log('[RESTORE] Set freq dropdown to:', state.freq);
    }
  }
}


// ---------------- Init ----------------
export function initLiveFilters() {
  formEl = document.querySelector('[data-filters-form]');
  if (!formEl) return;

  const metricHidden = document.getElementById('metricHidden');
  const freqHidden   = document.getElementById('freqHidden');
  optionsEndpoint = formEl.dataset.optionsEndpoint || '';

  hiddenSyncContainer = document.createElement('div');
  hiddenSyncContainer.style.display = 'none';
  hiddenSyncContainer.setAttribute('data-sync-hidden', 'true');
  formEl.appendChild(hiddenSyncContainer);

  // ---------------- Bind date inputs ----------------
  formEl.querySelectorAll('input[type="date"]').forEach(bindDateInput);

  // ---------------- Initialize filter checkboxes ----------------
  formEl.querySelectorAll('[data-filter-list][data-filter-name]').forEach(container => {
    const name = container.dataset.filterName;
    if (!name) return;

    listContainers.set(name, container);

    const initialSelected = parseJsonList(container.dataset.initialSelected);
    const set = ensureSelectionSet(name);
    initialSelected.forEach(v => set.add(v));

    container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
      if (cb.checked) set.add(cb.value);
    });

    if (name.toLowerCase() !== 'meterid') facetNames.push(name);
  });

  formEl.addEventListener('change', handleCheckboxChange);

  // ---------------- Metric & Frequency ----------------
  // Metric checkboxes
  document.querySelectorAll('.metric-checkbox').forEach(cb => {
    cb.addEventListener('change', () => {
      // Update state.metrics Set based on checked boxes
      const checkedBoxes = document.querySelectorAll('.metric-checkbox:checked');
      state.metrics.clear();
      checkedBoxes.forEach(checkbox => {
        state.metrics.add(checkbox.value);
      });
      
      // Update hidden input with comma-separated values
      const metricValue = Array.from(state.metrics).join(',');
      if (metricHidden) metricHidden.value = metricValue;
      
      console.log('[METRIC CHANGE] Updated metrics:', Array.from(state.metrics));
      
      debouncedFetch();  // refresh filter options with new metric
    });
  });

  // Frequency dropdown
  const freqSelect = document.getElementById('freqSelect');
  if (freqSelect) {
    freqSelect.addEventListener('change', () => {
      state.freq = freqSelect.value;
      if (freqHidden) freqHidden.value = state.freq;
      
      console.log('[FREQ CHANGE] Updated freq:', state.freq);

      debouncedFetch(); // refresh filter options with new freq
    });
  }

  // ---------------- Submit button ----------------
  const submitButton = document.querySelector('#submitButton');
  if (submitButton) {
    console.log('[INIT] Submit button found, adding click handler');
    submitButton.addEventListener('click', e => {
      e.preventDefault();
      console.log('[CLICK] Submit button clicked, calling handleSubmit');
      handleSubmit();
    });
  } else {
    console.error('[INIT] Submit button #submitButton not found!');
  }
  
  // Also intercept form submission as a backup
  if (formEl) {
    formEl.addEventListener('submit', e => {
      e.preventDefault();
      console.log('[SUBMIT] Form submit event, calling handleSubmit');
      handleSubmit();
    });
  }

  // ---------------- Initial fetch ----------------
  // Restore default metric/freq on first load
  restoreMetricAndFreq();
  debouncedFetch();
}


export function refreshFilterOptionsNow() {
  if (!optionsEndpoint) return;
  if (fetchController) fetchController.abort();
  debouncedFetch();
}

export { escapeHtml };