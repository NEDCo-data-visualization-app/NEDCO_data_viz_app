const DEFAULT_LIMIT = 200;

function debounce(fn, delay = 250) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

function parseInitialOptions(raw) {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.map((value) => String(value)) : [];
  } catch (error) {
    console.warn('Unable to parse initial meter options', error);
    return [];
  }
}

export function initPredictionFilters() {
  const form = document.querySelector('[data-prediction-filters]');
  if (!form) return;

  const searchInput = form.querySelector('[data-prediction-meter-search]');
  const listContainer = form.querySelector('[data-prediction-meter-list]');
  const hiddenInput = form.querySelector('[data-prediction-meter-input]');
  const summary = form.querySelector('[data-prediction-meter-summary]');
  const summaryLabel = summary ? summary.querySelector('[data-prediction-meter-label]') : null;
  const resetButton = form.querySelector('[data-prediction-reset]');

  if (!searchInput || !listContainer || !hiddenInput) return;

  const endpoint = listContainer.dataset.optionsEndpoint || '/options/meterid';
  const limit = Number.parseInt(listContainer.dataset.limit || '', 10) || DEFAULT_LIMIT;

  let controller = null;
  let currentOptions = parseInitialOptions(listContainer.dataset.initialOptions);
  let currentSelection = String(hiddenInput.value || '');

  function showSummary(value) {
    if (!summary || !summaryLabel) return;
    if (value) {
      summary.classList.remove('d-none');
      summaryLabel.textContent = value;
    } else {
      summary.classList.add('d-none');
      summaryLabel.textContent = '';
    }
  }

  function renderEmpty(message = 'No meter IDs available') {
    listContainer.innerHTML = `<div class="text-muted small px-2 py-1">${message}</div>`;
  }

  function renderOptions(options) {
    const normalized = Array.from(new Set((options || []).map((value) => String(value))));
    currentOptions = normalized;

    if (!normalized.length) {
      renderEmpty();
      return;
    }

    const listGroup = document.createElement('div');
    listGroup.className = 'list-group list-group-flush';

    normalized.forEach((value) => {
      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'list-group-item list-group-item-action prediction-meter-option text-start';
      button.dataset.meterValue = value;
      button.textContent = value;
      listGroup.appendChild(button);
    });

    listContainer.innerHTML = '';
    listContainer.appendChild(listGroup);

    if (currentSelection) {
      applySelection(currentSelection, { broadcast: false });
    }
  }

  function applySelection(value, { broadcast = true } = {}) {
    currentSelection = value;
    hiddenInput.value = value;
    showSummary(value);
    searchInput.disabled = Boolean(value);

    const buttons = listContainer.querySelectorAll('[data-meter-value]');
    if (!buttons.length) return;

    buttons.forEach((button) => {
      const isMatch = button.dataset.meterValue === value;
      button.classList.toggle('active', isMatch);
      button.setAttribute('aria-pressed', isMatch ? 'true' : 'false');
      if (value) {
        button.classList.toggle('d-none', !isMatch);
      } else {
        button.classList.remove('d-none');
      }
    });

    if (!value && broadcast) {
      // When clearing the selection we should ensure the list shows all current options.
      renderOptions(currentOptions);
    }
  }

  function clearSelection() {
    currentSelection = '';
    hiddenInput.value = '';
    searchInput.disabled = false;
    showSummary('');
    renderOptions(currentOptions);
  }

  function handleOptionClick(event) {
    const target = event.target.closest('[data-meter-value]');
    if (!target) return;

    const { meterValue } = target.dataset;
    if (!meterValue) return;

    if (currentSelection === meterValue) return;
    applySelection(meterValue);
  }

  function setLoading() {
    listContainer.innerHTML = '<div class="text-muted small px-2 py-1">Loading…</div>';
  }

  function fetchOptions(query) {
    const payload = {
      q: query || '',
      limit,
    };

    if (controller) {
      controller.abort();
    }
    controller = new AbortController();

    setLoading();

    return fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    })
      .then((response) => {
        if (!response.ok) throw new Error(`Failed to load meter IDs: ${response.status}`);
        return response.json();
      })
      .then((data) => {
        const options = Array.isArray(data) ? data.map((value) => String(value)) : [];
        renderOptions(options);
        if (currentSelection && !options.includes(currentSelection)) {
          clearSelection();
        }
      })
      .catch((error) => {
        if (error.name === 'AbortError') return;
        console.error('Prediction meter options request failed', error);
        renderEmpty('Unable to load meter IDs');
      })
      .finally(() => {
        controller = null;
      });
  }

  const debouncedSearch = debounce((value) => {
    fetchOptions((value || '').trim());
  }, 300);

  searchInput.addEventListener('input', () => {
    if (currentSelection) return;
    debouncedSearch(searchInput.value);
  });

  listContainer.addEventListener('click', handleOptionClick);

  if (resetButton) {
    resetButton.addEventListener('click', (event) => {
      event.preventDefault();
      searchInput.value = '';
      clearSelection();
      fetchOptions('');
    });
  }

  renderOptions(currentOptions);

  if (!currentOptions.length) {
    fetchOptions('');
  }
}