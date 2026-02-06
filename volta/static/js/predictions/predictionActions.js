import { updatePredictionCharts } from './predictionCharts.js';

const PREVIEW_LIMIT = 10;

export function initPredictionActions({ isPublic = false } = {}) {
  const form = document.querySelector('[data-prediction-filters]');
  if (!form) return;

  const predictButton = form.querySelector('[data-prediction-predict]');
  const predictAllButton = form.querySelector('[data-prediction-predict-all]');
  const meterInput = form.querySelector('[data-prediction-meter-input]');

  // Helper to get current filter selection
  const getSelectedFilters = () => {
    const filters = {};

    // Meter ID only if private
    if (!isPublic && meterInput && meterInput.value) {
      filters.meterid = meterInput.value;
    }

    // Utility / Location always
    const utilities = Array.from(form.querySelectorAll('[name="utility"]:checked'))
      .map(el => el.value);
    if (utilities.length) filters.utility = utilities;

    return filters;
  };

  // Update Predict All button label
  const updatePredictAllLabel = () => {
    const filters = getSelectedFilters();
    const hasFilter = Object.keys(filters).length > 0;
    predictAllButton.textContent = hasFilter ? 'Predict Filtered' : 'Predict All';
  };

  // --- Watch for changes on meter input ---
  if (!isPublic && meterInput) {
    meterInput.addEventListener('input', updatePredictAllLabel);
  }

  // --- Watch for changes on location checkboxes ---
  const locationCheckboxes = form.querySelectorAll('[name="utility"]');
  locationCheckboxes.forEach(cb => cb.addEventListener('change', updatePredictAllLabel));

  // Initialize label on load
  updatePredictAllLabel();

  // --- Predict / Predict All button handlers ---
  const predictEndpoint = form.dataset.predictionPredictEndpoint;
  const predictAllEndpoint = form.dataset.predictionPredictAllEndpoint;

  const setButtonLoading = (button, isLoading) => {
    if (!button) return;
    if (isLoading) {
      button.dataset.originalText = button.dataset.originalText || button.textContent;
      button.textContent = 'Loading…';
      button.disabled = true;
    } else {
      if (button.dataset.originalText) button.textContent = button.dataset.originalText;
      button.disabled = false;
    }
  };

  const statusEl = document.querySelector('[data-prediction-status]');
  const tableContainer = document.querySelector('[data-prediction-table]');
  const summaryEl = document.querySelector('[data-prediction-summary]');
  const downloadButton = document.querySelector('[data-prediction-download]');

  const setStatus = (type, message) => {
    if (!statusEl) return;
    statusEl.classList.remove('alert-info', 'alert-success', 'alert-danger');
    if (type) {
      statusEl.classList.add(`alert-${type}`);
      statusEl.classList.remove('d-none');
      statusEl.textContent = message;
    } else {
      statusEl.classList.add('d-none');
      statusEl.textContent = '';
    }
  };

  let activeDownloadUrl = null;
  const setDownloadTarget = (url) => {
    if (!downloadButton) return;
    if (url) {
      activeDownloadUrl = url;
      downloadButton.disabled = false;
      downloadButton.classList.remove('disabled');
    } else {
      activeDownloadUrl = null;
      downloadButton.disabled = true;
      downloadButton.classList.add('disabled');
    }
  };

  const handleApiResponse = (data, isPredictAll = false) => {
    if (!data.ok) {
      setStatus('danger', data.error || 'Prediction failed');
      tableContainer.innerHTML = '';
      setDownloadTarget(null);
      return;
    }

    tableContainer.innerHTML = data.preview_html || '';
    if (typeof updatePredictionCharts === 'function') updatePredictionCharts(data.charts);

    const { row_count, as_of, meterid, scope } = data;
    const shown = Math.min(10, row_count);
    let summaryText = `Showing first ${shown} of ${row_count} rows`;
    if (!isPublic && meterid) summaryText += ` for meter ${meterid}`;
    else if (scope === 'all') summaryText += ' across all meters';
    if (as_of) summaryText += ` (as of ${as_of})`;
    summaryEl.textContent = summaryText;
    summaryEl.classList.remove('d-none');

    // Build download URL with current filters
    const filters = getSelectedFilters();
    const query = new URLSearchParams();
    if (filters.meterid) query.append('meterid', filters.meterid);
    (filters.utility || []).forEach(u => query.append('utility', u));

    const downloadUrl = filters.meterid
      ? form.dataset.downloadMeterUrl + '?' + query.toString()
      : form.dataset.downloadAllUrl + '?' + query.toString();
    setDownloadTarget(downloadUrl);
  };

  if (predictButton) {
    predictButton.addEventListener('click', async () => {
      const filters = getSelectedFilters();
      if (!isPublic && !filters.meterid) {
        setStatus('danger', 'Please select a meter to predict.');
        return;
      }
      setButtonLoading(predictButton, true);
      setStatus(null);

      try {
        const response = await fetch(predictEndpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters)
        });
        const data = await response.json();
        handleApiResponse(data);
      } catch (err) {
        console.error(err);
        setStatus('danger', 'Prediction failed due to network or server error.');
      } finally {
        setButtonLoading(predictButton, false);
      }
    });
  }

  if (predictAllButton) {
    predictAllButton.addEventListener('click', async () => {
      const filters = getSelectedFilters();
      setButtonLoading(predictAllButton, true);
      setStatus(null);

      try {
        const response = await fetch(predictAllEndpoint, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(filters)
        });
        const data = await response.json();
        handleApiResponse(data, true);
      } catch (err) {
        console.error(err);
        setStatus('danger', 'Prediction failed due to network or server error.');
      } finally {
        setButtonLoading(predictAllButton, false);
      }
    });
  }

  const resetButton = form.querySelector('[data-prediction-reset]');
if (resetButton) {
  resetButton.addEventListener('click', () => {
    // Timeout needed because form.reset() happens after click
    setTimeout(() => {
      updatePredictAllLabel();
    }, 0);
  });
}
}
