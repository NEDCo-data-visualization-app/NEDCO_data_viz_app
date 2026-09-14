// "Predict All" / "Predict" buttons on the predictions page and the CSV download.
import { updatePredictionCharts } from './predictionCharts.js';

const PREVIEW_LIMIT = 10;

export function initPredictionActions() {
  const form = document.querySelector('[data-prediction-filters]');
  if (!form) return;

  const isPublic = form.dataset.predictionPublic === 'true';
  const predictButton = form.querySelector('[data-prediction-predict]');
  const predictAllButton = form.querySelector('[data-prediction-predict-all]');
  const meterInput = form.querySelector('[data-prediction-meter-input]');
  const resetButton = form.querySelector('[data-prediction-reset]');

  const statusEl = document.querySelector('[data-prediction-status]');
  const tableContainer = document.querySelector('[data-prediction-table]');
  const summaryEl = document.querySelector('[data-prediction-summary]');
  const downloadButton = document.querySelector('[data-prediction-download]');

  const predictEndpoint = form.dataset.predictionPredictEndpoint;
  const predictAllEndpoint = form.dataset.predictionPredictAllEndpoint;

  const getSelectedFilters = () => {
    const filters = {};
    if (!isPublic && meterInput && meterInput.value) filters.meterid = meterInput.value;
    const utilities = Array.from(form.querySelectorAll('[name="utility"]:checked')).map((el) => el.value);
    if (utilities.length) filters.utility = utilities;
    return filters;
  };

  const updatePredictAllLabel = () => {
    if (!predictAllButton) return;
    const hasFilter = Object.keys(getSelectedFilters()).length > 0;
    predictAllButton.textContent = hasFilter ? 'Predict Filtered' : 'Predict All';
    delete predictAllButton.dataset.originalText;
  };

  if (meterInput) meterInput.addEventListener('input', updatePredictAllLabel);
  form.querySelectorAll('[name="utility"]').forEach((cb) => cb.addEventListener('change', updatePredictAllLabel));
  if (resetButton) resetButton.addEventListener('click', () => setTimeout(updatePredictAllLabel, 0));
  updatePredictAllLabel();

  const setButtonLoading = (button, isLoading) => {
    if (!button) return;
    if (isLoading) {
      button.dataset.originalText = button.textContent;
      button.textContent = 'Loading…';
      button.disabled = true;
    } else {
      if (button.dataset.originalText) button.textContent = button.dataset.originalText;
      button.disabled = false;
    }
  };

  const setStatus = (type, message) => {
    if (!statusEl) return;
    statusEl.classList.remove('alert-info', 'alert-success', 'alert-danger', 'alert-warning');
    if (type) {
      statusEl.classList.add(`alert-${type}`);
      statusEl.classList.remove('d-none');
      statusEl.textContent = message;
    } else {
      statusEl.classList.add('d-none');
      statusEl.textContent = '';
    }
  };

  // ---- CSV download ----
  let activeDownloadUrl = null;
  const setDownloadTarget = (url) => {
    if (!downloadButton) return;
    activeDownloadUrl = url;
    if (url && !isPublic) {
      downloadButton.disabled = false;
      downloadButton.classList.remove('disabled');
    } else {
      downloadButton.disabled = true;
      downloadButton.classList.add('disabled');
    }
  };
  if (downloadButton) {
    downloadButton.addEventListener('click', () => {
      if (activeDownloadUrl) window.location.assign(activeDownloadUrl);
    });
  }

  const buildDownloadUrl = (filters) => {
    if (!downloadButton) return null;
    const base = filters.meterid ? downloadButton.dataset.downloadMeterUrl : downloadButton.dataset.downloadAllUrl;
    if (!base) return null;
    const query = new URLSearchParams();
    if (filters.meterid) query.append('meterid', filters.meterid);
    (filters.utility || []).forEach((u) => query.append('utility', u));
    const qs = query.toString();
    return qs ? `${base}?${qs}` : base;
  };

  const handleApiResponse = (data) => {
    if (!data || !data.ok) {
      setStatus('danger', (data && data.error) || 'Prediction failed');
      if (tableContainer) tableContainer.innerHTML = '';
      setDownloadTarget(null);
      return;
    }

    if (tableContainer) tableContainer.innerHTML = data.preview_html || '';
    updatePredictionCharts(data.charts);

    const { row_count: rowCount = 0, as_of: asOf, meterid, scope } = data;
    if (rowCount === 0) {
      setStatus('warning', 'No cached predictions are available for this selection.');
    } else {
      setStatus(null);
    }

    if (summaryEl) {
      let summaryText = `Showing first ${Math.min(PREVIEW_LIMIT, rowCount)} of ${rowCount} rows`;
      if (!isPublic && meterid) summaryText += ` for meter ${meterid}`;
      else if (scope === 'all') summaryText += ' across all meters';
      if (asOf) summaryText += ` (as of ${asOf})`;
      summaryEl.textContent = summaryText;
      summaryEl.classList.remove('d-none');
    }

    setDownloadTarget(rowCount > 0 ? buildDownloadUrl(getSelectedFilters()) : null);
  };

  const runPrediction = async (button, endpoint, filters) => {
    setButtonLoading(button, true);
    setStatus(null);
    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(filters),
      });
      handleApiResponse(await response.json());
    } catch (err) {
      console.error(err);
      setStatus('danger', 'Prediction failed due to a network or server error.');
    } finally {
      setButtonLoading(button, false);
    }
  };

  if (predictButton) {
    predictButton.addEventListener('click', () => {
      const filters = getSelectedFilters();
      if (!isPublic && !filters.meterid) {
        setStatus('danger', 'Please select a meter to predict.');
        return;
      }
      runPrediction(predictButton, predictEndpoint, filters);
    });
  }

  if (predictAllButton) {
    predictAllButton.addEventListener('click', () => runPrediction(predictAllButton, predictAllEndpoint, getSelectedFilters()));
  }
}
