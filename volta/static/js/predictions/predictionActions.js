import { updatePredictionCharts } from './predictionCharts.js';

const PREVIEW_LIMIT = 10;

function setButtonLoading(button, isLoading) {
  if (!button) return;
  if (isLoading) {
    button.dataset.originalText = button.dataset.originalText || button.textContent;
    button.textContent = 'Loading…';
    button.disabled = true;
  } else {
    if (button.dataset.originalText) {
      button.textContent = button.dataset.originalText;
    }
    button.disabled = false;
  }
}

function setStatus(statusEl, type, message) {
  if (!statusEl) return;
  statusEl.classList.remove('alert-info', 'alert-success', 'alert-danger');
  if (type) {
    statusEl.classList.add(`alert-${type}`);
    statusEl.classList.remove('d-none');
  } else {
    statusEl.classList.add('d-none');
  }
  if (message !== undefined) {
    statusEl.textContent = message;
  }
}

export function initPredictionActions() {
  const form = document.querySelector('[data-prediction-filters]');
  if (!form) return;

  const predictButton = form.querySelector('[data-prediction-predict]');
  const predictAllButton = form.querySelector('[data-prediction-predict-all]');
  const meterInput = form.querySelector('[data-prediction-meter-input]');
  const statusEl = document.querySelector('[data-prediction-status]');
  const summaryEl = document.querySelector('[data-prediction-summary]');
  const tableContainer = document.querySelector('[data-prediction-table]');
  const downloadButton = document.querySelector('[data-prediction-download]');

  if ((!predictButton && !predictAllButton) || !statusEl || !tableContainer) return;

  const predictEndpoint = form.dataset.predictionPredictEndpoint;
  const predictAllEndpoint = form.dataset.predictionPredictAllEndpoint;
  const downloadAllUrl = downloadButton
    ? downloadButton.dataset.downloadAllUrl || downloadButton.dataset.downloadUrl
    : null;
  const downloadMeterUrl = downloadButton
    ? downloadButton.dataset.downloadMeterUrl || downloadButton.dataset.downloadUrl
    : null;

  let activeDownloadUrl = null;

  const resetDownloadButton = () => {
    if (!downloadButton) return;
    activeDownloadUrl = null;
    downloadButton.disabled = true;
    downloadButton.classList.add('disabled');
  };

  const setDownloadTarget = (url) => {
    if (!downloadButton) return;
    if (url) {
      activeDownloadUrl = url;
      downloadButton.disabled = false;
      downloadButton.classList.remove('disabled');
    } else {
      resetDownloadButton();
    }
  };

  const updateSummary = ({ count = 0, asOf = '', meterid = '', scope = '' } = {}) => {
    if (!summaryEl) return;
    if (!count) {
      summaryEl.textContent = '';
      summaryEl.classList.add('d-none');
      return;
    }
    const shown = Math.min(PREVIEW_LIMIT, count);
    const details = [];
    if (meterid) {
      details.push(`for meter ${meterid}`);
    } else if (scope === 'all') {
      details.push('across all meters');
    }
    const suffix = asOf ? ` (as of ${asOf})` : '';
    const detailText = details.length ? ` ${details.join(' ')}` : '';
    summaryEl.textContent = `Showing first ${shown} of ${count} rows${detailText}${suffix}.`;
    summaryEl.classList.remove('d-none');
  };

  const clearTable = () => {
    if (!tableContainer) return;
    tableContainer.innerHTML = '';
    tableContainer.classList.add('d-none');
  };

  const renderTable = (html) => {
    if (!tableContainer) return;
    if (html) {
      tableContainer.innerHTML = html;
      tableContainer.classList.remove('d-none');
    } else {
      clearTable();
    }
  };

  const runPredictionRequest = async ({
    endpoint,
    button,
    body = {},
    loadingMessage,
    successMessage,
    enableDownload = false,
    scopeFallback = '',
    meterid,
    downloadUrl,
  }) => {
    if (!endpoint || !button) return;

    setButtonLoading(button, true);
    setStatus(statusEl, 'info', loadingMessage || 'Generating predictions…');
    clearTable();
    updateSummary();
    resetDownloadButton();
    updatePredictionCharts(null);

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body || {}),
      });
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      const payload = await response.json();
      if (!payload.ok) {
        const message = payload.error || 'Unable to generate predictions.';
        setStatus(statusEl, 'danger', message);
        updateSummary();
        updatePredictionCharts(null);
        return;
      }

      renderTable(payload.preview_html || '');
      updateSummary({
        count: payload.row_count || 0,
        asOf: payload.as_of || '',
        meterid: payload.meterid || meterid || '',
        scope: payload.scope || scopeFallback,
      });
      updatePredictionCharts(payload.charts || null);

      if (payload.row_count) {
        const message =
          typeof successMessage === 'function'
            ? successMessage(payload)
            : successMessage || 'Predictions loaded.';
        setStatus(statusEl, 'success', message);
        if (enableDownload && downloadUrl) {
          setDownloadTarget(downloadUrl);
        } else {
          resetDownloadButton();
        }
      } else {
        setStatus(statusEl, 'info', 'No prediction results returned.');
        resetDownloadButton();
      }
    } catch (error) {
      console.error('Prediction request failed', error);
      setStatus(statusEl, 'danger', 'Unable to generate predictions. Please try again later.');
      updatePredictionCharts(null);
    } finally {
      setButtonLoading(button, false);
    }
  };

  if (predictAllButton) {
    predictAllButton.addEventListener('click', (event) => {
      event.preventDefault();
      runPredictionRequest({
        endpoint: predictAllEndpoint,
        button: predictAllButton,
        body: {},
        loadingMessage: 'Generating predictions…',
        successMessage: 'Predictions loaded.',
        enableDownload: true,
        downloadUrl: downloadAllUrl,
        scopeFallback: 'all',
      });
    });
  }

  if (predictButton) {
    predictButton.addEventListener('click', (event) => {
      event.preventDefault();
      const selectedMeter = (meterInput?.value || '').trim();
      if (!selectedMeter) {
        setStatus(statusEl, 'info', 'Select a meter before running Predict.');
        return;
      }

      const meterDownloadUrl = downloadMeterUrl
        ? `${downloadMeterUrl}?meterid=${encodeURIComponent(selectedMeter)}`
        : null;

      runPredictionRequest({
        endpoint: predictEndpoint,
        button: predictButton,
        body: { meterid: selectedMeter },
        loadingMessage: `Generating predictions for meter ${selectedMeter}…`,
        successMessage: (payload) =>
          `Predictions loaded for meter ${payload.meterid || selectedMeter}.`,
        enableDownload: true,
        scopeFallback: 'meter',
        meterid: selectedMeter,
        downloadUrl: meterDownloadUrl,
      });
    });
  }

  if (downloadButton) {
    downloadButton.addEventListener('click', (event) => {
      event.preventDefault();
      if (downloadButton.disabled || !activeDownloadUrl) return;
      window.location.assign(activeDownloadUrl);
    });
  }

  resetDownloadButton();
}