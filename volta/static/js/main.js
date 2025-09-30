import { initCharts } from './charts/initCharts.js';
import { initLiveFilters } from './filters/liveFiltersController.js';
import { initFilterSearch } from './filters/filterSearch.js';
import { initMeteridDynamic } from './filters/meteridDynamic.js';

// Table + chart export helpers
import { downloadTableAsCSV, downloadChart, downloadCurrentTable, downloadFilteredCSV } from './utils/export.js';

// Expose export helpers globally for inline buttons
window.downloadChart = downloadChart;
window.downloadCurrentTable = downloadCurrentTable;
window.downloadFilteredCSV = downloadFilteredCSV;

document.addEventListener('DOMContentLoaded', () => {
  initLiveFilters();
  initFilterSearch();
  initMeteridDynamic();
  initCharts();
});
