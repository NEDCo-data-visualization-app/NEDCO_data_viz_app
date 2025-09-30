// ---------- URL helpers ----------
export function urlWithFilters(path, extra = {}) {
  const qs = new URLSearchParams(window.location.search);
  for (const [k, v] of Object.entries(extra)) {
    if (v != null && v !== "") qs.set(k, v);
  }
  return `${path}?${qs.toString()}`;
}

export function updateUrlQuery(metric, freq, splitBy) {
  const qs = new URLSearchParams(window.location.search);
  if (metric) qs.set("metric", metric); else qs.delete("metric");
  if (freq)   qs.set("freq", freq);     else qs.delete("freq");
  if (splitBy) qs.set("split_by", splitBy); else qs.delete("split_by");

  history.replaceState(null, "", `${location.pathname}?${qs.toString()}`);
}
