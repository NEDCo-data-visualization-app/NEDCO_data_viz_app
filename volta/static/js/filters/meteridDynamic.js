// Dynamic search-as-you-type for meterid filter
export function initMeteridDynamic() {
  const input = document.getElementById('meteridSearch');
  const list  = document.getElementById('meteridList');
  if (!input || !list) return;

  const preselected = new URLSearchParams(window.location.search).getAll('meterid').map(String);
  const selected = new Set(preselected);

  function render(items) {
    const merged = [...new Set([...Array.from(selected), ...items.map(String)])];
    list.innerHTML = merged.map(v => {
      const checked = selected.has(v) ? 'checked' : '';
      return `
        <label class="filter-item meterid-item">
          <input type="checkbox" name="meterid" value="${v}" ${checked}>
          <span class="label-text">${v}</span>
        </label>
      `;
    }).join('');

    list.querySelectorAll('input[type="checkbox"]').forEach(cb => {
      cb.addEventListener('change', () => {
        if (cb.checked) selected.add(cb.value);
        else selected.delete(cb.value);
      });
    });
  }

  function currentLoc() {
    const params = new URLSearchParams(window.location.search);
    const locs = params.getAll('loc');
    return locs.length === 1 ? locs[0] : '';
  }

  let ctrl;
  function fetchItems(q) {
    if (ctrl) ctrl.abort();
    ctrl = new AbortController();
    const url = new URL('/options/meterid', window.location.origin);
    if (q) url.searchParams.set('q', q);
    const loc = currentLoc();
    if (loc) url.searchParams.set('loc', loc);
    url.searchParams.set('limit', '200');
    return fetch(url.toString(), { signal: ctrl.signal }).then(r => r.json());
  }

  const onType = (() => {
    let t;
    return () => {
      clearTimeout(t);
      t = setTimeout(() => {
        list.innerHTML = '<div class="text-muted small px-2 py-1">Loading…</div>';
        fetchItems(input.value).then(render).catch(() => {});
      }, 250);
    };
  })();

  fetchItems('').then(render).catch(() => {});
  input.addEventListener('input', onType);
}
