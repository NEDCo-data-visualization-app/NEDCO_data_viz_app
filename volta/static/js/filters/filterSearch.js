// Simple live filter for checkbox lists
export function initFilterSearch() {
  const inputs = document.querySelectorAll('.filter-search-input[data-filter-target]');
  if (!inputs.length) return;

  function scoreItem(text, q) {
    if (!q) return 2;
    if (text.startsWith(q)) return 0;
    if (text.includes(q))  return 1;
    return 2;
  }

  function reorder(input, list) {
    if (!list) return;
    const q = input.value.trim().toLowerCase();
    const items = Array.from(list.querySelectorAll('.filter-item'));
    if (!items.length) return;

    const selected = [];
    const unselected = [];
    for (const el of items) {
      const cb = el.querySelector('input[type="checkbox"]');
      if (cb && cb.checked) selected.push(el);
      else unselected.push(el);
    }

    const textFor = (el) => {
      const label = el.querySelector('.label-text');
      return (label ? label.textContent : el.textContent || '').trim().toLowerCase();
    };

    unselected.sort((a, b) => {
      const sa = scoreItem(textFor(a), q);
      const sb = scoreItem(textFor(b), q);
      if (sa !== sb) return sa - sb;
      return textFor(a).localeCompare(textFor(b));
    });

    for (const el of [...selected, ...unselected]) list.appendChild(el);
  }

  inputs.forEach((input) => {
    const selector = input.getAttribute('data-filter-target');
    if (!selector) return;
    const list = document.querySelector(selector);
    if (!list) return;

    const run = () => reorder(input, list);
    input.addEventListener('input', run);
    list.addEventListener('change', run);
    run();
  });
}
