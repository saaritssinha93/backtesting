/* Refresh continuity and ordering for the operations dashboard. */
(function (root) {
  "use strict";
  function orderIds(ids, byId, pinned, problemsFirst, bucketFor) {
    const rank = { bad: 0, warn: 1, unknown: 2, scheduled: 3, ok: 4, disabled: 5 };
    return ids.map((id, index) => {
      const status = (byId[id] || {}).status || {};
      return { id, index, pinned: !!pinned[id], disabled: status.status === "DISABLED", bucket: bucketFor(status.status, status.phase) };
    }).sort((a, b) => Number(b.pinned) - Number(a.pinned)
      || Number(a.disabled) - Number(b.disabled)
      || (problemsFirst ? (rank[a.bucket] ?? 9) - (rank[b.bucket] ?? 9) : 0)
      || a.index - b.index).map(item => item.id);
  }
  function inOrder(ordered, members) {
    const allowed = new Set(members);
    return ordered.filter(id => allowed.has(id));
  }
  function scrollState(element) {
    return { top: element.scrollTop, left: element.scrollLeft, follow: element.scrollHeight - element.clientHeight - element.scrollTop <= 24 };
  }
  function restoreScroll(element, state, follow = false) {
    element.scrollLeft = state ? state.left : 0;
    element.scrollTop = follow && (!state || state.follow) ? element.scrollHeight : (state ? state.top : 0);
  }
  function pathTo(node, ancestor) {
    const path = [];
    while (node && node !== ancestor) {
      const parent = node.parentNode;
      if (!parent) return null;
      path.unshift(Array.prototype.indexOf.call(parent.childNodes, node));
      node = parent;
    }
    return node === ancestor ? path : null;
  }
  function atPath(ancestor, path) {
    if (!path) return null;
    return path.reduce((node, index) => node && node.childNodes[index], ancestor);
  }
  function capture(cards, doc) {
    const state = { cards: {}, focus: null, selection: null };
    if (!cards) return state;
    cards.querySelectorAll('.card[data-id]').forEach(card => {
      const key = card.dataset.id;
      state.cards[key] = {
        scroll: scrollState(card),
        logs: Array.from(card.querySelectorAll('pre, .table-shell')).map(scrollState),
        details: Array.from(card.querySelectorAll('details')).map(el => el.open),
        controls: Array.from(card.querySelectorAll('input, select, textarea')).map(el => ({ value: el.value, checked: el.checked }))
      };
      const focus = doc.activeElement;
      if (focus && card.contains(focus)) state.focus = { card: key, path: pathTo(focus, card), start: focus.selectionStart, end: focus.selectionEnd, direction: focus.selectionDirection };
    });
    const selection = doc.getSelection && doc.getSelection();
    if (selection && !selection.isCollapsed && selection.rangeCount) {
      const range = selection.getRangeAt(0);
      const start = range.startContainer.parentElement && range.startContainer.parentElement.closest('.card[data-id]');
      const end = range.endContainer.parentElement && range.endContainer.parentElement.closest('.card[data-id]');
      if (start && end) state.selection = {
        startCard: start.dataset.id, endCard: end.dataset.id,
        startPath: pathTo(range.startContainer, start), endPath: pathTo(range.endContainer, end),
        startOffset: range.startOffset, endOffset: range.endOffset,
        text: range.toString()
      };
    }
    return state;
  }
  function restore(cards, state, doc) {
    const byId = new Map();
    cards.querySelectorAll('.card[data-id]').forEach(card => {
      const prior = state.cards[card.dataset.id];
      byId.set(card.dataset.id, card);
      card.querySelectorAll('details').forEach((el, index) => { if (prior && prior.details[index] !== undefined) el.open = prior.details[index]; });
      card.querySelectorAll('input, select, textarea').forEach((el, index) => {
        const control = prior && prior.controls[index];
        if (!control) return;
        if (el.tagName !== 'SELECT' || Array.from(el.options).some(option => option.value === control.value)) el.value = control.value;
        if (el.type === 'checkbox' || el.type === 'radio') el.checked = control.checked;
      });
      card.querySelectorAll('pre, .table-shell').forEach((el, index) => restoreScroll(el, prior && prior.logs[index], el.tagName === 'PRE'));
      if (prior) restoreScroll(card, prior.scroll);
    });
    if (state.focus) {
      const focus = atPath(byId.get(state.focus.card), state.focus.path);
      if (focus && focus.focus) {
        focus.focus({ preventScroll: true });
        if (typeof state.focus.start === 'number' && focus.setSelectionRange) {
          try { focus.setSelectionRange(state.focus.start, state.focus.end, state.focus.direction); } catch (_) {}
        }
      }
    }
    if (state.selection) {
      const saved = state.selection;
      const start = atPath(byId.get(saved.startCard), saved.startPath);
      const end = atPath(byId.get(saved.endCard), saved.endPath);
      if (start && end) {
        try {
          const range = doc.createRange();
          range.setStart(start, saved.startOffset); range.setEnd(end, saved.endOffset);
          // Appended output may change its text node, while the selected passage
          // remains intact. If tail truncation moved it, follow a unique match.
          if (range.toString() !== saved.text && start === end && start.nodeType === 3 && saved.text) {
            const offset = start.textContent.indexOf(saved.text);
            if (offset >= 0 && start.textContent.indexOf(saved.text, offset + 1) < 0) {
              range.setStart(start, offset); range.setEnd(end, offset + saved.text.length);
            }
          }
          if (range.toString() === saved.text) {
            const selection = doc.getSelection(); selection.removeAllRanges(); selection.addRange(range);
          }
        } catch (_) {}
      }
    }
  }
  root.DashboardOps = { orderIds, inOrder, capture, restore, scrollState, restoreScroll };
})(typeof window === "undefined" ? globalThis : window);
