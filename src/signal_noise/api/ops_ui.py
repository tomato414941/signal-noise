from __future__ import annotations


OPS_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>signal-noise ops board</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600;9..144,700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --paper: #f3eee3;
      --panel: rgba(255, 251, 245, 0.88);
      --panel-strong: rgba(252, 247, 239, 0.96);
      --ink: #171411;
      --muted: #6a6258;
      --line: rgba(23, 20, 17, 0.14);
      --accent: #b53a1f;
      --accent-soft: rgba(181, 58, 31, 0.12);
      --ok: #245a3f;
      --warn: #9a5a00;
      --fail: #8f1d17;
      --chip: rgba(23, 20, 17, 0.06);
      --shadow: 0 24px 60px rgba(53, 40, 27, 0.12);
      --radius: 24px;
    }

    * {
      box-sizing: border-box;
    }

    html {
      background: var(--paper);
    }

    body {
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at top right, rgba(181, 58, 31, 0.18), transparent 28rem),
        radial-gradient(circle at bottom left, rgba(36, 90, 63, 0.13), transparent 30rem),
        linear-gradient(180deg, #f8f3ea 0%, #eee6d7 100%);
      color: var(--ink);
      font-family: "IBM Plex Mono", "SFMono-Regular", Consolas, monospace;
    }

    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      opacity: 0.38;
      background-image:
        linear-gradient(rgba(23, 20, 17, 0.05) 1px, transparent 1px),
        linear-gradient(90deg, rgba(23, 20, 17, 0.05) 1px, transparent 1px);
      background-size: 24px 24px;
      mask-image: linear-gradient(180deg, rgba(0, 0, 0, 0.58), transparent 92%);
    }

    main {
      position: relative;
      max-width: 1180px;
      margin: 0 auto;
      padding: 28px 18px 40px;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
      opacity: 0;
      transform: translateY(16px);
      animation: rise 0.5s ease forwards;
    }

    .hero {
      position: relative;
      overflow: hidden;
      padding: 24px;
      background:
        linear-gradient(135deg, rgba(255, 251, 245, 0.98), rgba(243, 235, 221, 0.88)),
        var(--panel);
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: auto -12% -38% 50%;
      height: 220px;
      background: radial-gradient(circle, rgba(181, 58, 31, 0.18), transparent 62%);
      pointer-events: none;
    }

    .kicker {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 14px;
      padding: 7px 12px;
      border-radius: 999px;
      background: rgba(23, 20, 17, 0.06);
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    h1 {
      margin: 0;
      max-width: 10ch;
      font-family: "Fraunces", Georgia, serif;
      font-size: clamp(2.6rem, 7vw, 4.9rem);
      line-height: 0.94;
      letter-spacing: -0.05em;
    }

    .hero-grid {
      display: grid;
      gap: 18px;
      grid-template-columns: minmax(0, 1.4fr) minmax(300px, 0.95fr);
      align-items: end;
    }

    .lede {
      margin: 12px 0 0;
      max-width: 42rem;
      color: var(--muted);
      font-size: 0.97rem;
      line-height: 1.7;
    }

    .hero-side {
      position: relative;
      z-index: 1;
      display: grid;
      gap: 12px;
      justify-items: start;
      padding: 18px;
      border-radius: 20px;
      background: rgba(255, 251, 245, 0.84);
      border: 1px solid rgba(23, 20, 17, 0.08);
    }

    .status-chip {
      display: inline-flex;
      align-items: center;
      gap: 9px;
      padding: 10px 14px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--ink);
      font-size: 13px;
    }

    .status-dot {
      width: 11px;
      height: 11px;
      border-radius: 999px;
      background: var(--warn);
      box-shadow: 0 0 0 6px rgba(154, 90, 0, 0.08);
      transition: background 0.25s ease, box-shadow 0.25s ease;
    }

    .status-chip[data-tone="ok"] .status-dot {
      background: var(--ok);
      box-shadow: 0 0 0 6px rgba(36, 90, 63, 0.08);
    }

    .status-chip[data-tone="warn"] .status-dot {
      background: var(--warn);
      box-shadow: 0 0 0 6px rgba(154, 90, 0, 0.08);
    }

    .status-chip[data-tone="fail"] .status-dot {
      background: var(--fail);
      box-shadow: 0 0 0 6px rgba(143, 29, 23, 0.08);
    }

    .hero-meta {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
    }

    .hero-links {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .hero-links a {
      color: var(--ink);
      text-decoration: none;
      border-bottom: 1px solid rgba(23, 20, 17, 0.22);
    }

    .stats {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      margin-top: 16px;
    }

    .stat-card {
      padding: 18px 16px;
      background: var(--panel-strong);
      border: 1px solid var(--line);
      border-radius: 20px;
      min-height: 122px;
    }

    .stat-label {
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    .stat-value {
      margin-top: 10px;
      font-family: "Fraunces", Georgia, serif;
      font-size: clamp(2rem, 5vw, 3rem);
      line-height: 0.92;
      letter-spacing: -0.05em;
    }

    .stat-note {
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.55;
    }

    .layout {
      display: grid;
      gap: 16px;
      grid-template-columns: minmax(0, 1.05fr) minmax(0, 0.95fr);
      margin-top: 16px;
    }

    .panel-body {
      padding: 20px;
    }

    .panel-head {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 16px;
    }

    .panel-title {
      margin: 0;
      font-family: "Fraunces", Georgia, serif;
      font-size: 1.6rem;
      letter-spacing: -0.04em;
    }

    .panel-copy {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.55;
    }

    .stack {
      display: grid;
      gap: 12px;
    }

    .notice {
      border: 1px dashed rgba(23, 20, 17, 0.18);
      border-radius: 16px;
      padding: 16px;
      color: var(--muted);
      background: rgba(255, 255, 255, 0.32);
      line-height: 1.65;
      font-size: 13px;
    }

    .event-card {
      display: grid;
      gap: 10px;
      padding: 16px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.48);
      border: 1px solid rgba(23, 20, 17, 0.08);
    }

    .event-card[data-tone="fail"] {
      border-color: rgba(143, 29, 23, 0.16);
      background: rgba(143, 29, 23, 0.05);
    }

    .event-card[data-tone="warn"] {
      border-color: rgba(154, 90, 0, 0.16);
      background: rgba(154, 90, 0, 0.05);
    }

    .event-name {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      font-weight: 600;
      font-size: 14px;
    }

    .event-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(23, 20, 17, 0.06);
      color: var(--muted);
      font-size: 11px;
      white-space: nowrap;
    }

    .event-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
    }

    .event-error {
      padding-top: 10px;
      border-top: 1px solid rgba(23, 20, 17, 0.08);
      line-height: 1.6;
      font-size: 13px;
      word-break: break-word;
    }

    .group-grid {
      display: grid;
      gap: 12px;
    }

    .mini-panel {
      padding: 16px;
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.42);
      border: 1px solid rgba(23, 20, 17, 0.08);
    }

    .mini-title {
      margin: 0 0 10px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 7px 10px;
      border-radius: 999px;
      background: var(--chip);
      font-size: 12px;
      line-height: 1.2;
    }

    .suppressed-tools {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 14px;
    }

    .suppressed-tools input {
      width: min(100%, 320px);
      border: 1px solid rgba(23, 20, 17, 0.14);
      border-radius: 999px;
      padding: 11px 14px;
      background: rgba(255, 255, 255, 0.58);
      color: var(--ink);
      font: inherit;
    }

    .suppressed-tools input:focus {
      outline: 2px solid rgba(181, 58, 31, 0.18);
      outline-offset: 2px;
    }

    details.bucket {
      border: 1px solid rgba(23, 20, 17, 0.09);
      border-radius: 18px;
      background: rgba(255, 255, 255, 0.46);
      overflow: hidden;
    }

    details.bucket summary {
      cursor: pointer;
      list-style: none;
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
    }

    details.bucket summary::-webkit-details-marker {
      display: none;
    }

    .bucket-head {
      display: grid;
      gap: 4px;
    }

    .bucket-title {
      font-weight: 600;
      font-size: 14px;
    }

    .bucket-meta {
      color: var(--muted);
      font-size: 12px;
    }

    .bucket-count {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 44px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
    }

    .bucket-list {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding: 0 16px 16px;
    }

    .signal-tag {
      display: inline-flex;
      padding: 7px 10px;
      border-radius: 999px;
      background: rgba(23, 20, 17, 0.06);
      font-size: 12px;
      word-break: break-word;
    }

    .footer-note {
      margin-top: 14px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.65;
    }

    .tone-ok {
      color: var(--ok);
    }

    .tone-warn {
      color: var(--warn);
    }

    .tone-fail {
      color: var(--fail);
    }

    @keyframes rise {
      from {
        opacity: 0;
        transform: translateY(16px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }

    @media (max-width: 900px) {
      .hero-grid,
      .layout {
        grid-template-columns: 1fr;
      }

      .hero-side {
        justify-items: stretch;
      }
    }

    @media (max-width: 640px) {
      main {
        padding: 16px 12px 28px;
      }

      .hero,
      .panel-body {
        padding: 16px;
      }

      .stats {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
  </style>
</head>
<body>
  <main>
    <section class="panel hero">
      <div class="kicker">tailnet operations board</div>
      <div class="hero-grid">
        <div>
          <h1>signal-noise</h1>
          <p class="lede">
            Read-only field board for the collector fleet. This page only reads
            <code>/health</code> and <code>/health/signals</code>; no control
            actions live here.
          </p>
        </div>
        <aside class="hero-side">
          <div class="status-chip" id="status-chip" data-tone="warn">
            <span class="status-dot" aria-hidden="true"></span>
            <span id="status-text">Connecting to board...</span>
          </div>
          <div class="hero-meta">
            <div id="refresh-text">Waiting for first refresh.</div>
            <div id="scope-text">Tailnet view only. Auto refresh every 30 seconds.</div>
          </div>
          <nav class="hero-links">
            <a href="/health" target="_blank" rel="noreferrer">/health</a>
            <a href="/health/signals" target="_blank" rel="noreferrer">/health/signals</a>
            <a href="/docs" target="_blank" rel="noreferrer">/docs</a>
          </nav>
        </aside>
      </div>
    </section>

    <section class="stats" aria-label="Health overview">
      <article class="panel stat-card" style="animation-delay:0.04s">
        <div class="stat-label">Status</div>
        <div class="stat-value" id="status-value">...</div>
        <div class="stat-note" id="status-note">Preparing counters.</div>
      </article>
      <article class="panel stat-card" style="animation-delay:0.08s">
        <div class="stat-label">Fresh</div>
        <div class="stat-value" id="fresh-value">0</div>
        <div class="stat-note">Collectors inside their interval window.</div>
      </article>
      <article class="panel stat-card" style="animation-delay:0.12s">
        <div class="stat-label">Failing</div>
        <div class="stat-value" id="failing-value">0</div>
        <div class="stat-note">Retry exhausted or upstream unavailable.</div>
      </article>
      <article class="panel stat-card" style="animation-delay:0.16s">
        <div class="stat-label">Suppressed</div>
        <div class="stat-value" id="suppressed-value">0</div>
        <div class="stat-note">Excluded from scheduler with tracked reason.</div>
      </article>
      <article class="panel stat-card" style="animation-delay:0.2s">
        <div class="stat-label">Stale</div>
        <div class="stat-value" id="stale-value">0</div>
        <div class="stat-note">Collected before, currently outside threshold.</div>
      </article>
      <article class="panel stat-card" style="animation-delay:0.24s">
        <div class="stat-label">Never Seen</div>
        <div class="stat-value" id="never-seen-value">0</div>
        <div class="stat-note">Registered, but no successful sample yet.</div>
      </article>
    </section>

    <section class="layout">
      <section class="panel panel-body" style="animation-delay:0.28s">
        <div class="panel-head">
          <div>
            <h2 class="panel-title">Failing Now</h2>
            <div class="panel-copy">Only active failures are shown here. Each card mirrors the JSON health payload.</div>
          </div>
        </div>
        <div id="failing-list" class="stack"></div>
      </section>

      <section class="stack">
        <section class="panel panel-body" style="animation-delay:0.32s">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Stale</h2>
              <div class="panel-copy">Signals that missed their expected interval window.</div>
            </div>
          </div>
          <div id="stale-list" class="stack"></div>
        </section>

        <section class="panel panel-body" style="animation-delay:0.36s">
          <div class="panel-head">
            <div>
              <h2 class="panel-title">Never Seen</h2>
              <div class="panel-copy">Tracked collectors with no successful write yet.</div>
            </div>
          </div>
          <div id="never-seen-list" class="pill-row"></div>
        </section>
      </section>
    </section>

    <section class="panel panel-body" style="animation-delay:0.4s">
      <div class="panel-head">
        <div>
          <h2 class="panel-title">Suppressed Ledger</h2>
          <div class="panel-copy">Grouped by reason and source so long exclude lists stay usable on mobile.</div>
        </div>
      </div>
      <div class="suppressed-tools">
        <div class="panel-copy" id="suppressed-summary">No suppressed collectors loaded yet.</div>
        <input id="suppressed-filter" type="search" placeholder="Filter by signal name, reason, or source">
      </div>
      <div id="suppressed-groups" class="group-grid"></div>
      <div class="footer-note">This board is intentionally read-only. Use the repo and server workflow for changes.</div>
    </section>
  </main>

  <script>
    const state = {
      health: null,
      signals: null,
      filter: '',
    };

    function escapeHtml(value) {
      return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function formatDate(value) {
      if (!value) {
        return 'n/a';
      }
      const parsed = new Date(value);
      if (Number.isNaN(parsed.getTime())) {
        return escapeHtml(value);
      }
      return parsed.toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
      });
    }

    function pluralize(count, singular, plural) {
      return count === 1 ? singular : plural;
    }

    function setStatusCopy(text, tone) {
      const chip = document.getElementById('status-chip');
      chip.dataset.tone = tone;
      document.getElementById('status-text').textContent = text;
      document.getElementById('status-value').textContent = tone === 'ok' ? 'OK' : tone === 'warn' ? 'Degraded' : 'Offline';
      document.getElementById('status-note').innerHTML = tone === 'ok'
        ? '<span class="tone-ok">No failing or stale collectors.</span>'
        : tone === 'warn'
          ? '<span class="tone-warn">Attention needed, but API is reachable.</span>'
          : '<span class="tone-fail">Board cannot reach the health endpoints.</span>';
    }

    function renderEmpty(containerId, copy) {
      document.getElementById(containerId).innerHTML = '<div class="notice">' + escapeHtml(copy) + '</div>';
    }

    function renderFailing(items) {
      if (!items.length) {
        renderEmpty('failing-list', 'No failing collectors.');
        return;
      }

      document.getElementById('failing-list').innerHTML = items.map((item) => `
        <article class="event-card" data-tone="fail">
          <div class="event-name">
            <span>${escapeHtml(item.name)}</span>
            <span class="event-badge">${escapeHtml(String(item.consecutive_failures))} retries</span>
          </div>
          <div class="event-meta">
            <span>Last error: ${escapeHtml(formatDate(item.error_at))}</span>
          </div>
          <div class="event-error">${escapeHtml(item.error || 'No error detail')}</div>
        </article>
      `).join('');
    }

    function renderStale(items) {
      if (!items.length) {
        renderEmpty('stale-list', 'No stale collectors.');
        return;
      }

      document.getElementById('stale-list').innerHTML = items.map((item) => `
        <article class="event-card" data-tone="warn">
          <div class="event-name">
            <span>${escapeHtml(item.name)}</span>
            <span class="event-badge">${escapeHtml(String(item.interval))}s interval</span>
          </div>
          <div class="event-meta">
            <span>Age: ${escapeHtml(String(item.age_seconds))}s</span>
          </div>
        </article>
      `).join('');
    }

    function renderNeverSeen(items) {
      if (!items.length) {
        document.getElementById('never-seen-list').innerHTML = '<div class="notice">No never-seen collectors.</div>';
        return;
      }

      document.getElementById('never-seen-list').innerHTML = items
        .map((name) => '<span class="pill">' + escapeHtml(name) + '</span>')
        .join('');
    }

    function buildSuppressedGroups(items) {
      const groups = new Map();
      for (const item of items) {
        const reason = item.reason || 'unspecified';
        const source = item.source || 'unknown';
        const detail = item.detail || '';
        const scope = item.scope || '';
        const reviewAfter = item.review_after || '';
        const key = [source, reason, detail, scope, reviewAfter].join('::');
        if (!groups.has(key)) {
          groups.set(key, {
            reason,
            detail,
            scope,
            review_after: reviewAfter,
            source,
            items: [],
          });
        }
        groups.get(key).items.push(item);
      }
      return [...groups.values()].sort((left, right) => {
        if (right.items.length !== left.items.length) {
          return right.items.length - left.items.length;
        }
        return left.reason.localeCompare(right.reason);
      });
    }

    function renderSuppressed(items) {
      const query = state.filter.trim().toLowerCase();
      const filtered = query
        ? items.filter((item) => [item.name, item.reason, item.detail, item.source, item.scope, item.review_after].some((value) => String(value || '').toLowerCase().includes(query)))
        : items;

      document.getElementById('suppressed-summary').textContent =
        filtered.length + ' ' + pluralize(filtered.length, 'collector', 'collectors') +
        ' visible across ' + buildSuppressedGroups(filtered).length + ' grouped reasons.';

      if (!filtered.length) {
        document.getElementById('suppressed-groups').innerHTML =
          '<div class="notice">No suppressed collectors matched the current filter.</div>';
        return;
      }

      document.getElementById('suppressed-groups').innerHTML = buildSuppressedGroups(filtered)
        .map((group, index) => `
          <details class="bucket" ${index < 2 ? 'open' : ''}>
            <summary>
              <div class="bucket-head">
                <div class="bucket-title">${escapeHtml(group.reason)}</div>
                <div class="bucket-meta">
                  source: ${escapeHtml(group.source)}
                  ${group.scope ? ' | scope: ' + escapeHtml(group.scope) : ''}
                  ${group.review_after ? ' | review: ' + escapeHtml(group.review_after) : ''}
                  ${group.detail ? '<br>' + escapeHtml(group.detail) : ''}
                </div>
              </div>
              <span class="bucket-count">${escapeHtml(String(group.items.length))}</span>
            </summary>
            <div class="bucket-list">
              ${group.items
                .sort((left, right) => left.name.localeCompare(right.name))
                .map((item) => '<span class="signal-tag">' + escapeHtml(item.name) + '</span>')
                .join('')}
            </div>
          </details>
        `)
        .join('');
    }

    function renderBoard() {
      const health = state.health;
      const signals = state.signals;
      const total =
        health.fresh +
        health.stale +
        health.failing +
        health.never_seen +
        health.suppressed;

      document.getElementById('fresh-value').textContent = String(health.fresh);
      document.getElementById('failing-value').textContent = String(health.failing);
      document.getElementById('suppressed-value').textContent = String(health.suppressed);
      document.getElementById('stale-value').textContent = String(health.stale);
      document.getElementById('never-seen-value').textContent = String(health.never_seen);
      document.getElementById('scope-text').textContent =
        total + ' tracked collectors across fresh, failing, stale, never-seen, and suppressed states.';

      const tone = health.status === 'ok' ? 'ok' : 'warn';
      setStatusCopy(
        health.status === 'ok' ? 'System healthy and reachable.' : 'API reachable, but operator attention is needed.',
        tone,
      );

      renderFailing(signals.failing || []);
      renderStale(signals.stale || []);
      renderNeverSeen(signals.never_seen || []);
      renderSuppressed(signals.suppressed || []);
    }

    async function loadBoard() {
      try {
        const [healthRes, signalsRes] = await Promise.all([
          fetch('/health', { cache: 'no-store' }),
          fetch('/health/signals', { cache: 'no-store' }),
        ]);
        if (!healthRes.ok || !signalsRes.ok) {
          throw new Error('Health endpoint returned a non-200 response.');
        }

        state.health = await healthRes.json();
        state.signals = await signalsRes.json();
        renderBoard();
        document.getElementById('refresh-text').textContent = 'Last refresh ' + formatDate(new Date().toISOString());
      } catch (error) {
        setStatusCopy('Board cannot refresh right now.', 'fail');
        document.getElementById('refresh-text').textContent = error instanceof Error
          ? error.message
          : 'Unknown refresh error';
        renderEmpty('failing-list', 'Health fetch failed. Check the raw JSON endpoints.');
        renderEmpty('stale-list', 'Health fetch failed. Check the raw JSON endpoints.');
        document.getElementById('never-seen-list').innerHTML =
          '<div class="notice">Health fetch failed. Check the raw JSON endpoints.</div>';
        document.getElementById('suppressed-groups').innerHTML =
          '<div class="notice">Health fetch failed. Check the raw JSON endpoints.</div>';
      }
    }

    document.getElementById('suppressed-filter').addEventListener('input', (event) => {
      state.filter = event.target.value || '';
      if (state.signals) {
        renderSuppressed(state.signals.suppressed || []);
      }
    });

    loadBoard();
    window.setInterval(loadBoard, 30000);
  </script>
</body>
</html>
"""


def render_ops_page() -> str:
    return OPS_HTML
