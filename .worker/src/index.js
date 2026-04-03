const LOADING_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="5">
  <title>rm -rf cancer — warming up</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
      --bg: #f4f6f9;
      --card-bg: #ffffff;
      --card-border: #dde3ed;
      --text: #1e293b;
      --text-muted: #64748b;
      --text-faint: #94a3b8;
      --accent: #1d4ed8;
      --done: #16a34a;
      --progress-track: #e2e8f0;
    }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'IBM Plex Mono', 'Courier New', monospace;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 2rem;
      overflow: hidden;
    }

    /* subtle dot-grid lab-notebook texture */
    body::before {
      content: '';
      position: fixed;
      inset: 0;
      background-image: radial-gradient(circle, #c8d3e8 1px, transparent 1px);
      background-size: 26px 26px;
      opacity: 0.55;
      z-index: 0;
      pointer-events: none;
    }

    canvas {
      position: fixed;
      top: 0; left: 0;
      width: 100%; height: 100%;
      z-index: 1;
      pointer-events: none;
    }

    /* macOS-style window card */
    .card {
      position: relative;
      z-index: 2;
      width: 100%;
      max-width: 600px;
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 10px;
      box-shadow:
        0 1px 2px rgba(0,0,0,0.04),
        0 4px 12px rgba(0,0,0,0.06),
        0 16px 40px rgba(0,0,0,0.05);
    }

    .card-chrome {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 10px 14px;
      border-bottom: 1px solid var(--card-border);
      background: #f8fafc;
      border-radius: 10px 10px 0 0;
    }
    .dot { width: 11px; height: 11px; border-radius: 50%; }
    .dot-close  { background: #ff6058; }
    .dot-min    { background: #ffbd2e; }
    .dot-expand { background: #29cc41; }
    .chrome-title {
      margin-left: 6px;
      font-size: 0.68rem;
      color: var(--text-faint);
      letter-spacing: 0.06em;
    }

    .card-body { padding: 1.5rem 1.75rem; }

    .prompt-line {
      margin-bottom: 0.45rem;
      opacity: 0;
      transition: opacity 0.22s ease;
      font-size: 0.8rem;
      display: flex;
      align-items: baseline;
      gap: 0.5rem;
      white-space: nowrap;
      overflow: hidden;
    }
    .prompt-line.visible { opacity: 1; }
    .ps { color: var(--accent); font-weight: 500; flex-shrink: 0; }
    .pc { color: var(--text-muted); flex: 1; overflow: hidden; text-overflow: ellipsis; }
    .pd { color: var(--done); font-size: 0.72rem; flex-shrink: 0; }

    .cursor-line {
      display: flex;
      align-items: center;
      gap: 0.35rem;
      margin-top: 0.85rem;
      font-size: 0.8rem;
    }
    .cs { color: var(--accent); font-weight: 500; }
    .cc { color: var(--text); }

    @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0; } }
    .cursor {
      display: inline-block;
      width: 7px; height: 15px;
      background: var(--accent);
      border-radius: 1px;
      vertical-align: middle;
      animation: blink 1s step-end infinite;
    }

    .progress-section {
      margin-top: 1.75rem;
      padding-top: 1.25rem;
      border-top: 1px solid var(--progress-track);
    }
    .progress-meta {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 6px;
    }
    .progress-label { font-size: 0.66rem; color: var(--text-faint); text-transform: uppercase; letter-spacing: 0.07em; }
    .progress-pct   { font-size: 0.68rem; color: var(--accent); font-weight: 500; }

    .progress-track {
      width: 100%;
      height: 3px;
      background: var(--progress-track);
      border-radius: 2px;
      overflow: hidden;
    }
    @keyframes fill { from { width: 0%; } to { width: 100%; } }
    .progress-bar {
      height: 100%;
      background: linear-gradient(90deg, #60a5fa, #1d4ed8);
      animation: fill 5s linear forwards;
      border-radius: 2px;
    }

    .footer-note {
      margin-top: 1rem;
      font-size: 0.67rem;
      color: var(--text-faint);
      text-align: center;
      letter-spacing: 0.03em;
    }
  </style>
</head>
<body>
  <canvas id="dna-rain"></canvas>
  <div class="card">
    <div class="card-chrome">
      <span class="dot dot-close"></span>
      <span class="dot dot-min"></span>
      <span class="dot dot-expand"></span>
      <span class="chrome-title">rm-rf-cancer &mdash; initializing</span>
    </div>
    <div class="card-body" aria-live="polite">
      <div id="lines"></div>
      <div class="cursor-line">
        <span class="cs">$</span><span class="cc" id="active-cmd"></span><span class="cursor"></span>
      </div>
      <div class="progress-section">
        <div class="progress-meta">
          <span class="progress-label">service warmup</span>
          <span class="progress-pct" id="pct">0%</span>
        </div>
        <div class="progress-track"><div class="progress-bar"></div></div>
        <p class="footer-note">page refreshes automatically &mdash; no action needed</p>
      </div>
    </div>
  </div>
  <script>
    // --- DNA matrix rain (light theme) ---
    (function () {
      const canvas = document.getElementById('dna-rain');
      const ctx = canvas.getContext('2d');
      const CHARS = 'AATTTGGCC'.split(''); // weight toward bases
      const FONT_SIZE = 14;
      let cols, drops;

      function resize() {
        canvas.width  = window.innerWidth;
        canvas.height = window.innerHeight;
        const rows = Math.ceil(canvas.height / FONT_SIZE);
        cols  = Math.floor(canvas.width / FONT_SIZE);
        // start spread across the screen so rain is visible immediately
        drops = Array.from({ length: cols }, () => Math.random() * rows);
      }

      function draw() {
        // fade trail — light overlay so characters dissolve gently
        ctx.fillStyle = 'rgba(244, 246, 249, 0.18)';
        ctx.fillRect(0, 0, canvas.width, canvas.height);

        ctx.font = FONT_SIZE + 'px "IBM Plex Mono", "Courier New", monospace';

        for (let c = 0; c < cols; c++) {
          const y = drops[c] * FONT_SIZE;

          // head character: vivid blue
          ctx.fillStyle = 'rgba(59, 130, 246, 0.5)';
          ctx.fillText(CHARS[Math.floor(Math.random() * CHARS.length)], c * FONT_SIZE, y);

          // one step behind: faint blue trail
          if (drops[c] > 1) {
            ctx.fillStyle = 'rgba(99, 155, 230, 0.18)';
            ctx.fillText(CHARS[Math.floor(Math.random() * CHARS.length)], c * FONT_SIZE, y - FONT_SIZE);
          }

          if (y > canvas.height && Math.random() > 0.97) drops[c] = 0;
          drops[c] += 0.6;
        }
      }

      resize();
      window.addEventListener('resize', resize);
      setInterval(draw, 40); // 25fps
    })();

    // --- terminal typewriter ---
    const steps = [
      'initializing cancer genome database',
      'mounting genomic study index',
      'loading mutation profiles',
      'preparing clinical data engine',
      'warming up compute workers',
    ];
    const linesEl = document.getElementById('lines');
    const activeEl = document.getElementById('active-cmd');
    const pctEl   = document.getElementById('pct');
    let i = 0;

    // animated percentage counter
    const t0 = Date.now();
    const pctTimer = setInterval(() => {
      const pct = Math.min(99, Math.round((Date.now() - t0) / 5000 * 100));
      pctEl.textContent = pct + '%';
      if (pct >= 99) clearInterval(pctTimer);
    }, 80);

    function addLine(text) {
      const el = document.createElement('div');
      el.className = 'prompt-line';
      el.innerHTML =
        '<span class="ps">$</span>' +
        '<span class="pc">' + text + '</span>' +
        '<span class="pd">&#10003; done</span>';
      linesEl.appendChild(el);
      requestAnimationFrame(() => el.classList.add('visible'));
    }
    function tick() {
      if (i > 0) addLine(steps[i - 1]);
      if (i < steps.length) { activeEl.textContent = ' ' + steps[i]; i++; }
      else { activeEl.textContent = ' waiting for service...'; clearInterval(timer); }
    }
    const timer = setInterval(tick, 900);
    tick();
    setTimeout(() => location.replace(location.href), 4800);
  </script>
</body>
</html>`;

const STRIP_REQUEST = new Set([
  'host', 'connection', 'keep-alive', 'transfer-encoding',
  'upgrade', 'proxy-authorization', 'proxy-connection', 'te', 'trailer',
]);
const STRIP_RESPONSE = new Set(['transfer-encoding', 'connection', 'keep-alive', 'upgrade']);

function buildUpstreamRequest(request, cloudRunUrl) {
  const url = new URL(request.url);
  const target = cloudRunUrl.replace(/\/$/, '') + url.pathname + url.search;
  const headers = new Headers();
  for (const [k, v] of request.headers.entries()) {
    const lower = k.toLowerCase();
    if (STRIP_REQUEST.has(lower) || lower.startsWith('cf-')) continue;
    headers.set(k, v);
  }
  headers.set('x-forwarded-host', url.hostname);
  headers.set('x-forwarded-proto', 'https');
  return new Request(target, {
    method: request.method,
    headers,
    // duplex: 'half' is required when streaming a request body (POST chart endpoints)
    ...(request.body ? { body: request.body, duplex: 'half' } : {}),
    // pass 3xx through rather than silently following (FastAPI trailing-slash redirects)
    redirect: 'manual',
  });
}

function buildDownstreamResponse(res) {
  const headers = new Headers();
  for (const [k, v] of res.headers.entries()) {
    if (!STRIP_RESPONSE.has(k.toLowerCase())) headers.set(k, v);
  }
  return new Response(res.body, { status: res.status, headers });
}

function loadingResponse() {
  return new Response(LOADING_HTML, {
    status: 503,
    headers: {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-store',
      'retry-after': '5',
    },
  });
}

export default {
  async fetch(request, env) {
    const timeout = new Promise((_, reject) =>
      setTimeout(() => reject(new Error('timeout')), 5000)
    );
    let res;
    try {
      res = await Promise.race([
        fetch(buildUpstreamRequest(request, env.CLOUD_RUN_URL)),
        timeout,
      ]);
    } catch {
      return loadingResponse();
    }
    if (res.status >= 500) return loadingResponse();
    return buildDownstreamResponse(res);
  },
};
