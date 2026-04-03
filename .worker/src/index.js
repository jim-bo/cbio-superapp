const LOADING_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="5">
  <title>rm -rf cancer — warming up</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #0a0a0a;
      color: #00ff41;
      font-family: 'Courier New', Courier, monospace;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 2rem;
      overflow: hidden;
    }
    canvas {
      position: fixed;
      top: 0; left: 0;
      width: 100%; height: 100%;
      z-index: 0;
      pointer-events: none;
    }
    .terminal {
      position: relative;
      z-index: 1;
      width: 100%;
      max-width: 640px;
      background: rgba(10, 10, 10, 0.75);
      backdrop-filter: blur(2px);
      border: 1px solid #003a10;
      border-radius: 4px;
      padding: 1.5rem;
    }
    .prompt-line { margin-bottom: 0.4rem; opacity: 0; transition: opacity 0.2s ease; white-space: pre; }
    .prompt-line.visible { opacity: 1; }
    .cursor-line { display: flex; align-items: center; gap: 0.25rem; margin-top: 0.8rem; }
    @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0; } }
    .cursor { animation: blink 1s step-end infinite; }
    .progress-track { margin-top: 2rem; width: 100%; height: 2px; background: #1a1a1a; border-radius: 1px; overflow: hidden; }
    @keyframes fill { from { width: 0%; } to { width: 100%; } }
    .progress-bar { height: 100%; background: #00ff41; animation: fill 5s linear forwards; }
    .footer-note { margin-top: 1.5rem; font-size: 0.75rem; color: #006b1a; text-align: center; }
  </style>
</head>
<body>
  <canvas id="dna-rain"></canvas>
  <div class="terminal" aria-live="polite">
    <div id="lines"></div>
    <div class="cursor-line">
      <span>$</span><span id="active-cmd"></span><span class="cursor">_</span>
    </div>
    <div class="progress-track"><div class="progress-bar"></div></div>
    <p class="footer-note">page refreshes automatically &mdash; no action needed</p>
  </div>
  <script>
    // --- DNA matrix rain ---
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
        // fade trail — slightly transparent black overlay each frame
        ctx.fillStyle = 'rgba(10, 10, 10, 0.12)';
        ctx.fillRect(0, 0, canvas.width, canvas.height);

        ctx.font = FONT_SIZE + 'px "Courier New", monospace';

        for (let c = 0; c < cols; c++) {
          const y = drops[c] * FONT_SIZE;

          // head character: bright white-green flash
          ctx.fillStyle = '#c8ffc8';
          ctx.fillText(CHARS[Math.floor(Math.random() * CHARS.length)], c * FONT_SIZE, y);

          // one step behind: full green
          if (drops[c] > 1) {
            ctx.fillStyle = '#00ff41';
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
    let i = 0;
    function addLine(text) {
      const el = document.createElement('div');
      el.className = 'prompt-line';
      el.textContent = '$ ' + text + '  [done]';
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
