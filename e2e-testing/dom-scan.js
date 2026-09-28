#!/usr/bin/env node
/*
 * dom-scan.js — the browser DOM gate for the data-lab Superset dashboards.
 *
 * WHY THIS EXISTS
 * ---------------
 * An API 200 is not proof that a chart renders. Seeded charts answer 200 while
 * the browser shows "Columns missing in dataset" / "There is no chart definition
 * associated with this component…". t_f0e6a35a found 39 broken tiles that every
 * API and DB gate reported green. So the e2e full cycle's FINAL gate is this:
 * render every dashboard in a real (headless) Chromium and read the DOM the user
 * actually sees. A screenshot is not a substitute — ECharts canvases render
 * blank headless, while the tile's text (the error message) is right there in
 * `.dashboard-component`.
 *
 * This file is that gate, in the repo, so every worker runs the SAME one instead
 * of pasting a fresh ad-hoc snippet into a console. It was extracted from the
 * harness written for t_6cab9400 (which found data-quality-ops rendering 48
 * error tiles on dev while the API stayed green).
 *
 * FALSE-PASS GUARDS (each one is a failure mode observed in this repo)
 * -------------------------------------------------------------------
 *  - an absent/expired session silently lands on /login/ — a login page has no
 *    `.dashboard-component` at all, so it scans "clean": fail loudly;
 *  - the SPA can settle on a DIFFERENT dashboard than the one requested, which
 *    also reports a clean count. The requested target is therefore resolved
 *    through the API first, and the rendered `location.pathname` AND
 *    `document.title` must both match it, or the scan retries and then reports
 *    exit 2 — a clean count for the wrong page is not a pass;
 *  - a dashboard that renders NO tile although the API says it carries charts
 *    is the silent-skip symptom of t_6cab9400 (9 charts skipped, exit 0). It is
 *    reported as an error-tile failure, not as a clean empty page;
 *  - the error regex below is the whole detection surface: a missing keyword is
 *    a false negative, not a pass (the superset-seed-doctor skill documents a
 *    scan that passed because its regex lacked `chart definition|deleted`).
 *    Keep it in sync with every new frontend error string.
 *
 * AUTH: the stack's documented seed credential (admin/admin, the pair the seed
 * scripts under superset/ use; override with SUPERSET_USER/SUPERSET_PASSWORD).
 * The session cookie is minted over HTTP and injected with Network.setCookie —
 * no credential is ever typed into a page.
 *
 * ADDRESS DASHBOARDS BY SLUG. Dashboard ids drift between instances and reseeds
 * (`data-quality-ops` is dash 12 on test, dash 6 on dev), so a slug is the only
 * stable handle. With no dashboard argument every published dashboard is
 * discovered through the API and scanned by slug (a dashboard whose slug is
 * NULL — dash 3 "Grocery Overview" on test — falls back to its id).
 *
 * Usage:
 *   node e2e-testing/dom-scan.js <host> [dash[,<dash>...] ...] [--json <path>]
 *   node e2e-testing/dom-scan.js <test-slot>                      # all dashboards
 *   node e2e-testing/dom-scan.js <test-slot> data-quality-ops,12   # by slug and id
 *   node e2e-testing/dom-scan.js <test-slot> --json /tmp/dom.json  # all + report
 *
 * Env:  SUPERSET_PORT (8088)  SUPERSET_USER/SUPERSET_PASSWORD (admin/admin)
 *       CHROMIUM_BIN (/usr/sbin/chromium, /usr/bin/chromium, chromium)
 *       CDP_PORT (auto: a free port is chosen so parallel scans cannot collide)
 *       DOM_SCAN_OUT (report path; --json wins)
 *       DOM_SCAN_{SETTLE_MS,POLL_MS,MAX_POLLS,ATTEMPTS,DASH_TIMEOUT_MS,EXTRA_WAIT_MS}
 *
 * Exit codes (the e2e gate reads these):
 *   0  every requested dashboard rendered with no error tile
 *   1  error tiles found (or a dashboard that should carry charts rendered none)
 *   2  wrong page / unresolved target / landed on /login/ — the false-pass guard
 *   3  harness failure (host unreachable, no Chromium, CDP/websocket error, usage)
 */
'use strict';

const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const net = require('net');
const path = require('path');

const EXIT_CLEAN = 0;
const EXIT_TILES = 1;
const EXIT_WRONG_PAGE = 2;
const EXIT_HARNESS = 3;

const DASH_BASE = '/superset/dashboard';

const env = process.env;
const SETTLE_MS = Number(env.DOM_SCAN_SETTLE_MS || 3000);
const POLL_MS = Number(env.DOM_SCAN_POLL_MS || 1000);
const MAX_POLLS = Number(env.DOM_SCAN_MAX_POLLS || 45);
const ATTEMPTS = Number(env.DOM_SCAN_ATTEMPTS || 3);
const DASH_TIMEOUT_MS = Number(env.DOM_SCAN_DASH_TIMEOUT_MS || 120000);
const EXTRA_WAIT_MS = Number(env.DOM_SCAN_EXTRA_WAIT_MS || 5000);

// Every error class the frontend can show for a broken tile. A keyword missing
// here is a false negative (see the header).
const ERROR_RE = new RegExp(
  [
    'Unexpected error',
    'Columns missing in datas[oe]t',
    'Columns missing in datasource',
    'Datetime column not provided',
    'Something went wrong',
    'rolling window',
    'chart definition',
    'deleted',
    'could it have been',
    'Error:',
  ].join('|'),
  'i',
);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (msg) => process.stdout.write(msg + '\n');
const warn = (msg) => process.stderr.write(msg + '\n');

function usage() {
  log(`usage: node e2e-testing/dom-scan.js <host> [dash[,<dash>...] ...] [--json <path>]

Renders the data-lab Superset dashboards in headless Chromium and reads the DOM
the user sees (ECharts canvases are blank headless, so this is not a screenshot).

  <host>            Superset host (IP or name; port via SUPERSET_PORT, default 8088)
  dash              dashboard slug (preferred) or numeric id; several may be
                    comma- or space-separated. Omitted = every published
                    dashboard, discovered through the API and scanned by slug.
  --json <path>     also write the machine-readable report to <path>
  -h, --help        this text

exit: 0 clean | 1 error tiles found | 2 wrong page (false-pass guard) | 3 harness failure`);
}

// --- args ------------------------------------------------------------------

function parseArgs(argv) {
  const out = { host: null, targets: [], json: env.DOM_SCAN_OUT || null, help: false };
  const rest = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '-h' || a === '--help') out.help = true;
    else if (a === '--json') out.json = argv[++i];
    else if (a.startsWith('--json=')) out.json = a.slice('--json='.length);
    else rest.push(a);
  }
  if (rest.length === 0) return out;
  out.host = rest[0];
  out.targets = rest.slice(1).flatMap((s) => s.split(',')).map((s) => s.trim()).filter(Boolean);
  return out;
}

// --- Superset HTTP ---------------------------------------------------------

class Superset {
  constructor(host, port) {
    this.base = `http://${host}:${port}`;
    this.session = null;
  }

  async login(username, password) {
    let r1;
    try {
      r1 = await fetch(`${this.base}/login/`);
    } catch (e) {
      throw new HarnessError(`cannot reach ${this.base}/login/ (${e.message})`);
    }
    if (!r1.ok) throw new HarnessError(`${this.base}/login/ answered ${r1.status}`);
    const html = await r1.text();
    const m =
      html.match(/name="csrf_token"[^>]*value="([^"]+)"/) ||
      html.match(/value="([^"]+)"[^>]*name="csrf_token"/);
    const csrf = m ? m[1] : '';
    const pre = r1.headers.getSetCookie().map((c) => c.split(';')[0]).join('; ');
    const r2 = await fetch(`${this.base}/login/`, {
      method: 'POST',
      body: new URLSearchParams({ username, password, csrf_token: csrf }),
      redirect: 'manual',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', Cookie: pre },
    });
    let session = null;
    for (const c of r2.headers.getSetCookie()) {
      const [n, v] = c.split(';')[0].split('=');
      if (n === 'session' && v) session = v;
    }
    if (!session) {
      for (const c of pre.split('; ')) {
        const [n, v] = c.split('=');
        if (n === 'session' && v) session = v;
      }
    }
    if (!session) throw new HarnessError(`login produced no session cookie (POST /login/ -> ${r2.status})`);
    this.session = session;
    // The cookie is only trusted if it authenticates: an anonymous session would
    // make every dashboard 302 to /login/ and the scan report a clean nothing.
    const me = await this.api('/api/v1/me/');
    const user = me && me.result && me.result.username;
    if (!user) throw new HarnessError(`the minted session does not authenticate (GET /api/v1/me/ -> ${JSON.stringify((me || {}).result)})`);
    return { status: r2.status, user };
  }

  async api(pathname) {
    const r = await fetch(`${this.base}${pathname}`, { headers: { Cookie: `session=${this.session}` } });
    let body = null;
    try { body = await r.json(); } catch (e) { body = null; }
    return { status: r.status, ...(body || {}) };
  }

  /** All dashboards, discovered through the API (RSION `q` form only — the
   *  plain page/page_size form is ignored by this endpoint, see t_6cab9400). */
  async listDashboards() {
    const q = encodeURIComponent('(page:0,page_size:100)');
    const r = await this.api(`/api/v1/dashboard/?q=${q}`);
    if (r.status !== 200 || !Array.isArray(r.result)) {
      throw new HarnessError(`GET /api/v1/dashboard/ -> ${r.status} (dashboard list unreadable)`);
    }
    return r.result;
  }

  async dashboardDetail(id) {
    const r = await this.api(`/api/v1/dashboard/${id}`);
    return r.status === 200 && r.result ? r.result : null;
  }
}

class HarnessError extends Error {}

/** Resolve a CLI target (slug preferred, numeric id accepted) into the canonical
 *  {id, slug, title, charts} the rendered page is asserted against. */
async function resolveTarget(superset, all, arg) {
  let d = null;
  if (/^\d+$/.test(arg)) {
    d = await superset.dashboardDetail(arg);
  } else {
    d = all.find((x) => x.slug === arg) || null;
    if (!d) {
      const detail = await superset.api(
        `/api/v1/dashboard/?q=${encodeURIComponent(`(page:0,page_size:100,filters:!((col:slug,opr:eq,value:${arg})))`)}`,
      );
      const hit = (detail.result || []).find((x) => x.slug === arg);
      if (hit) d = await superset.dashboardDetail(hit.id);
    }
  }
  if (!d) return null;
  const full = d.charts && Array.isArray(d.charts) ? d : await superset.dashboardDetail(d.id);
  const charts = full && Array.isArray(full.charts)
    ? full.charts.length
    : (full && Array.isArray(full.slices) ? full.slices.length : null);
  return { id: d.id, slug: d.slug || null, title: d.dashboard_title || null, charts, published: d.published };
}

// --- CDP -------------------------------------------------------------------

class CDP {
  constructor(ws) {
    this.ws = ws;
    this.id = 0;
    this.pending = new Map();
    this.logs = [];
    ws.addEventListener('message', (ev) => {
      let msg;
      try { msg = JSON.parse(ev.data); } catch (e) { return; }
      if (msg.id && this.pending.has(msg.id)) {
        const { res, rej } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        if (msg.error) rej(new Error(`${msg.error.message} (${JSON.stringify(msg.error.data || '')})`));
        else res(msg.result);
        return;
      }
      if (msg.method === 'Log.entryAdded' && msg.params.entry.level === 'error') {
        this.logs.push(String(msg.params.entry.text).slice(0, 200));
      }
      if (msg.method === 'Runtime.consoleAPICalled' && msg.params.type === 'error') {
        this.logs.push(msg.params.args.map((a) => String(a.value || a.description || '')).join(' ').slice(0, 200));
      }
    });
  }

  send(method, params = {}, timeoutMs = 30000) {
    return new Promise((resolve, reject) => {
      const id = ++this.id;
      this.pending.set(id, { res: resolve, rej: reject });
      this.ws.send(JSON.stringify({ id, method, params }));
      setTimeout(() => {
        if (this.pending.has(id)) {
          this.pending.delete(id);
          reject(new Error(`CDP timeout: ${method}`));
        }
      }, timeoutMs);
    });
  }

  /** js evaluator calls stay short and synchronous — all waiting is on this side. */
  async evaluate(expression) {
    const r = await this.send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: false }, 20000);
    if (r.exceptionDetails) {
      throw new Error('evaluate failed: ' + JSON.stringify(r.exceptionDetails.exception || r.exceptionDetails));
    }
    return r.result.value;
  }
}

const SCAN_JS = `(() => {
  const re = new RegExp(${JSON.stringify(ERROR_RE.source)}, ${JSON.stringify(ERROR_RE.flags)});
  const comps = Array.from(document.querySelectorAll('.dashboard-component'));
  const texts = comps.map(c => (c.innerText || '').trim().replace(/\\s+/g, ' '));
  const bad = texts.filter(t => t.length > 3 && re.test(t));
  return JSON.stringify({
    path: location.pathname,
    title: (document.title || '').trim(),
    body_head: (document.body ? document.body.innerText : '').trim().replace(/\\s+/g, ' ').slice(0, 400),
    comps: comps.length,
    errs: bad.length,
    broken: bad.slice(0, 6).map(t => t.slice(0, 110)),
    on_login: location.pathname.startsWith('/login'),
  });
})()`;

const PROBE_JS = `JSON.stringify({p: location.pathname, c: document.querySelectorAll('.dashboard-component').length})`;

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.on('error', reject);
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

function findChromium() {
  const candidates = env.CHROMIUM_BIN
    ? [env.CHROMIUM_BIN]
    : ['/usr/sbin/chromium', '/usr/bin/chromium', '/usr/bin/chromium-browser', '/usr/bin/google-chrome'];
  for (const c of candidates) {
    try {
      fs.accessSync(c, fs.constants.X_OK);
      return c;
    } catch (e) { /* next */ }
  }
  return null;
}

async function waitForCdp(port, tries = 60) {
  for (let i = 0; i < tries; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/json/version`);
      if (r.ok) return await r.json();
    } catch (e) { /* not up yet */ }
    await sleep(500);
  }
  throw new HarnessError('chromium CDP endpoint never came up');
}

// --- the scan itself -------------------------------------------------------

/** The SPA keeps whichever form was requested in the URL (numeric id or slug),
 *  so both are acceptable — but the rendered page must be the dashboard we asked
 *  for, which is what the title check pins down. */
function pathOk(rendered, target) {
  const norm = (p) => String(p || '').replace(/\/+$/, '');
  const p = norm(rendered);
  const allowed = [];
  if (target.slug) allowed.push(`${DASH_BASE}/${target.slug}`);
  if (target.id) allowed.push(`${DASH_BASE}/${target.id}`);
  return allowed.includes(p);
}

async function scanDashboard(cdp, superset, arg, target, url) {
  const row = {
    dash: arg,
    expected_id: target.id,
    expected_slug: target.slug,
    expected_title: target.title,
    expected_charts: target.charts,
    url,
    landed_on_expected: false,
    path: null,
    title: null,
    body_head: null,
    comps: 0,
    errs: 0,
    empty: false,
    broken: [],
    attempts: 0,
    on_login: false,
    js_errors: [],
  };
  let snap = null;
  for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
    row.attempts = attempt;
    cdp.logs.length = 0;
    await cdp.send('Page.navigate', { url });
    const deadline = Date.now() + DASH_TIMEOUT_MS;
    let last = -1, stable = 0;
    while (Date.now() < deadline) {
      await sleep(POLL_MS);
      let probe;
      try { probe = await cdp.evaluate(PROBE_JS); } catch (e) { continue; }
      const o = JSON.parse(probe);
      if (o.c > 0 && o.c === last) stable += 1; else stable = 0;
      last = o.c;
      if (stable >= 2) break;
    }
    await sleep(SETTLE_MS);
    snap = JSON.parse(await cdp.evaluate(SCAN_JS));
    // Give slow tiles a second look before calling anything clean: a tile that is
    // still querying renders its error late (observed with a stale dataset).
    if (snap.errs > 0 && !snap.on_login) {
      await sleep(EXTRA_WAIT_MS);
      snap = JSON.parse(await cdp.evaluate(SCAN_JS));
    }
    const landed = !snap.on_login && pathOk(snap.path, target) &&
      (!target.title || snap.title === target.title);
    row.landed_on_expected = landed;
    if (landed) break;
    warn(`  retry ${arg} (attempt ${attempt}): rendered ${snap.path} "${snap.title}"` +
         ` — want ${target.slug || target.id} "${target.title || arg}"`);
    await sleep(2000);
  }
  row.path = snap.path;
  row.title = snap.title;
  row.body_head = snap.body_head;
  row.comps = snap.comps;
  row.errs = snap.errs;
  row.broken = snap.broken;
  row.on_login = snap.on_login;
  row.js_errors = cdp.logs.slice(0, 5);
  if (row.landed_on_expected) {
    row.empty = Number(target.charts || 0) > 0 && snap.comps === 0;
  }
  return row;
}

function verdict(row) {
  if (!row.landed_on_expected) return EXIT_WRONG_PAGE;
  if (row.errs > 0 || row.empty) return EXIT_TILES;
  return EXIT_CLEAN;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) { usage(); return EXIT_CLEAN; }
  if (!args.host) { usage(); return EXIT_HARNESS; }
  if (typeof WebSocket === 'undefined') {
    throw new HarnessError(`node ${process.version} has no global WebSocket — node >= 22 is required`);
  }
  const port = Number(env.SUPERSET_PORT || 8088);
  const superset = new Superset(args.host, port);
  log(`dom-scan | ${superset.base} | dashboards: ${args.targets.length ? args.targets.join(',') : '(all published, discovered via API)'}`);

  const auth = await superset.login(env.SUPERSET_USER || 'admin', env.SUPERSET_PASSWORD || 'admin');
  log(`login: POST /login/ -> ${auth.status}, session cookie minted, /api/v1/me -> ${auth.user}`);

  const all = await superset.listDashboards();
  let targets;
  if (args.targets.length) {
    targets = args.targets;
  } else {
    targets = all
      .filter((d) => d.published !== false)
      .sort((a, b) => a.id - b.id)
      .map((d) => d.slug || String(d.id));
    log(`discovered ${targets.length} published dashboard(s) of ${all.length} on the instance`);
  }

  const chromiumBin = findChromium();
  if (!chromiumBin) {
    throw new HarnessError('no Chromium binary found (set CHROMIUM_BIN; the DOM gate cannot run without a browser)');
  }
  const cdpPort = Number(env.CDP_PORT || await freePort());
  const profileDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dom-scan-profile-'));
  const chrome = spawn(chromiumBin, [
    '--headless=new', '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
    '--disable-features=Translate', '--no-first-run', '--hide-scrollbars',
    `--remote-debugging-port=${cdpPort}`,
    `--user-data-dir=${profileDir}`,
    'about:blank',
  ], { stdio: 'ignore' });
  let chromeFail = null;
  chrome.on('error', (e) => { chromeFail = e; });

  const results = [];
  try {
    const version = await waitForCdp(cdpPort);
    log(`chromium: ${version.Browser} on CDP ${cdpPort}`);
    const list = await (await fetch(`http://127.0.0.1:${cdpPort}/json/list`)).json();
    const page = list.find((t) => t.type === 'page');
    if (!page) throw new HarnessError('no page target in the Chromium instance');
    const ws = new WebSocket(page.webSocketDebuggerUrl);
    await new Promise((res, rej) => {
      ws.addEventListener('open', res);
      ws.addEventListener('error', () => rej(new HarnessError('CDP websocket refused')));
    });
    const cdp = new CDP(ws);
    await cdp.send('Page.enable');
    await cdp.send('Runtime.enable');
    await cdp.send('Log.enable');
    await cdp.send('Network.enable');
    await cdp.send('Network.setCookie', {
      name: 'session', value: superset.session, domain: args.host, path: '/', httpOnly: true,
    });

    for (const arg of targets) {
      const target = await resolveTarget(superset, all, arg);
      if (!target) {
        // A target the API cannot resolve cannot be certified: whatever the
        // browser lands on would be scanned under the wrong name = false pass.
        results.push({
          dash: arg, unresolved: true, landed_on_expected: false, path: null, title: null,
          comps: 0, errs: 0, empty: false, broken: [], url: null,
        });
        warn(`!! UNRESOLVED: '${arg}' is not a slug or id on this instance — refusing to certify it (exit 2)`);
        continue;
      }
      const url = `${superset.base}${DASH_BASE}/${target.slug || target.id}/`;
      const row = await scanDashboard(cdp, superset, arg, target, url);
      row.verdict = verdict(row);
      results.push(row);
      const flags = [];
      if (row.on_login) flags.push('ON /login/ — SESSION LOST');
      if (!row.landed_on_expected) flags.push('WRONG PAGE');
      if (row.empty) flags.push('NO TILES RENDERED');
      log(`dash ${arg}: path=${row.path} comps=${row.comps}/${row.expected_charts === null ? '?' : row.expected_charts} charts` +
          ` errs=${row.errs} title="${row.title}"${flags.length ? '  !! ' + flags.join(', ') : ''}`);
      if (row.broken.length) log(`   broken: ${JSON.stringify(row.broken.slice(0, 3))}`);
      if (row.js_errors.length) log(`   js errors: ${JSON.stringify(row.js_errors.slice(0, 2))}`);
    }
    ws.close();
  } catch (e) {
    if (chromeFail) throw new HarnessError(`chromium failed to start: ${chromeFail.message}`);
    throw e;
  } finally {
    chrome.kill('SIGKILL');
    try { fs.rmSync(profileDir, { recursive: true, force: true }); } catch (e) { /* best effort */ }
  }

  const wrong = results.filter((r) => verdict(r) === EXIT_WRONG_PAGE);
  const tiled = results.filter((r) => verdict(r) === EXIT_TILES);
  const clean = results.filter((r) => verdict(r) === EXIT_CLEAN);
  const totalErrs = results.reduce((n, r) => n + (r.errs || 0), 0);

  log('');
  log(`JSON ${JSON.stringify(results, null, 1)}`);
  log('');
  for (const r of wrong) {
    warn(`  WRONG PAGE      ${r.dash}: ${r.unresolved ? 'unresolved target' : `${r.path} "${r.title}"`}` +
         (r.on_login ? ' (landed on /login/ — the session did not take)' : ''));
  }
  for (const r of tiled) {
    warn(`  ERROR TILES     ${r.dash}: ${r.empty ? 'chart(s) expected but the page rendered 0 tiles' : `${r.errs} error tile(s)`}` +
         (r.broken.length ? ` — ${JSON.stringify(r.broken.slice(0, 2))}` : ''));
  }

  let worst = EXIT_CLEAN;
  for (const r of results) worst = Math.max(worst, verdict(r));
  const summary = `${clean.length} clean, ${tiled.length} with error tiles, ${wrong.length} wrong page` +
                  ` (${totalErrs} error tile(s) total)`;
  if (worst === EXIT_CLEAN) {
    log(`DOM SCAN: PASS — ${summary}`);
  } else {
    warn(`DOM SCAN: FAIL (exit ${worst}) — ${summary}`);
  }

  if (args.json) {
    const report = {
      host: args.host, base: superset.base, user: auth.user, scanned_at: new Date().toISOString(),
      exit_code: worst, clean: clean.length, with_error_tiles: tiled.length, wrong_page: wrong.length,
      total_error_tiles: totalErrs, dashboards: results,
    };
    fs.writeFileSync(args.json, JSON.stringify(report, null, 1) + '\n');
    log(`report: ${args.json}`);
  }
  return worst;
}

if (require.main === module) {
  main()
    .then((code) => process.exit(code))
    .catch((e) => {
      warn(`DOM SCAN HARNESS FAILURE: ${e.message}`);
      process.exit(EXIT_HARNESS);
    });
}

// Exported for test-dom-scan-guard.sh, which asserts the false-pass guards and
// the regex coverage offline (no host, no browser). `require.main` keeps the
// import from starting a scan.
module.exports = {
  ERROR_RE, pathOk, verdict, resolveTarget, parseArgs,
  EXIT_CLEAN, EXIT_TILES, EXIT_WRONG_PAGE, EXIT_HARNESS,
};
