const fs = require("fs");
const path = require("path");
const net = require("net");
const http = require("http");
const crypto = require("crypto");
const { spawn, spawnSync } = require("child_process");

const WORKDIR = path.resolve(process.env.RHIZOME_TOOL_WORKDIR || process.cwd());
const DEFAULT_ARTIFACT_ROOT = path.join(WORKDIR, ".runtime-config", "tool-artifacts", "browser_session", timestamp());
const ARTIFACT_ROOT = path.resolve(process.env.RHIZOME_TOOL_ARTIFACT_DIR || DEFAULT_ARTIFACT_ROOT);
const RESULT_PATH = path.join(ARTIFACT_ROOT, "result.json");
const SESSION_ROOT = path.join(WORKDIR, ".runtime-config", "browser-sessions");
const activeSessionLocks = [];

process.on("exit", () => {
  for (const lock of [...activeSessionLocks].reverse()) {
    releaseSessionLock(lock);
  }
});

function timestamp() {
  return new Date().toISOString().replace(/[:.]/g, "").replace("Z", "Z");
}

function readInput() {
  const raw = fs.readFileSync(0, "utf8").trim();
  return raw ? JSON.parse(raw) : {};
}

function writeResult(result) {
  fs.writeSync(1, JSON.stringify(result, null, 2));
}

function finish(status, extra = {}) {
  const result = {
    contract_version: "browser_session_result_v1",
    tool: "browser_session",
    status,
    workdir: WORKDIR,
    artifact_root: ARTIFACT_ROOT,
    ...extra
  };
  try {
    fs.mkdirSync(ARTIFACT_ROOT, { recursive: true });
    fs.writeFileSync(RESULT_PATH, JSON.stringify(result, null, 2), "utf8");
  } catch (_) {}
  try { writeResult(result); } catch (_) {}
  process.exit(0);
}

function fail(reason, extra = {}) {
  finish("fail", { reason, ...extra });
}

function block(reason, extra = {}) {
  finish("block", { reason, ...extra });
}

function summary(value, limit = 1600) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return text.length <= limit ? text : text.slice(0, limit) + "...";
}

function boolArg(value) {
  if (value === true) return true;
  if (value === false || value == null) return false;
  const text = String(value).trim().toLowerCase();
  return text === "true" || text === "1" || text === "yes" || text === "on";
}

function sanitizeID(value) {
  return String(value || "default").replace(/[^a-zA-Z0-9_.-]+/g, "_").slice(0, 64) || "default";
}

function isInside(root, candidate) {
  const rel = path.relative(path.resolve(root), path.resolve(candidate));
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

function sessionPaths(sessionID) {
  const id = sanitizeID(sessionID);
  const dir = path.join(SESSION_ROOT, id);
  return {
    id,
    dir,
    profileDir: path.join(dir, "profile"),
    statePath: path.join(dir, "session.json")
  };
}

function sessionLockPath(paths) {
  return path.join(SESSION_ROOT, ".locks", `${paths.id}.lock`);
}

function lockLooksStale(lockPath, staleMs) {
  try {
    const raw = fs.readFileSync(lockPath, "utf8");
    const data = raw ? JSON.parse(raw) : {};
    if (data && Number(data.pid) > 0 && !processExists(Number(data.pid))) return true;
  } catch (_) {}
  try {
    const stat = fs.statSync(lockPath);
    return Date.now() - stat.mtimeMs > staleMs;
  } catch (_) {
    return false;
  }
}

async function acquireSessionLock(paths, action) {
  const lockPath = sessionLockPath(paths);
  fs.mkdirSync(path.dirname(lockPath), { recursive: true });
  const timeoutMs = Math.max(10000, Math.min(120000, Number(process.env.RHIZOME_BROWSER_SESSION_LOCK_TIMEOUT_MS) || 90000));
  const staleMs = Math.max(timeoutMs * 2, 120000);
  const deadline = Date.now() + timeoutMs;
  let lastHolder = null;
  while (Date.now() < deadline) {
    try {
      const fd = fs.openSync(lockPath, "wx");
      const holder = {
        contract_version: "browser_session_lock_v1",
        session_id: paths.id,
        action,
        pid: process.pid,
        created_at: new Date().toISOString()
      };
      fs.writeFileSync(fd, JSON.stringify(holder, null, 2), "utf8");
      const lock = { fd, lockPath, released: false };
      activeSessionLocks.push(lock);
      return lock;
    } catch (error) {
      if (error && error.code !== "EEXIST") throw error;
      try { lastHolder = readJSONFile(lockPath); } catch (_) {}
      if (lockLooksStale(lockPath, staleMs)) {
        try { fs.rmSync(lockPath, { force: true }); } catch (_) {}
        continue;
      }
      await sleep(200);
    }
  }
  block("browser session action timed out waiting for session lock", {
    session_id: paths.id,
    action,
    lock_path: lockPath,
    lock_holder: lastHolder,
    retry_guidance: "Retry after the current browser_session action finishes, or close the stale session if no browser action is running."
  });
}

function releaseSessionLock(lock) {
  if (!lock || lock.released) return;
  lock.released = true;
  try { fs.closeSync(lock.fd); } catch (_) {}
  try { fs.rmSync(lock.lockPath, { force: true }); } catch (_) {}
  const idx = activeSessionLocks.indexOf(lock);
  if (idx >= 0) activeSessionLocks.splice(idx, 1);
}

async function waitForSessionStateBeforeAction(paths, action) {
  if (action === "open" || action === "close") return;
  if (fs.existsSync(paths.statePath)) return;
  const timeoutMs = Math.max(1000, Math.min(30000, Number(process.env.RHIZOME_BROWSER_SESSION_STATE_WAIT_MS) || 15000));
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fs.existsSync(paths.statePath)) return;
    await sleep(200);
  }
}

async function withSessionLock(paths, action, fn) {
  await waitForSessionStateBeforeAction(paths, action);
  const lock = await acquireSessionLock(paths, action);
  try {
    return await fn();
  } finally {
    releaseSessionLock(lock);
  }
}

function freshProfileDir(paths) {
  const suffix = `${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
  return path.join(paths.dir, `profile-${suffix}`);
}

function saveState(paths, state) {
  fs.mkdirSync(paths.dir, { recursive: true });
  fs.writeFileSync(paths.statePath, JSON.stringify(state, null, 2), "utf8");
  try {
    fs.mkdirSync(ARTIFACT_ROOT, { recursive: true });
    fs.writeFileSync(path.join(ARTIFACT_ROOT, "session-state.json"), JSON.stringify(state, null, 2), "utf8");
  } catch (_) {
    // The runtime session state is primary; artifact state is best-effort evidence.
  }
}

function loadState(paths) {
  if (!fs.existsSync(paths.statePath)) return null;
  return JSON.parse(fs.readFileSync(paths.statePath, "utf8"));
}

function readMaxBrowserSessions() {
  const anatomyPath = path.join(WORKDIR, "agent.anatomy.json");
  try {
    const raw = fs.readFileSync(anatomyPath, "utf8").replace(/^\uFEFF/, "");
    const anatomy = JSON.parse(raw);
    const value = Number(anatomy && anatomy.concurrency && anatomy.concurrency.max_browser_sessions);
    return Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;
  } catch (_) {
    return 0;
  }
}

function listSessionDirectories() {
  if (!fs.existsSync(SESSION_ROOT)) return [];
  return fs.readdirSync(SESSION_ROOT, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name !== ".locks")
    .map((entry) => sessionPaths(entry.name))
    .filter((paths) => isInside(SESSION_ROOT, paths.dir));
}

function pruneDeadSessionState() {
  const active = [];
  for (const paths of listSessionDirectories()) {
    const state = loadState(paths);
    if (!state || !processExists(state.pid)) {
      try { fs.rmSync(paths.dir, { recursive: true, force: true }); } catch (_) {}
      continue;
    }
    active.push({ paths, state });
  }
  return active;
}

function structurallyOwnedSession(state, paths) {
  if (!state || typeof state !== "object") return false;
  if (state.session_id !== paths.id) return false;
  const profileDir = typeof state.profile_dir === "string" ? path.resolve(state.profile_dir) : "";
  if (!profileDir || !isInside(paths.dir, profileDir)) return false;
  if (typeof state.ownership_nonce !== "string" || state.ownership_nonce.length < 16) return false;
  const markerPath = typeof state.owner_marker_path === "string" && state.owner_marker_path
    ? path.resolve(state.owner_marker_path)
    : ownershipMarkerPath(profileDir);
  if (!markerPath || !isInside(profileDir, markerPath)) return false;
  const marker = readJSONFile(markerPath);
  return Boolean(marker &&
    marker.contract_version === "browser_session_owner_v1" &&
    marker.session_id === paths.id &&
    marker.nonce === state.ownership_nonce &&
    path.resolve(marker.profile_dir || "") === profileDir &&
    marker.workdir === WORKDIR);
}

function enforceBrowserSessionLimit(paths) {
  const max = readMaxBrowserSessions();
  if (max <= 0) return;
  const active = pruneDeadSessionState()
    .filter((item) => item.paths.id !== paths.id)
    .filter((item) => structurallyOwnedSession(item.state, item.paths));
  if (active.length >= max) {
    block("browser session limit reached", {
      max_browser_sessions: max,
      active_sessions: active.map((item) => ({ session_id: item.paths.id, pid: item.state.pid, url: item.state.url || "" })),
      retry_guidance: "Close an existing browser_session with action=close or action=close_all before opening another."
    });
  }
}

function fileURL(filePath) {
  const normalized = path.resolve(filePath).replace(/\\/g, "/");
  return "file:///" + normalized.replace(/^\/+/, "");
}

function resolveURL(args) {
  const raw = String(args.url || args.html_path || "").trim();
  if (!raw) return "about:blank";
  if (/^https?:\/\//i.test(raw) || /^file:\/\//i.test(raw) || raw === "about:blank") {
    if (/^file:\/\//i.test(raw)) {
      const parsed = new URL(raw);
      const filePath = decodeURIComponent(parsed.pathname.replace(/^\/([A-Za-z]:\/)/, "$1"));
      const resolved = path.resolve(filePath);
      if (!fs.existsSync(resolved)) block("file URL target does not exist", { url: raw });
      return fileURL(resolved);
    }
    return raw;
  }
  const resolved = path.isAbsolute(raw) ? path.resolve(raw) : path.resolve(WORKDIR, raw);
  if (!fs.existsSync(resolved)) block("local browser target does not exist", { requested: raw, resolved });
  if (!fs.statSync(resolved).isFile()) block("local browser target is not a file", { requested: raw, resolved });
  return fileURL(resolved);
}

function candidateBrowsers() {
  const out = [];
  if (process.env.RHIZOME_BROWSER_CANDIDATES) {
    return [...new Set(process.env.RHIZOME_BROWSER_CANDIDATES
      .split(path.delimiter)
      .map((value) => value.trim())
      .filter(Boolean))];
  }
  for (const key of ["BROWSER", "CHROME_PATH", "EDGE_PATH"]) {
    if (process.env[key] && !/^(none|false|0)$/i.test(String(process.env[key]).trim())) out.push(process.env[key]);
  }
  if (process.platform === "win32") {
    const bases = [
      process.env.PROGRAMFILES,
      process.env.ProgramFiles,
      process.env.ProgramW6432,
      process.env["PROGRAMFILES(X86)"],
      process.env["ProgramFiles(x86)"],
      "C:\\Program Files",
      "C:\\Program Files (x86)"
    ].filter(Boolean);
    const localBases = [
      process.env.LOCALAPPDATA,
      process.env.LocalAppData
    ].filter(Boolean);
    for (const base of bases) {
      out.push(
        path.join(base, "Google", "Chrome", "Application", "chrome.exe"),
        path.join(base, "Microsoft", "Edge", "Application", "msedge.exe")
      );
    }
    for (const base of localBases) {
      out.push(
        path.join(base, "Google", "Chrome", "Application", "chrome.exe"),
        path.join(base, "Microsoft", "Edge", "Application", "msedge.exe")
      );
    }
    out.push("chrome.exe", "msedge.exe", "chromium.exe");
  } else if (process.platform === "darwin") {
    out.push(
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
      "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
      "/Applications/Chromium.app/Contents/MacOS/Chromium"
    );
  }
  out.push("chrome", "chromium-browser", "chromium", "google-chrome");
  return [...new Set(out.filter(Boolean))];
}

function pathExts() {
  if (process.platform !== "win32") return [""];
  const raw = process.env.PATHEXT || ".COM;.EXE;.BAT;.CMD";
  return raw.split(";").map((ext) => ext.trim()).filter(Boolean);
}

function resolveBrowserCommand(candidate) {
  const hasSeparator = candidate.includes("\\") || candidate.includes("/");
  if (hasSeparator) return fs.existsSync(candidate) ? candidate : "";
  const pathDirs = String(process.env.PATH || "")
    .split(path.delimiter)
    .map((dir) => dir.trim())
    .filter(Boolean);
  const exts = path.extname(candidate) ? [""] : pathExts();
  for (const dir of pathDirs) {
    for (const ext of exts) {
      const full = path.join(dir, candidate + ext);
      if (fs.existsSync(full)) return full;
    }
  }
  return "";
}

function findBrowser() {
  for (const candidate of candidateBrowsers()) {
    const browser = resolveBrowserCommand(candidate);
    if (browser) return browser;
  }
  return "";
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const port = server.address().port;
      server.close(() => resolve(port));
    });
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function requestLocalJSON(url, method = "GET") {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const req = http.request({
      hostname: parsed.hostname,
      port: Number(parsed.port || 80),
      path: parsed.pathname + parsed.search,
      method,
      timeout: 8000
    }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        let json = null;
        try { json = text ? JSON.parse(text) : null; } catch (error) {
          reject(new Error(`${url} returned invalid JSON: ${summary(error.message, 300)}`));
          return;
        }
        resolve({ ok: res.statusCode >= 200 && res.statusCode < 300, status: res.statusCode, json });
      });
    });
    req.on("timeout", () => req.destroy(new Error(`${url} timed out`)));
    req.on("error", reject);
    req.end();
  });
}

async function fetchJSON(url, options = {}) {
  const response = await requestLocalJSON(url, options.method || "GET");
  if (!response.ok) throw new Error(`${url} returned HTTP ${response.status}`);
  return response.json;
}

async function waitForCDP(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    try {
      return await fetchJSON(`http://127.0.0.1:${port}/json/version`);
    } catch (error) {
      last = error && error.message ? error.message : String(error);
      await sleep(250);
    }
  }
  throw new Error(`browser remote debugging endpoint did not become ready: ${last}`);
}

async function waitForDevToolsPort(profileDir, timeoutMs) {
  const activePortPath = path.join(profileDir, "DevToolsActivePort");
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    try {
      const raw = fs.readFileSync(activePortPath, "utf8");
      const firstLine = String(raw || "").split(/\r?\n/)[0];
      const port = Number(firstLine);
      if (Number.isFinite(port) && port > 0) return port;
      last = `invalid DevToolsActivePort content: ${summary(raw, 200)}`;
    } catch (error) {
      last = error && error.message ? error.message : String(error);
    }
    await sleep(100);
  }
  throw new Error(`browser DevToolsActivePort did not become ready: ${last}`);
}

async function waitForBrowserDebugEndpoint(port, profileDir, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let resolvedPort = Number(port) || 0;
  if (resolvedPort <= 0) {
    resolvedPort = await waitForDevToolsPort(profileDir, Math.max(1000, deadline - Date.now()));
  }
  const version = await waitForCDP(resolvedPort, Math.max(1000, deadline - Date.now()));
  return { port: resolvedPort, version };
}

async function waitForBrowserReady(child, port, profileDir, timeoutMs) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const done = (fn, value) => {
      if (settled) return;
      settled = true;
      child.off("error", onError);
      child.off("exit", onExit);
      fn(value);
    };
    const onError = (error) => done(reject, error);
    const onExit = (code, signal) => {
      done(reject, new Error(`browser process exited before CDP ready: code=${code} signal=${signal || ""}`));
    };
    child.once("error", onError);
    child.once("exit", onExit);
    waitForBrowserDebugEndpoint(port, profileDir, timeoutMs).then(
      (ready) => done(resolve, ready),
      (error) => done(reject, error)
    );
  });
}

function requestLocalJSONBody(url, method = "POST", body = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const payload = Buffer.from(JSON.stringify(body || {}), "utf8");
    const req = http.request({
      hostname: parsed.hostname,
      port: Number(parsed.port || 80),
      path: parsed.pathname + parsed.search,
      method,
      timeout: 10000,
      headers: {
        "content-type": "application/json",
        "content-length": String(payload.length)
      }
    }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        let json = null;
        try { json = text ? JSON.parse(text) : null; } catch (error) {
          reject(new Error(`${url} returned invalid JSON: ${summary(error.message, 300)}`));
          return;
        }
        resolve({ ok: res.statusCode >= 200 && res.statusCode < 300, status: res.statusCode, json });
      });
    });
    req.on("timeout", () => req.destroy(new Error(`${url} timed out`)));
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

async function brokerRequest(state, action, body = {}, method = "POST") {
  const port = Number(state && state.broker_port);
  if (!Number.isFinite(port) || port <= 0) throw new Error("browser pipe broker port is missing");
  const response = method === "GET"
    ? await requestLocalJSON(`http://127.0.0.1:${port}/${action}`, "GET")
    : await requestLocalJSONBody(`http://127.0.0.1:${port}/${action}`, method, body);
  if (!response.ok) {
    const reason = response.json && (response.json.error || response.json.reason) ? String(response.json.error || response.json.reason) : `HTTP ${response.status}`;
    throw new Error(`browser pipe broker ${action} failed: ${reason}`);
  }
  return response.json;
}

async function waitForBrokerReady(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() < deadline) {
    try {
      const response = await requestLocalJSON(`http://127.0.0.1:${port}/status`, "GET");
      if (response.ok && response.json && response.json.status === "pass") return response.json;
      last = response.json && response.json.reason ? response.json.reason : `HTTP ${response.status}`;
    } catch (error) {
      last = error && error.message ? error.message : String(error);
    }
    await sleep(250);
  }
  throw new Error(`browser pipe broker did not become ready: ${last}`);
}

class PipeCDP {
  constructor(child) {
    this.child = child;
    this.nextID = 1;
    this.pending = new Map();
    this.buffer = Buffer.alloc(0);
    this.closed = false;
    this.callTimeoutMs = Math.max(
      12000,
      Math.min(90000, Number(process.env.RHIZOME_BROWSER_SESSION_PIPE_CDP_TIMEOUT_MS) || 60000)
    );
    child.stdio[4].on("data", (chunk) => this.onData(chunk));
    child.once("exit", (code, signal) => {
      this.closed = true;
      this.rejectAll(new Error(`browser pipe process exited: code=${code} signal=${signal || ""}`));
    });
    child.once("error", (error) => {
      this.closed = true;
      this.rejectAll(error);
    });
  }

  onData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    let idx = this.buffer.indexOf(0);
    while (idx >= 0) {
      const raw = this.buffer.slice(0, idx).toString("utf8");
      this.buffer = this.buffer.slice(idx + 1);
      if (raw.trim()) {
        let message = null;
        try { message = JSON.parse(raw); } catch (_) {}
        if (message && message.id && this.pending.has(message.id)) {
          const pending = this.pending.get(message.id);
          this.pending.delete(message.id);
          if (message.error) pending.reject(new Error(summary(JSON.stringify(message.error), 1000)));
          else pending.resolve(message);
        }
      }
      idx = this.buffer.indexOf(0);
    }
  }

  call(method, params = {}, sessionId = "") {
    if (this.closed) return Promise.reject(new Error("browser pipe is closed"));
    const id = this.nextID++;
    const message = { id, method, params };
    if (sessionId) message.sessionId = sessionId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.has(id)) {
          this.pending.delete(id);
          reject(new Error(`browser pipe CDP call timed out: ${method}`));
        }
      }, this.callTimeoutMs);
      this.pending.set(id, {
        resolve: (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        }
      });
      this.child.stdio[3].write(JSON.stringify(message) + "\0");
    });
  }

  rejectAll(error) {
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
  }
}

function brokerInspectExpression() {
  return `(() => {
    const text = (document.body && document.body.innerText || "").replace(/\\s+/g, " ").trim();
    const overflowing = [];
    for (const el of Array.from(document.querySelectorAll("*")).slice(0, 2500)) {
      const r = el.getBoundingClientRect();
      if (r.width > window.innerWidth + 2 || r.left < -2 || r.right > window.innerWidth + 2) {
        overflowing.push({ tag: el.tagName, id: el.id || "", cls: String(el.className || "").slice(0, 120), left: Math.round(r.left), right: Math.round(r.right), width: Math.round(r.width) });
      }
      if (overflowing.length >= 20) break;
    }
    return {
      title: document.title || "",
      url: location.href,
      viewport: { width: window.innerWidth, height: window.innerHeight, devicePixelRatio: window.devicePixelRatio },
      body_rect: document.body ? (() => { const r = document.body.getBoundingClientRect(); return { width: Math.round(r.width), height: Math.round(r.height) }; })() : null,
      text_chars: text.length,
      text_summary: text.slice(0, 1200),
      overflowing_elements: overflowing,
      active_element: document.activeElement ? { tag: document.activeElement.tagName, id: document.activeElement.id || "", cls: String(document.activeElement.className || "").slice(0, 120) } : null
    };
  })()`;
}

async function runPipeBroker(configPath) {
  const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
  const browser = config.browser;
  const profileDir = config.profile_dir;
  const port = Number(config.broker_port);
  const width = Number(config.width) || 1440;
  const height = Number(config.height) || 1000;
  const headless = config.headless !== false;
  fs.mkdirSync(profileDir, { recursive: true });
  const launchArgs = [
    "--remote-debugging-pipe",
    `--user-data-dir=${profileDir}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-background-networking",
    "--disable-sync",
    "--disable-extensions",
    "--disable-default-apps",
    "--disable-component-update",
    "--disable-features=Translate,MediaRouter",
    "--disable-dev-shm-usage",
    "--allow-file-access-from-files",
    `--window-size=${width},${height}`
  ];
  if (headless) launchArgs.push("--headless=new", "--disable-gpu");
  launchArgs.push("about:blank");

  const child = spawn(browser, launchArgs, {
    cwd: WORKDIR,
    stdio: ["ignore", "ignore", "ignore", "pipe", "pipe"],
    windowsHide: headless
  });
  const cdp = new PipeCDP(child);
  const version = await cdp.call("Browser.getVersion");
  const target = await cdp.call("Target.createTarget", { url: "about:blank" });
  const attach = await cdp.call("Target.attachToTarget", { targetId: target.result.targetId, flatten: true });
  const sessionId = attach.result.sessionId;
  await cdp.call("Page.enable", {}, sessionId).catch(() => {});
  await cdp.call("Runtime.enable", {}, sessionId).catch(() => {});

  let currentUrl = "about:blank";
  const server = http.createServer(async (req, res) => {
    const send = (status, body) => {
      const raw = JSON.stringify(body || {});
      res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
      res.end(raw);
    };
    const readBody = () => new Promise((resolve, reject) => {
      const chunks = [];
      req.on("data", (chunk) => chunks.push(chunk));
      req.on("end", () => {
        const raw = Buffer.concat(chunks).toString("utf8");
        try { resolve(raw ? JSON.parse(raw) : {}); } catch (error) { reject(error); }
      });
      req.on("error", reject);
    });
    try {
      if (req.method === "GET" && req.url === "/status") {
        return send(200, { status: "pass", mode: "pipe_broker", pid: process.pid, browser_pid: child.pid, version: version.result, url: currentUrl });
      }
      const body = await readBody();
      if (req.url === "/goto") {
        const url = resolveURL(body);
        await cdp.call("Page.navigate", { url }, sessionId);
        const waitMs = Math.max(0, Math.min(15000, Number(body.wait_ms) || 1000));
        if (waitMs) await sleep(waitMs);
        currentUrl = url;
        return send(200, { status: "pass", action: "goto", url: currentUrl });
      }
      if (req.url === "/inspect") {
        const waitMs = Math.max(0, Math.min(10000, Number(body.wait_ms) || 0));
        if (waitMs) await sleep(waitMs);
        const evaluated = await cdp.call("Runtime.evaluate", { expression: brokerInspectExpression(), returnByValue: true, awaitPromise: true }, sessionId);
        return send(200, { status: "pass", action: "inspect", page: evaluated.result && evaluated.result.result ? evaluated.result.result.value : evaluated });
      }
      if (req.url === "/screenshot") {
        const shotWidth = Math.max(320, Math.min(3000, Number(body.width) || width));
        const shotHeight = Math.max(240, Math.min(2400, Number(body.height) || height));
        const waitMs = Math.max(0, Math.min(10000, Number(body.wait_ms) || 0));
        if (waitMs) await sleep(waitMs);
        await cdp.call("Emulation.setDeviceMetricsOverride", { width: shotWidth, height: shotHeight, deviceScaleFactor: 1, mobile: false }, sessionId);
        const shot = await cdp.call("Page.captureScreenshot", { format: "png", captureBeyondViewport: true }, sessionId);
        return send(200, { status: "pass", action: "screenshot", data: shot.result.data, width: shotWidth, height: shotHeight });
      }
      if (req.url === "/click") {
        const selector = String(body.selector || "").trim();
        if (!selector) return send(400, { status: "fail", error: "selector is required" });
        const expression = `(sel => { const el = document.querySelector(sel); if (!el) return null; const r = el.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, tag: el.tagName, text: (el.innerText || el.value || "").slice(0, 200) }; })(${JSON.stringify(selector)})`;
        const evaluated = await cdp.call("Runtime.evaluate", { expression, returnByValue: true }, sessionId);
        const rect = evaluated.result && evaluated.result.result ? evaluated.result.result.value : null;
        if (!rect) return send(404, { status: "fail", error: `selector not found: ${selector}` });
        await cdp.call("Input.dispatchMouseEvent", { type: "mouseMoved", x: rect.x, y: rect.y, button: "none" }, sessionId);
        await cdp.call("Input.dispatchMouseEvent", { type: "mousePressed", x: rect.x, y: rect.y, button: "left", clickCount: 1 }, sessionId);
        await cdp.call("Input.dispatchMouseEvent", { type: "mouseReleased", x: rect.x, y: rect.y, button: "left", clickCount: 1 }, sessionId);
        const waitMs = Math.max(0, Math.min(10000, Number(body.wait_ms) || 500));
        if (waitMs) await sleep(waitMs);
        return send(200, { status: "pass", action: "click", clicked: rect });
      }
      if (req.url === "/type") {
        const text = String(body.text || "");
        if (!text) return send(400, { status: "fail", error: "text is required" });
        const selector = String(body.selector || "").trim();
        if (selector) {
          const expression = `(sel => { const el = document.querySelector(sel); if (!el) return false; el.focus(); return true; })(${JSON.stringify(selector)})`;
          const evaluated = await cdp.call("Runtime.evaluate", { expression, returnByValue: true }, sessionId);
          if (!evaluated.result || !evaluated.result.result || evaluated.result.result.value !== true) return send(404, { status: "fail", error: `selector not found or not focusable: ${selector}` });
        }
        await cdp.call("Input.insertText", { text }, sessionId);
        const waitMs = Math.max(0, Math.min(10000, Number(body.wait_ms) || 300));
        if (waitMs) await sleep(waitMs);
        return send(200, { status: "pass", action: "type", typed_chars: text.length, selector });
      }
      if (req.url === "/evaluate") {
        const expression = String(body.expression || "").trim();
        if (!expression) return send(400, { status: "fail", error: "expression is required" });
        const evaluated = await cdp.call("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true }, sessionId);
        if (evaluated.result && evaluated.result.exceptionDetails) return send(500, { status: "fail", error: summary(JSON.stringify(evaluated.result.exceptionDetails), 1000) });
        return send(200, { status: "pass", action: "evaluate", result: evaluated.result ? evaluated.result.result.value : evaluated });
      }
      if (req.url === "/close") {
        send(200, { status: "pass", action: "close", pid: process.pid, browser_pid: child.pid });
        try { await cdp.call("Browser.close"); } catch (_) {}
        setTimeout(() => process.exit(0), 50);
        return;
      }
      return send(404, { status: "fail", error: "unknown broker action" });
    } catch (error) {
      return send(500, { status: "fail", error: error && error.message ? error.message : String(error) });
    }
  });
  server.listen(port, "127.0.0.1", () => {
    try {
      fs.writeFileSync(config.ready_path, JSON.stringify({ status: "pass", mode: "pipe_broker", pid: process.pid, browser_pid: child.pid, port, version: version.result }, null, 2), "utf8");
    } catch (_) {}
  });
}

function killProcessTree(pid) {
  if (!pid || pid <= 0) return "";
  if (process.platform === "win32") {
    const killed = spawnSync("taskkill", ["/PID", String(pid), "/T", "/F"], {
      encoding: "utf8",
      timeout: 8000,
      windowsHide: true
    });
    return summary(`${killed.stdout || ""}\n${killed.stderr || ""}`, 1000);
  }
  try {
    process.kill(-pid, "SIGTERM");
    return "sent SIGTERM to process group";
  } catch (_) {
    try {
      process.kill(pid, "SIGTERM");
      return "sent SIGTERM to process";
    } catch (error) {
      return error && error.message ? error.message : String(error);
    }
  }
}

function pseudoChild(pid, launchTransport) {
  return {
    pid,
    launch_transport: launchTransport,
    unref() {},
    once() { return this; },
    off() { return this; }
  };
}

function psSingleQuoted(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function launchBrowserProcess(browser, launchArgs, headless, options = {}) {
  const daemonSafeHeadlessTransport = "headless_node_spawn_for_daemon_cdp";
  const preferStartProcess = options.prefer_start_process !== false;
  const forceSpawn = process.env.RHIZOME_BROWSER_SESSION_WIN32_FORCE_SPAWN === "1";
  const useStartProcess = process.platform === "win32" &&
    process.env.RHIZOME_BROWSER_SESSION_WIN32_START_PROCESS !== "0" &&
    preferStartProcess &&
    !forceSpawn;
  if (useStartProcess) {
    const effectiveArgs = [...launchArgs];
    let windowStyle = "Hidden";
    if (!headless && process.env.RHIZOME_BROWSER_SESSION_VISIBLE_HEADFUL === "1") {
      windowStyle = "Normal";
    }
    const psArgList = `@(${effectiveArgs.map(psSingleQuoted).join(",")})`;
    const script = [
      "$ErrorActionPreference='Stop'",
      `$p=Start-Process -FilePath ${psSingleQuoted(browser)} -ArgumentList ${psArgList} -PassThru -WindowStyle ${psSingleQuoted(windowStyle)}`,
      "Write-Output $p.Id"
    ].join("; ");
    const psArgs = ["-NoProfile", "-NonInteractive", "-Command", script];
    const result = spawnSync("powershell.exe", psArgs, {
      cwd: WORKDIR,
      encoding: "utf8",
      timeout: 10000,
      windowsHide: true
    });
    if (result.status === 0) {
      const pid = Number(String(result.stdout || "").trim().split(/\s+/).pop());
      if (Number.isFinite(pid) && pid > 0) return pseudoChild(pid, "powershell_start_process");
    }
  }
  const child = spawn(browser, launchArgs, {
    cwd: WORKDIR,
    detached: true,
    stdio: "ignore",
    windowsHide: headless
  });
  child.launch_transport = headless && process.platform === "win32" ? daemonSafeHeadlessTransport : "node_spawn";
  return child;
}

function processExists(pid) {
  if (!pid || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (_) {
    return false;
  }
}

function ownershipMarkerPath(profileDir) {
  return path.join(profileDir, "rhizome-browser-session-owner.json");
}

function writeOwnershipMarker(paths, profileDir, nonce) {
  const markerPath = ownershipMarkerPath(profileDir);
  const marker = {
    contract_version: "browser_session_owner_v1",
    session_id: paths.id,
    nonce,
    workdir: WORKDIR,
    profile_dir: profileDir,
    created_at: new Date().toISOString()
  };
  fs.writeFileSync(markerPath, JSON.stringify(marker, null, 2), "utf8");
  return markerPath;
}

function readJSONFile(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    return null;
  }
}

function processCommandLine(pid) {
  if (!pid || pid <= 0) return "";
  if (process.platform === "linux") {
    try {
      return fs.readFileSync(`/proc/${pid}/cmdline`, "utf8").replace(/\0/g, " ").trim();
    } catch (_) {
      return "";
    }
  }
  if (process.platform === "win32") {
    const command = `Get-CimInstance Win32_Process -Filter "ProcessId=${Number(pid)}" | Select-Object -ExpandProperty CommandLine`;
    const result = spawnSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", command], {
      encoding: "utf8",
      timeout: 4000,
      windowsHide: true
    });
    return result.status === 0 ? String(result.stdout || "").trim() : "";
  }
  const result = spawnSync("ps", ["-p", String(pid), "-o", "command="], {
    encoding: "utf8",
    timeout: 4000
  });
  return result.status === 0 ? String(result.stdout || "").trim() : "";
}

async function validateOwnedSession(state, paths) {
  const checks = {
    session_id: false,
    pid_exists: false,
    profile_inside_session: false,
    nonce_present: false,
    marker_matches_nonce: false,
    cdp_endpoint: false,
    command_line_profile: "not_checked"
  };
  const reasons = [];
  const health_warnings = [];

  if (!state || typeof state !== "object") {
    return { owned: false, checks, reasons: ["missing session state"] };
  }
  checks.session_id = state.session_id === paths.id;
  if (!checks.session_id) reasons.push("state session_id does not match requested session");

  checks.pid_exists = processExists(state.pid);
  if (!checks.pid_exists) reasons.push("process is not running");

  const profileDir = typeof state.profile_dir === "string" ? path.resolve(state.profile_dir) : "";
  checks.profile_inside_session = Boolean(profileDir && isInside(paths.dir, profileDir));
  if (!checks.profile_inside_session) reasons.push("profile_dir is missing or outside the session directory");

  checks.nonce_present = typeof state.ownership_nonce === "string" && state.ownership_nonce.length >= 16;
  if (!checks.nonce_present) reasons.push("ownership nonce is missing");

  const markerPath = typeof state.owner_marker_path === "string" && state.owner_marker_path
    ? path.resolve(state.owner_marker_path)
    : (profileDir ? ownershipMarkerPath(profileDir) : "");
  const marker = markerPath && profileDir && isInside(profileDir, markerPath) ? readJSONFile(markerPath) : null;
  checks.marker_matches_nonce = Boolean(marker &&
    marker.contract_version === "browser_session_owner_v1" &&
    marker.session_id === paths.id &&
    marker.nonce === state.ownership_nonce &&
    path.resolve(marker.profile_dir || "") === profileDir &&
    marker.workdir === WORKDIR);
  if (!checks.marker_matches_nonce) reasons.push("ownership marker is missing or does not match state");

  if (state.mode === "pipe_broker") {
    try {
      await brokerRequest(state, "status", {}, "GET");
      checks.cdp_endpoint = true;
    } catch (error) {
      health_warnings.push(`pipe broker validation failed: ${error && error.message ? error.message : String(error)}`);
    }
  } else {
    try {
      await waitForCDP(state.port, 2500);
      checks.cdp_endpoint = true;
    } catch (error) {
      health_warnings.push(`CDP endpoint validation failed: ${error && error.message ? error.message : String(error)}`);
    }
  }

  const commandLine = processCommandLine(state.pid);
  if (commandLine) {
    const normalizedCommandLine = commandLine.toLowerCase().replace(/\\/g, "/");
    const normalizedProfile = profileDir.toLowerCase().replace(/\\/g, "/");
    const portText = String(state.port || "");
    const dynamicDebugPort = normalizedCommandLine.includes("--remote-debugging-port=0");
    const brokerConfigPath = String(state.broker_config_path || "").toLowerCase().replace(/\\/g, "/");
    const brokerMatches = state.mode === "pipe_broker" && brokerConfigPath && normalizedCommandLine.includes(brokerConfigPath);
    const portMatches = !portText || normalizedCommandLine.includes(portText) || (dynamicDebugPort && checks.cdp_endpoint) || brokerMatches;
    checks.command_line_profile = (normalizedCommandLine.includes(normalizedProfile) || brokerMatches) && portMatches;
    if (!checks.command_line_profile) {
      reasons.push("process command line does not include the owned profile and matching CDP port");
    }
  }

  const owned = checks.session_id &&
    checks.pid_exists &&
    checks.profile_inside_session &&
    checks.nonce_present &&
    checks.marker_matches_nonce &&
    checks.command_line_profile !== false;
  return { owned, checks, reasons, health_warnings };
}

async function waitForProcessExit(pid, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!processExists(pid)) return true;
    await sleep(150);
  }
  return !processExists(pid);
}

async function listTargets(port) {
  return fetchJSON(`http://127.0.0.1:${port}/json/list`);
}

async function createTarget(port, url) {
  const encoded = encodeURIComponent(url || "about:blank");
  let response = await requestLocalJSON(`http://127.0.0.1:${port}/json/new?${encoded}`, "PUT");
  if (!response.ok) {
    response = await requestLocalJSON(`http://127.0.0.1:${port}/json/new?${encoded}`, "GET");
  }
  if (!response.ok) throw new Error(`create browser target returned HTTP ${response.status}`);
  return response.json;
}

async function pickTarget(state, createIfMissing = true) {
  const targets = await listTargets(state.port);
  let target = targets.find((item) => item.id === state.target_id);
  if (!target) target = targets.find((item) => item.type === "page");
  if (!target && createIfMissing) target = await createTarget(state.port, state.url || "about:blank");
  if (!target || !target.webSocketDebuggerUrl) throw new Error("no page target with webSocketDebuggerUrl");
  return target;
}

async function verifySessionReady(state, timeoutMs) {
  await waitForCDP(state.port, timeoutMs);
  await pickTarget(state, false);
}

class CDP {
  constructor(wsURL) {
    this.wsURL = wsURL;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.seq = 0;
    this.pending = new Map();
  }

  async open() {
    const parsed = new URL(this.wsURL);
    if (parsed.protocol !== "ws:") throw new Error("only ws:// CDP endpoints are supported");
    const port = Number(parsed.port || 80);
    const host = parsed.hostname || "127.0.0.1";
    this.socket = net.createConnection({ host, port });
    const key = crypto.randomBytes(16).toString("base64");
    const request = [
      `GET ${parsed.pathname}${parsed.search || ""} HTTP/1.1`,
      `Host: ${host}:${port}`,
      "Upgrade: websocket",
      "Connection: Upgrade",
      `Sec-WebSocket-Key: ${key}`,
      "Sec-WebSocket-Version: 13",
      "",
      ""
    ].join("\r\n");

    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("CDP websocket handshake timed out")), 8000);
      let handshake = Buffer.alloc(0);
      const onData = (chunk) => {
        handshake = Buffer.concat([handshake, chunk]);
        const text = handshake.toString("utf8");
        const idx = text.indexOf("\r\n\r\n");
        if (idx < 0) return;
        clearTimeout(timer);
        this.socket.off("data", onData);
        if (!/^HTTP\/1\.1 101\b/.test(text.slice(0, idx))) {
          reject(new Error("CDP websocket handshake rejected: " + summary(text.slice(0, idx), 500)));
          return;
        }
        const headerBytes = Buffer.byteLength(text.slice(0, idx + 4), "utf8");
        const rest = handshake.slice(headerBytes);
        this.socket.on("data", (data) => this.onSocketData(data));
        if (rest.length) this.onSocketData(rest);
        resolve();
      };
      this.socket.on("data", onData);
      this.socket.on("error", reject);
      this.socket.write(request);
    });
  }

  onSocketData(chunk) {
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 2) {
      const first = this.buffer[0];
      const second = this.buffer[1];
      const opcode = first & 0x0f;
      let length = second & 0x7f;
      let offset = 2;
      if (length === 126) {
        if (this.buffer.length < offset + 2) return;
        length = this.buffer.readUInt16BE(offset);
        offset += 2;
      } else if (length === 127) {
        if (this.buffer.length < offset + 8) return;
        length = Number(this.buffer.readBigUInt64BE(offset));
        offset += 8;
      }
      const masked = (second & 0x80) !== 0;
      let mask;
      if (masked) {
        if (this.buffer.length < offset + 4) return;
        mask = this.buffer.slice(offset, offset + 4);
        offset += 4;
      }
      if (this.buffer.length < offset + length) return;
      let payload = this.buffer.slice(offset, offset + length);
      this.buffer = this.buffer.slice(offset + length);
      if (masked && mask) {
        const unmasked = Buffer.alloc(payload.length);
        for (let i = 0; i < payload.length; i++) unmasked[i] = payload[i] ^ mask[i % 4];
        payload = unmasked;
      }
      if (opcode === 8) {
        this.rejectAll(new Error("CDP websocket closed"));
        return;
      }
      if (opcode !== 1 && opcode !== 0) continue;
      this.onMessage(payload.toString("utf8"));
    }
  }

  onMessage(text) {
    let msg;
    try { msg = JSON.parse(text); } catch (_) { return; }
    if (!msg || typeof msg.id === "undefined") return;
    const pending = this.pending.get(msg.id);
    if (!pending) return;
    this.pending.delete(msg.id);
    if (msg.error) pending.reject(new Error(msg.error.message || JSON.stringify(msg.error)));
    else pending.resolve(msg.result || {});
  }

  rejectAll(error) {
    for (const [id, pending] of this.pending.entries()) {
      this.pending.delete(id);
      pending.reject(error);
    }
  }

  call(method, params = {}) {
    const id = ++this.seq;
    const payload = JSON.stringify({ id, method, params });
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.socket.write(encodeClientWebSocketFrame(payload));
      setTimeout(() => {
        if (this.pending.has(id)) {
          this.pending.delete(id);
          reject(new Error(`${method} timed out`));
        }
      }, 12000);
    });
  }

  async close() {
    try { this.socket && this.socket.destroy(); } catch (_) {}
  }
}

function encodeClientWebSocketFrame(text) {
  const payload = Buffer.from(String(text), "utf8");
  let header;
  if (payload.length < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x81;
    header[1] = 0x80 | payload.length;
  } else if (payload.length <= 0xffff) {
    header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(payload.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(payload.length), 2);
  }
  const mask = crypto.randomBytes(4);
  const masked = Buffer.alloc(payload.length);
  for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ mask[i % 4];
  return Buffer.concat([header, mask, masked]);
}

async function withPage(state, fn) {
  const target = await pickTarget(state);
  const cdp = new CDP(target.webSocketDebuggerUrl);
  await cdp.open();
  try {
    await cdp.call("Page.enable").catch(() => {});
    await cdp.call("Runtime.enable").catch(() => {});
    const result = await fn(cdp, target);
    return { target, result };
  } finally {
    await cdp.close();
  }
}

function launchPipeBrokerProcess(configPath, stdoutPath, stderrPath, launchTransport = "") {
  const brokerEnv = {
    RHIZOME_BROWSER_SESSION_PIPE_BROKER_CHILD: "1",
    RHIZOME_TOOL_WORKDIR: WORKDIR,
    RHIZOME_TOOL_BUNDLE_DIR: process.env.RHIZOME_TOOL_BUNDLE_DIR || path.dirname(__filename)
  };
  const preferStartProcess = process.platform === "win32" &&
    launchTransport !== "node_spawn_detached_pipe_broker" &&
    process.env.RHIZOME_BROWSER_SESSION_PIPE_BROKER_WIN32_START_PROCESS !== "0";
  if (preferStartProcess) {
    const envLines = Object.entries(brokerEnv).map(([key, value]) => `$env:${key}=${psSingleQuoted(value)}`);
    const psArgList = `@(${[__filename, "--pipe-broker", configPath].map(psSingleQuoted).join(",")})`;
    const script = [
      "$ErrorActionPreference='Stop'",
      ...envLines,
      `$p=Start-Process -FilePath ${psSingleQuoted(process.execPath)} -ArgumentList ${psArgList} -PassThru -WindowStyle Hidden -RedirectStandardOutput ${psSingleQuoted(stdoutPath)} -RedirectStandardError ${psSingleQuoted(stderrPath)}`,
      "Write-Output $p.Id"
    ].join("; ");
    const result = spawnSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", script], {
      cwd: path.dirname(__filename),
      encoding: "utf8",
      timeout: 10000,
      windowsHide: true
    });
    if (result.status !== 0) {
      throw new Error(`start pipe broker failed: ${summary((result.stderr || result.stdout || "").trim(), 1000)}`);
    }
    const pid = Number(String(result.stdout || "").trim().split(/\s+/).pop());
    if (!Number.isFinite(pid) || pid <= 0) {
      throw new Error(`start pipe broker did not return a pid: ${summary(result.stdout || result.stderr || "", 1000)}`);
    }
    const child = pseudoChild(pid, "powershell_start_process_pipe_broker");
    child.launch_transport = "powershell_start_process_pipe_broker";
    return child;
  }

  let brokerStdoutFD = null;
  let brokerStderrFD = null;
  try {
    brokerStdoutFD = fs.openSync(stdoutPath, "a");
    brokerStderrFD = fs.openSync(stderrPath, "a");
    const child = spawn(process.execPath, [__filename, "--pipe-broker", configPath], {
      cwd: path.dirname(__filename),
      detached: true,
      stdio: ["ignore", brokerStdoutFD, brokerStderrFD],
      windowsHide: true,
      env: {
        ...process.env,
        ...brokerEnv
      }
    });
    child.launch_transport = "node_spawn_detached_pipe_broker";
    child.unref();
    return child;
  } finally {
    try { if (brokerStdoutFD != null) fs.closeSync(brokerStdoutFD); } catch (_) {}
    try { if (brokerStderrFD != null) fs.closeSync(brokerStderrFD); } catch (_) {}
  }
}

async function openPipeBrokerSession(paths, browser, args, url, width, height, requestedHeadless, openTimeoutMs, cleanupNotes, brokerLaunchMode = "pipe_broker_fallback_after_tcp_cdp_failure", brokerLaunchTransport = "") {
  if (process.env.RHIZOME_BROWSER_SESSION_PIPE_BROKER === "0") {
    throw new Error("pipe broker fallback disabled by RHIZOME_BROWSER_SESSION_PIPE_BROKER=0");
  }
  const brokerPort = await getFreePort();
  const profileDir = freshProfileDir(paths);
  fs.mkdirSync(profileDir, { recursive: true });
  const ownershipNonce = crypto.randomBytes(16).toString("hex");
  const ownerMarkerPath = writeOwnershipMarker(paths, profileDir, ownershipNonce);
  const brokerConfigPath = path.join(paths.dir, "pipe-broker-config.json");
  const readyPath = path.join(paths.dir, "pipe-broker-ready.json");
  const brokerStdoutPath = path.join(paths.dir, "pipe-broker.stdout.log");
  const brokerStderrPath = path.join(paths.dir, "pipe-broker.stderr.log");
  fs.mkdirSync(paths.dir, { recursive: true });
  fs.writeFileSync(brokerConfigPath, JSON.stringify({
    schema: "browser_session_pipe_broker_config.v1",
    browser,
    broker_port: brokerPort,
    profile_dir: profileDir,
    ready_path: readyPath,
    width,
    height,
    headless: true,
    requested_headless: requestedHeadless,
    created_at: new Date().toISOString()
  }, null, 2), "utf8");

  const child = launchPipeBrokerProcess(brokerConfigPath, brokerStdoutPath, brokerStderrPath, brokerLaunchTransport);
  try {
    const ready = await waitForBrokerReady(brokerPort, openTimeoutMs);
    const state = {
      session_id: paths.id,
      mode: "pipe_broker",
      pid: child.pid,
      broker_port: brokerPort,
      port: 0,
      browser,
      profile_dir: profileDir,
      ownership_nonce: ownershipNonce,
      owner_marker_path: ownerMarkerPath,
      broker_config_path: brokerConfigPath,
      broker_ready_path: readyPath,
      broker_stdout_path: brokerStdoutPath,
      broker_stderr_path: brokerStderrPath,
      headless: true,
      requested_headless: requestedHeadless,
      launch_mode: brokerLaunchMode,
      launch_transport: child.launch_transport || "pipe_broker_remote_debugging_pipe",
      debug_port_source: "pipe_broker_no_tcp_cdp",
      url,
      opened_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };
    let navigation = { attempted: false, error: "" };
    if (url !== "about:blank") {
      navigation.attempted = true;
      try {
        await brokerRequest(state, "goto", { url, wait_ms: Math.max(0, Math.min(10000, Number(args.wait_ms) || 800)) });
      } catch (error) {
        navigation.error = error && error.message ? error.message : String(error);
      }
    }
    await brokerRequest(state, "status", {}, "GET");
    saveState(paths, state);
    finish("pass", {
      action: "open",
      session: state,
      browser_version: ready.version && (ready.version.Browser || ready.version.product) || "",
      navigation,
      requested_headless: requestedHeadless,
      effective_headless: true,
      launch_mode: state.launch_mode,
      launch_transport: state.launch_transport,
      debug_port_source: state.debug_port_source,
      pipe_broker_fallback_used: true,
      open_timeout_ms: openTimeoutMs,
      cleanup_notes: cleanupNotes
    });
  } catch (error) {
    const cleanup_note = child && child.pid ? killProcessTree(child.pid) : "";
    let broker_stderr_tail = "";
    try { broker_stderr_tail = summary(fs.readFileSync(brokerStderrPath, "utf8"), 1600); } catch (_) {}
    cleanupNotes.push({
      attempt: "pipe_broker",
      launch_mode: brokerLaunchMode,
      requested_headless: requestedHeadless,
      effective_headless: true,
      launch_transport: child && child.launch_transport ? child.launch_transport : "pipe_broker_remote_debugging_pipe",
      debug_port_source: "pipe_broker_no_tcp_cdp",
      open_timeout_ms: openTimeoutMs,
      error: error && error.message ? error.message : String(error),
      broker_stderr_tail,
      cleanup_note: summary(cleanup_note, 800)
    });
    try { fs.rmSync(profileDir, { recursive: true, force: true }); } catch (_) {}
    throw error;
  }
}

async function openSession(paths, args) {
  const browser = findBrowser();
  if (!browser) block("no browser executable found", {
    searched_candidates: candidateBrowsers().slice(0, 20),
    retry_guidance: "Set BROWSER, CHROME_PATH, EDGE_PATH, RHIZOME_BROWSER_CANDIDATES, or add Chrome/Edge/Chromium to PATH."
  });
  const existing = loadState(paths);
  if (existing) {
    const closed = await closeOwnedSession(existing, paths, true);
    if (closed.blocked) {
      block("existing browser session ownership could not be validated; skipped pid kill", {
        session_id: paths.id,
        ownership_validation: closed.ownership_validation,
        cleanup_note: closed.cleanup_note
      });
    }
  } else {
    try { fs.rmSync(paths.dir, { recursive: true, force: true }); } catch (_) {}
  }
  enforceBrowserSessionLimit(paths);
  const url = resolveURL(args);
  const width = Math.max(320, Math.min(3000, Number(args.width) || 1440));
  const height = Math.max(240, Math.min(2400, Number(args.height) || 1000));
  const requestedHeadless = boolArg(args.headless);
  const defaultOpenTimeoutMs = requestedHeadless && process.platform === "win32" ? 60000 : 20000;
  const openTimeoutMs = Math.max(
    5000,
    Math.min(90000, Number(args.open_timeout_ms) || Number(process.env.RHIZOME_BROWSER_SESSION_OPEN_TIMEOUT_MS) || defaultOpenTimeoutMs)
  );
  const defaultOpenAttempts = requestedHeadless && process.platform === "win32" ? 1 : 3;
  const requestedOpenAttempts = Math.max(1, Math.min(5, Number(args.open_attempts) || defaultOpenAttempts));
  const headfulAttempts = Math.max(
    1,
    Math.min(2, Number(args.headful_open_attempts) || Number(process.env.RHIZOME_BROWSER_SESSION_HEADFUL_OPEN_ATTEMPTS) || 1)
  );
  const headlessAttempts = Math.max(
    1,
    Math.min(5, Number(args.headless_open_attempts) || Number(process.env.RHIZOME_BROWSER_SESSION_HEADLESS_OPEN_ATTEMPTS) || requestedOpenAttempts)
  );
  let launchModes = requestedHeadless
    ? [{ headless: true, launch_mode: "requested_headless", attempts: requestedOpenAttempts }]
    : [
        { headless: false, launch_mode: "requested_headful", attempts: headfulAttempts },
        { headless: true, launch_mode: "headless_fallback_after_headful_cdp_failure", attempts: headlessAttempts }
      ];
  const cleanupNotes = [];
  let lastError = "";
  let tcpFallbackAfterPipeBrokerFailure = false;
  try {
    const forcePipeBroker = boolArg(args.pipe_broker) || process.env.RHIZOME_BROWSER_SESSION_PIPE_BROKER_ONLY === "1";
    const preferPipeBrokerFirst = requestedHeadless &&
      process.platform === "win32" &&
      (boolArg(args.pipe_broker_first) || process.env.RHIZOME_BROWSER_SESSION_PIPE_BROKER_FIRST === "1") &&
      process.env.RHIZOME_BROWSER_SESSION_TCP_FIRST !== "1" &&
      !boolArg(args.tcp_first);
    const disableDefaultTCPFallbackAfterPipe =
      boolArg(args.disable_tcp_fallback) ||
      process.env.RHIZOME_BROWSER_SESSION_DISABLE_TCP_FALLBACK_AFTER_PIPE === "1";
    const allowTCPFallbackAfterPipe =
      boolArg(args.tcp_fallback) ||
      process.env.RHIZOME_BROWSER_SESSION_ALLOW_TCP_FALLBACK === "1" ||
      (preferPipeBrokerFirst && !disableDefaultTCPFallbackAfterPipe);
    if (forcePipeBroker || preferPipeBrokerFirst) {
      const defaultPipeBrokerAttempts = process.platform === "win32" ? 2 : 1;
      const pipeBrokerAttempts = Math.max(
        1,
        Math.min(
          3,
          Number(args.pipe_broker_attempts) ||
          Number(process.env.RHIZOME_BROWSER_SESSION_PIPE_BROKER_ATTEMPTS) ||
          defaultPipeBrokerAttempts
        )
      );
      let pipeError = null;
      for (let pipeAttempt = 1; pipeAttempt <= pipeBrokerAttempts; pipeAttempt++) {
        const brokerLaunchTransport = process.platform === "win32" && pipeAttempt % 2 === 0
          ? "node_spawn_detached_pipe_broker"
          : "";
        try {
          await openPipeBrokerSession(
            paths,
            browser,
            args,
            url,
            width,
            height,
            requestedHeadless,
            openTimeoutMs,
            cleanupNotes,
            forcePipeBroker
              ? (pipeAttempt === 1 ? "pipe_broker_forced" : "pipe_broker_forced_retry")
              : (pipeAttempt === 1 ? "pipe_broker_primary_for_win32_headless" : "pipe_broker_primary_retry_for_win32_headless"),
            brokerLaunchTransport
          );
          return;
        } catch (error) {
          pipeError = error;
          lastError = error && error.message ? error.message : String(error);
          if (pipeAttempt < pipeBrokerAttempts) {
            try { fs.rmSync(paths.dir, { recursive: true, force: true }); } catch (_) {}
            await sleep(500);
          }
        }
      }
      if (pipeError) {
        lastError = pipeError && pipeError.message ? pipeError.message : String(pipeError);
        if (forcePipeBroker || (preferPipeBrokerFirst && !allowTCPFallbackAfterPipe)) throw pipeError;
        if (preferPipeBrokerFirst && allowTCPFallbackAfterPipe) {
          tcpFallbackAfterPipeBrokerFailure = true;
          launchModes = [{ headless: true, launch_mode: "tcp_cdp_fallback_after_pipe_broker_failure", attempts: 1 }];
        }
      }
    }
    if (preferPipeBrokerFirst) {
      try { fs.rmSync(paths.dir, { recursive: true, force: true }); } catch (_) {}
    }
    if (forcePipeBroker) {
      return;
    }
    for (let modeIndex = 0; modeIndex < launchModes.length; modeIndex++) {
      const launchMode = launchModes[modeIndex];
      const headless = launchMode.headless;
      const modeOpenTimeoutMs = tcpFallbackAfterPipeBrokerFailure
        ? Math.min(openTimeoutMs, 30000)
        : openTimeoutMs;
      for (let attempt = 1; attempt <= launchMode.attempts; attempt++) {
      let requestedPort = Number(args.remote_debugging_port) || 0;
      let debugPortSource = requestedPort > 0 ? "requested_remote_debugging_port" : "chrome_owned_dynamic_port";
      if (requestedPort <= 0 && headless && process.platform === "win32") {
        requestedPort = await getFreePort();
        debugPortSource = "daemon_safe_allocated_headless_port";
      }
      const profileDir = freshProfileDir(paths);
      fs.mkdirSync(profileDir, { recursive: true });
      const ownershipNonce = crypto.randomBytes(16).toString("hex");
      const ownerMarkerPath = writeOwnershipMarker(paths, profileDir, ownershipNonce);
      const launchArgs = [
        `--remote-debugging-port=${requestedPort}`,
        "--remote-debugging-address=127.0.0.1",
        "--remote-allow-origins=*",
        `--user-data-dir=${profileDir}`,
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-background-networking",
        "--disable-sync",
        "--disable-extensions",
        "--disable-default-apps",
        "--disable-component-update",
        "--disable-features=Translate,MediaRouter",
        "--disable-dev-shm-usage",
        "--allow-file-access-from-files",
        `--window-size=${width},${height}`
      ];
      if (headless) {
        launchArgs.push("--headless=new", "--disable-gpu");
      }
      launchArgs.push("about:blank");
      let child = null;
      try {
        const preferStartProcess = !(headless && process.platform === "win32") || attempt % 2 === 0;
        child = launchBrowserProcess(browser, launchArgs, headless, { prefer_start_process: preferStartProcess });
        child.unref();
        const ready = await waitForBrowserReady(child, requestedPort, profileDir, modeOpenTimeoutMs);
        const port = ready.port;
        const target = await createTarget(port, "about:blank");
        const state = {
          session_id: paths.id,
          pid: child.pid,
          port,
          browser,
          profile_dir: profileDir,
          ownership_nonce: ownershipNonce,
          owner_marker_path: ownerMarkerPath,
          headless,
          requested_headless: requestedHeadless,
          launch_mode: launchMode.launch_mode,
          launch_transport: child.launch_transport || "unknown",
          debug_port_source: debugPortSource,
          url,
          target_id: target.id,
          opened_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        };
        let navigation = { attempted: false, error: "" };
        if (url !== "about:blank") {
          navigation.attempted = true;
          try {
            await withPage(state, async (cdp) => {
              await cdp.call("Page.navigate", { url });
              return {};
            });
          } catch (error) {
            navigation.error = error && error.message ? error.message : String(error);
          }
        }
        const waitMs = Math.max(0, Math.min(10000, Number(args.wait_ms) || 800));
        if (waitMs) await sleep(waitMs);
        await verifySessionReady(state, 8000);
        saveState(paths, state);
        finish("pass", {
          action: "open",
          session: state,
          browser_version: ready.version.Browser || "",
          navigation,
          open_attempts: cleanupNotes.length + 1,
          mode_attempt: attempt,
          requested_headless: requestedHeadless,
          effective_headless: headless,
          launch_mode: launchMode.launch_mode,
          launch_transport: child.launch_transport || "unknown",
          debug_port_source: debugPortSource,
          headless_fallback_used: !requestedHeadless && headless,
          tcp_fallback_after_pipe_broker_failure: tcpFallbackAfterPipeBrokerFailure,
          open_timeout_ms: modeOpenTimeoutMs,
          launch_attempt_budget: launchModes.map((mode) => ({ launch_mode: mode.launch_mode, attempts: mode.attempts })),
          cleanup_notes: cleanupNotes
        });
      } catch (error) {
        lastError = error && error.message ? error.message : String(error);
        const cleanup_note = child && child.pid ? killProcessTree(child.pid) : "";
        cleanupNotes.push({
          attempt,
          launch_mode: launchMode.launch_mode,
          requested_headless: requestedHeadless,
          effective_headless: headless,
          launch_transport: child && child.launch_transport ? child.launch_transport : "unknown",
          debug_port_source: debugPortSource,
          open_timeout_ms: modeOpenTimeoutMs,
          error: lastError,
          cleanup_note: summary(cleanup_note, 800)
        });
        try { fs.rmSync(profileDir, { recursive: true, force: true }); } catch (_) {}
        const hasMoreAttempts = attempt < launchMode.attempts || modeIndex < launchModes.length - 1;
        if (hasMoreAttempts) await sleep(500);
      }
      }
    }
    try { fs.rmSync(paths.dir, { recursive: true, force: true }); } catch (_) {}
    try {
      await openPipeBrokerSession(paths, browser, args, url, width, height, requestedHeadless, openTimeoutMs, cleanupNotes);
      return;
    } catch (pipeError) {
      lastError = pipeError && pipeError.message ? pipeError.message : String(pipeError);
    }
    fail("open browser session failed", {
      error: lastError || "browser did not become ready",
      requested_headless: requestedHeadless,
      open_timeout_ms: openTimeoutMs,
      tcp_fallback_after_pipe_broker_failure: tcpFallbackAfterPipeBrokerFailure,
      launch_modes_attempted: launchModes.map((mode) => ({ launch_mode: mode.launch_mode, attempts: mode.attempts })),
      cleanup_notes: cleanupNotes
    });
  } catch (error) {
    try { fs.rmSync(paths.dir, { recursive: true, force: true }); } catch (_) {}
    fail("open browser session failed", {
      error: error && error.message ? error.message : String(error),
      cleanup_notes: cleanupNotes
    });
  }
}

async function requireState(paths) {
  const state = loadState(paths);
  if (!state) block("browser session is not open", { session_id: paths.id, retry_guidance: "Call browser_session action=open first." });
  if (state.mode === "pipe_broker") {
    await brokerRequest(state, "status", {}, "GET");
    return state;
  }
  await waitForCDP(state.port, 8000);
  return state;
}

async function goto(state, paths, args) {
  const url = resolveURL(args);
  if (state.mode === "pipe_broker") {
    await brokerRequest(state, "goto", { url, wait_ms: args.wait_ms });
    state.url = url;
    state.updated_at = new Date().toISOString();
    saveState(paths, state);
    finish("pass", { action: "goto", session: state });
  }
  const { target } = await withPage(state, async (cdp) => {
    await cdp.call("Page.navigate", { url });
    const waitMs = Math.max(0, Math.min(15000, Number(args.wait_ms) || 1000));
    if (waitMs) await sleep(waitMs);
    return {};
  });
  state.url = url;
  state.target_id = target.id;
  state.updated_at = new Date().toISOString();
  saveState(paths, state);
  finish("pass", { action: "goto", session: state });
}

async function inspect(state, paths, args) {
  const waitMs = Math.max(0, Math.min(10000, Number(args.wait_ms) || 0));
  if (waitMs) await sleep(waitMs);
  if (state.mode === "pipe_broker") {
    const result = await brokerRequest(state, "inspect", { wait_ms: 0 });
    state.updated_at = new Date().toISOString();
    saveState(paths, state);
    finish("pass", { action: "inspect", session: state, page: result.page });
  }
  const expression = `(() => {
    const text = (document.body && document.body.innerText || "").replace(/\\s+/g, " ").trim();
    const overflowing = [];
    for (const el of Array.from(document.querySelectorAll("*")).slice(0, 2500)) {
      const r = el.getBoundingClientRect();
      if (r.width > window.innerWidth + 2 || r.left < -2 || r.right > window.innerWidth + 2) {
        overflowing.push({ tag: el.tagName, id: el.id || "", cls: String(el.className || "").slice(0, 120), left: Math.round(r.left), right: Math.round(r.right), width: Math.round(r.width) });
      }
      if (overflowing.length >= 20) break;
    }
    return {
      title: document.title || "",
      url: location.href,
      viewport: { width: window.innerWidth, height: window.innerHeight, devicePixelRatio: window.devicePixelRatio },
      body_rect: document.body ? (() => { const r = document.body.getBoundingClientRect(); return { width: Math.round(r.width), height: Math.round(r.height) }; })() : null,
      text_chars: text.length,
      text_summary: text.slice(0, 1200),
      overflowing_elements: overflowing,
      active_element: document.activeElement ? { tag: document.activeElement.tagName, id: document.activeElement.id || "", cls: String(document.activeElement.className || "").slice(0, 120) } : null
    };
  })()`;
  const { result } = await withPage(state, async (cdp) => {
    const evaluated = await cdp.call("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    return evaluated.result ? evaluated.result.value : evaluated;
  });
  state.updated_at = new Date().toISOString();
  saveState(paths, state);
  finish("pass", { action: "inspect", session: state, page: result });
}

async function screenshot(state, paths, args) {
  const width = Math.max(320, Math.min(3000, Number(args.width) || 1440));
  const height = Math.max(240, Math.min(2400, Number(args.height) || 1000));
  const waitMs = Math.max(0, Math.min(10000, Number(args.wait_ms) || 0));
  if (waitMs) await sleep(waitMs);
  const screenshotDir = path.join(ARTIFACT_ROOT, "screenshots");
  fs.mkdirSync(screenshotDir, { recursive: true });
  const outPath = path.join(screenshotDir, `${paths.id}-${timestamp()}.png`);
  if (state.mode === "pipe_broker") {
    const result = await brokerRequest(state, "screenshot", { width, height, wait_ms: 0 });
    fs.writeFileSync(outPath, Buffer.from(result.data || "", "base64"));
    state.updated_at = new Date().toISOString();
    saveState(paths, state);
    finish("pass", { action: "screenshot", session: state, screenshot_path: outPath, screenshot_ref: path.relative(WORKDIR, outPath), width, height });
  }
  const { result } = await withPage(state, async (cdp) => {
    await cdp.call("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile: false });
    const shot = await cdp.call("Page.captureScreenshot", { format: "png", captureBeyondViewport: true });
    fs.writeFileSync(outPath, Buffer.from(shot.data || "", "base64"));
    return { screenshot_path: outPath, screenshot_ref: path.relative(WORKDIR, outPath), width, height };
  });
  state.updated_at = new Date().toISOString();
  saveState(paths, state);
  finish("pass", { action: "screenshot", session: state, ...result });
}

async function click(state, paths, args) {
  const selector = String(args.selector || "").trim();
  if (!selector) block("selector is required for click");
  if (state.mode === "pipe_broker") {
    const result = await brokerRequest(state, "click", { selector, wait_ms: args.wait_ms });
    state.updated_at = new Date().toISOString();
    saveState(paths, state);
    finish("pass", { action: "click", session: state, clicked: result.clicked });
  }
  const expression = `(sel => {
    const el = document.querySelector(sel);
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return { x: r.left + r.width / 2, y: r.top + r.height / 2, tag: el.tagName, text: (el.innerText || el.value || "").slice(0, 200) };
  })(${JSON.stringify(selector)})`;
  const { result } = await withPage(state, async (cdp) => {
    const evaluated = await cdp.call("Runtime.evaluate", { expression, returnByValue: true });
    const rect = evaluated.result ? evaluated.result.value : null;
    if (!rect) throw new Error(`selector not found: ${selector}`);
    await cdp.call("Input.dispatchMouseEvent", { type: "mouseMoved", x: rect.x, y: rect.y, button: "none" });
    await cdp.call("Input.dispatchMouseEvent", { type: "mousePressed", x: rect.x, y: rect.y, button: "left", clickCount: 1 });
    await cdp.call("Input.dispatchMouseEvent", { type: "mouseReleased", x: rect.x, y: rect.y, button: "left", clickCount: 1 });
    const waitMs = Math.max(0, Math.min(10000, Number(args.wait_ms) || 500));
    if (waitMs) await sleep(waitMs);
    return rect;
  });
  state.updated_at = new Date().toISOString();
  saveState(paths, state);
  finish("pass", { action: "click", session: state, clicked: result });
}

async function typeText(state, paths, args) {
  const text = String(args.text || "");
  if (!text) block("text is required for type");
  const selector = String(args.selector || "").trim();
  if (state.mode === "pipe_broker") {
    await brokerRequest(state, "type", { selector, text, wait_ms: args.wait_ms });
    state.updated_at = new Date().toISOString();
    saveState(paths, state);
    finish("pass", { action: "type", session: state, typed_chars: text.length, selector });
  }
  await withPage(state, async (cdp) => {
    if (selector) {
      const expression = `(sel => { const el = document.querySelector(sel); if (!el) return false; el.focus(); return true; })(${JSON.stringify(selector)})`;
      const evaluated = await cdp.call("Runtime.evaluate", { expression, returnByValue: true });
      if (!evaluated.result || evaluated.result.value !== true) throw new Error(`selector not found or not focusable: ${selector}`);
    }
    await cdp.call("Input.insertText", { text });
    const waitMs = Math.max(0, Math.min(10000, Number(args.wait_ms) || 300));
    if (waitMs) await sleep(waitMs);
    return {};
  });
  state.updated_at = new Date().toISOString();
  saveState(paths, state);
  finish("pass", { action: "type", session: state, typed_chars: text.length, selector });
}

async function evaluate(state, paths, args) {
  const expression = String(args.expression || "").trim();
  if (!expression) block("expression is required for evaluate");
  if (state.mode === "pipe_broker") {
    const result = await brokerRequest(state, "evaluate", { expression });
    state.updated_at = new Date().toISOString();
    saveState(paths, state);
    finish("pass", { action: "evaluate", session: state, result: result.result });
  }
  const { result } = await withPage(state, async (cdp) => {
    const evaluated = await cdp.call("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    if (evaluated.exceptionDetails) throw new Error(summary(JSON.stringify(evaluated.exceptionDetails), 1000));
    return evaluated.result ? evaluated.result.value : evaluated;
  });
  state.updated_at = new Date().toISOString();
  saveState(paths, state);
  finish("pass", { action: "evaluate", session: state, result });
}

async function status(state) {
  if (state.mode === "pipe_broker") {
    const result = await brokerRequest(state, "status", {}, "GET");
    finish("pass", { action: "status", session: state, browser_version: result.version && (result.version.Browser || result.version.product) || "", targets: [{ id: state.target_id || "pipe-broker", type: "page", title: "", url: result.url || state.url || "" }] });
  }
  const version = await waitForCDP(state.port, 3000);
  const targets = await listTargets(state.port);
  finish("pass", { action: "status", session: state, browser_version: version.Browser || "", targets: targets.map((item) => ({ id: item.id, type: item.type, title: item.title, url: item.url })) });
}

async function closeOwnedSession(state, paths, removeProfile) {
  let cleanup_note = "process already exited";
  let blocked = false;
  let ownership_validation = null;
  if (processExists(state.pid)) {
    ownership_validation = await validateOwnedSession(state, paths);
    if (!ownership_validation.owned) {
      blocked = true;
      cleanup_note = "skipped pid kill because ownership validation failed";
      return { blocked, cleanup_note, ownership_validation };
    }
    if (state.mode === "pipe_broker") {
      try { await brokerRequest(state, "close", {}); } catch (_) {}
      await waitForProcessExit(state.pid, 1500);
    }
    cleanup_note = killProcessTree(state.pid);
  }
  await waitForProcessExit(state.pid, 3500);
  if (removeProfile !== false) {
    try { fs.rmSync(paths.dir, { recursive: true, force: true }); } catch (_) {}
  } else {
    try { fs.unlinkSync(paths.statePath); } catch (_) {}
  }
  return { blocked, cleanup_note, ownership_validation };
}

async function closeSession(state, paths, args) {
  const closed = await closeOwnedSession(state, paths, args.remove_profile);
  if (closed.blocked) {
    block("browser session ownership could not be validated; skipped pid kill", {
      session_id: paths.id,
      ownership_validation: closed.ownership_validation,
      cleanup_note: closed.cleanup_note
    });
  }
  finish("pass", {
    action: "close",
    session_id: paths.id,
    cleanup_note: closed.cleanup_note,
    ownership_validation: closed.ownership_validation
  });
}

async function closeAllSessions(args) {
  const removeProfile = args.remove_profile !== false;
  const results = [];
  if (fs.existsSync(SESSION_ROOT)) {
    for (const entry of fs.readdirSync(SESSION_ROOT, { withFileTypes: true })) {
      if (!entry.isDirectory() || entry.name === ".locks") continue;
      const paths = sessionPaths(entry.name);
      if (!isInside(SESSION_ROOT, paths.dir)) continue;
      const state = loadState(paths);
      if (!state) {
        if (removeProfile) {
          try { fs.rmSync(paths.dir, { recursive: true, force: true }); } catch (_) {}
        }
        results.push({ session_id: paths.id, status: "stale_state_removed" });
        continue;
      }
      const closed = await closeOwnedSession(state, paths, removeProfile);
      results.push({
        session_id: paths.id,
        status: closed.blocked ? "skipped_unverified_ownership" : "closed",
        pid: state.pid,
        cleanup_note: closed.cleanup_note,
        ownership_validation: closed.ownership_validation
      });
    }
  }
  const skipped = results.filter((item) => item.status === "skipped_unverified_ownership");
  finish(skipped.length ? "warn" : "pass", { action: "close_all", closed_count: results.filter((item) => item.status === "closed").length, skipped_count: skipped.length, results });
}

async function main() {
  const args = readInput();
  fs.mkdirSync(ARTIFACT_ROOT, { recursive: true });
  fs.mkdirSync(SESSION_ROOT, { recursive: true });
  if (!isInside(WORKDIR, ARTIFACT_ROOT) || !isInside(WORKDIR, SESSION_ROOT)) {
    block("artifact/session root escapes RHIZOME_TOOL_WORKDIR");
  }
  const action = String(args.action || "").trim().toLowerCase();
  const paths = sessionPaths(args.session_id || "default");
  switch (action) {
    case "open":
      return withSessionLock(paths, action, () => openSession(paths, args));
    case "goto":
      return withSessionLock(paths, action, async () => goto(await requireState(paths), paths, args));
    case "inspect":
      return withSessionLock(paths, action, async () => inspect(await requireState(paths), paths, args));
    case "screenshot":
      return withSessionLock(paths, action, async () => screenshot(await requireState(paths), paths, args));
    case "click":
      return withSessionLock(paths, action, async () => click(await requireState(paths), paths, args));
    case "type":
      return withSessionLock(paths, action, async () => typeText(await requireState(paths), paths, args));
    case "evaluate":
      return withSessionLock(paths, action, async () => evaluate(await requireState(paths), paths, args));
    case "status":
      return withSessionLock(paths, action, async () => status(await requireState(paths)));
    case "close":
      return withSessionLock(paths, action, async () => {
        const state = loadState(paths);
        if (!state) block("browser session is not open", { session_id: paths.id });
        return closeSession(state, paths, args);
      });
    case "close_all":
      return closeAllSessions(args);
    default:
      return block("unsupported browser_session action", { action, supported_actions: ["open", "goto", "screenshot", "inspect", "click", "type", "evaluate", "status", "close", "close_all"] });
  }
}

if (process.argv[2] === "--pipe-broker") {
  runPipeBroker(process.argv[3]).catch((error) => {
    try {
      fs.writeSync(2, (error && error.stack ? error.stack : String(error)) + "\n");
    } catch (_) {}
    process.exit(1);
  });
} else {
  main().catch((error) => {
    fail("browser session tool crashed", { error: error && error.stack ? error.stack : String(error) });
  });
}
