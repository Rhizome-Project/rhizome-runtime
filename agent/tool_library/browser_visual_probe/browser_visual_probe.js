const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn, spawnSync } = require("child_process");

const WORKDIR = path.resolve(process.env.RHIZOME_TOOL_WORKDIR || process.cwd());
const BUNDLE_DIR = path.resolve(process.env.RHIZOME_TOOL_BUNDLE_DIR || __dirname);
const DEFAULT_ARTIFACT_ROOT = path.join(WORKDIR, ".runtime-config", "tool-artifacts", "browser_visual_probe", timestamp());
const ARTIFACT_ROOT = path.resolve(process.env.RHIZOME_TOOL_ARTIFACT_DIR || DEFAULT_ARTIFACT_ROOT);

function timestamp() {
  return new Date().toISOString().replace(/[:.]/g, "").replace("Z", "Z");
}

function readInput() {
  const raw = fs.readFileSync(0, "utf8").trim();
  if (!raw) return {};
  return JSON.parse(raw);
}

function writeResult(result) {
  persistProbeReport(result);
  process.stdout.write(JSON.stringify(result, null, 2));
}

function persistProbeReport(result) {
  try {
    if (!isInside(WORKDIR, ARTIFACT_ROOT)) return;
    fs.mkdirSync(ARTIFACT_ROOT, { recursive: true });
    fs.writeFileSync(path.join(ARTIFACT_ROOT, "probe-report.json"), JSON.stringify(result, null, 2), "utf8");
  } catch (_) {
    // Stdout remains the primary transport; report persistence is best effort.
  }
}

class ToolExit extends Error {
  constructor(result) {
    super(result.reason || "tool exit");
    this.name = "ToolExit";
    this.result = result;
  }
}

function resultEnvelope(status, extra = {}) {
  return {
    contract_version: "browser_visual_probe_result_v1",
    tool: "browser_visual_probe",
    status,
    workdir: WORKDIR,
    artifact_root: ARTIFACT_ROOT,
    ...extra
  };
}

function block(reason, extra = {}) {
  throw new ToolExit(resultEnvelope("block", {
    reason,
    ...extra
  }));
}

function summary(text, limit = 1600) {
  const value = String(text || "").replace(/\s+/g, " ").trim();
  if (value.length <= limit) return value;
  return value.slice(0, limit) + "...";
}

function sanitizeID(value) {
  return String(value || "viewport").replace(/[^a-zA-Z0-9_.-]+/g, "_").slice(0, 64) || "viewport";
}

function fileSHA256(filePath) {
  try {
    const hash = crypto.createHash("sha256");
    hash.update(fs.readFileSync(filePath));
    return `sha256:${hash.digest("hex")}`;
  } catch (_) {
    return "";
  }
}

function isInside(root, candidate) {
  const rel = path.relative(path.resolve(root), path.resolve(candidate));
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

function gitInspect(cwd) {
  const base = path.resolve(cwd || WORKDIR);
  const rootResult = spawnSync("git", ["-C", base, "rev-parse", "--show-toplevel"], {
    encoding: "utf8",
    timeout: 5000,
    windowsHide: true
  });
  if (rootResult.status !== 0) {
    return { is_git: false, cwd: base };
  }
  const root = path.resolve(String(rootResult.stdout || "").trim());
  if (!root) return { is_git: false, cwd: base };
  const headResult = spawnSync("git", ["-C", root, "rev-parse", "HEAD"], {
    encoding: "utf8",
    timeout: 5000,
    windowsHide: true
  });
  const statusResult = spawnSync("git", ["-C", root, "status", "--porcelain"], {
    encoding: "utf8",
    timeout: 5000,
    windowsHide: true
  });
  return {
    is_git: true,
    cwd: base,
    root,
    head_sha: headResult.status === 0 ? String(headResult.stdout || "").trim() : "",
    status: statusResult.status === 0 ? String(statusResult.stdout || "").trim() : "",
    inspect_error: statusResult.status === 0 ? "" : summary(statusResult.stderr || statusResult.stdout || "git status failed", 500)
  };
}

function pathSegments(value) {
  return path.resolve(value || "").split(/[\\/]+/).filter(Boolean);
}

function projectCheckoutKind(root) {
  const segments = pathSegments(root);
  const idx = segments.map((segment) => segment.toLowerCase()).lastIndexOf("project-checkouts");
  if (idx < 0 || idx + 1 >= segments.length) return "";
  const checkoutName = segments[idx + 1] || "";
  if (/^review[-_]/i.test(checkoutName)) return "read_only_validation";
  if (!isInside(WORKDIR, root)) return "foreign_project_checkout";
  return "owned_project_checkout";
}

function cleanGitGuard(args) {
  const cwd = resolveRunCwd(args);
  const git = gitInspect(cwd);
  if (!git.is_git) return null;
  const checkout_kind = projectCheckoutKind(git.root);
  const requested = args.require_clean_git === true || typeof args.expected_head_sha === "string";
  const required = requested || checkout_kind === "read_only_validation" || checkout_kind === "foreign_project_checkout";
  if (!required) {
    return { required: false, checkout_kind, before: git };
  }
  const expectedHead = typeof args.expected_head_sha === "string" ? args.expected_head_sha.trim() : "";
  if (git.inspect_error) {
    block("candidate git state could not be inspected before visual probe", {
      git_candidate: { ...git, checkout_kind, require_clean_git: true }
    });
  }
  if (expectedHead && git.head_sha && git.head_sha.toLowerCase() !== expectedHead.toLowerCase()) {
    block("candidate head does not match expected_head_sha before visual probe", {
      git_candidate: { ...git, checkout_kind, expected_head_sha: expectedHead, require_clean_git: true }
    });
  }
  if (git.status) {
    block("candidate checkout is dirty before visual probe", {
      git_candidate: { ...git, checkout_kind, expected_head_sha: expectedHead, require_clean_git: true },
      retry_guidance: "Use a fresh validation checkout, restore the read-only checkout to HEAD, or run dependency setup in a disposable clone before producing exact-head visual evidence."
    });
  }
  return { required: true, checkout_kind, before: git, expected_head_sha: expectedHead };
}

function verifyCleanGitAfterProbe(guard) {
  if (!guard || !guard.required || !guard.before || !guard.before.root) return null;
  const after = gitInspect(guard.before.root);
  const checkout_kind = guard.checkout_kind || projectCheckoutKind(guard.before.root);
  if (!after.is_git || after.inspect_error) {
    return {
      ok: false,
      reason: "candidate git state could not be inspected after visual probe",
      before: guard.before,
      after: { ...after, checkout_kind }
    };
  }
  if (guard.expected_head_sha && after.head_sha && after.head_sha.toLowerCase() !== guard.expected_head_sha.toLowerCase()) {
    return {
      ok: false,
      reason: "candidate head changed during visual probe",
      before: guard.before,
      after: { ...after, checkout_kind, expected_head_sha: guard.expected_head_sha }
    };
  }
  if (after.status) {
    return {
      ok: false,
      reason: "candidate checkout became dirty during visual probe",
      before: guard.before,
      after: { ...after, checkout_kind, expected_head_sha: guard.expected_head_sha }
    };
  }
  return {
    ok: true,
    before: guard.before,
    after: { ...after, checkout_kind, expected_head_sha: guard.expected_head_sha }
  };
}

function fileURL(filePath) {
  const normalized = path.resolve(filePath).replace(/\\/g, "/");
  return "file:///" + normalized.replace(/^\/+/, "");
}

function isLocalHTTPURL(url) {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.toLowerCase();
    return (parsed.protocol === "http:" || parsed.protocol === "https:") &&
      (host === "localhost" || host === "127.0.0.1" || host === "::1" || host === "[::1]");
  } catch (_) {
    return false;
  }
}

function resolveTarget(args) {
  if (typeof args.url === "string" && args.url.trim()) {
    const url = args.url.trim();
    let parsed;
    try {
      parsed = new URL(url);
    } catch (_) {
      block("url is not a valid URL", { url });
    }

    if (parsed.protocol === "file:") {
      const filePath = decodeURIComponent(parsed.pathname.replace(/^\/([A-Za-z]:\/)/, "$1"));
      const resolved = path.resolve(filePath);
      if (!fs.existsSync(resolved)) block("file URL target does not exist", {
        url,
        retry_guidance: "Verify the local file path first, or pass a running localhost URL in the url field."
      });
      return { target: fileURL(resolved), target_kind: "file", target_label: path.relative(WORKDIR, resolved) };
    }

    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
      block("unsupported URL protocol", { url });
    }
    return { target: url, target_kind: "url", target_label: url };
  }

  if (typeof args.html_path === "string" && args.html_path.trim()) {
    const requested = args.html_path.trim();
    if (/^https?:\/\//i.test(requested) || /^file:\/\//i.test(requested)) {
      return resolveTarget({ ...args, url: requested, html_path: "" });
    }
    const htmlPath = path.isAbsolute(requested) ? path.resolve(requested) : path.resolve(WORKDIR, requested);
    if (!fs.existsSync(htmlPath)) block("html_path does not exist", {
      html_path: requested,
      retry_guidance: "Do not invent html_path values. If the app is served by Vite/http-server, pass the verified localhost URL in the url field. Use html_path only after read_file/list_directory proves the file exists inside RHIZOME_TOOL_WORKDIR."
    });
    if (!fs.statSync(htmlPath).isFile()) block("html_path is not a file", { html_path: requested });
    return { target: fileURL(htmlPath), target_kind: "file", target_label: path.relative(WORKDIR, htmlPath) };
  }

  block("provide either url or html_path", {
    retry_guidance: "For running local apps, pass url such as http://127.0.0.1:3000. Use html_path only for an existing HTML file inside RHIZOME_TOOL_WORKDIR."
  });
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
    const programFilesBases = [
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
    for (const base of programFilesBases) {
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
  out.push(
    "chrome",
    "chromium-browser",
    "chromium",
    "google-chrome"
  );
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

function resolvePathCommand(command) {
  const value = String(command || "").trim();
  if (!value) return "";
  const hasSeparator = value.includes("\\") || value.includes("/");
  if (hasSeparator) return fs.existsSync(value) ? value : value;
  const pathDirs = String(process.env.PATH || "")
    .split(path.delimiter)
    .map((dir) => dir.trim())
    .filter(Boolean);
  const exts = path.extname(value) ? [""] : pathExts();
  for (const dir of pathDirs) {
    for (const ext of exts) {
      const full = path.join(dir, value + ext);
      if (fs.existsSync(full)) return full;
    }
  }
  return value;
}

function findBrowser() {
  for (const candidate of candidateBrowsers()) {
    const browser = resolveBrowserCommand(candidate);
    if (!browser) continue;
    if (browserSupportsHeadless(browser)) return browser;
  }
  return "";
}

function browserSupportsHeadless(candidate) {
  const profileDir = path.join(ARTIFACT_ROOT, `probe-profile-${process.pid}-${Math.random().toString(16).slice(2)}`);
  fs.mkdirSync(profileDir, { recursive: true });
  try {
    const probe = spawnSync(candidate, [
      "--headless=new",
      "--disable-gpu",
      "--no-first-run",
      "--no-default-browser-check",
      "--disable-background-networking",
      "--disable-sync",
      "--disable-extensions",
      "--disable-component-update",
      "--disable-default-apps",
      "--disable-features=Translate,MediaRouter",
      `--user-data-dir=${profileDir}`,
      "--dump-dom",
      "about:blank"
    ], {
      cwd: WORKDIR,
      encoding: "utf8",
      timeout: 10000,
      shell: windowsBatchCommand(candidate),
      windowsHide: true
    });
    if (probe.error || probe.status !== 0) return false;
    return String(probe.stdout || "").toLowerCase().includes("<html");
  } finally {
    try {
      fs.rmSync(profileDir, { recursive: true, force: true });
    } catch (_) {
      // Best-effort cleanup only; a later artifact cleanup pass may remove leftovers.
    }
  }
}

function windowsBatchCommand(command) {
  return process.platform === "win32" && /\.(bat|cmd)$/i.test(String(command || ""));
}

function parseViewports(raw) {
  const fallback = [
    { id: "desktop", width: 1440, height: 1000 },
    { id: "narrow", width: 390, height: 844 }
  ];
  if (!Array.isArray(raw)) return fallback;
  const out = [];
  for (const entry of raw.slice(0, 3)) {
    const width = Math.max(240, Math.min(2560, Number(entry && entry.width) || 0));
    const height = Math.max(240, Math.min(1800, Number(entry && entry.height) || 0));
    if (!width || !height) continue;
    out.push({
      id: sanitizeID(entry.id || `${width}x${height}`),
      width,
      height
    });
  }
  return out.length ? out : fallback;
}

function parseMarkers(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map((value) => String(value || "").trim()).filter(Boolean).slice(0, 20);
}

function resolveRunCwd(args) {
  const raw = typeof args.cwd === "string" && args.cwd.trim()
    ? args.cwd.trim()
    : (typeof args.workdir === "string" && args.workdir.trim() ? args.workdir.trim() : "");
  if (!raw) return WORKDIR;
  const resolved = path.isAbsolute(raw) ? path.resolve(raw) : path.resolve(WORKDIR, raw);
  if (!fs.existsSync(resolved)) block("cwd does not exist", { cwd: raw, resolved_cwd: resolved });
  if (!fs.statSync(resolved).isDirectory()) block("cwd is not a directory", { cwd: raw, resolved_cwd: resolved });
  return resolved;
}

function normalizeStartCommand(raw) {
  if (typeof raw === "string" && raw.trim()) {
    return { command: raw.trim(), shell: true };
  }
  if (Array.isArray(raw) && raw.length > 0) {
    return {
      command: String(raw[0] || "").trim(),
      args: raw.slice(1).map((value) => String(value)),
      shell: false
    };
  }
  return null;
}

function deriveURLFromServerArgs(args) {
  if (typeof args.url === "string" && args.url.trim()) return args.url.trim();
  const port = Number(args.port || args.dev_server_port || 0);
  if (!Number.isFinite(port) || port <= 0) return "";
  const host = String(args.host || "127.0.0.1").trim() || "127.0.0.1";
  const pathPart = String(args.path || args.url_path || "/").trim() || "/";
  return `http://${host}:${port}${pathPart.startsWith("/") ? pathPart : `/${pathPart}`}`;
}

function preflightTargetBeforeStartup(args) {
  const hasStartCommand = Boolean(args.start_command || args.dev_server_command);
  if (!args.url && !args.html_path && hasStartCommand) {
    const derived = deriveURLFromServerArgs(args);
    if (!derived) {
      block("url or port is required with start_command", {
        retry_guidance: "Pass url for an already-known target, or pass port/dev_server_port so the probe can derive and validate the localhost target before starting the dev server."
      });
    }
    args.url = derived;
  }
  return resolveTarget(args);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function probeHTTP(url, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { signal: controller.signal, redirect: "follow" });
    const text = await response.text().catch(() => "");
    return {
      ok: response.ok,
      status: response.status,
      text_summary: summary(text, 500)
    };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      error: error && error.message ? error.message : String(error)
    };
  } finally {
    clearTimeout(timer);
  }
}

async function waitForURL(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let last = { ok: false, status: 0, error: "not checked" };
  while (Date.now() < deadline) {
    last = await probeHTTP(url, Math.min(2500, Math.max(500, deadline - Date.now())));
    if (last.ok || (last.status >= 200 && last.status < 500)) {
      return { ready: true, last };
    }
    await sleep(500);
  }
  return { ready: false, last };
}

function cmdQuote(value) {
  const text = String(value);
  if (text === "") return "\"\"";
  return `"${text.replace(/(["^&|<>()%!])/g, "^$1")}"`;
}

function buildStartProcessSpec(spec) {
  if (spec.shell) {
    return { command: spec.command, args: [], shell: true };
  }
  const resolved = resolvePathCommand(spec.command);
  if (process.platform === "win32") {
    const ext = path.extname(resolved).toLowerCase();
    if (ext === ".cmd" || ext === ".bat") {
      const commandLine = [resolved, ...(spec.args || [])].map(cmdQuote).join(" ");
      return { command: commandLine, args: [], shell: true };
    }
  }
  return { command: resolved, args: spec.args || [], shell: false };
}

function killProcessTree(pid) {
  if (!pid || pid <= 0) return "";
  if (process.platform === "win32") {
    const killed = spawnSync("taskkill", ["/PID", String(pid), "/T", "/F"], {
      encoding: "utf8",
      timeout: 7000,
      windowsHide: true
    });
    return summary(`${killed.stdout || ""}\n${killed.stderr || ""}`, 1000);
  }
  try {
    process.kill(-pid, "SIGTERM");
    return "sent SIGTERM to process group";
  } catch (error) {
    try {
      process.kill(pid, "SIGTERM");
      return "sent SIGTERM to process";
    } catch (inner) {
      return inner && inner.message ? inner.message : String(inner);
    }
  }
}

async function startDevServerIfRequested(args) {
  const spec = normalizeStartCommand(args.start_command || args.dev_server_command);
  if (!spec) return null;
  const cwd = resolveRunCwd(args);
  const stdoutPath = path.join(ARTIFACT_ROOT, "dev-server.out.log");
  const stderrPath = path.join(ARTIFACT_ROOT, "dev-server.err.log");
  const stdout = fs.openSync(stdoutPath, "a");
  const stderr = fs.openSync(stderrPath, "a");
  const env = {
    ...process.env,
    RHIZOME_BROWSER_PROBE: "1",
    BROWSER: "none"
  };
  if (args.port || args.dev_server_port) {
    env.PORT = String(args.port || args.dev_server_port);
    env.RHIZOME_SMOKE_PORT_HINT = String(args.port || args.dev_server_port);
  }
  const processSpec = buildStartProcessSpec(spec);
  const child = spawn(processSpec.command, processSpec.args || [], {
    cwd,
    env,
    shell: processSpec.shell,
    detached: process.platform !== "win32",
    stdio: ["ignore", stdout, stderr],
    windowsHide: false
  });
  const spawnFailure = await waitForImmediateSpawnFailure(child, 250);
  if (spawnFailure) throw spawnFailure;
  child.unref();
  const targetURL = deriveURLFromServerArgs(args);
  if (!targetURL) {
    return { child, cwd, stdoutPath, stderrPath, targetURL: "", ready: false, ready_probe: { error: "url or port is required with start_command" } };
  }
  const waitMs = Math.max(1000, Math.min(90000, Number(args.wait_timeout_seconds || args.server_wait_seconds || 30) * 1000));
  const ready = await waitForURL(targetURL, waitMs);
  return { child, cwd, stdoutPath, stderrPath, targetURL, ready: ready.ready, ready_probe: ready.last };
}

function waitForImmediateSpawnFailure(child, timeoutMs) {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (value) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(value);
    };
    const cleanup = () => {
      clearTimeout(timer);
      child.off("error", onError);
      child.off("exit", onExit);
    };
    const onError = (error) => finish(error);
    const onExit = (code, signal) => {
      if (code === 0 || code === null) return finish(null);
      finish(new Error(`dev server command exited before readiness: code=${code} signal=${signal || ""}`));
    };
    const timer = setTimeout(() => finish(null), timeoutMs);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}

function stripTags(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function countMatches(text, pattern) {
  const matches = String(text || "").match(pattern);
  return matches ? matches.length : 0;
}

function extractNumericAttrs(html, attrName) {
  const pattern = new RegExp(`\\b${attrName}=["']?(-?\\d+)["']?`, "gi");
  const values = [];
  for (const match of html.matchAll(pattern)) {
    const value = Number(match[1]);
    if (Number.isFinite(value)) values.push(value);
  }
  return values;
}

function maxPlusOne(values) {
  if (!values.length) return 0;
  return Math.max(...values) + 1;
}

function inferRepeatColumns(lower) {
  const match = lower.match(/grid-template-columns\s*:\s*repeat\(\s*(\d+)\s*,/);
  if (!match) return 0;
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : 0;
}

function analyzePrimarySurfaceRisk(dom) {
  const html = String(dom || "");
  const lower = html.toLowerCase();
  const rowValues = extractNumericAttrs(html, "data-row");
  const colValues = extractNumericAttrs(html, "data-col");
  const cellClassCount = countMatches(lower, /\bclass=["'][^"']*\bcell\b[^"']*["']/g);
  const gridCellCount = Math.max(cellClassCount, rowValues.length, colValues.length);
  const repeatColumns = inferRepeatColumns(lower);
  const inferredRows = maxPlusOne(rowValues);
  const inferredCols = Math.max(maxPlusOne(colValues), repeatColumns);
  const boardHints = [
    /\bclass=["'][^"']*\bboard\b[^"']*["']/.test(lower),
    /\baria-label=["'][^"']*(?:board|grid|minesweeper|game)[^"']*["']/.test(lower),
    lower.includes("grid-template-columns"),
    gridCellCount >= 16 && (inferredRows > 0 || inferredCols > 0)
  ];
  const surfaceDetected = boardHints.some(Boolean);
  const hasStyleElement = /<style[\s\S]*?<\/style>/i.test(html);
  const hasStylesheetLink = /<link\b[^>]*\brel=["']?stylesheet/i.test(html);
  const hasCellStyleDefinition = /<style[\s\S]*\.(?:cell|tile|square|grid-cell|board-cell)\b[\s\S]*?<\/style>/i.test(html) ||
    /<link\b[^>]*\brel=["']?stylesheet/i.test(html) ||
    /\bclass=["'][^"']*\bcell\b[^"']*["'][^>]*\bstyle=["'][^"']*(?:width|height|min-width|min-height|aspect-ratio|flex-basis|grid-area)/i.test(html);
  const hasBoardDisplayGrid = /<style[\s\S]*\.(?:board|grid)\b[\s\S]*display\s*:\s*grid[\s\S]*?<\/style>/i.test(html) ||
    /\bclass=["'][^"']*\bboard\b[^"']*["'][^>]*\bstyle=["'][^"']*display\s*:\s*grid/i.test(html) ||
    /\brole=["']grid["']/i.test(html);

  const signals = [];
  if (surfaceDetected && gridCellCount >= 16 && !hasCellStyleDefinition) {
    signals.push("board_cells_without_visible_css");
  }
  if (surfaceDetected && repeatColumns >= 4 && !hasBoardDisplayGrid) {
    signals.push("board_grid_columns_without_display_grid");
  }
  if (surfaceDetected && gridCellCount >= 25 && !hasStyleElement && !hasStylesheetLink) {
    signals.push("unstyled_primary_surface");
  }
  if (surfaceDetected && gridCellCount >= 25 && inferredRows > 0 && inferredCols > 0 && inferredRows * inferredCols === gridCellCount && !hasCellStyleDefinition) {
    signals.push("game_board_likely_line_wrapped");
  }

  const score = Math.min(100,
    (signals.includes("board_cells_without_visible_css") ? 45 : 0) +
    (signals.includes("board_grid_columns_without_display_grid") ? 35 : 0) +
    (signals.includes("unstyled_primary_surface") ? 35 : 0) +
    (signals.includes("game_board_likely_line_wrapped") ? 25 : 0)
  );

  return {
    surface_detected: surfaceDetected,
    surface_kind: surfaceDetected ? "board/grid/game-surface" : "unknown",
    cell_count: gridCellCount,
    inferred_rows: inferredRows,
    inferred_cols: inferredCols,
    repeat_columns: repeatColumns,
    style_evidence: {
      style_element: hasStyleElement,
      stylesheet_link: hasStylesheetLink,
      cell_style_definition: hasCellStyleDefinition,
      board_display_grid: hasBoardDisplayGrid
    },
    risk_score: score,
    risk_level: score >= 50 ? "high" : (score >= 25 ? "medium" : "low"),
    risk_signals: signals,
    guidance: signals.length
      ? "Treat this as a blocking primary-surface geometry finding until screenshots/DOM prove the board has styled square cells, coherent grid geometry, and no line wrapping."
      : "No board/grid primary-surface geometry blocker detected by the heuristic; still inspect screenshots semantically."
  };
}

function analyzeLayoutRisk(dom, markerHits) {
  const html = String(dom || "");
  const lower = html.toLowerCase();
  const text = stripTags(html);
  const signals = [];
  const primarySurface = analyzePrimarySurfaceRisk(html);

  const inlineFontSizes = [...lower.matchAll(/font-size\s*:\s*([0-9.]+)\s*(px|rem|em|vw|vh)/g)]
    .map((match) => ({ value: Number(match[1]), unit: match[2] }))
    .filter((item) => Number.isFinite(item.value));
  const hugeInlineFont = inlineFontSizes.some((item) =>
    (item.unit === "px" && item.value >= 72) ||
    ((item.unit === "rem" || item.unit === "em") && item.value >= 4.5) ||
    ((item.unit === "vw" || item.unit === "vh") && item.value >= 9)
  );
  if (hugeInlineFont || /\btext-(?:7xl|8xl|9xl|\[[^\]]*(?:7[2-9]|[89][0-9]|1[0-9]{2})px)/.test(lower)) {
    signals.push("oversized_text");
  }
  if (/(overflow-x\s*:\s*(scroll|auto)|min-width\s*:\s*(?:[1-9][0-9]{3,})px|w-screen\s+.*max-w-none)/.test(lower)) {
    signals.push("horizontal_overflow_risk");
  }
  if (/(opacity\s*:\s*0\.[0-2]|text-(?:white|slate|stone|neutral|gray)-[12]00)/.test(lower) &&
      /(background|bg-(?:white|slate|stone|neutral|gray)-[0-2]00)/.test(lower)) {
    signals.push("low_contrast_text_hint");
  }
  const duplicateIDs = html.match(/\bid=["'][^"']+["']/gi) || [];
  if (new Set(duplicateIDs.map((value) => value.toLowerCase())).size < duplicateIDs.length) {
    signals.push("duplicate_dom_ids");
  }
  const missingMarkers = Object.entries(markerHits || {})
    .filter(([, hit]) => !hit)
    .map(([marker]) => marker);
  for (const marker of missingMarkers.slice(0, 5)) {
    signals.push(`missing_marker:${sanitizeID(marker)}`);
  }
  for (const signal of primarySurface.risk_signals || []) {
    signals.push(`primary_surface:${signal}`);
  }

  const score = Math.min(100,
    (signals.includes("oversized_text") ? 35 : 0) +
    (signals.includes("horizontal_overflow_risk") ? 30 : 0) +
    (signals.includes("low_contrast_text_hint") ? 25 : 0) +
    (signals.includes("duplicate_dom_ids") ? 10 : 0) +
    Math.min(30, missingMarkers.length * 10) +
    Math.min(70, primarySurface.risk_score || 0)
  );

  return {
    risk_score: score,
    risk_level: score >= 50 ? "high" : (score >= 25 ? "medium" : "low"),
    risk_signals: signals,
    dom_text_chars: text.length,
    dom_text_summary: summary(text, 320),
    inline_font_size_count: inlineFontSizes.length,
    missing_markers: missingMarkers,
    primary_surface_analysis: primarySurface
  };
}

function runBrowser(browser, args, options = {}) {
  const result = spawnSync(browser, args, {
    cwd: WORKDIR,
    encoding: "utf8",
    timeout: options.timeoutMs || 15000,
    shell: windowsBatchCommand(browser),
    windowsHide: true
  });
  return {
    status: result.status,
    signal: result.signal,
    error: result.error ? result.error.message : "",
    stdout: result.stdout || "",
    stderr: result.stderr || ""
  };
}

function captureHealthcheckBrowser(target, viewport, markers, timeoutMs) {
  const attempts = [];
  for (const candidate of candidateBrowsers()) {
    const browser = resolveBrowserCommand(candidate);
    if (!browser) continue;
    const capture = captureViewport(browser, target, viewport, markers, timeoutMs);
    attempts.push({
      browser,
      status: capture.status,
      dom_status: capture.dom_status,
      error: capture.error,
      stderr_summary: capture.stderr_summary,
      dom_stderr_summary: capture.dom_stderr_summary
    });
    if (capture.status !== "fail" && capture.marker_hits && capture.marker_hits[markers[0]] === true) {
      return { browser, capture, attempts };
    }
  }
  return { browser: "", capture: null, attempts };
}

function captureViewport(browser, target, viewport, markers, timeoutMs) {
  const id = sanitizeID(viewport.id || `${viewport.width}x${viewport.height}`);
  const scenarioID = sanitizeID(viewport.scenario_id || viewport.scenario || "visual_audit_probe");
  const stateID = sanitizeID(viewport.state_id || viewport.state || "observed_surface");
  const screenshotDir = path.join(ARTIFACT_ROOT, "screenshots");
  fs.mkdirSync(screenshotDir, { recursive: true });
  const screenshotPath = path.join(screenshotDir, `${id}.png`);
  const profileDir = path.join(ARTIFACT_ROOT, `profile-${id}`);
  fs.mkdirSync(profileDir, { recursive: true });

  const common = [
    "--headless=new",
    "--disable-gpu",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-background-networking",
    "--disable-sync",
    "--disable-extensions",
    "--hide-scrollbars",
    "--allow-file-access-from-files",
    `--user-data-dir=${profileDir}`,
    `--window-size=${viewport.width},${viewport.height}`
  ];

  let shot = { status: 1, signal: "", error: "capture did not start", stdout: "", stderr: "" };
  let dump = { status: 1, signal: "", error: "dom dump did not start", stdout: "", stderr: "" };
  const cleanupErrors = [];
  try {
    shot = runBrowser(browser, [...common, `--screenshot=${screenshotPath}`, target], { timeoutMs });
    dump = runBrowser(browser, [...common, "--dump-dom", target], { timeoutMs: Math.min(timeoutMs, 12000) });
  } finally {
    try {
      fs.rmSync(profileDir, { recursive: true, force: true });
    } catch (error) {
      cleanupErrors.push(error && error.message ? error.message : String(error));
    }
  }
  const markerHits = {};
  for (const marker of markers) {
    markerHits[marker] = dump.stdout.includes(marker);
  }
  const domFailed = dump.status !== 0 || Boolean(dump.error);
  const layoutRisk = analyzeLayoutRisk(dump.stdout, markerHits);
  const screenshotExists = fs.existsSync(screenshotPath);
  const screenshotPassed = shot.status === 0 && screenshotExists;
  return {
    id,
    viewport_id: id,
    scenario_id: scenarioID,
    state_id: stateID,
    width: viewport.width,
    height: viewport.height,
    status: screenshotPassed ? (domFailed ? "warn" : "pass") : "fail",
    dom_status: domFailed ? "warn" : "pass",
    dom_failure_reason: domFailed ? summary(dump.error || dump.stderr || `dump-dom exited ${dump.status}`, 800) : "",
    screenshot_path: screenshotPath,
    screenshot_ref: path.relative(WORKDIR, screenshotPath),
    screenshot_sha256: screenshotExists ? fileSHA256(screenshotPath) : "",
    exit_code: shot.status,
    signal: shot.signal || "",
    error: shot.error || "",
    stdout_summary: summary(shot.stdout),
    stderr_summary: summary(shot.stderr),
    dom_exit_code: dump.status,
    dom_stdout_summary: summary(dump.stdout),
    dom_stderr_summary: summary(dump.stderr),
    marker_hits: markerHits,
    layout_risk: layoutRisk,
    primary_surface_analysis: layoutRisk.primary_surface_analysis,
    cleanup_errors: cleanupErrors
  };
}

async function run() {
  const args = readInput();
  let serverRef = null;
  try {
    const result = await mainWithInput(args, (server) => { serverRef = server; });
    writeResult(result);
  } catch (error) {
    if (error instanceof ToolExit) {
      writeResult(error.result);
      return;
    }
    writeResult(resultEnvelope("block", {
      reason: "browser visual probe crashed",
      error: error && error.stack ? error.stack : String(error)
    }));
  } finally {
    if (serverRef && args.keep_server !== true) {
      const kill_note = killProcessTree(serverRef.child && serverRef.child.pid);
      const cleanupPath = path.join(ARTIFACT_ROOT, "dev-server.cleanup.txt");
      try {
        fs.writeFileSync(cleanupPath, kill_note || "server cleanup attempted", "utf8");
      } catch (_) {
        // The JSON result is already emitted; cleanup artifacts are best effort.
      }
    }
  }
}

function runHealthcheck() {
  try {
    fs.mkdirSync(ARTIFACT_ROOT, { recursive: true });
    if (!isInside(WORKDIR, ARTIFACT_ROOT)) {
      block("artifact root escapes RHIZOME_TOOL_WORKDIR");
    }
    const marker = "rhizome-browser-visual-probe-healthcheck";
    const htmlPath = path.join(ARTIFACT_ROOT, "healthcheck.html");
    fs.writeFileSync(htmlPath, `<!doctype html><html><body><main>${marker}</main></body></html>`, "utf8");
    const target = fileURL(htmlPath);
    const probe = captureHealthcheckBrowser(target, {
      id: "healthcheck",
      width: 320,
      height: 240
    }, [marker], 5000);
    if (!probe.browser) {
      block("no supported headless browser executable found", {
        searched_candidates: candidateBrowsers().slice(0, 20),
        attempts: probe.attempts
      });
    }
    const capture = probe.capture;
    if (capture.status === "fail" || !capture.marker_hits || capture.marker_hits[marker] !== true) {
      block("browser visual probe healthcheck failed to capture marker screenshot", {
        browser: probe.browser,
        capture,
        attempts: probe.attempts
      });
    }
    writeResult(resultEnvelope("pass", {
      healthcheck: true,
      browser: probe.browser,
      viewports: [capture],
      target: { target, target_kind: "file", target_label: "healthcheck.html" }
    }));
  } catch (error) {
    if (error instanceof ToolExit) {
      writeResult(error.result);
      return;
    }
    writeResult(resultEnvelope("block", {
      healthcheck: true,
      reason: "browser visual probe healthcheck crashed",
      error: error && error.stack ? error.stack : String(error)
    }));
  }
}

async function mainWithInput(args, setServer) {
  fs.mkdirSync(ARTIFACT_ROOT, { recursive: true });
  if (!isInside(WORKDIR, ARTIFACT_ROOT)) {
    block("artifact root escapes RHIZOME_TOOL_WORKDIR");
  }

  let server = null;
  const gitGuard = cleanGitGuard(args);
  const target = preflightTargetBeforeStartup(args);
  try {
    server = await startDevServerIfRequested(args);
    if (setServer) setServer(server);
    if (server && !server.ready) {
      block("dev server did not become ready", {
        dev_server: {
          pid: server.child && server.child.pid,
          cwd: server.cwd,
          stdout_path: server.stdoutPath,
          stderr_path: server.stderrPath,
          target_url: server.targetURL,
          ready_probe: server.ready_probe
        },
        cleanup_guarantee: args.keep_server === true ? "keep_server=true; server left running" : "owned dev server cleanup runs in finally"
      });
    }
  } catch (error) {
    if (error instanceof ToolExit) throw error;
    block("dev server start failed", { error: error && error.stack ? error.stack : String(error) });
  }

  const browser = findBrowser();
  if (!browser) {
    block("no supported headless browser executable found", {
      searched_from: BUNDLE_DIR,
      searched_candidates: candidateBrowsers().slice(0, 20),
      cleanup_guarantee: server && args.keep_server !== true ? "owned dev server cleanup runs in finally" : ""
    });
  }
  const timeoutSeconds = Math.max(3, Math.min(20, Number(args.timeout_seconds) || 12));
  const viewports = parseViewports(args.viewports);
  const markers = parseMarkers(args.markers);
  const captures = viewports.map((viewport) => captureViewport(browser, target.target, viewport, markers, timeoutSeconds * 1000));
  const failed = captures.filter((capture) => capture.status === "fail");
  const warned = captures.filter((capture) => capture.status === "warn" || capture.dom_status === "warn");
  const maxRiskScore = captures.reduce((max, capture) => Math.max(max, capture.layout_risk ? capture.layout_risk.risk_score : 0), 0);
  const primarySurfaceWarnings = captures
    .filter((capture) => capture.primary_surface_analysis && capture.primary_surface_analysis.risk_level !== "low")
    .map((capture) => ({
      viewport: capture.id,
      risk_level: capture.primary_surface_analysis.risk_level,
      risk_score: capture.primary_surface_analysis.risk_score,
      risk_signals: capture.primary_surface_analysis.risk_signals,
      guidance: capture.primary_surface_analysis.guidance
    }));
  const gitAfter = verifyCleanGitAfterProbe(gitGuard);
  if (gitAfter && !gitAfter.ok) {
    return resultEnvelope("block", {
      reason: gitAfter.reason,
      capture_status: failed.length ? "fail" : (warned.length ? "warn" : "pass"),
      visual_quality_status: "candidate_checkout_contaminated",
      browser,
      dev_server: server ? {
        pid: server.child && server.child.pid,
        cwd: server.cwd,
        stdout_path: server.stdoutPath,
        stderr_path: server.stderrPath,
        target_url: server.targetURL,
        ready_probe: server.ready_probe
      } : null,
      target,
      viewports: captures,
      git_candidate: gitAfter,
      retry_guidance: "Restore or recreate the read-only validation checkout before publishing exact-head visual evidence."
    });
  }
  return resultEnvelope(failed.length ? "fail" : (warned.length || primarySurfaceWarnings.some((warning) => warning.risk_level === "high") ? "warn" : "pass"), {
    capture_status: failed.length ? "fail" : (warned.length ? "warn" : "pass"),
    visual_quality_status: primarySurfaceWarnings.length ? "needs_semantic_review" : "no_probe_blocker_detected",
    browser,
    dev_server: server ? {
      pid: server.child && server.child.pid,
      cwd: server.cwd,
      stdout_path: server.stdoutPath,
      stderr_path: server.stderrPath,
      target_url: server.targetURL,
      ready_probe: server.ready_probe
    } : null,
    target,
    viewports: captures,
    layout_risk_summary: {
      max_risk_score: maxRiskScore,
      risk_level: maxRiskScore >= 50 ? "high" : (maxRiskScore >= 25 ? "medium" : "low"),
      high_risk_viewports: captures.filter((capture) => capture.layout_risk && capture.layout_risk.risk_level === "high").map((capture) => capture.id)
    },
    primary_surface_risk_summary: {
      warning_count: primarySurfaceWarnings.length,
      high_risk_viewports: primarySurfaceWarnings.filter((warning) => warning.risk_level === "high").map((warning) => warning.viewport),
      warnings: primarySurfaceWarnings
    },
    markers,
    git_candidate: gitAfter && gitAfter.ok ? gitAfter.after : undefined
  });
}

if (process.argv.includes("--healthcheck")) {
  runHealthcheck();
} else {
  run();
}
