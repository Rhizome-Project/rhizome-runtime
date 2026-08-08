const assert = require("assert");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawnSync } = require("child_process");

const repoRoot = path.resolve(__dirname, "..", "..", "..");
const visualProbe = path.join(repoRoot, "agent", "tool_library", "browser_visual_probe", "browser_visual_probe.js");
const browserSession = path.join(repoRoot, "agent", "tool_library", "browser_session", "browser_session.js");

function runTool(script, input, workdir) {
  const env = {
    ...process.env,
    RHIZOME_TOOL_WORKDIR: workdir,
    RHIZOME_TOOL_ARTIFACT_DIR: path.join(workdir, "artifacts", path.basename(script)),
    RHIZOME_BROWSER_CANDIDATES: path.join(workdir, "missing-browser.exe")
  };
  const result = spawnSync(process.execPath, [script], {
    cwd: workdir,
    env,
    input: JSON.stringify(input),
    encoding: "utf8",
    timeout: 15000,
    windowsHide: true
  });
  assert.strictEqual(result.status, 0, result.stderr || result.stdout);
  return JSON.parse(result.stdout);
}

function withTempWorkdir(fn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "rhizome-browser-smoke-"));
  try {
    fn(dir);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

function hasGit() {
  return spawnSync("git", ["--version"], { encoding: "utf8", timeout: 5000, windowsHide: true }).status === 0;
}

function git(cwd, args) {
  const result = spawnSync("git", args, { cwd, encoding: "utf8", timeout: 5000, windowsHide: true });
  assert.strictEqual(result.status, 0, result.stderr || result.stdout);
  return result;
}

const visualSource = fs.readFileSync(visualProbe, "utf8");
const sessionSource = fs.readFileSync(browserSession, "utf8");
const windowsUserHomePattern = /[A-Za-z]:\\+Users\\+[^\\/"'\s]+/i;
assert(!windowsUserHomePattern.test(visualSource), "visual probe must not contain user-specific browser paths");
assert(!windowsUserHomePattern.test(sessionSource), "browser session must not contain user-specific browser paths");

withTempWorkdir((workdir) => {
  const result = runTool(browserSession, {
    action: "open",
    session_id: "missing-browser",
    url: "about:blank",
    headless: true
  }, workdir);
  assert.strictEqual(result.status, "block");
  assert.strictEqual(result.reason, "no browser executable found");
});

withTempWorkdir((workdir) => {
  const sessionDir = path.join(workdir, ".runtime-config", "browser-sessions", "unowned");
  fs.mkdirSync(sessionDir, { recursive: true });
  fs.writeFileSync(path.join(sessionDir, "session.json"), JSON.stringify({
    session_id: "unowned",
    pid: process.pid,
    port: 9,
    profile_dir: path.join(sessionDir, "profile")
  }), "utf8");
  const result = runTool(browserSession, {
    action: "close",
    session_id: "unowned"
  }, workdir);
  assert.strictEqual(result.status, "block");
  assert.strictEqual(result.reason, "browser session ownership could not be validated; skipped pid kill");
});

withTempWorkdir((workdir) => {
  fs.writeFileSync(path.join(workdir, "agent.anatomy.json"), JSON.stringify({
    concurrency: { max_browser_sessions: 1 }
  }), "utf8");
  const sessionDir = path.join(workdir, ".runtime-config", "browser-sessions", "foreign-live");
  fs.mkdirSync(sessionDir, { recursive: true });
  fs.writeFileSync(path.join(sessionDir, "session.json"), JSON.stringify({
    session_id: "foreign-live",
    pid: process.pid,
    port: 9,
    profile_dir: path.join(sessionDir, "profile")
  }), "utf8");
  const result = runTool(browserSession, {
    action: "open",
    session_id: "new-owned",
    url: "about:blank",
    headless: true
  }, workdir);
  assert.strictEqual(result.status, "block");
  assert.strictEqual(result.reason, "no browser executable found");
});

withTempWorkdir((workdir) => {
  fs.writeFileSync(path.join(workdir, "index.html"), "<!doctype html><title>Smoke</title>", "utf8");
  const result = runTool(visualProbe, {
    html_path: "index.html",
    viewports: [{ id: "tiny", width: 320, height: 240 }]
  }, workdir);
  assert.strictEqual(result.status, "block");
  assert.strictEqual(result.reason, "no supported headless browser executable found");
  const reportPath = path.join(workdir, "artifacts", path.basename(visualProbe), "probe-report.json");
  assert(fs.existsSync(reportPath), "visual probe should materialize probe-report.json");
  assert.strictEqual(JSON.parse(fs.readFileSync(reportPath, "utf8")).reason, result.reason);
});

if (hasGit()) {
  withTempWorkdir((workdir) => {
    fs.writeFileSync(path.join(workdir, "index.html"), "<!doctype html><title>Smoke</title>", "utf8");
    const checkout = path.join(workdir, "project-checkouts", "review-dirty");
    fs.mkdirSync(checkout, { recursive: true });
    git(checkout, ["init"]);
    fs.writeFileSync(path.join(checkout, "package-lock.json"), "{\"dirty\":true}\n", "utf8");
    const result = runTool(visualProbe, {
      cwd: checkout,
      html_path: "index.html",
      viewports: [{ id: "tiny", width: 320, height: 240 }]
    }, workdir);
    assert.strictEqual(result.status, "block");
    assert.strictEqual(result.reason, "candidate checkout is dirty before visual probe");
    assert(result.git_candidate && result.git_candidate.checkout_kind === "read_only_validation",
      "dirty review checkout should be classified as read-only validation contamination");
  });

  withTempWorkdir((workdir) => {
    fs.writeFileSync(path.join(workdir, "index.html"), "<!doctype html><title>Smoke</title>", "utf8");
    const checkout = path.join(workdir, "project-checkouts", "p-owned");
    fs.mkdirSync(checkout, { recursive: true });
    git(checkout, ["init"]);
    fs.writeFileSync(path.join(checkout, "package-lock.json"), "{\"dirty\":true}\n", "utf8");
    const result = runTool(visualProbe, {
      cwd: checkout,
      html_path: "index.html",
      viewports: [{ id: "tiny", width: 320, height: 240 }]
    }, workdir);
    assert.strictEqual(result.status, "block");
    assert.strictEqual(result.reason, "no supported headless browser executable found");
  });
}

withTempWorkdir((workdir) => {
  const startedFlag = path.join(workdir, "started.flag");
  const startCommand = `"${process.execPath}" -e "require('fs').writeFileSync('started.flag','1');setInterval(()=>{},1000)"`;
  const result = runTool(visualProbe, {
    html_path: "missing.html",
    start_command: startCommand,
    port: 65501
  }, workdir);
  assert.strictEqual(result.status, "block");
  assert.strictEqual(result.reason, "html_path does not exist");
  assert(!fs.existsSync(startedFlag), "target preflight should block before starting the dev server");
});

withTempWorkdir((workdir) => {
  const result = runTool(visualProbe, {
    start_command: ["rhizome-definitely-missing-dev-server-command"],
    port: 65502
  }, workdir);
  assert.strictEqual(result.status, "block");
  assert(
    result.reason === "dev server start failed" || result.reason === "dev server did not become ready",
    "missing argv start_command should return contract JSON instead of crashing"
  );
});

withTempWorkdir((workdir) => {
  const result = runTool(visualProbe, {
    start_command: [
      process.execPath,
      "-e",
      "require('http').createServer((req,res)=>res.end('ok')).listen(process.env.PORT);setInterval(()=>{},1000)"
    ],
    port: 65503,
    wait_timeout_seconds: 5
  }, workdir);
  assert.strictEqual(result.status, "block");
  assert.strictEqual(result.reason, "no supported headless browser executable found");
  assert(fs.existsSync(path.join(workdir, "artifacts", path.basename(visualProbe), "dev-server.cleanup.txt")),
    "argv dev server should be owned and cleaned up");
});

process.stdout.write("browser tool smoke tests passed\n");
