package server

import (
	"html"
	"net/http"
	"net/url"
	"os"
	"strings"
)

const defaultDashboardSourceURL = "https://github.com/Rhizome-Project/rhizome-runtime"

// ServeDashboard returns an http.HandlerFunc that serves the Rhizome dashboard.
func ServeDashboard() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'; object-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		page := strings.Replace(dashboardHTML, "{{RHIZOME_SOURCE_URL}}", html.EscapeString(dashboardSourceURL()), 1)
		_, _ = w.Write([]byte(page))
	}
}

func dashboardSourceURL() string {
	raw := strings.TrimSpace(os.Getenv("RHIZOME_SOURCE_URL"))
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return defaultDashboardSourceURL
	}
	return parsed.String()
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rhizome Dashboard</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22 fill=%22%23a855f7%22>⬡</text></svg>">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#050505;--surface:#101015;--surface-2:#16161c;--card:#0a0a0d;--border:#1f1f24;
  --border-strong:#2a2a30;--border-soft:rgba(168,85,247,.16);
  --text:#e8e6e3;--muted:#a8a5a0;--faint:#847f79;
  --accent:#a855f7;--accent-strong:#7c3aed;--accent2:#d946ef;--magenta:#d946ef;
  --green:#4ea674;--red:#e06a6a;--yellow:#d6a23c;--blue:#5b9fe0;
  --orange:#d9813f;--glow:rgba(168,85,247,.14);
  --radius:8px;--radius-sm:6px;--radius-xs:4px;
  --font:Inter,system-ui,-apple-system,'Segoe UI',sans-serif;
  --font-mono:'Cascadia Code','SFMono-Regular',Consolas,ui-monospace,monospace;
}
body{font-family:var(--font);color:var(--text);min-height:100vh;background:#050505}
a{color:var(--accent);text-decoration:none}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:3px}
button:focus-visible,.tab:focus-visible,a:focus-visible,[role="button"]:focus-visible,input:focus-visible,select:focus-visible,textarea:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
/* Command palette (Cmd/Ctrl+K) */
.cmdk-overlay{position:fixed;inset:0;background:rgba(0,0,0,.5);backdrop-filter:blur(2px);display:none;align-items:flex-start;justify-content:center;z-index:1000;padding-top:14vh}
.cmdk-overlay.open{display:flex}
.cmdk-box{width:min(560px,calc(100% - 32px));height:fit-content;background:rgba(16,16,20,.92);backdrop-filter:blur(24px) saturate(140%);-webkit-backdrop-filter:blur(24px) saturate(140%);border:1px solid var(--border-strong);border-radius:var(--radius);box-shadow:0 24px 70px rgba(0,0,0,.5);overflow:hidden}
.cmdk-input{width:100%;box-sizing:border-box;background:transparent;border:none;border-bottom:1px solid var(--border);color:var(--text);font-family:var(--font);font-size:15px;padding:16px 18px;outline:none}
.cmdk-input::placeholder{color:var(--faint)}
.cmdk-list{max-height:50vh;overflow-y:auto;padding:6px}
.cmdk-item{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:var(--radius-sm);cursor:pointer;color:var(--text);font-size:13px}
.cmdk-item .cmdk-kind{font-family:var(--font-mono);font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:var(--faint);background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-xs);padding:2px 6px;flex:0 0 auto}
.cmdk-item .cmdk-label{flex:1 1 auto;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.cmdk-item .cmdk-hint{color:var(--faint);font-size:11px;font-family:var(--font-mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:42%}
.cmdk-item.active{background:rgba(168,85,247,.14)}
.cmdk-empty{padding:18px;text-align:center;color:var(--muted);font-size:13px}
/* RPC access log */
.rpc-log-stats{display:flex;gap:8px;padding:12px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.rpc-log-stat{display:inline-flex;align-items:baseline;gap:6px;font-size:10px;letter-spacing:.04em;text-transform:uppercase;color:var(--faint);background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:6px 11px}
.rpc-log-stat b{font-size:14px;color:var(--text);font-weight:700;letter-spacing:0;text-transform:none}
.rpc-log-stat.err b{color:var(--red)}
.rpc-log-head,.rpc-log-row{display:grid;grid-template-columns:44px minmax(170px,1.3fr) 92px 58px 2.4fr 88px;align-items:center;gap:10px;padding:6px 16px}
.rpc-log-head{color:var(--faint);font-family:var(--font-mono);font-size:9px;letter-spacing:.12em;text-transform:uppercase;border-bottom:1px solid var(--border)}
.rpc-log-row{border-bottom:1px solid var(--border);border-left:2px solid transparent;transition:background .12s;font-size:11px}
.rpc-log-row:hover{background:var(--surface)}
.rpc-log-row.err{border-left-color:var(--red);background:rgba(224,106,106,.05)}
.rpc-log-row.err:hover{background:rgba(224,106,106,.1)}
.rpc-log-badge{font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:.06em;padding:2px 0;border-radius:var(--radius-xs);text-align:center}
.rpc-log-badge.ok{background:rgba(78,166,116,.14);color:var(--green)}
.rpc-log-badge.err{background:rgba(224,106,106,.16);color:var(--red)}
.rpc-log-method{font-family:var(--font-mono);color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.rpc-log-actor{color:var(--muted);font-family:var(--font-mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.rpc-log-lat{text-align:right;font-family:var(--font-mono)}
.rpc-log-msg{color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
.rpc-log-row.err .rpc-log-msg{color:#c98b8b}
.rpc-log-time{color:var(--faint);font-family:var(--font-mono);font-size:10px;text-align:right;white-space:nowrap}
/* Interactions feed */
.ix-noise{display:inline-flex;align-items:center;gap:6px;font-size:11px;color:var(--muted);cursor:pointer;user-select:none}
.ix-noise input{accent-color:var(--accent);cursor:pointer}
.ix-head,.ix-row{display:grid;grid-template-columns:84px minmax(110px,1fr) minmax(150px,1.3fr) 2fr 78px;align-items:center;gap:12px;padding:8px 16px}
.ix-head{color:var(--faint);font-family:var(--font-mono);font-size:9px;letter-spacing:.12em;text-transform:uppercase;border-bottom:1px solid var(--border)}
.ix-row{border-bottom:1px solid var(--border);border-left:2px solid transparent;font-size:12px;transition:background .12s}
.ix-row:hover{background:var(--surface)}
.ix-row.ask{border-left-color:var(--accent)}
.ix-row.tool{border-left-color:var(--blue)}
.ix-row.err{border-left-color:var(--red)}
.ix-kind{font-family:var(--font-mono);font-size:9px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:2px 0;border-radius:var(--radius-xs);text-align:center}
.ix-kind.ask{background:rgba(168,85,247,.16);color:var(--accent)}
.ix-kind.tool{background:rgba(91,159,224,.16);color:var(--blue)}
.ix-kind.execution{background:rgba(214,162,60,.14);color:var(--yellow)}
.ix-kind.session{background:rgba(78,166,116,.14);color:var(--green)}
.ix-kind.task{background:rgba(168,85,247,.1);color:var(--accent)}
.ix-actor{font-family:var(--font-mono);color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ix-action{color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ix-detail{color:var(--faint);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
.ix-time{color:var(--faint);font-family:var(--font-mono);font-size:10px;text-align:right;white-space:nowrap}
button{font-family:var(--font)}
.hdr-btn,.btn-accent,.participant-btn,.msg-btn{
  appearance:none;display:inline-flex;align-items:center;justify-content:center;gap:8px;
  min-height:34px;padding:8px 14px;border-radius:var(--radius-sm);font-size:12px;font-weight:600;
  cursor:pointer;transition:border-color .15s,background .15s,color .15s;
  font-family:var(--font);white-space:nowrap;text-decoration:none
}
.hdr-btn{
  background:var(--surface);
  border:1px solid var(--border);color:var(--text)
}
.hdr-btn:hover{border-color:var(--border-strong);background:var(--surface-2)}
.hdr-btn:disabled{opacity:.5;cursor:not-allowed}
.btn-accent,.msg-btn{
  background:var(--accent-strong);
  border:1px solid var(--accent-strong);color:#f5f0ff
}
.btn-accent:hover,.msg-btn:hover{background:var(--accent);border-color:var(--accent)}
.btn-accent:disabled,.msg-btn:disabled{opacity:.5;cursor:not-allowed}

/* Header */
.header{background:rgba(5,5,7,.88);border-bottom:1px solid var(--border);padding:14px 24px;display:flex;align-items:center;gap:16px;position:sticky;top:0;z-index:100;backdrop-filter:blur(16px)}
.header h1{font-size:18px;font-weight:700;letter-spacing:.02em;color:var(--text);cursor:pointer}
.pulse{width:8px;height:8px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.header .hdr-btn{min-height:31px;padding:6px 11px;font-size:11px;color:var(--muted)}
.header .hdr-btn:hover{color:var(--text)}
.header .profile-wrap{position:relative;display:none;align-items:center;margin-left:auto;flex:0 0 auto}
.header .tabs{min-width:0;flex:1 1 auto}
.header .profile-wrap.open .profile-menu{display:flex}
.header .profile-btn{display:inline-flex;align-items:center;gap:8px}
.header .profile-label{max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.header .profile-caret{font-size:9px;opacity:.75}
.header .profile-menu{display:none;position:absolute;top:calc(100% + 8px);right:0;min-width:190px;background:var(--card);border:1px solid var(--border);border-radius:10px;padding:6px;box-shadow:0 10px 30px rgba(0,0,0,.35);z-index:180;flex-direction:column;gap:4px}
.header .profile-menu button{background:var(--surface);border:1px solid transparent;color:var(--text);padding:8px 10px;border-radius:8px;cursor:pointer;font-size:12px;text-align:left;transition:all .2s;font-family:var(--font)}
.header .profile-menu button:hover{border-color:var(--accent)}
.header .profile-menu .danger{color:var(--red);background:rgba(224,106,106,.12)}

/* Filters */
.filters{display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap;align-items:center}
.filter-btn{background:var(--surface);border:1px solid var(--border);color:var(--muted);padding:5px 11px;border-radius:var(--radius-sm);font-family:var(--font-mono);font-size:10px;text-transform:uppercase;letter-spacing:.08em;cursor:pointer;transition:border-color .15s,color .15s,background .15s}
.filter-btn:hover{border-color:var(--border-strong);color:var(--text)}
.filter-btn.active{background:var(--accent-strong);border-color:var(--accent-strong);color:#f5f0ff}
.filter-search{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);padding:6px 11px;color:var(--text);font-size:11px;outline:none;font-family:var(--font);min-width:160px;transition:border-color .15s}
.filter-search:focus{border-color:var(--accent)}

/* Participant dropdown */
.participant-wrap{position:relative;display:inline-block}
.participant-btn{min-height:30px;padding:6px 11px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border);color:var(--muted);font-size:11px;box-shadow:none}
.participant-btn:hover{border-color:var(--border-strong);color:var(--text)}
.participant-btn.has-filter{background:var(--accent-strong);border-color:var(--accent-strong);color:#f5f0ff}
.participant-drop{display:none;position:absolute;top:100%;left:0;margin-top:4px;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:6px;min-width:200px;z-index:100;box-shadow:0 8px 24px rgba(0,0,0,.4);max-height:250px;overflow-y:auto}
.participant-drop.open{display:block}
.participant-drop label{display:flex;align-items:center;gap:6px;padding:4px 6px;border-radius:4px;font-size:11px;color:var(--text);cursor:pointer;transition:background .15s}
.participant-drop label:hover{background:var(--surface)}
.participant-drop input[type=checkbox]{accent-color:var(--accent)}
.participant-drop .p-divider{border-top:1px solid var(--border);margin:4px 0}

/* Tool cards */
.tool-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px}
.tool-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;cursor:pointer;transition:all .2s}
.tool-card:hover{border-color:var(--accent);transform:translateY(-1px)}
.tool-name{font-size:13px;font-weight:600;color:var(--text);margin-bottom:4px}
.tool-desc{font-size:11px;color:var(--muted);margin-bottom:6px;line-height:1.4}
.tool-badges{display:flex;gap:4px;flex-wrap:wrap}
.tool-badge{font-size:9px;padding:2px 6px;border-radius:4px;font-weight:600}
.tool-badge.active{background:rgba(78,166,116,.15);color:var(--green)}
.tool-badge.planned{background:rgba(91,159,224,.15);color:var(--accent)}
.tool-badge.blocked{background:rgba(224,106,106,.15);color:var(--red)}
.tool-badge.kind{background:var(--card);color:var(--muted)}
.add-mcp-form{display:none;margin-top:12px}
.add-mcp-form.open{display:block}
.add-mcp-textarea{width:100%;min-height:100px;background:var(--surface);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px;font-size:12px;font-family:var(--font);resize:vertical;outline:none}
.add-mcp-textarea:focus{border-color:var(--accent)}

/* Create Task form */
.create-task-form{display:none;margin-bottom:14px}
.create-task-form.open{display:block}
.create-task-form .form-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px}
.create-task-form .form-grid.full{grid-template-columns:1fr}
.create-task-form label{font-family:var(--font-mono);font-size:9px;font-weight:600;color:var(--faint);text-transform:uppercase;letter-spacing:.16em;display:block;margin-bottom:5px}
.create-task-form input,.create-task-form select,.create-task-form textarea{width:100%;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);padding:9px 12px;font-size:12px;font-family:var(--font);outline:none;transition:border-color .15s}
.create-task-form input:focus,.create-task-form select:focus,.create-task-form textarea:focus{border-color:var(--accent)}
.create-task-form textarea{min-height:60px;resize:vertical}
.create-task-form .form-actions{display:flex;gap:8px;align-items:center;margin-top:8px}

/* Cancelled tasks section */
.cancelled-section{margin-top:14px;border-top:1px solid var(--border);padding-top:10px}
.cancelled-toggle{background:none;border:none;color:var(--muted);font-size:12px;cursor:pointer;font-family:var(--font);padding:4px 0;display:flex;align-items:center;gap:6px}
.cancelled-toggle:hover{color:var(--text)}
.cancelled-list{display:none;margin-top:8px}
.cancelled-list.open{display:block}
.cancelled-item{display:flex;justify-content:space-between;align-items:center;padding:6px 10px;background:var(--surface);border:1px solid var(--border);border-radius:8px;margin-bottom:4px;opacity:.65;cursor:pointer;transition:opacity .2s}
.cancelled-item:hover{opacity:1;border-color:var(--accent)}
.cancelled-item .ci-title{font-size:12px;color:var(--text);text-decoration:line-through}
.cancelled-item .ci-meta{font-size:10px;color:var(--muted)}

/* Delete confirmation overlay */
.confirm-overlay{display:none;position:fixed;inset:0;background:rgba(3,3,5,.7);backdrop-filter:blur(10px);z-index:500;justify-content:center;align-items:center}
.confirm-overlay.open{display:flex}
.confirm-box{background:var(--card);border:1px solid var(--border-strong);border-radius:var(--radius);padding:22px 24px;max-width:440px;width:90%;box-shadow:0 24px 80px rgba(0,0,0,.55)}
.confirm-box h3{margin:0 0 8px;font-size:15px;color:var(--text)}
.confirm-box p{font-size:12px;color:var(--muted);margin:0 0 12px;line-height:1.5}
.confirm-box textarea,.resolve-box textarea{width:100%;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);padding:12px 14px;font-size:12px;font-family:var(--font);min-height:72px;resize:vertical;outline:none;margin-bottom:12px;line-height:1.5;transition:border-color .15s}
.confirm-box textarea:focus,.resolve-box textarea:focus{border-color:var(--accent)}
.confirm-box .btn-row,.resolve-box .btn-row{display:flex;gap:10px;justify-content:flex-end;flex-wrap:wrap}
.confirm-box .btn-row button,.resolve-box .btn-row button{appearance:none;display:inline-flex;align-items:center;justify-content:center;min-height:38px;padding:10px 18px;border-radius:var(--radius-sm);font-size:12px;font-weight:600;cursor:pointer;font-family:var(--font);transition:border-color .15s,background .15s,opacity .15s}
.confirm-box .btn-cancel,.resolve-box .btn-cancel{background:var(--surface);border:1px solid var(--border);color:var(--text)}
.confirm-box .btn-cancel:hover,.resolve-box .btn-cancel:hover{border-color:var(--border-strong);background:var(--surface-2)}
.confirm-box .btn-danger{background:var(--red);border:1px solid var(--red);color:#fff}
.confirm-box .btn-danger:hover{opacity:.85}

/* Tabs */
.tabs{display:flex;gap:2px;background:var(--card);border:1px solid var(--border);padding:4px;border-radius:var(--radius);margin-bottom:16px;overflow-x:auto;white-space:nowrap;scrollbar-width:none;-ms-overflow-style:none}
.tabs::-webkit-scrollbar{display:none;}
.tab{flex:0 0 auto;padding:7px 14px;text-align:center;font-family:var(--font-mono);font-size:11px;text-transform:uppercase;letter-spacing:.06em;border-radius:var(--radius-sm);cursor:pointer;color:var(--muted);transition:color .15s,background .15s;border:1px solid transparent}
.tab:hover{color:var(--text);background:var(--surface)}
.tab.active{background:rgba(168,85,247,.12);color:var(--accent);border-color:rgba(168,85,247,.28)}
.tab-badge{display:inline-flex;align-items:center;justify-content:center;min-width:17px;height:16px;padding:0 5px;margin-left:6px;border-radius:999px;font-family:var(--font-mono);font-size:10px;font-weight:700;line-height:1;letter-spacing:0;background:rgba(168,85,247,.16);color:var(--accent)}
.tab-badge.warn{background:rgba(214,162,60,.2);color:var(--yellow)}
.tab-badge.alert{background:rgba(224,106,106,.2);color:var(--red)}
.tab-panel{display:none}
.tab-panel.active{display:block}

/* Stats toggle buttons */
.stats-mode-btn{background:transparent;border:none;color:var(--muted);padding:3px 10px;border-radius:5px;font-size:10px;font-weight:600;cursor:pointer;font-family:var(--font);transition:all .2s}
.stats-mode-btn:hover{color:var(--text)}
.stats-mode-btn.active{background:var(--accent);color:#fff}

/* Layout */
.container{max-width:1400px;margin:0 auto;padding:16px 20px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}
.grid-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:14px}
@media(max-width:900px){.grid,.grid-3{grid-template-columns:1fr}}

/* Cards */
.card{position:relative;background:var(--card);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden;transition:border-color .15s}
.card:hover{border-color:var(--border-strong)}
.card-header{padding:13px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}
.card-header h2{font-family:var(--font-mono);font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.16em;color:var(--muted)}
.card-header .badge{display:inline-flex;align-items:center;justify-content:center;min-height:24px;padding:0 10px;border-radius:var(--radius-xs);background:var(--accent-strong);color:#f5f0ff;font-size:11px;font-weight:600;letter-spacing:.02em}
.card-body{padding:14px 16px}

/* Graph HUD */
.graph-shell{position:relative;height:calc(100vh - 180px);margin-bottom:14px;overflow:hidden;background:#07070b;border:1px solid var(--border);border-radius:var(--radius)}
.graph-canvas{position:absolute;inset:0}
.graph-overlay{position:absolute;z-index:4;pointer-events:none}
.graph-overlay>*{pointer-events:auto}
.graph-overlay-left{top:18px;left:18px;max-width:min(440px,calc(100% - 36px))}
.graph-overlay-right{top:18px;right:18px;width:min(340px,calc(100% - 36px))}
.graph-title-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);padding:16px 18px;box-shadow:0 8px 28px rgba(0,0,0,.3)}
.graph-title-kicker{font-size:11px;font-weight:700;letter-spacing:.16em;text-transform:uppercase;color:var(--accent)}
.graph-title-text{font-size:28px;font-weight:700;line-height:1.05;letter-spacing:-.04em;color:var(--text);margin-bottom:10px}
.graph-title-sub{font-size:12px;line-height:1.55;color:var(--muted);max-width:42ch}
.graph-toolbar-overlay{position:absolute!important;top:18px;left:18px!important;right:auto!important;width:min(356px,calc(100% - 36px))!important;max-height:calc(100% - 36px);border:1px solid rgba(255,255,255,.09)!important;border-radius:var(--radius)!important;padding:14px!important;background:rgba(12,12,16,.46)!important;backdrop-filter:blur(20px) saturate(140%);-webkit-backdrop-filter:blur(20px) saturate(140%);box-shadow:0 16px 48px rgba(0,0,0,.42);overflow:auto;transition:width .16s ease,padding .16s ease}
.graph-toolbar-overlay::-webkit-scrollbar{width:8px}
.graph-toolbar-overlay::-webkit-scrollbar-thumb{background:rgba(168,85,247,.22);border-radius:999px}
.graph-display-settings-overlay{position:absolute!important;top:18px;right:18px!important;left:auto!important;width:min(320px,calc(100% - 36px))!important;max-height:calc(100% - 36px);border:1px solid rgba(255,255,255,.09)!important;border-radius:var(--radius)!important;padding:14px!important;background:rgba(12,12,16,.46)!important;backdrop-filter:blur(20px) saturate(140%);-webkit-backdrop-filter:blur(20px) saturate(140%);box-shadow:0 16px 48px rgba(0,0,0,.42);overflow:auto;z-index:4;transition:width .16s ease,padding .16s ease}
/* Display Settings collapsed -> tidy square gear chip */
.graph-display-settings-overlay.panel-collapsed{width:auto!important;min-width:0!important;max-height:none!important;padding:6px!important;overflow:visible}
.graph-display-settings-overlay.panel-collapsed .graph-section{padding:0!important}
.graph-display-settings-overlay.panel-collapsed .graph-section-toggle{width:auto;justify-content:center;gap:0;padding:0}
.graph-display-settings-overlay.panel-collapsed .graph-section-toggle > span:first-child{display:none}
.graph-settings-gear{display:inline-flex;align-items:center;justify-content:center;color:var(--muted);transition:color .15s,transform .15s}
.graph-settings-gear svg{display:block}
.graph-section-toggle:hover .graph-settings-gear{color:var(--accent);transform:rotate(35deg)}
/* Workspace Graph chevron toggle */
.graph-controls-chevron{display:inline-flex;align-items:center;justify-content:center;color:var(--muted);transition:transform .16s ease,color .15s}
.graph-controls-chevron svg{display:block}
.graph-controls-toggle:hover .graph-controls-chevron{color:var(--accent)}
.graph-controls-chevron.is-collapsed{transform:rotate(-90deg)}
.graph-display-settings-overlay::-webkit-scrollbar{width:8px}
.graph-display-settings-overlay::-webkit-scrollbar-thumb{background:rgba(168,85,247,.22);border-radius:999px}
.graph-toolbar-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px}
.graph-toolbar-title{font-size:24px!important;font-weight:700;line-height:1.02;letter-spacing:-.04em;color:var(--text)!important;margin:0!important}
.graph-controls-body{display:flex;flex-direction:column;gap:10px}
.graph-controls-body.is-collapsed{display:none}
.graph-section{padding:12px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border)}
.graph-meta-pill{display:none;margin-top:12px;padding:10px 12px;border-radius:var(--radius-sm);background:rgba(168,85,247,.10);border:1px solid var(--border-soft);font-size:11px;line-height:1.5;color:var(--muted)}
.graph-stats-pill{margin-top:12px;display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border);font-size:11px;line-height:1.4;color:var(--muted)}
.graph-controls-shell{display:flex;flex-direction:column;gap:10px;align-items:flex-end}
.graph-controls-toggle{display:inline-flex;align-items:center;justify-content:center;gap:0;width:32px;height:32px;padding:0;border-radius:10px;border:none;background:transparent;color:var(--text);font-size:16px;font-weight:700;cursor:pointer;box-shadow:none;transition:transform .18s,background .18s,color .18s}
.graph-controls-toggle:hover{transform:translateY(-1px);background:rgba(168,85,247,.08);color:#d8c4f5}
.graph-controls-toggle-icon{font-size:16px;line-height:1;color:var(--accent)}
.graph-controls-panel{width:100%;max-height:calc(100vh - 260px);overflow:auto;padding:14px;border-radius:var(--radius);background:var(--card);border:1px solid var(--border-strong);box-shadow:0 16px 48px rgba(0,0,0,.42)}
.graph-controls-panel.is-collapsed{display:none}
.graph-controls-panel::-webkit-scrollbar{width:8px}
.graph-controls-panel::-webkit-scrollbar-thumb{background:rgba(168,85,247,.22);border-radius:999px}
.graph-panel-section{padding:12px 12px 13px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border)}
.graph-panel-section + .graph-panel-section{margin-top:10px}
.graph-panel-label{display:block;margin-bottom:8px;font-size:10px;font-weight:700;letter-spacing:.16em;text-transform:uppercase;color:var(--faint)}
.graph-form-select{width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);padding:11px 12px;border-radius:var(--radius-sm);font-size:12px;outline:none;transition:border-color .18s,background .18s}
.graph-form-select:focus{border-color:var(--accent)}
.graph-inline-hint{margin-top:8px;font-size:11px;line-height:1.45;color:var(--muted)}
.graph-lens-bar{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}
.graph-lens-chip{appearance:none;border:1px solid var(--border);background:var(--surface);color:var(--muted);border-radius:var(--radius-sm);padding:7px 10px;font-size:11px;font-weight:600;cursor:pointer;transition:background .15s,border-color .15s,color .15s}
.graph-lens-chip:hover{border-color:var(--border-strong);color:var(--text)}
.graph-lens-chip.active{background:rgba(168,85,247,.12);border-color:rgba(168,85,247,.28);color:var(--accent)}
.graph-atlas-nav{display:none;margin-top:10px;padding:10px 12px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border)}
.graph-atlas-nav-row{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.graph-atlas-history{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
.graph-atlas-history-chip{appearance:none;border:1px solid var(--border);background:var(--surface);color:var(--muted);border-radius:var(--radius-sm);padding:6px 10px;font-size:11px;font-weight:600;cursor:pointer;transition:background .15s,border-color .15s,color .15s}
.graph-atlas-history-chip:hover{border-color:var(--border-strong);color:var(--text)}
.graph-atlas-legend{margin-top:10px;padding:10px 12px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border)}
.graph-atlas-legend-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px 12px;margin-top:8px}
.graph-atlas-legend-item{display:flex;align-items:flex-start;gap:8px;font-size:11px;line-height:1.35;color:var(--muted)}
.graph-atlas-legend-dot{width:10px;height:10px;border-radius:999px;margin-top:2px;flex:0 0 auto}
.graph-atlas-legend-ring{width:10px;height:10px;border-radius:999px;margin-top:2px;flex:0 0 auto;border:2px solid transparent;background:transparent}
.graph-action-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.graph-action-grid .wide{grid-column:1/-1}
.graph-panel-btn{width:100%;border:1px solid transparent;border-radius:var(--radius-sm);padding:11px 12px;font-size:12px;font-weight:600;cursor:pointer;transition:background .15s,border-color .15s;font-family:var(--font)}
.graph-panel-btn-primary{background:var(--accent-strong);border-color:var(--accent-strong);color:#f5f0ff}
.graph-panel-btn-primary:hover{background:var(--accent)}
.graph-panel-btn-secondary{background:var(--surface);border:1px solid var(--border);color:var(--text)}
.graph-panel-btn-secondary:hover{border-color:var(--border-strong)}
.graph-panel-btn-ghost{background:rgba(168,85,247,.10);border:1px solid var(--border-soft);color:var(--accent)}
.graph-panel-btn-ghost:hover{background:rgba(168,85,247,.16)}
.graph-layer-toggle{display:flex;align-items:flex-start;gap:10px;cursor:pointer;color:var(--text);font-size:12px;line-height:1.4}
.graph-layer-toggle input{margin-top:2px;accent-color:var(--accent)}
.graph-range-group + .graph-range-group{margin-top:10px}
.graph-range-head{display:flex;align-items:center;justify-content:space-between;font-size:11px;color:var(--text);margin-bottom:6px}
.graph-range-head span:last-child{color:#cbb8e8}
.graph-range-input{width:100%;appearance:none;background:transparent}
.graph-range-input::-webkit-slider-runnable-track{height:6px;border-radius:999px;background:var(--accent-strong)}
.graph-range-input::-moz-range-track{height:6px;border-radius:999px;background:var(--accent-strong)}
.graph-range-input::-webkit-slider-thumb{appearance:none;width:16px;height:16px;border-radius:50%;background:#f4f0ff;border:2px solid var(--accent);margin-top:-5px}
.graph-range-input::-moz-range-thumb{width:16px;height:16px;border-radius:50%;background:#f4f0ff;border:2px solid var(--accent)}
.graph-legend{display:grid;gap:8px}
.graph-legend-title{font-size:11px;font-weight:700;color:var(--text)}
.graph-legend-item{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--muted)}
.graph-legend-line{display:inline-block;width:18px;height:2px;border-radius:999px}
.graph-section-toggle{width:100%;display:flex;align-items:center;justify-content:space-between;gap:12px;padding:0;background:none;border:none;color:var(--text);font-size:12px;font-weight:700;cursor:pointer}
.graph-section-toggle small{display:block;margin-top:4px;font-size:10px;font-weight:500;color:var(--muted)}
.graph-section-toggle-icon{font-size:12px;color:var(--accent)}
.graph-section-body{margin-top:12px}
.graph-section-body.is-collapsed{display:none}
.graph-toolbar-overlay select,.graph-display-settings-overlay select,.graph-form-select{width:100%!important;background:var(--surface)!important;border:1px solid var(--border)!important;color:var(--text)!important;padding:11px 12px!important;border-radius:var(--radius-sm)!important;font-size:12px!important;outline:none}
.graph-toolbar-overlay input[type="range"],.graph-display-settings-overlay input[type="range"]{width:100%!important;appearance:none;background:transparent}
.graph-toolbar-overlay input[type="range"]::-webkit-slider-runnable-track,.graph-display-settings-overlay input[type="range"]::-webkit-slider-runnable-track{height:6px;border-radius:999px;background:var(--accent-strong)}
.graph-toolbar-overlay input[type="range"]::-moz-range-track,.graph-display-settings-overlay input[type="range"]::-moz-range-track{height:6px;border-radius:999px;background:var(--accent-strong)}
.graph-toolbar-overlay input[type="range"]::-webkit-slider-thumb,.graph-display-settings-overlay input[type="range"]::-webkit-slider-thumb{appearance:none;width:16px;height:16px;border-radius:50%;background:#f4f0ff;border:2px solid var(--accent);margin-top:-5px}
.graph-toolbar-overlay input[type="range"]::-moz-range-thumb,.graph-display-settings-overlay input[type="range"]::-moz-range-thumb{width:16px;height:16px;border-radius:50%;background:#f4f0ff;border:2px solid var(--accent)}
.graph-toolbar-overlay input[type="checkbox"],.graph-display-settings-overlay input[type="checkbox"]{appearance:none;-webkit-appearance:none;flex:0 0 auto;width:32px;height:18px;border-radius:999px;background:var(--border-strong);border:1px solid var(--border-strong);position:relative;cursor:pointer;margin:0;transition:background .15s,border-color .15s}
.graph-toolbar-overlay input[type="checkbox"]::after,.graph-display-settings-overlay input[type="checkbox"]::after{content:"";position:absolute;top:1px;left:1px;width:14px;height:14px;border-radius:50%;background:#e8e6e3;transition:transform .16s ease}
.graph-toolbar-overlay input[type="checkbox"]:checked,.graph-display-settings-overlay input[type="checkbox"]:checked{background:var(--accent-strong);border-color:var(--accent-strong)}
.graph-toolbar-overlay input[type="checkbox"]:checked::after,.graph-display-settings-overlay input[type="checkbox"]:checked::after{transform:translateX(14px)}
.graph-toolbar-overlay .hdr-btn,.graph-inspector-panel .hdr-btn{width:100%!important;border-radius:var(--radius-sm);padding:11px 12px;font-size:12px;font-weight:600;cursor:pointer;transition:border-color .15s,background .15s;font-family:var(--font);background:var(--surface)!important;border:1px solid var(--border)!important;color:var(--text)!important}
.graph-toolbar-overlay .hdr-btn:hover,.graph-inspector-panel .hdr-btn:hover{border-color:var(--border-strong)!important}
.graph-display-settings{background:transparent!important;border:none!important;padding:0!important}
.graph-display-settings-overlay .graph-section-toggle{padding:2px 0 4px}
.graph-display-settings-overlay .graph-section-body{margin-top:20px;padding-top:14px;border-top:1px solid var(--border)}
.graph-display-settings-label{display:block;font-size:12px;font-weight:700;letter-spacing:.04em;color:var(--text)}
.graph-display-settings-copy{display:block;margin-top:6px;font-size:11px;line-height:1.45;color:var(--muted)}
.graph-display-settings-copy.is-hidden{display:none}
.graph-display-settings{padding-top:0}
.graph-inspector-panel{position:absolute!important;right:18px;bottom:18px;width:min(320px,calc(100% - 36px))!important;max-height:calc(100% - 140px);padding:16px!important;border-radius:var(--radius)!important;background:rgba(12,12,16,.46)!important;backdrop-filter:blur(20px) saturate(140%);-webkit-backdrop-filter:blur(20px) saturate(140%);border:1px solid rgba(255,255,255,.09)!important;box-shadow:0 16px 48px rgba(0,0,0,.42);display:none;flex-direction:column;gap:12px;z-index:5;overflow-y:auto}
.graph-inspector-panel::-webkit-scrollbar{width:8px}
.graph-inspector-panel::-webkit-scrollbar-thumb{background:rgba(168,85,247,.22);border-radius:999px}
.graph-inspector-header{display:flex;justify-content:space-between;align-items:center;gap:10px}
.graph-inspector-title{font-size:14px;font-weight:700;color:var(--text)}
.graph-inspector-close{width:30px;height:30px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border);color:var(--muted);cursor:pointer}
.graph-inspector-body{font-size:12px;color:var(--text);line-height:1.5}
.graph-inspector-actions{margin-top:auto;display:flex;flex-direction:column;gap:8px}
@media(max-width:980px){
  .graph-shell{height:calc(100vh - 168px);min-height:620px}
  .graph-toolbar-overlay{width:min(320px,calc(100% - 36px))!important}
  .graph-display-settings-overlay,.graph-overlay-right{width:min(300px,calc(100% - 36px))!important}
}
@media(max-width:720px){
  .graph-shell{height:calc(100vh - 146px);min-height:600px}
  .graph-overlay-left{top:12px;left:12px;right:12px;max-width:none}
  .graph-toolbar-overlay{top:12px;left:12px!important;right:12px!important;width:auto!important;max-height:46vh}
  .graph-display-settings-overlay{top:auto;left:12px!important;right:12px!important;bottom:12px;width:auto!important;max-height:32vh}
  .graph-overlay-right{top:auto;right:12px;left:12px;bottom:12px;width:auto}
  .graph-controls-shell{align-items:stretch}
  .graph-controls-panel{max-height:42vh}
  .graph-title-text{font-size:22px}
  .graph-inspector-panel{left:12px;right:12px;bottom:72px;width:auto;max-height:40vh}
}

/* Stats Row */
.stats{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap}
.project-status-tab{background:none;border:none;color:var(--muted);font-size:12px;font-family:var(--font);padding:8px 16px;cursor:pointer;border-bottom:2px solid transparent;transition:all .2s}
.project-status-tab:hover{color:var(--text)}
.project-status-tab.active{color:var(--accent);border-bottom-color:var(--accent);font-weight:600}
.stat{position:relative;flex:1;min-width:100px;background:var(--card);border:1px solid var(--border);border-radius:var(--radius);padding:16px 14px;text-align:center;transition:border-color .15s}
.stat:hover{border-color:var(--border-strong)}
.stat .value{font-size:26px;font-weight:700;letter-spacing:-.02em;color:var(--text)}
.stat .label{font-family:var(--font-mono);font-size:9px;color:var(--faint);text-transform:uppercase;letter-spacing:.16em;margin-top:6px}

/* Agent List */
.agent-item{display:flex;align-items:center;gap:10px;padding:10px 12px;border-radius:8px;margin-bottom:4px;cursor:pointer;transition:background .2s}
.agent-item:hover{background:var(--surface)}
.agent-dot{width:10px;height:10px;border-radius:50%;flex-shrink:0}
.agent-dot.online{background:var(--green);box-shadow:0 0 8px rgba(78,166,116,.4)}
.agent-dot.offline{background:#555;opacity:.6}
.agent-info{flex:1;min-width:0}
.agent-name{font-weight:600;font-size:13px}
.agent-sub{font-size:10px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.agent-right{display:flex;flex-direction:column;align-items:flex-end;gap:2px}
.agent-role{font-size:10px;color:var(--muted);background:var(--surface);padding:2px 8px;border-radius:6px}
.agent-seen{font-size:9px;color:var(--muted)}
.agent-task-pills{display:flex;gap:3px;flex-wrap:wrap;margin-top:3px}
.task-pill{font-size:9px;padding:2px 6px;border-radius:4px;font-weight:500}
.task-pill.CLAIMED{background:rgba(124,58,237,.2);color:var(--accent)}
.task-pill.BLOCKED{background:rgba(214,162,60,.2);color:var(--yellow)}
.session-item{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:6px;cursor:pointer;transition:all .2s}
.session-item:hover{border-color:var(--accent);transform:translateY(-1px)}
.session-top{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin-bottom:6px}
.session-title{font-size:12px;font-weight:600;color:var(--text)}
.session-owner{font-size:10px;color:var(--muted);margin-top:2px}
.session-meta{font-size:10px;color:var(--muted);line-height:1.5}
.session-pill{display:inline-flex;align-items:center;font-size:9px;padding:2px 7px;border-radius:999px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.session-pill.ACTIVE,.session-pill.RUNNING{background:rgba(78,166,116,.15);color:var(--green)}
.session-pill.BLOCKED{background:rgba(214,162,60,.15);color:var(--yellow)}
.session-pill.WAITING_DECISION,.session-pill.HANDOFF_PENDING{background:rgba(249,115,22,.15);color:var(--orange)}
.session-pill.ENDED,.session-pill.COMPLETED,.session-pill.FAILED{background:rgba(136,136,160,.18);color:var(--muted)}

/* Workspace memory */
.memory-item{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:6px;cursor:pointer;transition:all .2s}
.memory-item:hover{border-color:var(--accent);transform:translateY(-1px)}
.memory-item.archived{opacity:.78}
.memory-top{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.memory-headline{font-size:12px;font-weight:600;color:var(--text)}
.memory-summary{font-size:11px;color:var(--text);margin-top:6px;line-height:1.5}
.memory-meta{font-size:10px;color:var(--muted);line-height:1.5;margin-top:4px}
.memory-badges{display:flex;gap:4px;flex-wrap:wrap;margin-top:8px}
.memory-badge{display:inline-flex;align-items:center;font-size:9px;padding:2px 7px;border-radius:999px;font-weight:700;text-transform:uppercase;letter-spacing:.4px}
.memory-badge.state-active{background:rgba(78,166,116,.15);color:var(--green)}
.memory-badge.state-archived{background:rgba(136,136,160,.18);color:var(--muted)}
.memory-badge.type{background:rgba(91,159,224,.15);color:var(--blue)}
.memory-badge.source{background:rgba(124,58,237,.15);color:var(--accent)}
.memory-badge.derived{background:rgba(217,70,239,.15);color:var(--accent2)}
.memory-badge.attention{background:rgba(249,115,22,.15);color:var(--orange)}
.memory-composer{display:none;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:12px;margin-bottom:10px}
.memory-composer.open{display:block}
.memory-composer label{display:block;font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
.memory-composer input,.memory-composer select,.memory-composer textarea{width:100%;background:var(--card);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:8px 10px;font-size:12px;font-family:var(--font);outline:none}
.memory-composer input:focus,.memory-composer select:focus,.memory-composer textarea:focus{border-color:var(--accent)}
.memory-form-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.memory-toolbar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:10px}
.memory-toolbar select{background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:4px 10px;font-size:11px;font-family:var(--font);outline:none}
.memory-toolbar select:focus{border-color:var(--accent)}
.memory-toolbar-check{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--muted)}
.memory-toolbar-check input{accent-color:var(--accent)}
.memory-filter-context{margin:-2px 0 10px;font-size:11px;color:var(--muted)}

/* Task Board */
.board{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
@media(max-width:900px){.board{grid-template-columns:1fr 1fr}}
.board-col{background:var(--surface);border-radius:var(--radius);padding:8px;min-height:100px}
.board-col-header{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.5px;padding:5px 8px;border-radius:6px;margin-bottom:6px;text-align:center;display:flex;align-items:center;justify-content:center;gap:4px}
.col-pending .board-col-header{background:rgba(91,159,224,.12);color:var(--blue)}
.col-claimed .board-col-header{background:rgba(168,85,247,.12);color:var(--accent)}
.col-blocked .board-col-header{background:rgba(214,162,60,.12);color:var(--yellow)}
.col-completed .board-col-header{background:rgba(78,166,116,.12);color:var(--green)}
.task-card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:10px;margin-bottom:5px;font-size:12px;cursor:pointer;transition:all .2s}
.task-card:hover{transform:translateY(-1px);border-color:rgba(124,58,237,.3)}
.task-card .task-title{font-weight:600;margin-bottom:3px;font-size:12px}
.task-card .task-meta{color:var(--muted);font-size:9px}
.task-tags{display:flex;gap:3px;flex-wrap:wrap;margin-top:3px}
.task-tag{background:rgba(124,58,237,.2);color:var(--accent);padding:1px 5px;border-radius:3px;font-size:8px;font-weight:500}
.priority-HIGH,.priority-CRITICAL{color:var(--red)}
.priority-MEDIUM{color:var(--yellow)}
.priority-LOW{color:var(--green)}

/* Docs */
.doc-item{display:flex;justify-content:space-between;align-items:center;padding:8px 12px;border-radius:8px;cursor:pointer;transition:background .2s;margin-bottom:2px}
.doc-item:hover{background:var(--surface)}
.doc-key{font-weight:600;font-size:12px}
.doc-meta{font-size:10px;color:var(--muted)}

/* Message Form */
.msg-form{display:flex;gap:8px}
.msg-input{flex:1;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px 14px;color:var(--text);font-family:var(--font);font-size:13px;outline:none;transition:border-color .2s}
.msg-input:focus{border-color:var(--accent)}
.msg-btn{min-height:38px;padding:9px 18px;font-size:13px}
.msg-btn:hover{transform:translateY(-1px);filter:brightness(1.03)}
.msg-btn:disabled{opacity:.45;cursor:not-allowed;transform:none;filter:none}
.msg-status{font-size:11px;margin-top:6px;min-height:16px}
.msg-status.ok{color:var(--green)}
.msg-status.err{color:var(--red)}

/* Activity Feed */
.feed-item{display:flex;gap:10px;padding:8px 0;border-bottom:1px solid rgba(255,255,255,.03);font-size:12px;align-items:flex-start}
.feed-item:last-child{border:none}
.feed-icon{width:24px;height:24px;border-radius:6px;display:flex;align-items:center;justify-content:center;font-size:11px;flex-shrink:0}
.feed-icon.task{background:rgba(124,58,237,.15)}
.feed-icon.msg{background:rgba(217,70,239,.15)}
.feed-icon.doc{background:rgba(78,166,116,.15)}
.feed-icon.agent{background:rgba(249,115,22,.15)}
.feed-content{flex:1;min-width:0}
.feed-main{font-size:12px}
.feed-actor{font-weight:600;color:var(--accent2)}
.feed-time{font-size:9px;color:var(--muted);margin-top:2px}

/* Messages list */
.msg-item{padding:9px 12px;border-radius:10px;margin-bottom:4px;background:rgba(255,255,255,.02);border:1px solid rgba(168,85,247,.07);font-size:12px}
.msg-from{font-weight:600;color:var(--accent2);font-size:11px}
.msg-content{margin-top:2px}
.msg-ts{font-size:9px;color:var(--muted);margin-top:2px}

/* Modal */
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(3,3,5,.72);z-index:200;justify-content:center;align-items:center;backdrop-filter:blur(10px)}
.modal-overlay.open{display:flex}
.modal{background:var(--card);border:1px solid var(--border-strong);border-radius:var(--radius);width:90vw;max-width:700px;max-height:80vh;overflow:hidden;display:flex;flex-direction:column;animation:modalIn .15s ease;box-shadow:0 24px 80px rgba(0,0,0,.6)}
@keyframes modalIn{from{opacity:0;transform:scale(.95)}to{opacity:1;transform:scale(1)}}
.modal-header{padding:16px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.modal-header h3{font-size:16px;font-weight:600}
.modal-close{background:none;border:none;color:var(--muted);font-size:20px;cursor:pointer;padding:4px}
.modal-close:hover{color:var(--text)}
.modal-body{padding:20px;overflow-y:auto;flex:1}
.modal-body pre{background:#070709;border:1px solid var(--border);padding:13px 14px;border-radius:var(--radius-sm);overflow:auto;max-height:300px;font-size:12px;line-height:1.55;white-space:pre-wrap;word-break:break-word;color:#cfcac2;font-family:var(--font-mono);margin-top:6px}
.modal-body pre::-webkit-scrollbar{width:8px;height:8px}
.modal-body pre::-webkit-scrollbar-thumb{background:rgba(168,85,247,.22);border-radius:999px}
.modal-body strong{font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}
.modal-body code{font-family:var(--font-mono);font-size:12px;color:#d8b4fe;background:rgba(168,85,247,.10);padding:1px 6px;border-radius:var(--radius-xs)}
.raw-dump-group{margin-top:6px;border-top:1px solid var(--border);padding-top:12px}
.raw-dump-title{font-size:10px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--faint);margin-bottom:8px}
.raw-section{border:1px solid var(--border);border-radius:var(--radius);background:var(--surface);margin-bottom:6px;overflow:hidden}
.raw-section>summary{list-style:none;cursor:pointer;padding:9px 12px;font-size:12px;font-weight:600;color:var(--text);display:flex;align-items:center;gap:8px;transition:background .15s}
.raw-section>summary::-webkit-details-marker{display:none}
.raw-section>summary::before{content:'\25B8';color:var(--accent);font-size:10px;transition:transform .18s}
.raw-section[open]>summary::before{transform:rotate(90deg)}
.raw-section>summary:hover{background:rgba(168,85,247,.07)}
.raw-section[open]>summary{border-bottom:1px solid var(--border)}
.raw-section pre{margin:0;border:none;border-radius:0;max-height:340px;background:rgba(8,7,12,.5)}
.diag-list{padding:4px 12px 10px}
.diag-row{font-size:11px;color:var(--muted);line-height:1.5;padding:6px 0;border-bottom:1px solid rgba(168,85,247,.06)}
.diag-row:last-child{border-bottom:none}
.dialog-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.62);z-index:260;justify-content:center;align-items:center;backdrop-filter:blur(8px)}
.dialog-overlay.open{display:flex}
.dialog-card{width:min(560px,92vw);background:var(--card);border:1px solid var(--border-strong);border-radius:var(--radius);box-shadow:0 24px 80px rgba(0,0,0,.55);overflow:hidden}
.dialog-head{padding:18px 20px 10px}
.dialog-title{font-size:18px;font-weight:700;color:var(--text)}
.dialog-subtitle{margin-top:8px;font-size:13px;line-height:1.6;color:var(--muted)}
.dialog-body{padding:0 20px 20px}
.dialog-input,.dialog-textarea{width:100%;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);padding:12px 14px;font-size:13px;font-family:var(--font);outline:none;transition:border-color .15s}
.dialog-input:focus,.dialog-textarea:focus{border-color:var(--accent)}
.dialog-textarea{min-height:132px;resize:vertical;line-height:1.55}
.dialog-actions{display:flex;justify-content:flex-end;gap:10px;padding:0 20px 20px}
.dialog-btn{appearance:none;display:inline-flex;align-items:center;justify-content:center;min-height:38px;padding:9px 16px;border-radius:var(--radius-sm);font-size:12px;font-weight:600;cursor:pointer;transition:border-color .15s,background .15s;font-family:var(--font)}
.dialog-btn-secondary{background:var(--surface);border:1px solid var(--border);color:var(--text)}
.dialog-btn-secondary:hover{border-color:var(--border-strong);background:var(--surface-2)}
.dialog-btn-primary{background:var(--accent-strong);border:1px solid var(--accent-strong);color:#f5f0ff}
.dialog-btn-primary:hover{background:var(--accent);border-color:var(--accent)}

/* Toast */
.toast-container{position:fixed;top:60px;right:20px;z-index:300;display:flex;flex-direction:column;gap:6px;max-width:300px;pointer-events:none}
.toast{display:flex;align-items:flex-start;gap:8px;background:rgba(16,16,20,.92);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid var(--border-strong);border-radius:var(--radius-sm);padding:7px 11px;font-size:11px;line-height:1.35;color:var(--muted);box-shadow:0 10px 30px rgba(0,0,0,.45);max-width:300px;pointer-events:auto;cursor:default;animation:toastIn .2s ease}
.toast.out{animation:toastOut .25s ease forwards}
.toast .toast-msg{flex:1 1 auto;min-width:0;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.toast .toast-count{flex:0 0 auto;margin-top:1px;font-family:var(--font-mono);font-size:9px;font-weight:700;color:var(--accent);background:rgba(168,85,247,.16);border-radius:999px;padding:1px 6px}
.toast .toast-kind{flex:0 0 auto;align-self:flex-start;margin-top:1px;font-family:var(--font-mono);font-size:8px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;padding:2px 5px;border-radius:var(--radius-xs);background:var(--surface);color:var(--faint)}
.toast .toast-kind.ask,.toast .toast-kind.task{background:rgba(168,85,247,.16);color:var(--accent)}
.toast .toast-kind.tool{background:rgba(91,159,224,.16);color:var(--blue)}
.toast .toast-kind.execution{background:rgba(214,162,60,.16);color:var(--yellow)}
.toast .toast-kind.session{background:rgba(78,166,116,.16);color:var(--green)}
.toast .toast-kind.tension,.toast .toast-kind.control{background:rgba(224,106,106,.16);color:var(--red)}
@keyframes toastIn{from{opacity:0;transform:translateX(40px)}to{opacity:1;transform:translateX(0)}}
@keyframes toastOut{from{opacity:1}to{opacity:0;transform:translateX(40px)}}

/* Scrollable */
.scroll{max-height:320px;overflow-y:auto}
.scroll::-webkit-scrollbar{width:4px}
.scroll::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}
.scroll.scroll-tall{max-height:none}

/* Empty */
.empty{text-align:center;color:var(--muted);font-size:11px;padding:16px}
  /* Action Required */
  .action-cards { display:flex; flex-direction:column; gap:10px; }
  .action-card { background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:14px; cursor:pointer; transition: all .15s; }
  .action-card:hover { border-color:var(--accent); transform:translateY(-1px); }
  .action-card.resolved { opacity:.55; }
  .action-title { font-weight:600; font-size:13px; margin-bottom:4px; }
  .action-meta { font-size:10px; color:var(--muted); display:flex; gap:8px; flex-wrap:wrap; align-items:center; }
  .action-status { display:inline-block; padding:2px 8px; border-radius:6px; font-size:9px; font-weight:700; text-transform:uppercase; }
  .action-status.OPEN { background:rgba(214,162,60,.14); color:var(--yellow); }
  .action-status.RESOLVED { background:rgba(78,166,116,.14); color:var(--green); }
  .action-status.CANCELLED { background:rgba(224,106,106,.14); color:var(--red); }
  .action-status.ACTIVE { background:rgba(91,159,224,.14); color:var(--blue); }
  .action-status.ARCHIVED { background:rgba(139,138,135,.14); color:var(--faint); }
  .action-status.PENDING { background:rgba(214,162,60,.14); color:var(--yellow); }
  .action-status.CONFIRMED { background:rgba(78,166,116,.14); color:var(--green); }
  .action-status.DISCARDED { background:rgba(224,106,106,.14); color:var(--red); }
  .action-status.COMPLETED { background:rgba(78,166,116,.14); color:var(--green); }
  .action-status.FAILED { background:rgba(224,106,106,.14); color:var(--red); }
  .action-detail-grid { display:grid; grid-template-columns:1fr 1fr; gap:10px; font-size:12px; margin-bottom:14px; }
  .action-detail-grid strong { display:block; font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; margin-bottom:2px; }
  .action-chat { border-top:1px solid var(--border); margin-top:14px; padding-top:10px; }
  .action-chat-messages { max-height:200px; overflow-y:auto; margin-bottom:8px; }
  .action-chat-msg { padding:6px 10px; margin-bottom:4px; border-radius:8px; font-size:12px; }
  .action-chat-msg.from-human { background:var(--accent); color:#fff; margin-left:30%; text-align:right; }
  .action-chat-msg.from-agent { background:var(--surface); border:1px solid var(--border); margin-right:30%; }
  .action-chat-msg .chat-meta { font-size:9px; color:var(--muted); margin-top:2px; }
  .action-chat-msg.from-human .chat-meta { color:rgba(255,255,255,.6); }
  .state-table { width:100%;border-collapse:collapse;font-size:11px;margin-top:6px; }
  .state-table td { padding:4px 8px;border-bottom:1px solid var(--border);vertical-align:top; }
  .state-table td:first-child { font-family:monospace;color:var(--accent);white-space:nowrap;width:30%; }
  .state-table td:last-child { color:var(--text);word-break:break-all; }
  .action-chat-input { display:flex; gap:6px; }
  .action-chat-input input { flex:1; background:var(--surface); border:1px solid var(--border); border-radius:6px; padding:6px 10px; color:var(--text); font-size:12px; }
  .action-chat-input button { background:var(--accent); color:#fff; border:none; border-radius:6px; padding:6px 14px; font-size:12px; cursor:pointer; }
  .action-btn-row { display:flex; gap:8px; margin-top:12px; flex-wrap:wrap; }
  .action-btn-row button { flex:1 1 140px; min-height:40px; padding:10px 16px; border:1px solid transparent; border-radius:var(--radius-sm); font-weight:600; font-size:13px; line-height:1.2; cursor:pointer; transition:opacity .15s; }
  .action-btn-row button:hover { opacity:.85; }
  .session-action-panel { background:var(--surface); border:1px solid var(--border); border-radius:var(--radius); padding:12px; margin-top:14px; }
  .session-action-panel label { display:block; font-size:10px; font-weight:600; color:var(--muted); text-transform:uppercase; letter-spacing:.5px; margin-bottom:4px; }
  .session-action-panel textarea, .session-action-panel input { width:100%; background:var(--card); border:1px solid var(--border); border-radius:8px; color:var(--text); padding:8px 10px; font-size:12px; font-family:var(--font); outline:none; }
  .session-action-panel textarea:focus, .session-action-panel input:focus { border-color:var(--accent); }
  .session-action-grid { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-top:10px; }
  .session-action-status { font-size:11px; color:var(--muted); min-height:16px; margin-top:10px; }
  .session-action-status.ok { color:var(--green); }
  .session-action-status.err { color:var(--red); }
  .btn-session-primary,.btn-session-warn,.btn-session-muted,.btn-session-danger,.btn-complete,.btn-fail{
    appearance:none;display:inline-flex;align-items:center;justify-content:center;gap:8px;
    min-height:34px;padding:8px 14px;border-radius:10px;font-size:12px;font-weight:600;line-height:1.2;
    cursor:pointer;transition:transform .18s,border-color .18s,background .18s,filter .18s,box-shadow .18s;
    font-family:var(--font);white-space:nowrap;text-decoration:none
  }
  .btn-session-primary:hover,.btn-session-warn:hover,.btn-session-muted:hover,.btn-session-danger:hover,.btn-complete:hover,.btn-fail:hover{transform:translateY(-1px);filter:brightness(1.03)}
  .btn-session-primary:disabled,.btn-session-warn:disabled,.btn-session-muted:disabled,.btn-session-danger:disabled,.btn-complete:disabled,.btn-fail:disabled{opacity:.5;cursor:not-allowed;transform:none;filter:none}
  .btn-session-primary { background:var(--accent-strong); color:#f5f0ff; border:1px solid var(--accent-strong); }
  .btn-session-primary:hover { background:var(--accent); }
  .btn-session-warn { background:rgba(214,162,60,.16); color:var(--yellow); border:1px solid rgba(214,162,60,.32); }
  .btn-session-muted { background:var(--surface); color:var(--text); border:1px solid var(--border); }
  .btn-session-danger { background:rgba(224,106,106,.15); color:var(--red); border:1px solid rgba(224,106,106,.32); }
  .btn-complete { background:rgba(78,166,116,.16); color:var(--green); border:1px solid rgba(78,166,116,.32); }
  .btn-fail { background:rgba(224,106,106,.15); color:var(--red); border:1px solid rgba(224,106,106,.32); }
  .resolve-overlay { position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.7);display:flex;align-items:center;justify-content:center;z-index:999;display:none; }
  .resolve-overlay.open { display:flex; }
  .resolve-box { background:var(--card);border:1px solid var(--border-strong);border-radius:var(--radius);padding:24px;max-width:400px;width:90%;box-shadow:0 16px 48px rgba(0,0,0,.4); }
  .resolve-box h3 { margin:0 0 10px; font-size:15px; }
  .resolve-box textarea { margin-bottom:12px; }
  .auth-shell{min-height:100vh;padding:28px 20px;background:#050505;display:flex;align-items:center;justify-content:center}
  .auth-panel{width:min(1240px,100%);display:grid;grid-template-columns:1.1fr .9fr;gap:18px}
  .auth-hero,.auth-card,.security-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);box-shadow:0 8px 28px rgba(0,0,0,.3)}
  .auth-hero{padding:28px;display:flex;flex-direction:column;justify-content:space-between;min-height:420px}
  .auth-kicker{font-size:11px;letter-spacing:.18em;text-transform:uppercase;color:var(--accent);font-weight:700}
  .auth-hero h1{font-size:38px;line-height:1.03;margin:10px 0 14px;color:var(--text);max-width:11ch}
  .auth-hero p{max-width:56ch;color:var(--muted);font-size:14px;line-height:1.7}
  .auth-points{display:flex;flex-wrap:wrap;gap:8px;margin-top:18px}
  .auth-point{padding:6px 10px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border);color:var(--text);font-size:11px}
  .auth-grid{display:grid;grid-template-columns:1fr;gap:16px}
  .auth-card{padding:22px}
  .auth-card-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px}
  .auth-card-head h2{font-size:13px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)}
  .auth-switch{display:flex;gap:8px;margin-bottom:14px}
  .auth-switch button,.auth-action-btn,.workspace-action-btn{border:none;border-radius:var(--radius-sm);padding:10px 14px;font-weight:600;cursor:pointer;font-family:var(--font);transition:background .15s,border-color .15s,color .15s}
  .auth-switch button{background:var(--surface);color:var(--muted);border:1px solid var(--border)}
  .auth-switch button.active{background:var(--accent-strong);color:#f5f0ff;border-color:var(--accent-strong)}
  .auth-form{display:none}
  .auth-form.active{display:block}
  .auth-field-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
  .auth-field-grid.one{grid-template-columns:1fr}
  .auth-field{margin-bottom:10px}
  .auth-field label,.security-form label{display:block;font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px}
  .auth-field input,.security-form input,.security-form textarea,.security-form select{width:100%;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);color:var(--text);padding:10px 12px;font-size:13px;font-family:var(--font);outline:none}
  .auth-field input:focus,.security-form input:focus,.security-form textarea:focus,.security-form select:focus{border-color:var(--accent)}
  .auth-note{font-size:11px;color:var(--muted);line-height:1.5;margin-top:8px}
  .auth-secret{margin-top:14px;padding:14px;border-radius:var(--radius-sm);background:rgba(78,166,116,.08);border:1px solid rgba(78,166,116,.22);display:none}
  .auth-secret.active{display:block}
  .auth-secret code,.security-code{display:inline-block;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-xs);padding:4px 8px;font-size:12px}
  .auth-secret .row{display:flex;justify-content:space-between;gap:8px;align-items:center;flex-wrap:wrap}
  .auth-secret .value{font-family:monospace;font-size:16px;word-break:break-all}
  .auth-shell .msg-status{min-height:18px}
  .security-card{padding:18px}
  .security-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  .security-form{display:grid;gap:10px}
  .security-form .security-row{display:grid;grid-template-columns:1fr 1fr;gap:10px}
  .security-log-list{display:flex;flex-direction:column;gap:8px}
  .security-log{padding:10px 12px;border-radius:var(--radius-sm);background:var(--surface);border:1px solid var(--border);font-size:12px;line-height:1.5}
  .security-log-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}
  .security-log-meta{font-size:10px;color:var(--muted);margin-top:4px}
  .security-log-badge{display:inline-flex;align-items:center;padding:2px 7px;border-radius:999px;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.08em}
  .security-log-badge.ok{background:rgba(78,166,116,.14);color:var(--green)}
  .security-log-badge.fail{background:rgba(224,106,106,.14);color:var(--red)}
  .shell-hidden{display:none}
  @media(max-width:1100px){.auth-panel,.security-grid{grid-template-columns:1fr}}
  @media(max-width:760px){.auth-hero h1{font-size:30px}.auth-field-grid,.security-form .security-row{grid-template-columns:1fr}}
  </style>
</head>
<body class="auth-locked">

<div class="auth-shell" id="auth-shell">
  <div class="auth-panel">
    <section class="auth-hero">
      <div>
        <div class="auth-kicker">Rhizome trusted workspace access</div>
        <h1>Auth, tokens, and workspace security in one place.</h1>
        <p>People sign in with a workspace name, then a personal username and password. Registration also sets a unique display name inside the workspace. Agents are registered once and receive a unique token that should stay on the host only.</p>
        <div class="auth-points">
          <span class="auth-point">Operator-configured workspace password</span>
          <span class="auth-point">One token per agent</span>
          <span class="auth-point">Workspace security logs included</span>
        </div>
      </div>
      <div class="auth-note">This screen provisions per-human and per-agent tokens through the public auth endpoints and keeps the dashboard session locally.</div>
    </section>

    <div class="auth-grid">
      <section class="auth-card">
        <div class="auth-card-head">
          <h2>Human access</h2>
          <span class="badge">Login / Register</span>
        </div>
        <div class="auth-switch">
          <button type="button" id="human-login-tab" class="active" onclick="switchAuthMode('human-login')">Login</button>
          <button type="button" id="human-register-tab" onclick="switchAuthMode('human-register')">Register</button>
        </div>
        <div class="auth-form active" id="human-login-form">
          <div class="auth-field-grid">
            <div class="auth-field"><label for="human-login-workspace">Workspace</label><input id="human-login-workspace" placeholder="rhizome-main"></div>
            <div class="auth-field"><label for="human-login-name">Username</label><input id="human-login-name" placeholder="your-login"></div>
          </div>
          <div class="auth-field"><label for="human-login-password">Personal password</label><input id="human-login-password" type="password" placeholder="your personal password"></div>
          <button type="button" class="auth-action-btn" style="background:var(--accent-strong);color:#f5f0ff" onclick="submitHumanLogin()">Sign in</button>
        </div>
        <div class="auth-form" id="human-register-form">
          <div class="auth-field-grid">
            <div class="auth-field"><label for="human-register-workspace">Workspace</label><input id="human-register-workspace" placeholder="rhizome-main"></div>
            <div class="auth-field"><label for="human-register-password">Workspace password</label><input id="human-register-password" type="password" autocomplete="new-password"></div>
          </div>
          <div class="auth-field-grid">
            <div class="auth-field"><label for="human-register-name">Username</label><input id="human-register-name" placeholder="your-login"></div>
            <div class="auth-field"><label for="human-register-display-name">Display name</label><input id="human-register-display-name" placeholder="your visible name"></div>
          </div>
          <div class="auth-field-grid">
            <div class="auth-field"><label for="human-register-personal-password">Personal password</label><input id="human-register-personal-password" type="password" placeholder="choose a password"></div>
          </div>
          <button type="button" class="auth-action-btn" style="background:var(--accent-strong);color:#f5f0ff" onclick="submitHumanRegister()">Create human account</button>
        </div>
        <div id="human-auth-status" class="msg-status"></div>
      </section>

      <section class="auth-card">
        <div class="auth-card-head">
          <h2>Agent provisioning</h2>
          <span class="badge">Token once</span>
        </div>
        <div class="auth-field-grid">
          <div class="auth-field"><label for="agent-register-workspace">Workspace</label><input id="agent-register-workspace" placeholder="rhizome-main"></div>
          <div class="auth-field"><label for="agent-register-password">Workspace password</label><input id="agent-register-password" type="password" autocomplete="new-password"></div>
        </div>
        <div class="auth-field-grid">
          <div class="auth-field"><label for="agent-register-name">Agent name</label><input id="agent-register-name" placeholder="agent-amber"></div>
          <div class="auth-field"><label for="agent-register-host">Host URL</label><input id="agent-register-host" placeholder="https://agent.example.internal"></div>
        </div>
        <div class="auth-field"><label for="agent-register-notes">Notes</label><input id="agent-register-notes" placeholder="optional host notes"></div>
        <button type="button" class="auth-action-btn" style="background:var(--accent-strong);color:#f5f0ff" onclick="submitAgentRegister()">Issue agent token</button>
        <div id="agent-token-box" class="auth-secret">
          <div class="row">
            <div>
              <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px">Generated token</div>
              <div id="agent-token-value" class="value"></div>
            </div>
            <button type="button" class="auth-action-btn" style="background:var(--surface);color:var(--text);border:1px solid var(--border)" onclick="copyAgentToken()">Copy</button>
          </div>
          <div class="auth-note" id="agent-token-help"></div>
        </div>
        <div id="agent-auth-status" class="msg-status"></div>
      </section>
    </div>
  </div>
</div>

<div id="dashboard-shell" class="shell-hidden">

<div class="header">
  <h1 onclick="switchTab('overview')">Rhizome</h1>
  <div class="tabs" style="margin:0">
    <div class="tab active" data-tab="overview" onclick="switchTab('overview')">Overview</div>
    <div class="tab" data-tab="graph" onclick="switchTab('graph')">Graph</div>
    <div class="tab" data-tab="tasks" onclick="switchTab('tasks')">Tasks</div>
    <div class="tab" data-tab="projects" onclick="switchTab('projects')">Projects</div>
    <div class="tab" data-tab="actions" onclick="switchTab('actions')">Actions <span class="tab-badge warn" id="actions-badge" style="display:none">0</span></div>
    <div class="tab" data-tab="control" onclick="switchTab('control')">Control <span class="tab-badge alert" id="control-badge" style="display:none">0</span></div>
    <div class="tab" data-tab="instrumentation" onclick="switchTab('instrumentation')">Instrumentation</div>
    <div class="tab" data-tab="tensions" onclick="switchTab('tensions')">Tensions <span class="tab-badge warn" id="tensions-badge" style="display:none">0</span></div>
    <div class="tab" data-tab="tools" onclick="switchTab('tools')">Tools</div>
    <div class="tab" data-tab="limits" onclick="switchTab('limits')">Limits</div>
    <div class="tab" data-tab="security" onclick="switchTab('security')">Security</div>
    <div class="tab" data-tab="news" onclick="switchTab('news')">News</div>
    <div class="tab" data-tab="vault" onclick="switchTab('vault')">Vault</div>
    <div class="tab" data-tab="logs" onclick="switchTab('logs')">Logs</div>
    <div class="tab" data-tab="messages" onclick="switchTab('messages')">Interactions</div>
  </div>
  <div class="profile-wrap" id="profile-wrap">
    <button class="hdr-btn profile-btn" id="profile-btn" type="button" onclick="toggleProfileMenu(event)">
      <span class="profile-label" id="profile-label">Profile</span>
      <span class="profile-caret">▾</span>
    </button>
    <div class="profile-menu" id="profile-menu" role="menu" aria-label="Profile menu">
      <button type="button" onclick="openProfileSettings()">Profile settings</button>
      <button type="button" class="danger" onclick="logout()">Logout</button>
    </div>
  </div>
</div>

<div class="toast-container" id="toasts"></div>

<!-- Modal -->
<div class="modal-overlay" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="modal-header">
      <h3 id="modal-title">Document</h3>
      <button class="modal-close" onclick="closeModal()">✕</button>
    </div>
    <div class="modal-body" id="modal-body"></div>
  </div>
</div>

<div class="dialog-overlay" id="dialog-modal" onclick="if(event.target===this)cancelDashboardDialog()">
  <div class="dialog-card">
    <div class="dialog-head">
      <div class="dialog-title" id="dialog-title">Input Required</div>
      <div class="dialog-subtitle" id="dialog-subtitle"></div>
    </div>
    <div class="dialog-body">
      <input id="dialog-input" class="dialog-input" type="text" />
      <textarea id="dialog-textarea" class="dialog-textarea" style="display:none"></textarea>
    </div>
    <div class="dialog-actions">
      <button id="dialog-cancel" class="dialog-btn dialog-btn-secondary" type="button" onclick="cancelDashboardDialog()">Cancel</button>
      <button id="dialog-confirm" class="dialog-btn dialog-btn-primary" type="button" onclick="confirmDashboardDialog()">OK</button>
    </div>
  </div>
</div>

<div class="container">
  <!-- Stats -->
  <div class="stats">
    <div class="stat"><div class="value" id="s-agents">-</div><div class="label">Agents</div></div>
    <div class="stat"><div class="value" id="s-online">-</div><div class="label">Online</div></div>
    <div class="stat"><div class="value" id="s-sessions">-</div><div class="label">Sessions</div></div>
    <div class="stat"><div class="value" id="s-attention">-</div><div class="label">Attention</div></div>
    <div class="stat"><div class="value" id="s-projects">-</div><div class="label">Projects</div></div>
    <div class="stat"><div class="value" id="s-tasks">-</div><div class="label">Tasks</div></div>
    <div class="stat"><div class="value" id="s-docs">-</div><div class="label">Docs</div></div>
    <div class="stat"><div class="value" id="s-msgs">-</div><div class="label">Interactions</div></div>
    <div class="stat"><div class="value" id="s-rpc-methods">-</div><div class="label">RPC Methods</div></div>
  </div>

  <!-- Tab: Graph -->
  <div class="tab-panel" id="panel-graph">
    <div class="card graph-shell" style="display:flex;height:calc(100vh - 180px);margin-bottom:14px;position:relative;overflow:hidden">
      <div id="graph-replay-bar" style="display:none;position:absolute;left:0;right:0;bottom:0;z-index:4;pointer-events:none">
        <div id="graph-replay-chip" style="position:absolute;right:10px;bottom:10px;font-size:10px;letter-spacing:.08em;color:var(--accent);background:rgba(12,12,16,.6);backdrop-filter:blur(8px);padding:3px 8px;border-radius:6px;border:1px solid rgba(168,85,247,.25)">replay</div>
        <div style="height:3px;background:rgba(168,85,247,.12)"><div id="graph-replay-fill" style="height:100%;width:0%;background:var(--accent)"></div></div>
      </div>
      <!-- Toolbar -->
      <div id="graph-toolbar" class="graph-toolbar-overlay" style="flex:0 0 auto;width:200px;border-right:1px solid var(--border);padding:14px;background:var(--surface);display:flex;flex-direction:column;gap:12px;z-index:2">
        <div class="graph-toolbar-head">
          <div>
            <div class="graph-title-kicker">Workspace Graph</div>
          </div>
          <button id="graph-controls-toggle" class="graph-controls-toggle" type="button" aria-label="Collapse graph panel" title="Collapse graph panel" onclick="toggleGraphControlsPanel()">
            <span id="graph-controls-toggle-icon" class="graph-controls-chevron"><svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"></polyline></svg></span>
          </button>
        </div>
        <div id="graph-controls-body" class="graph-controls-body">
        <div class="graph-section">
          <label class="graph-panel-label">Mode</label>
          <select id="graph-mode-select" class="graph-form-select" onchange="handleGraphModeChange()">
            <option value="SYSTEM">System V1 (Core)</option>
            <option value="TASK_FOCUS">Task Focus</option>
            <option value="CONTROL">Control View</option>
            <option value="MEMORY_OVERLAY">Memory Overlay</option>
            <option value="MEMORY_ATLAS">Memory Atlas</option>
          </select>
        </div>
        <div id="graph-task-focus-picker-wrap" class="graph-section" style="display:none">
          <label class="graph-panel-label">Focus Task</label>
          <select id="graph-task-focus-select" class="graph-form-select" onchange="handleGraphTaskFocusSelection()"></select>
          <div class="graph-inline-hint">Choose a task explicitly instead of hunting for the right node in the full graph.</div>
        </div>
        <div id="graph-control-focus-picker-wrap" class="graph-section" style="display:none">
          <label class="graph-panel-label">Focus Cluster</label>
          <select id="graph-control-focus-select" class="graph-form-select" onchange="handleGraphControlFocusSelection()"></select>
          <div class="graph-inline-hint">Pick a proto-cluster directly instead of trying to discover the right control neighborhood by eye.</div>
        </div>
        <div id="graph-memory-atlas-controls-wrap" class="graph-section" style="display:none">
          <label class="graph-panel-label">Memory Atlas</label>
          <input id="graph-memory-atlas-query" class="graph-form-select" type="text" placeholder="Search memory by title, summary, body..." onkeydown="if(event.key==='Enter'){handleGraphMemoryAtlasSearch()}" />
          <div class="graph-action-grid" style="margin-top:8px">
            <button class="graph-panel-btn graph-panel-btn-primary wide" onclick="handleGraphMemoryAtlasSearch()">Search Atlas</button>
          </div>
          <div id="graph-memory-atlas-lens-bar" class="graph-lens-bar">
            <button class="graph-lens-chip active" type="button" data-atlas-lens="all" onclick="graphApplyMemoryAtlasLens('all')">Everything</button>
            <button class="graph-lens-chip" type="button" data-atlas-lens="active" onclick="graphApplyMemoryAtlasLens('active')">Active</button>
            <button class="graph-lens-chip" type="button" data-atlas-lens="dormant" onclick="graphApplyMemoryAtlasLens('dormant')">Dormant</button>
            <button class="graph-lens-chip" type="button" data-atlas-lens="procedural" onclick="graphApplyMemoryAtlasLens('procedural')">Procedural</button>
            <button class="graph-lens-chip" type="button" data-atlas-lens="identity" onclick="graphApplyMemoryAtlasLens('identity')">Identity</button>
            <button class="graph-lens-chip" type="button" data-atlas-lens="disputed" onclick="graphApplyMemoryAtlasLens('disputed')">Disputed</button>
            <button class="graph-lens-chip" type="button" data-atlas-lens="canonical" onclick="graphApplyMemoryAtlasLens('canonical')">Canonical</button>
            <button class="graph-lens-chip" type="button" data-atlas-lens="derived" onclick="graphApplyMemoryAtlasLens('derived')">Derived</button>
          </div>
          <div class="graph-range-group" style="margin-top:10px">
            <div class="graph-range-head"><span>Layer</span><span id="graph-memory-atlas-layer-label">All</span></div>
            <select id="graph-memory-atlas-layer" class="graph-form-select" onchange="handleGraphMemoryAtlasFilterChange()">
              <option value="">All layers</option>
              <option value="EPISODIC">Episodic</option>
              <option value="SEMANTIC">Semantic</option>
              <option value="PROCEDURAL">Procedural</option>
              <option value="IDENTITY">Identity</option>
              <option value="ARCHIVE">Archive</option>
            </select>
          </div>
          <div class="graph-range-group" style="margin-top:10px">
            <div class="graph-range-head"><span>Lifecycle</span><span id="graph-memory-atlas-lifecycle-label">All</span></div>
            <select id="graph-memory-atlas-lifecycle" class="graph-form-select" onchange="handleGraphMemoryAtlasFilterChange()">
              <option value="">All states</option>
              <option value="ACTIVE">Active</option>
              <option value="DORMANT">Dormant</option>
              <option value="SUPERSEDED">Superseded</option>
              <option value="ARCHIVED">Archived</option>
            </select>
          </div>
          <div class="graph-range-group" style="margin-top:10px">
            <div class="graph-range-head"><span>Source</span><span id="graph-memory-atlas-origin-label">Mixed</span></div>
            <select id="graph-memory-atlas-origin" class="graph-form-select" onchange="handleGraphMemoryAtlasFilterChange()">
              <option value="">Mixed sources</option>
              <option value="workspace_memory">Canonical memory</option>
              <option value="knowledge_claim">Knowledge claims</option>
              <option value="episode_pack">Episode packs</option>
            </select>
          </div>
          <div class="graph-range-group" style="margin-top:10px">
            <div class="graph-range-head"><span>Epistemic</span><span id="graph-memory-atlas-epistemic-label">All</span></div>
            <select id="graph-memory-atlas-epistemic" class="graph-form-select" onchange="handleGraphMemoryAtlasFilterChange()">
              <option value="">All epistemic states</option>
              <option value="ALLEGED">Alleged</option>
              <option value="SUPPORTED">Supported</option>
              <option value="VERIFIED">Verified</option>
              <option value="DISPUTED">Disputed</option>
              <option value="RETRACTED">Retracted</option>
            </select>
          </div>
          <div class="graph-range-group" style="margin-top:10px">
            <div class="graph-range-head"><span>Neighborhood</span><span id="graph-memory-atlas-depth-label">1-hop</span></div>
            <select id="graph-memory-atlas-depth" class="graph-form-select" onchange="handleGraphMemoryAtlasFilterChange()">
              <option value="1">1-hop</option>
              <option value="2">2-hop</option>
            </select>
          </div>
          <label class="graph-layer-toggle" style="margin-top:10px">
            <input id="graph-memory-atlas-canonical" type="checkbox" onchange="handleGraphMemoryAtlasFilterChange()" />
            <span>
              <strong>Canonical Only</strong><br>
              <span class="graph-inline-hint" style="margin-top:4px;display:block">Keep the atlas anchored on canonical workspace memory. Turn this off to let derived knowledge claims and episode packs into the map.</span>
            </span>
          </label>
          <label class="graph-layer-toggle" style="margin-top:10px">
            <input id="graph-memory-atlas-archived" type="checkbox" onchange="handleGraphMemoryAtlasFilterChange()" />
            <span>
              <strong>Include Archived</strong><br>
              <span class="graph-inline-hint" style="margin-top:4px;display:block">Bring dormant archive material back into the visible atlas when you want historical neighborhoods, not only live memory.</span>
            </span>
          </label>
          <label class="graph-layer-toggle" style="margin-top:10px">
            <input id="graph-memory-atlas-anchors" type="checkbox" onchange="handleGraphMemoryAtlasFilterChange()" />
            <span>
              <strong>Show Anchors</strong><br>
              <span class="graph-inline-hint" style="margin-top:4px;display:block">Reveal faint task, session, and agent context for the visible memory cluster.</span>
            </span>
          </label>
          <div class="graph-atlas-legend">
            <div class="graph-panel-label" style="margin-bottom:0">Atlas Grammar</div>
            <div class="graph-inline-hint" style="margin-top:6px">In atlas mode, size tracks importance, glow tracks activation, and rings call out special memory state.</div>
            <div class="graph-atlas-legend-grid">
              <div class="graph-atlas-legend-item"><span class="graph-atlas-legend-dot" style="background:#79c0ff"></span><span>Semantic memory</span></div>
              <div class="graph-atlas-legend-item"><span class="graph-atlas-legend-dot" style="background:#5eead4"></span><span>Procedural memory</span></div>
              <div class="graph-atlas-legend-item"><span class="graph-atlas-legend-dot" style="background:#f2cc60"></span><span>Identity memory</span></div>
              <div class="graph-atlas-legend-item"><span class="graph-atlas-legend-dot" style="background:#d2a8ff"></span><span>Episodic memory</span></div>
              <div class="graph-atlas-legend-item"><span class="graph-atlas-legend-ring" style="border-color:#ffb86b"></span><span>High drift / fragile</span></div>
              <div class="graph-atlas-legend-item"><span class="graph-atlas-legend-ring" style="border-color:#7dd3fc"></span><span>Protected / guarded</span></div>
              <div class="graph-atlas-legend-item"><span class="graph-atlas-legend-ring" style="border-color:#fb7185"></span><span>Recovery candidate</span></div>
              <div class="graph-atlas-legend-item"><span class="graph-atlas-legend-ring" style="border-color:#f87171"></span><span>Unresolved or disputed</span></div>
            </div>
          </div>
          <div class="graph-inline-hint" style="margin-top:10px">Atlas is memory-first: search or center one node, then expand its neighborhood without letting runtime entities dominate the scene.</div>
        </div>
        <div class="graph-section">
          <label class="graph-panel-label">Actions</label>
          <div class="graph-action-grid">
            <button class="graph-panel-btn graph-panel-btn-primary wide" onclick="if(_graphInstance) { _graphInstance.zoomToFit(400) }">Zoom to Fit</button>
            <button class="graph-panel-btn graph-panel-btn-secondary" onclick="initGraphData()">Force Sync</button>
            <button class="graph-panel-btn graph-panel-btn-secondary" id="graph-replay-btn" onclick="toggleGraphReplay()">Replay 1h</button>
            <button class="graph-panel-btn graph-panel-btn-ghost" id="graph-back-to-system" style="display:none" onclick="graphReturnToSystem()">Back to System</button>
            <button class="graph-panel-btn graph-panel-btn-secondary wide" id="graph-atlas-overview-btn" style="display:none" onclick="graphShowMemoryAtlasOverview()">Atlas Overview</button>
            <button class="graph-panel-btn graph-panel-btn-secondary wide" id="graph-atlas-back-btn" style="display:none" onclick="graphGoBackMemoryAtlasFocus()">Back to Previous Focus</button>
          </div>
          <div id="graph-memory-atlas-nav" class="graph-atlas-nav">
            <div class="graph-atlas-nav-row">
              <div class="graph-inline-hint" style="margin:0">Recent memory pivots</div>
            </div>
            <div id="graph-memory-atlas-history" class="graph-atlas-history"></div>
          </div>
          <div id="graph-focus-meta" class="graph-meta-pill" style="display:none;margin-top:8px;font-size:10px;color:var(--muted);line-height:1.4"></div>
        </div>
        <div id="graph-affinity-controls-wrap" class="graph-section">
          <label class="graph-panel-label">Layers</label>
          <label class="graph-layer-toggle">
            <input id="graph-toggle-affinity" type="checkbox" checked onchange="updateGraphVisibility()" />
            <span>
              <strong>Potential Links</strong><br>
              <span class="graph-inline-hint" style="margin-top:4px;display:block">Shows soft links for agent fit, task surface pull, and cluster pressure.</span>
            </span>
          </label>
          <div class="graph-range-group" style="margin-top:8px">
            <div class="graph-range-head"><span>Potential Threshold</span><span id="graph-affinity-threshold-val">0.35</span></div>
            <input id="graph-affinity-threshold" class="graph-range-input" type="range" min="0.10" max="0.90" step="0.05" value="0.35" oninput="updateGraphVisibility()" />
          </div>
          <div class="graph-legend" style="margin-top:10px">
            <div class="graph-legend-title">Legend</div>
            <div class="graph-legend-item"><span class="graph-legend-line" style="background:rgba(244,208,63,0.6)"></span><span>Agent fit</span></div>
            <div class="graph-legend-item"><span class="graph-legend-line" style="background:rgba(94,234,212,0.6)"></span><span>Task surface</span></div>
            <div class="graph-legend-item"><span class="graph-legend-line" style="background:rgba(255,166,87,0.6)"></span><span>Cluster pressure</span></div>
          </div>
        </div>
        <div id="graph-stats" class="graph-stats-pill" style="font-size:10px;color:var(--muted);margin-top:auto"></div>
        </div>
      </div>
      <!-- Canvas -->
      <div id="graph-container" class="graph-canvas" style="flex:1;background:#07070b;position:relative"></div>
        <div id="graph-display-settings-panel" class="graph-display-settings-overlay">
        <div id="graph-display-settings-section" class="graph-section graph-display-settings">
          <button class="graph-section-toggle" type="button" onclick="toggleGraphDisplaySettings()">
            <span>
              <span class="graph-display-settings-label">Display Settings</span>
            </span>
            <span id="graph-display-settings-toggle-icon" class="graph-settings-gear"><svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"></circle><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"></path></svg></span>
          </button>
          <div id="graph-display-settings-body" class="graph-section-body is-collapsed">
            <div class="graph-range-group">
              <div class="graph-range-head"><span>Repulsion</span><span id="st-repulsion-val">-200</span></div>
              <input type="range" id="st-repulsion" class="graph-range-input" min="-1000" max="-10" value="-200" oninput="updateGraphSettings()">
            </div>
            <div class="graph-range-group">
              <div class="graph-range-head"><span>Link Distance</span><span id="st-linkdist-val">80</span></div>
              <input type="range" id="st-linkdist" class="graph-range-input" min="10" max="300" value="80" oninput="updateGraphSettings()">
            </div>
            <div class="graph-range-group">
              <div class="graph-range-head"><span>Node Size</span><span id="st-nodesize-val">1.0x</span></div>
              <input type="range" id="st-nodesize" class="graph-range-input" min="0.3" max="3.0" step="0.1" value="1.0" oninput="updateGraphSettings()">
            </div>
            <div class="graph-range-group">
              <div class="graph-range-head"><span>Center Gravity</span><span id="st-gravity-val">0.05</span></div>
              <input type="range" id="st-gravity" class="graph-range-input" min="0" max="250" value="50" oninput="updateGraphSettings()">
            </div>
          </div>
        </div>
      </div>
      <!-- Inspector -->
      <div id="graph-inspector" class="graph-inspector-panel" style="flex:0 0 auto;width:280px;border-left:1px solid var(--border);padding:14px;background:var(--surface);display:none;flex-direction:column;gap:12px;z-index:2;overflow-y:auto">
        <div class="graph-inspector-header">
          <h3 class="graph-inspector-title">Inspector</h3>
          <button class="graph-inspector-close" style="background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px" onclick="dismissGraphInspector()">&times;</button>
        </div>
        <div id="graph-inspector-body" class="graph-inspector-body" style="font-size:12px;color:var(--text);line-height:1.5">Select a node to inspect...</div>
        <div id="graph-inspector-actions" class="graph-inspector-actions" style="margin-top:auto;display:flex;flex-direction:column;gap:6px"></div>
      </div>
    </div>
  </div>

  <!-- Tab: Overview -->
  <div class="tab-panel active" id="panel-overview">
    <div class="card" style="margin-bottom:14px">
      <div class="card-header"><h2>Runtime Health</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadRuntimeHealth()">Refresh</button><button class="hdr-btn" onclick="showRuntimeHealthDetail()">Details</button><span class="badge" id="runtime-health-badge">loading</span></div></div>
      <div class="card-body" id="runtime-health-summary"><div class="empty">Loading runtime health...</div></div>
    </div>
    <div class="grid">
      <!-- Agents -->
      <div class="card">
        <div class="card-header"><h2>Agents</h2><span class="badge" id="agents-count">0</span></div>
        <div class="card-body scroll" id="agents-list"><div class="empty">Loading...</div></div>
      </div>
      <!-- Activity Feed -->
      <div class="card">
        <div class="card-header"><h2>Activity Feed</h2><span class="badge" id="feed-count">0</span></div>
        <div class="card-body scroll" id="feed-list"><div class="empty">Loading...</div></div>
      </div>
    </div>

    <div class="grid-3">
      <!-- Docs -->
      <div class="card">
        <div class="card-header"><h2>Documents</h2><span class="badge" id="docs-count">0</span></div>
        <div class="card-body scroll" id="docs-list"><div class="empty">Loading...</div></div>
      </div>
      <!-- Active Sessions -->
      <div class="card">
        <div class="card-header"><h2>Active Sessions</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="startSessionFromDashboard()">Start Session</button><span class="badge" id="sessions-count">0</span></div></div>
        <div class="card-body scroll" id="sessions-list"><div class="empty">Loading...</div></div>
      </div>
      <div class="card">
        <div class="card-header"><h2>Recent Memory</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="toggleMemoryComposer()">Add Memory</button><span class="badge" id="memory-attention-badge" style="display:none;background:var(--orange)">0 attention</span><span class="badge" id="memory-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="memory-composer" id="memory-composer">
            <div id="memory-composer-title" style="font-size:12px;font-weight:600;margin-bottom:10px">New Memory</div>
            <div class="memory-form-grid">
              <div>
                <label for="memory-form-type">Type</label>
                <select id="memory-form-type">
                  <option value="NOTE">NOTE</option>
                  <option value="DECISION">DECISION</option>
                  <option value="LESSON">LESSON</option>
                  <option value="PROCEDURE">PROCEDURE</option>
                  <option value="ANTI_PROCEDURE">ANTI_PROCEDURE</option>
                  <option value="INCIDENT">INCIDENT</option>
                  <option value="ENTITY">ENTITY</option>
                  <option value="EXPERIENCE">EXPERIENCE</option>
                  <option value="UPDATE_DIGEST">UPDATE_DIGEST</option>
                  <option value="SUMMARY">SUMMARY</option>
                  <option value="SELF_MODEL">SELF_MODEL</option>
                  <option value="GOAL_COMMITMENT">GOAL_COMMITMENT</option>
                  <option value="POLICY_TRACE">POLICY_TRACE</option>
                </select>
              </div>
              <div>
                <label for="memory-form-title">Title</label>
                <input id="memory-form-title" placeholder="Short headline">
              </div>
            </div>
            <div style="margin-top:8px">
              <label for="memory-form-summary">Summary</label>
              <input id="memory-form-summary" placeholder="One-line summary">
            </div>
            <div style="margin-top:8px">
              <label for="memory-form-body">Body</label>
              <textarea id="memory-form-body" rows="4" placeholder="What should future sessions remember?"></textarea>
            </div>
            <div style="margin-top:8px">
              <label for="memory-form-tags">Tags</label>
              <input id="memory-form-tags" placeholder="comma,separated,tags">
            </div>
            <div class="action-btn-row">
              <button class="btn-session-primary" id="memory-form-submit" onclick="submitMemoryEntry()">Record Memory</button>
              <button class="btn-session-muted" onclick="cancelMemoryComposer()">Cancel</button>
            </div>
            <div id="memory-form-status" class="session-action-status"></div>
          </div>
          <div class="memory-toolbar">
            <input id="memory-search-query" class="filter-search" placeholder="Search memory..." oninput="scheduleMemoryRefresh()">
            <select id="memory-filter-type" onchange="loadMemory()">
              <option value="">All types</option>
              <option value="DECISION">DECISION</option>
              <option value="LESSON">LESSON</option>
              <option value="INCIDENT">INCIDENT</option>
              <option value="UPDATE_DIGEST">UPDATE_DIGEST</option>
              <option value="NOTE">NOTE</option>
            </select>
            <select id="memory-filter-source" onchange="loadMemory()">
              <option value="">All sources</option>
              <option value="manual">manual</option>
              <option value="compaction">compaction</option>
              <option value="reflection">reflection</option>
              <option value="session_event">session_event</option>
            </select>
            <label class="memory-toolbar-check"><input id="memory-filter-archived" type="checkbox" onchange="loadMemory()"> archived</label>
            <button class="participant-btn" onclick="resetMemoryFilters()">Reset</button>
          </div>
          <div id="memory-filter-context" class="memory-filter-context" style="display:none"></div>
          <div id="memory-list"><div class="empty">Loading...</div></div>
        </div>
      </div>
    </div>
  </div>

  <!-- Tab: Tasks (Kanban) -->
  <div class="tab-panel" id="panel-tasks">
    <div class="card" style="margin-bottom:14px">
      <div class="card-header"><h2>Task Board</h2><div style="display:flex;gap:8px;align-items:center"><button class="btn-accent" onclick="toggleCreateTask()">Create Task</button><span class="badge" id="tasks-count">0</span></div></div>
      <div class="card-body">
        <div class="create-task-form" id="create-task-form">
          <div class="form-grid">
            <div><label>Title *</label><input id="ct-title" placeholder="Task title"></div>
            <div><label>Priority</label><select id="ct-priority"><option value="normal">Normal</option><option value="low">Low</option><option value="high" selected>High</option><option value="critical">Critical</option></select></div>
          </div>
          <div class="form-grid full"><div><label>Description</label><textarea id="ct-desc" placeholder="Describe the task in detail..."></textarea></div></div>
          <div class="form-grid">
            <div><label>Kind</label><select id="ct-kind"><option value="EXECUTION">Execution</option><option value="COORDINATION">Coordination</option></select></div>
            <div><label>Template</label><select id="ct-template"><option value="generic">Generic</option><option value="research">Research</option><option value="bugfix">Bugfix</option><option value="deploy">Deploy</option><option value="integration">Integration</option><option value="ops">Ops</option><option value="tooling">Tooling</option></select></div>
          </div>
          <div class="form-grid">
            <div><label>Task Class (optional)</label><select id="ct-task-class"><option value="">Let corridor derive</option><option value="PROOF">Proof</option><option value="EXPLORATION">Exploration</option><option value="INTEGRATION">Integration</option><option value="INCIDENT">Incident</option></select></div>
            <div><label>Class Source</label><div style="padding:10px 12px;border-radius:8px;border:1px solid var(--border);background:var(--surface);color:var(--muted);font-size:12px">operator-authored if set, otherwise derived read-side only</div></div>
          </div>
          <div><label>Tags (comma-separated)</label><input id="ct-tags" placeholder="e.g. frontend, urgent, refactor"></div>
          <div><label>Project</label><select id="ct-project"><option value="">— No project —</option></select></div>
          <div class="form-actions">
            <button class="btn-accent" onclick="submitNewTask()">Create Task →</button>
            <span id="ct-status" class="msg-status"></span>
          </div>
        </div>
        <div class="filters">
          <button class="filter-btn active" data-priority="all" onclick="filterTasks('all',this)">All</button>
          <button class="filter-btn" data-priority="critical" onclick="filterTasks('critical',this)">Critical</button>
          <button class="filter-btn" data-priority="high" onclick="filterTasks('high',this)">High</button>
          <button class="filter-btn" data-priority="low" onclick="filterTasks('low',this)">Low</button>
          <input class="filter-search" id="task-search" placeholder="Search tasks..." oninput="filterTasksBySearch()">
          <select id="task-project-filter" onchange="filterTasksByProject()" style="padding:6px 11px;border-radius:6px;border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:11px"><option value="">All Projects</option></select>
        </div>
        <div class="board">
          <div class="board-col col-pending"><div class="board-col-header">Pending <span id="cnt-pending"></span></div><div id="col-pending"></div></div>
          <div class="board-col col-claimed"><div class="board-col-header">Claimed <span id="cnt-claimed"></span></div><div id="col-claimed"></div></div>
          <div class="board-col col-blocked"><div class="board-col-header">Blocked <span id="cnt-blocked"></span></div><div id="col-blocked"></div></div>
          <div class="board-col col-completed"><div class="board-col-header">✓ Done <span id="cnt-completed"></span></div><div id="col-completed"></div></div>
        </div>
        <!-- Cancelled tasks -->
        <div class="cancelled-section" id="cancelled-section" style="display:none">
          <button class="cancelled-toggle" onclick="document.getElementById('cancelled-list').classList.toggle('open'); this.querySelector('.arrow').textContent = document.getElementById('cancelled-list').classList.contains('open') ? '▾' : '▸'">
            <span class="arrow">▸</span> Cancelled / Removed <span class="badge" id="cnt-cancelled" style="font-size:9px;padding:1px 6px">0</span>
          </button>
          <div class="cancelled-list" id="cancelled-list"></div>
        </div>
      </div>
    </div>

    <!-- Delete Confirmation Overlay -->
    <div class="confirm-overlay" id="delete-confirm" onclick="if(event.target===this)cancelDelete()">
      <div class="confirm-box">
        <h3>Delete Task</h3>
        <p id="delete-confirm-text">Are you sure you want to delete this task? This action cannot be undone.</p>
        <label style="font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:3px">Reason (optional)</label>
        <textarea id="delete-reason" placeholder="Why is this task being removed?"></textarea>
        <div class="btn-row">
          <button class="btn-cancel" onclick="cancelDelete()">Cancel</button>
          <button class="btn-danger" onclick="confirmDeleteTask()">Delete Task</button>
        </div>
      </div>
    </div>
  </div>

  <!-- Tab: Projects -->
  <div class="tab-panel" id="panel-projects">
    <div class="card" style="margin-bottom:14px">
      <div class="card-header"><h2>Projects</h2><div style="display:flex;gap:8px;align-items:center"><button class="btn-accent" onclick="toggleCreateProject()">New Project</button><span class="badge" id="projects-count">0</span></div></div>
      <div class="card-body">
        <div class="create-task-form" id="create-project-form">
          <div class="form-grid">
            <div><label>Project ID *</label><input id="cp-id" placeholder="e.g. argumate" style="font-family:monospace"></div>
            <div><label>Title *</label><input id="cp-title" placeholder="e.g. Argumate Platform"></div>
          </div>
          <div style="margin-top:8px"><label style="font-size:11px;color:var(--muted);display:block;margin-bottom:4px">Description</label><textarea id="cp-desc" rows="2" placeholder="What is this project about?" style="width:100%;padding:6px 10px;border-radius:6px;border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:12px;font-family:var(--font);resize:vertical"></textarea></div>
          <div class="form-actions" style="margin-top:10px">
            <button class="btn-accent" onclick="submitNewProject()">Create Project →</button>
            <span id="cp-status" class="msg-status" style="font-size:12px"></span>
          </div>
        </div>
        <div style="display:flex;gap:0;margin-bottom:12px;border-bottom:1px solid var(--border)" id="project-status-tabs">
          <button class="project-status-tab active" data-status="ACTIVE" onclick="switchProjectTab('ACTIVE')">Active</button>
          <button class="project-status-tab" data-status="ARCHIVED" onclick="switchProjectTab('ARCHIVED')">Archived</button>
        </div>
        <div id="projects-list"></div>
      </div>
    </div>
  </div>

  <!-- Tab: Messages -->
  <div class="tab-panel" id="panel-messages">
    <div class="card">
      <div class="card-header">
        <h2>Interactions</h2>
        <div style="display:flex;gap:8px;align-items:center;margin-left:auto">
          <select id="ix-kind-filter" onchange="renderInteractions()" style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:4px 8px;color:var(--text);font-size:11px;font-family:var(--font)">
            <option value="">All kinds</option>
            <option value="ask">Model ask (Q&amp;A)</option>
            <option value="tool">Tool calls</option>
            <option value="execution">Execution</option>
            <option value="session">Sessions</option>
            <option value="task">Tasks</option>
          </select>
          <input id="ix-search" oninput="renderInteractions()" placeholder="Filter…" style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:4px 10px;color:var(--text);font-size:11px;font-family:var(--font);width:160px">
          <label class="ix-noise" title="Hide infrastructure noise (bridge polling, lease renewals)"><input type="checkbox" id="ix-hide-noise" checked onchange="renderInteractions()"><span>Hide noise</span></label>
          <span class="badge" id="msgs-count">0</span>
        </div>
      </div>
      <div class="ix-head"><span>Kind</span><span>Actor</span><span>Interaction</span><span>Detail</span><span style="text-align:right">Time</span></div>
      <div class="card-body" style="padding:0">
        <div id="msgs-list"><div class="empty" style="padding:16px">Loading...</div></div>
      </div>
    </div>
  </div>

  <!-- Tab: Tools -->
  <div class="tab-panel" id="panel-tools">
    <!-- Add MCP -->
    <div class="card" style="margin-bottom:14px">
      <div class="card-header">
        <h2>Request New MCP / Tool</h2>
        <button class="filter-btn" onclick="toggleAddMcp()" id="add-mcp-toggle">Add MCP</button>
      </div>
      <div class="card-body">
        <div class="add-mcp-form" id="add-mcp-form">
          <input class="msg-input" id="mcp-title" placeholder="Tool name, e.g. Firecrawl MCP" style="margin-bottom:8px">
          <textarea class="add-mcp-textarea" id="mcp-desc" placeholder="Describe what tool to add, how to configure it, API keys, etc..."></textarea>
          <div style="display:flex;gap:8px;margin-top:8px;align-items:center">
            <button class="msg-btn" onclick="submitMcpRequest()">Create Task →</button>
            <span style="font-size:10px;color:var(--muted)">Will be marked as CRITICAL priority</span>
            <span class="msg-status" id="mcp-status"></span>
          </div>
        </div>
      </div>
    </div>
    <!-- MCP Servers -->
    <div class="card" style="margin-bottom:14px">
      <div class="card-header"><h2>MCP Servers</h2><span class="badge" id="mcp-count">0</span></div>
      <div class="card-body">
        <div class="tool-grid" id="mcp-list"><div class="empty">Loading...</div></div>
      </div>
    </div>
    <!-- Tools -->
    <div class="card">
      <div class="card-header"><h2>Registered Tools</h2><span class="badge" id="tools-count">0</span></div>
      <div class="card-body">
        <div class="tool-grid" id="tools-list"><div class="empty">Loading...</div></div>
      </div>
    </div>
  </div>

  <!-- Tab: Actions -->
  <div class="tab-panel" id="panel-actions">
    <div class="card">
      <div class="card-header"><h2>Action Required</h2><span class="badge" id="actions-count">0</span></div>
      <div class="card-body">
        <div class="action-cards" id="actions-list"><div class="empty">Loading...</div></div>
      </div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Resolved Actions</h2><span class="badge" id="resolved-actions-count">0</span></div>
      <div class="card-body">
        <div class="action-cards" id="resolved-actions-list"><div class="empty">No resolved actions</div></div>
      </div>
    </div>
    <!-- Resolve Confirmation -->
    <div class="resolve-overlay" id="resolve-overlay" onclick="if(event.target===this)cancelResolve()">
      <div class="resolve-box">
        <h3 id="resolve-title">Resolve Action</h3>
        <p style="font-size:12px;color:var(--muted);margin:0 0 10px" id="resolve-subtitle">Add a comment or reason</p>
        <textarea id="resolve-comment" placeholder="Comment / reason..."></textarea>
        <div class="btn-row">
          <button class="btn-cancel" onclick="cancelResolve()">Cancel</button>
          <button id="resolve-confirm-btn" onclick="confirmResolveAction()">Confirm</button>
        </div>
      </div>
    </div>
  </div>

  <!-- Tab: Control -->
  <div class="tab-panel" id="panel-control">
    <div class="grid-3">
      <div class="card">
        <div class="card-header"><h2>Operator Queue</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="openOperatorQueueComposer()">Add Queue Item</button><span class="badge" id="ops-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="memory-toolbar" style="margin-bottom:10px">
            <select id="ops-filter-status" onchange="loadOperatorQueue()">
              <option value="">All statuses</option>
              <option value="OPEN">OPEN</option>
              <option value="RESOLVED">RESOLVED</option>
              <option value="CANCELLED">CANCELLED</option>
            </select>
            <select id="ops-filter-type" onchange="loadOperatorQueue()">
              <option value="">All queue types</option>
              <option value="BLOCKER">BLOCKER</option>
              <option value="DECISION">DECISION</option>
              <option value="HANDOFF">HANDOFF</option>
              <option value="FOLLOW_UP">FOLLOW_UP</option>
            </select>
          </div>
          <div class="action-cards" id="ops-list"><div class="empty">Loading...</div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-header"><h2>Knowledge Claims</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="openClaimComposer()">Add Claim</button><span class="badge" id="claims-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="memory-toolbar" style="margin-bottom:10px">
            <input id="claim-search-query" class="filter-search" placeholder="Search claims..." oninput="scheduleClaimsRefresh()">
            <select id="claim-filter-status" onchange="loadClaims()">
              <option value="">All statuses</option>
              <option value="CONFIRMED">CONFIRMED</option>
              <option value="ACTIVE">ACTIVE</option>
              <option value="REVIEW">REVIEW</option>
              <option value="STALE">STALE</option>
              <option value="SUPERSEDED">SUPERSEDED</option>
              <option value="DISPUTED">DISPUTED</option>
              <option value="ARCHIVED">ARCHIVED</option>
            </select>
            <label class="memory-toolbar-check"><input id="claim-filter-archived" type="checkbox" onchange="loadClaims()"> archived</label>
          </div>
          <div class="action-cards" id="claims-list"><div class="empty">Loading...</div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-header"><h2>Execution Runs</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="openExecutionRunComposer()">New Run</button><span class="badge" id="execution-runs-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="memory-toolbar" style="margin-bottom:10px">
            <select id="execution-filter-status" onchange="loadExecutionRuns()">
              <option value="">All statuses</option>
              <option value="ACTIVE">ACTIVE</option>
              <option value="WAITING">WAITING</option>
              <option value="BLOCKED">BLOCKED</option>
              <option value="COMPLETED">COMPLETED</option>
              <option value="FAILED">FAILED</option>
            </select>
          </div>
          <div class="action-cards" id="execution-runs-list"><div class="empty">Loading...</div></div>
        </div>
      </div>
    </div>
    <div class="grid-3" style="margin-top:14px">
      <div class="card">
        <div class="card-header"><h2>Capability Policies</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="openPolicyComposer()">Add Policy</button><button class="hdr-btn" onclick="runPolicyCheckFromDashboard()">Check Policy</button><span class="badge" id="policy-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="memory-toolbar" style="margin-bottom:10px">
            <input id="policy-filter-subject" class="filter-search" placeholder="Filter by subject/capability..." oninput="loadPolicies()">
          </div>
          <div class="action-cards" id="policy-list"><div class="empty">Loading...</div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-header"><h2>Runtime Journal</h2><div style="display:flex;gap:8px;align-items:center"><input id="runtime-event-filter" class="filter-search" placeholder="event type / entity" oninput="loadRuntimeEvents()" style="width:170px"><span class="badge" id="events-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="action-cards" id="runtime-events-list"><div class="empty">Loading...</div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-header"><h2>Compaction</h2><div style="display:flex;gap:8px;align-items:center"><span class="badge" id="compaction-candidate-count">0 candidates</span><span class="badge" id="compaction-snapshot-count">0 snapshots</span></div></div>
        <div class="card-body scroll">
          <div style="font-size:12px;font-weight:600;margin-bottom:8px">Candidates</div>
          <div class="action-cards" id="compaction-candidates-list"><div class="empty">Loading...</div></div>
          <div style="font-size:12px;font-weight:600;margin:14px 0 8px">Recent Snapshots</div>
          <div class="action-cards" id="compaction-snapshots-list"><div class="empty">Loading...</div></div>
        </div>
      </div>
    </div>
    <div class="grid" style="margin-top:14px">
      <div class="card">
        <div class="card-header"><h2>Advisory Control</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadControlPolicyOverlay()">Refresh View</button><button class="hdr-btn" id="control-policy-snapshot-btn" onclick="createControlPolicySnapshot()">Record Snapshot</button><button class="hdr-btn" id="unified-control-snapshot-btn" onclick="createUnifiedControlSnapshot()">Record Unified Snapshot</button><span class="badge" id="control-policy-list-count">0</span></div></div>
        <div class="card-body">
          <div class="memory-toolbar" style="margin-bottom:10px;flex-wrap:wrap">
            <select id="control-policy-filter-type" onchange="loadControlPolicyOverlay()">
              <option value="">All advisory tensions</option>
              <option value="bottleneck">bottleneck</option>
              <option value="contradiction">contradiction</option>
              <option value="ambiguity">ambiguity</option>
              <option value="gap">gap</option>
              <option value="bridge">bridge</option>
            </select>
            <select id="control-policy-filter-mode" onchange="loadControlPolicyOverlay()">
              <option value="">All attention bands</option>
              <option value="hot">hot</option>
              <option value="watch">watch</option>
              <option value="steady">steady</option>
            </select>
            <input id="control-policy-filter-query" class="filter-search" placeholder="cluster / task / agent" oninput="loadControlPolicyOverlay()">
            <button class="participant-btn" onclick="resetControlPolicyOverlayFilters()">Reset</button>
          </div>
          <div id="control-policy-summary"><div class="empty">Loading advisory control report...</div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-header"><h2>Control Clusters</h2><div style="display:flex;gap:8px;align-items:center"><span class="badge" id="control-policy-generated-at">loading</span><span class="badge" id="control-policy-selected">none</span></div></div>
        <div class="card-body scroll scroll-tall">
          <div class="action-cards" id="control-policy-cluster-list"><div class="empty">No advisory clusters are available yet.</div></div>
        </div>
      </div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Latest Advisory Snapshot</h2><span class="badge" id="control-policy-snapshot-state">none</span></div>
      <div class="card-body" id="control-policy-snapshot-summary"><div class="empty">Record a control advisory snapshot to persist the current control report into the runtime journal.</div></div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Latest Unified Advisory Snapshot</h2><span class="badge" id="unified-control-snapshot-state">none</span></div>
      <div class="card-body" id="unified-control-snapshot-summary"><div class="empty">Record a unified advisory snapshot to persist the current unified-control report into the runtime journal.</div></div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Task-Class / Corridor Readiness</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadCorridorReadiness()">Refresh</button><button class="hdr-btn" id="corridor-readiness-snapshot-btn" onclick="createCorridorReadinessSnapshot()">Record Snapshot</button><span class="badge" id="corridor-readiness-state">read-only</span></div></div>
      <div class="card-body" id="corridor-readiness-summary"><div class="empty">Task-class evidence and corridor-readiness approximation will appear once the corridor read-side report loads.</div></div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Task-First Corridor Authority</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadCorridorAuthority()">Refresh</button><span class="badge" id="corridor-authority-state">read-only</span></div></div>
      <div class="card-body" id="corridor-authority-summary"><div class="empty">Task-first corridor authority remains a read-only precedence surface over explicit task_class evidence and derived lookup context.</div></div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Corridor Ownership / Basis</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadCorridorOwnership()">Refresh</button><button class="hdr-btn" id="corridor-ownership-snapshot-btn" onclick="createCorridorOwnershipSnapshot()">Record Snapshot</button><span class="badge" id="corridor-ownership-state">read-only</span></div></div>
      <div class="card-body" id="corridor-ownership-summary"><div class="empty">Cluster-level corridor ownership and basis will appear once the ownership read-side report loads.</div></div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Corridor Boundary / Violations</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadCorridorFit()">Refresh</button><button class="hdr-btn" id="corridor-fit-snapshot-btn" onclick="createCorridorFitSnapshot()">Record Snapshot</button><span class="badge" id="corridor-fit-state">read-only</span></div></div>
      <div class="card-body" id="corridor-fit-summary"><div class="empty">Corridor boundary and violation approximation will appear once the corridor-fit read-side report loads.</div></div>
    </div>
    <div class="card" style="margin-top:14px">
<div class="card-header"><h2>Control-State Scaffold</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadControlStateScaffold()">Refresh</button><button class="hdr-btn" id="control-state-tick-btn" onclick="tickControlStateScaffold()">Tick</button><button class="hdr-btn" id="control-state-snapshot-btn" onclick="createControlStateSnapshot()">Record Snapshot</button><span class="badge" id="control-policy-scaffold-state">read-only</span></div></div>
      <div class="card-body" id="control-policy-scaffold-summary"><div class="empty">Control-state scaffold will appear once advisory control report loads.</div></div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Control Cluster Detail</h2><span class="badge" id="control-policy-detail-state">none selected</span></div>
      <div class="card-body" id="control-policy-detail"><div class="empty">Select a control cluster to inspect advisory signals, suggested controls, and recent runtime events.</div></div>
    </div>
    <div class="grid" style="margin-top:14px">
      <div class="card">
        <div class="card-header"><h2>Operator Inbox</h2><div style="display:flex;gap:8px;align-items:center"><span class="badge" id="ops-inbox-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="memory-toolbar" style="margin-bottom:10px">
            <input id="ops-inbox-search" class="filter-search" placeholder="Search attention..." oninput="loadOperatorInbox()">
            <select id="ops-inbox-filter" onchange="loadOperatorInbox()">
              <option value="">All signals</option>
              <option value="session">Sessions</option>
              <option value="queue">Queue</option>
              <option value="claim">Claims</option>
              <option value="policy">Policy</option>
              <option value="compaction">Compaction</option>
              <option value="tension">Tensions</option>
            </select>
          </div>
          <div class="action-cards" id="ops-inbox-list"><div class="empty">Loading...</div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-header"><h2>Replay & Evaluation</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="runReplayWorkbench('evaluate')">Evaluate</button><button class="hdr-btn" onclick="runReplayWorkbench('replay')">Replay</button><span class="badge" id="replay-verdict-badge">idle</span></div></div>
        <div class="card-body scroll">
          <div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-bottom:10px">
            <input id="replay-filter-agent" class="filter-search" placeholder="Agent id">
            <input id="replay-filter-session" class="filter-search" placeholder="Session id">
            <input id="replay-filter-task" class="filter-search" placeholder="Task id">
            <input id="replay-filter-limit" class="filter-search" placeholder="Limit" value="200">
          </div>
          <div class="memory-toolbar" style="margin-bottom:10px">
            <label class="memory-toolbar-check"><input id="replay-include-events" type="checkbox"> include events</label>
            <button class="participant-btn" onclick="resetReplayWorkbench()">Reset</button>
            <button class="participant-btn" onclick="openReplayReportModal()">Open Report</button>
          </div>
          <div id="replay-summary"><div class="empty">Run evaluation to inspect runtime journal health.</div></div>
          <div style="font-size:12px;font-weight:600;margin:14px 0 8px">Findings</div>
          <div class="action-cards" id="replay-findings-list"><div class="empty">No replay findings yet.</div></div>
        </div>
      </div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Claim Review Workbench</h2><div style="display:flex;gap:8px;align-items:center"><span class="badge" id="claim-review-count">0</span></div></div>
      <div class="card-body scroll">
        <div class="memory-toolbar" style="margin-bottom:10px">
          <input id="claim-review-search" class="filter-search" placeholder="Search review claims..." oninput="loadClaimReviewWorkbench()">
          <select id="claim-review-status" onchange="loadClaimReviewWorkbench()">
            <option value="">All review states</option>
            <option value="REVIEW">REVIEW</option>
            <option value="DISPUTED">DISPUTED</option>
            <option value="STALE">STALE</option>
            <option value="SUPERSEDED">SUPERSEDED</option>
          </select>
        </div>
        <div class="action-cards" id="claim-review-list"><div class="empty">Loading...</div></div>
      </div>
    </div>
  </div>

  <!-- Tab: Instrumentation -->
  <div class="tab-panel" id="panel-instrumentation">
    <div class="card" style="margin-bottom:14px">
      <div class="card-header"><h2>Proto-Cluster Instrumentation</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadInstrumentation()">Refresh</button><button class="hdr-btn" id="instrumentation-snapshot-btn" onclick="createInstrumentationSnapshot()">Record Snapshot</button><span class="badge" id="instrumentation-generated-at">loading</span></div></div>
      <div class="card-body">
        <div class="memory-toolbar" style="margin-bottom:10px;flex-wrap:wrap">
          <input id="instrumentation-filter-agent" class="filter-search" placeholder="agent id">
          <input id="instrumentation-filter-session" class="filter-search" placeholder="session id">
          <input id="instrumentation-filter-task" class="filter-search" placeholder="task id">
          <input id="instrumentation-filter-limit" class="filter-search" placeholder="event limit" value="200" style="max-width:90px">
          <input id="instrumentation-filter-cluster-limit" class="filter-search" placeholder="cluster limit" value="12" style="max-width:100px">
          <button class="participant-btn" onclick="resetInstrumentationFilters()">Reset</button>
        </div>
        <div id="instrumentation-filter-summary" class="memory-filter-context" style="display:none"></div>
        <div id="instrumentation-workspace-summary"><div class="empty">Loading instrumentation...</div></div>
      </div>
    </div>
    <div class="grid" style="margin-top:14px">
      <div class="card">
        <div class="card-header"><h2>Top Proto-Clusters</h2><div style="display:flex;gap:8px;align-items:center"><span class="badge" id="instrumentation-clusters-count">0</span><span class="badge" id="instrumentation-truncated-badge">0 shown</span></div></div>
        <div class="card-body scroll">
          <div class="action-cards" id="instrumentation-clusters-list"><div class="empty">Loading instrumentation...</div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-header"><h2>Latest Snapshot Event</h2><span class="badge" id="instrumentation-snapshot-state">none</span></div>
        <div class="card-body" id="instrumentation-snapshot-summary"><div class="empty">Record a snapshot to persist cluster metrics into the runtime journal.</div></div>
      </div>
    </div>
  </div>

  <!-- Tab: Tensions -->
  <div class="tab-panel" id="panel-tensions">
    <div class="card" style="margin-bottom:14px">
      <div class="card-header"><h2>Tension Overlay</h2><div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" id="tension-refresh-btn" onclick="refreshTensions()">Refresh Frontier</button><button class="hdr-btn" onclick="loadTensions()">Reload</button><span class="badge" id="tensions-generated-at">loading</span></div></div>
      <div class="card-body">
        <div class="memory-toolbar" style="margin-bottom:10px;flex-wrap:wrap">
          <select id="tension-filter-type" onchange="loadTensions()">
            <option value="">All types</option>
            <option value="bottleneck">bottleneck</option>
            <option value="contradiction">contradiction</option>
            <option value="ambiguity">ambiguity</option>
            <option value="gap">gap</option>
            <option value="bridge">bridge</option>
          </select>
          <select id="tension-filter-lifecycle" onchange="loadTensions()">
            <option value="">All lifecycle states</option>
            <option value="ACTIVE">ACTIVE</option>
            <option value="ARCHIVED">ARCHIVED</option>
          </select>
          <select id="tension-filter-review" onchange="loadTensions()">
            <option value="">All review states</option>
            <option value="PENDING">PENDING</option>
            <option value="CONFIRMED">CONFIRMED</option>
            <option value="DISCARDED">DISCARDED</option>
          </select>
          <input id="tension-filter-task" class="filter-search" placeholder="task id" oninput="loadTensions()">
          <input id="tension-filter-agent" class="filter-search" placeholder="agent id" oninput="loadTensions()">
          <input id="tension-filter-cluster" class="filter-search" placeholder="proto-cluster id" oninput="loadTensions()">
          <button class="participant-btn" onclick="resetTensionFilters()">Reset</button>
        </div>
        <div id="tension-filter-summary" class="memory-filter-context" style="display:none"></div>
        <div id="tension-workspace-summary"><div class="empty">Loading tensions...</div></div>
      </div>
    </div>
    <div class="grid" style="margin-top:14px">
      <div class="card">
        <div class="card-header"><h2>Tension Frontier</h2><div style="display:flex;gap:8px;align-items:center"><span class="badge" id="tension-frontier-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="action-cards" id="tension-frontier-list"><div class="empty">Loading frontier...</div></div>
        </div>
      </div>
      <div class="card">
        <div class="card-header"><h2>All Tensions</h2><div style="display:flex;gap:8px;align-items:center"><span class="badge" id="tension-list-count">0</span></div></div>
        <div class="card-body scroll">
          <div class="action-cards" id="tension-list"><div class="empty">Loading tensions...</div></div>
        </div>
      </div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Tension Detail</h2><span class="badge" id="tension-detail-state">none selected</span></div>
      <div class="card-body" id="tension-detail-summary"><div class="empty">Select a tension from the frontier or list to inspect evidence and take action.</div></div>
    </div>
  </div>

  <!-- Tab: Vault -->
  <div class="tab-panel" id="panel-vault">
    <div class="card">
      <div class="card-header"><h2>Vault</h2><span class="badge" id="vault-count">0</span>
        <button class="hdr-btn" style="margin-left:auto" onclick="toggleCreateVault()">+ Add Entry</button>
      </div>
      <div id="create-vault-form" class="add-mcp-form">
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px">
          <input id="cv-title" placeholder="Title (e.g. AWS Credentials)" style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:6px 8px;color:var(--text);font-size:12px;font-family:var(--font)">
          <input id="cv-desc" placeholder="Description (optional)" style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:6px 8px;color:var(--text);font-size:12px;font-family:var(--font)">
        </div>
        <div id="cv-fields" style="margin-bottom:8px"></div>
        <div style="display:flex;gap:8px;align-items:center">
          <button onclick="addCvField()" style="background:none;border:1px dashed var(--border);border-radius:6px;color:var(--accent);font-size:11px;padding:4px 10px;cursor:pointer;font-family:var(--font)">+ Add Field</button>
          <button onclick="submitNewVault()" style="background:var(--accent);border:none;color:#fff;padding:6px 16px;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;font-family:var(--font)">Create</button>
          <span id="cv-status" style="font-size:11px"></span>
        </div>
      </div>
      <div class="card-body">
        <div class="tool-grid" id="vault-list"><div class="empty">Loading...</div></div>
      </div>
    </div>
    <div class="card" style="margin-top:14px">
      <div class="card-header"><h2>Audit Log</h2><span class="badge" id="audit-count">0</span></div>
      <div class="card-body">
        <div id="vault-audit-list"><div class="empty">Loading...</div></div>
      </div>
    </div>
  </div>

  <!-- Tab: Logs -->
  <div class="tab-panel" id="panel-logs">
    <div class="card">
      <div class="card-header">
        <h2>RPC Access Log</h2>
        <div style="display:flex;gap:8px;align-items:center;margin-left:auto">
          <select id="logs-method-filter" onchange="resetRpcLogs()" style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:4px 8px;color:var(--text);font-size:11px;font-family:var(--font)">
            <option value="">All methods</option>
          </select>
          <select id="logs-status-filter" onchange="resetRpcLogs()" style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:4px 8px;color:var(--text);font-size:11px;font-family:var(--font)">
            <option value="">All</option>
            <option value="ok">OK</option>
            <option value="error">Errors</option>
          </select>
        </div>
      </div>
      <div id="logs-stats" class="rpc-log-stats"></div>
      <div class="card-body" style="padding:0">
        <div class="rpc-log-head"><span></span><span>Method</span><span>Actor</span><span style="text-align:right">Latency</span><span>Message</span><span style="text-align:right">Time</span></div>
        <div id="logs-list"><div class="empty" style="padding:16px">Loading...</div></div>
      </div>
    </div>
  </div>

  <!-- Tab: Limits -->
  <div class="tab-panel" id="panel-limits">
    <div class="card" style="margin-bottom:14px">
      <div class="card-header"><h2>Limit Groups</h2><div style="display:flex;gap:8px;align-items:center"><button class="btn-accent" onclick="toggleCreateLimitGroup()">New Group</button><span class="badge" id="limit-groups-count">0</span></div></div>
      <div class="card-body">
        <div class="create-task-form" id="create-limit-group-form">
          <div class="form-grid">
            <div><label>Group ID *</label><input id="clg-id" placeholder="e.g. openai-team" style="font-family:monospace"></div>
            <div><label>Title *</label><input id="clg-title" placeholder="e.g. OpenAI Plus Team"></div>
          </div>
          <div class="form-grid">
            <div><label>Owner</label><input id="clg-owner" placeholder="Subscription owner name"></div>
            <div><label>Subscription Tier</label><input id="clg-tier" placeholder="e.g. openai-plus, claude-pro"></div>
          </div>
          <div class="form-actions" style="margin-top:10px">
            <button class="btn-accent" onclick="submitNewLimitGroup()">Create Group →</button>
            <span id="clg-status" class="msg-status" style="font-size:12px"></span>
          </div>
        </div>
        <div class="tool-grid" id="limit-groups-list"><div class="empty">Loading...</div></div>
      </div>
    </div>
    <!-- Stats Panel -->
    <div class="card">
      <div class="card-header"><h2>Usage History</h2><div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <select id="stats-group-select" onchange="loadLimitStats()" style="background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:4px 8px;font-size:11px;font-family:var(--font);outline:none"></select>
        <div style="display:flex;gap:2px;background:var(--surface);border-radius:6px;padding:2px;border:1px solid var(--border)">
          <button class="stats-mode-btn active" data-mode="weekly" onclick="setStatsMode('weekly',this)">Weekly</button>
          <button class="stats-mode-btn" data-mode="daily" onclick="setStatsMode('daily',this)">Daily</button>
        </div>
        <div style="display:flex;gap:2px;background:var(--surface);border-radius:6px;padding:2px;border:1px solid var(--border)">
          <button class="stats-mode-btn" data-gran="1" onclick="setStatsGranularity(1,this)">1h</button>
          <button class="stats-mode-btn" data-gran="6" onclick="setStatsGranularity(6,this)">6h</button>
          <button class="stats-mode-btn active" data-gran="24" onclick="setStatsGranularity(24,this)">1d</button>
        </div>
      </div></div>
      <div class="card-body">
        <div id="stats-chart" style="display:flex;align-items:flex-end;gap:2px;height:120px;padding:8px 0;overflow-x:auto"><div class="empty">Select a group to see history</div></div>
        <div id="stats-x-labels" style="display:flex;gap:2px;font-size:9px;color:var(--muted);overflow-x:auto"></div>
      </div>
    </div>
  </div>

  <!-- Tab: Security -->
  <div class="tab-panel" id="panel-security">
    <div class="security-grid">
      <div class="security-card">
        <div class="card-header" style="padding:0 0 14px;border:none">
          <h2>Workspace security</h2>
          <div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadWorkspaceSecurity()">Refresh</button><span class="badge" id="security-settings-badge">ready</span></div>
        </div>
        <div class="security-form" id="workspace-security-form">
          <div class="security-row">
            <div>
              <label for="ws-security-id">Workspace ID</label>
              <input id="ws-security-id" readonly>
            </div>
            <div>
              <label for="ws-security-name">Workspace name</label>
              <input id="ws-security-name" placeholder="rhizome-main">
            </div>
          </div>
          <div class="security-row">
            <div>
              <label for="ws-security-password">Workspace password</label>
              <input id="ws-security-password" type="password" autocomplete="new-password" placeholder="Leave blank to keep the current password.">
            </div>
            <div>
              <label for="ws-security-description">Description</label>
              <input id="ws-security-description" placeholder="trusted internal workspace">
            </div>
          </div>
          <div class="security-row">
            <div>
              <label for="ws-security-default-human">Default human password hint</label>
              <input id="ws-security-default-human" value="set per user" readonly>
            </div>
            <div>
              <label for="ws-security-default-agent">Default agent token policy</label>
              <input id="ws-security-default-agent" value="one token per agent" readonly>
            </div>
          </div>
          <button type="button" class="workspace-action-btn" style="background:var(--accent-strong);color:#f5f0ff;width:fit-content" onclick="saveWorkspaceSecurity()">Save workspace security</button>
          <div id="workspace-security-status" class="msg-status"></div>
        </div>
      </div>
      <div class="security-card">
        <div class="card-header" style="padding:0 0 14px;border:none">
          <h2>Security logs</h2>
          <div style="display:flex;gap:8px;align-items:center"><button class="hdr-btn" onclick="loadSecurityLogs()">Refresh</button><span class="badge" id="security-log-count">0</span></div>
        </div>
        <div class="security-log-list scroll" id="security-log-list"><div class="empty">Loading security logs...</div></div>
      </div>
    </div>
  </div>

  <!-- Tab: News -->
  <div class="tab-panel" id="panel-news">
    <div class="card" style="margin-bottom:14px">
      <div class="card-header"><h2>News Feed</h2><div style="display:flex;gap:8px;align-items:center"><button class="btn-accent" onclick="toggleCreateNews()">Publish</button><span class="badge" id="news-count">0</span></div></div>
      <div class="card-body">
        <div class="create-task-form" id="create-news-form">
          <div class="form-grid">
            <div><label>Title *</label><input id="cn-title" placeholder="News headline"></div>
            <div><label>Author</label><input id="cn-author" placeholder="Your name or agent_id"></div>
          </div>
          <div style="margin-bottom:10px">
            <label style="font-size:11px;font-weight:600;color:var(--muted);display:block;margin-bottom:4px">Content (markdown)</label>
            <textarea id="cn-content" rows="4" placeholder="What's new? Share tools, features, findings..." style="width:100%;background:var(--surface);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px;font-size:12px;font-family:var(--font);resize:vertical;outline:none"></textarea>
          </div>
          <div class="form-actions">
            <button class="btn-accent" onclick="submitNews()">Publish →</button>
            <span id="cn-status" class="msg-status" style="font-size:12px"></span>
          </div>
        </div>
        <div id="news-list" style="display:flex;flex-direction:column;gap:10px"><div class="empty">Loading...</div></div>
      </div>
    </div>
  </div>

</div>
</div>

<div class="cmdk-overlay" id="cmd-palette" onclick="if(event.target===this)closeCmdPalette()">
  <div class="cmdk-box" role="dialog" aria-label="Command palette">
    <input id="cmdk-input" class="cmdk-input" type="text" placeholder="Jump to a tab or agent…" autocomplete="off" spellcheck="false" aria-label="Command palette search" />
    <div id="cmdk-list" class="cmdk-list" role="listbox"></div>
  </div>
</div>

<script>
const API = window.location.origin + '/rpc';
let WS_ID = new URLSearchParams(window.location.search).get('ws_id') || localStorage.getItem('rhizome_ws_id') || 'rhizome-main';
if (WS_ID === 'workspace-main' && !new URLSearchParams(window.location.search).get('ws_id')) {
    WS_ID = 'rhizome-main'; // Auto-recover from the temporary test setup
}
localStorage.setItem('rhizome_ws_id', WS_ID);

const AUTH_API = window.location.origin + '/api/auth';
const HUMAN_PROFILE_API = AUTH_API + '/human/profile';
const HUMAN_SESSIONS_API = AUTH_API + '/human/sessions';
const AGENT_TOKEN_ROTATE_API = AUTH_API + '/agent/token/rotate';
const SECURITY_API = window.location.origin + '/api/workspace/security';
const SECURITY_LOGS_API = window.location.origin + '/api/workspace/security/logs';
// Contract note:
// - /api/auth/human/login
// - /api/auth/human/register
// - /api/auth/human/profile/get
// - /api/auth/human/profile/update
// - /api/auth/human/sessions/list
// - /api/auth/human/sessions/revoke
// - /api/auth/agent/register
// - /api/auth/agent/token/rotate
// - /api/workspace/security/get
// - /api/workspace/security/update
// - /api/workspace/security/logs
const AUTH_STATE_KEY = 'rhizome_auth_state_v1';
const LEGACY_TOKEN_KEY = 'rhizome_token';

function loadAuthState() {
  const params = new URLSearchParams(window.location.search);
  if (params.has('token')) {
    params.delete('token');
    const cleaned = window.location.pathname + (params.toString() ? ('?' + params.toString()) : '') + window.location.hash;
    window.history.replaceState({}, '', cleaned);
  }
  const raw = localStorage.getItem(AUTH_STATE_KEY);
  if (raw) {
    try {
      const parsed = JSON.parse(raw);
      if (parsed && parsed.access_token) return parsed;
    } catch (e) {}
  }
  const legacyToken = localStorage.getItem(LEGACY_TOKEN_KEY);
  if (legacyToken) {
    return { access_token: legacyToken, actor_type: 'legacy', actor_name: 'Legacy token', workspace_id: WS_ID, source: 'legacy' };
  }
  return null;
}

let AUTH_STATE = loadAuthState();
let TOKEN = AUTH_STATE?.access_token || '';
let idCounter = 0;
let appStarted = false;
let refreshTimer = null;
let runtimeHealthRefreshTimer = null;
let presenceTickTimer = null;
let sse = null;
let sseAbortController = null;
let sseRetryMs = 1000;
let lastSSEActivityAt = 0;
let authMode = 'human-login';
let HUMAN_PROFILE_STATE = null;
let HUMAN_SESSIONS_STATE = null;
let _dashboardDialogState = null;

function persistAuthState(state) {
  AUTH_STATE = state;
  TOKEN = state?.access_token || '';
  if (state) {
    localStorage.setItem(AUTH_STATE_KEY, JSON.stringify(state));
    localStorage.setItem(LEGACY_TOKEN_KEY, state.access_token);
    localStorage.setItem('rhizome_ws_id', state.workspace_id || WS_ID);
    if (state.workspace_id) {
      WS_ID = state.workspace_id;
    }
  } else {
    localStorage.removeItem(AUTH_STATE_KEY);
  }
  syncWorkspaceInputs();
  syncAuthChrome();
}

function clearAuthState() {
  AUTH_STATE = null;
  TOKEN = '';
  HUMAN_PROFILE_STATE = null;
  HUMAN_SESSIONS_STATE = null;
  localStorage.removeItem(AUTH_STATE_KEY);
  localStorage.removeItem(LEGACY_TOKEN_KEY);
  syncAuthChrome();
}

function syncWorkspaceInputs() {
  const inputs = ['human-login-workspace','human-register-workspace','agent-register-workspace','ws-security-id','ws-security-name'];
  inputs.forEach(id => {
    const el = document.getElementById(id);
    if (el && WS_ID) el.value = WS_ID;
  });
  const wsName = document.getElementById('ws-security-name');
  if (wsName && AUTH_STATE?.workspace_name) wsName.value = AUTH_STATE.workspace_name;
}

function syncAuthChrome() {
  const shell = document.getElementById('auth-shell');
  const dash = document.getElementById('dashboard-shell');
  const wrap = document.getElementById('profile-wrap');
  const label = document.getElementById('profile-label');
  const principal = AUTH_STATE?.actor_name || AUTH_STATE?.actor_type || 'Profile';
  if (TOKEN) {
    if (shell) shell.classList.add('shell-hidden');
    if (dash) dash.classList.remove('shell-hidden');
    if (wrap) wrap.style.display = 'flex';
    if (label) label.textContent = principal;
    document.body.classList.remove('auth-locked');
  } else {
    if (shell) shell.classList.remove('shell-hidden');
    if (dash) dash.classList.add('shell-hidden');
    if (wrap) wrap.style.display = 'none';
    closeProfileMenu();
    document.body.classList.add('auth-locked');
  }
}

function openAuthShell(mode = 'human-login') {
  switchAuthMode(mode);
  syncAuthChrome();
}

function finalizeLogoutUI() {
  clearAuthState();
  disconnectSSE();
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  if (runtimeHealthRefreshTimer) {
    clearInterval(runtimeHealthRefreshTimer);
    runtimeHealthRefreshTimer = null;
  }
  if (presenceTickTimer) {
    clearInterval(presenceTickTimer);
    presenceTickTimer = null;
  }
  appStarted = false;
  syncAuthChrome();
  openAuthShell('human-login');
}

async function logout(options = {}) {
  const revokeCurrentSession = options.revokeCurrentSession !== false;
  if (revokeCurrentSession && TOKEN && (AUTH_STATE?.actor_type || 'human') === 'human') {
    try {
      const sessionState = await loadHumanSessions(true);
      const currentTokenID = String(sessionState?.current_token_id || '').trim();
      if (currentTokenID) {
        await apiPost(HUMAN_SESSIONS_API + '/revoke', {token_id: currentTokenID}, true, false);
      }
    } catch (err) {}
  }
  finalizeLogoutUI();
}

function closeProfileMenu() {
  document.getElementById('profile-wrap')?.classList.remove('open');
}

function toggleProfileMenu(ev) {
  if (ev) {
    ev.preventDefault();
    ev.stopPropagation();
  }
  if (!TOKEN) return;
  const wrap = document.getElementById('profile-wrap');
  if (!wrap) return;
  const isOpen = wrap.classList.contains('open');
  closeProfileMenu();
  if (!isOpen) wrap.classList.add('open');
}

function currentProfileId() {
  return HUMAN_PROFILE_STATE?.user_id || AUTH_STATE?.actor_id || AUTH_STATE?.user_id || AUTH_STATE?.agent_id || '';
}

function currentProfileType() {
  if (HUMAN_PROFILE_STATE) return 'human';
  return AUTH_STATE?.actor_type || (AUTH_STATE?.agent_id ? 'agent' : 'human');
}

function currentProfileName() {
  return HUMAN_PROFILE_STATE?.display_name || AUTH_STATE?.actor_name || AUTH_STATE?.display_name || AUTH_STATE?.name || AUTH_STATE?.actor_type || 'Profile';
}

function currentProfileUsername() {
  return HUMAN_PROFILE_STATE?.username || AUTH_STATE?.username || AUTH_STATE?.login_name || '';
}

function currentWorkspaceName() {
  return HUMAN_PROFILE_STATE?.workspace_name || AUTH_STATE?.workspace_name || WS_ID;
}

function syncAuthStateFromProfile(profile) {
  if (!profile || !TOKEN) return;
  persistAuthState({
    ...(AUTH_STATE || {}),
    access_token: (AUTH_STATE && AUTH_STATE.access_token) || TOKEN,
    actor_type: 'human',
    actor_id: profile.user_id || AUTH_STATE?.actor_id || AUTH_STATE?.user_id || '',
    user_id: profile.user_id || AUTH_STATE?.user_id || AUTH_STATE?.actor_id || '',
    username: profile.username || AUTH_STATE?.username || '',
    actor_name: profile.display_name || AUTH_STATE?.actor_name || '',
    display_name: profile.display_name || AUTH_STATE?.display_name || '',
    workspace_id: profile.workspace_id || AUTH_STATE?.workspace_id || WS_ID,
    workspace_name: profile.workspace_name || AUTH_STATE?.workspace_name || WS_ID,
    source: AUTH_STATE?.source || 'auth'
  });
}

async function loadHumanProfile(force = false) {
  if (!TOKEN) {
    throw new Error('Authentication required');
  }
  if ((AUTH_STATE?.actor_type || 'human') !== 'human') {
    return null;
  }
  if (!force && HUMAN_PROFILE_STATE) {
    return HUMAN_PROFILE_STATE;
  }
  const res = await apiPost(HUMAN_PROFILE_API + '/get', {});
  HUMAN_PROFILE_STATE = res.profile || res;
  syncAuthStateFromProfile(HUMAN_PROFILE_STATE);
  return HUMAN_PROFILE_STATE;
}

async function loadHumanSessions(force = false) {
  if (!TOKEN) {
    throw new Error('Authentication required');
  }
  if ((AUTH_STATE?.actor_type || 'human') !== 'human') {
    return { sessions: [], count: 0, current_token_id: '' };
  }
  if (!force && HUMAN_SESSIONS_STATE) {
    return HUMAN_SESSIONS_STATE;
  }
  HUMAN_SESSIONS_STATE = await apiPost(HUMAN_SESSIONS_API + '/list', {});
  return HUMAN_SESSIONS_STATE;
}

function ownedAgentsForProfile() {
  const profileId = String(currentProfileId() || '').trim();
  return agentsCache.filter(agent => {
    const ownerID = String(agent.owner_user_id || agent.owner_userID || agent.owner_id || agent.owner || '').trim();
    return profileId && ownerID === profileId;
  });
}

function renderOwnedAgentsList(agents) {
  const list = Array.isArray(agents) ? agents : ownedAgentsForProfile();
  const sorted = list.slice().sort((a, b) => String(a.display_name || a.agent_id || '').localeCompare(String(b.display_name || b.agent_id || '')));
  if (!sorted.length) {
    return '<div class="empty" style="margin:0">No owned agents found for this profile.</div>';
  }
  return '<div style="display:grid;gap:8px;max-height:260px;overflow:auto;padding-right:4px">' + sorted.map(agent => {
    const registration = String(agent.status || 'REGISTERED').trim() || 'REGISTERED';
    const presence = String(agent.liveness_status || (agent.is_online ? 'ONLINE' : 'REGISTERED_OFFLINE')).trim() || 'REGISTERED_OFFLINE';
    const presenceLabel = presence === 'ONLINE' ? 'Online' : 'Registered only';
    const protocolVersion = String(agent.protocol_version || '').trim();
    const capabilities = Array.isArray(agent.capabilities) ? agent.capabilities.map(item => String(item || '').trim()).filter(Boolean) : [];
    const capabilitySummary = capabilities.length ? capabilities.join(', ') : 'none declared';
    const seenAt = agent.last_seen_at ? timeAgo(agent.last_seen_at) : 'never seen';
    const summary = String(agent.summary || '').trim();
    const token = agent.auth_token || {};
    const tokenIssued = token.issued_at ? timeAgo(token.issued_at) : 'not issued';
    const tokenUsed = token.last_used_at ? timeAgo(token.last_used_at) : 'never';
    return '<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 12px;display:flex;justify-content:space-between;gap:10px;align-items:flex-start">' +
      '<div style="min-width:0">' +
        '<div style="font-size:13px;font-weight:600;color:var(--text)">' + esc(agent.display_name || agent.agent_id) + '</div>' +
        '<div style="font-size:10px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + esc(agent.agent_id) + '</div>' +
        '<div style="font-size:10px;color:var(--muted);margin-top:4px">Registration: ' + esc(registration) + ' · Presence: ' + esc(presenceLabel) + '</div>' +
        '<div style="font-size:10px;color:var(--muted);margin-top:4px">Protocol: ' + esc(protocolVersion || 'not declared') + '</div>' +
        '<div style="font-size:10px;color:var(--muted);margin-top:4px">Capabilities: ' + esc(capabilitySummary) + '</div>' +
        '<div style="font-size:10px;color:var(--muted);margin-top:4px">Last seen: ' + esc(seenAt) + '</div>' +
        '<div style="font-size:10px;color:var(--muted);margin-top:4px">Token: ' + esc(token.token_prefix || 'none') + ' · issued ' + esc(tokenIssued) + ' · last used ' + esc(tokenUsed) + '</div>' +
        (summary ? '<div style="font-size:11px;color:var(--muted);margin-top:6px;line-height:1.4">' + esc(summary) + '</div>' : '') +
      '</div>' +
      '<div style="display:flex;flex-direction:column;align-items:flex-end;gap:6px;flex-shrink:0">' +
        '<span class="agent-role">' + esc(agent.role || 'agent') + '</span>' +
        '<span class="agent-seen">' + esc(presenceLabel) + '</span>' +
        '<button type="button" class="hdr-btn" ' + dashboardAction(function(dashboardEvent){rotateOwnedAgentToken((agent.agent_id))}) + '>Rotate token</button>' +
      '</div>' +
    '</div>';
  }).join('') + '</div>';
}

function renderHumanSessionsList(sessionState) {
  const payload = sessionState || HUMAN_SESSIONS_STATE || {sessions: [], current_token_id: ''};
  const sessions = Array.isArray(payload.sessions) ? payload.sessions : [];
  if (!sessions.length) {
    return '<div class="empty" style="margin:0">No active auth sessions found.</div>';
  }
  return '<div style="display:grid;gap:8px;max-height:240px;overflow:auto;padding-right:4px">' + sessions.map(session => {
    const current = !!session.current;
    const issuedAt = session.issued_at ? timeAgo(session.issued_at) : 'unknown';
    const lastUsedAt = session.last_used_at ? timeAgo(session.last_used_at) : 'never';
    const status = session.revoked_at ? ('revoked ' + timeAgo(session.revoked_at)) : (current ? 'current session' : 'active');
    const canRevoke = !current && !session.revoked_at;
    return '<div style="background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:10px 12px;display:flex;justify-content:space-between;gap:10px;align-items:flex-start">' +
      '<div style="min-width:0">' +
        '<div style="font-size:12px;font-weight:600;color:var(--text)">' + esc(session.token_prefix || session.token_id || 'session') + '</div>' +
        '<div style="font-size:10px;color:var(--muted);margin-top:4px">Issued ' + esc(issuedAt) + ' · last used ' + esc(lastUsedAt) + '</div>' +
        '<div style="font-size:10px;color:var(--muted);margin-top:4px">' + esc(status) + '</div>' +
      '</div>' +
      '<div style="display:flex;flex-direction:column;align-items:flex-end;gap:6px;flex-shrink:0">' +
        '<span class="agent-seen">' + esc(current ? 'Current' : 'Session') + '</span>' +
        (canRevoke ? '<button type="button" class="hdr-btn" ' + dashboardAction(function(dashboardEvent){revokeHumanSession((session.token_id))}) + '>Revoke</button>' : '') +
      '</div>' +
    '</div>';
  }).join('') + '</div>';
}

function renderProfileSettingsModal(profile, feedback = {}) {
  const profileId = profile?.user_id || currentProfileId() || 'not available';
  const profileUsername = profile?.username || currentProfileUsername() || 'not available';
  const profileName = profile?.display_name || currentProfileName();
  const workspaceName = profile?.workspace_name || currentWorkspaceName();
  const agents = Array.isArray(profile?.agents) ? profile.agents : ownedAgentsForProfile();
  const lastLogin = profile?.last_login_at ? timeAgo(profile.last_login_at) : 'never';
  const sessionPayload = HUMAN_SESSIONS_STATE || {sessions: [], current_token_id: ''};
  const sessionCount = Array.isArray(sessionPayload.sessions) ? sessionPayload.sessions.length : 0;
  const toneColor = feedback.tone === 'error' ? 'var(--red)' : (feedback.tone === 'success' ? 'var(--green)' : 'var(--muted)');
  let body = '';
  body += '<div style="display:grid;gap:14px">';
  body += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">';
  body += '<div>';
  body += '<div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Immutable user id</div>';
  body += '<code style="display:block;background:var(--surface);padding:10px 12px;border-radius:8px;border:1px solid var(--border);font-size:11px;word-break:break-word">' + esc(profileId) + '</code>';
  body += '</div>';
  body += '<div>';
  body += '<div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Workspace</div>';
  body += '<div style="background:var(--surface);padding:10px 12px;border-radius:8px;border:1px solid var(--border);font-size:12px">' + esc(workspaceName) + '</div>';
  body += '</div>';
  body += '</div>';
  body += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">';
  body += '<div>';
  body += '<div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Immutable username</div>';
  body += '<div style="background:var(--surface);padding:10px 12px;border-radius:8px;border:1px solid var(--border);font-size:12px">' + esc(profileUsername) + '</div>';
  body += '</div>';
  body += '<div>';
  body += '<label style="display:block;font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Display name</label>';
  body += '<input id="profile-display-name" value="' + esc(profileName) + '" style="width:100%;background:var(--surface);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px 12px;font-size:12px;font-family:var(--font);outline:none">';
  body += '<div style="font-size:10px;color:var(--muted);margin-top:6px">Name must stay unique inside the workspace.</div>';
  body += '</div>';
  body += '</div>';
  body += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">';
  body += '<div>';
  body += '<label style="display:block;font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">New password</label>';
  body += '<input id="profile-password" type="password" placeholder="Leave blank to keep the current password" style="width:100%;background:var(--surface);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px 12px;font-size:12px;font-family:var(--font);outline:none">';
  body += '<div style="font-size:10px;color:var(--muted);margin-top:6px">A filled password field updates the stored personal password.</div>';
  body += '</div>';
  body += '<div>';
  body += '<label style="display:block;font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Telegram User ID</label>';
  body += '<input id="profile-telegram-id" type="number" value="' + esc(profile?.telegram_user_id || '') + '" placeholder="e.g. 123456789" style="width:100%;background:var(--surface);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:10px 12px;font-size:12px;font-family:var(--font);outline:none">';
  body += '<div style="font-size:10px;color:var(--muted);margin-top:6px">Reserved for compatible external Telegram notification integrations; no Telegram bridge is included in this repository.</div>';
  body += '</div>';
  body += '</div>';

  body += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">';
  body += '<div>';
  body += '<div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Auth sessions</div>';
  body += '<div style="background:var(--surface);padding:10px 12px;border-radius:8px;border:1px solid var(--border);font-size:12px">' + esc(String(sessionCount)) + '</div>';
  body += '</div>';
  body += '<div>';
  body += '<div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Last login</div>';
  body += '<div style="background:var(--surface);padding:10px 12px;border-radius:8px;border:1px solid var(--border);font-size:12px">' + esc(lastLogin) + '</div>';
  body += '</div>';
  body += '</div>';

  body += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">';
  body += '<div>';
  body += '<div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">Owned agents</div>';
  body += '<div style="background:var(--surface);padding:10px 12px;border-radius:8px;border:1px solid var(--border);font-size:12px">' + esc(String(profile?.agent_count ?? agents.length)) + '</div>';
  body += '</div>';
  body += '<div></div>';
  body += '</div>';
  body += '</div>';
  body += '<div id="profile-settings-status" style="min-height:18px;font-size:11px;color:' + toneColor + '">' + esc(feedback.message || '') + '</div>';
  if (feedback.token) {
    body += '<div style="background:rgba(15,23,42,0.65);border:1px solid var(--border);border-radius:10px;padding:12px">';
    body += '<div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px">Reissued token for ' + esc(feedback.agent_name || feedback.agent_id || 'agent') + '</div>';
    body += '<code style="display:block;background:var(--surface);padding:10px 12px;border-radius:8px;border:1px solid var(--border);font-size:11px;word-break:break-word">' + esc(feedback.token) + '</code>';
    body += '<div style="font-size:10px;color:var(--muted);margin-top:8px">The new token is shown once here and was also delivered to the agent inbox as a private security notice.</div>';
    body += '</div>';
  }
  body += '<div>';
  body += '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">';
  body += '<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--muted)">Auth sessions</div>';
  body += '<button type="button" class="hdr-btn" onclick="revokeOtherHumanSessions()">Logout other sessions</button>';
  body += '</div>';
  body += renderHumanSessionsList(sessionPayload);
  body += '</div>';
  body += '<div>';
  body += '<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--muted);margin-bottom:8px">Owned agents</div>';
  body += renderOwnedAgentsList(agents);
  body += '</div>';
  body += '<div style="display:flex;justify-content:flex-end;gap:8px">';
  body += '<button type="button" class="hdr-btn" onclick="closeModal()">Close</button>';
  body += '<button type="button" class="msg-btn" onclick="saveProfileSettings()">Save changes</button>';
  body += '</div>';
  body += '</div>';
  document.getElementById('modal-body').innerHTML = body;
}

async function openProfileSettings(forceReload = false) {
  closeProfileMenu();
  if (!TOKEN) {
    openAuthShell('human-login');
    return;
  }
  if ((AUTH_STATE?.actor_type || 'human') !== 'human') {
    openModal('Profile settings', '<div class="empty">Only authenticated human profiles can be edited in the dashboard.</div>');
    return;
  }
  openModal('Profile settings', '<div class="empty">Loading profile...</div>');
  try {
    const [profile] = await Promise.all([loadHumanProfile(forceReload), loadHumanSessions(forceReload)]);
    renderProfileSettingsModal(profile);
  } catch (err) {
    document.getElementById('modal-body').innerHTML = '<div class="empty">Failed to load profile: ' + esc(err.message || 'unknown error') + '</div>';
  }
}

async function saveProfileSettings() {
  const nextName = String(document.getElementById('profile-display-name')?.value || '').trim();
  const nextPassword = String(document.getElementById('profile-password')?.value || '').trim();
  const tgValue = document.getElementById('profile-telegram-id')?.value || '';
  const nextTelegramID = tgValue ? parseInt(tgValue, 10) : 0;

  const statusEl = document.getElementById('profile-settings-status');
  if (!nextName) {
    if (statusEl) {
      statusEl.textContent = 'Display name is required.';
      statusEl.style.color = 'var(--red)';
    }
    return;
  }
  const currentName = HUMAN_PROFILE_STATE?.display_name || currentProfileName();
  const currentTgId = HUMAN_PROFILE_STATE?.telegram_user_id || 0;
  if (nextName === currentName && !nextPassword && nextTelegramID === currentTgId) {
    if (statusEl) {
      statusEl.textContent = 'Nothing changed yet.';
      statusEl.style.color = 'var(--muted)';
    }
    return;
  }
  if (statusEl) {
    statusEl.textContent = 'Saving profile...';
    statusEl.style.color = 'var(--muted)';
  }
  try {
    const payload = {
      display_name: nextName,
      password: nextPassword,
      telegram_user_id: nextTelegramID
    };
    const result = await apiPost(HUMAN_PROFILE_API + '/update', payload);
    HUMAN_PROFILE_STATE = result.profile || result;
    syncAuthStateFromProfile(HUMAN_PROFILE_STATE);
    renderProfileSettingsModal(HUMAN_PROFILE_STATE, {message: 'Profile updated.', tone: 'success'});
    toast('Profile updated');
  } catch (err) {
    if (statusEl) {
      statusEl.textContent = err.message || 'Save failed';
      statusEl.style.color = 'var(--red)';
    }
  }
}

async function revokeHumanSession(tokenID) {
  if (!tokenID) return;
  const statusEl = document.getElementById('profile-settings-status');
  if (statusEl) {
    statusEl.textContent = 'Revoking session...';
    statusEl.style.color = 'var(--muted)';
  }
  try {
    await apiPost(HUMAN_SESSIONS_API + '/revoke', {token_id: tokenID});
    HUMAN_SESSIONS_STATE = null;
    const [profile] = await Promise.all([loadHumanProfile(true), loadHumanSessions(true)]);
    renderProfileSettingsModal(profile, {message: 'Session revoked.', tone: 'success'});
    toast('Session revoked');
  } catch (err) {
    if (statusEl) {
      statusEl.textContent = err.message || 'Revoke failed';
      statusEl.style.color = 'var(--red)';
    }
  }
}

async function revokeOtherHumanSessions() {
  const statusEl = document.getElementById('profile-settings-status');
  if (statusEl) {
    statusEl.textContent = 'Revoking other sessions...';
    statusEl.style.color = 'var(--muted)';
  }
  try {
    await apiPost(HUMAN_SESSIONS_API + '/revoke', {all_other_sessions: true});
    HUMAN_SESSIONS_STATE = null;
    const [profile] = await Promise.all([loadHumanProfile(true), loadHumanSessions(true)]);
    renderProfileSettingsModal(profile, {message: 'Other sessions revoked.', tone: 'success'});
    toast('Other sessions revoked');
  } catch (err) {
    if (statusEl) {
      statusEl.textContent = err.message || 'Revoke failed';
      statusEl.style.color = 'var(--red)';
    }
  }
}

async function rotateOwnedAgentToken(agentID) {
  const statusEl = document.getElementById('profile-settings-status');
  if (!agentID) return;
  if (statusEl) {
    statusEl.textContent = 'Rotating agent token...';
    statusEl.style.color = 'var(--muted)';
  }
  try {
    const result = await apiPost(AGENT_TOKEN_ROTATE_API, {agent_id: agentID});
    try {
      await navigator.clipboard?.writeText(result.access_token || result.token || '');
    } catch (copyErr) {}
    HUMAN_PROFILE_STATE = null;
    const [profile] = await Promise.all([loadHumanProfile(true), loadHumanSessions(false)]);
    renderProfileSettingsModal(profile, {
      message: 'Agent token rotated and sent to the agent inbox.',
      tone: 'success',
      token: result.access_token || result.token || '',
      agent_id: result.agent_id || agentID,
      agent_name: result.display_name || agentID
    });
    toast('Agent token rotated');
  } catch (err) {
    if (statusEl) {
      statusEl.textContent = err.message || 'Rotate failed';
      statusEl.style.color = 'var(--red)';
    }
  }
}

function switchAuthMode(mode) {
  authMode = mode;
  document.getElementById('human-login-tab')?.classList.toggle('active', mode === 'human-login');
  document.getElementById('human-register-tab')?.classList.toggle('active', mode === 'human-register');
  document.getElementById('human-login-form')?.classList.toggle('active', mode === 'human-login');
  document.getElementById('human-register-form')?.classList.toggle('active', mode === 'human-register');
  fillAuthWorkspaceDefaults();
}

async function apiPost(url, payload, useAuth = true, logoutOnUnauthorized = true) {
  const headers = {'Content-Type':'application/json'};
  if (useAuth && TOKEN) {
    headers.Authorization = 'Bearer ' + TOKEN;
  }
  const res = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    if (useAuth && logoutOnUnauthorized && (res.status === 401 || res.status === 403)) {
      logout({revokeCurrentSession: false});
    }
    const err = data && (data.error || data.message) ? (data.error?.message || data.message) : ('HTTP ' + res.status);
    throw new Error(err);
  }
  return data;
}

function updateWorkspaceFromAuth(result) {
  const workspaceId = result.workspace_id || result.workspaceId || WS_ID;
  const accessToken = result.access_token || result.token || result.api_token;
  if (!accessToken) {
    throw new Error('Auth endpoint did not return a token');
  }
  const actorType = result.actor_type || result.kind || (result.agent_id ? 'agent' : 'human');
  const actorId = result.actor_id || result.user_id || result.agent_id || result.subject_id || '';
  HUMAN_PROFILE_STATE = null;
  HUMAN_SESSIONS_STATE = null;
  persistAuthState({
    access_token: accessToken,
    actor_type: actorType,
    actor_id: actorId,
    actor_name: result.actor_name || result.name || result.display_name || result.workspace_name || '',
    user_id: result.user_id || (actorType === 'human' ? actorId : ''),
    username: result.username || result.login_name || '',
    display_name: result.display_name || result.actor_name || result.name || '',
    workspace_id: workspaceId,
    workspace_name: result.workspace_name || workspaceId,
    source: 'auth'
  });
}

async function rpc(method, params = {}) {
  if (!TOKEN) {
    throw new Error('Authentication required');
  }
  const res = await fetch(API, {
    method: 'POST',
    headers: {'Content-Type':'application/json','Authorization':'Bearer '+TOKEN},
    body: JSON.stringify({jsonrpc:'2.0',id:String(++idCounter),method,params})
  });
  if (res.status === 401 || res.status === 403) {
    logout({revokeCurrentSession: false});
    throw new Error('Authentication expired or invalid');
  }
  const data = await res.json().catch(() => ({}));
  if (data.error) throw new Error(data.error.message);
  return data.result;
}

function authorityReferenceMs(authority) {
  const ref = authority && authority.reference_at ? Date.parse(String(authority.reference_at || '')) : NaN;
  return Number.isFinite(ref) ? ref : Date.now();
}

// Mirrors server-side computeIsOnline: an agent is online if its last heartbeat
// is within 15 minutes. Recomputing this on the client (instead of trusting the
// server's is_online snapshot) lets presence decay reactively between refreshes.
const AGENT_ONLINE_WINDOW_MS = 15 * 60 * 1000;
function clientIsOnline(lastSeenAt) {
  if (!lastSeenAt) return false;
  const t = Date.parse(String(lastSeenAt));
  if (!Number.isFinite(t)) return false;
  return (Date.now() - t) < AGENT_ONLINE_WINDOW_MS;
}

function timeAgo(ts, authority) {
  if (!ts) return 'never';
  const d = new Date(ts);
  const diff = (authorityReferenceMs(authority) - d.getTime()) / 1000;
  const exact = d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',hour12:false});
  let rel;
  if (diff < 0) rel = 'just now';
  else if (diff < 60) rel = Math.floor(diff) + 's ago';
  else if (diff < 3600) rel = Math.floor(diff/60) + 'm ago';
  else if (diff < 86400) rel = Math.floor(diff/3600) + 'h ago';
  else rel = Math.floor(diff/86400) + 'd ago';
  return rel + ' · ' + exact;
}

function esc(s) {
  if (s === undefined || s === null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Dynamic values must never be serialized into inline event-handler source.
// Renderers register closures here and emit only an opaque, generated data
// attribute. A single delegated listener invokes the closure with the original
// values still captured in JavaScript memory.
let dashboardActionSequence = 0;
const dashboardActions = new Map();
let dashboardActionPruneQueued = false;

function dashboardAction(callback) {
  if (typeof callback !== 'function') return '';
  const actionID = 'dashboard-action-' + (++dashboardActionSequence).toString(36);
  dashboardActions.set(actionID, callback);
  if (!dashboardActionPruneQueued) {
    dashboardActionPruneQueued = true;
    const enqueue = typeof queueMicrotask === 'function'
      ? queueMicrotask
      : (fn) => Promise.resolve().then(fn);
    enqueue(() => {
      dashboardActionPruneQueued = false;
      const live = new Set();
      document.querySelectorAll('[data-dashboard-action]').forEach((element) => {
        const id = element.getAttribute('data-dashboard-action');
        if (id) live.add(id);
      });
      dashboardActions.forEach((_, id) => {
        if (!live.has(id)) dashboardActions.delete(id);
      });
    });
  }
  return 'data-dashboard-action="' + actionID + '"';
}

document.addEventListener('click', (event) => {
  const target = event.target instanceof Element
    ? event.target.closest('[data-dashboard-action]')
    : null;
  if (!target) return;
  const callback = dashboardActions.get(target.getAttribute('data-dashboard-action') || '');
  if (typeof callback === 'function') callback(event, target);
});

function bindTaskDetailElements(root, tasks, selector, beforeOpen) {
  if (!root) return;
  const items = Array.isArray(tasks) ? tasks : [];
  root.querySelectorAll(selector).forEach((element, index) => {
    const task = items[index] || {};
    element.addEventListener('click', (event) => {
      if (event) event.preventDefault();
      const taskId = String(task.task_id || '').trim();
      if (!taskId) return;
      if (typeof beforeOpen === 'function') beforeOpen();
      showTaskDetail(taskId, String(task.title || taskId));
    });
  });
}

function renderMemoryPacketBoundarySummary(summary) {
  if (!summary) return 'No packet-local boundary summary visible.';
  const lines = [];
  const push = (label, value) => {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0) return;
    lines.push(label + ' = ' + n);
  };
  push('hard_constraints', summary.hard_constraint_count);
  push('accepted_decisions', summary.accepted_decision_count);
  push('decision_records', summary.decision_record_count);
  push('active_blockers', summary.active_blocker_count);
  push('blocker_hypotheses', summary.blocker_hypothesis_count);
  push('dissent_claims', summary.dissent_claim_count);
  push('archived_dissent_claims', summary.archived_dissent_claim_count);
  push('alternative_branches', summary.alternative_branch_count);
  push('archived_alternative_branches', summary.archived_alternative_branch_count);
  push('procedural_claims', summary.procedural_claim_count);
  push('identity_memories', summary.identity_memory_count);
  push('trace_context', summary.trace_context_count);
  return lines.length ? lines.map(esc).join('<br>') : 'No packet-local boundary counts are visible on the current shell packet.';
}

function renderMemoryPacketBasisSummary(summary) {
  if (!summary) return 'No packet-local basis summary visible.';
  const lines = [];
  const push = (label, value) => {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n <= 0) return;
    lines.push(label + ' = ' + n);
  };
  push('total_basis_refs', summary.total_ref_count);
  push('runtime_events', summary.runtime_event_ref_count);
  push('episode_packs', summary.episode_pack_ref_count);
  push('knowledge_claims', summary.knowledge_claim_ref_count);
  push('workspace_memories', summary.workspace_memory_ref_count);
  push('coordination_basis', summary.coordination_basis_count);
  push('differential_basis', summary.differential_basis_count);
  push('procedural_basis', summary.procedural_basis_count);
  push('identity_basis', summary.identity_basis_count);
  push('recent_trace_basis', summary.recent_trace_basis_count);
  return lines.length ? lines.map(esc).join('<br>') : 'No packet-local basis mix is visible on the current shell packet.';
}

// Bounded, coalescing toast manager — during a real run SSE fires many events,
// so cap how many show at once, merge identical messages into a ×N counter,
// keep them compact, and auto-dismiss quickly.
const TOAST_MAX = 4;
const TOAST_TTL = 3000;
const _toastActive = new Map(); // message -> {el, countEl, count, timer}
function _toastArm(msg, rec) {
  if (rec.timer) clearTimeout(rec.timer);
  rec.timer = setTimeout(() => _toastDismiss(msg), TOAST_TTL);
}
function _toastDismiss(msg) {
  const rec = _toastActive.get(msg);
  if (!rec) return;
  _toastActive.delete(msg);
  if (rec.timer) clearTimeout(rec.timer);
  rec.el.classList.add('out');
  setTimeout(() => { try { rec.el.remove(); } catch (e) {} }, 250);
}
// Coarse category for an event type, so live events group into one toast per
// kind (task / session / tool / ...) instead of one per distinct message.
function _toastCategory(type) {
  const t = String(type || '');
  if (!t) return '';
  if (t.indexOf('agent.request') === 0 || t === 'agent.response') return 'ask';
  if (t.indexOf('tool.call') === 0) return 'tool';
  if (t.indexOf('execution') === 0 || t.indexOf('workspace.execution') === 0) return 'execution';
  if (t.indexOf('agent.session') === 0 || t.indexOf('session') !== -1) return 'session';
  if (t.indexOf('task') !== -1 || t.indexOf('node.') === 0) return 'task';
  if (t.indexOf('tension') === 0) return 'tension';
  if (t.indexOf('cluster.') === 0 || t.indexOf('control') !== -1 || t.indexOf('corridor') !== -1) return 'control';
  if (t.indexOf('memory') !== -1) return 'memory';
  if (t.indexOf('project.') === 0) return 'project';
  if (t.indexOf('vault.') === 0) return 'vault';
  if (t.indexOf('agent.') === 0) return 'agent';
  return 'event';
}
function toast(msg, type) {
  msg = String(msg == null ? '' : msg).trim();
  if (!msg) return;
  const container = document.getElementById('toasts');
  if (!container) return;
  const cat = _toastCategory(type);
  const key = cat ? ('kind:' + cat) : ('msg:' + msg);
  const existing = _toastActive.get(key);
  if (existing) { // group: bump count, show the latest message, reset timer
    existing.count += 1;
    existing.msgEl.textContent = msg;
    existing.countEl.textContent = '×' + existing.count;
    existing.countEl.style.display = '';
    container.appendChild(existing.el);
    _toastArm(key, existing);
    return;
  }
  while (_toastActive.size >= TOAST_MAX) { // evict oldest
    _toastDismiss(_toastActive.keys().next().value);
  }
  const el = document.createElement('div');
  el.className = 'toast';
  if (cat) {
    const k = document.createElement('span');
    k.className = 'toast-kind ' + cat;
    k.textContent = cat === 'ask' ? 'ASK' : cat;
    el.appendChild(k);
  }
  const m = document.createElement('span'); m.className = 'toast-msg'; m.textContent = msg;
  const c = document.createElement('span'); c.className = 'toast-count'; c.style.display = 'none';
  el.appendChild(m); el.appendChild(c);
  el.addEventListener('click', () => _toastDismiss(key));
  container.appendChild(el);
  const rec = { el, msgEl: m, countEl: c, count: 1, timer: null };
  _toastActive.set(key, rec);
  _toastArm(key, rec);
}

function fillAuthWorkspaceDefaults() {
  const workspace = WS_ID || 'rhizome-main';
  ['human-login-workspace','human-register-workspace','agent-register-workspace','ws-security-id','ws-security-name'].forEach(id => {
    const el = document.getElementById(id);
    if (el && !el.value) el.value = workspace;
  });
  const hostEl = document.getElementById('agent-register-host');
  if (hostEl && !hostEl.value) hostEl.value = window.location.origin;
}

async function submitHumanLogin() {
  const statusEl = document.getElementById('human-auth-status');
  const workspace = document.getElementById('human-login-workspace').value.trim();
  const username = document.getElementById('human-login-name').value.trim();
  const password = document.getElementById('human-login-password').value;
  if (!workspace || !username || !password) {
    statusEl.textContent = 'Workspace, username, and password are required.';
    statusEl.style.color = 'var(--red)';
    return;
  }
  try {
    statusEl.textContent = 'Signing in...';
    statusEl.style.color = 'var(--muted)';
    const result = await apiPost(AUTH_API + '/human/login', {
      workspace_id: workspace,
      workspace_name: workspace,
      username,
      password
    }, false);
    updateWorkspaceFromAuth(result);
    fillAuthWorkspaceDefaults();
    statusEl.textContent = 'Signed in.';
    statusEl.style.color = 'var(--green)';
    startApp();
  } catch (err) {
    statusEl.textContent = err.message || 'Login failed';
    statusEl.style.color = 'var(--red)';
  }
}

async function submitHumanRegister() {
  const statusEl = document.getElementById('human-auth-status');
  const workspace = document.getElementById('human-register-workspace').value.trim();
  const workspacePassword = document.getElementById('human-register-password').value;
  const username = document.getElementById('human-register-name').value.trim();
  const displayName = document.getElementById('human-register-display-name').value.trim();
  const password = document.getElementById('human-register-personal-password').value;
  if (!workspace || !workspacePassword || !username || !displayName || !password) {
    statusEl.textContent = 'Workspace, workspace password, username, display name, and personal password are required.';
    statusEl.style.color = 'var(--red)';
    return;
  }
  try {
    statusEl.textContent = 'Creating human account...';
    statusEl.style.color = 'var(--muted)';
    const result = await apiPost(AUTH_API + '/human/register', {
      workspace_id: workspace,
      workspace_name: workspace,
      workspace_password: workspacePassword,
      username,
      display_name: displayName,
      password
    }, false);
    updateWorkspaceFromAuth(result);
    fillAuthWorkspaceDefaults();
    statusEl.textContent = 'Human account created and signed in.';
    statusEl.style.color = 'var(--green)';
    startApp();
  } catch (err) {
    statusEl.textContent = err.message || 'Registration failed';
    statusEl.style.color = 'var(--red)';
  }
}

async function submitAgentRegister() {
  const statusEl = document.getElementById('agent-auth-status');
  const workspace = document.getElementById('agent-register-workspace').value.trim();
  const workspacePassword = document.getElementById('agent-register-password').value;
  const agentName = document.getElementById('agent-register-name').value.trim();
  const hostUrl = document.getElementById('agent-register-host').value.trim();
  const notes = document.getElementById('agent-register-notes').value.trim();
  const ownerUserID = AUTH_STATE?.actor_type === 'human' ? currentProfileId() : '';
  if (!workspace || !workspacePassword || !agentName || !hostUrl) {
    statusEl.textContent = 'Workspace, workspace password, agent name, and host URL are required.';
    statusEl.style.color = 'var(--red)';
    return;
  }
  try {
    statusEl.textContent = 'Issuing token...';
    statusEl.style.color = 'var(--muted)';
    const result = await apiPost(AUTH_API + '/agent/register', {
      workspace_id: workspace,
      workspace_name: workspace,
      workspace_password: workspacePassword,
      agent_name: agentName,
      owner_user_id: ownerUserID,
      host_url: hostUrl,
      notes
    }, false);
    const token = result.access_token || result.token || '';
    if (!token) {
      throw new Error('Backend did not return an agent token');
    }
    const box = document.getElementById('agent-token-box');
    const valueEl = document.getElementById('agent-token-value');
    const helpEl = document.getElementById('agent-token-help');
    valueEl.textContent = token;
    helpEl.textContent = 'Store this token on the agent host. It is shown once in the UI.';
    box.classList.add('active');
    statusEl.textContent = 'Agent token issued.';
    statusEl.style.color = 'var(--green)';
    if (ownerUserID) {
      HUMAN_PROFILE_STATE = null;
      loadHumanProfile(true).catch(() => {});
    }
    try {
      await navigator.clipboard?.writeText(token);
    } catch (copyErr) {}
  } catch (err) {
    statusEl.textContent = err.message || 'Agent registration failed';
    statusEl.style.color = 'var(--red)';
  }
}

async function copyAgentToken() {
  const token = document.getElementById('agent-token-value')?.textContent || '';
  if (!token) return;
  try {
    await navigator.clipboard?.writeText(token);
  } catch (err) {}
  toast('Agent token copied');
}

async function loadWorkspaceSecurity() {
  const statusEl = document.getElementById('workspace-security-status');
  if (!TOKEN) return;
  try {
    const res = await apiPost(SECURITY_API + '/get', {
      workspace_id: WS_ID,
      workspace_name: WS_ID
    });
    const settings = res.settings || res.workspace || res;
    document.getElementById('ws-security-id').value = settings.workspace_id || WS_ID;
    document.getElementById('ws-security-name').value = settings.workspace_name || settings.title || WS_ID;
    document.getElementById('ws-security-password').value = '';
    document.getElementById('ws-security-description').value = settings.description || '';
    document.getElementById('security-settings-badge').textContent = settings.status || 'ready';
    if (statusEl) {
      statusEl.textContent = 'Workspace settings loaded.';
      statusEl.style.color = 'var(--green)';
    }
  } catch (err) {
    document.getElementById('ws-security-id').value = WS_ID;
    document.getElementById('ws-security-name').value = WS_ID;
    document.getElementById('ws-security-password').value = '';
    if (statusEl) {
      statusEl.textContent = err.message || 'Failed to load workspace security';
      statusEl.style.color = 'var(--red)';
    }
  }
}

async function saveWorkspaceSecurity() {
  const statusEl = document.getElementById('workspace-security-status');
  try {
    statusEl.textContent = 'Saving workspace security...';
    statusEl.style.color = 'var(--muted)';
    const result = await apiPost(SECURITY_API + '/update', {
      workspace_id: WS_ID,
      workspace_name: document.getElementById('ws-security-name').value.trim() || WS_ID,
      workspace_password: document.getElementById('ws-security-password').value,
      description: document.getElementById('ws-security-description').value.trim()
    });
    const nextWorkspace = result.workspace_id || result.workspace_name;
    if (nextWorkspace) {
      WS_ID = nextWorkspace;
      localStorage.setItem('rhizome_ws_id', WS_ID);
      if (AUTH_STATE) {
        AUTH_STATE.workspace_id = WS_ID;
        AUTH_STATE.workspace_name = result.workspace_name || AUTH_STATE.workspace_name || WS_ID;
        localStorage.setItem(AUTH_STATE_KEY, JSON.stringify(AUTH_STATE));
      }
      syncWorkspaceInputs();
    }
    statusEl.textContent = 'Workspace security saved.';
    statusEl.style.color = 'var(--green)';
    document.getElementById('ws-security-password').value = '';
    await loadSecurityLogs();
  } catch (err) {
    statusEl.textContent = err.message || 'Save failed';
    statusEl.style.color = 'var(--red)';
  }
}

function renderSecurityLog(entry) {
  const success = (entry.status || 'ok').toLowerCase() === 'ok' || entry.success !== false;
  const label = entry.event_type || entry.action || 'event';
  const actor = entry.actor_name || entry.actor || entry.actor_id || entry.identity_name || 'system';
  const ip = entry.ip_address || entry.ip || 'unknown ip';
  const ua = entry.user_agent || entry.userAgent || '';
  const timestamp = timeAgo(entry.created_at || entry.timestamp || entry.ts);
  return '<div class="security-log">' +
    '<div class="security-log-head">' +
      '<div><strong>'+esc(label)+'</strong><div class="security-log-meta">'+esc(actor)+' · '+esc(ip)+'</div></div>' +
      '<span class="security-log-badge '+(success ? 'ok' : 'fail')+'">'+(success ? 'ok' : 'failed')+'</span>' +
    '</div>' +
    '<div class="security-log-meta">'+esc(timestamp)+(ua ? ' · '+esc(ua) : '')+'</div>' +
  '</div>';
}

async function loadSecurityLogs() {
  if (!TOKEN) return;
  const listEl = document.getElementById('security-log-list');
  if (!listEl) return;
  try {
    const res = await apiPost(SECURITY_LOGS_API, {
      workspace_id: WS_ID,
      limit: 50
    });
    const items = res.items || res.logs || res.events || [];
    document.getElementById('security-log-count').textContent = items.length;
    if (!items.length) {
      listEl.innerHTML = '<div class="empty">No security logs yet.</div>';
      return;
    }
    listEl.innerHTML = items.map(renderSecurityLog).join('');
  } catch (err) {
    listEl.innerHTML = '<div class="empty">'+esc(err.message || 'Failed to load security logs')+'</div>';
  }
}

function startApp() {
  if (appStarted || !TOKEN) {
    syncAuthChrome();
    fillAuthWorkspaceDefaults();
    return;
  }
  appStarted = true;
  syncAuthChrome();
  fillAuthWorkspaceDefaults();
  if ((AUTH_STATE?.actor_type || 'human') === 'human') {
    loadHumanProfile(true).catch(err => console.error('profile preload', err));
  }
  refresh().catch(err => console.error('initial refresh', err));
  loadRuntimeHealth().catch(err => console.error('initial runtime health', err));
  connectSSE();
  if (!refreshTimer) {
    refreshTimer = setInterval(() => {
      if (TOKEN) refresh().catch(err => console.error('refresh', err));
    }, 60000);
  }
  if (!runtimeHealthRefreshTimer) {
    runtimeHealthRefreshTimer = setInterval(() => {
      if (TOKEN) loadRuntimeHealth().catch(err => console.error('runtime health refresh', err));
    }, 120000);
  }
  if (!presenceTickTimer) {
    // Recompute agent presence + "last seen" labels from cache between polls, so
    // an agent going quiet decays to offline without waiting for the 60s refresh.
    presenceTickTimer = setInterval(() => {
      if (TOKEN && !document.hidden) renderAgents();
    }, 20000);
  }
}

// Tabs
function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.id === 'panel-' + name));
  if (name === 'instrumentation') loadInstrumentation();
  if (name === 'tensions') loadTensions();
  if (name === 'security') loadWorkspaceSecurity();
  if (name === 'logs') resetRpcLogs();
}

// ── Agents ──
let agentsCache = [];
let humansCache = [];
let sessionsCache = [];
let memoryCache = [];
let memorySurfaceTimeAuthority = null;
let memorySearchTimer = null;
let memoryContextFilters = {agent_id:'', session_id:'', task_id:''};
let memoryComposerDraft = null;
let operatorQueueCache = [];
let operatorQueueTimeAuthority = null;
let claimsCache = [];
let executionRunsCache = [];
let executionRunDetailCache = {};
let policiesCache = [];
let runtimeEventsCache = [];
let instrumentationReportCache = null;
let instrumentationClustersCache = [];
let instrumentationSnapshotEventCache = null;
let tensionRefreshCache = null;
let tensionRuntimeEventCache = null;
let tensionsCache = [];
let tensionsUniverseCache = [];
let tensionFrontierCache = [];
let tensionDetailCache = {};
let tensionSurfaceTimeAuthority = null;
let controlPolicyReportCache = null;
let controlPolicyClustersCache = [];
let controlPolicyDetailCache = {};
let controlPolicySnapshotEventCache = null;
let unifiedControlSnapshotEventCache = null;
let corridorReadinessReportCache = null;
let corridorReadinessDetailCache = {};
let corridorReadinessSnapshotEventCache = null;
let corridorAuthorityReportCache = null;
let corridorAuthorityDetailCache = {};
let corridorOwnershipReportCache = null;
let corridorOwnershipDetailCache = {};
let corridorOwnershipSnapshotEventCache = null;
let corridorFitReportCache = null;
let corridorFitDetailCache = {};
let corridorFitSnapshotEventCache = null;
let controlStateReportCache = null;
let controlStateDetailCache = {};
let controlStateSnapshotEventCache = null;
let rspCapabilityFlagsCache = null;
let rspBeliefReportCache = null;
let rspForecastReportCache = null;
let rspTelemetryDumpCache = null;
let controlPolicySelectedClusterID = '';
let controlPolicyLoadSeq = 0;
let controlPolicyDetailSeq = 0;
let compactionCandidatesCache = [];
let compactionSnapshotsCache = [];
let operatorInboxCache = [];
let replayReportCache = null;
let replayEvaluationCache = null;
let runtimeHealthCache = null;
let claimSearchTimer = null;

function splitCsv(value) {
  return String(value || '').split(',').map(v => String(v || '').trim()).filter(Boolean);
}

function boolPromptDefault(value, fallback = false) {
  const normalized = String(value === undefined || value === null || value === '' ? fallback : value).trim().toLowerCase();
  return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'y';
}

function parseJSONOrEmpty(value, fallback) {
  const text = String(value || '').trim();
  if (!text) return fallback;
  try { return JSON.parse(text); } catch (e) { return fallback; }
}

function dashboardGeneratedID(prefix) {
  return prefix + '-' + Date.now().toString(36);
}

function sessionBadgeClass(status) {
  const normalized = String(status || 'ACTIVE').toUpperCase();
  if (normalized === 'BLOCKED') return 'session-pill BLOCKED';
  if (normalized === 'WAITING_DECISION' || normalized === 'HANDOFF_PENDING') return 'session-pill ' + normalized;
  if (normalized === 'ENDED' || normalized === 'COMPLETED' || normalized === 'FAILED') return 'session-pill ' + normalized;
  return 'session-pill ACTIVE';
}

function sessionOwnerLabel(session) {
  const owner = agentsCache.find(a => a.agent_id === session.agent_id);
  return owner ? (owner.display_name || owner.agent_id) : (session.agent_id || 'unknown');
}

function sessionAttentionCount(items) {
  return items.filter(s => {
    const status = String(s.status || '').toUpperCase();
    return status === 'BLOCKED' || status === 'WAITING_DECISION' || status === 'HANDOFF_PENDING';
  }).length;
}

function sessionContextSummary(session) {
  const parts = [];
  if (session.task_id) parts.push('Task ' + session.task_id);
  if (session.decision_needed_from) parts.push('Decision from ' + session.decision_needed_from);
  if (session.handoff_to) parts.push('Handoff to ' + session.handoff_to);
  if (session.blocked_on && session.blocked_on.length) {
    const blocker = session.blocked_on[0] || {};
    parts.push('Blocked on ' + (blocker.detail || blocker.kind || 'blocker'));
  }
  if (session.keep_session_active === false) parts.push('Peers can stand down');
  if (session.keep_session_active === true && parts.length < 4) parts.push('Peers stay active');
  return parts.join(' · ');
}

function isQueueOpen(item) {
  return String(item && item.status || 'OPEN').toUpperCase() === 'OPEN';
}

function operatorQueueAuthorityFor(item, authority) {
  if (authority && authority.reference_at) return authority;
  if (item && item.time_authority && item.time_authority.reference_at) return item.time_authority;
  if (operatorQueueTimeAuthority && operatorQueueTimeAuthority.reference_at) return operatorQueueTimeAuthority;
  return null;
}

function tensionAuthorityFor(item, authority) {
  if (authority && authority.reference_at) return authority;
  if (item && item.time_authority && item.time_authority.reference_at) return item.time_authority;
  if (tensionSurfaceTimeAuthority && tensionSurfaceTimeAuthority.reference_at) return tensionSurfaceTimeAuthority;
  return null;
}

function isQueueOverdue(item, authority) {
  if (!item || !item.due_at || !isQueueOpen(item)) return false;
  const dueAtMs = Date.parse(String(item.due_at || ''));
  return Number.isFinite(dueAtMs) && dueAtMs < authorityReferenceMs(operatorQueueAuthorityFor(item, authority));
}

function queueMetaBadges(item, authority) {
  const parts = [];
  if (item && item.due_at) parts.push(isQueueOverdue(item, authority) ? 'OVERDUE' : ('due ' + item.due_at));
  if (item && item.escalation_count) parts.push('escalations ' + String(item.escalation_count));
  return parts;
}

function queueTaskLockEvidence(item, authority) {
  if (!item || !item.task_id) return '';
  const task = (_cachedTasks || []).find(candidate => candidate.task_id === item.task_id);
  if (!task) return '';
  const parts = [];
  const claimStatus = String(task.claim_status || task.status || '').trim();
  if (claimStatus) parts.push(claimStatus.toUpperCase());
  if (task.claim_agent_id) parts.push(task.claim_agent_id);
  if (task.claim_updated_at) parts.push('updated ' + timeAgo(task.claim_updated_at, authority));
  return parts.length ? ('task lock ' + parts.join(' | ')) : '';
}

function queueLifecycleEvidence(item, authority) {
  const parts = [];
  if (item && item.session_id) {
    parts.push(item.keep_session_active ? 'keep session active' : 'allow session to end');
  }
  if (item && item.last_escalated_at) {
    parts.push('last escalated ' + timeAgo(item.last_escalated_at, authority) + (item.last_escalated_by ? (' by ' + item.last_escalated_by) : ''));
  }
  const taskLock = queueTaskLockEvidence(item, authority);
  if (taskLock) parts.push(taskLock);
  return parts;
}

function queueRebaseFollowupPayload(item) {
  if (!item) return null;
  const queueKey = String(item.queue_key || '').trim().toLowerCase();
  const payload = parseJSON(item.payload_json);
  const nextAction = String(payload.next_action || '').trim().toLowerCase();
  const repairTensionID = String(payload.repair_tension_id || '').trim();
  const forkTensionID = String(payload.fork_tension_id || '').trim();
  if (queueKey.indexOf('tension_rebase_followup:') !== 0 && nextAction !== 'attempt_rebase' && !repairTensionID && !forkTensionID) {
    return null;
  }
  return {
    fork_tension_id: forkTensionID,
    repair_tension_id: repairTensionID,
    next_action: String(payload.next_action || '').trim(),
    rebase_plan_class: String(payload.rebase_plan_class || '').trim(),
    conflict_safe_class: String(payload.conflict_safe_class || '').trim(),
    alternative_patch: String(payload.alternative_patch || '').trim(),
    action_id: String(payload.action_id || '').trim(),
    action_status: String(payload.action_status || '').trim(),
    action_title: String(payload.action_title || '').trim(),
    action_assigned_to: String(payload.action_assigned_to || '').trim(),
    action_started_by: String(payload.action_started_by || '').trim(),
    action_paused_by: String(payload.action_paused_by || '').trim(),
    rebase_workflow_state: String(payload.rebase_workflow_state || '').trim(),
    rebase_workflow_step: String(payload.rebase_workflow_step || '').trim()
  };
}

function queueRebaseFollowupBadges(item) {
  const payload = queueRebaseFollowupPayload(item);
  if (!payload) return [];
  const parts = ['REBASE'];
  if (payload.rebase_workflow_state) parts.push(payload.rebase_workflow_state);
  if (payload.next_action) parts.push(payload.next_action);
  if (payload.rebase_plan_class) parts.push(payload.rebase_plan_class);
  if (payload.conflict_safe_class) parts.push(payload.conflict_safe_class);
  return parts;
}

function queueRebaseFollowupSummary(item) {
  const payload = queueRebaseFollowupPayload(item);
  if (!payload) return '';
  const plan = String(payload.rebase_plan_class || '').trim().replaceAll('_', ' ');
  const conflictClass = String(payload.conflict_safe_class || '').trim().replaceAll('_', ' ');
  const parts = ['Bounded overlap rebase follow-up'];
  if (payload.rebase_workflow_state) parts.push(String(payload.rebase_workflow_state).replaceAll('_', ' '));
  if (plan) parts.push(plan);
  if (conflictClass) parts.push(conflictClass);
  if (payload.action_id) parts.push('action ' + payload.action_id);
  if (payload.repair_tension_id) parts.push('repair ' + payload.repair_tension_id);
  return parts.join(' | ');
}

function findLinkedRebaseQueueForAction(actionId) {
  const normalized = String(actionId || '').trim();
  if (!normalized) return null;
  return (operatorQueueCache || []).find(item => {
    const payload = queueRebaseFollowupPayload(item);
    return payload && String(payload.action_id || '').trim() === normalized;
  }) || null;
}

function isClaimReviewStatus(status) {
  const normalized = String(status || '').toUpperCase();
  return normalized === 'REVIEW' || normalized === 'DISPUTED' || normalized === 'STALE';
}

function memoryBodyPreview(entry) {
  if (!entry || !entry.record) return '';
  const text = entry.record.summary || entry.record.body || '';
  if (!text) return '';
  return text.length > 140 ? text.substring(0, 140) + '...' : text;
}

function memoryBadge(kind, label) {
  return '<span class="memory-badge '+kind+'">'+esc(label)+'</span>';
}

async function loadRPCMethodCount(force = false) {
  if (_rpcMethodsLoaded && !force) return;
  try {
    const r = await rpc('rpc.methods.list', {});
    document.getElementById('s-rpc-methods').textContent = r.count || 0;
    _rpcMethodsLoaded = true;
  } catch(e) {
    console.error('loadRPCMethodCount', e);
  }
}

async function loadAgents() {
  try {
    const r = await rpc('workspace.agents.list', {workspace_id:WS_ID});
    agentsCache = r.agents || [];
    renderAgents();
  } catch(e) { console.error('loadAgents', e); }
}

// Pure render from agentsCache. Presence (dot, online count) and "last seen" are
// recomputed client-side on every call, so a periodic tick keeps them honest
// without a network round-trip. Safe to call when the agents panel is absent.
function renderAgents() {
  const agents = agentsCache || [];
  const countEl = document.getElementById('agents-count');
  if (countEl) countEl.textContent = agents.length;
  const sAgents = document.getElementById('s-agents');
  if (sAgents) sAgents.textContent = agents.length;
  const sOnline = document.getElementById('s-online');
  if (sOnline) sOnline.textContent = agents.filter(a => clientIsOnline(a.last_seen_at)).length;

  const el = document.getElementById('agents-list');
  if (!el) return;
  if (!agents.length) { el.innerHTML = '<div class="empty">No agents registered</div>'; return; }
  el.innerHTML = agents.map(a => {
    const online = clientIsOnline(a.last_seen_at);
    const pills = (a.active_tasks||[]).map(t =>
      '<span class="task-pill '+esc(t.claim_status)+'">'+esc(t.task_id)+'</span>').join('');
    const session = a.current_session;
    const sessionLine = session ? '<div class="agent-sub" style="margin-top:3px">'+
      '<span class="task-pill '+esc(session.status||'ACTIVE')+'" style="margin-right:4px">'+esc(session.status||'ACTIVE')+'</span>'+
      esc(session.summary||session.session_id||'active session')+
    '</div>' : '';
    return '<div class="agent-item" ' + dashboardAction(function(dashboardEvent){showAgentDetail((a.agent_id))}) + '>' +
      '<div class="agent-dot '+(online?'online':'offline')+'"></div>' +
      '<div class="agent-info">' +
        '<div class="agent-name">'+esc(a.display_name||a.agent_id)+'</div>' +
        '<div class="agent-sub">'+esc(a.agent_id)+'</div>' +
        sessionLine +
        (pills?'<div class="agent-task-pills">'+pills+'</div>':'') +
      '</div>' +
      '<div class="agent-right">' +
        '<span class="agent-role">'+esc(a.role||'agent')+'</span>' +
        '<span class="agent-seen">'+timeAgo(a.last_seen_at)+'</span>' +
      '</div>' +
    '</div>';
  }).join('');
}

async function loadHumans() {
  try {
    const r = await rpc('workspace.humans.list', {workspace_id:WS_ID});
    humansCache = r.humans || [];
  } catch(e) { console.error('loadHumans', e); }
}

function resolveAgentName(id) {
  if (!id) return '';
  const a = agentsCache.find(x => x.agent_id === id);
  if (a) return a.display_name || a.agent_id;
  const h = humansCache.find(x => x.user_id === id);
  if (h) return h.display_name || h.username || h.user_id;
  return id;
}

async function showAgentDetail(agentId) {
  const a = agentsCache.find(x => x.agent_id === agentId);
  if (!a) return;
  openModal(esc(a.display_name || a.agent_id), '<div class="empty">Loading...</div>');
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px">';
  html += '<div><strong>Agent ID</strong><br><code style="background:var(--surface);padding:2px 6px;border-radius:4px;font-size:11px">'+esc(a.agent_id)+'</code></div>';
  html += '<div><strong>Display Name</strong><br>'+esc(a.display_name)+'</div>';
  html += '<div><strong>Role</strong><br>'+esc(a.role)+'</div>';
  html += '<div><strong>Status</strong><br>'+(clientIsOnline(a.last_seen_at)?'Online':'Offline')+' ('+timeAgo(a.last_seen_at)+')</div>';
  html += '</div>';
  // Load profile
  try {
    const p = await rpc('agent.profile.get', {workspace_id:WS_ID, agent_id:agentId});
    if (p) {
      html += '<hr style="border-color:var(--border);margin:14px 0">';
      if (p.specialization) html += '<div style="margin-bottom:8px"><strong>Specialization:</strong> '+esc(p.specialization)+'</div>';
      if (p.bio) html += '<div style="margin-bottom:8px"><strong>Bio:</strong> '+esc(p.bio)+'</div>';
      if (p.tags && p.tags.length) html += '<div style="margin-bottom:8px"><strong>Tags:</strong> '+p.tags.map(t=>'<span class="task-tag" style="font-size:11px;padding:2px 8px">'+esc(t)+'</span>').join(' ')+'</div>';
      if (p.tools_access && p.tools_access.length) html += '<div style="margin-bottom:8px"><strong>Tools:</strong> '+p.tools_access.map(t=>'<code style="background:var(--surface);padding:2px 6px;border-radius:4px;font-size:11px">'+esc(t)+'</code>').join(' ')+'</div>';
      if (p.owner_name) html += '<div><strong>Owner:</strong> '+esc(p.owner_name)+' '+(p.owner_contact?esc(p.owner_contact):'')+'</div>';
    }
  } catch(e) {}
  // Load limit group for this agent
  try {
    const lg = _limitGroupsCache.find(g => (g.agents||[]).includes(agentId));
    if (lg) {
      html += '<hr style="border-color:var(--border);margin:14px 0">';
      html += '<strong>Limits ('+esc(lg.title)+')</strong>';
      html += '<div style="margin-top:8px">';
      if (lg.subscription_tier) html += '<span style="background:rgba(124,58,237,.15);color:var(--accent);padding:2px 8px;border-radius:6px;font-size:10px;font-weight:600;margin-right:6px">'+esc(lg.subscription_tier)+'</span>';
      if (lg.owner_name) html += '<span style="color:var(--muted);font-size:11px">Owner: '+esc(lg.owner_name)+'</span>';
      html += '</div>';
      html += renderLimitBars(lg.daily_remaining, lg.weekly_remaining);
      if (lg.last_reported_at) html += '<div style="font-size:10px;color:var(--muted);margin-top:4px">Last reported: '+timeAgo(lg.last_reported_at)+'</div>';
    }
  } catch(e) {}
  if (a.current_session) {
    const s = a.current_session;
    html += '<hr style="border-color:var(--border);margin:14px 0"><strong>Current Session:</strong>';
    html += '<div class="msg-item" style="margin-top:6px">';
    html += '<div><strong>Session ID:</strong> <code>'+esc(s.session_id)+'</code></div>';
    html += '<div><strong>Status:</strong> '+esc(s.status||'ACTIVE')+'</div>';
    if (s.task_id) html += '<div><strong>Task:</strong> '+esc(s.task_id)+'</div>';
    if (s.summary) html += '<div><strong>Summary:</strong> '+esc(s.summary)+'</div>';
    if (s.decision_needed_from) html += '<div><strong>Decision Needed From:</strong> '+esc(s.decision_needed_from)+'</div>';
    if (s.handoff_to) html += '<div><strong>Handoff To:</strong> '+esc(s.handoff_to)+'</div>';
    if (Array.isArray(s.blocked_on) && s.blocked_on.length) {
      html += '<div><strong>Blocked On:</strong> '+s.blocked_on.map(b => esc((b.kind||'')+': '+(b.detail||''))).join(', ')+'</div>';
    }
    if (typeof s.keep_session_active === 'boolean') html += '<div><strong>Keep Session Active:</strong> '+(s.keep_session_active?'true':'false')+'</div>';
    html += '<div><strong>Updated:</strong> '+timeAgo(s.updated_at||s.started_at)+'</div>';
    html += '</div>';
  }
  // Active tasks
  if (a.active_tasks && a.active_tasks.length) {
    html += '<hr style="border-color:var(--border);margin:14px 0"><strong>Active Tasks:</strong>';
    a.active_tasks.forEach(t => {
      html += '<div class="msg-item" style="margin-top:6px;cursor:pointer" ' + dashboardAction(function(dashboardEvent){closeModal();switchTab('tasks');setTimeout(()=>showTaskDetail((t.task_id),(t.task_id)),100)}) + '><strong>'+esc(t.task_id)+'</strong> <span class="task-pill '+esc(t.claim_status)+'">'+esc(t.claim_status)+'</span><br><span style="font-size:11px;color:var(--muted)">'+esc(t.summary||'')+'</span></div>';
    });
  }
  // State / Memory
  try {
    const stateR = await rpc('agent.state.list', {workspace_id:WS_ID, agent_id:agentId});
    const stateEntries = stateR.entries || stateR || [];
    if (stateEntries.length) {
      html += '<hr style="border-color:var(--border);margin:14px 0"><strong>Memory / State:</strong>';
      html += '<table class="state-table">';
      stateEntries.forEach(e => {
        let val = esc(e.value||'');
        if (val.length > 120) val = '<details><summary>'+val.substring(0,80)+'...</summary><div style="margin-top:4px;white-space:pre-wrap">'+val+'</div></details>';
        html += '<tr><td>'+esc(e.key)+'</td><td>'+val+'</td></tr>';
      });
      html += '</table>';
    }
  } catch(e) {}
  // Recent messages from/to this agent
  try {
    const mc = await rpc('workspace.messages.list', {workspace_id:WS_ID, limit:50});
    const agentMsgs = (mc.messages||[]).filter(m => m.from_agent_id===agentId || m.to_agent_id===agentId).slice(0,10);
    if (agentMsgs.length) {
      html += '<hr style="border-color:var(--border);margin:14px 0"><strong>Recent Messages:</strong>';
      agentMsgs.forEach(m => {
        const dir = m.from_agent_id===agentId ? '→ '+esc(m.to_agent_id||'all') : '← '+esc(m.from_agent_id);
        html += '<div style="padding:6px 0;border-bottom:1px solid var(--border);font-size:12px">';
        html += '<span style="color:var(--accent)">'+dir+'</span>';
        html += ' <span style="color:var(--muted);font-size:10px">'+timeAgo(m.created_at)+'</span>';
        html += '<div style="color:var(--text);margin-top:2px">'+esc((m.content||'').substring(0,200))+(m.content&&m.content.length>200?'...':'')+'</div>';
        html += '</div>';
      });
    }
  } catch(e) {}
  // Delete button
  html += '<hr style="border-color:var(--border);margin:14px 0">';
  html += '<button style="width:100%;padding:8px 20px;font-size:12px;background:rgba(224,106,106,0.1);color:var(--red);border:1px solid rgba(224,106,106,0.3);border-radius:6px;cursor:pointer;font-family:var(--font);transition:background 0.2s" onmouseover="this.style.background=\'rgba(224,106,106,0.2)\'" onmouseout="this.style.background=\'rgba(224,106,106,0.1)\'" ' + dashboardAction(function(dashboardEvent){deleteAgent((agentId))}) + '>Delete Agent</button>';
  document.getElementById('modal-body').innerHTML = html;
}

async function deleteAgent(agentId) {
  const btn = event.target;
  if (btn.dataset.confirm !== 'yes') {
    btn.dataset.confirm = 'yes';
    btn.textContent = '! Click again to confirm deletion';
    btn.style.background = 'rgba(224,106,106,0.3)';
    btn.style.fontWeight = '700';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete Agent'; btn.style.background = 'rgba(224,106,106,0.1)'; btn.style.fontWeight = ''; }, 4000);
    return;
  }
  btn.textContent = 'Deleting...';
  btn.style.pointerEvents = 'none';
  try {
    const actorID = currentProfileId();
    if (!actorID) {
      btn.textContent = 'Select profile to delete';
      btn.style.pointerEvents = '';
      toast('Select a profile before deleting agents');
      setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete Agent'; btn.style.background = 'rgba(224,106,106,0.1)'; btn.style.fontWeight = ''; }, 3000);
      return;
    }
    await rpc('agent.delete', {workspace_id: WS_ID, agent_id: agentId, actor: actorID});
    closeModal();
    toast('Agent ' + agentId + ' deleted');
    await loadAgents();
  } catch(e) {
    btn.textContent = '✗ ' + (e.message || 'Failed');
    btn.style.pointerEvents = '';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete Agent'; btn.style.background = 'rgba(224,106,106,0.1)'; btn.style.fontWeight = ''; }, 3000);
  }
}

async function loadSessions() {
  try {
    const r = await rpc('workspace.sessions.list', {workspace_id:WS_ID, active_only:true, limit:25});
    sessionsCache = r.sessions || [];
    document.getElementById('sessions-count').textContent = sessionsCache.length;
    document.getElementById('s-sessions').textContent = sessionsCache.length;
    document.getElementById('s-attention').textContent = sessionAttentionCount(sessionsCache);
    const el = document.getElementById('sessions-list');
    if (!sessionsCache.length) {
      el.innerHTML = '<div class="empty">No active sessions. Agents are idle or have already ended their work.</div>';
      return;
    }
    el.innerHTML = sessionsCache.map(s => {
      const status = String(s.status || 'ACTIVE').toUpperCase();
      const context = sessionContextSummary(s);
      return '<div class="session-item" ' + dashboardAction(function(dashboardEvent){showSessionDetail((s.session_id))}) + '>' +
        '<div class="session-top">' +
          '<div>' +
            '<div class="session-title">'+esc(s.summary || s.session_id || 'active session')+'</div>' +
            '<div class="session-owner">'+esc(sessionOwnerLabel(s))+' · '+esc(s.agent_id || '')+' · '+timeAgo(s.updated_at || s.started_at)+'</div>' +
          '</div>' +
          '<span class="'+sessionBadgeClass(status)+'">'+esc(status)+'</span>' +
        '</div>' +
        (context ? '<div class="session-meta">'+esc(context)+'</div>' : '') +
      '</div>';
    }).join('');
  } catch(e) {
    console.error('loadSessions', e);
    document.getElementById('sessions-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load sessions')+'</div>';
  }
  loadOperatorInbox();
}

async function startSessionFromDashboard() {
  const defaultAgent = agentsCache.length ? (agentsCache[0].agent_id || '') : '';
  const agentId = await dashboardPrompt('Agent ID for the new session:', defaultAgent);
  if (agentId === null) return;
  if (!String(agentId || '').trim()) {
    toast('Agent ID is required');
    return;
  }
  const sessionId = await dashboardPrompt('Session ID for the new session:', dashboardGeneratedID('sess'));
  if (sessionId === null) return;
  if (!String(sessionId || '').trim()) {
    toast('Session ID is required');
    return;
  }
  const summary = await dashboardPrompt('Summary for the new session:', 'Starting active work');
  if (summary === null) return;
  if (!String(summary || '').trim()) {
    toast('Summary is required');
    return;
  }
  const taskId = await dashboardPrompt('Optional task ID for the session:', '');
  if (taskId === null) return;
  const ownerScope = await dashboardPrompt('Optional owner scope for the session:', '');
  if (ownerScope === null) return;
  const keepPeers = await dashboardPrompt('Keep peer sessions active? (true/false)', 'true');
  if (keepPeers === null) return;
  try {
    await rpc('agent.session.start', {
      workspace_id: WS_ID,
      session_id: String(sessionId || '').trim(),
      agent_id: String(agentId || '').trim(),
      task_id: String(taskId || '').trim(),
      summary: String(summary || '').trim(),
      owner_scope: String(ownerScope || '').trim(),
      keep_session_active: boolPromptDefault(keepPeers, true)
    });
    toast('Session started');
    await Promise.all([loadSessions(), loadAgents(), loadMemory(), loadOperatorQueue(), loadExecutionRuns(), loadRuntimeEvents(), loadCompaction()]);
    switchTab('overview');
    showSessionDetail(String(sessionId || '').trim());
  } catch (e) {
    console.error('agent.session.start', e);
    toast('Failed to start session: ' + e.message);
  }
}

function showSessionDetail(sessionId) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Owner</strong><br>'+esc(sessionOwnerLabel(session))+' <span style="color:var(--muted)">('+esc(session.agent_id || '')+')</span></div>';
  html += '<div><strong>Status</strong><br><span class="'+sessionBadgeClass(session.status || 'ACTIVE')+'">'+esc(session.status || 'ACTIVE')+'</span></div>';
  html += '<div><strong>Started</strong><br>'+esc(timeAgo(session.started_at))+'</div>';
  html += '<div><strong>Updated</strong><br>'+esc(timeAgo(session.updated_at || session.started_at))+'</div>';
  if (session.task_id) html += '<div><strong>Task</strong><br>'+esc(session.task_id)+'</div>';
  if (session.owner_scope) html += '<div><strong>Owner Scope</strong><br>'+esc(session.owner_scope)+'</div>';
  if (session.decision_needed_from) html += '<div><strong>Decision Needed From</strong><br>'+esc(session.decision_needed_from)+'</div>';
  if (session.decision_type) html += '<div><strong>Decision Type</strong><br>'+esc(session.decision_type)+'</div>';
  if (session.handoff_to) html += '<div><strong>Handoff To</strong><br>'+esc(session.handoff_to)+'</div>';
  if (session.keep_session_active !== undefined && session.keep_session_active !== null) html += '<div><strong>Peer Sessions</strong><br>'+(session.keep_session_active ? 'Keep active' : 'Can stop')+'</div>';
  html += '</div>';
  html += '<div style="margin-bottom:12px"><strong>Summary</strong><div class="msg-item" style="margin-top:6px">'+esc(session.summary || session.session_id || 'No summary.')+'</div></div>';
  if (session.blocked_on && session.blocked_on.length) {
    html += '<div style="margin-bottom:12px"><strong>Blocked On</strong>';
    html += session.blocked_on.map(b => '<div class="msg-item" style="margin-top:6px"><strong>'+esc(b.kind || 'blocker')+'</strong><div style="margin-top:4px">'+esc(b.detail || 'Waiting on external blocker')+'</div></div>').join('');
    html += '</div>';
  }
  if (session.related_doc_keys && session.related_doc_keys.length) {
    html += '<div style="margin-bottom:12px"><strong>Related Docs</strong><div class="msg-item" style="margin-top:6px">'+esc(session.related_doc_keys.join(', '))+'</div></div>';
  }
  if (session.related_artifact_refs && session.related_artifact_refs.length) {
    html += '<div style="margin-bottom:12px"><strong>Related Artifacts</strong>';
    html += session.related_artifact_refs.map(a => '<div class="msg-item" style="margin-top:6px"><strong>'+esc(a.kind || 'artifact')+'</strong><div style="margin-top:4px">'+esc(a.ref || '')+'</div></div>').join('');
    html += '</div>';
  }
  html += '<div class="session-action-panel">';
  html += '<div style="font-size:12px;font-weight:600;margin-bottom:10px">Operator Actions</div>';
  html += '<label for="session-summary-input">Summary</label>';
  html += '<textarea id="session-summary-input" rows="3" placeholder="Summarize the current session state">'+esc(session.summary || '')+'</textarea>';
  html += '<div class="session-action-grid">';
  html += '<div><label for="session-blocker-input">Blocker Detail</label><input id="session-blocker-input" placeholder="Waiting on API key / human approval / dependency"></div>';
  html += '<div><label for="session-decision-input">Decision Needed From</label><input id="session-decision-input" placeholder="operator / reviewer / owner"></div>';
  html += '<div><label for="session-handoff-input">Handoff To</label><input id="session-handoff-input" placeholder="agent id / operator / reviewer" value="'+esc(session.handoff_to || '')+'"></div>';
  html += '</div>';
  html += '<div class="action-btn-row">';
  html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){resumeSessionFromModal((session.session_id))}) + '>Resume Active</button>';
  html += '<button class="btn-session-warn" ' + dashboardAction(function(dashboardEvent){markSessionBlockedFromModal((session.session_id))}) + '>Mark Blocked</button>';
  html += '<button class="btn-session-warn" ' + dashboardAction(function(dashboardEvent){markSessionDecisionNeededFromModal((session.session_id))}) + '>Need Decision</button>';
  html += '</div>';
  html += '<div class="action-btn-row">';
  html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){requestSessionHandoffFromModal((session.session_id))}) + '>Request Handoff</button>';
  html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){takeOverSessionFromModal((session.session_id))}) + '>Take Over</button>';
  html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){clearSessionHandoffFromModal((session.session_id))}) + '>Clear Handoff</button>';
  html += '<button class="btn-session-danger" ' + dashboardAction(function(dashboardEvent){endSessionFromModal((session.session_id))}) + '>End Session</button>';
  html += '</div>';
  html += '<div class="action-btn-row">';
  html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){setSessionPeerState((session.session_id), true)}) + '>Keep Peers Active</button>';
  html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){setSessionPeerState((session.session_id), false)}) + '>Stand Down Peers</button>';
  html += '</div>';
  html += '<div id="session-action-status" class="session-action-status"></div>';
  html += '</div>';
  openModal('' + (session.session_id || 'session'), html);
}

// ── Documents ──
function sessionModalValue(id, fallback = '') {
  const el = document.getElementById(id);
  if (!el) return String(fallback || '').trim();
  return String(el.value || '').trim();
}

function setSessionActionStatus(message, isError = false) {
  const el = document.getElementById('session-action-status');
  if (!el) return;
  el.textContent = message || '';
  el.className = 'session-action-status' + (message ? (isError ? ' err' : ' ok') : '');
}

function buildSessionEventParams(session, overrides = {}) {
  const params = {
    workspace_id: WS_ID,
    session_id: session.session_id,
    agent_id: session.agent_id,
    task_id: session.task_id || undefined,
    summary: overrides.summary !== undefined ? overrides.summary : (session.summary || session.session_id || 'session update'),
    status: overrides.status !== undefined ? overrides.status : (session.status || 'ACTIVE'),
    owner_scope: overrides.owner_scope !== undefined ? overrides.owner_scope : (session.owner_scope || ''),
    blocked_on: overrides.blocked_on !== undefined ? overrides.blocked_on : (session.blocked_on || []),
    decision_needed_from: overrides.decision_needed_from !== undefined ? overrides.decision_needed_from : (session.decision_needed_from || ''),
    decision_type: overrides.decision_type !== undefined ? overrides.decision_type : (session.decision_type || ''),
    handoff_to: overrides.handoff_to !== undefined ? overrides.handoff_to : (session.handoff_to || ''),
    related_doc_keys: overrides.related_doc_keys !== undefined ? overrides.related_doc_keys : (session.related_doc_keys || []),
    related_artifact_refs: overrides.related_artifact_refs !== undefined ? overrides.related_artifact_refs : (session.related_artifact_refs || [])
  };
  if (overrides.keep_session_active !== undefined) {
    params.keep_session_active = overrides.keep_session_active;
  } else if (typeof session.keep_session_active === 'boolean') {
    params.keep_session_active = session.keep_session_active;
  }
  return params;
}

async function dispatchSessionModalEvent(sessionId, method, overrides, successMessage) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) {
    toast('Session not found');
    return;
  }
  const params = buildSessionEventParams(session, overrides || {});
  if (!String(params.summary || '').trim()) {
    setSessionActionStatus('Summary is required', true);
    return;
  }
  setSessionActionStatus('Updating session...');
  try {
    await rpc(method, params);
    setSessionActionStatus(successMessage || 'Session updated');
    toast(successMessage || 'Session updated');
    await Promise.all([loadSessions(), loadAgents(), loadMemory(), loadOperatorQueue(), loadExecutionRuns(), loadRuntimeEvents(), loadCompaction()]);
    if (sessionsCache.find(x => x.session_id === sessionId)) showSessionDetail(sessionId);
    else closeModal();
  } catch (e) {
    console.error(method, e);
    setSessionActionStatus(e.message || 'Session update failed', true);
  }
}

async function resumeSessionFromModal(sessionId) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  const currentStatus = String(session.status || 'ACTIVE').toUpperCase();
  const keepActive = currentStatus === 'HANDOFF_PENDING' ? false : (typeof session.keep_session_active === 'boolean' ? session.keep_session_active : true);
  return dispatchSessionModalEvent(sessionId, 'agent.session.status', {
    summary: sessionModalValue('session-summary-input', session.summary || ''),
    status: 'ACTIVE',
    blocked_on: [],
    decision_needed_from: '',
    decision_type: '',
    handoff_to: '',
    keep_session_active: keepActive
  }, 'Session marked active');
}

async function markSessionBlockedFromModal(sessionId) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  const blockerDetail = sessionModalValue('session-blocker-input');
  if (!blockerDetail) {
    setSessionActionStatus('Blocker detail is required', true);
    return;
  }
  return dispatchSessionModalEvent(sessionId, 'agent.session.blocked', {
    summary: sessionModalValue('session-summary-input', session.summary || ''),
    status: 'BLOCKED',
    blocked_on: [{kind:'operator', detail:blockerDetail}],
    handoff_to: '',
    keep_session_active: typeof session.keep_session_active === 'boolean' ? session.keep_session_active : false
  }, 'Session marked blocked');
}

async function markSessionDecisionNeededFromModal(sessionId) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  const actor = sessionModalValue('session-decision-input');
  if (!actor) {
    setSessionActionStatus('Decision owner is required', true);
    return;
  }
  return dispatchSessionModalEvent(sessionId, 'agent.session.decision_needed', {
    summary: sessionModalValue('session-summary-input', session.summary || ''),
    status: 'WAITING_DECISION',
    blocked_on: [],
    decision_needed_from: actor,
    handoff_to: '',
    keep_session_active: typeof session.keep_session_active === 'boolean' ? session.keep_session_active : true
  }, 'Session marked waiting for decision');
}

async function requestSessionHandoffFromModal(sessionId) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  const target = sessionModalValue('session-handoff-input');
  if (!target) {
    setSessionActionStatus('Handoff target is required', true);
    return;
  }
  return dispatchSessionModalEvent(sessionId, 'agent.session.status', {
    summary: sessionModalValue('session-summary-input', session.summary || ''),
    status: 'HANDOFF_PENDING',
    blocked_on: [],
    decision_needed_from: '',
    decision_type: '',
    handoff_to: target,
    keep_session_active: true
  }, 'Session marked handoff pending');
}

async function clearSessionHandoffFromModal(sessionId) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  return dispatchSessionModalEvent(sessionId, 'agent.session.status', {
    summary: sessionModalValue('session-summary-input', session.summary || ''),
    status: 'ACTIVE',
    blocked_on: [],
    decision_needed_from: '',
    decision_type: '',
    handoff_to: '',
    keep_session_active: false
  }, 'Handoff cleared');
}

async function takeOverSessionFromModal(sessionId) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  const target = sessionModalValue('session-handoff-input');
  if (!target) {
    setSessionActionStatus('Takeover target is required', true);
    return;
  }
  const summary = sessionModalValue('session-summary-input', session.summary || '');
  if (!summary) {
    setSessionActionStatus('Summary is required', true);
    return;
  }
  setSessionActionStatus('Transferring session...');
  try {
    const response = await rpc('agent.session.takeover', {
      workspace_id: WS_ID,
      session_id: session.session_id,
      takeover_agent_id: target,
      summary,
      successor_summary: 'Takeover from ' + (session.agent_id || 'previous owner') + ': ' + summary
    });
    setSessionActionStatus('Session transferred');
    toast('Session transferred');
    await Promise.all([loadSessions(), loadAgents(), loadMemory(), loadOperatorQueue(), loadExecutionRuns(), loadRuntimeEvents(), loadCompaction()]);
    const successor = response && response.successor_state ? response.successor_state : null;
    if (successor && successor.session_id) showSessionDetail(successor.session_id);
    else closeModal();
  } catch (e) {
    console.error('agent.session.takeover', e);
    setSessionActionStatus(e.message || 'Session takeover failed', true);
  }
}

async function setSessionPeerState(sessionId, keepActive) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  const status = String(session.status || 'ACTIVE').toUpperCase();
  return dispatchSessionModalEvent(sessionId, 'agent.session.keepalive', {
    summary: sessionModalValue('session-summary-input', session.summary || ''),
    status: status,
    keep_session_active: !!keepActive,
    blocked_on: status === 'BLOCKED' ? (session.blocked_on || []) : [],
    decision_needed_from: status === 'WAITING_DECISION' ? (session.decision_needed_from || '') : '',
    decision_type: status === 'WAITING_DECISION' ? (session.decision_type || '') : ''
  }, keepActive ? 'Peers should stay active' : 'Peers can stand down');
}

async function endSessionFromModal(sessionId) {
  const session = sessionsCache.find(x => x.session_id === sessionId);
  if (!session) return;
  return dispatchSessionModalEvent(sessionId, 'agent.session.end', {
    summary: sessionModalValue('session-summary-input', session.summary || ''),
    status: 'ENDED',
    blocked_on: [],
    decision_needed_from: '',
    decision_type: '',
    keep_session_active: false
  }, 'Session ended');
}

async function loadDocs() {
  try {
    const r = await rpc('workspace.doc.list', {workspace_id:WS_ID});
    const docs = r.docs || [];
    document.getElementById('docs-count').textContent = docs.length;
    document.getElementById('s-docs').textContent = docs.length;
    const el = document.getElementById('docs-list');
    if (!docs.length) { el.innerHTML = '<div class="empty">No documents</div>'; return; }
    el.innerHTML = docs.map(d =>
      '<div class="doc-item" ' + dashboardAction(function(dashboardEvent){showDoc((d.doc_key||d.title))}) + '>' +
        '<span class="doc-key">'+esc(d.doc_key||d.title)+'</span>' +
        '<span class="doc-meta">'+esc(d.updated_by||'')+' · '+timeAgo(d.updated_at)+'</span>' +
      '</div>'
    ).join('');
  } catch(e) { console.error('loadDocs', e); }
}

async function showDoc(docKey) {
  openModal('' + docKey, '<div class="empty">Loading...</div>');
  try {
    const r = await rpc('workspace.doc.get', {workspace_id:WS_ID, doc_key:docKey});
    let content = r.content || '';
    const docSegments = corridorSegmentEntries(r);
    const taskFirstAuthority = corridorAuthorityApproximation(r);
    const authorityFreshness = corridorAuthorityBasisFreshnessApproximation(r);
    const showDocApproximationNote = docSegments.length || taskFirstAuthority !== 'not surfaced' || authorityFreshness !== 'no task-metadata lookup basis';
    document.getElementById('modal-body').innerHTML =
      '<div style="font-size:11px;color:var(--muted);margin-bottom:8px">'+esc(r.updated_by||'')+' · '+timeAgo(r.updated_at)+' · SHA: <code>'+esc((r.sha||'').substring(0,8))+'</code></div>' +
      ((taskFirstAuthority !== 'not surfaced' || authorityFreshness !== 'no task-metadata lookup basis') ? '<div style="font-size:11px;color:var(--muted);margin-bottom:8px">task-first authority ' + esc(String(taskFirstAuthority).toLowerCase()) + ' | basis ' + esc(authorityFreshness) + '</div>' : '') +
      renderSegmentBadgeRow('Document Segments', docSegments) +
      (showDocApproximationNote ? '<div style="font-size:11px;color:var(--muted);margin-bottom:8px">Document segments and task-first corridor authority stay read-only operator evidence only.</div>' : '') +
      '<pre>'+esc(content)+'</pre>';
  } catch(e) {
    document.getElementById('modal-body').innerHTML = '<div class="empty">Error: '+esc(e.message)+'</div>';
  }
}

// ── Workspace Memory ──
function toggleMemoryComposer(forceOpen) {
  const composer = document.getElementById('memory-composer');
  const shouldOpen = typeof forceOpen === 'boolean' ? forceOpen : !composer.classList.contains('open');
  composer.classList.toggle('open', shouldOpen);
  if (!shouldOpen) {
    clearMemoryComposer();
    setMemoryFormStatus('');
    return;
  }
  renderMemoryComposerChrome();
}

function setMemoryFormStatus(message, isError = false) {
  const el = document.getElementById('memory-form-status');
  if (!el) return;
  el.textContent = message || '';
  el.className = 'session-action-status' + (message ? (isError ? ' err' : ' ok') : '');
}

function renderMemoryComposerChrome() {
  const titleEl = document.getElementById('memory-composer-title');
  const submitEl = document.getElementById('memory-form-submit');
  let title = 'New Memory';
  let submit = 'Record Memory';
  if (memoryComposerDraft) {
    if (memoryComposerDraft.mode === 'clone') {
      title = 'Copy As Manual';
      submit = 'Create Copy';
    } else if (memoryComposerDraft.archived) {
      title = 'Restore & Edit Memory';
      submit = 'Restore Memory';
    } else {
      title = 'Edit Memory';
      submit = 'Save Memory';
    }
  }
  if (titleEl) titleEl.textContent = title;
  if (submitEl) submitEl.textContent = submit;
}

function clearMemoryComposer() {
  memoryComposerDraft = null;
  document.getElementById('memory-form-type').value = 'NOTE';
  document.getElementById('memory-form-title').value = '';
  document.getElementById('memory-form-summary').value = '';
  document.getElementById('memory-form-body').value = '';
  document.getElementById('memory-form-tags').value = '';
  renderMemoryComposerChrome();
}

function cancelMemoryComposer() {
  toggleMemoryComposer(false);
}

function buildMemoryWriteParamsFromRecord(record, overrides = {}) {
  const params = {
    workspace_id: WS_ID,
    memory_type: overrides.memory_type !== undefined ? overrides.memory_type : (record.memory_type || 'NOTE'),
    title: overrides.title !== undefined ? overrides.title : (record.title || ''),
    summary: overrides.summary !== undefined ? overrides.summary : (record.summary || ''),
    body: overrides.body !== undefined ? overrides.body : (record.body || ''),
    source_kind: overrides.source_kind !== undefined ? overrides.source_kind : (record.source_kind || 'manual'),
    source_id: overrides.source_id !== undefined ? overrides.source_id : (record.source_id || 'dashboard'),
    tags: overrides.tags !== undefined ? overrides.tags : (Array.isArray(record.tags) ? record.tags : [])
  };
  const memoryId = overrides.memory_id !== undefined ? overrides.memory_id : (record.memory_id || '');
  const agentID = overrides.agent_id !== undefined ? overrides.agent_id : (record.agent_id || '');
  const sessionID = overrides.session_id !== undefined ? overrides.session_id : (record.session_id || '');
  const taskID = overrides.task_id !== undefined ? overrides.task_id : (record.task_id || '');
  if (memoryId) params.memory_id = memoryId;
  if (agentID) params.agent_id = agentID;
  if (sessionID) params.session_id = sessionID;
  if (taskID) params.task_id = taskID;
  return params;
}

function openMemoryComposerForEntry(memoryId, cloneOnly = false) {
  const entry = memoryCache.find(x => x.record && x.record.memory_id === memoryId);
  if (!entry || !entry.record) return;
  const record = entry.record;
  const isClone = !!cloneOnly || String(record.source_kind || '').toLowerCase() !== 'manual';
  memoryComposerDraft = {
    mode: isClone ? 'clone' : 'edit',
    archived: !!record.archived_at,
    memory_id: isClone ? '' : (record.memory_id || ''),
    source_kind: isClone ? 'manual' : (record.source_kind || 'manual'),
    source_id: isClone ? 'dashboard' : (record.source_id || 'dashboard'),
    agent_id: isClone ? '' : (record.agent_id || ''),
    session_id: record.session_id || '',
    task_id: record.task_id || ''
  };
  document.getElementById('memory-form-type').value = record.memory_type || 'NOTE';
  document.getElementById('memory-form-title').value = record.title || '';
  document.getElementById('memory-form-summary').value = record.summary || '';
  document.getElementById('memory-form-body').value = record.body || '';
  document.getElementById('memory-form-tags').value = Array.isArray(record.tags) ? record.tags.join(', ') : '';
  setMemoryFormStatus('');
  renderMemoryComposerChrome();
  closeModal();
  toggleMemoryComposer(true);
}

async function submitMemoryEntry() {
  const body = String(document.getElementById('memory-form-body').value || '').trim();
  if (!body) {
    setMemoryFormStatus('Body is required', true);
    return;
  }
  const tags = String(document.getElementById('memory-form-tags').value || '')
    .split(',')
    .map(tag => tag.trim())
    .filter(Boolean);
  setMemoryFormStatus(memoryComposerDraft ? 'Saving memory...' : 'Recording memory...');
  try {
    const response = await rpc('workspace.memory.write', {
      workspace_id: WS_ID,
      memory_id: memoryComposerDraft && memoryComposerDraft.memory_id ? memoryComposerDraft.memory_id : undefined,
      memory_type: document.getElementById('memory-form-type').value || 'NOTE',
      title: String(document.getElementById('memory-form-title').value || '').trim(),
      summary: String(document.getElementById('memory-form-summary').value || '').trim(),
      body,
      source_kind: memoryComposerDraft && memoryComposerDraft.source_kind ? memoryComposerDraft.source_kind : 'manual',
      source_id: memoryComposerDraft && memoryComposerDraft.source_id ? memoryComposerDraft.source_id : 'dashboard',
      agent_id: memoryComposerDraft && memoryComposerDraft.agent_id ? memoryComposerDraft.agent_id : undefined,
      session_id: memoryComposerDraft && memoryComposerDraft.session_id ? memoryComposerDraft.session_id : undefined,
      task_id: memoryComposerDraft && memoryComposerDraft.task_id ? memoryComposerDraft.task_id : undefined,
      tags
    });
    const savedMemoryID = response && response.memory && response.memory.memory_id ? response.memory.memory_id : '';
    const successLabel = memoryComposerDraft ? (memoryComposerDraft.mode === 'clone' ? 'Memory copied' : 'Memory saved') : 'Memory recorded';
    clearMemoryComposer();
    toggleMemoryComposer(false);
    toast(successLabel);
    await loadMemory();
    if (savedMemoryID) showMemoryDetail(savedMemoryID);
  } catch (e) {
    console.error('submitMemoryEntry', e);
    setMemoryFormStatus(e.message || (memoryComposerDraft ? 'Failed to save memory' : 'Failed to record memory'), true);
  }
}

async function restoreMemoryFromModal(memoryId) {
  const entry = memoryCache.find(x => x.record && x.record.memory_id === memoryId);
  if (!entry || !entry.record) return;
  const record = entry.record;
  if (!record.archived_at) {
    toast('Memory is already active');
    return;
  }
  if (String(record.source_kind || '').toLowerCase() !== 'manual') {
    toast('Only manual memory can be restored directly');
    return;
  }
  try {
    const response = await rpc('workspace.memory.restore', {
      workspace_id: WS_ID,
      memory_id: memoryId,
      restored_by: 'dashboard'
    });
    const restoredID = response && response.memory && response.memory.memory_id ? response.memory.memory_id : record.memory_id;
    toast('Memory restored');
    await loadMemory();
    if (restoredID) showMemoryDetail(restoredID);
  } catch (e) {
    console.error('restoreMemoryFromModal', e);
    toast(e.message || 'Failed to restore memory');
  }
}

async function archiveMemoryFromModal(memoryId) {
  const entry = memoryCache.find(x => x.record && x.record.memory_id === memoryId);
  if (!entry || !entry.record) return;
  if (entry.record.archived_at) {
    toast('Memory is already archived');
    return;
  }
  const reason = await dashboardPrompt('Archive reason (optional):', '') || '';
  try {
    await rpc('workspace.memory.remove', {
      workspace_id: WS_ID,
      memory_id: memoryId,
      removed_by: 'dashboard',
      reason: String(reason || '').trim()
    });
    toast('Memory archived');
    await loadMemory();
    const refreshed = memoryCache.find(x => x.record && x.record.memory_id === memoryId);
    if (refreshed) showMemoryDetail(memoryId);
    else closeModal();
  } catch (e) {
    console.error('archiveMemoryFromModal', e);
    toast(e.message || 'Failed to archive memory');
  }
}

function currentMemoryFilters() {
  const queryEl = document.getElementById('memory-search-query');
  const typeEl = document.getElementById('memory-filter-type');
  const sourceEl = document.getElementById('memory-filter-source');
  const archivedEl = document.getElementById('memory-filter-archived');
  return {
    query: String(queryEl && queryEl.value || '').trim(),
    memory_type: String(typeEl && typeEl.value || '').trim(),
    source_kind: String(sourceEl && sourceEl.value || '').trim(),
    include_archived: !!(archivedEl && archivedEl.checked),
    agent_id: String(memoryContextFilters.agent_id || '').trim(),
    session_id: String(memoryContextFilters.session_id || '').trim(),
    task_id: String(memoryContextFilters.task_id || '').trim()
  };
}

function scheduleMemoryRefresh() {
  if (memorySearchTimer) clearTimeout(memorySearchTimer);
  memorySearchTimer = setTimeout(() => loadMemory(), 180);
}

function renderMemoryFilterContext() {
  const el = document.getElementById('memory-filter-context');
  if (!el) return;
  const parts = [];
  if (memoryContextFilters.agent_id) parts.push('agent: ' + memoryContextFilters.agent_id);
  if (memoryContextFilters.session_id) parts.push('session: ' + memoryContextFilters.session_id);
  if (memoryContextFilters.task_id) parts.push('task: ' + memoryContextFilters.task_id);
  if (!parts.length) {
    el.textContent = '';
    el.style.display = 'none';
    return;
  }
  el.textContent = 'Scoped to ' + parts.join(' • ');
  el.style.display = '';
}

function applyMemoryContextFilter(kind, value) {
  const normalizedKind = String(kind || '').trim();
  if (!['agent_id', 'session_id', 'task_id'].includes(normalizedKind)) return;
  memoryContextFilters[normalizedKind] = String(value || '').trim();
  renderMemoryFilterContext();
  closeModal();
  loadMemory();
}

function resetMemoryFilters() {
  document.getElementById('memory-search-query').value = '';
  document.getElementById('memory-filter-type').value = '';
  document.getElementById('memory-filter-source').value = '';
  document.getElementById('memory-filter-archived').checked = false;
  memoryContextFilters = {agent_id:'', session_id:'', task_id:''};
  renderMemoryFilterContext();
  loadMemory();
}

async function loadMemory() {
  try {
    const filters = currentMemoryFilters();
    const params = {
      workspace_id: WS_ID,
      include_archived: filters.include_archived,
      limit: filters.query ? 20 : 12
    };
    if (filters.memory_type) params.memory_type = filters.memory_type;
    if (filters.source_kind) params.source_kind = filters.source_kind;
    if (filters.query) params.query = filters.query;
    if (filters.agent_id) params.agent_id = filters.agent_id;
    if (filters.session_id) params.session_id = filters.session_id;
    if (filters.task_id) params.task_id = filters.task_id;
    const r = await rpc(filters.query ? 'workspace.memory.search' : 'workspace.memory.list', params);
    memoryCache = r.entries || [];
    memorySurfaceTimeAuthority = r.time_authority || memorySurfaceTimeAuthority;
    const summary = r.summary || {};
    document.getElementById('memory-count').textContent = memoryCache.length;
    renderMemoryFilterContext();
    const attentionBadge = document.getElementById('memory-attention-badge');
    const attentionCount = summary.attention_count || 0;
    if (attentionCount > 0) {
      attentionBadge.style.display = '';
      attentionBadge.textContent = attentionCount + ' attention';
    } else {
      attentionBadge.style.display = 'none';
    }

    const el = document.getElementById('memory-list');
    if (!memoryCache.length) {
      el.innerHTML = '<div class="empty">'+(filters.query || filters.memory_type || filters.source_kind || filters.include_archived || filters.agent_id || filters.session_id || filters.task_id ? 'No memory matched the current filters.' : 'No canonical memory recorded yet.')+'</div>';
      return;
    }
    el.innerHTML = memoryCache.map(entry => {
      const record = entry.record || {};
      const meta = entry.meta || {};
      const badges = [
        memoryBadge(meta.state === 'ARCHIVED' ? 'state-archived' : 'state-active', meta.state || 'ACTIVE'),
        memoryBadge('type', meta.type_label || record.memory_type || 'memory'),
        memoryBadge('source', meta.source_label || record.source_kind || 'unknown'),
      ];
      if (meta.derived) badges.push(memoryBadge('derived', 'derived'));
      if (meta.requires_attention) badges.push(memoryBadge('attention', meta.attention_label || 'attention'));
      if (meta.anchor_semantic_lineage_id) {
        const anchorBits = ['rev=' + Number(meta.anchor_revision || 0)];
        if (meta.anchor_protect !== undefined) anchorBits.push(meta.anchor_protect ? 'protect' : 'unprotected');
        if (meta.anchor_unresolved !== undefined && meta.anchor_unresolved) anchorBits.push('unresolved');
        badges.push(memoryBadge('type', 'anchor ' + anchorBits.join(' ')));
      }
      if (meta.retention_band) {
        const retentionClass = meta.retention_prunable ? 'attention' : 'source';
        badges.push(memoryBadge(retentionClass, 'retention ' + meta.retention_band.toLowerCase()));
      }
      if (meta.retention_guard_reason) {
        badges.push(memoryBadge('derived', 'guard ' + String(meta.retention_guard_reason || '').toLowerCase()));
      }
      if (meta.salience_a !== undefined) {
        const a_str = Number(meta.salience_a).toFixed(2);
        const h_str = Number(meta.salience_h / 3600).toFixed(1) + 'h';
        badges.push(memoryBadge('derived', 'a=' + a_str + ' h=' + h_str + ' n=' + meta.salience_n));
      }
      return '<div class="memory-item '+(meta.state === 'ARCHIVED' ? 'archived' : '')+'" ' + dashboardAction(function(dashboardEvent){showMemoryDetail((record.memory_id))}) + '>' +
        '<div class="memory-top">' +
          '<div>' +
            '<div class="memory-headline">'+esc(meta.headline || record.title || record.memory_id || 'memory')+'</div>' +
            '<div class="memory-meta">'+esc(meta.provenance || '')+' | '+timeAgo(record.updated_at || record.created_at, memorySurfaceTimeAuthority)+'</div>' +
          '</div>' +
        '</div>' +
        (memoryBodyPreview(entry) ? '<div class="memory-summary">'+esc(memoryBodyPreview(entry))+'</div>' : '') +
        (meta.context ? '<div class="memory-meta">'+esc(meta.context)+'</div>' : '') +
        '<div class="memory-badges">'+badges.join('')+'</div>' +
      '</div>';
    }).join('');
  } catch(e) {
    console.error('loadMemory', e);
    document.getElementById('memory-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load memory')+'</div>';
  }
}

function openSessionFromMemory(sessionId) {
  const normalized = String(sessionId || '').trim();
  if (!normalized) return;
  closeModal();
  showSessionDetail(normalized);
}

function mergeClaims(items) {
  (items || []).forEach(item => {
    const idx = claimsCache.findIndex(existing => existing.claim_id === item.claim_id);
    if (idx >= 0) claimsCache[idx] = item;
    else claimsCache.push(item);
  });
}

async function reinforceMemoryNode(memoryId) {
  try {
    const btn = event.target;
    btn.textContent = 'Reinforcing...';
    btn.disabled = true;
    await rpc('workspace.memory.node.touch', {
      workspace_id: WS_ID,
      node_id: memoryId,
      trusted: true,
      actor: currentProfileId() || 'dashboard'
    });
    await dashboardAlert('Memory node successfully reinforced.');
    loadMemory();
    closeModal();
  } catch(e) {
    await dashboardAlert(e.message || 'Failed to reinforce memory node.');
  }
}


async function showMemoryDetail(memoryId) {
  const entry = memoryCache.find(x => x.record && x.record.memory_id === memoryId);
  if (!entry) return;
  const record = entry.record || {};
  const meta = entry.meta || {};
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Type</strong><br>'+esc(meta.type_label || record.memory_type || 'Memory')+'</div>';
  html += '<div><strong>Source</strong><br>'+esc(meta.source_label || record.source_kind || 'Unknown')+'</div>';
  html += '<div><strong>State</strong><br>'+esc(meta.state || 'ACTIVE')+'</div>';
  html += '<div><strong>Updated</strong><br>'+esc(timeAgo(record.updated_at || record.created_at, memorySurfaceTimeAuthority))+'</div>';
  if (record.agent_id) html += '<div><strong>Agent</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();applyMemoryContextFilter('agent_id', (record.agent_id))}) + '>'+esc(record.agent_id)+'</a></div>';
  if (record.task_id) html += '<div><strong>Task</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tasks');setTimeout(()=>showTaskDetail((record.task_id),(record.task_id)),100)}) + '>'+esc(record.task_id)+'</a></div>';
  if (record.session_id) html += '<div><strong>Session</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();openSessionFromMemory((record.session_id))}) + '>'+esc(record.session_id)+'</a></div>';
  if (record.archived_at) html += '<div><strong>Archived</strong><br>'+esc(timeAgo(record.archived_at, memorySurfaceTimeAuthority))+'</div>';
  html += '</div>';
  html += '<div style="margin-bottom:12px"><strong>Provenance</strong><div class="msg-item" style="margin-top:6px">'+esc(meta.provenance || 'No provenance recorded.')+'</div></div>';
  if (meta.context) html += '<div style="margin-bottom:12px"><strong>Context</strong><div class="msg-item" style="margin-top:6px">'+esc(meta.context)+'</div></div>';
  if (meta.anchor_status || meta.anchor_status_reason || meta.anchor_signal_state || meta.anchor_invariant_state || meta.anchor_invariant_summary || meta.anchor_projection_lag_state || meta.anchor_projection_lag_message || meta.anchor_semantic_lineage_id || meta.anchor_revision !== undefined || meta.anchor_protect !== undefined || meta.anchor_unresolved !== undefined || meta.anchor_last_any_access || meta.anchor_last_trusted_access || meta.anchor_t_life !== undefined) {
    html += '<div style="margin-bottom:12px"><strong>Anchor State (Read-Side)</strong><div class="msg-item" style="margin-top:6px; font-family:var(--font-mono); font-size:12px;">';
    if (meta.anchor_status) html += 'anchor_status = ' + esc(meta.anchor_status) + '<br>';
    if (meta.anchor_status_reason) html += 'anchor_status_reason = ' + esc(meta.anchor_status_reason) + '<br>';
    if (meta.anchor_signal_state) html += 'anchor_signal_state = ' + esc(meta.anchor_signal_state) + '<br>';
    if (meta.anchor_invariant_state) html += 'anchor_invariant_state = ' + esc(meta.anchor_invariant_state) + '<br>';
    if (meta.anchor_invariant_summary) html += 'anchor_invariant_summary = ' + esc(meta.anchor_invariant_summary) + '<br>';
    if (meta.anchor_projection_lag_state) html += 'anchor_projection_lag_state = ' + esc(meta.anchor_projection_lag_state) + '<br>';
    if (meta.anchor_projection_lag_message) html += 'anchor_projection_lag_message = ' + esc(meta.anchor_projection_lag_message) + '<br>';
    if (meta.anchor_semantic_lineage_id) html += 'semantic_lineage_id = ' + esc(meta.anchor_semantic_lineage_id) + '<br>';
    if (meta.anchor_revision !== undefined) html += 'revision = ' + esc(meta.anchor_revision) + '<br>';
    if (meta.anchor_protect !== undefined) html += 'protect = ' + esc(meta.anchor_protect ? 'true' : 'false') + '<br>';
    if (meta.anchor_unresolved !== undefined) html += 'unresolved = ' + esc(meta.anchor_unresolved ? 'true' : 'false') + '<br>';
    if (meta.anchor_last_any_access) html += 'last_any_access = ' + esc(meta.anchor_last_any_access) + '<br>';
    if (meta.anchor_last_trusted_access) html += 'last_trusted_access = ' + esc(meta.anchor_last_trusted_access) + '<br>';
    if (meta.anchor_t_life !== undefined) html += 't_life = ' + Number(meta.anchor_t_life).toFixed(1) + 's';
    html += '</div></div>';
  }
  if (meta.retention_band || meta.retention_hot_until || meta.retention_warm_until || meta.retention_expires_at || meta.retention_guard_reason || meta.retention_prunable !== undefined) {
    html += '<div style="margin-bottom:12px"><strong>Retention State (Read-Side)</strong><div class="msg-item" style="margin-top:6px; font-family:var(--font-mono); font-size:12px;">';
    if (meta.retention_band) html += 'retention_band = ' + esc(meta.retention_band) + '<br>';
    if (meta.retention_prunable !== undefined) html += 'retention_prunable = ' + esc(String(!!meta.retention_prunable)) + '<br>';
    if (meta.retention_guard_reason) html += 'retention_guard_reason = ' + esc(meta.retention_guard_reason) + '<br>';
    if (meta.retention_hot_until) html += 'retention_hot_until = ' + esc(meta.retention_hot_until) + ' (' + esc(timeAgo(meta.retention_hot_until, memorySurfaceTimeAuthority)) + ')<br>';
    if (meta.retention_warm_until) html += 'retention_warm_until = ' + esc(meta.retention_warm_until) + ' (' + esc(timeAgo(meta.retention_warm_until, memorySurfaceTimeAuthority)) + ')<br>';
    if (meta.retention_expires_at) html += 'retention_expires_at = ' + esc(meta.retention_expires_at) + ' (' + esc(timeAgo(meta.retention_expires_at, memorySurfaceTimeAuthority)) + ')';
    html += '</div></div>';
  }
  if (meta.recovery_candidate || meta.recovery_guard_reason || (Array.isArray(meta.recovery_trigger_kinds) && meta.recovery_trigger_kinds.length) || meta.recovery_trigger_count) {
    html += '<div style="margin-bottom:12px"><strong>Recovery State (Read-Side)</strong><div class="msg-item" style="margin-top:6px; font-family:var(--font-mono); font-size:12px;">';
    if (meta.recovery_candidate !== undefined) html += 'recovery_candidate = ' + esc(String(!!meta.recovery_candidate)) + '<br>';
    if (meta.recovery_trigger_count !== undefined) html += 'recovery_trigger_count = ' + esc(String(meta.recovery_trigger_count)) + '<br>';
    if (Array.isArray(meta.recovery_trigger_kinds) && meta.recovery_trigger_kinds.length) html += 'recovery_trigger_kinds = ' + esc(meta.recovery_trigger_kinds.join(', ')) + '<br>';
    if (meta.recovery_guard_reason) html += 'recovery_guard_reason = ' + esc(meta.recovery_guard_reason);
    html += '</div></div>';
  }
  if (meta.salience_a !== undefined) {
    html += '<div style="margin-bottom:12px"><strong>RMP Salience Telemetry</strong><div class="msg-item" style="margin-top:6px; font-family:var(--font-mono); font-size:12px;">';
    html += 'a<sub>i</sub> = ' + Number(meta.salience_a).toFixed(4) + '<br>';
    html += 't<sub>i</sub><sup>★</sup> = ' + esc(meta.salience_t_star) + '<br>';
    html += 'h<sub>i</sub> = ' + Number(meta.salience_h).toFixed(1) + 's (' + Number(meta.salience_h/3600).toFixed(2) + 'h)<br>';
    html += 'n<sub>i</sub> = ' + esc(meta.salience_n);
    html += '</div></div>';
  }
  if (record.summary) html += '<div style="margin-bottom:12px"><strong>Summary</strong><div class="msg-item" style="margin-top:6px">'+esc(record.summary)+'</div></div>';
  html += '<div style="margin-bottom:12px"><strong>Body</strong><pre>'+esc(record.body || 'No body recorded.')+'</pre></div>';
  if (record.tags && record.tags.length) {
    html += '<div style="margin-bottom:12px"><strong>Tags</strong><div style="margin-top:6px">'+record.tags.map(tag => '<span class="task-tag">'+esc(tag)+'</span>').join(' ')+'</div></div>';
  }
  if (record.archived_reason) {
    html += '<div style="margin-bottom:12px"><strong>Archive Reason</strong><div class="msg-item" style="margin-top:6px">'+esc(record.archived_reason)+'</div></div>';
  }
  if (record.agent_id && (record.task_id || record.session_id)) {
    try {
      const packetResult = await rpc('workspace.memory.packet.shell', {
        workspace_id: WS_ID,
        agent_id: record.agent_id,
        task_id: record.task_id || undefined,
        session_id: record.session_id || undefined
      });
      const packet = packetResult.packet || {};
      const packetMeta = packet.meta || {};
      html += '<div style="margin-bottom:12px"><strong>Packet Context (Inspectability Only)</strong><div class="msg-item" style="margin-top:6px">Current shell packet for the present task/session scope; not a historical memory snapshot, not global truth, not complete lineage, and not rollback authority.';
      if (packetMeta.packet_key || packetMeta.basis_digest) {
        html += '<div style="margin-top:8px; font-family:var(--font-mono); font-size:12px;">';
        if (packetMeta.generated_at) html += 'as_of = ' + esc(packetMeta.generated_at) + '<br>';
        if (packetMeta.packet_key) html += 'packet_key = ' + esc(packetMeta.packet_key) + '<br>';
        if (packetMeta.basis_digest) html += 'basis_digest = ' + esc(packetMeta.basis_digest);
        html += '</div>';
      }
      html += '</div>';
      html += '<div class="msg-item" style="margin-top:6px; font-family:var(--font-mono); font-size:12px;"><strong>Boundary Summary</strong><br>' + renderMemoryPacketBoundarySummary(packet.boundary_summary) + '</div>';
      html += '<div class="msg-item" style="margin-top:6px; font-family:var(--font-mono); font-size:12px;"><strong>Basis Summary</strong><br>' + renderMemoryPacketBasisSummary(packet.basis_summary) + '</div></div>';
    } catch (e) {
      console.error('memory packet context', e);
      html += '<div style="margin-bottom:12px"><strong>Packet Context (Inspectability Only)</strong><div class="msg-item" style="margin-top:6px">Packet-local shell context is unavailable for the current task/session scope.</div></div>';
    }
  }
  try {
    const relatedClaimsResult = await rpc('workspace.claim.list', {
      workspace_id: WS_ID,
      memory_id: record.memory_id,
      include_archived: true,
      limit: 10
    });
    const relatedClaims = relatedClaimsResult.items || [];
    mergeClaims(relatedClaims);
    if (relatedClaims.length) {
      html += '<div style="margin-bottom:12px"><strong>Related Claims</strong><div style="margin-top:6px">';
      html += relatedClaims.map(claim =>
        '<div class="msg-item" style="margin-bottom:6px">' +
          '<a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showClaimDetail((claim.claim_id))}) + ' style="color:var(--accent);font-weight:600">'+esc(claim.subject || claim.claim_id)+'</a>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">'+esc((claim.claim_type || 'FACT')+' · '+(claim.status || 'ACTIVE'))+'</div>' +
        '</div>'
      ).join('');
      html += '</div></div>';
    }
  } catch (e) {
    console.error('memory related claims', e);
  }
  if (record.agent_id || record.session_id || record.task_id) {
    html += '<div class="action-btn-row">';
    if (linkedRebasePayload && linkedRebasePayload.rebase_workflow_state !== 'in_progress') {
      html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){startAction((a.action_id))}) + '>▶ Start Rebase Work</button>';
    }
    if (record.agent_id) html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){applyMemoryContextFilter('agent_id', (record.agent_id))}) + '>Filter Agent</button>';
    if (record.session_id) html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){applyMemoryContextFilter('session_id', (record.session_id))}) + '>Filter Session</button>';
    if (record.task_id) html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){applyMemoryContextFilter('task_id', (record.task_id))}) + '>Filter Task</button>';
    if (record.session_id) html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){openSessionFromMemory((record.session_id))}) + '>Open Session</button>';
    html += '</div>';
  }
  const isManualMemory = String(record.source_kind || '').toLowerCase() === 'manual';
  html += '<div class="action-btn-row" style="margin-bottom:8px;">';
  html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){reinforceMemoryNode((record.memory_id))}) + '>Reinforce (Touch)</button>';
  html += '</div>';

  html += '<div class="action-btn-row">';
  if (isManualMemory) {
    if (record.archived_at) {
      html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){restoreMemoryFromModal((record.memory_id))}) + '>Restore Memory</button>';
      html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openMemoryComposerForEntry((record.memory_id), false)}) + '>Restore & Edit</button>';
    } else {
      html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openMemoryComposerForEntry((record.memory_id), false)}) + '>Edit / Promote</button>';
    }
  } else {
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openMemoryComposerForEntry((record.memory_id), true)}) + '>Copy As Manual</button>';
  }
  html += '</div>';
  if (!record.archived_at) {
    html += '<div class="action-btn-row">';
    html += '<button class="btn-session-danger" ' + dashboardAction(function(dashboardEvent){archiveMemoryFromModal((record.memory_id))}) + '>Archive Memory</button>';
    html += '</div>';
  }
  openModal('Memory ' + esc(meta.headline || record.memory_id || 'memory'), html);
}

function scheduleClaimsRefresh() {
  clearTimeout(claimSearchTimer);
  claimSearchTimer = setTimeout(loadClaims, 250);
}

function currentClaimsFilters() {
  return {
    query: document.getElementById('claim-search-query').value.trim(),
    status: document.getElementById('claim-filter-status').value,
    include_archived: document.getElementById('claim-filter-archived').checked
  };
}

function shortHealthRevision(value) {
  const normalized = String(value || "").trim();
  if (!normalized) return "unknown";
  return normalized.length > 12 ? normalized.slice(0, 12) : normalized;
}

function runtimeHealthTone(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized === "ok" || normalized === "healthy") return {bg: "rgba(78,166,116,.15)", fg: "#22c55e"};
  if (normalized === "degraded" || normalized === "warn" || normalized === "warning") return {bg: "rgba(249,115,22,.15)", fg: "#f97316"};
  if (normalized === "missing" || normalized === "error" || normalized === "failed") return {bg: "rgba(224,106,106,.15)", fg: "var(--red)"};
  return {bg: "rgba(139,138,135,.18)", fg: "var(--faint)"};
}

function runtimeHealthReviewerScarcityBreakdown(reviewerScarcityHealth) {
  if (!reviewerScarcityHealth || typeof reviewerScarcityHealth !== "object") return "";
  const parts = [];
  const groups = [
    ["saturated", reviewerScarcityHealth.saturated_workspace_examples],
    ["scarce", reviewerScarcityHealth.scarce_workspace_examples],
    ["unknown", reviewerScarcityHealth.unknown_workspace_examples],
  ];
  for (const [label, workspaces] of groups) {
    if (!Array.isArray(workspaces) || workspaces.length === 0) continue;
    parts.push(label + "=" + workspaces.join(", "));
  }
  return parts.join(" | ");
}

function renderRuntimeHealth() {
  const badge = document.getElementById("runtime-health-badge");
  const summary = document.getElementById("runtime-health-summary");
  const payload = runtimeHealthCache;
  if (!badge || !summary) return;
  if (!payload) {
    badge.textContent = "loading";
    badge.style.background = "rgba(139,138,135,.18)";
    badge.style.color = "var(--faint)";
    summary.innerHTML = '<div class="empty">Loading runtime health...</div>';
    return;
  }

  const tone = runtimeHealthTone(payload.status);
  badge.textContent = String(payload.status || "unknown").toUpperCase();
  badge.style.background = tone.bg;
  badge.style.color = tone.fg;

  const runtime = payload.runtime || {};
  const checkout = payload.checkout || {};
  const metrics = payload.metrics || {};
  const metricsHealth = metrics.health || {};
  const semantics = payload.semantics || {};
  const extended = payload.extended_readiness || {};
  const reviewerScarcityHealth = extended.reviewer_scarcity_health || {};
  const authorityNode = payload.authority_node || {};
  const authorityLease = payload.authority_lease || {};
  const patchQueueDurability = payload.project_patch_queue_durability || {};
  const repoMutationActivation = payload.repo_mutation_activation || {};
  const repoMutationActuatorDryRun = payload.repo_mutation_actuator_dry_run || {};
  const loopReadiness = Array.isArray(payload.loop_readiness) ? payload.loop_readiness : [];
  const reasons = Array.isArray(metricsHealth.reasons) ? metricsHealth.reasons.filter(Boolean) : [];
  const warnings = [];
  if (checkout.dirty) warnings.push("dirty checkout");
  if (runtime.vcs_modified) warnings.push("binary built from modified tree");
  if (checkout.error) warnings.push("checkout error");
  if (metrics.error) warnings.push("metrics error");
  if (metrics.status && String(metrics.status).toLowerCase() !== "ok") warnings.push("metrics " + metrics.status);

  let html = '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:10px">';
  html += '<div class="msg-item" style="margin:0"><div><strong>Service</strong><div style="margin-top:4px;color:'+tone.fg+'">'+esc(String(payload.status || "unknown").toUpperCase())+'</div></div></div>';
  html += '<div class="msg-item" style="margin:0"><div><strong>Revision</strong><div style="margin-top:4px"><code>'+esc(shortHealthRevision(runtime.vcs_revision || checkout.head))+'</code></div></div></div>';
  html += '<div class="msg-item" style="margin:0"><div><strong>Branch</strong><div style="margin-top:4px">'+esc(checkout.branch || "unknown")+'</div></div></div>';
  html += '<div class="msg-item" style="margin:0"><div><strong>Metrics</strong><div style="margin-top:4px">'+esc(String(metrics.status || "unknown").toUpperCase())+'</div></div></div>';
  html += '</div>';

  html += '<div style="font-size:12px;color:var(--muted);line-height:1.6">';
  html += '<div><strong style="color:var(--text)">Checkout</strong>: '+esc(checkout.repo_root || runtime.repo_root || "unknown")+'</div>';
  html += '<div><strong style="color:var(--text)">Binary</strong>: '+esc(runtime.binary_path || "unknown")+'</div>';
  html += '<div><strong style="color:var(--text)">Updated</strong>: '+esc(timeAgo(payload.ts))+'</div>';
  if (metrics.latest_timestamp) html += '<div><strong style="color:var(--text)">Latest Metrics</strong>: '+esc(timeAgo(metrics.latest_timestamp))+'</div>';
  if (semantics.readiness && semantics.readiness.state && String(semantics.readiness.state).toLowerCase() !== "ok") {
    html += '<div style="margin-top:8px"><strong style="color:var(--orange)">Readiness</strong>: '+esc(semantics.readiness.message || semantics.readiness.state)+'</div>';
  }
  if (semantics.degraded && semantics.degraded.state && String(semantics.degraded.state).toLowerCase() !== "ok") {
    html += '<div style="margin-top:8px"><strong style="color:var(--orange)">Degraded</strong>: '+esc(semantics.degraded.message || semantics.degraded.state)+'</div>';
  }
  if (warnings.length) {
    html += '<div style="margin-top:8px"><strong style="color:var(--orange)">Warnings</strong>: '+esc(warnings.join(" | "))+'</div>';
  }
  if (reasons.length) {
    html += '<div style="margin-top:8px"><strong style="color:var(--orange)">Health Reasons</strong>: '+esc(reasons.join(" | "))+'</div>';
  }
  const diagnosticSignals = [];
  for (const [label, signal] of [
    ["Reviewer Scarcity", extended.reviewer_scarcity],
    ["Operator Queue Lag", extended.operator_queue_lag],
    ["Stuck Agents", extended.stuck_agents],
    ["Authority Node", authorityNode],
    ["Authority Lease", authorityLease],
  ]) {
    const state = String(signal && signal.state || '').toLowerCase();
    if (!state || state === "ok" || state === "unsupported") continue;
    let detail = String(signal.message || signal.state || 'unknown');
    if (label === "Reviewer Scarcity") {
      const breakdown = runtimeHealthReviewerScarcityBreakdown(reviewerScarcityHealth);
      if (breakdown) detail += ' [' + breakdown + ']';
    }
    diagnosticSignals.push(label + ': ' + detail);
  }
  const loopIssues = loopReadiness.filter(loop => {
    const state = String(loop && loop.state || '').toLowerCase();
    return state && state !== "running" && state !== "disabled" && state !== "ok";
  });
  if (loopIssues.length) {
    diagnosticSignals.push('Loop Readiness: ' + loopIssues.map(loop => {
      return String(loop.name || 'loop') + '=' + String(loop.state || 'unknown');
    }).join(' | '));
  }
  if (repoMutationActivation.schema || repoMutationActivation.status || typeof repoMutationActivation.mutation_allowed === "boolean") {
    const status = String(repoMutationActivation.status || "unknown");
    const allowed = repoMutationActivation.mutation_allowed ? "mutation allowed" : "fail-closed";
    const reasons = Array.isArray(repoMutationActivation.blocking_reasons) ? repoMutationActivation.blocking_reasons : [];
    const reasonText = reasons.length ? " (" + reasons.slice(0, 3).join("; ") + (reasons.length > 3 ? "; ..." : "") + ")" : "";
    diagnosticSignals.push("Repo Mutation Gate: " + status + " / " + allowed + reasonText);
  }
  if (repoMutationActuatorDryRun.schema || repoMutationActuatorDryRun.status || typeof repoMutationActuatorDryRun.mutation_executed === "boolean" || typeof repoMutationActuatorDryRun.would_mutate === "boolean") {
    const status = String(repoMutationActuatorDryRun.status || "unknown");
    const executed = repoMutationActuatorDryRun.mutation_executed ? "mutation executed" : "no mutation";
    const planned = repoMutationActuatorDryRun.would_mutate ? "would mutate" : "no planned mutation";
    const reasons = Array.isArray(repoMutationActuatorDryRun.blocking_reasons) ? repoMutationActuatorDryRun.blocking_reasons : [];
    const reasonText = reasons.length ? " (" + reasons.slice(0, 3).join("; ") + (reasons.length > 3 ? "; ..." : "") + ")" : "";
    diagnosticSignals.push("Repo Mutation Actuator Dry Run: " + status + " / " + executed + " / " + planned + reasonText);
  }
  if (patchQueueDurability.contract || patchQueueDurability.state || typeof patchQueueDurability.durable === "boolean") {
    const state = String(patchQueueDurability.state || "unknown");
    const durability = patchQueueDurability.durable ? "durable" : "not durable";
    const live = Number(patchQueueDurability.live_item_count || 0);
    const claimed = Number(patchQueueDurability.claimed_item_count || 0);
    diagnosticSignals.push("Patch Queue Durability: " + state + " / " + durability + " (live=" + live + ", claimed=" + claimed + ")");
  }
  if (diagnosticSignals.length) {
    html += '<details class="raw-section" style="margin-top:10px"><summary>Diagnostics · '+diagnosticSignals.length+'</summary><div class="diag-list">';
    diagnosticSignals.forEach(function(sig){ html += '<div class="diag-row">'+esc(sig)+'</div>'; });
    html += '</div></details>';
  }
  if (payload.error) {
    html += '<div style="margin-top:8px;color:var(--red)"><strong>Error</strong>: '+esc(payload.error)+'</div>';
  }
  html += '</div>';
  summary.innerHTML = html;
}

async function loadRuntimeHealth() {
  try {
    const headers = {"Accept": "application/json"};
    if (TOKEN) headers.Authorization = 'Bearer ' + TOKEN;
    const response = await fetch(window.location.origin + "/api/diagnostics", {
      method: "GET",
      headers
    });
    if (!response.ok) {
      throw new Error("runtime diagnostics endpoint returned " + String(response.status));
    }
    runtimeHealthCache = await response.json();
  } catch (e) {
    console.error("loadRuntimeHealth", e);
    runtimeHealthCache = {
      status: "error",
      ts: new Date().toISOString(),
      error: e && e.message ? e.message : "failed to load runtime health"
    };
  }
  renderRuntimeHealth();
}

function showRuntimeHealthDetail() {
  if (!runtimeHealthCache) {
    toast("Runtime health is still loading");
    return;
  }
  const payload = runtimeHealthCache || {};
  const runtime = payload.runtime || {};
  const checkout = payload.checkout || {};
  const metrics = payload.metrics || {};
  const metricsHealth = metrics.health || {};
  const semantics = payload.semantics || {};
  const extended = payload.extended_readiness || {};
  const reviewerScarcityHealth = extended.reviewer_scarcity_health || {};
  const authorityNode = payload.authority_node || {};
  const authorityLease = payload.authority_lease || {};
  const patchQueueDurability = payload.project_patch_queue_durability || {};
  const repoMutationActivation = payload.repo_mutation_activation || {};
  const repoMutationActuatorDryRun = payload.repo_mutation_actuator_dry_run || {};
  const loopReadiness = Array.isArray(payload.loop_readiness) ? payload.loop_readiness : [];
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Status</strong><br>'+esc(String(payload.status || "unknown").toUpperCase())+'</div>';
  html += '<div><strong>Reported</strong><br>'+esc(payload.ts || "unknown")+'</div>';
  html += '<div><strong>Readiness</strong><br>'+esc(String(semantics.readiness && (semantics.readiness.message || semantics.readiness.state) || "unknown"))+'</div>';
  html += '<div><strong>Degraded Signal</strong><br>'+esc(String(semantics.degraded && (semantics.degraded.message || semantics.degraded.state) || "none"))+'</div>';
  html += '<div><strong>Branch</strong><br>'+esc(checkout.branch || "unknown")+'</div>';
  html += '<div><strong>Head</strong><br><code>'+esc(checkout.head || runtime.vcs_revision || "unknown")+'</code></div>';
  html += '<div><strong>Checkout Dirty</strong><br>'+esc(checkout.dirty ? "yes" : "no")+'</div>';
  html += '<div><strong>Build Modified</strong><br>'+esc(runtime.vcs_modified ? "yes" : "no")+'</div>';
  html += '<div><strong>Metrics Status</strong><br>'+esc(String(metrics.status || "unknown").toUpperCase())+'</div>';
  html += '<div><strong>Metrics Verdict</strong><br>'+esc(String(metricsHealth.verdict || "unknown").toUpperCase())+'</div>';
  html += '</div>';
  const rawSections = [
    ['Semantics', semantics],
    ['Extended Readiness', extended],
    ['Reviewer Scarcity Health', reviewerScarcityHealth],
    ['Authority Node', authorityNode],
    ['Authority Lease', authorityLease],
    ['Project Patch Queue Durability', patchQueueDurability],
    ['Repo Mutation Activation', repoMutationActivation],
    ['Repo Mutation Actuator Dry Run', repoMutationActuatorDryRun],
    ['Loop Readiness', loopReadiness],
    ['Config Snapshot', payload.config],
    ['Runtime Build', runtime],
    ['Checkout', checkout],
    ['Metrics', metrics]
  ].filter(function(s){ const v = s[1]; if (v == null) return false; if (Array.isArray(v)) return v.length > 0; if (typeof v === 'object') return Object.keys(v).length > 0; return true; });
  if (rawSections.length) {
    html += '<div class="raw-dump-group"><div class="raw-dump-title">Raw diagnostics · '+rawSections.length+'</div>';
    rawSections.forEach(function(s){ html += '<details class="raw-section"><summary>'+esc(s[0])+'</summary><pre>'+esc(JSON.stringify(s[1], null, 2))+'</pre></details>'; });
    html += '</div>';
  }
  openModal("Runtime Health", html);
}

function updateControlBadge() {
  const pendingOps = buildOperatorInboxItems().length;
  const badge = document.getElementById('control-badge');
  const stat = document.getElementById('s-attention');
  if (stat) stat.textContent = String(pendingOps);
  if (pendingOps > 0) {
    badge.style.display = '';
    badge.textContent = pendingOps;
  } else {
    badge.style.display = 'none';
  }
}

function replayCountsFromReport(report) {
  return {
    sessions: (report && report.sessions ? report.sessions.length : 0),
    queues: (report && report.queues ? report.queues.length : 0),
    claims: (report && report.claims ? report.claims.length : 0),
    execution_runs: (report && report.execution_runs ? report.execution_runs.length : 0),
    events: (report && report.events ? report.events.length : 0)
  };
}

function urgencyRank(urgency) {
  const normalized = String(urgency || "NORMAL").toUpperCase();
  if (normalized === "CRITICAL") return 4;
  if (normalized === "HIGH") return 3;
  if (normalized === "NORMAL") return 2;
  if (normalized === "LOW") return 1;
  return 0;
}

function sessionAttentionRank(status) {
  const normalized = String(status || "").toUpperCase();
  if (normalized === "BLOCKED") return 95;
  if (normalized === "WAITING_DECISION") return 90;
  if (normalized === "HANDOFF_PENDING") return 85;
  return 40;
}

function buildOperatorInboxItems() {
  const items = [];
  const openClaimQueues = new Set(
    operatorQueueCache
      .filter(item => isQueueOpen(item) && String(item.source_kind || "").toLowerCase() === "knowledge_claim" && item.source_id)
      .map(item => item.source_id)
  );

  sessionsCache.forEach(session => {
    const status = String(session.status || "").toUpperCase();
    if (!["BLOCKED", "WAITING_DECISION", "HANDOFF_PENDING"].includes(status)) return;
    items.push({
      key: "session:" + session.session_id,
      kind: "session",
      id: session.session_id,
      title: session.summary || session.session_id || "session",
      summary: sessionContextSummary(session) || "Session needs operator attention",
      meta: status + " | " + (session.agent_id || "unknown"),
      priority: sessionAttentionRank(status),
      timestamp: session.updated_at || session.started_at || "",
      status: status
    });
  });

  operatorQueueCache.forEach(item => {
    if (!isQueueOpen(item)) return;
    const authority = operatorQueueAuthorityFor(item);
    const overdueBoost = isQueueOverdue(item, authority) ? 15 : 0;
    const rebaseBadges = queueRebaseFollowupBadges(item);
    const rebaseSummary = queueRebaseFollowupSummary(item);
    items.push({
      key: "queue:" + item.queue_id,
      kind: "queue",
      id: item.queue_id,
      title: item.title || item.queue_id,
      summary: rebaseSummary || item.summary || item.details || "Operator queue item is open",
      meta: [item.queue_type || "QUEUE", item.urgency || "NORMAL", item.assigned_to || "", isQueueOverdue(item, authority) ? "OVERDUE" : ""].concat(rebaseBadges).filter(Boolean).join(" | "),
      priority: 60 + (urgencyRank(item.urgency) * 10) + overdueBoost,
      timestamp: item.updated_at || item.created_at || "",
      status: item.status || "OPEN",
      time_authority: authority
    });
  });

  claimsCache.forEach(claim => {
    const status = String(claim.status || "").toUpperCase();
    if (!isClaimReviewStatus(status) || openClaimQueues.has(claim.claim_id)) return;
    items.push({
      key: "claim:" + claim.claim_id,
      kind: "claim",
      id: claim.claim_id,
      title: claim.subject || claim.claim_id,
      summary: claim.summary || claim.lifecycle_reason || "Claim requires review attention",
      meta: [status, claim.claim_type || "FACT", claim.agent_id || ""].filter(Boolean).join(" | "),
      priority: status === "DISPUTED" ? 88 : (status === "REVIEW" ? 82 : 78),
      timestamp: claim.updated_at || claim.created_at || "",
      status: status
    });
  });

  compactionCandidatesCache.forEach(item => {
    items.push({
      key: "compaction:" + item.session_id,
      kind: "compaction",
      id: item.session_id,
      title: (item.agent_id || "agent") + " / " + item.session_id,
      summary: "Compaction candidate at " + String(item.total_tokens || item.message_tokens || 0) + " tokens and " + String(item.message_count || 0) + " messages",
      meta: [item.status || "ACTIVE", item.task_id ? ("Task " + item.task_id) : ""].filter(Boolean).join(" | "),
      priority: 55 + Math.min(25, Math.floor(Number(item.total_tokens || 0) / 4000)),
      timestamp: item.last_message_at || item.started_at || "",
      status: item.status || "ACTIVE"
    });
  });

  runtimeEventsCache.forEach(item => {
    const eventType = String(item.event_type || "").toLowerCase();
    if (eventType !== "tool.call.denied" && eventType !== "tool.call.approval_required") return;
    items.push({
      key: "policy:" + item.event_id,
      kind: "policy",
      id: item.event_id,
      title: item.event_type || item.event_id,
      summary: [item.entity_type || "", item.entity_id || "", item.actor_id || item.agent_id || ""].filter(Boolean).join(" | ") || "Capability policy intervention",
      meta: eventType === "tool.call.denied" ? "DENIED" : "REQUIRES APPROVAL",
      priority: eventType === "tool.call.denied" ? 86 : 74,
      timestamp: item.created_at || "",
      status: item.event_type || ""
    });
  });

  dedupeTensionRecords([].concat(tensionsUniverseCache || [], tensionsCache || []))
    .filter(item => String(item.review_status || "").toUpperCase() === "CONFIRMED" && String(item.lifecycle_state || "").toUpperCase() !== "ARCHIVED")
    .forEach(item => {
      const surfaceScore = Number(item.surface_score || 0);
      const authority = tensionAuthorityFor(item);
      items.push({
        key: "tension:" + item.tension_id,
        kind: "tension",
        id: item.tension_id,
        title: item.title || item.tension_id || "tension",
        summary: item.summary || item.proto_cluster_id || "Confirmed tension requires operator follow-through",
        meta: [
          item.tension_type || "tension",
          item.proto_cluster_id || "",
          item.evidence_count ? (String(item.evidence_count) + " evidence") : ""
        ].filter(Boolean).join(" | "),
        priority: 72 + Math.min(24, Math.floor(surfaceScore / 4)),
        timestamp: item.last_seen_at || item.updated_at || item.created_at || "",
        status: item.review_status || "CONFIRMED",
        time_authority: authority
      });
    });

  items.sort((left, right) => {
    if (right.priority !== left.priority) return right.priority - left.priority;
    return Date.parse(right.timestamp || "") - Date.parse(left.timestamp || "");
  });
  return items;
}

function loadOperatorInbox() {
  const kindFilter = String((document.getElementById("ops-inbox-filter") || {}).value || "").trim().toLowerCase();
  const query = String((document.getElementById("ops-inbox-search") || {}).value || "").trim().toLowerCase();
  let items = buildOperatorInboxItems();
  if (kindFilter) {
    items = items.filter(item => item.kind === kindFilter);
  }
  if (query) {
    items = items.filter(item => {
      const haystack = [item.title, item.summary, item.meta, item.status, item.kind].join(" ").toLowerCase();
      return haystack.includes(query);
    });
  }
  operatorInboxCache = items;
  updateControlBadge();
  document.getElementById("ops-inbox-count").textContent = String(items.length);
  const el = document.getElementById("ops-inbox-list");
  if (!items.length) {
    el.innerHTML = '<div class="empty">No active attention signals. Runtime control plane is calm.</div>';
    return;
  }
  el.innerHTML = items.map(item =>
    '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showOperatorInboxItem((item.key))}) + '>' +
      '<div class="action-title">'+esc(item.title)+'</div>' +
      '<div class="action-meta">' +
        '<span class="action-status '+esc(String(item.status || item.kind).toUpperCase())+'">'+esc(String(item.kind || "signal").toUpperCase())+'</span>' +
        '<span>'+esc(item.meta || "")+'</span>' +
        '<span>'+timeAgo(item.timestamp, item.time_authority || null)+'</span>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(item.summary || "Needs attention")+'</div>' +
    '</div>'
  ).join("");
}

function claimReviewStatuses() {
  return ["REVIEW", "DISPUTED", "STALE", "SUPERSEDED"];
}

function claimReviewRelatedIDs(claim) {
  return Array.from(new Set([
    claim && claim.conflicts_claim_id,
    claim && claim.supersedes_claim_id,
    claim && claim.superseded_by_claim_id
  ].filter(Boolean)));
}

function buildClaimReviewWorkbenchItems() {
  const statuses = new Set(claimReviewStatuses());
  return claimsCache
    .filter(claim => statuses.has(String(claim.status || "").toUpperCase()))
    .map(claim => {
      const status = String(claim.status || "").toUpperCase();
      const relatedClaims = claimReviewRelatedIDs(claim)
        .map(id => claimsCache.find(item => item.claim_id === id))
        .filter(Boolean);
      const relatedOps = operatorQueueCache.filter(item =>
        isQueueOpen(item) &&
        String(item.source_kind || "").toLowerCase() === "knowledge_claim" &&
        item.source_id === claim.claim_id
      );
      let priority = status === "DISPUTED" ? 100 : (status === "REVIEW" ? 92 : (status === "STALE" ? 84 : 76));
      if (relatedOps.some(isQueueOverdue)) priority += 8;
      if (relatedClaims.some(item => String(item.status || "").toUpperCase() === "DISPUTED")) priority += 4;
      return {
        claim: claim,
        status: status,
        relatedClaims: relatedClaims,
        relatedOps: relatedOps,
        priority: priority,
        timestamp: claim.review_due_at || claim.updated_at || claim.created_at || ""
      };
    })
    .sort((left, right) => {
      if (right.priority !== left.priority) return right.priority - left.priority;
      return Date.parse(right.timestamp || "") - Date.parse(left.timestamp || "");
    });
}

function loadClaimReviewWorkbench() {
  const statusFilter = String((document.getElementById("claim-review-status") || {}).value || "").trim().toUpperCase();
  const query = String((document.getElementById("claim-review-search") || {}).value || "").trim().toLowerCase();
  let items = buildClaimReviewWorkbenchItems();
  if (statusFilter) {
    items = items.filter(item => item.status === statusFilter);
  }
  if (query) {
    items = items.filter(item => {
      const haystack = [
        item.claim.subject,
        item.claim.summary,
        item.claim.body,
        item.claim.lifecycle_reason,
        item.status,
        item.relatedClaims.map(related => [related.subject, related.summary, related.status].join(" ")).join(" "),
        item.relatedOps.map(op => [op.title, op.summary, op.assigned_to].join(" ")).join(" ")
      ].join(" ").toLowerCase();
      return haystack.includes(query);
    });
  }

  document.getElementById("claim-review-count").textContent = String(items.length);
  const el = document.getElementById("claim-review-list");
  if (!items.length) {
    el.innerHTML = '<div class="empty">No claim reviews need active operator focus.</div>';
    return;
  }

  el.innerHTML = items.map(item => {
    const claim = item.claim;
    const relatedLabel = item.relatedClaims.length ? (item.relatedClaims.length + " related") : "no linked claims";
    const queueLabel = item.relatedOps.length ? (item.relatedOps.length + " queue") : "no queue";
    const dueLabel = claim.review_due_at ? (isQueueOverdue({due_at: claim.review_due_at, status: "OPEN", time_authority: operatorQueueTimeAuthority}, operatorQueueTimeAuthority) ? "OVERDUE" : ("due " + claim.review_due_at)) : "";
    return '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showClaimReviewWorkbench((claim.claim_id))}) + '>' +
      '<div class="action-title">'+esc(claim.subject || claim.claim_id)+'</div>' +
      '<div class="action-meta">' +
        '<span class="action-status '+esc(item.status)+'">'+esc(item.status)+'</span>' +
        '<span>'+esc(claim.claim_type || "FACT")+'</span>' +
        '<span>'+esc(relatedLabel)+'</span>' +
        '<span>'+esc(queueLabel)+'</span>' +
        (dueLabel ? '<span>'+esc(dueLabel)+'</span>' : '') +
        '<span>'+esc(timeAgo(item.timestamp))+'</span>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(claim.summary || claim.lifecycle_reason || "Claim review requires operator decision.")+'</div>' +
    '</div>';
  }).join("");
}

function showClaimReviewWorkbench(claimId) {
  const claim = claimsCache.find(item => item.claim_id === claimId);
  if (!claim) return;
  const relatedClaims = claimReviewRelatedIDs(claim)
    .map(id => claimsCache.find(item => item.claim_id === id) || {claim_id: id, subject: id, body: "Claim not loaded in current cache", status: "UNKNOWN"})
    .filter(Boolean);
  const relatedOps = operatorQueueCache.filter(item =>
    isQueueOpen(item) &&
    String(item.source_kind || "").toLowerCase() === "knowledge_claim" &&
    item.source_id === claim.claim_id
  );
  const status = String(claim.status || "ACTIVE").toUpperCase();
  let html = '<div style="display:grid;grid-template-columns:minmax(0,1.1fr) minmax(0,.9fr);gap:12px;margin-bottom:12px">';
  html += '<div class="msg-item" style="margin:0;background:var(--card)">';
  html += '<div style="font-size:12px;font-weight:700;margin-bottom:8px">Primary Claim</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;font-size:12px;margin-bottom:10px">';
  html += '<div><strong>Status</strong><br>'+esc(status)+'</div>';
  html += '<div><strong>Type</strong><br>'+esc(claim.claim_type || "FACT")+'</div>';
  html += '<div><strong>Confidence</strong><br>'+esc((claim.confidence || 0).toFixed ? (claim.confidence || 0).toFixed(2) : claim.confidence || 0)+'</div>';
  html += '<div><strong>Updated</strong><br>'+esc(timeAgo(claim.updated_at || claim.created_at))+'</div>';
  html += '</div>';
  if (claim.summary) html += '<div style="margin-bottom:8px"><strong>Summary</strong><div style="margin-top:4px">'+esc(claim.summary)+'</div></div>';
  if (claim.lifecycle_reason) html += '<div style="margin-bottom:8px"><strong>Lifecycle Reason</strong><div style="margin-top:4px">'+esc(claim.lifecycle_reason)+'</div></div>';
  html += '<div><strong>Body</strong><pre>'+esc(claim.body || "No body recorded.")+'</pre></div>';
  html += '</div>';
  html += '<div>';
  html += '<div class="msg-item" style="margin:0 0 10px;background:var(--card)"><div style="font-size:12px;font-weight:700;margin-bottom:8px">Related Claims</div>';
  if (relatedClaims.length) {
    html += relatedClaims.map(related =>
      '<div style="border:1px solid var(--border);border-radius:8px;padding:8px 10px;margin-bottom:8px;background:var(--surface)">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showClaimDetail((related.claim_id))}) + ' style="color:var(--accent);font-weight:600">'+esc(related.subject || related.claim_id)+'</a><span class="task-tag">'+esc(String(related.status || "UNKNOWN").toUpperCase())+'</span></div>' +
        (related.summary ? '<div style="font-size:11px;color:var(--muted);margin-top:4px">'+esc(related.summary)+'</div>' : '') +
      '</div>'
    ).join("");
  } else {
    html += '<div class="empty" style="margin:0">No related claims linked to this review.</div>';
  }
  html += '</div>';
  html += '<div class="msg-item" style="margin:0;background:var(--card)"><div style="font-size:12px;font-weight:700;margin-bottom:8px">Follow-Up Queue</div>';
  if (relatedOps.length) {
    html += relatedOps.map(item =>
      '<div style="border:1px solid var(--border);border-radius:8px;padding:8px 10px;margin-bottom:8px;background:var(--surface)">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showOperatorQueueDetail((item.queue_id))}) + ' style="color:var(--accent);font-weight:600">'+esc(item.title || item.queue_id)+'</a><span class="task-tag">'+esc(item.urgency || "NORMAL")+'</span></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">'+esc(item.queue_type || "FOLLOW_UP")+(item.assigned_to ? ' · '+esc(item.assigned_to) : '')+(queueMetaBadges(item).length ? ' · '+esc(queueMetaBadges(item).join(' · ')) : '')+'</div>' +
      '</div>'
    ).join("");
  } else {
    html += '<div class="empty" style="margin:0">No open queue item linked to this claim.</div>';
  }
  html += '</div>';
  html += '</div>';
  html += '</div>';
  html += '<div class="action-btn-row">';
  html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();showClaimDetail((claim.claim_id))}) + '>Open Claim Detail</button>';
  if (status !== 'CONFIRMED') html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'confirm')}) + '>Confirm</button>';
  if (status !== 'REVIEW') html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'review')}) + '>Request Review</button>';
  if (status !== 'DISPUTED') html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'dispute')}) + '>Dispute</button>';
  if (status !== 'SUPERSEDED') html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'supersede')}) + '>Supersede</button>';
  if (status !== 'STALE') html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'stale')}) + '>Mark Stale</button>';
  html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'escalate')}) + '>Escalate</button>';
  html += '<button class="btn-session-danger" ' + dashboardAction(function(dashboardEvent){archiveClaimFromModal((claim.claim_id))}) + '>Archive</button>';
  html += '</div>';
  openModal("Claim Review · " + esc(claim.subject || claim.claim_id), html);
}

function showOperatorInboxItem(key) {
  const item = operatorInboxCache.find(entry => entry.key === key);
  if (!item) return;
  if (item.kind === "session") {
    showSessionDetail(item.id);
    return;
  }
  if (item.kind === "queue") {
    showOperatorQueueDetail(item.id);
    return;
  }
  if (item.kind === "claim") {
    showClaimDetail(item.id);
    return;
  }
  if (item.kind === "policy") {
    showRuntimeEventDetail(item.id);
    return;
  }
  if (item.kind === "compaction") {
    showSessionDetail(item.id);
    return;
  }
  if (item.kind === "tension") {
    switchTab('tensions');
    showTensionDetail(item.id);
  }
}

function currentReplayFilters() {
  const limitValue = Number((document.getElementById("replay-filter-limit") || {}).value || 200);
  return {
    workspace_id: WS_ID,
    agent_id: String((document.getElementById("replay-filter-agent") || {}).value || "").trim(),
    session_id: String((document.getElementById("replay-filter-session") || {}).value || "").trim(),
    task_id: String((document.getElementById("replay-filter-task") || {}).value || "").trim(),
    limit: Number.isFinite(limitValue) && limitValue > 0 ? limitValue : 200,
    include_events: !!((document.getElementById("replay-include-events") || {}).checked)
  };
}

function resetReplayWorkbench() {
  document.getElementById("replay-filter-agent").value = "";
  document.getElementById("replay-filter-session").value = "";
  document.getElementById("replay-filter-task").value = "";
  document.getElementById("replay-filter-limit").value = "200";
  document.getElementById("replay-include-events").checked = false;
  replayReportCache = null;
  replayEvaluationCache = null;
  document.getElementById("replay-verdict-badge").textContent = "idle";
  document.getElementById("replay-summary").innerHTML = '<div class="empty">Run evaluation to inspect runtime journal health.</div>';
  document.getElementById("replay-findings-list").innerHTML = '<div class="empty">No replay findings yet.</div>';
}

async function runReplayWorkbench(mode = "evaluate") {
  const filters = currentReplayFilters();
  document.getElementById("replay-verdict-badge").textContent = "running";
  document.getElementById("replay-summary").innerHTML = '<div class="empty">Running ' + esc(mode) + "...</div>";
  try {
    if (mode === "replay") {
      const response = await rpc("workspace.events.replay", filters);
      replayReportCache = response && response.report ? response.report : null;
      replayEvaluationCache = replayReportCache ? {
        workspace_id: replayReportCache.workspace_id,
        truncated: replayReportCache.truncated,
        filter: replayReportCache.filter,
        metrics: replayReportCache.metrics,
        evaluation: replayReportCache.evaluation,
        counts: replayCountsFromReport(replayReportCache)
      } : null;
    } else {
      replayEvaluationCache = await rpc("workspace.events.evaluate", filters);
      if (replayReportCache && JSON.stringify(replayReportCache.filter || {}) !== JSON.stringify({
        workspace_id: filters.workspace_id,
        agent_id: filters.agent_id,
        session_id: filters.session_id,
        task_id: filters.task_id,
        limit: filters.limit
      })) {
        replayReportCache = null;
      }
    }
    renderReplayWorkbench();
    toast(mode === "replay" ? "Replay report updated" : "Replay evaluation updated");
  } catch (e) {
    console.error("runReplayWorkbench", e);
    document.getElementById("replay-verdict-badge").textContent = "error";
    document.getElementById("replay-summary").innerHTML = '<div class="empty">' + esc(e.message || "Replay failed") + '</div>';
    document.getElementById("replay-findings-list").innerHTML = '<div class="empty">Replay failed.</div>';
  }
}

function renderReplayWorkbench() {
  const summary = replayEvaluationCache || (replayReportCache ? {
    workspace_id: replayReportCache.workspace_id,
    time_authority: replayReportCache.time_authority,
    truncated: replayReportCache.truncated,
    filter: replayReportCache.filter,
    metrics: replayReportCache.metrics,
    evaluation: replayReportCache.evaluation,
    events_order: replayReportCache.events_order,
    applied_order: replayReportCache.applied_order,
    counts: replayCountsFromReport(replayReportCache)
  } : null);
  if (!summary) {
    resetReplayWorkbench();
    return;
  }
  const verdict = String((summary.evaluation && summary.evaluation.verdict) || "unknown").toUpperCase();
  document.getElementById("replay-verdict-badge").textContent = verdict;
  const counts = summary.counts || {};
  const metrics = summary.metrics || {};
  const replayTimeAuthority = summary.time_authority || null;
  const eventsOrder = String(summary.events_order || "latest_first_ingest");
  const appliedOrder = String(summary.applied_order || "causal_parent_before_child");
  const retentionRisk = (summary.evaluation && summary.evaluation.retention_risk) || null;
  const findingSummary = (summary.evaluation && summary.evaluation.finding_summary) || null;
  const provenanceSummary = (summary.evaluation && summary.evaluation.provenance_summary) || null;
  let html = '<div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-bottom:12px">';
  html += '<div class="msg-item"><strong>Events</strong><div style="margin-top:4px">'+esc(String(metrics.total_events || counts.events || 0))+'</div></div>';
  html += '<div class="msg-item"><strong>Sessions / Queues</strong><div style="margin-top:4px">'+esc(String(counts.sessions || 0))+' / '+esc(String(counts.queues || 0))+'</div></div>';
  html += '<div class="msg-item"><strong>Claims / Runs</strong><div style="margin-top:4px">'+esc(String(counts.claims || 0))+' / '+esc(String(counts.execution_runs || 0))+'</div></div>';
  html += '<div class="msg-item"><strong>Ingest Order</strong><div style="margin-top:4px">'+esc(eventsOrder)+'</div></div>';
  html += '<div class="msg-item"><strong>Apply Order</strong><div style="margin-top:4px">'+esc(appliedOrder)+'</div></div>';
  html += '</div>';
  html += '<div class="msg-item" style="margin-bottom:12px"><strong>Verdict</strong><div style="margin-top:4px">'+esc(verdict)+' | warnings '+esc(String((summary.evaluation && summary.evaluation.warning_count) || 0))+' | errors '+esc(String((summary.evaluation && summary.evaluation.error_count) || 0))+(summary.truncated ? ' | truncated' : '')+'</div></div>';
  if (findingSummary && Number(findingSummary.total_findings || 0) > 0) {
    html += '<div class="msg-item" style="margin-bottom:12px"><strong>Finding Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Bounded family rollups over the current replay findings; inspectability only, not certified replay correctness.</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Total</strong><br>'+esc(String(findingSummary.total_findings || 0))+'</div>';
    html += '<div><strong>Errors</strong><br>'+esc(String(findingSummary.error_finding_count || 0))+'</div>';
    html += '<div><strong>Warnings</strong><br>'+esc(String(findingSummary.warning_finding_count || 0))+'</div>';
    html += '<div><strong>Info</strong><br>'+esc(String(findingSummary.info_finding_count || 0))+'</div>';
    html += '</div>';
    html += '<div style="margin-top:8px;display:flex;gap:6px;flex-wrap:wrap">';
    html += '<span class="tool-badge kind">Dedup Conflicts ' + esc(String(findingSummary.dedup_conflict_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">Causal Order ' + esc(String(findingSummary.causal_order_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">Missing Parents ' + esc(String(findingSummary.missing_parent_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">Cycle-Affected ' + esc(String(findingSummary.cycle_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">Scope Partial ' + esc(String(findingSummary.scope_partial_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">Retention Findings ' + esc(String(findingSummary.retention_finding_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">Other Findings ' + esc(String(findingSummary.other_finding_count || 0)) + '</span>';
    html += '</div></div>';
  }
  if (provenanceSummary && (Number(provenanceSummary.total_findings_with_source_event || 0) > 0 || Number(findingSummary && findingSummary.total_findings || 0) > 0)) {
    html += '<div class="msg-item" style="margin-bottom:12px"><strong>Provenance Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Additive provenance visibility over the current replay findings; inspectability only, not complete causal history or immutable-audit authority.</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Source Event</strong><br>'+esc(String(provenanceSummary.total_findings_with_source_event || 0))+'</div>';
    html += '<div><strong>Root Cause</strong><br>'+esc(String(provenanceSummary.findings_with_root_cause_id || 0))+'</div>';
    html += '<div><strong>Prov Group</strong><br>'+esc(String(provenanceSummary.findings_with_provenance_group_id || 0))+'</div>';
    html += '<div><strong>Parent Refs</strong><br>'+esc(String(provenanceSummary.findings_with_parent_refs || 0))+'</div>';
    html += '<div><strong>Full Lineage Fields</strong><br>'+esc(String(provenanceSummary.full_lineage_field_finding_count || 0))+'</div>';
    html += '</div></div>';
  }
  if (retentionRisk && (retentionRisk.band || retentionRisk.compaction_candidate_count || retentionRisk.compaction_snapshot_count || retentionRisk.episode_pack_count)) {
    const retentionReasons = Array.isArray(retentionRisk.reasons) ? retentionRisk.reasons.filter(Boolean) : [];
    const candidateSessions = Array.isArray(retentionRisk.candidate_session_ids) ? retentionRisk.candidate_session_ids.filter(Boolean) : [];
    const snapshotSessions = Array.isArray(retentionRisk.snapshot_session_ids) ? retentionRisk.snapshot_session_ids.filter(Boolean) : [];
    html += '<div class="msg-item" style="margin-bottom:12px"><strong>Retention Risk</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Inspectable retention/compaction risk over existing replay and session-compaction read-side artifacts; no immutable-audit guarantee.</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Band</strong><br>'+esc(String(retentionRisk.band || "CLEAR"))+'</div>';
    html += '<div><strong>Compaction Candidates</strong><br>'+esc(String(retentionRisk.compaction_candidate_count || 0))+'</div>';
    html += '<div><strong>Compaction Snapshots</strong><br>'+esc(String(retentionRisk.compaction_snapshot_count || 0))+'</div>';
    html += '<div><strong>Episode Packs</strong><br>'+esc(String(retentionRisk.episode_pack_count || 0))+'</div>';
    html += '</div>';
    if (retentionRisk.latest_snapshot_at) {
      html += '<div style="font-size:11px;color:var(--muted);margin-top:8px"><strong>Latest Snapshot</strong> ' + esc(timeAgo(retentionRisk.latest_snapshot_at, replayTimeAuthority)) + '</div>';
    }
    if (candidateSessions.length || snapshotSessions.length) {
      html += '<div style="font-size:11px;color:var(--muted);margin-top:8px">';
      if (candidateSessions.length) {
        html += '<strong>Candidate Sessions</strong> ' + esc(candidateSessions.join(", "));
      }
      if (snapshotSessions.length) {
        if (candidateSessions.length) html += ' | ';
        html += '<strong>Compacted Sessions</strong> ' + esc(snapshotSessions.join(", "));
      }
      html += '</div>';
    }
    if (retentionReasons.length) {
      html += '<div style="font-size:11px;color:var(--muted);margin-top:8px"><strong>Reasons</strong> ' + esc(retentionReasons.join(", ")) + '</div>';
    }
    html += '</div>';
  }
  document.getElementById("replay-summary").innerHTML = html;

  const findings = (summary.evaluation && summary.evaluation.findings) || [];
  const findingsEl = document.getElementById("replay-findings-list");
  if (!findings.length) {
    findingsEl.innerHTML = '<div class="empty">No replay findings. Runtime journal looks coherent for this filter.</div>';
    return;
  }
  findingsEl.innerHTML = findings.map((finding, idx) =>
    '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showReplayFindingDetail(String(idx))}) + '>' +
      '<div class="action-title">'+esc(finding.code || "finding")+'</div>' +
      '<div class="action-meta">' +
        '<span class="action-status '+esc(String(finding.severity || "info").toUpperCase())+'">'+esc(String(finding.severity || "info").toUpperCase())+'</span>' +
        '<span>'+esc(finding.entity_type || "")+'</span>' +
        '<span>'+esc(finding.entity_id || "")+'</span>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(finding.message || "")+'</div>' +
    '</div>'
  ).join("");
}

function openReplayEntity(entityType, entityID) {
  const normalizedType = String(entityType || "").toLowerCase();
  const normalizedID = String(entityID || "").trim();
  if (!normalizedID) return;
  if (normalizedType === "agent_session" || normalizedType === "session") {
    showSessionDetail(normalizedID);
    return;
  }
  if (normalizedType === "operator_queue") {
    showOperatorQueueDetail(normalizedID);
    return;
  }
  if (normalizedType === "knowledge_claim") {
    showClaimDetail(normalizedID);
    return;
  }
  if (normalizedType === "execution_run") {
    showExecutionRunDetail(normalizedID);
    return;
  }
  toast("No dashboard entity route for " + normalizedType);
}

function showReplayFindingDetail(index) {
  const summary = replayEvaluationCache || (replayReportCache ? {evaluation: replayReportCache.evaluation} : null);
  const findings = summary && summary.evaluation ? (summary.evaluation.findings || []) : [];
  const finding = findings[index];
  if (!finding) return;
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Code</strong><br>'+esc(finding.code || "finding")+'</div>';
  html += '<div><strong>Severity</strong><br>'+esc(String(finding.severity || "info").toUpperCase())+'</div>';
  html += '<div><strong>Entity Type</strong><br>'+esc(finding.entity_type || "-")+'</div>';
  html += '<div><strong>Entity ID</strong><br>'+esc(finding.entity_id || "-")+'</div>';
  html += '</div>';
  if (finding.source_event_id || finding.source_event_type || finding.source_dedup_key || finding.source_root_cause_id || finding.source_provenance_group_id || finding.source_parent_refs_json) {
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
    html += '<div><strong>Source Event ID</strong><br>'+esc(finding.source_event_id || "-")+'</div>';
    html += '<div><strong>Source Event Type</strong><br>'+esc(finding.source_event_type || "-")+'</div>';
    html += '<div><strong>Source Dedup Key</strong><br>'+esc(finding.source_dedup_key || "-")+'</div>';
    html += '<div><strong>Source Root Cause</strong><br>'+esc(finding.source_root_cause_id || "-")+'</div>';
    html += '<div><strong>Source Group</strong><br>'+esc(finding.source_provenance_group_id || "-")+'</div>';
    html += '<div><strong>Source Parent Refs</strong><br>'+esc(finding.source_parent_refs_json || "-")+'</div>';
    html += '</div>';
  }
  html += '<div style="margin-bottom:12px"><strong>Message</strong><div class="msg-item" style="margin-top:6px">'+esc(finding.message || "")+'</div></div>';
  if (finding.entity_type && finding.entity_id) {
    html += '<div class="action-btn-row"><button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){closeModal();openReplayEntity((finding.entity_type), (finding.entity_id))}) + '>Open Entity</button></div>';
  }
  openModal("Replay Finding", html);
}

function openReplayReportModal() {
  if (!replayReportCache) {
    toast("Run replay first to open the full report");
    return;
  }
  openModal("Replay Report", '<pre>'+esc(JSON.stringify(replayReportCache, null, 2))+'</pre>');
}

function executionStatusTone(status) {
  const normalized = String(status || "PENDING").toUpperCase();
  if (normalized === "COMPLETED") return "var(--green)";
  if (normalized === "ACTIVE") return "#38bdf8";
  if (normalized === "BLOCKED") return "var(--yellow)";
  if (normalized === "FAILED") return "var(--red)";
  if (normalized === "SKIPPED") return "var(--faint)";
  return "#6366f1";
}

function renderExecutionGraph(run, steps) {
  if (!steps || !steps.length) return "";
  const grouped = {PLAN: [], EXECUTE: [], VERIFY: [], OTHER: []};
  const stepMap = {};
  const statusCounts = {};
  steps.forEach(step => {
    stepMap[step.step_id] = step;
    const phase = String(step.phase || "").toUpperCase();
    if (grouped[phase]) grouped[phase].push(step);
    else grouped.OTHER.push(step);
    const status = String(step.status || "PENDING").toUpperCase();
    statusCounts[status] = (statusCounts[status] || 0) + 1;
  });
  Object.keys(grouped).forEach(key => {
    grouped[key].sort((left, right) => {
      const leftOrder = Number(left.sort_order || 0);
      const rightOrder = Number(right.sort_order || 0);
      if (leftOrder !== rightOrder) return leftOrder - rightOrder;
      return String(left.title || left.step_id).localeCompare(String(right.title || right.step_id));
    });
  });
  let html = '<div style="margin-bottom:12px"><strong>Execution Graph</strong>';
  html += '<div style="margin:8px 0 10px">'+Object.keys(statusCounts).sort().map(status => '<span class="task-tag" style="margin-right:6px">'+esc(status)+': '+esc(String(statusCounts[status]))+'</span>').join("")+'</div>';
  const columns = ["PLAN", "EXECUTE", "VERIFY"];
  if (grouped.OTHER.length) columns.push("OTHER");
  html += '<div style="display:grid;grid-template-columns:repeat('+String(columns.length)+', minmax(0, 1fr));gap:10px">';
  columns.forEach(phase => {
    const phaseSteps = grouped[phase] || [];
    html += '<div class="msg-item" style="background:var(--card)"><div style="font-size:12px;font-weight:700;margin-bottom:8px">'+esc(phase)+'</div>';
    if (!phaseSteps.length) {
      html += '<div class="empty" style="margin:0">No '+esc(phase.toLowerCase())+' steps</div>';
    } else {
      phaseSteps.forEach(step => {
        const tone = executionStatusTone(step.status);
        const parent = step.parent_step_id && stepMap[step.parent_step_id] ? (stepMap[step.parent_step_id].title || step.parent_step_id) : step.parent_step_id;
        html += '<div style="border-left:3px solid '+tone+';background:var(--surface);border-radius:8px;padding:8px 10px;margin-bottom:8px">';
        html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><div style="font-weight:600">'+esc(step.title || step.step_id)+'</div><span class="task-tag">'+esc(String(step.status || "PENDING").toUpperCase())+'</span></div>';
        if (step.summary) html += '<div style="font-size:11px;color:var(--muted);margin-top:4px">'+esc(step.summary)+'</div>';
        html += '<div style="font-size:10px;color:var(--muted);margin-top:6px">'+esc("order " + String(step.sort_order || 0));
        if (parent) html += " | " + esc("parent " + parent);
        if (step.evidence && step.evidence.length) html += " | " + esc(String(step.evidence.length) + " evidence");
        html += '</div>';
        html += '</div>';
      });
    }
    html += '</div>';
  });
  html += '</div></div>';
  return html;
}

function openReplayFromExecutionRun(runId) {
  const detail = executionRunDetailCache[runId] || {};
  const run = detail.run || executionRunsCache.find(item => item.run_id === runId);
  if (!run) {
    toast("Execution run not found");
    return;
  }
  document.getElementById("replay-filter-agent").value = run.agent_id || "";
  document.getElementById("replay-filter-session").value = run.session_id || "";
  document.getElementById("replay-filter-task").value = run.task_id || "";
  switchTab("control");
  setTimeout(() => { runReplayWorkbench("evaluate"); }, 50);
}

async function openOperatorQueueComposer(queueId = '') {
  const existing = queueId ? operatorQueueCache.find(x => x.queue_id === queueId) : null;
  const queueKey = await dashboardPrompt('Queue key:', existing ? (existing.queue_key || '') : ('manual:' + dashboardGeneratedID('queue')));
  if (queueKey === null) return;
  if (!String(queueKey || '').trim()) {
    toast('Queue key is required');
    return;
  }
  const title = await dashboardPrompt('Queue title:', existing ? (existing.title || '') : '');
  if (title === null) return;
  if (!String(title || '').trim()) {
    toast('Queue title is required');
    return;
  }
  const queueType = await dashboardPrompt('Queue type (BLOCKER/DECISION/HANDOFF/FOLLOW_UP):', existing ? (existing.queue_type || 'BLOCKER') : 'BLOCKER');
  if (queueType === null) return;
  const summary = await dashboardPrompt('Optional queue summary:', existing ? (existing.summary || '') : '');
  if (summary === null) return;
  const details = await dashboardPrompt('Optional queue details:', existing ? (existing.details || '') : '');
  if (details === null) return;
  const assignedTo = await dashboardPrompt('Optional assignee:', existing ? (existing.assigned_to || '') : '');
  if (assignedTo === null) return;
  const urgency = await dashboardPrompt('Urgency (LOW/NORMAL/HIGH/CRITICAL):', existing ? (existing.urgency || 'NORMAL') : 'NORMAL');
  if (urgency === null) return;
  const dueAt = await dashboardPrompt('Optional due timestamp (RFC3339):', existing ? (existing.due_at || '') : '');
  if (dueAt === null) return;
  const sourceKind = await dashboardPrompt('Optional source kind:', existing ? (existing.source_kind || 'manual') : 'manual');
  if (sourceKind === null) return;
  const sourceID = await dashboardPrompt('Optional source id:', existing ? (existing.source_id || '') : '');
  if (sourceID === null) return;
  const taskID = await dashboardPrompt('Optional task id:', existing ? (existing.task_id || '') : '');
  if (taskID === null) return;
  const sessionID = await dashboardPrompt('Optional session id:', existing ? (existing.session_id || '') : '');
  if (sessionID === null) return;
  const agentID = await dashboardPrompt('Optional agent id:', existing ? (existing.agent_id || '') : '');
  if (agentID === null) return;
  const keepPeers = await dashboardPrompt('Keep peer sessions active? (true/false)', existing ? String(!!existing.keep_session_active) : 'false');
  if (keepPeers === null) return;
  try {
    const response = await rpc('workspace.ops.upsert', {
      workspace_id: WS_ID,
      queue_id: existing ? existing.queue_id : undefined,
      queue_key: String(queueKey || '').trim(),
      queue_type: String(queueType || '').trim(),
      title: String(title || '').trim(),
      summary: String(summary || '').trim(),
      details: String(details || '').trim(),
      assigned_to: String(assignedTo || '').trim(),
      urgency: String(urgency || '').trim(),
      due_at: String(dueAt || '').trim(),
      source_kind: String(sourceKind || '').trim(),
      source_id: String(sourceID || '').trim(),
      task_id: String(taskID || '').trim(),
      session_id: String(sessionID || '').trim(),
      agent_id: String(agentID || '').trim(),
      keep_session_active: boolPromptDefault(keepPeers, false),
      current_revision: existing && Number(existing.revision || 0) > 0 ? Number(existing.revision) : undefined,
      current_updated_at: existing ? String(existing.updated_at || '').trim() : undefined
    });
    toast(existing ? 'Queue item updated' : 'Queue item created');
    await Promise.all([loadOperatorQueue(), loadRuntimeEvents()]);
    const created = response && response.item ? response.item : null;
    if (created && created.queue_id) showOperatorQueueDetail(created.queue_id);
  } catch (e) {
    console.error('workspace.ops.upsert', e);
    toast('Queue update failed: ' + e.message);
  }
}

async function loadOperatorQueue() {
  try {
    const params = {workspace_id: WS_ID, limit: 20};
    const status = document.getElementById('ops-filter-status').value;
    const queueType = document.getElementById('ops-filter-type').value;
    if (status) params.status = status;
    if (queueType) params.queue_type = queueType;
    const r = await rpc('workspace.ops.list', params);
    operatorQueueTimeAuthority = r.time_authority || null;
    operatorQueueCache = r.items || [];
    updateControlBadge();
    document.getElementById('ops-count').textContent = operatorQueueCache.length;
    const el = document.getElementById('ops-list');
    if (!operatorQueueCache.length) {
      el.innerHTML = '<div class="empty">Operator queue is clear.</div>';
      return;
    }
    el.innerHTML = operatorQueueCache.map(item => {
      const authority = operatorQueueAuthorityFor(item, operatorQueueTimeAuthority);
      const lifecycleEvidence = queueLifecycleEvidence(item, authority);
      const rebaseBadges = queueRebaseFollowupBadges(item);
      const rebaseSummary = queueRebaseFollowupSummary(item);
      return '<div class="action-card'+(!isQueueOpen(item) ? ' resolved' : '')+'" ' + dashboardAction(function(dashboardEvent){showOperatorQueueDetail((item.queue_id))}) + '>' +
        '<div class="action-title">'+esc(item.title || item.queue_id)+'</div>' +
        '<div class="action-meta">' +
          '<span class="action-status '+esc(item.status || 'OPEN')+'">'+esc(item.status || 'OPEN')+(isQueueOverdue(item, authority) ? ' · OVERDUE' : '')+'</span>' +
          '<span>'+esc(item.queue_type || 'QUEUE')+'</span>' +
          '<span>'+esc(item.urgency || 'NORMAL')+'</span>' +
          (item.due_at ? '<span>'+esc(isQueueOverdue(item, authority) ? 'OVERDUE' : ('due ' + item.due_at))+'</span>' : '') +
          (item.escalation_count ? '<span>'+esc('esc ' + item.escalation_count)+'</span>' : '') +
          rebaseBadges.map(part => '<span>'+esc(part)+'</span>').join('') +
          '<span>'+timeAgo(item.updated_at || item.created_at, authority)+'</span>' +
        '</div>' +
        ((item.summary || rebaseSummary) ? '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(item.summary || rebaseSummary)+'</div>' : '') +
        (lifecycleEvidence.length ? '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(lifecycleEvidence.join(' | '))+'</div>' : '') +
      '</div>';
    }).join('');
  } catch (e) {
    console.error('loadOperatorQueue', e);
    operatorQueueTimeAuthority = null;
    document.getElementById('ops-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load operator queue')+'</div>';
  }
  loadOperatorInbox();
  loadClaimReviewWorkbench();
}

async function showOperatorQueueDetail(queueId) {
  const item = operatorQueueCache.find(x => x.queue_id === queueId);
  if (!item) return;
  const authority = operatorQueueAuthorityFor(item, operatorQueueTimeAuthority);
  const lifecycleEvidence = queueLifecycleEvidence(item, authority);
  const rebasePayload = queueRebaseFollowupPayload(item);
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Status</strong><br>'+esc(item.status || 'OPEN')+(isQueueOverdue(item) ? ' · OVERDUE' : '')+'</div>';
  html += '<div><strong>Queue Type</strong><br>'+esc(item.queue_type || 'QUEUE')+'</div>';
  html += '<div><strong>Urgency</strong><br>'+esc(item.urgency || 'NORMAL')+'</div>';
  html += '<div><strong>Keep Session Active</strong><br>'+esc(item.keep_session_active ? 'yes' : 'no')+'</div>';
  if (item.assigned_to) html += '<div><strong>Assigned To</strong><br>'+esc(item.assigned_to)+'</div>';
  if (item.due_at) html += '<div><strong>Due</strong><br>'+esc(item.due_at)+(isQueueOverdue(item) ? ' (OVERDUE)' : '')+'</div>';
  if (item.escalation_count) html += '<div><strong>Escalations</strong><br>'+esc(String(item.escalation_count))+'</div>';
  if (item.last_escalated_at) html += '<div><strong>Last Escalated</strong><br>'+esc(timeAgo(item.last_escalated_at, authority))+(item.last_escalated_by ? ' by '+esc(item.last_escalated_by) : '')+'</div>';
  if (item.session_id) html += '<div><strong>Session</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showSessionDetail((item.session_id))}) + ' style="color:var(--accent)">'+esc(item.session_id)+'</a></div>';
  if (item.task_id) html += '<div><strong>Task</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tasks');setTimeout(()=>showTaskDetail((item.task_id),(item.task_id)),100)}) + ' style="color:var(--accent)">'+esc(item.task_id)+'</a></div>';
  html += '</div>';
  if (lifecycleEvidence.length) html += '<div style="margin-bottom:12px"><strong>Visible Lock Evidence</strong><div class="msg-item" style="margin-top:6px">'+esc(lifecycleEvidence.join(' | '))+'</div></div>';
  if (rebasePayload) {
    html += '<div style="margin-bottom:12px"><strong>Rebase Follow-Up</strong><div class="msg-item" style="margin-top:6px">';
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;font-size:12px">';
    html += '<div><strong>Next Action</strong><br>'+esc(rebasePayload.next_action || 'attempt_rebase')+'</div>';
    if (rebasePayload.rebase_workflow_state) html += '<div><strong>Workflow State</strong><br>'+esc(rebasePayload.rebase_workflow_state)+'</div>';
    if (rebasePayload.rebase_workflow_step) html += '<div><strong>Workflow Step</strong><br>'+esc(rebasePayload.rebase_workflow_step)+'</div>';
    if (rebasePayload.rebase_plan_class) html += '<div><strong>Rebase Plan</strong><br>'+esc(rebasePayload.rebase_plan_class)+'</div>';
    if (rebasePayload.conflict_safe_class) html += '<div><strong>Conflict Safety</strong><br>'+esc(rebasePayload.conflict_safe_class)+'</div>';
    if (rebasePayload.action_id) html += '<div><strong>Linked Action</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('actions');setTimeout(()=>showActionDetail((rebasePayload.action_id)),100)}) + ' style="color:var(--accent)">'+esc(rebasePayload.action_title || rebasePayload.action_id)+'</a></div>';
    if (rebasePayload.action_status) html += '<div><strong>Action Status</strong><br>'+esc(rebasePayload.action_status)+'</div>';
    if (rebasePayload.action_assigned_to) html += '<div><strong>Action Assignee</strong><br>'+esc(rebasePayload.action_assigned_to)+'</div>';
    if (rebasePayload.action_paused_by) html += '<div><strong>Paused By</strong><br>'+esc(rebasePayload.action_paused_by)+'</div>';
    if (rebasePayload.fork_tension_id) html += '<div><strong>Fork Tension</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tensions');setTimeout(()=>showTensionDetail((rebasePayload.fork_tension_id)),100)}) + ' style="color:var(--accent)">'+esc(rebasePayload.fork_tension_id)+'</a></div>';
    if (rebasePayload.repair_tension_id) html += '<div><strong>Repair Tension</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tensions');setTimeout(()=>showTensionDetail((rebasePayload.repair_tension_id)),100)}) + ' style="color:var(--accent)">'+esc(rebasePayload.repair_tension_id)+'</a></div>';
    if (rebasePayload.alternative_patch) html += '<div><strong>Alternative Patch</strong><br>'+esc(rebasePayload.alternative_patch)+'</div>';
    html += '</div></div></div>';
  }
  if (item.summary) html += '<div style="margin-bottom:12px"><strong>Summary</strong><div class="msg-item" style="margin-top:6px">'+esc(item.summary)+'</div></div>';
  if (item.details) html += '<div style="margin-bottom:12px"><strong>Details</strong><pre>'+esc(item.details)+'</pre></div>';
  if (item.resolution) html += '<div style="margin-bottom:12px"><strong>Resolution</strong><div class="msg-item" style="margin-top:6px">'+esc(item.resolution)+'</div></div>';
  html += '<div class="action-btn-row"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openOperatorQueueComposer((item.queue_id))}) + '>Edit Queue Item</button></div>';
  if (isQueueOpen(item)) {
    html += '<div class="action-btn-row">';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){escalateOperatorQueueFromModal((item.queue_id))}) + '>Escalate Queue Item</button>';
    html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){resolveOperatorQueueFromModal((item.queue_id))}) + '>Resolve Queue Item</button>';
    html += '</div>';
  }
  openModal(item.title || item.queue_id, html);
}

async function escalateOperatorQueueFromModal(queueId) {
  const item = operatorQueueCache.find(x => x.queue_id === queueId);
  if (!item) return;
  const reason = await dashboardPrompt('Escalation reason for this queue item:', item.escalation_reason || '');
  if (reason === null) return;
  const dueAt = await dashboardPrompt('Optional updated due timestamp (RFC3339):', item.due_at || '');
  if (dueAt === null) return;
  const assignedTo = await dashboardPrompt('Optional assignee for this escalation:', item.assigned_to || '');
  if (assignedTo === null) return;
  const urgency = await dashboardPrompt('Optional urgency (LOW/NORMAL/HIGH/CRITICAL):', item.urgency || '');
  if (urgency === null) return;
  try {
    if (String(item.source_kind || '').toLowerCase() === 'knowledge_claim' && item.source_id) {
      await rpc('workspace.claim.escalate', {
        workspace_id: WS_ID,
        claim_id: item.source_id,
        actor_id: currentProfileId() || 'dashboard',
        reason: String(reason || '').trim(),
        due_at: String(dueAt || '').trim(),
        assigned_to: String(assignedTo || '').trim(),
        urgency: String(urgency || '').trim()
      });
      toast('Claim review escalated');
    } else {
      await rpc('workspace.ops.escalate', {
        workspace_id: WS_ID,
        queue_id: queueId,
        escalated_by: currentProfileId() || 'dashboard',
        reason: String(reason || '').trim(),
        due_at: String(dueAt || '').trim(),
        assigned_to: String(assignedTo || '').trim(),
        urgency: String(urgency || '').trim(),
        current_revision: Number(item.revision || 0) > 0 ? Number(item.revision) : undefined,
        current_updated_at: String(item.updated_at || '').trim()
      });
      toast('Queue item escalated');
    }
    closeModal();
    await Promise.all([loadOperatorQueue(), loadClaims(), loadRuntimeEvents(), loadCompaction()]);
  } catch (e) {
    toast('Escalation failed: ' + e.message);
  }
}

async function resolveOperatorQueueFromModal(queueId) {
  const item = operatorQueueCache.find(x => x.queue_id === queueId);
  if (!item) return;
  const resolution = await dashboardPrompt('Resolution note for this queue item:', '');
  if (resolution === null) return;
  try {
    await rpc('workspace.ops.resolve', {
      workspace_id: WS_ID,
      queue_id: queueId,
      resolved_by: currentProfileId() || 'dashboard',
      resolution: resolution,
      status: 'RESOLVED',
      current_revision: Number(item.revision || 0) > 0 ? Number(item.revision) : undefined,
      current_updated_at: String(item.updated_at || '').trim()
    });
    toast('Queue item resolved');
    closeModal();
    await Promise.all([loadOperatorQueue(), loadRuntimeEvents()]);
  } catch (e) {
    toast('Resolve failed: ' + e.message);
  }
}

async function openClaimComposer(claimId = '') {
  const existing = claimId ? claimsCache.find(x => x.claim_id === claimId) : null;
const claimType = await dashboardPrompt('Claim type (FACT/DECISION/LESSON/PROCEDURE/ANTI_PROCEDURE/INCIDENT/UPDATE_DIGEST/BLOCKER/CONSTRAINT/DISSENT/DISSENT_MARKER/DISSENT_CONTENT/HYPOTHESIS/ALTERNATIVE_BRANCH/ENTITY/SUMMARY/EXPERIENCE):', existing ? (existing.claim_type || 'FACT') : 'FACT');
  if (claimType === null) return;
  const subject = await dashboardPrompt('Claim subject:', existing ? (existing.subject || '') : '');
  if (subject === null) return;
  if (!String(subject || '').trim()) {
    toast('Claim subject is required');
    return;
  }
  const body = await dashboardPrompt('Claim body:', existing ? (existing.body || '') : '');
  if (body === null) return;
  if (!String(body || '').trim()) {
    toast('Claim body is required');
    return;
  }
  const summary = await dashboardPrompt('Optional claim summary:', existing ? (existing.summary || '') : '');
  if (summary === null) return;
  const status = await dashboardPrompt('Status (ACTIVE/CONFIRMED/REVIEW/STALE/SUPERSEDED/DISPUTED):', existing ? (existing.status || 'ACTIVE') : 'ACTIVE');
  if (status === null) return;
  const confidence = await dashboardPrompt('Confidence (0..1):', existing && existing.confidence !== undefined ? String(existing.confidence) : '0.8');
  if (confidence === null) return;
  const sourceKind = await dashboardPrompt('Source kind:', existing ? (existing.source_kind || 'manual') : 'manual');
  if (sourceKind === null) return;
  const sourceID = await dashboardPrompt('Optional source id:', existing ? (existing.source_id || '') : '');
  if (sourceID === null) return;
  const memoryID = await dashboardPrompt('Optional linked memory id:', existing ? (existing.memory_id || '') : '');
  if (memoryID === null) return;
  const taskID = await dashboardPrompt('Optional task id:', existing ? (existing.task_id || '') : '');
  if (taskID === null) return;
  const sessionID = await dashboardPrompt('Optional session id:', existing ? (existing.session_id || '') : '');
  if (sessionID === null) return;
  const agentID = await dashboardPrompt('Optional agent id:', existing ? (existing.agent_id || '') : '');
  if (agentID === null) return;
  const tags = await dashboardPrompt('Optional tags (comma-separated):', existing && existing.tags ? existing.tags.join(',') : '');
  if (tags === null) return;
  const evidence = await dashboardPrompt('Optional evidence refs (comma-separated):', existing && existing.evidence ? existing.evidence.join(',') : '');
  if (evidence === null) return;
  try {
    const response = await rpc('workspace.claim.write', {
      workspace_id: WS_ID,
      claim_id: existing ? existing.claim_id : undefined,
      claim_type: String(claimType || '').trim(),
      status: String(status || '').trim(),
      subject: String(subject || '').trim(),
      body: String(body || '').trim(),
      summary: String(summary || '').trim(),
      confidence: Number(confidence || 0),
      source_kind: String(sourceKind || '').trim(),
      source_id: String(sourceID || '').trim(),
      memory_id: String(memoryID || '').trim(),
      task_id: String(taskID || '').trim(),
      session_id: String(sessionID || '').trim(),
      agent_id: String(agentID || '').trim(),
      tags: splitCsv(tags),
      evidence: splitCsv(evidence)
    });
    toast(existing ? 'Claim updated' : 'Claim recorded');
    await Promise.all([loadClaims(), loadOperatorQueue(), loadRuntimeEvents(), loadPolicies()]);
    const claim = response && response.claim ? response.claim : null;
    if (claim && claim.claim_id) showClaimDetail(claim.claim_id);
  } catch (e) {
    console.error('workspace.claim.write', e);
    toast('Claim update failed: ' + e.message);
  }
}

async function loadClaims() {
  try {
    const filters = currentClaimsFilters();
    const params = {
      workspace_id: WS_ID,
      include_archived: filters.include_archived,
      limit: filters.query ? 20 : 12
    };
    if (filters.status) params.status = filters.status;
    const r = await rpc(filters.query ? 'workspace.claim.search' : 'workspace.claim.list', Object.assign(params, filters.query ? {query: filters.query} : {}));
    claimsCache = r.items || [];
    document.getElementById('claims-count').textContent = claimsCache.length;
    const el = document.getElementById('claims-list');
    if (!claimsCache.length) {
      el.innerHTML = '<div class="empty">'+(filters.query || filters.status || filters.include_archived ? 'No claims matched the current filters.' : 'No durable claims recorded yet.')+'</div>';
    } else {
      el.innerHTML = claimsCache.map(claim =>
        '<div class="action-card'+(String(claim.status || '').toUpperCase() === 'ARCHIVED' ? ' resolved' : '')+'" ' + dashboardAction(function(dashboardEvent){showClaimDetail((claim.claim_id))}) + '>' +
          '<div class="action-title">'+esc(claim.subject || claim.claim_id)+'</div>' +
          '<div class="action-meta">' +
            '<span class="action-status '+esc(claim.status || 'ACTIVE')+'">'+esc(claim.status || 'ACTIVE')+'</span>' +
            '<span>'+esc(claim.claim_type || 'FACT')+'</span>' +
            '<span>'+esc(claim.source_kind || 'unknown')+'</span>' +
            '<span>'+timeAgo(claim.updated_at || claim.created_at)+'</span>' +
          '</div>' +
          (claim.summary ? '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(claim.summary)+'</div>' : '') +
        '</div>'
      ).join('');
    }
  } catch (e) {
    console.error('loadClaims', e);
    document.getElementById('claims-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load claims')+'</div>';
  }
  loadOperatorInbox();
  loadClaimReviewWorkbench();
}

async function showClaimDetail(claimId) {
  const claim = claimsCache.find(x => x.claim_id === claimId);
  if (!claim) return;
  let relationItems = [];
  try {
    const relationResp = await rpc('workspace.claim.links.list', {
      workspace_id: WS_ID,
      claim_id: claim.claim_id,
      limit: 32
    });
    relationItems = Array.isArray(relationResp.items) ? relationResp.items : [];
  } catch (e) {
    console.error('workspace.claim.links.list', e);
  }
  const status = String(claim.status || 'ACTIVE').toUpperCase();
  const claimLink = (linkedClaimId, label) => {
    if (!linkedClaimId) return '';
    const text = label || linkedClaimId;
    if (!claimsCache.find(x => x.claim_id === linkedClaimId)) {
      return esc(text);
    }
    return '<a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showClaimDetail((linkedClaimId))}) + ' style="color:var(--accent)">'+esc(text)+'</a>';
  };
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Type</strong><br>'+esc(claim.claim_type || 'FACT')+'</div>';
  html += '<div><strong>Status</strong><br>'+esc(status)+'</div>';
  html += '<div><strong>Source</strong><br>'+esc(claim.source_kind || 'unknown')+'</div>';
  html += '<div><strong>Confidence</strong><br>'+esc((claim.confidence || 0).toFixed ? (claim.confidence || 0).toFixed(2) : claim.confidence || 0)+'</div>';
  if (claim.reviewed_at) html += '<div><strong>Reviewed</strong><br>'+esc(timeAgo(claim.reviewed_at))+(claim.reviewed_by ? ' by '+esc(claim.reviewed_by) : '')+'</div>';
  if (claim.review_due_at) html += '<div><strong>Review Due</strong><br>'+esc(claim.review_due_at)+'</div>';
  if (claim.session_id) html += '<div><strong>Session</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showSessionDetail((claim.session_id))}) + ' style="color:var(--accent)">'+esc(claim.session_id)+'</a></div>';
  if (claim.task_id) html += '<div><strong>Task</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tasks');setTimeout(()=>showTaskDetail((claim.task_id),(claim.task_id)),100)}) + ' style="color:var(--accent)">'+esc(claim.task_id)+'</a></div>';
  html += '</div>';
  if (claim.summary) html += '<div style="margin-bottom:12px"><strong>Summary</strong><div class="msg-item" style="margin-top:6px">'+esc(claim.summary)+'</div></div>';
  html += '<div style="margin-bottom:12px"><strong>Body</strong><pre>'+esc(claim.body || 'No body recorded.')+'</pre></div>';
  if (claim.lifecycle_reason) {
    html += '<div style="margin-bottom:12px"><strong>Lifecycle Reason</strong><div class="msg-item" style="margin-top:6px">'+esc(claim.lifecycle_reason)+'</div></div>';
  }
  if (claim.tags && claim.tags.length) {
    html += '<div style="margin-bottom:12px"><strong>Tags</strong><div style="margin-top:6px">'+claim.tags.map(tag => '<span class="task-tag">'+esc(tag)+'</span>').join(' ')+'</div></div>';
  }
  if (claim.memory_id) {
    html += '<div style="margin-bottom:12px"><strong>Memory Link</strong><div class="msg-item" style="margin-top:6px">'+esc(claim.memory_id)+'</div></div>';
  }
  const relationLines = [];
  const seenRelations = new Set();
  const pushRelationLine = (prefix, targetId, relationType, direction, weight) => {
    targetId = String(targetId || '').trim();
    relationType = String(relationType || '').trim().toUpperCase();
    direction = String(direction || '').trim().toUpperCase();
    if (!targetId || !relationType) return;
    const dedupeKey = [prefix, relationType, direction, targetId].join('|');
    if (seenRelations.has(dedupeKey)) return;
    seenRelations.add(dedupeKey);
    let suffix = '';
    const numericWeight = Number(weight);
    if (!Number.isNaN(numericWeight) && numericWeight > 0 && numericWeight < 1) {
      suffix = ' <span style="color:var(--muted)">(' + esc(numericWeight.toFixed(2)) + ')</span>';
    }
    relationLines.push('<div>' + esc(prefix) + ': ' + claimLink(targetId) + suffix + '</div>');
  };
  if (claim.supersedes_claim_id) pushRelationLine('Supersedes', claim.supersedes_claim_id, 'SUPERSEDES', 'OUTGOING', 1);
  if (claim.superseded_by_claim_id) pushRelationLine('Superseded by', claim.superseded_by_claim_id, 'SUPERSEDED_BY', 'OUTGOING', 1);
  if (claim.conflicts_claim_id) pushRelationLine('Conflicts with', claim.conflicts_claim_id, 'CONTRADICTS', 'OUTGOING', 1);
  relationItems.forEach(rel => {
    const fromId = String(rel.from_claim_id || '').trim();
    const toId = String(rel.to_claim_id || '').trim();
    const relType = String(rel.relation_type || '').trim().toUpperCase();
    const isOutgoing = fromId === claim.claim_id;
    const targetId = isOutgoing ? toId : fromId;
    let prefix = relType;
    if (isOutgoing) {
      if (relType === 'SUPPORTS') prefix = 'Supports';
      else if (relType === 'CONTRADICTS') prefix = 'Contradicts';
      else if (relType === 'SUPERSEDES') prefix = 'Supersedes';
      else if (relType === 'VALIDATED_BY') prefix = 'Validated by';
      else if (relType === 'BLOCKS') prefix = 'Blocks';
      else if (relType === 'RESOLVES') prefix = 'Resolves';
    } else {
      if (relType === 'SUPPORTS') prefix = 'Supported by';
      else if (relType === 'CONTRADICTS') prefix = 'Contradicted by';
      else if (relType === 'SUPERSEDES') prefix = 'Superseded by';
      else if (relType === 'VALIDATED_BY') prefix = 'Validates';
      else if (relType === 'BLOCKS') prefix = 'Blocked by';
      else if (relType === 'RESOLVES') prefix = 'Resolved by';
    }
    pushRelationLine(prefix, targetId, relType, isOutgoing ? 'OUTGOING' : 'INCOMING', rel.weight);
  });
  if (relationLines.length) {
    html += '<div style="margin-bottom:12px"><strong>Related Claims</strong><div class="msg-item" style="margin-top:6px">';
    html += relationLines.join('');
    html += '</div></div>';
  }
  const relatedOps = operatorQueueCache.filter(item => String(item.source_kind || '').toLowerCase() === 'knowledge_claim' && item.source_id === claim.claim_id && String(item.status || '').toUpperCase() === 'OPEN');
  if (relatedOps.length) {
    html += '<div style="margin-bottom:12px"><strong>Open Follow-Up</strong><div style="margin-top:6px">';
    html += relatedOps.map(item =>
      '<div class="msg-item" style="margin-bottom:6px">' +
        '<a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showOperatorQueueDetail((item.queue_id))}) + ' style="color:var(--accent);font-weight:600">'+esc(item.title || item.queue_id)+'</a>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">'+esc(item.queue_type || 'FOLLOW_UP')+(item.assigned_to ? ' · '+esc(item.assigned_to) : '')+(queueMetaBadges(item).length ? ' · '+esc(queueMetaBadges(item).join(' · ')) : '')+'</div>' +
      '</div>'
    ).join('');
    html += '</div></div>';
  }
  if (status !== 'ARCHIVED') {
    html += '<div class="action-btn-row">';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openClaimComposer((claim.claim_id))}) + '>Edit Claim</button>';
    if (status !== 'CONFIRMED') html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'confirm')}) + '>Confirm</button>';
    if (status !== 'REVIEW') html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'review')}) + '>Request Review</button>';
    if (status !== 'DISPUTED') html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'dispute')}) + '>Dispute</button>';
    if (status !== 'STALE') html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'stale')}) + '>Mark Stale</button>';
    if (status !== 'SUPERSEDED') html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'supersede')}) + '>Supersede</button>';
    if (isClaimReviewStatus(status) || relatedOps.length) html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){claimLifecycleFromModal((claim.claim_id), 'escalate')}) + '>Escalate Review</button>';
    html += '<button class="btn-session-danger" ' + dashboardAction(function(dashboardEvent){archiveClaimFromModal((claim.claim_id))}) + '>Archive Claim</button>';
    html += '</div>';
  }
  openModal(claim.subject || claim.claim_id, html);
}

async function archiveClaimFromModal(claimId) {
  const reason = await dashboardPrompt('Archive reason for this claim:', '');
  if (reason === null) return;
  try {
    await rpc('workspace.claim.archive', {
      workspace_id: WS_ID,
      claim_id: claimId,
      archived_by: currentProfileId() || 'dashboard',
      reason: reason
    });
    toast('Claim archived');
    closeModal();
    await Promise.all([loadClaims(), loadRuntimeEvents()]);
  } catch (e) {
    toast('Archive failed: ' + e.message);
  }
}

async function claimLifecycleFromModal(claimId, action) {
  const params = {workspace_id: WS_ID, claim_id: claimId, actor_id: currentProfileId() || 'dashboard'};
  let method = '';
  let okText = 'Claim updated';
  if (action === 'review') {
    method = 'workspace.claim.review';
    const reason = await dashboardPrompt('Review reason for this claim:', '');
    if (reason === null) return;
    const dueAt = await dashboardPrompt('Optional review due timestamp (RFC3339):', '');
    if (dueAt === null) return;
    const assignedTo = await dashboardPrompt('Optional assignee for the follow-up queue:', '');
    if (assignedTo === null) return;
    const urgency = await dashboardPrompt('Optional urgency (LOW/NORMAL/HIGH/CRITICAL):', '');
    if (urgency === null) return;
    params.reason = reason;
    if (dueAt.trim()) params.due_at = dueAt.trim();
    if (assignedTo.trim()) params.assigned_to = assignedTo.trim();
    if (urgency.trim()) params.urgency = urgency.trim();
    okText = 'Claim marked for review';
  } else if (action === 'confirm') {
    method = 'workspace.claim.confirm';
    const reason = await dashboardPrompt('Optional confirmation note:', '');
    if (reason === null) return;
    params.reason = reason;
    okText = 'Claim confirmed';
  } else if (action === 'dispute') {
    method = 'workspace.claim.dispute';
    const reason = await dashboardPrompt('Why is this claim disputed?', '');
    if (reason === null) return;
    const conflictId = await dashboardPrompt('Optional conflicting claim id:', '');
    if (conflictId === null) return;
    const dueAt = await dashboardPrompt('Optional review due timestamp (RFC3339):', '');
    if (dueAt === null) return;
    const assignedTo = await dashboardPrompt('Optional assignee for the follow-up queue:', '');
    if (assignedTo === null) return;
    const urgency = await dashboardPrompt('Optional urgency (LOW/NORMAL/HIGH/CRITICAL):', '');
    if (urgency === null) return;
    params.reason = reason;
    if (conflictId.trim()) params.conflicts_claim_id = conflictId.trim();
    if (dueAt.trim()) params.due_at = dueAt.trim();
    if (assignedTo.trim()) params.assigned_to = assignedTo.trim();
    if (urgency.trim()) params.urgency = urgency.trim();
    okText = 'Claim disputed';
  } else if (action === 'supersede') {
    method = 'workspace.claim.supersede';
    const supersedingId = await dashboardPrompt('Superseding claim id:', '');
    if (supersedingId === null) return;
    if (!supersedingId.trim()) {
      toast('Superseding claim id is required');
      return;
    }
    const reason = await dashboardPrompt('Optional supersede note:', '');
    if (reason === null) return;
    params.superseding_claim_id = supersedingId.trim();
    params.reason = reason;
    okText = 'Claim superseded';
  } else if (action === 'stale') {
    method = 'workspace.claim.stale';
    const reason = await dashboardPrompt('Why is this claim stale?', '');
    if (reason === null) return;
    const dueAt = await dashboardPrompt('Optional review due timestamp (RFC3339):', '');
    if (dueAt === null) return;
    const assignedTo = await dashboardPrompt('Optional assignee for the follow-up queue:', '');
    if (assignedTo === null) return;
    const urgency = await dashboardPrompt('Optional urgency (LOW/NORMAL/HIGH/CRITICAL):', '');
    if (urgency === null) return;
    params.reason = reason;
    if (dueAt.trim()) params.due_at = dueAt.trim();
    if (assignedTo.trim()) params.assigned_to = assignedTo.trim();
    if (urgency.trim()) params.urgency = urgency.trim();
    okText = 'Claim marked stale';
  } else if (action === 'escalate') {
    method = 'workspace.claim.escalate';
    const reason = await dashboardPrompt('Escalation reason for this claim review:', '');
    if (reason === null) return;
    const dueAt = await dashboardPrompt('Optional updated due timestamp (RFC3339):', '');
    if (dueAt === null) return;
    const assignedTo = await dashboardPrompt('Optional assignee for this escalation:', '');
    if (assignedTo === null) return;
    const urgency = await dashboardPrompt('Optional urgency (LOW/NORMAL/HIGH/CRITICAL):', 'HIGH');
    if (urgency === null) return;
    params.reason = reason;
    if (dueAt.trim()) params.due_at = dueAt.trim();
    if (assignedTo.trim()) params.assigned_to = assignedTo.trim();
    if (urgency.trim()) params.urgency = urgency.trim();
    okText = 'Claim review escalated';
  } else {
    toast('Unknown claim action: ' + action);
    return;
  }
  try {
    await rpc(method, params);
    toast(okText);
    closeModal();
    await Promise.all([loadClaims(), loadOperatorQueue(), loadRuntimeEvents(), loadPolicies()]);
  } catch (e) {
    toast('Claim update failed: ' + e.message);
  }
}

async function openExecutionRunComposer(runId = '') {
  const existing = runId ? ((executionRunDetailCache[runId] && executionRunDetailCache[runId].run) || executionRunsCache.find(x => x.run_id === runId)) : null;
  const title = await dashboardPrompt('Execution run title:', existing ? (existing.title || '') : '');
  if (title === null) return;
  if (!String(title || '').trim()) {
    toast('Run title is required');
    return;
  }
  const summary = await dashboardPrompt('Optional run summary:', existing ? (existing.summary || '') : '');
  if (summary === null) return;
  const status = await dashboardPrompt('Run status (PLANNED/ACTIVE/BLOCKED/VERIFYING/COMPLETED/FAILED/CANCELLED):', existing ? (existing.status || 'PLANNED') : 'PLANNED');
  if (status === null) return;
  const outcome = await dashboardPrompt('Optional run outcome:', existing ? (existing.outcome || '') : '');
  if (outcome === null) return;
  const taskID = await dashboardPrompt('Optional task id:', existing ? (existing.task_id || '') : '');
  if (taskID === null) return;
  const sessionID = await dashboardPrompt('Optional session id:', existing ? (existing.session_id || '') : '');
  if (sessionID === null) return;
  const agentID = await dashboardPrompt('Optional agent id:', existing ? (existing.agent_id || '') : '');
  if (agentID === null) return;
  try {
    const response = await rpc('workspace.execution.run.write', {
      workspace_id: WS_ID,
      run_id: existing ? existing.run_id : undefined,
      title: String(title || '').trim(),
      summary: String(summary || '').trim(),
      status: String(status || '').trim(),
      outcome: String(outcome || '').trim(),
      task_id: String(taskID || '').trim(),
      session_id: String(sessionID || '').trim(),
      agent_id: String(agentID || '').trim()
    });
    toast(existing ? 'Execution run updated' : 'Execution run recorded');
    await Promise.all([loadExecutionRuns(), loadRuntimeEvents()]);
    const run = response && response.run ? response.run : null;
    if (run && run.run_id) showExecutionRunDetail(run.run_id);
  } catch (e) {
    console.error('workspace.execution.run.write', e);
    toast('Execution run update failed: ' + e.message);
  }
}

async function openExecutionStepComposer(runId, stepId = '') {
  const detail = executionRunDetailCache[runId] || {};
  const existing = stepId ? ((detail.steps || []).find(x => x.step_id === stepId) || null) : null;
  const title = await dashboardPrompt('Execution step title:', existing ? (existing.title || '') : '');
  if (title === null) return;
  if (!String(title || '').trim()) {
    toast('Step title is required');
    return;
  }
  const phase = await dashboardPrompt('Phase (PLAN/EXECUTE/VERIFY):', existing ? (existing.phase || 'PLAN') : 'PLAN');
  if (phase === null) return;
  const summary = await dashboardPrompt('Optional step summary:', existing ? (existing.summary || '') : '');
  if (summary === null) return;
  const status = await dashboardPrompt('Step status (PENDING/ACTIVE/BLOCKED/COMPLETED/FAILED/SKIPPED):', existing ? (existing.status || 'PENDING') : 'PENDING');
  if (status === null) return;
  const sortOrder = await dashboardPrompt('Optional sort order:', existing && existing.sort_order !== undefined ? String(existing.sort_order) : '');
  if (sortOrder === null) return;
  const parentStepID = await dashboardPrompt('Optional parent step id:', existing ? (existing.parent_step_id || '') : '');
  if (parentStepID === null) return;
  const evidence = await dashboardPrompt('Optional evidence refs (comma-separated):', existing && existing.evidence ? existing.evidence.join(',') : '');
  if (evidence === null) return;
  const verification = await dashboardPrompt('Optional verification JSON:', existing && existing.verification ? JSON.stringify(existing.verification) : '');
  if (verification === null) return;
  try {
    await rpc('workspace.execution.step.write', {
      workspace_id: WS_ID,
      run_id: runId,
      step_id: existing ? existing.step_id : undefined,
      parent_step_id: String(parentStepID || '').trim(),
      phase: String(phase || '').trim(),
      title: String(title || '').trim(),
      summary: String(summary || '').trim(),
      status: String(status || '').trim(),
      sort_order: String(sortOrder || '').trim() ? Number(sortOrder) : 0,
      evidence: splitCsv(evidence),
      verification: parseJSONOrEmpty(verification, {})
    });
    toast(existing ? 'Execution step updated' : 'Execution step recorded');
    await Promise.all([loadExecutionRuns(), loadRuntimeEvents()]);
    showExecutionRunDetail(runId);
  } catch (e) {
    console.error('workspace.execution.step.write', e);
    toast('Execution step update failed: ' + e.message);
  }
}

async function loadExecutionRuns() {
  try {
    const params = {workspace_id: WS_ID, limit: 20};
    const status = document.getElementById('execution-filter-status').value;
    if (status) params.status = status;
    const r = await rpc('workspace.execution.run.list', params);
    executionRunsCache = r.items || [];
    document.getElementById('execution-runs-count').textContent = executionRunsCache.length;
    const el = document.getElementById('execution-runs-list');
    if (!executionRunsCache.length) {
      el.innerHTML = '<div class="empty">No execution runs yet.</div>';
      return;
    }
    el.innerHTML = executionRunsCache.map(run =>
      '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showExecutionRunDetail((run.run_id))}) + '>' +
        '<div class="action-title">'+esc(run.title || run.run_id)+'</div>' +
        '<div class="action-meta">' +
          '<span class="action-status '+esc(run.status || 'ACTIVE')+'">'+esc(run.status || 'ACTIVE')+'</span>' +
          '<span>'+esc(run.agent_id || 'system')+'</span>' +
          '<span>'+timeAgo(run.updated_at || run.created_at)+'</span>' +
        '</div>' +
        (run.summary ? '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(run.summary)+'</div>' : '') +
      '</div>'
    ).join('');
  } catch (e) {
    console.error('loadExecutionRuns', e);
    document.getElementById('execution-runs-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load execution runs')+'</div>';
  }
}

async function showExecutionRunDetail(runId) {
  try {
    const r = await rpc('workspace.execution.run.get', {workspace_id: WS_ID, run_id: runId});
    const detail = r.detail || {};
    executionRunDetailCache[runId] = detail;
    const run = detail.run || executionRunsCache.find(x => x.run_id === runId) || {};
    const steps = detail.steps || [];
    let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
    html += '<div><strong>Status</strong><br>'+esc(run.status || 'ACTIVE')+'</div>';
    html += '<div><strong>Agent</strong><br>'+esc(run.agent_id || 'system')+'</div>';
    if (run.session_id) html += '<div><strong>Session</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showSessionDetail((run.session_id))}) + ' style="color:var(--accent)">'+esc(run.session_id)+'</a></div>';
    if (run.task_id) html += '<div><strong>Task</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tasks');setTimeout(()=>showTaskDetail((run.task_id),(run.task_id)),100)}) + ' style="color:var(--accent)">'+esc(run.task_id)+'</a></div>';
    html += '</div>';
    if (run.summary) html += '<div style="margin-bottom:12px"><strong>Summary</strong><div class="msg-item" style="margin-top:6px">'+esc(run.summary)+'</div></div>';
    if (run.outcome) html += '<div style="margin-bottom:12px"><strong>Outcome</strong><div class="msg-item" style="margin-top:6px">'+esc(run.outcome)+'</div></div>';
    html += '<div class="action-btn-row"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openExecutionRunComposer((run.run_id))}) + '>Edit Run</button><button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){openExecutionStepComposer((run.run_id))}) + '>Add Step</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openReplayFromExecutionRun((run.run_id))}) + '>Replay Run</button></div>';
    html += renderExecutionGraph(run, steps);
    html += '<div style="margin-bottom:12px"><strong>Steps</strong>';
    if (!steps.length) {
      html += '<div class="empty" style="margin-top:6px">No execution steps recorded.</div>';
    } else {
      html += '<div style="margin-top:6px">';
      html += steps.map(step =>
        '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><div style="font-weight:600">'+esc(step.title || step.step_id)+'</div><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openExecutionStepComposer((run.run_id), (step.step_id))}) + '>Edit Step</button></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">'+esc((step.phase || 'STEP')+' · '+(step.status || 'ACTIVE'))+'</div>' +
          (step.summary ? '<div style="margin-top:4px">'+esc(step.summary)+'</div>' : '') +
        '</div>'
      ).join('');
      html += '</div>';
    }
    html += '</div>';
    openModal(run.title || run.run_id, html);
  } catch (e) {
    toast('Failed to load execution run: ' + e.message);
  }
}

// ── Tasks ──
async function loadPolicies() {
  try {
    const subjectQuery = String((document.getElementById('policy-filter-subject') || {}).value || '').trim().toLowerCase();
    const r = await rpc('workspace.policy.list', {workspace_id: WS_ID, limit: 50});
    let items = r.items || [];
    if (subjectQuery) {
      items = items.filter(item => {
        const haystack = [
          item.policy_id,
          item.subject_type,
          item.subject_id,
          item.capability,
          item.tool_id,
          item.effect,
          item.reason,
          item.created_by
        ].join(' ').toLowerCase();
        return haystack.includes(subjectQuery);
      });
    }
    policiesCache = items;
    document.getElementById('policy-count').textContent = items.length;
    const el = document.getElementById('policy-list');
    if (!items.length) {
      el.innerHTML = '<div class="empty">No capability policies recorded.</div>';
      return;
    }
    el.innerHTML = items.map(policy =>
      '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showPolicyDetail((policy.policy_id))}) + '>' +
        '<div class="action-title">'+esc(policy.subject_type || 'subject')+' · '+esc(policy.subject_id || '*')+'</div>' +
        '<div class="action-meta">' +
          '<span class="action-status '+esc(policy.effect || 'ALLOW')+'">'+esc(policy.effect || 'ALLOW')+'</span>' +
          '<span>'+esc(policy.capability || '*')+'</span>' +
          '<span>'+esc(policy.tool_id || '*')+'</span>' +
          '<span>'+timeAgo(policy.updated_at || policy.created_at)+'</span>' +
        '</div>' +
        (policy.reason ? '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(policy.reason)+'</div>' : '') +
      '</div>'
    ).join('');
  } catch (e) {
    console.error('loadPolicies', e);
    document.getElementById('policy-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load policies')+'</div>';
  }
}

function showPolicyDetail(policyId) {
  const policy = policiesCache.find(x => x.policy_id === policyId);
  if (!policy) return;
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Subject</strong><br>'+esc(policy.subject_type || 'subject')+' / '+esc(policy.subject_id || '*')+'</div>';
  html += '<div><strong>Effect</strong><br>'+esc(policy.effect || 'ALLOW')+'</div>';
  html += '<div><strong>Capability</strong><br>'+esc(policy.capability || '*')+'</div>';
  html += '<div><strong>Tool</strong><br>'+esc(policy.tool_id || '*')+'</div>';
  html += '<div><strong>Created By</strong><br>'+esc(policy.created_by || 'unknown')+'</div>';
  html += '<div><strong>Updated</strong><br>'+esc(timeAgo(policy.updated_at || policy.created_at))+'</div>';
  html += '</div>';
  if (policy.reason) {
    html += '<div style="margin-bottom:12px"><strong>Reason</strong><div class="msg-item" style="margin-top:6px">'+esc(policy.reason)+'</div></div>';
  }
  html += '<div class="action-btn-row">';
  html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openPolicyComposer((policy.policy_id))}) + '>Edit Policy</button>';
  html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){runPolicyCheckFromDashboard((policy.policy_id))}) + '>Check Policy</button>';
  html += '</div>';
  openModal('Policy ' + esc(policy.policy_id), html);
}

async function openPolicyComposer(policyId = '') {
  const existing = policyId ? policiesCache.find(x => x.policy_id === policyId) : null;
  const subjectType = await dashboardPrompt('Subject type (agent/session/operator/tool/human/*):', existing ? (existing.subject_type || 'agent') : 'agent');
  if (subjectType === null) return;
  if (!String(subjectType || '').trim()) {
    toast('Subject type is required');
    return;
  }
  const subjectId = await dashboardPrompt('Subject id (defaults to *):', existing ? (existing.subject_id || '*') : '*');
  if (subjectId === null) return;
  const capability = await dashboardPrompt('Capability (defaults to *):', existing ? (existing.capability || '*') : '*');
  if (capability === null) return;
  const toolId = await dashboardPrompt('Tool id (defaults to *):', existing ? (existing.tool_id || '*') : '*');
  if (toolId === null) return;
  const effect = await dashboardPrompt('Effect (ALLOW/DENY/REQUIRE_APPROVAL):', existing ? (existing.effect || 'ALLOW') : 'ALLOW');
  if (effect === null) return;
  const reason = await dashboardPrompt('Optional policy reason:', existing ? (existing.reason || '') : '');
  if (reason === null) return;
  const createdBy = await dashboardPrompt('Actor creating/updating this policy:', existing ? (existing.created_by || currentProfileId() || 'dashboard') : (currentProfileId() || 'dashboard'));
  if (createdBy === null) return;
  if (!String(createdBy || '').trim()) {
    toast('created_by is required');
    return;
  }
  try {
    const response = await rpc('workspace.policy.put', {
      workspace_id: WS_ID,
      policy_id: existing ? existing.policy_id : undefined,
      subject_type: String(subjectType || '').trim(),
      subject_id: String(subjectId || '').trim(),
      capability: String(capability || '').trim(),
      tool_id: String(toolId || '').trim(),
      effect: String(effect || '').trim(),
      reason: String(reason || '').trim(),
      created_by: String(createdBy || '').trim()
    });
    toast(existing ? 'Policy updated' : 'Policy created');
    await Promise.all([loadPolicies(), loadRuntimeEvents()]);
    const policy = response && response.policy ? response.policy : null;
    if (policy && policy.policy_id) showPolicyDetail(policy.policy_id);
  } catch (e) {
    console.error('workspace.policy.put', e);
    toast('Policy update failed: ' + e.message);
  }
}

async function runPolicyCheckFromDashboard(policyId = '') {
  const existing = policyId ? policiesCache.find(x => x.policy_id === policyId) : null;
  const subjectType = await dashboardPrompt('Subject type for this check:', existing ? (existing.subject_type || 'agent') : 'agent');
  if (subjectType === null) return;
  const subjectId = await dashboardPrompt('Subject id for this check:', existing ? (existing.subject_id || '*') : '');
  if (subjectId === null) return;
  if (!String(subjectId || '').trim()) {
    toast('Subject id is required');
    return;
  }
  const capability = await dashboardPrompt('Capability to evaluate:', existing ? (existing.capability || '*') : '');
  if (capability === null) return;
  if (!String(capability || '').trim()) {
    toast('Capability is required');
    return;
  }
  const toolId = await dashboardPrompt('Optional tool id for this check:', existing ? (existing.tool_id || '') : '');
  if (toolId === null) return;
  try {
    const response = await rpc('workspace.policy.check', {
      workspace_id: WS_ID,
      subject_type: String(subjectType || '').trim(),
      subject_id: String(subjectId || '').trim(),
      capability: String(capability || '').trim(),
      tool_id: String(toolId || '').trim()
    });
    const check = response && response.check ? response.check : {};
    let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
    html += '<div><strong>Verdict</strong><br>'+esc(check.verdict || 'ALLOW')+'</div>';
    html += '<div><strong>Subject</strong><br>'+esc(check.subject_type || subjectType)+' / '+esc(check.subject_id || subjectId)+'</div>';
    html += '<div><strong>Capability</strong><br>'+esc(check.capability || capability)+'</div>';
    html += '<div><strong>Tool</strong><br>'+esc(check.tool_id || toolId || '*')+'</div>';
    html += '</div>';
    const matched = check.matched_policies || (check.matched_policy ? [check.matched_policy] : []);
    if (matched.length) {
      html += '<div style="margin-bottom:12px"><strong>Matched Policies</strong><div style="margin-top:6px">';
      html += matched.map(policy =>
        '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="font-weight:600">'+esc(policy.effect || 'ALLOW')+' · '+esc(policy.subject_type || 'subject')+' / '+esc(policy.subject_id || '*')+'</div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">'+esc(policy.capability || '*')+' · '+esc(policy.tool_id || '*')+'</div>' +
          (policy.reason ? '<div style="margin-top:4px">'+esc(policy.reason)+'</div>' : '') +
        '</div>'
      ).join('');
      html += '</div></div>';
    } else {
      html += '<div class="empty">No matching policies found for this tuple.</div>';
    }
    openModal('Policy Check', html);
  } catch (e) {
    console.error('workspace.policy.check', e);
    toast('Policy check failed: ' + e.message);
  }
}

function instrumentationNumberInput(id, fallback) {
  const raw = String((document.getElementById(id) || {}).value || '').trim();
  const value = parseInt(raw, 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function instrumentationFilterParams(extra = {}) {
  const params = {
    workspace_id: WS_ID,
    limit: instrumentationNumberInput('instrumentation-filter-limit', 200),
    cluster_limit: instrumentationNumberInput('instrumentation-filter-cluster-limit', 12)
  };
  const agentID = String((document.getElementById('instrumentation-filter-agent') || {}).value || '').trim();
  const sessionID = String((document.getElementById('instrumentation-filter-session') || {}).value || '').trim();
  const taskID = String((document.getElementById('instrumentation-filter-task') || {}).value || '').trim();
  if (agentID) params.agent_id = agentID;
  if (sessionID) params.session_id = sessionID;
  if (taskID) params.task_id = taskID;
  return Object.assign(params, extra || {});
}

function resetInstrumentationFilters() {
  document.getElementById('instrumentation-filter-agent').value = '';
  document.getElementById('instrumentation-filter-session').value = '';
  document.getElementById('instrumentation-filter-task').value = '';
  document.getElementById('instrumentation-filter-limit').value = '200';
  document.getElementById('instrumentation-filter-cluster-limit').value = '12';
  loadInstrumentation();
}

function instrumentationPercent(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return '0.0%';
  return (num * 100).toFixed(1) + '%';
}

function instrumentationAuthority(report) {
  return report && report.time_authority && report.time_authority.reference_at ? report.time_authority : null;
}

function instrumentationRoleLock(cluster) {
  return ((cluster || {}).metrics || {}).role_lock || {};
}

function instrumentationRoleLockSummary(roleLock) {
  const pct = instrumentationPercent(Number((roleLock || {}).index || 0));
  return roleLock && roleLock.partial ? (pct + ' partial') : pct;
}

function instrumentationRoleLockPeak(clusters) {
  let best = null;
  (clusters || []).forEach(cluster => {
    const roleLock = instrumentationRoleLock(cluster);
    const score = Number(roleLock.index || 0);
    if (!best || score > best.score || (score === best.score && String(cluster.proto_cluster_id || '') < String((best.cluster || {}).proto_cluster_id || ''))) {
      best = {cluster, roleLock, score};
    }
  });
  return best;
}

function renderInstrumentationWorkspaceSummary(report) {
  const summaryEl = document.getElementById('instrumentation-workspace-summary');
  const generatedEl = document.getElementById('instrumentation-generated-at');
  const filterEl = document.getElementById('instrumentation-filter-summary');
  if (!report) {
    generatedEl.textContent = 'no data';
    filterEl.style.display = 'none';
    summaryEl.innerHTML = '<div class="empty">No instrumentation report available.</div>';
    return;
  }
  generatedEl.textContent = report.generated_at ? timeAgo(report.generated_at, instrumentationAuthority(report)) : 'just now';
  const filterBits = [];
  const activeFilter = report.filter || {};
  if (activeFilter.agent_id) filterBits.push('agent ' + activeFilter.agent_id);
  if (activeFilter.session_id) filterBits.push('session ' + activeFilter.session_id);
  if (activeFilter.task_id) filterBits.push('task ' + activeFilter.task_id);
  filterBits.push('event window ' + (activeFilter.limit || 0));
  filterBits.push('cluster cap ' + (activeFilter.cluster_limit || 0));
  if (report.truncated) filterBits.push('runtime window truncated');
  filterEl.textContent = filterBits.join(' | ');
  filterEl.style.display = '';

  const workspace = report.workspace || {};
  const replay = report.replay || {};
  const roleLockPeak = instrumentationRoleLockPeak(report.clusters || []);
  const cards = [
    {label:'Total Clusters', value:String(workspace.total_clusters || 0), detail:'Resolved from journal + replay state'},
    {label:'Blocked Clusters', value:String(workspace.blocked_cluster_count || 0), detail:'Queues or blocker signals are open'},
    {label:'Duplication Signals', value:String(workspace.duplicate_prone_cluster_count || 0), detail:'Clusters with overlapping coordination'},
    {label:'Top Agent Share', value:instrumentationPercent(workspace.top_agent_activity_share), detail:(workspace.top_agent_by_activity || 'n/a') + ' dominates cluster activity'},
    {label:'Centralization', value:instrumentationPercent(workspace.workspace_communication_centralization), detail:'Communication concentration across the workspace'},
    {label:'Role-Lock Peak', value:roleLockPeak ? instrumentationRoleLockSummary(roleLockPeak.roleLock) : '0.0%', detail:roleLockPeak ? ('Highest read-side anti-lock-in estimate in ' + String((roleLockPeak.cluster || {}).proto_cluster_id || 'cluster')) : 'No steward / builder / reviewer concentration is currently visible'},
    {label:'Runtime Events', value:String(replay.total_events || 0), detail:String(replay.active_session_count || 0) + ' active sessions, ' + String(replay.open_queue_count || 0) + ' open queue items'}
  ];
  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px">';
  cards.forEach(card => {
    html += '<div class="msg-item">' +
      '<div style="font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted)">' + esc(card.label) + '</div>' +
      '<div style="font-size:22px;font-weight:700;margin-top:4px">' + esc(card.value) + '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:6px;line-height:1.4">' + esc(card.detail) + '</div>' +
    '</div>';
  });
  html += '</div>';
  summaryEl.innerHTML = html;
}

function renderInstrumentationClusters(clusters, count, report) {
  const total = report && report.workspace ? Number(report.workspace.total_clusters || 0) : Number(count || 0);
  document.getElementById('instrumentation-clusters-count').textContent = String(total) + ' total';
  document.getElementById('instrumentation-truncated-badge').textContent = String((clusters || []).length) + ' shown';
  const el = document.getElementById('instrumentation-clusters-list');
  if (!clusters || !clusters.length) {
    el.innerHTML = '<div class="empty">No proto-clusters matched the current filter.</div>';
    return;
  }
  const authority = instrumentationAuthority(report);
  el.innerHTML = clusters.map(cluster => {
    const metrics = cluster.metrics || {};
    const roleLock = instrumentationRoleLock(cluster);
    const context = [];
    if (cluster.task_ids && cluster.task_ids.length) context.push('tasks: ' + cluster.task_ids.slice(0, 2).join(', '));
    if (cluster.session_ids && cluster.session_ids.length) context.push('sessions: ' + cluster.session_ids.slice(0, 2).join(', '));
    if (cluster.doc_keys && cluster.doc_keys.length) context.push('docs: ' + cluster.doc_keys.slice(0, 2).join(', '));
    if (cluster.artifact_refs && cluster.artifact_refs.length) context.push('artifacts: ' + cluster.artifact_refs.slice(0, 2).join(', '));
    return '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>' +
      '<div class="action-title">' + esc(cluster.proto_cluster_id || 'proto-cluster') + '</div>' +
      '<div class="action-meta">' +
        '<span>' + esc(cluster.resolution_kind || 'entity') + '</span>' +
        '<span>' + esc(String(metrics.event_count || 0) + ' events') + '</span>' +
        '<span>' + esc(String(metrics.open_queue_count || 0) + ' queues') + '</span>' +
        '<span>' + esc(timeAgo(metrics.last_event_at || report.generated_at, authority)) + '</span>' +
      '</div>' +
      '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">' +
        '<span class="tool-badge kind">blockers ' + esc(String(metrics.blocker_signal_count || 0)) + '</span>' +
        '<span class="tool-badge kind">dup ' + esc(String(metrics.duplication_signal_count || 0)) + '</span>' +
        '<span class="tool-badge kind">active ' + esc(String(metrics.active_session_count || 0)) + '</span>' +
        '<span class="tool-badge active">share ' + esc(instrumentationPercent(metrics.max_agent_activity_share)) + '</span>' +
        '<span class="tool-badge kind">role-lock ' + esc(instrumentationRoleLockSummary(roleLock)) + '</span>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:8px;line-height:1.4">' + esc(context.join(' | ') || 'No linked task/session/doc/artifact evidence') + '</div>' +
    '</div>';
  }).join('');
}

function renderInstrumentationSnapshotState() {
  const badge = document.getElementById('instrumentation-snapshot-state');
  const el = document.getElementById('instrumentation-snapshot-summary');
  const snapshot = instrumentationSnapshotEventCache;
  if (!snapshot) {
    badge.textContent = 'none';
    el.innerHTML = '<div class="empty">Record a snapshot to persist cluster metrics into the runtime journal.</div>';
    return;
  }
  const payload = parseJSON(snapshot.payload_json);
  const workspace = payload.workspace || {};
  const filter = payload.filter || {};
  const clusters = payload.clusters || [];
  badge.textContent = snapshot.created_at ? timeAgo(snapshot.created_at) : 'recorded';
  let html = '<div class="msg-item">';
  html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">';
  html += '<div><strong>Workspace</strong><br>' + esc(payload.workspace_id || snapshot.workspace_id || WS_ID) + '</div>';
  html += '<div><strong>Clusters</strong><br>' + esc(String(workspace.total_clusters || clusters.length || 0)) + '</div>';
  html += '<div><strong>Blocked</strong><br>' + esc(String(workspace.blocked_cluster_count || 0)) + '</div>';
  html += '<div><strong>Filter</strong><br>' + esc(filter.task_id || filter.session_id || filter.agent_id || 'workspace-wide') + '</div>';
  html += '</div>';
  if (clusters.length) {
    html += '<div style="margin-top:10px;font-size:11px;color:var(--muted)">Top cluster ids</div>';
    html += '<div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + clusters.slice(0, 4).map(cluster => '<span class="tool-badge kind">' + esc(cluster.proto_cluster_id || 'cluster') + '</span>').join('') + '</div>';
  }
  if (snapshot.event_id) {
    html += '<div style="margin-top:12px"><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){switchTab('control');setTimeout(function(){ showRuntimeEventDetail((snapshot.event_id)); }, 100)}) + '>Open Runtime Event</button></div>';
  }
  html += '</div>';
  el.innerHTML = html;
}

function syncInstrumentationSnapshotFromRuntimeEvents() {
  instrumentationSnapshotEventCache = runtimeEventsCache.find(item => String(item.event_type || '').toLowerCase() === 'cluster.metric_snapshot') || null;
  renderInstrumentationSnapshotState();
}

function isTensionRuntimeEventType(eventType) {
  const normalized = String(eventType || '').toLowerCase();
  return normalized === 'tension.refreshed' ||
    normalized === 'tension.detected' ||
    normalized === 'tension.updated' ||
    normalized === 'tension.confirmed' ||
    normalized === 'tension.discarded' ||
    normalized === 'tension.archived' ||
    normalized === 'tension.resolved' ||
    normalized === 'tension.dormant' ||
    normalized === 'tension.active' ||
    normalized === 'tension.recovered' ||
    normalized === 'tension.emergent' ||
    normalized === 'tension.condensed' ||
    normalized === 'tension.dependency.added' ||
    normalized === 'tension.dependency.removed' ||
    normalized === 'tension.agent.attached' ||
    normalized === 'tension.agent.detached';
}

function renderTensionGeneratedAt() {
  const badge = document.getElementById('tensions-generated-at');
  if (!badge) return;
  const authority = (tensionRefreshCache && (tensionRefreshCache.time_authority || ((tensionRefreshCache.report || {}).time_authority))) || tensionSurfaceTimeAuthority;
  if (tensionRefreshCache && tensionRefreshCache.refreshed_at) {
    badge.textContent = timeAgo(tensionRefreshCache.refreshed_at, authority);
    return;
  }
  if (tensionRuntimeEventCache && tensionRuntimeEventCache.created_at) {
    badge.textContent = 'live ' + timeAgo(tensionRuntimeEventCache.created_at, authority);
    return;
  }
  badge.textContent = 'cached';
}

function syncTensionStateFromRuntimeEvents() {
  tensionRuntimeEventCache = runtimeEventsCache.find(item => isTensionRuntimeEventType(item.event_type)) || null;
  renderTensionGeneratedAt();
}

function showProtoClusterDetail(clusterID) {
  const cluster = instrumentationClustersCache.find(item => item.proto_cluster_id === clusterID) || ((instrumentationReportCache && instrumentationReportCache.clusters) || []).find(item => item.proto_cluster_id === clusterID);
  if (!cluster) return;
  const metrics = cluster.metrics || {};
  const roleLock = instrumentationRoleLock(cluster);
  const missingRoleLock = dedupeStrings(roleLock.missing_components || []);
  const authority = instrumentationAuthority(instrumentationReportCache);
  const relatedTensions = relatedTensionsForProtoCluster(clusterID);
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Resolution</strong><br>' + esc(cluster.resolution_kind || 'entity') + '</div>';
  html += '<div><strong>Events</strong><br>' + esc(String(metrics.event_count || 0)) + '</div>';
  html += '<div><strong>Blocker Density</strong><br>' + esc(instrumentationPercent(metrics.blocker_density)) + '</div>';
  html += '<div><strong>Duplication Index</strong><br>' + esc(instrumentationPercent(metrics.duplication_index)) + '</div>';
  html += '<div><strong>Active Sessions</strong><br>' + esc(String(metrics.active_session_count || 0)) + '</div>';
  html += '<div><strong>Open Queues</strong><br>' + esc(String(metrics.open_queue_count || 0)) + '</div>';
  html += '<div><strong>Role-Lock Index</strong><br>' + esc(instrumentationRoleLockSummary(roleLock)) + '</div>';
  html += '<div><strong>Last Event</strong><br>' + esc(timeAgo(metrics.last_event_at || (instrumentationReportCache || {}).generated_at || '', authority)) + '</div>';
  html += '</div>';
  html += '<div style="margin-bottom:12px"><strong>Role-Lock Components</strong><div style="font-size:11px;color:var(--muted);margin-top:6px">Read-side anti-lock-in estimate from steward, accepted builder, and blocking reviewer concentration. Missing components stay visible rather than inferred.</div>';
  html += '<div style="font-size:11px;color:var(--muted);margin-top:4px">This is operator-facing evidence only; it does not assign roles, leases, or write policy.</div>';
  html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-top:10px">';
  html += '<div class="msg-item" style="margin:0"><strong>Steward HHI</strong><div style="margin-top:4px">' + esc(instrumentationPercent(roleLock.steward_hhi)) + '</div><div style="font-size:11px;color:var(--muted);margin-top:4px">Active stewards ' + esc(String(roleLock.active_steward_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Accepted Builder HHI</strong><div style="margin-top:4px">' + esc(instrumentationPercent(roleLock.accepted_builder_hhi)) + '</div><div style="font-size:11px;color:var(--muted);margin-top:4px">Active claims ' + esc(String(roleLock.active_claim_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Default Reviewer HHI</strong><div style="margin-top:4px">' + esc(instrumentationPercent(roleLock.default_reviewer_hhi)) + '</div><div style="font-size:11px;color:var(--muted);margin-top:4px">Blocking reviews ' + esc(String(roleLock.blocking_review_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Motif Reuse HHI</strong><div style="margin-top:4px">' + esc(missingRoleLock.includes('motif_reuse') ? 'unavailable' : instrumentationPercent(roleLock.motif_reuse_hhi)) + '</div><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(missingRoleLock.includes('motif_reuse') ? 'Component not yet surfaced in read-side metrics.' : 'Observed reuse concentration across the cluster.') + '</div></div>';
  html += '</div>';
  html += '<div style="margin-top:10px"><strong>Missing Components</strong><div style="margin-top:6px">' + (missingRoleLock.length
    ? ('<div style="display:flex;gap:6px;flex-wrap:wrap">' + missingRoleLock.map(value => '<span class="tool-badge kind">' + esc(value) + '</span>').join('') + '</div>')
    : '<div class="empty">No missing role-lock components are currently visible.</div>') + '</div></div>';
  html += '</div>';
  [['Tasks', cluster.task_ids], ['Sessions', cluster.session_ids], ['Docs', cluster.doc_keys], ['Artifacts', cluster.artifact_refs], ['Agents', cluster.agent_ids]].forEach(section => {
    if (!section[1] || !section[1].length) return;
    html += '<div style="margin-bottom:12px"><strong>' + esc(section[0]) + '</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + section[1].map(value => '<span class="tool-badge kind">' + esc(value) + '</span>').join('') + '</div></div>';
  });
  const actionButtons = [];
  if (cluster.task_ids && cluster.task_ids.length) actionButtons.push('<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){closeModal();switchTab('tasks');setTimeout(()=>showTaskDetail((cluster.task_ids[0]),(cluster.task_ids[0])),100)}) + '>Open Task</button>');
  if (cluster.session_ids && cluster.session_ids.length) actionButtons.push('<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();openSessionFromMemory((cluster.session_ids[0]))}) + '>Open Session</button>');
  if (cluster.doc_keys && cluster.doc_keys.length) actionButtons.push('<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();showDoc((cluster.doc_keys[0]))}) + '>Open Doc</button>');
  actionButtons.push('<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorSurface((clusterID))}) + '>Open Corridor Surface</button>');
  actionButtons.push('<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorBoundarySurface((clusterID))}) + '>Open Boundary Surface</button>');
  actionButtons.push('<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((clusterID))}) + '>Open Control Scaffold</button>');
  if (actionButtons.length) {
    html += '<div class="action-btn-row">' + actionButtons.join('') + '</div>';
  }
  html += '<div style="margin-bottom:12px"><strong>Tensions</strong><div style="margin-top:6px">';
  html += '<div style="display:flex;gap:8px;align-items:center;margin-bottom:8px"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();openTensionsForProtoCluster((clusterID))}) + '>Open Tensions Surface</button></div>';
  html += renderTensionLinkList(relatedTensions, 'No projected tensions are cached for this proto-cluster yet.');
  html += '</div></div>';
  const eventTypes = Object.entries(metrics.event_type_counts || {});
  if (eventTypes.length) {
    html += '<div style="margin-bottom:12px"><strong>Event Types</strong><div style="margin-top:6px">';
    html += eventTypes.sort((left, right) => right[1] - left[1]).map(entry =>
      '<div class="msg-item" style="margin-bottom:6px;display:flex;justify-content:space-between;gap:8px"><span>' + esc(entry[0]) + '</span><strong>' + esc(String(entry[1])) + '</strong></div>'
    ).join('');
    html += '</div></div>';
  }
  const agentShares = Object.entries(metrics.activity_share_by_agent || {});
  if (agentShares.length) {
    html += '<div style="margin-bottom:12px"><strong>Agent Activity Share</strong><div style="margin-top:6px">';
    html += agentShares.sort((left, right) => right[1] - left[1]).map(entry =>
      '<div class="msg-item" style="margin-bottom:6px;display:flex;justify-content:space-between;gap:8px"><span>' + esc(entry[0]) + '</span><strong>' + esc(instrumentationPercent(entry[1])) + '</strong></div>'
    ).join('');
    html += '</div></div>';
  }
  openModal('Proto-Cluster ' + esc(cluster.proto_cluster_id || clusterID), html);
}

async function loadInstrumentation() {
  const params = instrumentationFilterParams();
  try {
    const results = await Promise.all([
      rpc('workspace.instrumentation.report', params),
      rpc('workspace.instrumentation.clusters', params)
    ]);
    const reportResponse = results[0] || {};
    const clusterResponse = results[1] || {};
    instrumentationReportCache = reportResponse.report || null;
    instrumentationClustersCache = clusterResponse.clusters || ((instrumentationReportCache && instrumentationReportCache.clusters) || []);
    renderInstrumentationWorkspaceSummary(instrumentationReportCache);
    renderInstrumentationClusters(instrumentationClustersCache, clusterResponse.count || instrumentationClustersCache.length, instrumentationReportCache);
    renderInstrumentationSnapshotState();
    loadControlPolicyOverlay();
  } catch (e) {
    console.error('loadInstrumentation', e);
    instrumentationReportCache = null;
    instrumentationClustersCache = [];
    document.getElementById('instrumentation-generated-at').textContent = 'error';
    document.getElementById('instrumentation-filter-summary').style.display = 'none';
    document.getElementById('instrumentation-workspace-summary').innerHTML = '<div class="empty">' + esc(e.message || 'Failed to load instrumentation') + '</div>';
    document.getElementById('instrumentation-clusters-count').textContent = '0 total';
    document.getElementById('instrumentation-truncated-badge').textContent = 'error';
    document.getElementById('instrumentation-clusters-list').innerHTML = '<div class="empty">' + esc(e.message || 'Failed to load instrumentation clusters') + '</div>';
    loadControlPolicyOverlay();
  }
}

async function createInstrumentationSnapshot() {
  const btn = document.getElementById('instrumentation-snapshot-btn');
  const originalText = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Recording...';
  }
  try {
    const response = await rpc('workspace.instrumentation.snapshot', instrumentationFilterParams({actor_id: 'dashboard'}));
    instrumentationReportCache = response.report || instrumentationReportCache;
    instrumentationClustersCache = (instrumentationReportCache && instrumentationReportCache.clusters) || instrumentationClustersCache;
    instrumentationSnapshotEventCache = response.event || instrumentationSnapshotEventCache;
    renderInstrumentationWorkspaceSummary(instrumentationReportCache);
    renderInstrumentationClusters(instrumentationClustersCache, instrumentationClustersCache.length, instrumentationReportCache);
    renderInstrumentationSnapshotState();
    loadControlPolicyOverlay();
    await loadRuntimeEvents();
    toast('Instrumentation snapshot recorded');
  } catch (e) {
    console.error('workspace.instrumentation.snapshot', e);
    toast('Snapshot failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Record Snapshot';
    }
  }
}

function tensionFilterParams(extra = {}) {
  const params = {
    workspace_id: WS_ID,
    tension_type: document.getElementById('tension-filter-type')?.value || '',
    lifecycle_state: document.getElementById('tension-filter-lifecycle')?.value || '',
    review_status: document.getElementById('tension-filter-review')?.value || '',
    task_id: String(document.getElementById('tension-filter-task')?.value || '').trim(),
    agent_id: String(document.getElementById('tension-filter-agent')?.value || '').trim(),
    proto_cluster_id: String(document.getElementById('tension-filter-cluster')?.value || '').trim(),
    limit: 50
  };
  Object.keys(params).forEach(key => {
    if (params[key] === '' || params[key] === undefined || params[key] === null) delete params[key];
  });
  return Object.assign(params, extra || {});
}

function tensionRefreshParams() {
  const params = {
    workspace_id: WS_ID,
    actor_id: currentProfileId() || 'dashboard',
    proto_cluster_id: String(document.getElementById('tension-filter-cluster')?.value || '').trim(),
    limit: 200,
    cluster_limit: 20
  };
  Object.keys(params).forEach(key => {
    if (params[key] === '' || params[key] === undefined || params[key] === null) delete params[key];
  });
  return params;
}

function renderTensionFilterSummary(params) {
  const el = document.getElementById('tension-filter-summary');
  if (!el) return;
  const chips = [];
  if (params.tension_type) chips.push('type: ' + params.tension_type);
  if (params.lifecycle_state) chips.push('lifecycle: ' + params.lifecycle_state);
  if (params.review_status) chips.push('review: ' + params.review_status);
  if (params.task_id) chips.push('task: ' + params.task_id);
  if (params.agent_id) chips.push('agent: ' + params.agent_id);
  if (params.proto_cluster_id) chips.push('cluster: ' + params.proto_cluster_id);
  if (!chips.length) {
    el.style.display = 'none';
    el.textContent = '';
    return;
  }
  el.style.display = 'block';
  const refreshScoped = !params.tension_type && !params.lifecycle_state && !params.review_status && !params.task_id && !params.agent_id;
  const refreshNote = refreshScoped ? '' : '<div style="margin-top:6px;font-size:11px;color:var(--muted)">Refresh Frontier only scopes by proto-cluster. Type, lifecycle, review, task, and agent filters apply to the rendered frontier/list.</div>';
  el.innerHTML = '<div>Filters: ' + esc(chips.join(' | ')) + '</div>' + refreshNote;
}

function resetTensionFilters() {
  document.getElementById('tension-filter-type').value = '';
  document.getElementById('tension-filter-lifecycle').value = '';
  document.getElementById('tension-filter-review').value = '';
  document.getElementById('tension-filter-task').value = '';
  document.getElementById('tension-filter-agent').value = '';
  document.getElementById('tension-filter-cluster').value = '';
  loadTensions();
}

function tensionBadgeColor(tensionType) {
  const normalized = String(tensionType || '').toLowerCase();
  if (normalized === 'contradiction') return 'var(--red)';
  if (normalized === 'bottleneck') return 'var(--orange)';
  if (normalized === 'ambiguity') return 'var(--yellow)';
  if (normalized === 'bridge') return 'var(--accent2)';
  return 'var(--accent)';
}

function dedupeTensionRecords(items) {
  const seen = new Set();
  return (items || []).filter(item => {
    const key = String(item && item.tension_id || '').trim();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function currentTensionUniverse() {
  const detailRecords = Object.values(tensionDetailCache || {}).map(detail => detail && detail.tension).filter(Boolean);
  return dedupeTensionRecords([].concat(tensionsUniverseCache || [], tensionsCache || [], detailRecords));
}

function relatedTensionsForProtoCluster(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return [];
  return dedupeTensionRecords([]
    .concat(currentTensionUniverse())
    .concat((tensionFrontierCache || []).filter(item => String(item.proto_cluster_id || '') === clusterID))
    .filter(item => String(item.proto_cluster_id || '') === clusterID)
  );
}

function relatedTensionsForRuntimeEvent(item, payloadObj) {
  const event = item || {};
  const payload = payloadObj || {};
  const directID = String(payload.tension_id || (event.entity_type === 'tension' ? event.entity_id : '') || '').trim();
  const clusterID = String(payload.proto_cluster_id || '').trim();
  const taskID = String(event.task_id || payload.task_id || '').trim();
  const sessionID = String(event.session_id || payload.session_id || '').trim();
  const agentID = String(event.agent_id || payload.agent_id || '').trim();
  const universe = currentTensionUniverse();
  return dedupeTensionRecords(universe.filter(record => {
    if (!record) return false;
    if (directID && String(record.tension_id || '') === directID) return true;
    if (clusterID && String(record.proto_cluster_id || '') === clusterID) return true;
    if (taskID && Array.isArray(record.task_ids) && record.task_ids.includes(taskID)) return true;
    if (sessionID && Array.isArray(record.session_ids) && record.session_ids.includes(sessionID)) return true;
    if (agentID && Array.isArray(record.agent_ids) && record.agent_ids.includes(agentID)) return true;
    return false;
  }));
}

function renderTensionLinkList(items, emptyText) {
  if (!items || !items.length) {
    return '<div class="empty">' + esc(emptyText || 'No related tensions') + '</div>';
  }
  return items.map(item =>
    '<div class="msg-item" style="margin-bottom:6px">' +
      '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center">' +
        '<strong>' + esc(item.title || item.tension_id) + '</strong>' +
        '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showTensionDetail((item.tension_id))}) + '>Open</button>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:4px">' +
        esc([
          item.tension_type || 'tension',
          item.review_status || '',
          item.lifecycle_state || '',
          item.proto_cluster_id || '',
          item.surface_score !== undefined ? ('surface ' + String(item.surface_score)) : ''
        ].filter(Boolean).join(' | ')) +
      '</div>' +
    '</div>'
  ).join('');
}

function updateTensionTabBadge(items, frontier) {
  const badge = document.getElementById('tensions-badge');
  if (!badge) return;
  const pendingCount = (items || []).filter(item => String(item.review_status || '').toUpperCase() === 'PENDING' && String(item.lifecycle_state || '').toUpperCase() !== 'ARCHIVED').length;
  const visible = Math.max(Number((frontier || []).length || 0), pendingCount);
  if (visible > 0) {
    badge.style.display = '';
    badge.textContent = String(visible);
  } else {
    badge.style.display = 'none';
    badge.textContent = '0';
  }
}

function renderTensionSummary(items, frontier) {
  const el = document.getElementById('tension-workspace-summary');
  if (!el) return;
  const total = items.length;
  const refreshReport = ((tensionRefreshCache || {}).report || {});
  const active = items.filter(item => item.lifecycle_state === 'ACTIVE').length;
  const archived = items.filter(item => item.lifecycle_state === 'ARCHIVED').length;
  const pending = items.filter(item => item.review_status === 'PENDING').length;
  const confirmed = items.filter(item => item.review_status === 'CONFIRMED').length;
  const discarded = items.filter(item => item.review_status === 'DISCARDED').length;
  const strongest = items.slice().sort((a, b) => Number(b.surface_score || 0) - Number(a.surface_score || 0))[0];
  let html = '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px">';
  html += '<div class="msg-item" style="margin:0"><strong>Total</strong><div style="margin-top:4px">' + esc(String(total)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Active</strong><div style="margin-top:4px">' + esc(String(active)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Archived</strong><div style="margin-top:4px">' + esc(String(archived)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Pending</strong><div style="margin-top:4px">' + esc(String(pending)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Confirmed</strong><div style="margin-top:4px">' + esc(String(confirmed)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Discarded</strong><div style="margin-top:4px">' + esc(String(discarded)) + '</div></div>';
  html += '</div>';
  html += '<div style="display:flex;justify-content:space-between;gap:12px;margin-top:12px;font-size:12px;color:var(--muted)">';
  html += '<span>Frontier surfaced: ' + esc(String(frontier.length)) + '</span>';
  if (Number(refreshReport.frontier_capacity || 0) > 0) {
    html += '<span>Frontier Capacity: ' + esc(String(refreshReport.frontier_capacity || 0)) + '</span>';
  }
  if (refreshReport.free_agent_count !== undefined && refreshReport.free_agent_count !== null) {
    html += '<span>Free Agents: ' + esc(String(refreshReport.free_agent_count || 0)) + '</span>';
  }
  if (strongest) {
    html += '<span>Top signal: <a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showTensionDetail((strongest.tension_id))}) + ' style="color:var(--accent2)">' + esc(strongest.title || strongest.tension_id) + '</a> (' + esc(String(strongest.surface_score || 0)) + ')</span>';
  } else {
    html += '<span>No tensions projected yet.</span>';
  }
  html += '</div>';
  el.innerHTML = html;
  updateTensionTabBadge(items, frontier);
}

function renderTensionFrontier(items) {
  document.getElementById('tension-frontier-count').textContent = items.length;
  const el = document.getElementById('tension-frontier-list');
  if (!items.length) {
    el.innerHTML = '<div class="empty">No surfaced tensions. Run workspace.tension.refresh to project the current frontier.</div>';
    return;
  }
  el.innerHTML = items.map(item => {
    const color = tensionBadgeColor(item.tension_type);
    const authority = tensionAuthorityFor(item);
    const kind = String(item.kind || 'atomic');
    const members = Array.isArray(item.members) ? item.members.length : 0;
    return '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showTensionDetail((item.tension_id))}) + '>' +
      '<div class="action-title">' + esc(item.title || item.tension_id) + '</div>' +
      '<div class="action-meta">' +
        '<span style="color:' + color + '">' + esc((item.tension_type || 'tension') + ' / ' + kind) + '</span>' +
        '<span class="action-status ' + esc(String(item.review_status || 'PENDING').toUpperCase()) + '">' + esc(item.review_status || 'PENDING') + '</span>' +
        '<span>prio ' + esc(String(item.surfaced_priority || 0)) + '</span>' +
        '<span>vis ' + esc(String(item.visibility_score || 0)) + '</span>' +
        (members ? '<span>' + esc(String(members)) + ' members</span>' : '') +
        '<span>' + esc(timeAgo(item.last_seen_at || '', authority)) + '</span>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(item.summary || item.proto_cluster_id || 'No summary') + '</div>' +
    '</div>';
  }).join('');
}

function renderTensionList(items) {
  document.getElementById('tension-list-count').textContent = items.length;
  const el = document.getElementById('tension-list');
  if (!items.length) {
    el.innerHTML = '<div class="empty">No tensions match the current filter.</div>';
    return;
  }
  el.innerHTML = items.map(item => {
    const color = tensionBadgeColor(item.tension_type);
    const kind = String(item.kind || 'atomic');
    const blockedBy = Array.isArray(item.blocked_by_tension_ids) ? item.blocked_by_tension_ids.length : 0;
    const blocks = Array.isArray(item.blocks_tension_ids) ? item.blocks_tension_ids.length : 0;
    return '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showTensionDetail((item.tension_id))}) + '>' +
      '<div class="action-title">' + esc(item.title || item.tension_id) + '</div>' +
      '<div class="action-meta">' +
        '<span style="color:' + color + '">' + esc((item.tension_type || 'tension') + ' / ' + kind) + '</span>' +
        '<span class="action-status ' + esc(String(item.lifecycle_state || 'ACTIVE').toUpperCase()) + '">' + esc(item.lifecycle_state || 'ACTIVE') + '</span>' +
        '<span class="action-status ' + esc(String(item.review_status || 'PENDING').toUpperCase()) + '">' + esc(item.review_status || 'PENDING') + '</span>' +
        '<span>' + esc(String(item.evidence_count || 0) + ' evidence') + '</span>' +
        '<span>' + esc('blocked_by ' + blockedBy + ' / blocks ' + blocks) + '</span>' +
      '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(item.proto_cluster_id || 'No proto-cluster') + '</div>' +
    '</div>';
  }).join('');
}

function tensionDetailActions(detail) {
  const tension = detail.tension || {};
  const actions = [];
  const lifecycle = String(tension.lifecycle_state || '').toUpperCase();
  const review = String(tension.review_status || '').toUpperCase();
  const mutable = lifecycle === 'EMERGENT' || lifecycle === 'ACTIVE' || lifecycle === 'DORMANT';
  if (mutable) {
    if (review !== 'CONFIRMED') actions.push('<button class="btn-accent" ' + dashboardAction(function(dashboardEvent){actOnTension('confirm',(tension.tension_id))}) + '>Confirm</button>');
    if (review !== 'DISCARDED') actions.push('<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){actOnTension('discard',(tension.tension_id))}) + '>Discard</button>');
  }
  if (lifecycle === 'ACTIVE' || lifecycle === 'DORMANT') {
    actions.push('<button class="btn-accent" ' + dashboardAction(function(dashboardEvent){actOnTension('resolve',(tension.tension_id))}) + '>Resolve</button>');
  }
  if (lifecycle === 'ACTIVE') {
    actions.push('<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){actOnTension('dormant',(tension.tension_id))}) + '>Dormant</button>');
  }
  if (lifecycle === 'RESOLVED' || lifecycle === 'DISCARDED') {
    actions.push('<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){actOnTension('archive',(tension.tension_id))}) + '>Archive</button>');
  }
  return actions.join('');
}

function renderTensionDetailState(detail) {
  const container = document.getElementById('tension-detail-summary');
  const badge = document.getElementById('tension-detail-state');
  if (!detail || !detail.tension) {
    badge.textContent = 'none selected';
    container.innerHTML = '<div class="empty">Select a tension from the frontier or list to inspect evidence and take action.</div>';
    return;
  }
  const tension = detail.tension;
  const authority = tensionAuthorityFor(tension, detail.time_authority || null);
  badge.textContent = String(tension.review_status || 'PENDING') + ' / ' + String(tension.lifecycle_state || 'ACTIVE');
  let html = '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:12px">';
  html += '<div><div style="font-size:16px;font-weight:700;color:var(--text)">' + esc(tension.title || tension.tension_id) + '</div>';
  html += '<div style="font-size:12px;color:var(--muted);margin-top:4px">' + esc(tension.summary || tension.proto_cluster_id || '') + '</div></div>';
  html += '<div style="display:flex;gap:8px;flex-wrap:wrap">' + tensionDetailActions(detail) + '</div></div>';
  html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-bottom:12px">';
  html += '<div class="msg-item" style="margin:0"><strong>Type</strong><div style="margin-top:4px">' + esc(tension.tension_type || '') + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Kind</strong><div style="margin-top:4px">' + esc(tension.kind || '') + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Base Importance</strong><div style="margin-top:4px">' + esc(String(tension.base_importance || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Visibility Score</strong><div style="margin-top:4px">' + esc(String(tension.visibility_score || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Surfaced Priority</strong><div style="margin-top:4px">' + esc(String(tension.surfaced_priority || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Evidence</strong><div style="margin-top:4px">' + esc(String(tension.evidence_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Last Seen</strong><div style="margin-top:4px">' + esc(timeAgo(tension.last_seen_at || '', authority)) + '</div></div>';
  html += '</div>';
  html += '<div style="font-size:12px;color:var(--muted);margin-bottom:10px">Proto-cluster: ';
  if (tension.proto_cluster_id) {
    html += '<a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showProtoClusterDetail((tension.proto_cluster_id))}) + ' style="color:var(--accent2)">' + esc(tension.proto_cluster_id) + '</a>';
  } else {
    html += 'n/a';
  }
  html += '</div>';
  if (tension.proto_cluster_id) {
    html += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px">';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorSurface((tension.proto_cluster_id))}) + '>Open Corridor Surface</button>';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorBoundarySurface((tension.proto_cluster_id))}) + '>Open Boundary Surface</button>';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((tension.proto_cluster_id))}) + '>Open Control Scaffold</button>';
    if (controlPolicySnapshotEventCache && controlPolicySnapshotEventCache.event_id) {
      html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((controlPolicySnapshotEventCache.event_id))}) + '>Open Latest Advisory Snapshot</button>';
    }
    if (unifiedControlSnapshotEventCache && unifiedControlSnapshotEventCache.event_id) {
      html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((unifiedControlSnapshotEventCache.event_id))}) + '>Open Latest Unified Snapshot</button>';
    }
    html += '</div>';
  }
  if ((Array.isArray(tension.segment_refs) && tension.segment_refs.length) || (Array.isArray(detail.docs) && detail.docs.length) || (Array.isArray(detail.artifacts) && detail.artifacts.length)) {
    html += '<div style="font-size:11px;color:var(--muted);margin-bottom:12px">Artifact/doc segments stay read-only evidence anchors, separate from corridor readiness/fit.</div>';
  }
  [['Tasks', tension.task_ids], ['Sessions', tension.session_ids], ['Docs', tension.doc_keys], ['Agents', tension.agent_ids], ['Artifacts', tension.artifact_refs], ['Segments', tension.segment_refs], ['Constraints', tension.constraint_refs], ['Members', tension.members], ['Blocked By', tension.blocked_by_tension_ids], ['Blocks', tension.blocks_tension_ids]].forEach(section => {
    if (!Array.isArray(section[1]) || !section[1].length) return;
    html += '<div style="margin-bottom:12px"><strong>' + esc(section[0]) + '</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">';
    html += section[1].map(value => {
      if (section[0] === 'Tasks') return '<span class="tool-badge kind"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tasks');setTimeout(()=>showTaskDetail((value),(value)),100)}) + ' style="color:var(--accent2)">' + esc(value) + '</a></span>';
      if (section[0] === 'Sessions') return '<span class="tool-badge kind"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();openSessionFromMemory((value))}) + ' style="color:var(--accent2)">' + esc(value) + '</a></span>';
      if (section[0] === 'Docs') return '<span class="tool-badge kind"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showDoc((value))}) + ' style="color:var(--accent2)">' + esc(value) + '</a></span>';
      if (section[0] === 'Members' || section[0] === 'Blocked By' || section[0] === 'Blocks') return '<span class="tool-badge kind"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showTensionDetail((value))}) + ' style="color:var(--accent2)">' + esc(value) + '</a></span>';
      return '<span class="tool-badge kind">' + esc(value) + '</span>';
    }).join('');
    html += '</div></div>';
  });
  if ((Array.isArray(detail.dependencies) && detail.dependencies.length) || (Array.isArray(detail.dependents) && detail.dependents.length)) {
    [['Dependencies', detail.dependencies, 'blocked by'], ['Dependents', detail.dependents, 'blocks']].forEach(section => {
      if (!Array.isArray(section[1]) || !section[1].length) return;
      html += '<div style="margin-bottom:12px"><strong>' + esc(section[0]) + '</strong><div style="margin-top:6px">';
      html += section[1].map(edge => {
        const source = String((edge || {}).tension_id || '').trim();
        const target = String((edge || {}).depends_on_tension_id || '').trim();
        const verb = String(section[2] || '').trim();
        return '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px"><strong>' + esc((edge || {}).dependency_type || 'BLOCKS') + '</strong><span style="color:var(--muted)">' + esc(verb) + '</span></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">' +
            '<a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showTensionDetail((source))}) + ' style="color:var(--accent2)">' + esc(source) + '</a>' +
            ' → ' +
            '<a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showTensionDetail((target))}) + ' style="color:var(--accent2)">' + esc(target) + '</a>' +
          '</div>' +
        '</div>';
      }).join('');
      html += '</div></div>';
    });
  }
  if (Array.isArray(detail.evidence) && detail.evidence.length) {
    html += '<div style="margin-bottom:12px"><strong>Evidence</strong><div style="margin-top:6px">';
    html += detail.evidence.map(item => {
      const evidenceSegments = corridorSegmentEntries(item);
      return '<div class="msg-item" style="margin-bottom:6px">' +
        '<div style="display:flex;justify-content:space-between;gap:8px"><strong>' + esc(item.evidence_kind || 'evidence') + '</strong><span style="color:var(--muted)">' + esc(String(item.weight || 0)) + '</span></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(item.evidence_ref || item.summary || '') + '</div>' +
        renderSegmentBadgeRow('Segments', evidenceSegments) +
        (item.event_id ? '<div style="margin-top:6px"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((item.event_id))}) + '>Open Runtime Event</button></div>' : '') +
      '</div>';
    }).join('');
    html += '</div></div>';
  }
  if (detail.proto_cluster && detail.proto_cluster.metrics) {
    html += '<div style="margin-bottom:12px"><strong>Proto-Cluster Snapshot</strong><div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:6px">';
    html += '<div class="msg-item" style="margin:0"><strong>Events</strong><div style="margin-top:4px">' + esc(String(detail.proto_cluster.metrics.event_count || 0)) + '</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Queues</strong><div style="margin-top:4px">' + esc(String(detail.proto_cluster.metrics.open_queue_count || 0)) + '</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Blockers</strong><div style="margin-top:4px">' + esc(String(detail.proto_cluster.metrics.blocker_signal_count || 0)) + '</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Duplication</strong><div style="margin-top:4px">' + esc(instrumentationPercent(detail.proto_cluster.metrics.duplication_index || 0)) + '</div></div>';
    html += '</div></div>';
  }
  if (Array.isArray(detail.events) && detail.events.length) {
    html += '<div style="margin-bottom:12px"><strong>Runtime Events</strong><div style="margin-top:6px">';
    html += detail.events.slice(0, 8).map(item =>
      '<div class="msg-item" style="margin-bottom:6px">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(item.event_type || item.event_id) + '</strong><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((item.event_id))}) + '>Open</button></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([item.entity_type || '', item.entity_id || '', timeAgo(item.created_at)].filter(Boolean).join(' | ')) + '</div>' +
      '</div>'
    ).join('');
    html += '</div></div>';
  }
  if (Array.isArray(detail.claims) && detail.claims.length) {
    html += '<div style="margin-bottom:12px"><strong>Claims</strong><div style="margin-top:6px">' + detail.claims.map(item =>
      '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(item.subject || item.claim_id) + '</strong><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();showClaimDetail((item.claim_id))}) + '>Open</button></div><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(item.claim_state || item.claim_type || '') + '</div></div>'
    ).join('') + '</div></div>';
  }
  if (Array.isArray(detail.queues) && detail.queues.length) {
    html += '<div style="margin-bottom:12px"><strong>Queues</strong><div style="margin-top:6px">' + detail.queues.map(item =>
      '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(item.title || item.queue_key) + '</strong><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();showOperatorQueueDetail((item.queue_id))}) + '>Open</button></div><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(item.queue_type || '') + ' | ' + esc(item.queue_state || '') + '</div></div>'
    ).join('') + '</div></div>';
  }
  if (Array.isArray(detail.docs) && detail.docs.length) {
    html += '<div style="margin-bottom:12px"><strong>Docs</strong><div style="margin-top:6px">' + detail.docs.map(item => {
      const docSegments = corridorSegmentEntries(item);
      return '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(item.doc_key || item.title) + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();showDoc((item.doc_key || item.title))}) + '>Open</button>' + (item.event_id ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((item.event_id))}) + '>Runtime Event</button>' : '') + '</div></div><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(timeAgo(item.updated_at)) + '</div>' + renderSegmentBadgeRow('Document Segments', docSegments) + '</div>';
    }).join('') + '</div></div>';
  }
  if (Array.isArray(detail.artifacts) && detail.artifacts.length) {
    html += '<div style="margin-bottom:12px"><strong>Artifacts</strong><div style="margin-top:6px">' + detail.artifacts.map(item => {
      const artifactSegments = corridorSegmentEntries(item);
      return '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(item.title || item.artifact_ref || item.artifact_id) + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap">' + (item.doc_key ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();showDoc((item.doc_key))}) + '>Open Doc</button>' : '') + (item.event_id ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((item.event_id))}) + '>Runtime Event</button>' : '') + '</div></div><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([item.kind || '', item.content_type || '', item.artifact_ref || ''].filter(Boolean).join(' | ')) + '</div>' + renderSegmentBadgeRow('Artifact Segments', artifactSegments) + '</div>';
    }).join('') + '</div></div>';
  }
  container.innerHTML = html;
}

async function showTensionDetail(tensionID) {
  if (!tensionID) return;
  try {
    const detail = await rpc('workspace.tension.get', {workspace_id: WS_ID, tension_id: tensionID});
    tensionDetailCache[tensionID] = detail;
    renderTensionDetailState(detail);
    let modalHtml = '<div style="margin-bottom:10px;font-size:12px;color:var(--muted)">tension_id: ' + esc(tensionID) + '</div>';
    modalHtml += document.getElementById('tension-detail-summary').innerHTML;
    openModal('Tension ' + esc((detail.tension || {}).title || tensionID), modalHtml);
  } catch (e) {
    console.error('workspace.tension.get', e);
    toast('Tension detail failed: ' + e.message);
  }
}

async function loadTensions() {
  const params = tensionFilterParams();
  renderTensionFilterSummary(params);
  try {
    const frontierParams = Object.assign({}, params);
    const universeParams = {workspace_id: WS_ID, limit: 200};
    const results = await Promise.all([
      rpc('workspace.tension.list', params),
      rpc('workspace.tension.frontier', frontierParams),
      rpc('workspace.tension.list', universeParams)
    ]);
    const listResponse = results[0] || {};
    const frontierResponse = results[1] || {};
    const universeResponse = results[2] || {};
    tensionSurfaceTimeAuthority = listResponse.time_authority || frontierResponse.time_authority || universeResponse.time_authority || tensionSurfaceTimeAuthority;
    tensionsCache = listResponse.items || [];
    tensionsUniverseCache = universeResponse.items || tensionsCache;
    tensionFrontierCache = frontierResponse.items || [];
    renderTensionSummary(tensionsCache, tensionFrontierCache);
    renderTensionFrontier(tensionFrontierCache);
    renderTensionList(tensionsCache);
    renderTensionGeneratedAt();
    loadControlPolicyOverlay();
    loadOperatorInbox();
  } catch (e) {
    console.error('loadTensions', e);
    document.getElementById('tensions-generated-at').textContent = 'error';
    document.getElementById('tension-workspace-summary').innerHTML = '<div class="empty">' + esc(e.message || 'Failed to load tensions') + '</div>';
    document.getElementById('tension-frontier-list').innerHTML = '<div class="empty">' + esc(e.message || 'Failed to load frontier') + '</div>';
    document.getElementById('tension-list').innerHTML = '<div class="empty">' + esc(e.message || 'Failed to load tensions') + '</div>';
    document.getElementById('tension-frontier-count').textContent = '0';
    document.getElementById('tension-list-count').textContent = '0';
    tensionsCache = [];
    tensionFrontierCache = [];
    tensionsUniverseCache = [];
    tensionSurfaceTimeAuthority = null;
    updateTensionTabBadge([], []);
    loadControlPolicyOverlay();
    loadOperatorInbox();
  }
}

async function refreshTensions() {
  const btn = document.getElementById('tension-refresh-btn');
  const originalText = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Refreshing...';
  }
  try {
    const response = await rpc('workspace.tension.refresh', tensionRefreshParams());
    tensionRefreshCache = response.refresh || null;
    tensionSurfaceTimeAuthority = response.time_authority || ((response.refresh || {}).time_authority) || tensionSurfaceTimeAuthority;
    renderTensionGeneratedAt();
    await Promise.all([loadTensions(), loadRuntimeEvents(), loadInstrumentation()]);
    toast('Tension overlay refreshed');
  } catch (e) {
    console.error('workspace.tension.refresh', e);
    toast('Tension refresh failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Refresh Frontier';
    }
  }
}

async function actOnTension(action, tensionID) {
  const reasonInput = await dashboardPrompt('Reason (' + action + ')', '');
  if (reasonInput === null) return;
  const reason = String(reasonInput || '');
  const methodMap = {
    confirm: 'workspace.tension.confirm',
    discard: 'workspace.tension.discard',
    archive: 'workspace.tension.archive',
    resolve: 'workspace.tension.resolve',
    dormant: 'workspace.tension.dormant'
  };
  const method = methodMap[action] || '';
  if (!method) {
    toast('Unknown tension action: ' + action);
    return;
  }
  try {
    const response = await rpc(method, {
      workspace_id: WS_ID,
      tension_id: tensionID,
      actor_id: currentProfileId() || 'dashboard',
      reason: reason
    });
    if (response && response.tension) {
      tensionDetailCache[tensionID] = Object.assign({}, tensionDetailCache[tensionID] || {}, {tension: response.tension});
    }
    await Promise.all([loadTensions(), loadRuntimeEvents()]);
    await showTensionDetail(tensionID);
    toast('Tension ' + action + (action === 'archive' ? 'd' : action === 'recover' ? 'ed' : 'ed'));
  } catch (e) {
    console.error(method, e);
    toast('Tension action failed: ' + e.message);
  }
}

function openTensionsForProtoCluster(protoClusterID) {
  if (!protoClusterID) return;
  closeModal();
  switchTab('tensions');
  document.getElementById('tension-filter-type').value = '';
  document.getElementById('tension-filter-lifecycle').value = '';
  document.getElementById('tension-filter-review').value = '';
  document.getElementById('tension-filter-task').value = '';
  document.getElementById('tension-filter-agent').value = '';
  document.getElementById('tension-filter-cluster').value = protoClusterID;
  loadTensions();
}

function openTensionFromRuntimeEvent(tensionID) {
  if (!tensionID) return;
  closeModal();
  switchTab('tensions');
  setTimeout(() => showTensionDetail(tensionID), 80);
}

function openCorridorSurface(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return;
  controlPolicySelectedClusterID = clusterID;
  closeModal();
  switchTab('control');
  setTimeout(async () => {
    await Promise.allSettled([
      loadCorridorReadiness(),
      loadCorridorAuthority(),
      (typeof loadCorridorBasis === 'function' ? loadCorridorBasis() : Promise.resolve(null)),
      (typeof loadCorridorOwnership === 'function' ? loadCorridorOwnership() : Promise.resolve(null)),
      showCorridorReadinessClusterDetail(clusterID),
      (typeof showCorridorOwnershipClusterDetail === 'function' ? showCorridorOwnershipClusterDetail(clusterID) : Promise.resolve(null)),
      loadCorridorFit(),
      showCorridorFitClusterDetail(clusterID)
    ]);
  }, 100);
}

function focusCorridorFitSurface() {
  const target = document.getElementById('corridor-fit-summary') || document.getElementById('corridor-fit-state');
  if (target && typeof target.scrollIntoView === 'function') {
    target.scrollIntoView({behavior:'smooth', block:'start'});
  }
}

function openCorridorBoundarySurface(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return;
  controlPolicySelectedClusterID = clusterID;
  closeModal();
  switchTab('control');
  setTimeout(async () => {
    await Promise.allSettled([
      loadCorridorFit(),
      showCorridorFitClusterDetail(clusterID)
    ]);
    focusCorridorFitSurface();
  }, 100);
}

function openCorridorOwnershipSurface(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return;
  controlPolicySelectedClusterID = clusterID;
  closeModal();
  switchTab('control');
  setTimeout(async () => {
    await Promise.allSettled([
      loadCorridorOwnership(),
      showCorridorOwnershipClusterDetail(clusterID)
    ]);
    const target = document.getElementById('corridor-ownership-summary') || document.getElementById('corridor-ownership-state');
    if (target && typeof target.scrollIntoView === 'function') {
      target.scrollIntoView({behavior:'smooth', block:'start'});
    }
  }, 100);
}

function openControlScaffold(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return;
  controlPolicySelectedClusterID = clusterID;
  closeModal();
  switchTab('control');
  setTimeout(async () => {
    await Promise.allSettled([
      loadControlStateScaffold(),
      showControlPolicyClusterDetail(clusterID)
    ]);
  }, 100);
}

function controlPolicyFilterParams() {
  return {
    tension_type: String((document.getElementById('control-policy-filter-type') || {}).value || '').trim().toLowerCase(),
    attention_band: String((document.getElementById('control-policy-filter-mode') || {}).value || '').trim().toLowerCase(),
    query: String((document.getElementById('control-policy-filter-query') || {}).value || '').trim().toLowerCase()
  };
}

function resetControlPolicyOverlayFilters() {
  document.getElementById('control-policy-filter-type').value = '';
  document.getElementById('control-policy-filter-mode').value = '';
  document.getElementById('control-policy-filter-query').value = '';
  loadControlPolicyOverlay();
}

function dedupeStrings(items) {
  return Array.from(new Set((items || []).map(value => String(value || '').trim()).filter(Boolean))).sort();
}

function controlPolicyTimeValue(ts) {
  const value = Date.parse(String(ts || '').trim());
  return Number.isFinite(value) ? value : 0;
}

function controlPolicyModeColor(mode) {
  const normalized = String(mode || '').toLowerCase();
  if (normalized === 'hot') return 'var(--red)';
  if (normalized === 'watch') return 'var(--yellow)';
  return 'var(--muted)';
}

function controlPolicyReportParams(extra = {}) {
  const params = {
    workspace_id: WS_ID,
    limit: 40
  };
  Object.keys(extra || {}).forEach(key => {
    if (extra[key] !== undefined && extra[key] !== null && extra[key] !== '') params[key] = extra[key];
  });
  return params;
}

function controlCapabilityFlagEntries(flags) {
  const source = flags || {};
  return [
    {key:'belief_live', label:'belief', enabled: !!source.belief_live},
    {key:'anomaly_shadow', label:'anomaly shadow', enabled: !!source.anomaly_shadow},
    {key:'state_shadow', label:'state shadow', enabled: !!source.state_shadow},
    {key:'forecast_shadow', label:'forecast shadow', enabled: !!source.forecast_shadow},
    {key:'safe_local_autonomics_live', label:'local autonomics', enabled: !!source.safe_local_autonomics_live},
    {key:'governed_hints_live', label:'governed hints', enabled: !!source.governed_hints_live},
    {key:'strong_consequences_live', label:'strong consequences', enabled: !!source.strong_consequences_live}
  ];
}

function renderControlCapabilityFlags(flags) {
  const entries = controlCapabilityFlagEntries(flags);
  if (!entries.length) {
    return '<div class="empty">Capability flags unavailable.</div>';
  }
  return entries.map(entry => {
    const style = entry.enabled
      ? 'background:#16a34a22;color:#16a34a'
      : 'background:#64748b22;color:var(--muted)';
    return '<span class="tool-badge kind" style="' + style + '">' + esc(entry.label + ' ' + (entry.enabled ? 'live' : 'off')) + '</span>';
  }).join('');
}

function rspTelemetryLatestAt(summary) {
  if (!summary) return '';
  const values = [summary.latest_belief_at || '', summary.latest_anomaly_at || '', summary.latest_state_at || ''].filter(Boolean).sort();
  return values.length ? values[values.length - 1] : '';
}

function renderTelemetryCoverageBadges(gaps) {
  const items = Array.isArray(gaps) ? gaps.filter(Boolean) : [];
  if (!items.length) return '<span class="tool-badge active">No coverage gaps</span>';
  return items.map(item => '<span class="tool-badge kind">' + esc(String(item).replaceAll('_', ' ')) + '</span>').join('');
}

function renderControlActionBadgeRow(items, emptyLabel, tone = 'neutral') {
  const values = dedupeStrings(items || []);
  if (!values.length) {
    return '<div class="empty">' + esc(emptyLabel) + '</div>';
  }
  const toneStyle = tone === 'positive'
    ? 'background:#16a34a22;color:#16a34a'
    : (tone === 'warning'
      ? 'background:rgba(214,162,60,.14);color:var(--yellow)'
      : (tone === 'danger'
        ? 'background:rgba(224,106,106,.14);color:var(--red)'
        : 'background:#64748b22;color:var(--muted)'));
  return '<div style="display:flex;gap:6px;flex-wrap:wrap">' + values.map(value =>
    '<span class="tool-badge kind" style="' + toneStyle + '">' + esc(value) + '</span>'
  ).join('') + '</div>';
}

function unifiedControlAcceptanceChecklistNotApplicable(readiness) {
  return ['UNAVAILABLE', 'ALIGNED'].includes(String(readiness || '').trim());
}

function unifiedControlAcceptanceChecklistCounts(readiness, checklist) {
  if (!checklist || unifiedControlAcceptanceChecklistNotApplicable(readiness)) {
    return { clear: 0, total: 0 };
  }
  const value = String(readiness || '').trim();
  if (value === 'READY_PENDING') {
    return {
      clear: checklist.cooldown_clear ? 1 : 0,
      total: 1,
    };
  }
  if (value === 'BLOCKED') {
    const total =
      (checklist.contradiction_clear ? 0 : 1) +
      (checklist.memory_attention_clear ? 0 : 1);
    return {
      clear: 0,
      total,
    };
  }
  if (value === 'OBSERVING') {
    return {
      clear: 0,
      total: 0,
    };
  }
  if (value === 'WARMING') {
    return {
      clear:
        (checklist.candidate_present ? 1 : 0) +
        (checklist.candidate_diverges ? 1 : 0) +
        (checklist.hysteresis_satisfied ? 1 : 0) +
        (checklist.contradiction_clear ? 1 : 0) +
        (checklist.memory_attention_clear ? 1 : 0),
      total: 5,
    };
  }
  return {
    clear:
      (checklist.candidate_present ? 1 : 0) +
      (checklist.candidate_diverges ? 1 : 0) +
      (checklist.hysteresis_satisfied ? 1 : 0) +
      (checklist.cooldown_clear ? 1 : 0) +
      (checklist.contradiction_clear ? 1 : 0) +
      (checklist.memory_attention_clear ? 1 : 0),
    total: 6,
  };
}

function unifiedControlCooldownReasonLabel(reason) {
  const value = String(reason || '').trim();
  switch (value) {
    case 'no_candidate_mode':
      return 'no candidate mode';
    case 'candidate_aligned':
      return 'candidate aligned';
    case 'contradictions_and_memory_attention':
      return 'contradictions and memory attention';
    case 'contradictions_present':
      return 'contradictions present';
    case 'memory_attention_active':
      return 'memory attention active';
    case 'ready_window_pending_cooldown':
      return 'ready window pending cooldown';
    case 'ready_window_open':
      return 'ready window open';
    case 'hysteresis_pending':
      return 'hysteresis pending';
    case 'candidate_streak_not_started':
      return 'candidate streak not started';
    case 'candidate_transition_pending':
      return 'candidate transition pending';
    case 'candidate_transition_observing':
      return 'candidate transition observing';
    default:
      return value || 'n/a';
  }
}

function unifiedControlAcceptanceGateLabel(reason) {
  const value = String(reason || '').trim();
  switch (value) {
    case 'NO_CANDIDATE':
      return 'no candidate';
    case 'ALREADY_ALIGNED':
      return 'already aligned';
    case 'CONTRADICTIONS_AND_MEMORY_ATTENTION':
      return 'contradictions and memory attention';
    case 'CONTRADICTIONS_PRESENT':
      return 'contradictions present';
    case 'COOLDOWN_ACTIVE':
      return 'cooldown active';
    case 'MEMORY_ATTENTION_ACTIVE':
      return 'memory attention active';
    case 'OBSERVING_CANDIDATE':
      return 'observing candidate';
    case 'READY_WINDOW_OPEN':
      return 'ready window open';
    case 'STREAK_BELOW_HYSTERESIS':
      return 'streak below hysteresis';
    default:
      return value || 'n/a';
  }
}

function unifiedControlStageLabel(stage) {
  const value = String(stage || '').trim();
  switch (value) {
    case 'NO_CANDIDATE':
      return 'no candidate';
    case 'ALIGNED':
      return 'aligned';
    case 'OBSERVING':
      return 'observing';
    case 'WARMING':
      return 'warming';
    case 'READY_WINDOW':
      return 'ready window';
    default:
      return value || 'n/a';
  }
}

function unifiedControlAcceptanceReadinessLabel(readiness) {
  const value = String(readiness || '').trim();
  switch (value) {
    case 'UNAVAILABLE':
      return 'unavailable';
    case 'ALIGNED':
      return 'aligned';
    case 'BLOCKED':
      return 'blocked';
    case 'OBSERVING':
      return 'observing';
    case 'WARMING':
      return 'warming';
    case 'READY_PENDING':
      return 'ready pending';
    default:
      return value || 'n/a';
  }
}

function unifiedControlAcceptanceProgressLabel(progress) {
  const value = String(progress || '').trim();
  switch (value) {
    case 'NONE':
      return 'None';
    case 'ALIGNED':
      return 'Aligned';
    case 'BLOCKED':
      return 'Blocked';
    case 'EARLY':
      return 'Early';
    case 'PARTIAL':
      return 'Partial';
    case 'NEARLY_READY':
      return 'Nearly Ready';
    case 'READY_WINDOW_PENDING':
      return 'Ready Window Pending';
    case 'FULLY_CLEAR':
      return 'Ready Window Clear';
    default:
      return value || 'n/a';
  }
}

function unifiedControlModeLabel(mode) {
  const value = String(mode || '').trim();
  switch (value) {
    case 'STEADY':
      return 'steady';
    case 'ANTI_COLLAPSE':
      return 'anti-collapse';
    case 'COHERENCE':
      return 'coherence';
    case 'DECENTRALIZE':
      return 'decentralize';
    case 'SYNERGY_SEEKING':
      return 'synergy seeking';
    case 'UNFREEZE':
      return 'unfreeze';
    case 'STABILIZE':
      return 'stabilize';
    default:
      return value || 'n/a';
  }
}

function unifiedControlBlockingReasonLabel(reason) {
  const value = String(reason || '').trim();
  switch (value) {
    case 'cooldown_active':
      return 'Cooldown Active';
    case 'streak_below_hysteresis':
      return 'Streak Below Hysteresis';
    case 'contradictions_present':
      return 'Contradictions Present';
    case 'memory_attention_active':
      return 'Memory Attention Active';
    default:
      return value || 'n/a';
  }
}

function unifiedControlAcceptanceRequirementLabel(readiness, requirement) {
  const normalizedReadiness = String(readiness || '').trim();
  const value = String(requirement || '').trim();
  switch (value) {
    case 'candidate_present':
      return 'Candidate Present';
    case 'candidate_diverges':
      return 'Candidate Diverges';
    case 'hysteresis_satisfied':
      return 'Hysteresis Satisfied';
    case 'cooldown_clear':
      return normalizedReadiness === 'READY_PENDING' ? 'Ready Window Cooldown Clear' : 'Cooldown Clear';
    case 'contradiction_clear':
      return 'Contradiction Clear';
    case 'memory_attention_clear':
      return 'Memory Attention Clear';
    default:
      return value || 'n/a';
  }
}

function renderUnifiedControlActionAudit(items) {
  const entries = items || [];
  if (!entries.length) {
    return '<div class="empty">No structured applied-action trace recorded.</div>';
  }
  return entries.map(entry => {
    const meta = [];
    const sourceKinds = dedupeStrings(entry.source_kinds || []);
    const sourceKindLabels = sourceKinds.map(kind => unifiedControlAuditSummaryKeyLabel(kind));
    const hintIDs = dedupeStrings(entry.hint_ids || []);
    const deltaFields = dedupeStrings(entry.delta_fields || []);
    if (sourceKindLabels.length) meta.push('sources ' + sourceKindLabels.join(', '));
    if (hintIDs.length) meta.push('hints ' + hintIDs.join(', '));
    if (deltaFields.length) meta.push('changed ' + deltaFields.join(', '));
    else meta.push('no parameter delta');
    const summary = String(entry.summary || '').trim();
    const actionLabel = unifiedControlAuditSummaryKeyLabel(entry.action || 'action');
    return '<div class="msg-item" style="margin-bottom:6px">' +
      '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>' + esc(actionLabel) + '</strong><span style="font-size:11px;color:var(--muted)">' + esc(sourceKindLabels.join(', ') || 'advisory') + '</span></div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(meta.join(' | ')) + '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(summary || 'Inspectability-only action trace over the current unified arbitration result.') + '</div>' +
    '</div>';
  }).join('');
}

function renderUnifiedControlSuppressedHintAudit(items) {
  const entries = items || [];
  if (!entries.length) {
    return '<div class="empty">No structured suppressed-hint trace recorded.</div>';
  }
  return entries.map(entry => {
    const meta = [];
    const sourceKindLabel = unifiedControlAuditSummaryKeyLabel(entry.source_kind);
    const reasonLabel = unifiedControlAuditSummaryKeyLabel(entry.reason);
    const actionLabel = unifiedControlAuditSummaryKeyLabel(entry.action);
    if (entry.source_kind) meta.push(sourceKindLabel);
    if (entry.action) meta.push('action ' + actionLabel);
    if (entry.reason) meta.push('reason ' + reasonLabel);
    const summary = String(entry.summary || '').trim();
    return '<div class="msg-item" style="margin-bottom:6px">' +
      '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>' + esc(entry.hint_id || 'rsp_hint') + '</strong><span style="font-size:11px;color:var(--muted)">' + esc(sourceKindLabel || 'advisory') + '</span></div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(meta.join(' | ')) + '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(summary || 'Inspectability-only suppression trace over the current unified arbitration result.') + '</div>' +
    '</div>';
  }).join('');
}

function unifiedControlAuditSummaryKeyLabel(value) {
  const normalized = String(value || '').trim();
  switch (normalized) {
    case 'governed_hint':
      return 'Governed Hint';
    case 'memory_coherence_floor':
      return 'Memory Coherence Floor';
    case 'prefer_kernel_refresh':
      return 'Prefer Kernel Refresh';
    case 'raise_reviewer_diversity':
      return 'Raise Reviewer Diversity';
    case 'reduce_solver_fanout':
      return 'Reduce Solver Fanout';
    case 'tighten_context_cap':
      return 'Tighten Context Cap';
    case 'require_far_reviewer':
      return 'Require Far Reviewer';
    case 'mode_cooldown':
      return 'Mode Cooldown';
    case 'mode_cooldown_active':
      return 'Mode Cooldown Active';
    case 'requires_memory_pressure':
      return 'Requires Memory Pressure';
    case 'unsupported_actuation_class':
      return 'Unsupported Actuation Class';
    case 'unsupported_action':
      return 'Unsupported Action';
    case 'no_actions':
      return 'No Actions';
    case 'expired':
      return 'Expired';
    default:
      if (!normalized) return 'n/a';
      return normalized
        .split('_')
        .filter(Boolean)
        .map(part => part.charAt(0).toUpperCase() + part.slice(1))
        .join(' ');
  }
}

function renderUnifiedControlAuditSummary(summary) {
  const item = summary || null;
  if (!item || (!Number(item.applied_entry_count || 0) && !Number(item.suppressed_entry_count || 0))) {
    return '<div class="empty">No audit summary available.</div>';
  }
  const renderCounts = (title, counts, emptyLabel) => {
    const entries = Object.entries(counts || {}).sort((a, b) => String(a[0]).localeCompare(String(b[0])));
    return '<div style="margin-top:8px"><strong>' + esc(title) + '</strong><div style="margin-top:6px">' +
      renderControlActionBadgeRow(entries.map(entry => unifiedControlAuditSummaryKeyLabel(entry[0]) + ' ' + String(entry[1])), emptyLabel, 'neutral') +
    '</div></div>';
  };
  let html = '<div class="msg-item" style="margin-bottom:6px">';
  html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Applied Entries ' + esc(String(item.applied_entry_count || 0)) + ' | Suppressed Entries ' + esc(String(item.suppressed_entry_count || 0)) + '</strong><span style="font-size:11px;color:var(--muted)">inspectability only</span></div>';
  html += '<div style="margin-top:6px">' + renderControlActionBadgeRow([
    'Hint-backed Actions ' + String(item.hint_backed_action_count || 0),
    'Delta-bearing Actions ' + String(item.delta_bearing_action_count || 0),
    'Suppressed Action Refs ' + String(item.suppressed_entries_with_action_ref_count || 0),
  ], 'No audit rollups recorded.', 'neutral') + '</div>';
  html += renderCounts('Applied Source Kinds', item.applied_source_kind_count, 'No applied source-kind counts recorded.');
  html += renderCounts('Suppression Reasons', item.suppression_reason_count, 'No suppression reasons recorded.');
  html += renderCounts('Suppressed Source Kinds', item.suppressed_source_kind_count, 'No suppressed source-kind counts recorded.');
  html += '</div>';
  return html;
}

function renderUnifiedControlAuditCoverage(coverage) {
  const item = coverage || null;
  if (!item) {
    return '<div class="empty">No audit trace coverage available.</div>';
  }
  return '<div class="msg-item" style="margin-bottom:6px">' +
    '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Trace Coverage</strong><span style="font-size:11px;color:var(--muted)">inspectability only</span></div>' +
    '<div style="margin-top:6px">' + renderControlActionBadgeRow([
      'Applied Full Trace ' + String(item.full_applied_trace_entry_count || 0),
      'Suppressed Full Trace ' + String(item.full_suppressed_trace_entry_count || 0),
    ], 'No audit trace coverage recorded.', 'neutral') + '</div>' +
    '<div style="margin-top:8px"><strong>Applied Trace Fields</strong><div style="margin-top:6px">' + renderControlActionBadgeRow([
      'Source Kinds ' + String(item.applied_entries_with_source_kinds || 0),
      'Hint Refs ' + String(item.applied_entries_with_hint_refs || 0),
      'Delta Fields ' + String(item.applied_entries_with_delta_fields || 0),
      'Summaries ' + String(item.applied_entries_with_summary || 0),
    ], 'No applied trace-field coverage recorded.', 'neutral') + '</div></div>' +
    '<div style="margin-top:8px"><strong>Suppressed Trace Fields</strong><div style="margin-top:6px">' + renderControlActionBadgeRow([
      'Source Kind ' + String(item.suppressed_entries_with_source_kind || 0),
      'Action Ref ' + String(item.suppressed_entries_with_action_ref || 0),
      'Reasons ' + String(item.suppressed_entries_with_reason || 0),
      'Summaries ' + String(item.suppressed_entries_with_summary || 0),
    ], 'No suppressed trace-field coverage recorded.', 'neutral') + '</div></div>' +
  '</div>';
}

function renderUnifiedEffectiveControlBasis(items) {
  const entries = items || [];
  if (!entries.length) {
    return '<div class="empty">No effective-control basis available.</div>';
  }
  return entries.map(entry => {
    const meta = [];
    const actions = dedupeStrings(entry.applied_actions || []).map(action => unifiedControlAuditSummaryKeyLabel(action));
    const sourceKinds = dedupeStrings(entry.source_kinds || []);
    const hintIDs = dedupeStrings(entry.hint_ids || []);
    meta.push('suggested ' + String(entry.suggested_value || 'n/a') + ' -> effective ' + String(entry.effective_value || 'n/a'));
    if (actions.length) meta.push('actions ' + actions.join(', '));
    else meta.push(entry.changed ? 'no current delta-bearing trace' : 'inherits suggested control');
    if (sourceKinds.length) meta.push('sources ' + sourceKinds.join(', '));
    if (hintIDs.length) meta.push('hints ' + hintIDs.join(', '));
    const summary = String(entry.summary || '').trim();
    return '<div class="msg-item" style="margin-bottom:6px">' +
      '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>' + esc(unifiedControlBasisFieldLabel(entry.field || 'control')) + '</strong><span style="font-size:11px;color:var(--muted)">' + esc(entry.changed ? 'changed' : 'inherited') + '</span></div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(meta.join(' | ')) + '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(summary || 'Per-control inspectability over the current effective controls; not a second arbiter or control authority.') + '</div>' +
    '</div>';
  }).join('');
}

function unifiedControlBasisFieldLabel(field) {
  const value = String(field || '').trim();
  switch (value) {
    case 'priority_focus':
      return 'Priority Focus';
    case 'fanout_cap':
      return 'Fanout Cap';
    case 'review_depth':
      return 'Review Depth';
    case 'context_cap':
      return 'Context Cap';
    case 'bridge_quota':
      return 'Bridge Quota';
    case 'merge_threshold':
      return 'Merge Threshold';
    default:
      return value || 'control';
  }
}

function renderUnifiedControlBasisSummary(summary) {
  const item = summary || null;
  if (!item || !Number(item.field_count || 0)) {
    return '<div class="empty">No effective-control basis summary available.</div>';
  }
  return '<div class="msg-item" style="margin-bottom:6px">' +
    '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Basis Summary</strong><span style="font-size:11px;color:var(--muted)">inspectability only</span></div>' +
    '<div style="margin-top:6px">' + renderControlActionBadgeRow([
      'Basis Fields ' + String(item.field_count || 0),
      'Changed Fields ' + String(item.changed_field_count || 0),
      'Action-Backed ' + String(item.fields_with_action_trace_count || 0),
      'Hint-Backed ' + String(item.fields_with_hint_trace_count || 0),
      'Multi-Source ' + String(item.fields_with_multi_source_count || 0),
    ], 'No effective-control basis summary recorded.', 'neutral') + '</div>' +
  '</div>';
}

function unifiedControlContradictionFamilyLabel(family) {
  const value = String(family || '').trim();
  switch (value) {
    case 'hard_safety_clamp':
      return 'Hard Safety Clamp';
    case 'memory_safety_override':
      return 'Memory Safety Override';
    case 'other':
      return 'Other';
    default:
      return value || 'n/a';
  }
}

function renderUnifiedControlContradictionSummary(summary) {
  const item = summary || null;
  if (!item || !Number(item.total_count || 0)) {
    return '<div class="empty">No contradiction summary available.</div>';
  }
  const familyEntries = Object.entries(item.family_count || {}).sort((a, b) => String(a[0]).localeCompare(String(b[0])));
  return '<div class="msg-item" style="margin-bottom:6px">' +
    '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Contradiction Summary</strong><span style="font-size:11px;color:var(--muted)">inspectability only</span></div>' +
    '<div style="margin-top:6px">' + renderControlActionBadgeRow([
      'Total ' + String(item.total_count || 0),
      'Hard Safety Clamp ' + String(item.hard_safety_clamp_count || 0),
      'Memory Safety Override ' + String(item.memory_safety_override_count || 0),
      'Other ' + String(item.other_count || 0),
    ], 'No contradiction summary recorded.', 'danger') + '</div>' +
    '<div style="margin-top:8px"><strong>Families</strong><div style="margin-top:6px">' +
      renderControlActionBadgeRow(familyEntries.map(entry => unifiedControlContradictionFamilyLabel(entry[0]) + ' ' + String(entry[1])), 'No contradiction families recorded.', 'neutral') +
    '</div></div>' +
  '</div>';
}

function renderUnifiedControlCooldownBasis(item) {
  const basis = item || null;
  if (!basis) {
    return '<div class="empty">No cooldown basis available.</div>';
  }
  const blockers = dedupeStrings(basis.blocking_reasons || []).map(reason => unifiedControlBlockingReasonLabel(reason));
  const acceptanceReadiness = String(basis.acceptance_readiness || '').trim();
  const acceptanceProgressBand = String(basis.acceptance_progress_band || '').trim();
  const rawReason = String(basis.reason || '').trim();
  const displayReason = unifiedControlCooldownReasonLabel(basis.reason);
  const acceptanceProgressBadge = 'Acceptance Progress ' + unifiedControlAcceptanceProgressLabel(acceptanceProgressBand);
  const badges = [
    'Current ' + unifiedControlModeLabel(basis.current_mode),
    'Candidate ' + unifiedControlModeLabel(basis.candidate_mode),
    'Stage ' + unifiedControlStageLabel(basis.stage),
    'Acceptance Readiness ' + unifiedControlAcceptanceReadinessLabel(basis.acceptance_readiness),
    'Acceptance Gate ' + unifiedControlAcceptanceGateLabel(basis.acceptance_gate_reason),
    acceptanceProgressBadge,
    'Candidate Streak ' + String(basis.candidate_streak || 0),
    'Required Streak ' + String(basis.required_streak || 0),
    'Remaining Streak ' + String(basis.remaining_streak || 0),
    basis.ready_to_stabilize ? 'Ready To Stabilize' : 'Not Ready To Stabilize',
    basis.cooldown_active ? 'Cooldown Active' : 'Cooldown Inactive',
    basis.transitioning ? 'Transitioning Yes' : 'Transitioning No',
    'Reason ' + displayReason,
  ];
  const readyWindowReason = String(basis.reason || '').trim();
  if (readyWindowReason === 'ready_window_pending_cooldown') {
    badges.push('Ready Window Pending Cooldown');
  } else if (readyWindowReason === 'ready_window_open') {
    badges.push('Ready Window Open');
  }
  let html = '<div class="msg-item" style="margin-bottom:6px">' +
    '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Cooldown Basis</strong><span style="font-size:11px;color:var(--muted)">inspectability only</span></div>' +
    '<div style="margin-top:6px">' + renderControlActionBadgeRow(badges, 'No cooldown basis recorded.', 'neutral') + '</div>' +
    '<div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(String(basis.summary || 'Current mode/candidate-mode cooldown context over the advisory unified-control read-side; inspectability only.')) + '</div>';
  const inactiveAcceptancePath = unifiedControlAcceptanceChecklistNotApplicable(acceptanceReadiness);
  const checklist = basis.acceptance_checklist || null;
  if (checklist) {
    if (inactiveAcceptancePath) {
      html += '<div style="margin-top:8px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Acceptance Checklist</strong><span style="font-size:11px;color:var(--muted)">' +
        esc('readiness-aware 0/0 clear') +
      '</span></div><div style="margin-top:6px">' +
        renderControlActionBadgeRow([], 'Acceptance path not active.', 'neutral') +
      '</div></div>';
    } else if (acceptanceReadiness === 'BLOCKED') {
      const checklistCounts = unifiedControlAcceptanceChecklistCounts(acceptanceReadiness, checklist);
      const checklistBadges = [];
      if (!checklist.contradiction_clear) checklistBadges.push('Contradictions Present');
      if (!checklist.memory_attention_clear) checklistBadges.push('Memory Attention Active');
      html += '<div style="margin-top:8px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Acceptance Checklist</strong><span style="font-size:11px;color:var(--muted)">' +
        esc('active-blocker-scoped ' + String(checklistCounts.clear) + '/' + String(checklistCounts.total) + ' clear') +
      '</span></div><div style="margin-top:6px">' +
        renderControlActionBadgeRow(checklistBadges, 'No active blockers recorded.', 'neutral') +
      '</div></div>';
    } else if (acceptanceReadiness === 'READY_PENDING') {
      const checklistCounts = unifiedControlAcceptanceChecklistCounts(acceptanceReadiness, checklist);
      const checklistBadges = [
        checklist.cooldown_clear ? 'Ready Window Open' : 'Ready Window Pending Cooldown',
      ];
      html += '<div style="margin-top:8px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Acceptance Checklist</strong><span style="font-size:11px;color:var(--muted)">' +
        esc('ready-window ' + String(checklistCounts.clear) + '/' + String(checklistCounts.total) + ' clear') +
      '</span></div><div style="margin-top:6px">' +
        renderControlActionBadgeRow(checklistBadges, 'No ready-window checklist recorded.', checklist.cooldown_clear ? 'neutral' : 'warning') +
      '</div></div>';
    } else {
      const cooldownDeferred = ['OBSERVING', 'WARMING'].includes(acceptanceReadiness) && !checklist.cooldown_clear;
      const checklistCounts = unifiedControlAcceptanceChecklistCounts(acceptanceReadiness, checklist);
      const checklistCountLabel = acceptanceReadiness === 'OBSERVING'
        ? ('observing-deferred ' + String(checklistCounts.clear) + '/' + String(checklistCounts.total) + ' clear')
        : ('readiness-aware ' + String(checklistCounts.clear) + '/' + String(checklistCounts.total) + ' clear');
      const cooldownBadge = checklist.cooldown_clear ? 'Cooldown Clear' : (cooldownDeferred ? 'Cooldown Deferred' : 'Cooldown Blocking');
      const hysteresisBadge = acceptanceReadiness === 'OBSERVING'
        ? (checklist.hysteresis_satisfied ? 'Hysteresis Satisfied' : 'Hysteresis Deferred')
        : (checklist.hysteresis_satisfied ? 'Hysteresis Satisfied' : 'Hysteresis Pending');
      const checklistBadges = [
        checklist.candidate_present ? 'Candidate Present' : 'Candidate Missing',
        checklist.candidate_diverges ? 'Candidate Diverges' : 'Candidate Not Diverged',
        hysteresisBadge,
        cooldownBadge,
        checklist.contradiction_clear ? 'Contradiction Clear' : 'Contradictions Present',
        checklist.memory_attention_clear ? 'Memory Attention Clear' : 'Memory Attention Active',
      ];
      html += '<div style="margin-top:8px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Acceptance Checklist</strong><span style="font-size:11px;color:var(--muted)">' +
        esc(checklistCountLabel) +
      '</span></div><div style="margin-top:6px">' +
        renderControlActionBadgeRow(checklistBadges, 'No acceptance checklist recorded.', 'neutral') +
      '</div></div>';
    }
  }
  const missingRequirements = dedupeStrings(basis.acceptance_missing_requirements || []);
  const observingCandidateNotStarted = acceptanceReadiness === 'OBSERVING' && String(basis.reason || '').trim() === 'candidate_streak_not_started';
  const readyWindowClear = acceptanceReadiness === 'READY_PENDING' && String(basis.reason || '').trim() === 'ready_window_open' && missingRequirements.length === 0;
  const displayMissingRequirements = missingRequirements.map(requirement => unifiedControlAcceptanceRequirementLabel(acceptanceReadiness, requirement));
  const missingRequirementsEmptyState = inactiveAcceptancePath
    ? 'Acceptance path not active.'
    : (readyWindowClear ? 'Ready window clear.' : (observingCandidateNotStarted ? 'Observing candidate not started yet.' : 'No acceptance requirements currently missing.'));
  html += '<div style="margin-top:8px"><strong>Acceptance Missing Requirements</strong><div style="margin-top:6px">' +
    renderControlActionBadgeRow(displayMissingRequirements, missingRequirementsEmptyState, displayMissingRequirements.length ? 'warning' : 'neutral') +
  '</div></div>';
  html += '<div style="margin-top:8px"><strong>Blocking Reasons</strong><div style="margin-top:6px">' +
    renderControlActionBadgeRow(blockers, 'No blocking reasons recorded.', blockers.length ? 'warning' : 'neutral') +
  '</div></div>';
  html += '</div>';
  return html;
}

function renderGovernedHintSummary(summary) {
  const item = summary || null;
  if (!item || !Number(item.total_hints || 0)) {
    return '<div class="empty">No governed-hint summary available.</div>';
  }
  const renderCounts = (title, counts) => {
    const entries = Object.entries(counts || {}).sort((a, b) => String(a[0]).localeCompare(String(b[0])));
    return '<div style="margin-top:8px"><strong>' + esc(title) + '</strong><div style="margin-top:6px">' +
      renderControlActionBadgeRow(entries.map(entry => String(entry[0]) + ' ' + String(entry[1])), 'No counts recorded.', 'neutral') +
    '</div></div>';
  };
  let html = '<div class="msg-item" style="margin-bottom:6px">';
  html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><strong>Hints ' + esc(String(item.total_hints || 0)) + '</strong><span style="font-size:11px;color:var(--muted)">inspectability only</span></div>';
  html += renderCounts('Recommendation Classes', item.recommendation_class_count);
  html += renderCounts('Evidence Source Mix', item.evidence_source_mix_count);
  html += renderCounts('TTL Window State', item.ttl_window_state_count);
  html += renderCounts('Runtime Lineage Basis', item.runtime_lineage_basis_count);
  if (item.outcome_count && Object.keys(item.outcome_count).length) {
    html += renderCounts('Advisory Outcomes', item.outcome_count);
  }
  html += '</div>';
  return html;
}

function renderGovernedHintEvidence(hints) {
  const items = hints || [];
  if (!items.length) {
    return '<div class="empty">No governed hints matched this cluster.</div>';
  }
  return items.map(hint => {
    const recommendationClass = String(hint.recommendation_class || '').trim();
    const evidenceDiversity = Number(hint.evidence_diversity || 0);
    const evidenceDiversityBand = String(hint.evidence_diversity_band || '').trim();
    const evidenceSourceMix = String(hint.evidence_source_mix || '').trim();
    const runtimeEventRefCount = Number(hint.runtime_event_ref_count || 0);
    const evidenceSourceKinds = dedupeStrings(hint.evidence_source_kinds || []);
    const rootCauseGroups = dedupeStrings(hint.root_cause_groups || []);
    const runtimeLineageBasis = String(hint.runtime_lineage_basis || '').trim();
    const ttlWindowState = String(hint.ttl_window_state || '').trim();
    const meta = [
      hint.type || 'hint',
      hint.scope || '',
      'severity ' + Number(hint.severity || 0).toFixed(2),
      'uncertainty ' + Number(hint.uncertainty || 0).toFixed(2),
      'ttl ' + String(hint.ttl_epochs || 0),
      hint.actuation_class || ''
    ].filter(Boolean);
    const evidenceRefs = dedupeStrings(hint.evidence_refs || []);
    const actions = dedupeStrings(hint.recommended_actions || []).map(action => unifiedControlAuditSummaryKeyLabel(action));
    return '<div class="msg-item" style="margin-bottom:6px">' +
      '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>' + esc(hint.hint_id || 'rsp_hint') + '</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(meta.join(' | ')) + '</div></div><div style="font-size:11px;color:var(--muted)">' + esc(String(hint.entity_id || 'workspace')) + '</div></div>' +
      '<div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:8px">' +
        '<div><strong>Recommendation Class</strong><br>' + esc(recommendationClass || 'n/a') + '</div>' +
        '<div><strong>Evidence Diversity</strong><br>' + esc(evidenceDiversity ? evidenceDiversity.toFixed(2) : '0.00') + '</div>' +
      '</div>' +
      '<div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:8px">' +
        '<div><strong>Diversity Band</strong><br>' + esc(evidenceDiversityBand || 'UNKNOWN') + '</div>' +
        '<div><strong>Evidence Source Mix</strong><br>' + esc(evidenceSourceMix || 'UNKNOWN') + '</div>' +
      '</div>' +
      '<div style="margin-top:8px"><strong>Runtime Event Refs</strong><div class="msg-item" style="margin-top:6px">' + esc(String(runtimeEventRefCount || 0)) + '</div></div>' +
      '<div style="margin-top:8px"><strong>Evidence Source Kinds</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(evidenceSourceKinds, 'No evidence source kinds recorded.', 'neutral') + '</div></div>' +
      '<div style="margin-top:8px"><strong>Root-Cause Groups</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(rootCauseGroups, 'No root-cause groups resolved from current runtime-event refs.', 'neutral') + '</div></div>' +
      '<div style="margin-top:8px"><strong>Runtime Lineage Basis</strong><div class="msg-item" style="margin-top:6px">' + esc(runtimeLineageBasis || 'NONE') + '</div></div>' +
      '<div style="margin-top:8px"><strong>TTL Window State</strong><div class="msg-item" style="margin-top:6px">' + esc(ttlWindowState || 'UNSPECIFIED') + '</div></div>' +
      '<div style="margin-top:8px"><strong>Recommended Actions</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(actions, 'No recommended actions recorded.', 'neutral') + '</div></div>' +
      '<div style="margin-top:8px"><strong>Evidence Refs</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(evidenceRefs, 'No evidence refs recorded.', 'neutral') + '</div></div>' +
      (hint.summary ? '<div style="margin-top:8px"><strong>Hint Summary</strong><div class="msg-item" style="margin-top:6px">' + esc(hint.summary) + '</div></div>' : '') +
    '</div>';
  }).join('');
}

function renderGovernedHintOutcomes(outcomes) {
  const items = outcomes || [];
  if (!items.length) {
    return '<div class="empty">No governed-hint arbitration outcomes recorded.</div>';
  }
  return items.map(item => {
    const applied = dedupeStrings(item.applied_actions || []).map(action => unifiedControlAuditSummaryKeyLabel(action));
    const suppressedActions = dedupeStrings(item.suppressed_actions || []).map(action => unifiedControlAuditSummaryKeyLabel(action));
    const suppressionReasons = dedupeStrings(item.suppression_reasons || []).map(reason => unifiedControlAuditSummaryKeyLabel(reason));
    return '<div class="msg-item" style="margin-bottom:6px">' +
      '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>' + esc(item.hint_id || 'rsp_hint') + '</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([item.type || '', item.recommendation_class || '', item.arbitration_outcome || 'OBSERVED_ONLY'].filter(Boolean).join(' | ')) + '</div></div><div style="font-size:11px;color:var(--muted)">severity ' + esc(Number(item.severity || 0).toFixed(2)) + '</div></div>' +
      '<div style="margin-top:8px"><strong>Applied Actions</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(applied, 'No hint-backed actions routed.', 'positive') + '</div></div>' +
      '<div style="margin-top:8px"><strong>Suppressed Actions</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(suppressedActions, 'No suppressed actions recorded.', 'warning') + '</div></div>' +
      '<div style="margin-top:8px"><strong>Suppression Reasons</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(suppressionReasons, 'No suppression reasons recorded.', 'warning') + '</div></div>' +
      (item.summary ? '<div style="margin-top:8px"><strong>Outcome Summary</strong><div class="msg-item" style="margin-top:6px">' + esc(item.summary) + '</div></div>' : '') +
    '</div>';
  }).join('');
}

function controlPolicyMode(cluster) {
  return String((((cluster || {}).signals || {}).attention_band) || 'STEADY').trim().toLowerCase() || 'steady';
}

function controlPolicyClusterEvents(clusterOrID, maybeTensions, sessions, agents, tensionIDs) {
  let clusterID = '';
  let tasks = [];
  let sessionIDs = [];
  let agentIDs = [];
  let relatedTensionIDs = [];
  if (clusterOrID && typeof clusterOrID === 'object') {
    const cluster = clusterOrID || {};
    clusterID = String(cluster.proto_cluster_id || '').trim();
    tasks = cluster.task_ids || [];
    sessionIDs = cluster.session_ids || [];
    agentIDs = cluster.agent_ids || [];
    relatedTensionIDs = (maybeTensions || []).map(tension => String((tension || {}).tension_id || '').trim()).filter(Boolean);
  } else {
    clusterID = String(clusterOrID || '').trim();
    tasks = maybeTensions || [];
    sessionIDs = sessions || [];
    agentIDs = agents || [];
    relatedTensionIDs = tensionIDs || [];
  }
  const taskSet = new Set(tasks || []);
  const sessionSet = new Set(sessionIDs || []);
  const agentSet = new Set(agentIDs || []);
  const tensionSet = new Set(relatedTensionIDs || []);
  return (runtimeEventsCache || []).filter(item => {
    const payload = parseJSON(item.payload_json);
    if (clusterID && String(payload.proto_cluster_id || '').trim() === clusterID) return true;
    if (tensionSet.size) {
      if (tensionSet.has(String(payload.tension_id || '').trim())) return true;
      if (String(item.entity_type || '').toLowerCase() === 'tension' && tensionSet.has(String(item.entity_id || '').trim())) return true;
    }
    const taskID = String(item.task_id || payload.task_id || '').trim();
    if (taskID && taskSet.has(taskID)) return true;
    const sessionID = String(item.session_id || payload.session_id || '').trim();
    if (sessionID && sessionSet.has(sessionID)) return true;
    const agentID = String(item.agent_id || payload.agent_id || '').trim();
    if (agentID && agentSet.has(agentID)) return true;
    return false;
  }).sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at)).slice(0, 8);
}

function controlPolicyClusterMatchesType(cluster, typeFilter) {
  const normalized = String(typeFilter || '').trim().toLowerCase();
  if (!normalized) return true;
  const confirmed = (cluster && cluster.confirmed_counts_by_type) || {};
  const pending = (cluster && cluster.pending_counts_by_type) || {};
  return Number(confirmed[normalized] || 0) > 0 || Number(pending[normalized] || 0) > 0;
}

function findControlPolicyCluster(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return null;
  return ((controlPolicyReportCache && controlPolicyReportCache.clusters) || []).find(item => String(item.proto_cluster_id || '').trim() === clusterID) ||
    (controlPolicyClustersCache || []).find(item => String(item.proto_cluster_id || '').trim() === clusterID) ||
    (((controlPolicyDetailCache || {})[clusterID] || {}).cluster) ||
    null;
}

function filteredControlPolicyClusters() {
  const filters = controlPolicyFilterParams();
  let items = ((controlPolicyReportCache && controlPolicyReportCache.clusters) || []).slice();
  if (filters.tension_type) {
    items = items.filter(item => controlPolicyClusterMatchesType(item, filters.tension_type));
  }
  if (filters.attention_band) {
    items = items.filter(item => controlPolicyMode(item) === filters.attention_band);
  }
  if (filters.query) {
    items = items.filter(item => {
      const detail = (controlPolicyDetailCache || {})[String(item.proto_cluster_id || '').trim()] || {};
      const tensions = detail.tensions || [];
      const haystack = [
        item.proto_cluster_id,
        item.resolution_kind,
        (((item.signals || {}).attention_band) || ''),
        (((item.suggested_controls || {}).priority_focus) || ''),
        item.summary,
        (item.task_ids || []).join(' '),
        (item.session_ids || []).join(' '),
        (item.agent_ids || []).join(' '),
        (item.doc_keys || []).join(' '),
        (item.artifact_refs || []).join(' '),
        tensions.map(tension => [tension.title, tension.summary, tension.tension_type].join(' ')).join(' ')
      ].join(' ').toLowerCase();
      return haystack.includes(filters.query);
    });
  }
  items.sort((left, right) => {
    const rightScore = Number((((right || {}).signals || {}).pressure_score) || 0);
    const leftScore = Number((((left || {}).signals || {}).pressure_score) || 0);
    if (rightScore !== leftScore) return rightScore - leftScore;
    if (Number(right.confirmed_tension_count || 0) !== Number(left.confirmed_tension_count || 0)) {
      return Number(right.confirmed_tension_count || 0) - Number(left.confirmed_tension_count || 0);
    }
    return controlPolicyTimeValue(((right || {}).metrics || {}).last_event_at) - controlPolicyTimeValue(((left || {}).metrics || {}).last_event_at);
  });
  return {filters, items};
}

function renderControlPolicySummary(report, items) {
  const el = document.getElementById('control-policy-summary');
  const generated = document.getElementById('control-policy-generated-at');
  const count = document.getElementById('control-policy-list-count');
  if (!el || !generated || !count) return;
  if (!report) {
    generated.textContent = controlPolicySnapshotEventCache && controlPolicySnapshotEventCache.created_at ? ('live ' + timeAgo(controlPolicySnapshotEventCache.created_at)) : 'no data';
    count.textContent = '0';
    el.innerHTML = '<div class="empty">No advisory control report available yet.</div>';
    return;
  }
  const workspace = report.workspace || {};
  const visible = items || [];
  const capabilityFlags = rspCapabilityFlagsCache || null;
  const beliefReport = rspBeliefReportCache || null;
  const forecastReport = rspForecastReportCache || null;
  const telemetryDump = rspTelemetryDumpCache || null;
  const telemetrySummary = telemetryDump && telemetryDump.summary ? telemetryDump.summary : null;
  const topClusterID = workspace.highest_pressure_cluster_id || (visible[0] && visible[0].proto_cluster_id) || '';
  generated.textContent = report.generated_at ? timeAgo(report.generated_at, report.time_authority || null) : (controlPolicySnapshotEventCache && controlPolicySnapshotEventCache.created_at ? ('live ' + timeAgo(controlPolicySnapshotEventCache.created_at)) : 'cached');
  count.textContent = String(visible.length) + ' clusters';
  const cards = [
    {label:'Hot', value:String(workspace.hot_cluster_count || 0), detail:'Clusters currently in HOT advisory band'},
    {label:'Attention', value:String(workspace.attention_cluster_count || 0), detail:'Clusters above steady-state attention'},
    {label:'Confirmed', value:String(workspace.confirmed_tension_count || 0), detail:'Confirmed tensions in advisory scope'},
    {label:'Pending', value:String(workspace.pending_tension_count || 0), detail:'Pending tensions tracked separately from advisory banding'},
    {label:'Visible', value:String(visible.length), detail:String((report.clusters || []).length) + ' clusters returned by server report'},
    {label:'Pressure', value:String(workspace.highest_pressure_score || 0), detail:'Highest server-computed advisory pressure'}
  ];
  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(165px,1fr));gap:10px">';
  cards.forEach(card => {
    html += '<div class="msg-item">' +
      '<div style="font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted)">' + esc(card.label) + '</div>' +
      '<div style="font-size:22px;font-weight:700;margin-top:4px">' + esc(card.value) + '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:6px;line-height:1.4">' + esc(card.detail) + '</div>' +
    '</div>';
  });
  html += '</div>';
  if (capabilityFlags) {
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start"><div><strong>Capability Flags</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Rollout matrix for belief, state/anomaly/forecast shadowing, governed hints, and live consequence gates.</div></div><div style="font-size:11px;color:var(--muted)">workspace scope</div></div>';
    html += '<div style="margin-top:8px;display:flex;gap:6px;flex-wrap:wrap">' + renderControlCapabilityFlags(capabilityFlags) + '</div>';
    html += '</div>';
  }
  if (beliefReport) {
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start"><div><strong>Belief Calibration</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Shadow-only claim calibration summary for operator review; not a live actuation surface.</div></div><div style="font-size:11px;color:var(--muted)">' + esc(timeAgo(beliefReport.generated_at || '', beliefReport.time_authority || null)) + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:8px">';
    html += '<div><strong>Shadow Phase</strong><br>' + esc(String(beliefReport.shadow_phase || 'n/a')) + '</div>';
    html += '<div><strong>Stable</strong><br>' + esc(String(beliefReport.stable_count || 0)) + '</div>';
    html += '<div><strong>Needs Review</strong><br>' + esc(String(beliefReport.needs_review_count || 0)) + '</div>';
    html += '<div><strong>High Drift</strong><br>' + esc(String(beliefReport.high_drift_count || 0)) + '</div>';
    html += '</div>';
    html += '<div style="margin-top:10px"><strong>Heuristic Diagnostics</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Bounded belief-health rollups over the current report items; inspectability only, not posterior or evaluator authority.</div><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">';
    html += '<span class="tool-badge kind">Low Independence ' + esc(String(beliefReport.low_independence_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">High Contradiction ' + esc(String(beliefReport.high_contradiction_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">Verifier Stale ' + esc(String(beliefReport.verifier_stale_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">High Uncertainty ' + esc(String(beliefReport.high_uncertainty_count || 0)) + '</span>';
    html += '</div></div>';
    if ((beliefReport.items || []).length) {
      html += '<div style="margin-top:10px"><strong>Calibration Evidence</strong><div style="margin-top:6px">';
      html += beliefReport.items.slice(0, 3).map(item =>
        '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>' + esc(item.claim_id || item.subject || 'claim') + '</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([item.claim_type || '', item.suggested_state || '', item.drift_state || '', item.verifier_fresh ? 'verifier fresh' : 'verifier stale'].filter(Boolean).join(' | ')) + '</div></div><div style="font-size:11px;color:var(--muted)">' + esc(timeAgo(item.claim_updated_at || '', item.time_authority || beliefReport.time_authority || null)) + '</div></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:6px">posterior ' + esc(Number(item.posterior || 0).toFixed(2)) + ' | uncertainty ' + esc(Number(item.uncertainty || 0).toFixed(2)) + ' | drift ' + esc(Number(item.drift_score || 0).toFixed(2)) + '</div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">source diversity ' + esc(Number(item.source_diversity || 0).toFixed(2)) + ' | independence ' + esc(Number(item.independence_discount || 0).toFixed(2)) + ' | correlated ' + esc(String(item.correlated_evidence_count || 0)) + '</div>' +
        '</div>'
      ).join('');
      html += '</div></div>';
    }
    html += '</div>';
  }
  if (forecastReport) {
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start"><div><strong>Forecast Outlook</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Shadow-only heuristic forecast summary for operator review; readiness and provenance are inspectable but not calibrated quality.</div></div><div style="font-size:11px;color:var(--muted)">' + esc(timeAgo(forecastReport.generated_at || '', forecastReport.time_authority || null)) + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-top:8px">';
    html += '<div><strong>Shadow Phase</strong><br>' + esc(String(forecastReport.shadow_phase || 'n/a')) + '</div>';
    html += '<div><strong>Readiness</strong><br>' + esc(String(forecastReport.forecast_readiness || 'n/a')) + '</div>';
    html += '<div><strong>Band</strong><br>' + esc(String(forecastReport.forecast_band || 'n/a')) + '</div>';
    html += '<div><strong>Supported Vars</strong><br>' + esc(String(forecastReport.supported_variables || 0)) + '</div>';
    html += '<div><strong>Alert Vars</strong><br>' + esc(String((forecastReport.alert_variables || []).length)) + '</div>';
    html += '</div>';
    if ((forecastReport.forecast_provenance_hints || []).length) {
      html += '<div style="margin-top:10px"><strong>Forecast Provenance</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + (forecastReport.forecast_provenance_hints || []).slice(0, 6).map(item => '<span class="tool-badge kind">' + esc(item) + '</span>').join('') + '</div></div>';
    }
    if (forecastReport.forecast_coverage_summary) {
      const coverage = forecastReport.forecast_coverage_summary || {};
      const renderForecastCountBadges = counts => Object.entries(counts || {}).sort((a, b) => String(a[0]).localeCompare(String(b[0]))).map(entry => '<span class="tool-badge kind">' + esc(String(entry[0]) + ' ' + String(entry[1])) + '</span>').join('');
      html += '<div style="margin-top:10px"><strong>Forecast Coverage</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Additive rollup over current projections, readiness, and provenance fields; inspectability only, not calibrated quality or evaluator authority.</div>';
      html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:8px">';
      html += '<div><strong>Projections</strong><br>' + esc(String(coverage.projection_count || 0)) + '</div>';
      html += '<div><strong>Alert-Proj</strong><br>' + esc(String(coverage.alert_projection_count || 0)) + '</div>';
      html += '<div><strong>History-Backed</strong><br>' + esc(String(coverage.history_backed_projection_count || 0)) + '</div>';
      html += '<div><strong>Evidence-Backed</strong><br>' + esc(String(coverage.evidence_backed_projection_count || 0)) + '</div>';
      html += '<div><strong>Missing Inputs</strong><br>' + esc(String(coverage.missing_input_count || 0)) + '</div>';
      html += '<div><strong>Evidence Refs</strong><br>' + esc(String(coverage.evidence_ref_count || 0)) + '</div>';
      html += '</div>';
      if (Object.keys(coverage.basis_count || {}).length) {
        html += '<div style="margin-top:8px"><strong>Projection Bases</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + renderForecastCountBadges(coverage.basis_count || {}) + '</div></div>';
      }
      if (Object.keys(coverage.model_count || {}).length) {
        html += '<div style="margin-top:8px"><strong>Projection Models</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + renderForecastCountBadges(coverage.model_count || {}) + '</div></div>';
      }
      html += '</div>';
    }
    if ((forecastReport.projections || []).length) {
      html += '<div style="margin-top:10px"><strong>Projection Highlights</strong><div style="margin-top:6px">';
      html += (forecastReport.projections || []).slice(0, 3).map(item => {
        const maxProbability = Math.max.apply(null, (item.forecasts || []).map(f => Number(f.probability_exceed_threshold || 0)).concat([0]));
        return '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>' + esc(item.variable || 'metric') + '</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([item.model || '', item.basis || '', item.summary || ''].filter(Boolean).join(' | ')) + '</div></div><div style="font-size:11px;color:var(--muted)">p(max) ' + esc(Number(maxProbability || 0).toFixed(2)) + '</div></div>' +
        '</div>';
      }).join('');
      html += '</div></div>';
    }
    html += '</div>';
  }
  if (telemetrySummary && telemetrySummary.readiness_coverage_rollup) {
    const telemetryRollup = telemetrySummary.readiness_coverage_rollup || {};
    const baselineScopeCounts = telemetrySummary.anomaly_baseline_scope_counts || {};
    const shrunkScopeCounts = telemetrySummary.shrunk_anomaly_scope_counts || {};
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start"><div><strong>Telemetry Coverage</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Bounded report-level rollup over existing belief/anomaly/state telemetry readiness and coverage; inspectability only, not calibration quality or evaluator authority.</div></div><div style="font-size:11px;color:var(--muted)">' + esc(timeAgo(rspTelemetryLatestAt(telemetrySummary), report.time_authority || null)) + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-top:8px">';
    html += '<div><strong>Overall</strong><br>' + esc(String(telemetryRollup.overall_readiness_band || telemetrySummary.readiness_band || 'n/a')) + '</div>';
    html += '<div><strong>Integrity</strong><br>' + esc(String(telemetrySummary.calibration_integrity_band || 'n/a')) + '</div>';
    html += '<div><strong>Observable</strong><br>' + esc(String(telemetryRollup.observable_stream_count || 0)) + '</div>';
    html += '<div><strong>Warming</strong><br>' + esc(String(telemetryRollup.warming_stream_count || 0)) + '</div>';
    html += '<div><strong>Insufficient</strong><br>' + esc(String(telemetryRollup.insufficient_stream_count || 0)) + '</div>';
    html += '</div>';
    html += '<div style="margin-top:10px"><strong>Coverage Gaps</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Current read-side coverage gaps for the surfaced telemetry streams; not an evaluator verdict.</div><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + renderTelemetryCoverageBadges(telemetrySummary.coverage_gaps || []) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Calibration Integrity</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Versioning and legacy-row honesty for current telemetry streams; use this to distinguish coverage readiness from calibration comparability.</div><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + renderTelemetryCoverageBadges(telemetrySummary.calibration_gaps || []) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Telemetry Streams</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">';
    html += '<span class="tool-badge kind">belief ' + esc(String(telemetrySummary.belief_readiness_band || 'n/a').toLowerCase()) + '</span>';
    html += '<span class="tool-badge kind">anomaly ' + esc(String(telemetrySummary.anomaly_readiness_band || 'n/a').toLowerCase()) + '</span>';
    html += '<span class="tool-badge kind">state ' + esc(String(telemetrySummary.state_readiness_band || 'n/a').toLowerCase()) + '</span>';
    html += '</div></div>';
html += '<div style="margin-top:10px"><strong>Telemetry Provenance</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Existing warm-baseline, shrinkage-provenance, and warming-driver context from the telemetry dump; inspectability only, not evaluator authority.</div><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">';
    html += '<span class="tool-badge kind">warm baseline coverage ' + esc(String(telemetrySummary.warm_anomaly_baseline_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">alerts with baseline ' + esc(String(telemetrySummary.anomaly_logs_with_baseline_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">shrunk alerts ' + esc(String(telemetrySummary.shrunk_anomaly_alert_count || 0)) + '</span>';
    html += '<span class="tool-badge kind">shrinkage reliance ' + esc(String(telemetrySummary.shrinkage_reliance_band || 'NONE').toLowerCase()) + '</span>';
    html += '<span class="tool-badge kind">fallback quality ' + esc(String(telemetrySummary.shrinkage_fallback_quality_band || 'NONE').toLowerCase()) + '</span>';
    html += '<span class="tool-badge kind">workspace fallback mix ' + esc(String(telemetrySummary.workspace_fallback_mix_band || 'NONE').toLowerCase()) + '</span>';
    html += '<span class="tool-badge kind">workspace tier pressure ' + esc(String(telemetrySummary.workspace_tier_pressure_band || 'NONE').toLowerCase()) + '</span>';
    html += '<span class="tool-badge kind">workspace-tier alerts ' + esc(String(((telemetrySummary.workspace_tier_pressure_counts || {}).workspace_tier) || 0)) + '</span>';
    html += '<span class="tool-badge kind">agent-tier alerts ' + esc(String(((telemetrySummary.workspace_tier_pressure_counts || {}).agent_tier) || 0)) + '</span>';
    html += '<span class="tool-badge kind">workspace exact ' + esc(String(((telemetrySummary.workspace_fallback_mix_counts || {}).exact_workspace) || 0)) + '</span>';
    html += '<span class="tool-badge kind">workspace agent-default ' + esc(String(((telemetrySummary.workspace_fallback_mix_counts || {}).agent_default_workspace) || 0)) + '</span>';
    html += '<span class="tool-badge kind">warming driver ' + esc(String(telemetrySummary.anomaly_warming_driver || 'NONE').toLowerCase()) + '</span>';
    html += '<span class="tool-badge kind">fallback scope tier ' + esc(String(telemetrySummary.shrinkage_fallback_scope_tier || 'NONE').toLowerCase()) + '</span>';
    html += '<span class="tool-badge kind">exact ' + esc(String(baselineScopeCounts.EXACT || 0)) + '</span>';
    html += '<span class="tool-badge kind">agent default ' + esc(String(baselineScopeCounts.AGENT_DEFAULT || 0)) + '</span>';
    html += '<span class="tool-badge kind">workspace default ' + esc(String(baselineScopeCounts.WORKSPACE_DEFAULT || 0)) + '</span>';
    html += '<span class="tool-badge kind">exact shrunk agent default ' + esc(String(shrunkScopeCounts.EXACT_SHRUNK_AGENT_DEFAULT || 0)) + '</span>';
    html += '<span class="tool-badge kind">exact shrunk workspace default ' + esc(String(shrunkScopeCounts.EXACT_SHRUNK_WORKSPACE_DEFAULT || 0)) + '</span>';
    html += '<span class="tool-badge kind">agent default shrunk workspace default ' + esc(String(shrunkScopeCounts.AGENT_DEFAULT_SHRUNK_WORKSPACE_DEFAULT || 0)) + '</span>';
    html += '</div></div>';
    html += '<div style="margin-top:10px"><strong>Recent Telemetry Markers</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Most recent observed timestamps for the current telemetry streams; inspectability only.</div><div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:6px">';
    html += '<div><strong>Belief</strong><br>' + esc(telemetrySummary.latest_belief_at ? timeAgo(telemetrySummary.latest_belief_at, report.time_authority || null) : 'n/a') + '</div>';
    html += '<div><strong>Anomaly</strong><br>' + esc(telemetrySummary.latest_anomaly_at ? timeAgo(telemetrySummary.latest_anomaly_at, report.time_authority || null) : 'n/a') + '</div>';
    html += '<div><strong>State</strong><br>' + esc(telemetrySummary.latest_state_at ? timeAgo(telemetrySummary.latest_state_at, report.time_authority || null) : 'n/a') + '</div>';
    html += '</div></div>';
    html += '</div>';
  }
  html += '<div style="display:flex;justify-content:space-between;gap:12px;margin-top:12px;font-size:12px;color:var(--muted)">';
  html += '<span>Server advisory read-side over proto-cluster metrics + confirmed tensions</span>';
  if (topClusterID) {
    html += '<span>Top cluster: <a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showControlPolicyClusterDetail((topClusterID))}) + ' style="color:var(--accent2)">' + esc(topClusterID) + '</a></span>';
  } else if (unifiedControlSnapshotEventCache && unifiedControlSnapshotEventCache.event_id) {
    html += '<span><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showRuntimeEventDetail((unifiedControlSnapshotEventCache.event_id))}) + ' style="color:var(--accent2)">Open latest unified snapshot</a></span>';
  } else if (controlPolicySnapshotEventCache && controlPolicySnapshotEventCache.event_id) {
    html += '<span><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showRuntimeEventDetail((controlPolicySnapshotEventCache.event_id))}) + ' style="color:var(--accent2)">Open latest durable snapshot</a></span>';
  } else {
    html += '<span>No control cluster is currently selected.</span>';
  }
  html += '</div>';
  el.innerHTML = html;
}

function renderControlPolicyClusters(items) {
  const el = document.getElementById('control-policy-cluster-list');
  const selected = document.getElementById('control-policy-selected');
  if (!el || !selected) return;
  selected.textContent = controlPolicySelectedClusterID ? ('selected ' + controlPolicySelectedClusterID) : 'none';
  if (!items || !items.length) {
    el.innerHTML = '<div class="empty">No advisory clusters matched the current control filter.</div>';
    return;
  }
  el.innerHTML = items.map(item => {
    const metrics = item.metrics || {};
    const signals = item.signals || {};
    const controls = item.suggested_controls || {};
    const band = controlPolicyMode(item);
    const focus = String(controls.priority_focus || 'throughput');
    const freshnessBadges = [];
    if (item.metrics_missing) freshnessBadges.push('<span class="tool-badge kind" style="background:rgba(214,162,60,.14);color:var(--yellow)">metrics missing</span>');
    if (item.basis_stale) freshnessBadges.push('<span class="tool-badge kind" style="background:#f9731622;color:#f97316">stale basis</span>');
    return '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showControlPolicyClusterDetail((item.proto_cluster_id))}) + '>' +
      '<div class="action-title">' + esc(item.proto_cluster_id || 'control-cluster') + '</div>' +
      '<div class="action-meta">' +
        '<span style="color:' + controlPolicyModeColor(band) + ';font-weight:600">' + esc(band) + '</span>' +
        '<span>' + esc(String(item.confirmed_tension_count || 0) + ' confirmed') + '</span>' +
        '<span>' + esc(String(item.pending_tension_count || 0) + ' pending') + '</span>' +
        '<span>' + esc(String(signals.pressure_score || 0) + ' score') + '</span>' +
      '</div>' +
      '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px">' +
        '<span class="tool-badge kind">events ' + esc(String(metrics.event_count || 0)) + '</span>' +
        '<span class="tool-badge kind">queues ' + esc(String(metrics.open_queue_count || 0)) + '</span>' +
        '<span class="tool-badge kind">blockers ' + esc(String(metrics.blocker_signal_count || 0)) + '</span>' +
        '<span class="tool-badge active">' + esc(focus) + '</span>' +
        '<span class="tool-badge kind">fanout hint ' + esc(String(controls.fanout_cap || 0)) + '</span>' +
        freshnessBadges.join('') +
      '</div>' +
      '<div style="font-size:11px;color:var(--muted);margin-top:8px;line-height:1.4">' + esc(item.summary || (item.proto_cluster_id || 'Advisory control cluster')) + '</div>' +
    '</div>';
  }).join('');
}

function renderControlPolicySnapshotState() {
  const badge = document.getElementById('control-policy-snapshot-state');
  const el = document.getElementById('control-policy-snapshot-summary');
  const snapshot = controlPolicySnapshotEventCache;
  if (!badge || !el) return;
  if (!snapshot) {
    badge.textContent = 'none';
    el.innerHTML = '<div class="empty">Record a control advisory snapshot to persist the current control report into the runtime journal.</div>';
    return;
  }
  const payload = parseJSON(snapshot.payload_json);
  const workspace = payload.workspace || {};
  const clusters = payload.clusters || [];
  badge.textContent = snapshot.created_at ? timeAgo(snapshot.created_at) : 'recorded';
  let html = '<div class="msg-item">';
  html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">';
  html += '<div><strong>Generated</strong><br>' + esc(timeAgo(payload.generated_at || snapshot.created_at || '')) + '</div>';
  html += '<div><strong>Hot</strong><br>' + esc(String(workspace.hot_cluster_count || 0)) + '</div>';
  html += '<div><strong>Attention</strong><br>' + esc(String(workspace.attention_cluster_count || 0)) + '</div>';
  html += '<div><strong>Confirmed</strong><br>' + esc(String(workspace.confirmed_tension_count || 0)) + '</div>';
  html += '</div>';
  if (clusters.length) {
    html += '<div style="margin-top:10px;font-size:11px;color:var(--muted)">Captured clusters</div>';
    html += '<div style="margin-top:6px">';
    html += clusters.slice(0, 4).map(cluster =>
      '<div class="msg-item" style="margin-bottom:6px">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(cluster.proto_cluster_id || 'cluster') + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((cluster.proto_cluster_id))}) + '>Control</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((cluster.proto_cluster_id))}) + '>Tensions</button></div></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([((cluster.signals || {}).attention_band || ''), (((cluster.suggested_controls || {}).priority_focus) || ''), cluster.summary || ''].filter(Boolean).join(' | ')) + '</div>' +
      '</div>'
    ).join('');
    html += '</div>';
  }
  if (snapshot.event_id) {
    html += '<div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap"><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){switchTab('control');setTimeout(function(){ showRuntimeEventDetail((snapshot.event_id)); }, 100)}) + '>Open Runtime Event</button></div>';
  }
  html += '</div>';
  el.innerHTML = html;
}

function renderUnifiedControlSnapshotState() {
  const badge = document.getElementById('unified-control-snapshot-state');
  const el = document.getElementById('unified-control-snapshot-summary');
  const snapshot = unifiedControlSnapshotEventCache;
  if (!badge || !el) return;
  if (!snapshot) {
    badge.textContent = 'none';
    el.innerHTML = '<div class="empty">Record a unified advisory snapshot to persist the current unified-control report into the runtime journal.</div>';
    return;
  }
  const payload = parseJSON(snapshot.payload_json);
  const report = payload.report || {};
  const filter = payload.filter || {};
  const summary = payload.summary || report.summary || '';
  const timeAuthority = report.time_authority || null;
  const governedHintCount = Number(payload.governed_hint_count || (Array.isArray(report.governed_hints) ? report.governed_hints.length : 0) || 0);
  const appliedActionCount = Number(payload.applied_action_count || (Array.isArray(report.applied_actions) ? report.applied_actions.length : 0) || 0);
  const suppressedHintCount = Number(payload.suppressed_hint_count || (Array.isArray(report.suppressed_hints) ? report.suppressed_hints.length : 0) || 0);
  const outcomeCount = Number(payload.governed_hint_outcome_count || (Array.isArray(report.governed_hint_outcomes) ? report.governed_hint_outcomes.length : 0) || 0);
  const basisFieldCount = Number(payload.effective_control_basis_field_count || ((report.effective_control_basis_summary || {}).field_count) || 0);
  const basisChangedCount = Number(payload.effective_control_basis_changed_count || ((report.effective_control_basis_summary || {}).changed_field_count) || 0);
  const contradictionCount = Number(payload.contradiction_count || ((report.contradiction_summary || {}).total_count) || 0);
  const scopeLabel = report.proto_cluster_id ? ('cluster ' + report.proto_cluster_id) : (payload.workspace_id || report.workspace_id || WS_ID);
  badge.textContent = snapshot.created_at ? timeAgo(snapshot.created_at, timeAuthority) : 'recorded';
  let html = '<div class="msg-item">';
  html += '<div style="font-size:11px;color:var(--muted);margin-bottom:8px">Current advisory unified-control snapshot mirrored from the runtime journal; inspectability only, not a second arbiter, not execution history, and not rollback authority.</div>';
  html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">';
  html += '<div><strong>Generated</strong><br>' + esc(timeAgo(report.generated_at || snapshot.created_at || '', timeAuthority)) + '</div>';
  html += '<div><strong>Scope</strong><br>' + esc(scopeLabel || 'workspace') + '</div>';
  html += '<div><strong>Mode</strong><br>' + esc(unifiedControlModeLabel(report.control_mode || report.candidate_mode || 'n/a')) + '</div>';
  html += '<div><strong>Risk</strong><br>' + esc(String(report.rsp_risk_band || 'n/a')) + '</div>';
  html += '<div><strong>Hints</strong><br>' + esc(String(governedHintCount)) + '</div>';
  html += '<div><strong>Applied</strong><br>' + esc(String(appliedActionCount)) + '</div>';
  html += '<div><strong>Suppressed</strong><br>' + esc(String(suppressedHintCount)) + '</div>';
  html += '<div><strong>Outcomes</strong><br>' + esc(String(outcomeCount)) + '</div>';
  html += '<div><strong>Basis Fields</strong><br>' + esc(String(basisFieldCount)) + '</div>';
  html += '<div><strong>Changed Fields</strong><br>' + esc(String(basisChangedCount)) + '</div>';
  html += '<div><strong>Contradictions</strong><br>' + esc(String(contradictionCount)) + '</div>';
  html += '</div>';
  if (summary) {
    html += '<div style="margin-top:10px"><strong>Snapshot Summary</strong><div class="msg-item" style="margin-top:6px">' + esc(summary) + '</div></div>';
  }
  html += '<div style="margin-top:10px"><strong>Cooldown Basis</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Current mode/candidate-mode cooldown context captured in this advisory snapshot; inspectability only.</div><div style="margin-top:6px">' + renderUnifiedControlCooldownBasis(report.cooldown_basis || null) + '</div></div>';
  html += '<div style="margin-top:10px"><strong>Effective Control Basis Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Compact rollup over the current per-control basis entries captured in this advisory snapshot; inspectability only.</div><div style="margin-top:6px">' + renderUnifiedControlBasisSummary(report.effective_control_basis_summary || null) + '</div></div>';
  html += '<div style="margin-top:10px"><strong>Contradiction Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Compact rollup over the current contradiction markers captured in this advisory snapshot; inspectability only.</div><div style="margin-top:6px">' + renderUnifiedControlContradictionSummary(report.contradiction_summary || null) + '</div></div>';
  const clusterID = String(report.proto_cluster_id || filter.proto_cluster_id || '').trim();
  if (clusterID) {
    html += '<div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">';
    html += '<button class="participant-btn" ' + dashboardAction(function(dashboardEvent){showControlPolicyClusterDetail((clusterID))}) + '>Open Unified Cluster</button>';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((clusterID))}) + '>Open Control Scaffold</button>';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((clusterID))}) + '>Open Tensions</button>';
    html += '</div>';
  }
  if (snapshot.event_id) {
    html += '<div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap"><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){switchTab('control');setTimeout(function(){ showRuntimeEventDetail((snapshot.event_id)); }, 100)}) + '>Open Runtime Event</button></div>';
  }
  html += '</div>';
  el.innerHTML = html;
}

function syncControlPolicySnapshotFromRuntimeEvents() {
  controlPolicySnapshotEventCache = (runtimeEventsCache || [])
    .filter(item => String(item.event_type || '').toLowerCase() === 'cluster.control_advisory_snapshot')
    .sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at))[0] || null;
  renderControlPolicySnapshotState();
}

function syncUnifiedControlSnapshotFromRuntimeEvents() {
  unifiedControlSnapshotEventCache = (runtimeEventsCache || [])
    .filter(item => {
      const eventType = String(item.event_type || '').toLowerCase();
      return eventType === 'cluster.unified_control_advisory_snapshot' || eventType === 'cluster.unified_control_effective_snapshot';
    })
    .sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at))[0] || null;
  renderUnifiedControlSnapshotState();
}

function corridorReadinessParams(extra = {}) {
  const params = {
    workspace_id: WS_ID,
    limit: 40
  };
  Object.keys(extra || {}).forEach(key => {
    if (extra[key] !== undefined && extra[key] !== null && extra[key] !== '') params[key] = extra[key];
  });
  return params;
}

function corridorReadinessColor(status) {
  const normalized = String(status || '').toUpperCase();
  if (normalized === 'READY') return 'var(--green)';
  if (normalized === 'BORDERLINE') return 'var(--yellow)';
  if (normalized === 'MIXED') return 'var(--orange)';
  if (normalized === 'STALE_BASIS') return 'var(--accent2)';
  return 'var(--muted)';
}

function corridorTaskClassColor(hint) {
  const normalized = String(hint || '').toUpperCase();
  if (normalized === 'PROOF') return 'var(--accent2)';
  if (normalized === 'EXPLORATION') return 'var(--yellow)';
  if (normalized === 'INTEGRATION') return 'var(--accent)';
  if (normalized === 'INCIDENT') return 'var(--red)';
  return 'var(--muted)';
}

function corridorFirstNonEmpty(...values) {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    const normalized = String(value).trim();
    if (normalized) return normalized;
  }
  return '';
}

function corridorTaskClassValue(record = {}) {
  return corridorFirstNonEmpty(record.task_class, record.task_class_hint) || 'UNKNOWN';
}

function corridorTaskClassSource(record = {}) {
  const explicitSource = corridorFirstNonEmpty(record.task_class_source, record.class_source);
  if (explicitSource) return explicitSource;
  if (corridorFirstNonEmpty(record.task_class)) return 'explicit class';
  if (corridorFirstNonEmpty(record.task_class_hint)) return 'heuristic hint';
  return 'not surfaced';
}

function corridorWorkspaceTaskClassCounts(workspace = {}) {
  return workspace.task_class_counts || workspace.task_class_hint_counts || {};
}

function corridorCatalogApproximation(cluster = {}, detail = null) {
  const detailCluster = (detail && detail.cluster) || {};
  const detailLookup = (detailCluster.corridor_lookup && typeof detailCluster.corridor_lookup === 'object') ? detailCluster.corridor_lookup : null;
  const clusterLookup = (cluster.corridor_lookup && typeof cluster.corridor_lookup === 'object') ? cluster.corridor_lookup : null;
  return corridorFirstNonEmpty(
    detailLookup && detailLookup.display_name,
    detailLookup && detailLookup.catalog_key,
    detailCluster.corridor_catalog,
    detailCluster.corridor_catalog_name,
    detailCluster.corridor_catalog_key,
    detailCluster.corridor_catalog_hint,
    clusterLookup && clusterLookup.display_name,
    clusterLookup && clusterLookup.catalog_key,
    cluster.corridor_catalog,
    cluster.corridor_catalog_name,
    cluster.corridor_catalog_key,
    cluster.corridor_catalog_hint
  ) || 'not surfaced';
}

function corridorLookupApproximation(cluster = {}, detail = null) {
  const detailCluster = (detail && detail.cluster) || {};
  const detailLookup = (detailCluster.corridor_lookup && typeof detailCluster.corridor_lookup === 'object') ? detailCluster.corridor_lookup : null;
  const clusterLookup = (cluster.corridor_lookup && typeof cluster.corridor_lookup === 'object') ? cluster.corridor_lookup : null;
  return corridorFirstNonEmpty(
    detailLookup && detailLookup.lookup_status,
    detailLookup && detailLookup.summary,
    detailCluster.corridor_lookup,
    detailCluster.corridor_lookup_status,
    detailCluster.lookup_status,
    detailCluster.lookup_result,
    detailCluster.corridor_hint,
    clusterLookup && clusterLookup.lookup_status,
    clusterLookup && clusterLookup.summary,
    cluster.corridor_lookup,
    cluster.corridor_lookup_status,
    cluster.lookup_status,
    cluster.lookup_result,
    cluster.corridor_hint
  ) || 'not surfaced';
}

function corridorBasisFreshnessApproximation(cluster = {}) {
  const basisAt = corridorFirstNonEmpty(cluster.last_basis_event_at, cluster.last_basis_at);
  if (cluster.basis_stale) {
    return basisAt ? ('stale lookup basis from ' + timeAgo(basisAt)) : 'stale lookup basis';
  }
  if (basisAt) return 'fresh lookup basis ' + timeAgo(basisAt);
  return 'no task-metadata lookup basis';
}

function corridorAuthorityParams(extra = {}) {
  const params = {
    workspace_id: WS_ID,
    limit: 40
  };
  Object.keys(extra || {}).forEach(key => {
    if (extra[key] !== undefined && extra[key] !== null && extra[key] !== '') params[key] = extra[key];
  });
  return params;
}

function corridorAuthorityEffectiveRecord(cluster = {}, detail = null) {
  if (detail && detail.task && typeof detail.task === 'object') return detail.task;
  if (detail && typeof detail === 'object' && detail.basis_state) return detail;
  if (cluster && typeof cluster === 'object') return cluster;
  return {};
}

function corridorAuthorityApproximation(cluster = {}, detail = null) {
  const record = corridorAuthorityEffectiveRecord(cluster, detail);
  if (record && record.basis_state) return String(record.basis_state || '');
  const detailCluster = (detail && detail.cluster) || {};
  const detailLookup = (detailCluster.corridor_lookup && typeof detailCluster.corridor_lookup === 'object') ? detailCluster.corridor_lookup : null;
  const clusterLookup = (cluster.corridor_lookup && typeof cluster.corridor_lookup === 'object') ? cluster.corridor_lookup : null;
  return corridorFirstNonEmpty(
    detailCluster.task_first_corridor_authority,
    detailCluster.task_first_authority,
    detailCluster.corridor_authority,
    detailCluster.authority,
    detailCluster.task_class_authority,
    detailLookup && detailLookup.task_first_authority,
    detailLookup && detailLookup.authority,
    detailLookup && detailLookup.authority_state,
    cluster.task_first_corridor_authority,
    cluster.task_first_authority,
    cluster.corridor_authority,
    cluster.authority,
    cluster.task_class_authority,
    clusterLookup && clusterLookup.task_first_authority,
    clusterLookup && clusterLookup.authority,
    clusterLookup && clusterLookup.authority_state
  ) || 'not surfaced';
}

function corridorAuthorityBasisFreshnessApproximation(cluster = {}, detail = null) {
  const record = corridorAuthorityEffectiveRecord(cluster, detail);
  if (record && record.basis_state) {
    const basisAt = corridorFirstNonEmpty(record.basis_updated_at, record.task_class_updated_at);
    if (String(record.basis_state || '').toUpperCase() === 'AUTHORED_STALE') {
      return basisAt ? ('stale authority basis from ' + timeAgo(basisAt)) : 'stale authority basis';
    }
    if (String(record.basis_state || '').toUpperCase() === 'AUTHORED_FRESH') {
      return basisAt ? ('fresh authority basis ' + timeAgo(basisAt)) : 'fresh authority basis';
    }
    if (String(record.basis_state || '').toUpperCase() === 'DERIVED_ONLY') {
      return basisAt ? ('derived-only basis ' + timeAgo(basisAt)) : 'derived-only basis';
    }
    return 'no authoritative basis';
  }
  const detailCluster = (detail && detail.cluster) || {};
  const detailLookup = (detailCluster.corridor_lookup && typeof detailCluster.corridor_lookup === 'object') ? detailCluster.corridor_lookup : null;
  const clusterLookup = (cluster.corridor_lookup && typeof cluster.corridor_lookup === 'object') ? cluster.corridor_lookup : null;
  const explicit = corridorFirstNonEmpty(
    detailCluster.task_first_authority_basis_freshness,
    detailCluster.authority_basis_freshness,
    detailCluster.task_class_authority_freshness,
    detailLookup && detailLookup.authority_basis_freshness,
    detailLookup && detailLookup.basis_freshness,
    cluster.task_first_authority_basis_freshness,
    cluster.authority_basis_freshness,
    cluster.task_class_authority_freshness,
    clusterLookup && clusterLookup.authority_basis_freshness,
    clusterLookup && clusterLookup.basis_freshness
  );
  if (explicit) return explicit;
  const basisAt = corridorFirstNonEmpty(
    detailCluster.last_task_first_authority_basis_at,
    detailCluster.last_authority_basis_at,
    detailCluster.authority_basis_updated_at,
    detailLookup && detailLookup.authority_basis_updated_at,
    detailLookup && detailLookup.basis_updated_at,
    cluster.last_task_first_authority_basis_at,
    cluster.last_authority_basis_at,
    cluster.authority_basis_updated_at,
    clusterLookup && clusterLookup.authority_basis_updated_at,
    clusterLookup && clusterLookup.basis_updated_at
  );
  const stale = Boolean(
    detailCluster.task_first_authority_basis_stale ||
    detailCluster.authority_basis_stale ||
    detailCluster.task_class_authority_stale ||
    (detailLookup && (detailLookup.authority_basis_stale || detailLookup.basis_stale)) ||
    cluster.task_first_authority_basis_stale ||
    cluster.authority_basis_stale ||
    cluster.task_class_authority_stale ||
    (clusterLookup && (clusterLookup.authority_basis_stale || clusterLookup.basis_stale))
  );
  if (stale) return basisAt ? ('stale authority basis from ' + timeAgo(basisAt)) : 'stale authority basis';
  if (basisAt) return 'fresh authority basis ' + timeAgo(basisAt);
  return corridorBasisFreshnessApproximation(detailCluster.proto_cluster_id ? detailCluster : cluster);
}

function renderCorridorAuthorityState() {
  const badge = document.getElementById('corridor-authority-state');
  const el = document.getElementById('corridor-authority-summary');
  if (!badge || !el) return;
  const report = corridorAuthorityReportCache;
  if (!report) {
    badge.textContent = 'no data';
    el.innerHTML = '<div class="empty">Task-first corridor authority remains a read-only precedence surface over explicit task_class evidence and derived lookup context.</div>';
    return;
  }
  const workspace = report.workspace || {};
  const tasks = Array.isArray(report.tasks) ? report.tasks : [];
  const selectedCluster = findCorridorReadinessCluster(controlPolicySelectedClusterID) || findControlStateCluster(controlPolicySelectedClusterID) || findControlPolicyCluster(controlPolicySelectedClusterID) || null;
  const scopedTaskIDs = new Set(((selectedCluster && selectedCluster.task_ids) || []).map(item => String(item || '').trim()).filter(Boolean));
  const scopedTasks = scopedTaskIDs.size ? tasks.filter(item => scopedTaskIDs.has(String(item.task_id || '').trim())) : [];
  const visibleTasks = scopedTasks.length ? scopedTasks : tasks;
  badge.textContent = report.generated_at ? timeAgo(report.generated_at) : 'read-only';
  const cards = [
    ['Authored Fresh', String(workspace.authored_fresh_count || 0), 'Explicit task_class evidence that is still fresh'],
    ['Authored Stale', String(workspace.authored_stale_count || 0), 'Explicit task_class evidence that remains visible but stale'],
    ['Derived Only', String(workspace.derived_only_count || 0), 'Tasks that currently depend on derived corridor lookup only'],
    ['No Basis', String(workspace.no_basis_count || 0), 'Tasks without enough class evidence yet'],
    ['Inactive Authored', String(workspace.inactive_authored_task_count || 0), 'Explicit authored basis preserved even without live proto-cluster visibility'],
    ['Visible', String(workspace.visible_task_count || 0), 'Tasks currently surfaced through active instrumentation']
  ];
  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">';
  cards.forEach(card => {
    html += '<div class="msg-item" style="margin:0"><strong>' + esc(card[0]) + '</strong><div style="margin-top:4px;font-size:20px;font-weight:700">' + esc(card[1]) + '</div><div style="margin-top:6px;font-size:11px;color:var(--muted);line-height:1.4">' + esc(card[2]) + '</div></div>';
  });
  html += '</div>';
  html += '<div style="margin-top:12px;font-size:12px;color:var(--muted)">Read-only task-first precedence surface. Explicit task_class evidence can outrank instrumentation visibility here, but this still does not assign a corridor or carry policy authority.</div>';
  const renderedTasks = visibleTasks.slice(0, 6);
  if (renderedTasks.length) {
    html += '<div style="margin-top:12px"><strong>' + esc(scopedTasks.length ? 'Selected Cluster Task Authority' : 'Top Authority Tasks') + '</strong><div style="margin-top:6px">';
    html += renderedTasks.map(item => {
      const basisState = corridorAuthorityApproximation(item);
      const basisFreshness = corridorAuthorityBasisFreshnessApproximation(item);
      const taskClass = corridorTaskClassValue(item);
      const taskClassSource = corridorTaskClassSource(item);
      return '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(item.title || item.task_id) + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showCorridorAuthorityTaskDetail((item.task_id))}) + '>Authority</button><button class="btn-session-muted corridor-open-task">Task</button></div></div><div style="font-size:11px;color:var(--muted);margin-top:4px">class ' + esc(String(taskClass).toLowerCase()) + ' | source ' + esc(String(taskClassSource).toLowerCase()) + ' | ' + esc(String(basisState).toLowerCase()) + ' | ' + esc(basisFreshness) + '</div></div>';
    }).join('');
    html += '</div></div>';
  }
  el.innerHTML = html;
  bindTaskDetailElements(el, renderedTasks, '.corridor-open-task');
}

async function loadCorridorAuthority() {
  try {
    const response = await rpc('workspace.instrumentation.corridor.authority.report', corridorAuthorityParams());
    corridorAuthorityReportCache = response.report || null;
    corridorAuthorityDetailCache = {};
    renderCorridorAuthorityState();
  } catch (e) {
    console.error('workspace.instrumentation.corridor.authority.report', e);
    corridorAuthorityReportCache = null;
    corridorAuthorityDetailCache = {};
    renderCorridorAuthorityState();
  }
}

async function showCorridorAuthorityTaskDetail(taskID) {
  const normalizedTaskID = String(taskID || '').trim();
  if (!normalizedTaskID) return;
  try {
    const response = await rpc('workspace.instrumentation.corridor.authority.task', corridorAuthorityParams({task_id: normalizedTaskID}));
    const detail = response.detail || null;
    corridorAuthorityDetailCache[normalizedTaskID] = detail;
    const task = (detail && detail.task) || {};
    let html = '<div style="font-size:11px;color:var(--muted);margin-bottom:10px">task_id: <code style="background:var(--surface);padding:2px 6px;border-radius:4px">' + esc(normalizedTaskID) + '</code></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;font-size:13px;margin-bottom:12px">';
    html += '<div><strong>Basis State</strong><br>' + esc(String(corridorAuthorityApproximation(task)).toLowerCase()) + '</div>';
    html += '<div><strong>Authority Basis</strong><br>' + esc(corridorAuthorityBasisFreshnessApproximation(task)) + '</div>';
    html += '<div><strong>Task Class</strong><br><span style="color:' + corridorTaskClassColor(corridorTaskClassValue(task)) + '">' + esc(String(corridorTaskClassValue(task)).toLowerCase()) + '</span></div>';
    html += '<div><strong>Class Source</strong><br>' + esc(String(corridorTaskClassSource(task)).toLowerCase()) + '</div>';
    html += '</div>';
    if (task.summary) {
      html += '<div style="margin-bottom:10px;font-size:12px;color:var(--muted)">' + esc(task.summary) + '</div>';
    }
    if (Array.isArray(task.task_class_basis) && task.task_class_basis.length) {
      html += '<div style="margin-bottom:12px"><strong>Basis</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + task.task_class_basis.slice(0, 10).map(item => '<span class="tool-badge kind">' + esc(item) + '</span>').join('') + '</div></div>';
    }
    if (task.corridor_lookup && typeof task.corridor_lookup === 'object') {
      html += '<div style="margin-bottom:12px"><strong>Derived Corridor Lookup</strong><div style="margin-top:6px;font-size:12px;color:var(--muted)">' + esc(corridorFirstNonEmpty(task.corridor_lookup.summary, task.corridor_lookup.lookup_status, 'not surfaced')) + '</div></div>';
    }
    const clusters = (detail && detail.clusters) || [];
    if (clusters.length) {
      html += '<div style="margin-bottom:12px"><strong>Visible Proto-Clusters</strong><div style="margin-top:6px">' + clusters.map(cluster => '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(cluster.proto_cluster_id || 'cluster') + '</strong><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();showProtoClusterDetail((cluster.proto_cluster_id || ''))}) + '>Proto-Cluster</button></div><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(cluster.summary || '') + '</div></div>').join('') + '</div></div>';
    }
    html += '<div style="font-size:11px;color:var(--muted)">Task-first corridor authority is a read-only precedence surface only. It does not assign a corridor or apply policy.</div>';
    openModal('Corridor Authority ' + esc(task.title || normalizedTaskID), html);
  } catch (e) {
    console.error('workspace.instrumentation.corridor.authority.task', e);
    toast('Corridor authority detail failed: ' + e.message);
  }
}

function corridorOwnershipParams(extra = {}) {
  return corridorAuthorityParams(extra);
}

function findCorridorOwnershipCluster(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return null;
  return ((corridorOwnershipReportCache && corridorOwnershipReportCache.clusters) || []).find(item => String(item.proto_cluster_id || '').trim() === clusterID) ||
    (((corridorOwnershipDetailCache || {})[clusterID] || {}).cluster) ||
    null;
}

function corridorOwnershipStateColor(state) {
  const normalized = String(state || '').toUpperCase();
  if (normalized === 'OWNED_EXPLICIT') return 'var(--green)';
  if (normalized === 'OWNED_EXPLICIT_STALE') return 'var(--accent2)';
  if (normalized === 'SEEDED_TEMPLATE') return 'var(--yellow)';
  if (normalized === 'DERIVED_CLUSTER') return 'var(--accent)';
  if (normalized === 'CONTESTED') return 'var(--red)';
  return 'var(--muted)';
}

function corridorOwnershipBasisFreshness(digest = {}) {
  const updatedAt = corridorFirstNonEmpty(digest.basis_updated_at, digest.task_class_updated_at);
  if (digest.basis_authoritative && digest.basis_fresh) {
    return updatedAt ? ('authoritative basis ' + timeAgo(updatedAt)) : 'authoritative basis';
  }
  if (digest.basis_authoritative && !digest.basis_fresh) {
    return updatedAt ? ('stale authoritative basis from ' + timeAgo(updatedAt)) : 'stale authoritative basis';
  }
  if (updatedAt) return 'derived basis ' + timeAgo(updatedAt);
  return 'no stable basis timestamp';
}

function syncCorridorOwnershipSnapshotFromRuntimeEvents() {
  corridorOwnershipSnapshotEventCache = (runtimeEventsCache || [])
    .filter(item => String(item.event_type || '').toLowerCase() === 'cluster.corridor_ownership_snapshot')
    .sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at))[0] || null;
  renderCorridorOwnershipState();
}

function renderCorridorOwnershipState() {
  const badge = document.getElementById('corridor-ownership-state');
  const el = document.getElementById('corridor-ownership-summary');
  if (!badge || !el) return;
  const report = corridorOwnershipReportCache;
  if (!report) {
    badge.textContent = 'no data';
    let html = '<div class="empty">Cluster-level corridor ownership and basis will appear once the ownership read-side report loads.</div>';
    if (corridorOwnershipSnapshotEventCache && corridorOwnershipSnapshotEventCache.event_id) {
      html += '<div style="margin-top:12px"><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((corridorOwnershipSnapshotEventCache.event_id))}) + '>Open Latest Ownership Snapshot</button></div>';
    }
    el.innerHTML = html;
    return;
  }
  const workspace = report.workspace || {};
  const authority = report.time_authority || null;
  const clusters = Array.isArray(report.clusters) ? report.clusters : [];
  const selectedID = controlPolicySelectedClusterID || ((clusters[0] || {}).proto_cluster_id || '');
  const selectedDetail = selectedID ? ((corridorOwnershipDetailCache || {})[selectedID] || null) : null;
  const selectedCluster = (selectedDetail && selectedDetail.cluster) || findCorridorOwnershipCluster(selectedID) || null;
  const selectedTasks = (selectedDetail && selectedDetail.tasks) || [];
  badge.textContent = report.generated_at ? timeAgo(report.generated_at, authority) : 'read-only';

  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">';
  [
    ['Owned Explicit', String(workspace.owned_explicit_count || 0), 'Fresh explicit task-owned basis anchored by authored task_class evidence'],
    ['Explicit Stale', String(workspace.owned_explicit_stale_count || 0), 'Authored basis still visible, but past the freshness window'],
    ['Seeded Template', String(workspace.seeded_template_count || 0), 'Template-default basis currently anchors the cluster without policy authority'],
    ['Derived Cluster', String(workspace.derived_cluster_count || 0), 'Cluster still leans on derived basis only'],
    ['Contested', String(workspace.contested_count || 0), 'Conflicting task-owned bases block a single cluster owner'],
    ['Unresolved', String(workspace.unresolved_count || 0), 'No stable cluster-owned basis is visible yet'],
    ['Active Steward Leases', String(workspace.active_steward_count || 0), 'Clusters with a currently active stewardship lease under workspace time authority']
  ].forEach(card => {
    html += '<div class="msg-item" style="margin:0"><strong>' + esc(card[0]) + '</strong><div style="margin-top:4px;font-size:20px;font-weight:700">' + esc(card[1]) + '</div><div style="margin-top:6px;font-size:11px;color:var(--muted);line-height:1.4">' + esc(card[2]) + '</div></div>';
  });
  html += '</div>';
  html += '<div style="margin-top:12px;font-size:12px;color:var(--muted)">Read-only cluster-level basis layer between task-first corridor authority and downstream corridor fit / boundary diagnostics. It keeps a single visible basis for operator inspection only and does not assign a corridor or apply policy.</div>';

  if (selectedCluster) {
    const ownership = selectedCluster.ownership || {};
    const steward = selectedCluster.steward || null;
    const relatedTensions = relatedTensionsForProtoCluster(selectedCluster.proto_cluster_id || '');
    const relatedEvents = controlPolicyClusterEvents(
      selectedCluster.proto_cluster_id || '',
      selectedCluster.task_ids || [],
      selectedCluster.session_ids || [],
      selectedCluster.agent_ids || [],
      relatedTensions.map(item => item.tension_id)
    );
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Selected Ownership Basis</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(selectedCluster.proto_cluster_id || 'cluster') + ' | ownership ' + esc(String(ownership.ownership_state || 'UNRESOLVED').toLowerCase()) + ' | readiness ' + esc(String(selectedCluster.corridor_readiness || 'UNDER_EVIDENCED').toLowerCase()) + '</div></div><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showCorridorOwnershipClusterDetail((selectedCluster.proto_cluster_id || ''))}) + '>Refresh Detail</button>' + (ownership.owner_task_id ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showCorridorAuthorityTaskDetail((ownership.owner_task_id))}) + '>Owner Task</button>' : '') + '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((selectedCluster.proto_cluster_id || ''))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorBoundarySurface((selectedCluster.proto_cluster_id || ''))}) + '>Boundary</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((selectedCluster.proto_cluster_id || ''))}) + '>Tensions</button>' + (relatedEvents.length ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((relatedEvents[0].event_id))}) + '>Latest Runtime Event</button>' : '') + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Ownership State</strong><br><span style="color:' + corridorOwnershipStateColor(ownership.ownership_state) + '">' + esc(String(ownership.ownership_state || 'UNRESOLVED').toLowerCase()) + '</span></div>';
    html += '<div><strong>Basis Task Class</strong><br><span style="color:' + corridorTaskClassColor(ownership.basis_task_class) + '">' + esc(String(ownership.basis_task_class || selectedCluster.task_class_hint || 'UNKNOWN').toLowerCase()) + '</span></div>';
    html += '<div><strong>Basis Source</strong><br>' + esc(String(ownership.basis_task_class_source || 'n/a').toLowerCase()) + '</div>';
    html += '<div><strong>Basis Freshness</strong><br>' + esc(corridorOwnershipBasisFreshness(ownership)) + '</div>';
    html += '<div><strong>Owner Tasks</strong><br>' + esc(String((ownership.owner_task_ids || []).length || 0)) + '</div>';
    html += '<div><strong>Conflicts</strong><br>' + esc(String((ownership.conflicting_task_ids || []).length || 0)) + '</div>';
    html += '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Readiness</strong><br><span style="color:' + corridorReadinessColor(selectedCluster.corridor_readiness) + '">' + esc(String(selectedCluster.corridor_readiness || 'UNDER_EVIDENCED').toLowerCase()) + '</span></div>';
    html += '<div><strong>Lookup</strong><br>' + esc(corridorLookupApproximation(selectedCluster, selectedDetail)) + '</div>';
    html += '<div><strong>Catalog</strong><br>' + esc(corridorCatalogApproximation(selectedCluster, selectedDetail)) + '</div>';
    html += '<div><strong>Unknown Tasks</strong><br>' + esc(String(selectedCluster.unknown_task_count || 0)) + '</div>';
    html += '<div><strong>Task Anchors</strong><br>' + esc(String((selectedCluster.task_ids || []).length)) + '</div>';
    html += '<div><strong>Agent Anchors</strong><br>' + esc(String((selectedCluster.agent_ids || []).length)) + '</div>';
    html += '</div>';
    if (steward) {
      html += '<div style="margin-top:12px"><strong>Current Steward Lease</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Read-only lease visibility for overlapping coordination and stewarded recovery; this does not grant write authority by itself.</div>';
      html += '<div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-top:8px">';
      html += '<div><strong>Steward Agent</strong><br>' + esc(String(steward.steward_agent_id || 'n/a')) + '</div>';
      html += '<div><strong>Epoch</strong><br>' + esc(String(steward.epoch_id || 'n/a')) + '</div>';
      html += '<div><strong>Granted</strong><br>' + esc(timeAgo(steward.granted_at || '', authority)) + '</div>';
      html += '<div><strong>Expires</strong><br>' + esc(timeAgo(steward.expires_at || '', authority)) + '</div>';
      html += '<div><strong>Status</strong><br>' + esc(String(steward.status || 'ACTIVE').toLowerCase()) + '</div>';
      html += '</div></div>';
    } else {
      html += '<div style="margin-top:12px"><strong>Current Steward Lease</strong><div class="empty" style="margin-top:6px">No active steward lease is currently visible for this cluster.</div></div>';
    }
    if ((ownership.owner_task_ids || []).length) {
      html += '<div style="margin-top:12px"><strong>Owner Task Anchors</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + (ownership.owner_task_ids || []).map(taskID => '<button class="tool-badge kind" style="cursor:pointer;border:none" ' + dashboardAction(function(dashboardEvent){showCorridorAuthorityTaskDetail((taskID))}) + '>' + esc(taskID) + '</button>').join('') + '</div></div>';
    }
    if ((ownership.supporting_task_ids || []).length) {
      html += '<div style="margin-top:12px"><strong>Supporting Tasks</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + (ownership.supporting_task_ids || []).map(taskID => '<span class="tool-badge kind">' + esc(taskID) + '</span>').join('') + '</div></div>';
    }
    if ((ownership.conflicting_task_ids || []).length) {
      html += '<div style="margin-top:12px"><strong>Conflicting Tasks</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + (ownership.conflicting_task_ids || []).map(taskID => '<span class="tool-badge kind" style="border-color:var(--red);color:var(--red)">' + esc(taskID) + '</span>').join('') + '</div></div>';
    }
    if (selectedTasks.length) {
      html += '<div style="margin-top:12px"><strong>Task Basis Inputs</strong><div style="margin-top:6px">';
      html += selectedTasks.slice(0, 6).map(task => '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(task.title || task.task_id) + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><span style="color:' + corridorTaskClassColor(corridorTaskClassValue(task)) + '">' + esc(String(corridorTaskClassValue(task)).toLowerCase()) + '</span><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showCorridorAuthorityTaskDetail((task.task_id))}) + '>Authority</button></div></div><div style="font-size:11px;color:var(--muted);margin-top:4px">source ' + esc(String(corridorTaskClassSource(task)).toLowerCase()) + ' | lookup ' + esc(corridorFirstNonEmpty(task.corridor_lookup && task.corridor_lookup.lookup_status, 'NO_MATCH').toLowerCase()) + ' | basis ' + esc(corridorFirstNonEmpty(task.basis_updated_at, task.task_class_updated_at, 'n/a')) + '</div></div>').join('');
      html += '</div></div>';
    }
    html += '<div style="margin-top:12px;font-size:11px;color:var(--muted);line-height:1.5">' + esc(selectedCluster.summary || ownership.summary || 'Cluster-level corridor ownership remains an operator-facing basis layer only and must not be treated as applied policy.') + '</div>';
    html += '</div>';
  }

  if (corridorOwnershipSnapshotEventCache && corridorOwnershipSnapshotEventCache.event_id) {
    const snapshotPayload = parseJSON(corridorOwnershipSnapshotEventCache.payload_json);
    const snapshotWorkspace = snapshotPayload.workspace || {};
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Corridor Ownership Snapshot Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Persisted corridor ownership/basis report for replay and operator inspection.</div></div><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((corridorOwnershipSnapshotEventCache.event_id))}) + '>Open Latest Ownership Snapshot</button></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Generated</strong><br>' + esc(timeAgo((snapshotPayload.generated_at || corridorOwnershipSnapshotEventCache.created_at || ''), authority)) + '</div>';
    html += '<div><strong>Owned Explicit</strong><br>' + esc(String(snapshotWorkspace.owned_explicit_count || 0)) + '</div>';
    html += '<div><strong>Explicit Stale</strong><br>' + esc(String(snapshotWorkspace.owned_explicit_stale_count || 0)) + '</div>';
    html += '<div><strong>Seeded Template</strong><br>' + esc(String(snapshotWorkspace.seeded_template_count || 0)) + '</div>';
    html += '<div><strong>Contested</strong><br>' + esc(String(snapshotWorkspace.contested_count || 0)) + '</div>';
    html += '<div><strong>Unresolved</strong><br>' + esc(String(snapshotWorkspace.unresolved_count || 0)) + '</div>';
    html += '<div><strong>Active Leases</strong><br>' + esc(String(snapshotWorkspace.active_steward_count || 0)) + '</div>';
    html += '</div></div>';
  }

  el.innerHTML = html;
}

async function loadCorridorOwnership() {
  try {
    const response = await rpc('workspace.instrumentation.corridor.ownership.report', corridorOwnershipParams());
    corridorOwnershipReportCache = response.report || null;
    corridorOwnershipDetailCache = {};
    renderCorridorOwnershipState();
    const selected = controlPolicySelectedClusterID || ((((corridorOwnershipReportCache || {}).clusters || [])[0] || {}).proto_cluster_id || '');
    if (selected) await showCorridorOwnershipClusterDetail(selected);
  } catch (e) {
    console.error('workspace.instrumentation.corridor.ownership.report', e);
    corridorOwnershipReportCache = null;
    corridorOwnershipDetailCache = {};
    renderCorridorOwnershipState();
  }
}

async function showCorridorOwnershipClusterDetail(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) {
    renderCorridorOwnershipState();
    return;
  }
  try {
    const response = await rpc('workspace.instrumentation.corridor.ownership.cluster', {
      workspace_id: WS_ID,
      proto_cluster_id: clusterID
    });
    corridorOwnershipDetailCache[clusterID] = response.detail || null;
  } catch (e) {
    console.error('workspace.instrumentation.corridor.ownership.cluster', e);
    corridorOwnershipDetailCache[clusterID] = null;
  }
  renderCorridorOwnershipState();
}

async function createCorridorOwnershipSnapshot() {
  const btn = document.getElementById('corridor-ownership-snapshot-btn');
  const originalText = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Recording...';
  }
  try {
    const response = await rpc('workspace.instrumentation.corridor.ownership.snapshot', corridorOwnershipParams({
      proto_cluster_id: controlPolicySelectedClusterID || '',
      actor_id: 'dashboard',
      limit: 40
    }));
    corridorOwnershipReportCache = response.report || corridorOwnershipReportCache;
    corridorOwnershipSnapshotEventCache = response.event || corridorOwnershipSnapshotEventCache;
    renderCorridorOwnershipState();
    if (controlPolicySelectedClusterID) await showCorridorOwnershipClusterDetail(controlPolicySelectedClusterID);
    await loadRuntimeEvents();
    toast('Corridor ownership snapshot recorded');
  } catch (e) {
    console.error('workspace.instrumentation.corridor.ownership.snapshot', e);
    toast('Corridor ownership snapshot failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Record Snapshot';
    }
  }
}

function corridorSegmentEntries(...sources) {
  const seen = new Set();
  const entries = [];
  const pushEntry = (value, scopeHint = '') => {
    if (value === undefined || value === null) return;
    if (typeof value === 'string') {
      const normalized = corridorFirstNonEmpty(value);
      if (!normalized) return;
      const key = [normalized.toLowerCase(), scopeHint.toLowerCase()].join('|');
      if (seen.has(key)) return;
      seen.add(key);
      entries.push({label: normalized, ref: normalized, scope: scopeHint, meta: scopeHint ? [scopeHint] : []});
      return;
    }
    if (typeof value !== 'object') return;
    const nestedKeys = [
      ['segment_refs', scopeHint],
      ['segments', scopeHint],
      ['doc_segment_refs', 'doc'],
      ['doc_segments', 'doc'],
      ['artifact_segment_refs', 'artifact'],
      ['artifact_segments', 'artifact']
    ];
    let nestedFound = false;
    nestedKeys.forEach(([key, nestedScope]) => {
      if (Array.isArray(value[key]) && value[key].length) {
        nestedFound = true;
        value[key].forEach(item => pushEntry(item, nestedScope || scopeHint));
      }
    });
    const ref = corridorFirstNonEmpty(
      value.segment_ref,
      value.segment_id,
      value.segment_key,
      value.segment,
      value.ref,
      value.id
    );
    if (!ref) {
      if (nestedFound) return;
      return;
    }
    const label = corridorFirstNonEmpty(
      value.label,
      value.title,
      value.name,
      value.segment_label,
      ref
    );
    const scope = corridorFirstNonEmpty(
      value.segment_scope,
      value.segment_kind,
      value.scope,
      scopeHint
    );
    const owner = corridorFirstNonEmpty(
      value.doc_key,
      value.source_doc_key,
      value.artifact_ref,
      value.source_artifact_ref,
      value.artifact_id
    );
    const note = corridorFirstNonEmpty(
      value.summary,
      value.reason,
      value.kind,
      value.content_type
    );
    const meta = [scope, owner, note].filter(Boolean);
    const key = [ref.toLowerCase(), scope.toLowerCase(), owner.toLowerCase()].join('|');
    if (seen.has(key)) return;
    seen.add(key);
    entries.push({label, ref, scope, meta});
  };
  sources.forEach(source => {
    if (Array.isArray(source)) {
      source.forEach(item => pushEntry(item));
      return;
    }
    pushEntry(source);
  });
  return entries;
}

async function showWorkspaceSegmentDetail(segmentRef) {
  const normalizedRef = String(segmentRef || '').trim();
  if (!normalizedRef) return;
  try {
    const response = await rpc('workspace.segment.get', {workspace_id: WS_ID, segment_ref: normalizedRef});
    const segment = response.segment || {};
    let html = '<div style="font-size:11px;color:var(--muted);margin-bottom:10px">segment_ref: <code style="background:var(--surface);padding:2px 6px;border-radius:4px">' + esc(normalizedRef) + '</code></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;font-size:13px;margin-bottom:12px">';
    html += '<div><strong>Source</strong><br>' + esc(segment.source_kind || 'segment') + '</div>';
    html += '<div><strong>Owner</strong><br>' + esc(segment.source_ref || 'n/a') + '</div>';
    html += '<div><strong>Kind</strong><br>' + esc(segment.segment_kind || 'root') + '</div>';
    html += '<div><strong>Lines</strong><br>' + esc((segment.start_line || 0) + '–' + (segment.end_line || 0)) + '</div>';
    html += '</div>';
    if (segment.title) html += '<div style="margin-bottom:8px"><strong>Title</strong><br>' + esc(segment.title) + '</div>';
    if (segment.summary) html += '<div style="margin-bottom:8px"><strong>Summary</strong><br>' + esc(segment.summary) + '</div>';
    html += '<div style="font-size:11px;color:var(--muted)">Derived structural segment only. This is a read-only locality anchor, not canonical policy or proof authority.</div>';
    if (String(segment.source_kind || '') === 'workspace_doc' && segment.source_ref) {
      html += '<div style="margin-top:12px"><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){closeModal();showDoc((segment.source_ref))}) + '>Open Doc</button></div>';
    }
    openModal('Segment ' + esc(segment.title || segment.source_ref || normalizedRef), html);
  } catch (e) {
    console.error('workspace.segment.get', e);
    toast('Segment detail failed: ' + e.message);
  }
}

function renderSegmentBadgeRow(title, entries) {
  if (!Array.isArray(entries) || !entries.length) return '';
  return '<div style="margin-top:12px"><strong>' + esc(title) + '</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' +
    entries.slice(0, 12).map(entry => {
      const meta = Array.isArray(entry.meta) ? entry.meta.filter(Boolean).join(' | ') : '';
      const tooltip = meta ? (' title="' + esc(meta) + '"') : '';
      const ref = String(entry.ref || '').trim();
      const clickable = ref.startsWith('workspace_doc:') || ref.startsWith('artifact:');
      if (clickable) {
        return '<button class="tool-badge kind" style="cursor:pointer;border:none"' + tooltip + ' ' + dashboardAction(function(dashboardEvent){showWorkspaceSegmentDetail((ref))}) + '>' + esc(entry.label || ref || 'segment') + '</button>';
      }
      return '<span class="tool-badge kind"' + tooltip + '>' + esc(entry.label || entry.ref || 'segment') + '</span>';
    }).join('') +
  '</div></div>';
}

function findCorridorReadinessCluster(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return null;
  return ((corridorReadinessReportCache && corridorReadinessReportCache.clusters) || []).find(item => String(item.proto_cluster_id || '').trim() === clusterID) ||
    (((corridorReadinessDetailCache || {})[clusterID] || {}).cluster) ||
    null;
}

function corridorFitParams(extra = {}) {
  const params = {
    workspace_id: WS_ID,
    limit: 40
  };
  Object.keys(extra || {}).forEach(key => {
    if (extra[key] !== undefined && extra[key] !== null && extra[key] !== '') params[key] = extra[key];
  });
  return params;
}

function findCorridorFitCluster(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return null;
  return ((corridorFitReportCache && corridorFitReportCache.clusters) || []).find(item => String(item.proto_cluster_id || '').trim() === clusterID) ||
    (((corridorFitDetailCache || {})[clusterID] || {}).cluster) ||
    null;
}

function corridorFitStatusColor(status) {
  const normalized = String(status || '').toUpperCase();
  if (normalized === 'IN_CORRIDOR') return 'var(--green)';
  if (normalized === 'NEAR_BOUNDARY') return 'var(--yellow)';
  if (normalized === 'OUT_OF_CORRIDOR') return 'var(--red)';
  if (normalized === 'STALE_BASIS') return 'var(--accent2)';
  return 'var(--muted)';
}

function corridorFitSummaryCounts(workspace = {}) {
  return [
    ['In Corridor', String(workspace.in_corridor_count || 0), 'Clusters whose proxy metric vector currently sits within the selected corridor range'],
    ['Near Boundary', String(workspace.near_boundary_count || 0), 'Clusters that still fit but are hugging one or more corridor boundaries'],
    ['Out of Corridor', String(workspace.out_of_corridor_count || 0), 'Clusters whose proxy metric vector currently violates at least one corridor range'],
    ['Under-Evidenced', String(workspace.under_evidenced_count || 0), 'Clusters without enough stable task-class evidence for corridor-fit evaluation'],
    ['Stale Basis', String(workspace.stale_basis_count || 0), 'Clusters whose fit basis is stale and should be refreshed before any stronger interpretation'],
    ['Visible', String(workspace.total_clusters || 0), 'Corridor-fit clusters currently returned by the server']
  ];
}

function corridorFitViolationGaps(cluster) {
  return (Array.isArray(cluster && cluster.metric_gap_breakdown) ? cluster.metric_gap_breakdown : [])
    .filter(gap => String((gap && gap.status) || 'IN_RANGE').toUpperCase() !== 'IN_RANGE');
}

function corridorFitDominantGap(cluster) {
  const gaps = corridorFitViolationGaps(cluster).slice();
  gaps.sort((left, right) => Math.abs(Number(right.delta || 0)) - Math.abs(Number(left.delta || 0)));
  return gaps[0] || null;
}

function corridorFitBasisSummary(cluster) {
  const rangeCheck = (cluster && cluster.catalog_range_check) || {};
  return corridorFirstNonEmpty(rangeCheck.basis_summary, rangeCheck.basis_fresh ? 'catalog lookup is backed by current task-class evidence' : '', 'No stable corridor boundary basis is visible yet.');
}

function renderCorridorFitState() {
  const badge = document.getElementById('corridor-fit-state');
  const el = document.getElementById('corridor-fit-summary');
  if (!badge || !el) return;
  badge.textContent = 'read-only';
  const report = corridorFitReportCache || null;
  if (!report) {
    let html = '<div class="empty">Corridor boundary and violation approximation will appear once the corridor-fit read-side report loads.</div>';
    html += '<div style="margin-top:12px;font-size:11px;color:var(--muted);line-height:1.5">Read-only corridor-fit approximation over task-class evidence, corridor catalog lookup, proto-cluster metrics, and confirmed tensions. It stays operator-facing and does not apply policy. Read-only corridor-boundary and violation approximation over catalog-range checks, proxy metric vectors, and confirmed tensions. It stays operator-facing and does not assign corridors or carry policy authority.</div>';
    if (corridorFitSnapshotEventCache && corridorFitSnapshotEventCache.event_id) {
      html += '<div style="margin-top:12px"><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((corridorFitSnapshotEventCache.event_id))}) + '>Open Latest Fit Snapshot</button></div>';
    }
    el.innerHTML = html;
    return;
  }
  const workspace = report.workspace || {};
  const clusters = report.clusters || [];
  const selectedID = controlPolicySelectedClusterID || ((clusters[0] || {}).proto_cluster_id || '');
  const selectedDetail = selectedID ? ((corridorFitDetailCache || {})[selectedID] || null) : null;
  const selectedCluster = (selectedDetail && selectedDetail.cluster) || findCorridorFitCluster(selectedID) || null;
  const selectedTensions = (selectedDetail && selectedDetail.confirmed_tensions) || [];
  const topCatalogs = Object.entries(workspace.catalog_key_counts || {})
    .sort((left, right) => right[1] - left[1])
    .slice(0, 4);

  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">';
  corridorFitSummaryCounts(workspace).forEach(card => {
    html += '<div class="msg-item" style="margin:0"><strong>' + esc(card[0]) + '</strong><div style="margin-top:4px;font-size:20px;font-weight:700">' + esc(card[1]) + '</div><div style="margin-top:6px;font-size:11px;color:var(--muted);line-height:1.4">' + esc(card[2]) + '</div></div>';
  });
  html += '</div>';
  html += '<div style="display:flex;justify-content:space-between;gap:12px;margin-top:12px;font-size:12px;color:var(--muted)">';
  html += '<span>Read-only corridor-fit approximation over task-class evidence, corridor catalog lookup, proto-cluster metrics, and confirmed tensions. It stays operator-facing and does not apply policy. Read-only corridor-boundary and violation approximation over catalog-range checks, proxy metric vectors, and confirmed tensions. It stays operator-facing and does not assign corridors or carry policy authority.</span>';
  if (topCatalogs.length) {
    html += '<span>' + topCatalogs.map(entry => '<span class="tool-badge kind" style="margin-left:6px">' + esc(entry[0] + ' ' + entry[1]) + '</span>').join('') + '</span>';
  } else {
    html += '<span>No dominant corridor boundary basis is visible yet.</span>';
  }
  html += '</div>';
  if (selectedCluster) {
    const rangeCheck = selectedCluster.catalog_range_check || {};
    const metricVector = selectedCluster.metric_vector || {};
    const gaps = Array.isArray(selectedCluster.metric_gap_breakdown) ? selectedCluster.metric_gap_breakdown : [];
    const dominantGap = corridorFitDominantGap(selectedCluster);
    const relatedEvents = controlPolicyClusterEvents(
      selectedCluster.proto_cluster_id || '',
      selectedCluster.task_ids || [],
      selectedCluster.session_ids || [],
      selectedCluster.agent_ids || [],
      (selectedCluster.confirmed_tension_ids || []).slice()
    );
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Selected Boundary Approximation</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(selectedCluster.proto_cluster_id || 'cluster') + ' | fit ' + esc(String(selectedCluster.fit_status || 'UNDER_EVIDENCED').toLowerCase()) + ' | catalog ' + esc(corridorFirstNonEmpty(rangeCheck.display_name, rangeCheck.catalog_key, 'not surfaced')) + '</div></div><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorSurface((selectedCluster.proto_cluster_id || ''))}) + '>Open Corridor Surface</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((selectedCluster.proto_cluster_id || ''))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((selectedCluster.proto_cluster_id || ''))}) + '>Tensions</button>' + (relatedEvents.length ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((relatedEvents[0].event_id))}) + '>Latest Runtime Event</button>' : '') + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Fit Status</strong><br><span style="color:' + corridorFitStatusColor(selectedCluster.fit_status) + '">' + esc(String(selectedCluster.fit_status || 'UNDER_EVIDENCED').toLowerCase()) + '</span></div>';
    html += '<div><strong>Fit Confidence</strong><br>' + esc(Math.round(Number(selectedCluster.fit_confidence || 0) * 100) + '%') + '</div>';
    html += '<div><strong>Fit Score</strong><br>' + esc(String(selectedCluster.fit_score || 0)) + '</div>';
    html += '<div><strong>Catalog Range</strong><br>' + esc(corridorFirstNonEmpty(rangeCheck.display_name, rangeCheck.catalog_key, 'not surfaced')) + '</div>';
    html += '<div><strong>Boundary Basis</strong><br>' + esc(corridorFitBasisSummary(selectedCluster)) + '</div>';
    html += '<div><strong>Confirmed Tensions</strong><br>' + esc(String(selectedCluster.confirmed_tension_count || 0)) + '</div>';
    html += '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Out-of-Range Metrics</strong><br>' + esc(String(corridorFitViolationGaps(selectedCluster).length)) + '</div>';
    html += '<div><strong>Dominant Violation</strong><br>' + esc(corridorFirstNonEmpty(dominantGap && dominantGap.metric, selectedCluster.fit_status === 'NEAR_BOUNDARY' ? 'boundary proximity' : 'none')) + '</div>';
    html += '<div><strong>Largest Delta</strong><br>' + esc(dominantGap ? Math.abs(Number(dominantGap.delta || 0)).toFixed(2) : '0.00') + '</div>';
    html += '<div><strong>Match Source</strong><br>' + esc(corridorFirstNonEmpty(rangeCheck.match_source, (selectedCluster.corridor_lookup || {}).match_source, 'corridor_lookup')) + '</div>';
    html += '<div><strong>Basis Freshness</strong><br>' + esc(rangeCheck.basis_fresh ? 'fresh' : 'stale') + '</div>';
    html += '<div><strong>Last Basis</strong><br>' + esc(corridorFirstNonEmpty(selectedCluster.last_basis_event_at, selectedCluster.task_class_updated_at, 'n/a')) + '</div>';
    html += '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:10px">';
    [['Alignment', metricVector.alignment], ['Differentiation', metricVector.differentiation], ['Synergy', metricVector.synergy], ['Centralization', metricVector.centralization], ['Metastability', metricVector.metastability], ['Progress', metricVector.progress]].forEach(entry => {
      html += '<div><strong>' + esc(entry[0]) + '</strong><br>' + esc(Number(entry[1] || 0).toFixed(2)) + '</div>';
    });
    html += '</div>';
    if (gaps.length) {
      html += '<div style="margin-top:12px"><strong>Metric Gap Breakdown</strong><div style="margin-top:6px">';
      html += gaps.map(gap => {
        const status = String(gap.status || 'IN_RANGE');
        const lower = gap.lower_bound === undefined || gap.lower_bound === null ? 'none' : Number(gap.lower_bound).toFixed(2);
        const upper = gap.upper_bound === undefined || gap.upper_bound === null ? 'none' : Number(gap.upper_bound).toFixed(2);
        const delta = Math.abs(Number(gap.delta || 0)).toFixed(2);
        return '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(String(gap.metric || '').toUpperCase()) + '</strong><span style="color:' + (status === 'IN_RANGE' ? 'var(--green)' : status === 'LOW' ? 'var(--accent2)' : 'var(--red)') + '">' + esc(status.toLowerCase()) + '</span></div><div style="font-size:11px;color:var(--muted);margin-top:4px">value ' + esc(Number(gap.value || 0).toFixed(2)) + ' | range ' + esc(lower + ' - ' + upper) + ' | delta ' + esc(delta) + '</div></div>';
      }).join('');
      html += '</div></div>';
    }
    if (selectedTensions.length) {
      html += '<div style="margin-top:12px"><strong>Confirmed Corroborating Tensions</strong><div style="margin-top:6px">';
      html += selectedTensions.slice(0, 4).map(item => '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(item.tension_type || item.tension_id) + '</strong><span>' + esc(String(item.surface_score || 0)) + '</span></div><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(item.summary || '') + '</div></div>').join('');
      html += '</div></div>';
    }
    html += '<div style="margin-top:10px;font-size:11px;color:var(--muted);line-height:1.5">' + esc(selectedCluster.summary || 'Corridor boundary and violation diagnostics remain operator-facing approximations only and should not be treated as applied policy.') + '</div>';
    html += '</div>';
  } else {
    html += '<div class="msg-item" style="margin-top:12px"><strong>Selected Boundary Approximation</strong><div style="font-size:11px;color:var(--muted);margin-top:6px">Corridor boundary detail will appear once a proto-cluster is selected and the read-side cluster detail loads.</div></div>';
  }
  if (corridorFitSnapshotEventCache && corridorFitSnapshotEventCache.event_id) {
    const fitSnapshotPayload = parseJSON(corridorFitSnapshotEventCache.payload_json);
    const fitSnapshotWorkspace = fitSnapshotPayload.workspace || {};
    html += '<div class="msg-item" style="margin-top:12px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Corridor Fit Snapshot Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Persisted corridor-fit report for replay and operator inspection.</div></div><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((corridorFitSnapshotEventCache.event_id))}) + '>Open Latest Fit Snapshot</button></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Generated</strong><br>' + esc(timeAgo((fitSnapshotPayload.generated_at || corridorFitSnapshotEventCache.created_at || ''))) + '</div>';
    html += '<div><strong>In Corridor</strong><br>' + esc(String(fitSnapshotWorkspace.in_corridor_count || 0)) + '</div>';
    html += '<div><strong>Near Boundary</strong><br>' + esc(String(fitSnapshotWorkspace.near_boundary_count || 0)) + '</div>';
    html += '<div><strong>Out of Corridor</strong><br>' + esc(String(fitSnapshotWorkspace.out_of_corridor_count || 0)) + '</div>';
    html += '<div><strong>Under-Evidenced</strong><br>' + esc(String(fitSnapshotWorkspace.under_evidenced_count || 0)) + '</div>';
    html += '<div><strong>Stale Basis</strong><br>' + esc(String(fitSnapshotWorkspace.stale_basis_count || 0)) + '</div>';
    html += '</div></div>';
  }
  el.innerHTML = html;
}

function syncCorridorFitSnapshotFromRuntimeEvents() {
  corridorFitSnapshotEventCache = (runtimeEventsCache || [])
    .filter(item => String(item.event_type || '').toLowerCase() === 'cluster.corridor_fit_snapshot')
    .sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at))[0] || null;
  renderCorridorReadinessState();
}

function syncCorridorReadinessSnapshotFromRuntimeEvents() {
  corridorReadinessSnapshotEventCache = (runtimeEventsCache || [])
    .filter(item => String(item.event_type || '').toLowerCase() === 'cluster.corridor_readiness_snapshot')
    .sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at))[0] || null;
  renderCorridorReadinessState();
}

function renderCorridorReadinessState() {
  const badge = document.getElementById('corridor-readiness-state');
  const el = document.getElementById('corridor-readiness-summary');
  if (!badge || !el) return;
  renderCorridorFitState();
  badge.textContent = 'read-only';
  const report = corridorReadinessReportCache;
  if (!report) {
    let html = '<div class="empty">Task-class evidence and corridor-readiness approximation will appear once the corridor read-side report loads.</div>';
    if (corridorReadinessSnapshotEventCache && corridorReadinessSnapshotEventCache.event_id) {
      html += '<div style="margin-top:12px"><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((corridorReadinessSnapshotEventCache.event_id))}) + '>Open Latest Corridor Snapshot</button></div>';
    }
    el.innerHTML = html;
    return;
  }
  const workspace = report.workspace || {};
  const clusters = report.clusters || [];
  const catalogEntries = Array.isArray(report.catalog) ? report.catalog : [];
  const selectedID = controlPolicySelectedClusterID || ((clusters[0] || {}).proto_cluster_id || '');
  const selectedDetail = selectedID ? ((corridorReadinessDetailCache || {})[selectedID] || null) : null;
  const selectedCluster = (selectedDetail && selectedDetail.cluster) || findCorridorReadinessCluster(selectedID) || null;
  const topClasses = Object.entries(corridorWorkspaceTaskClassCounts(workspace))
    .sort((left, right) => right[1] - left[1])
    .slice(0, 4);

  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">';
  [
    ['Ready', String(workspace.ready_count || 0), 'Clusters with a clear, recent corridor basis'],
    ['Borderline', String(workspace.borderline_count || 0), 'Clusters that lean toward one class but still need cleaner task metadata'],
    ['Under-Evidenced', String(workspace.under_evidenced_count || 0), 'Clusters without enough task metadata for corridor catalog lookup'],
    ['Mixed', String(workspace.mixed_count || 0), 'Clusters spanning more than one concrete task-class hint'],
    ['Stale Lookup Basis', String(workspace.stale_basis_count || 0), 'Clusters whose last task or corridor lookup basis is older than the freshness window; treat readiness as a stale approximation'],
    ['Visible', String(clusters.length), 'Corridor read-side clusters currently returned by the server']
  ].forEach(card => {
    html += '<div class="msg-item" style="margin:0"><strong>' + esc(card[0]) + '</strong><div style="margin-top:4px;font-size:20px;font-weight:700">' + esc(card[1]) + '</div><div style="margin-top:6px;font-size:11px;color:var(--muted);line-height:1.4">' + esc(card[2]) + '</div></div>';
  });
  html += '</div>';
  html += '<div style="display:flex;justify-content:space-between;gap:12px;margin-top:12px;font-size:12px;color:var(--muted)">';
  html += '<span>Read-only approximation over task metadata and proto-cluster evidence. task_class, task_class_source, and corridor_readiness support operator inspection only; they do not assign a corridor or carry policy authority.</span>';
  if (topClasses.length) {
    html += '<span>' + topClasses.map(entry => '<span class="tool-badge kind" style="margin-left:6px">' + esc(entry[0].toLowerCase() + ' ' + entry[1]) + '</span>').join('') + '</span>';
  } else {
    html += '<span>No dominant task-class evidence is visible yet.</span>';
  }
  html += '</div>';
  if (catalogEntries.length) {
    html += '<div class="msg-item" style="margin-top:12px"><strong>Corridor Catalog</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Static read-side catalog used for explicit corridor lookup, still non-authoritative and read-only.</div><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;margin-top:10px">';
    html += catalogEntries.map(entry => {
      const templates = Array.isArray(entry.preferred_task_templates) ? entry.preferred_task_templates : [];
      return '<div class="msg-item" style="margin:0">' +
        '<strong>' + esc(entry.display_name || entry.catalog_key || 'catalog') + '</strong>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(entry.summary || 'No catalog summary') + '</div>' +
        '<div style="margin-top:6px;font-size:11px;color:var(--muted)">task class ' + esc(String(corridorTaskClassValue(entry)).toLowerCase()) + '</div>' +
        (templates.length ? '<div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + templates.map(template => '<span class="tool-badge kind">' + esc(template) + '</span>').join('') + '</div>' : '<div style="margin-top:6px;font-size:11px;color:var(--muted)">No exact template matches; class-only lookup.</div>') +
      '</div>';
    }).join('');
    html += '</div></div>';
  }

  if (selectedCluster) {
    const relatedTensions = relatedTensionsForProtoCluster(selectedCluster.proto_cluster_id || '');
    const relatedEvents = controlPolicyClusterEvents(
      selectedCluster.proto_cluster_id || '',
      selectedCluster.task_ids || [],
      selectedCluster.session_ids || [],
      selectedCluster.agent_ids || [],
      relatedTensions.map(item => item.tension_id)
    );
    const tasks = (selectedDetail && selectedDetail.tasks) || [];
    const freshness = corridorBasisFreshnessApproximation(selectedCluster);
    const catalogApproximation = corridorCatalogApproximation(selectedCluster, selectedDetail);
    const lookupApproximation = corridorLookupApproximation(selectedCluster, selectedDetail);
    const authorityApproximation = corridorAuthorityApproximation(selectedCluster, selectedDetail);
    const authorityFreshness = corridorAuthorityBasisFreshnessApproximation(selectedCluster, selectedDetail);
    const clusterTaskClass = corridorTaskClassValue(selectedCluster);
    const clusterTaskClassSource = corridorTaskClassSource(selectedCluster);
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Selected Cluster Approximation</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(selectedCluster.proto_cluster_id || 'cluster') + ' | task class evidence ' + esc(String(clusterTaskClass).toLowerCase()) + ' | task-first authority ' + esc(String(authorityApproximation).toLowerCase()) + ' | corridor-readiness approximation ' + esc(String(selectedCluster.corridor_readiness || 'UNDER_EVIDENCED').toLowerCase()) + '</div></div><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showControlPolicyClusterDetail((selectedCluster.proto_cluster_id))}) + '>Advisory</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorOwnershipSurface((selectedCluster.proto_cluster_id))}) + '>Open Ownership Surface</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorBoundarySurface((selectedCluster.proto_cluster_id))}) + '>Open Boundary Surface</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((selectedCluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((selectedCluster.proto_cluster_id))}) + '>Tensions</button>' + (relatedEvents.length ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((relatedEvents[0].event_id))}) + '>Latest Runtime Event</button>' : '') + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Task Class Evidence</strong><br><span style="color:' + corridorTaskClassColor(clusterTaskClass) + '">' + esc(String(clusterTaskClass).toLowerCase()) + '</span></div>';
    html += '<div><strong>Class Source</strong><br>' + esc(String(clusterTaskClassSource).toLowerCase()) + '</div>';
    html += '<div><strong>Readiness Approximation</strong><br><span style="color:' + corridorReadinessColor(selectedCluster.corridor_readiness) + '">' + esc(String(selectedCluster.corridor_readiness || 'UNDER_EVIDENCED').toLowerCase()) + '</span></div>';
    html += '<div><strong>Readiness Confidence</strong><br>' + esc(Math.round(Number(selectedCluster.readiness_confidence || 0) * 100) + '%') + '</div>';
    html += '<div><strong>Task Anchors</strong><br>' + esc(String((selectedCluster.task_ids || []).length)) + '</div>';
    html += '<div><strong>Linked Tensions</strong><br>' + esc(String(relatedTensions.length)) + '</div>';
    html += '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Lookup Basis Freshness</strong><br>' + esc(freshness) + '</div>';
    html += '<div><strong>Task-First Corridor Authority</strong><br>' + esc(String(authorityApproximation).toLowerCase()) + '</div>';
    html += '<div><strong>Authority Basis Freshness</strong><br>' + esc(authorityFreshness) + '</div>';
    html += '<div><strong>Unknown Tasks</strong><br>' + esc(String(selectedCluster.unknown_task_count || 0)) + '</div>';
    html += '<div><strong>Corridor Catalog</strong><br>' + esc(catalogApproximation) + '</div>';
    html += '<div><strong>Lookup Approximation</strong><br>' + esc(lookupApproximation) + '</div>';
    html += '</div>';
    html += '<div style="margin-top:10px;font-size:11px;color:var(--muted)">Resolution kind ' + esc(String(selectedCluster.resolution_kind || 'cluster')) + ' | authority and freshness stay operator-facing approximations only.</div>';
    if ((selectedCluster.task_class_basis || []).length) {
      html += '<div style="margin-top:12px"><strong>Evidence Basis</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">';
      html += (selectedCluster.task_class_basis || []).slice(0, 8).map(item => '<span class="tool-badge kind">' + esc(item) + '</span>').join('');
      html += '</div></div>';
    }
    html += '<div style="margin-top:12px;font-size:11px;color:var(--muted);line-height:1.5">Operator-facing approximation only: ' + esc(selectedCluster.summary || 'corridor readiness stays read-only in this slice and should not be treated as applied policy.') + '</div>';
    if (tasks.length) {
      html += '<div style="margin-top:12px"><strong>Task Basis</strong><div style="margin-top:6px">';
      html += tasks.slice(0, 5).map(task => {
        const taskClass = corridorTaskClassValue(task);
        const taskClassSource = corridorTaskClassSource(task);
        const taskAuthority = corridorAuthorityApproximation(task);
        const taskAuthorityFreshness = corridorAuthorityBasisFreshnessApproximation(task);
        return '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(task.title || task.task_id) + '</strong><span style="color:' + corridorTaskClassColor(taskClass) + '">' + esc(String(taskClass).toLowerCase()) + '</span></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(task.summary || 'No task-class evidence yet') + '</div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">source ' + esc(String(taskClassSource).toLowerCase()) + ' | authority ' + esc(String(taskAuthority).toLowerCase()) + ' | ' + esc(corridorFirstNonEmpty(task.corridor_lookup && task.corridor_lookup.lookup_status, task.corridor_lookup_status, 'NO_MATCH').toLowerCase()) + ' | basis ' + esc(corridorFirstNonEmpty(task.basis_updated_at, task.task_class_updated_at, 'n/a')) + ' | authority freshness ' + esc(taskAuthorityFreshness) + '</div>' +
        '</div>';
      }).join('');
      html += '</div></div>';
    }
    const fitDetail = (corridorFitDetailCache || {})[selectedCluster.proto_cluster_id || ''] || null;
    const fitCluster = (fitDetail && fitDetail.cluster) || findCorridorFitCluster(selectedCluster.proto_cluster_id || '') || null;
    if (fitCluster) {
      const dominantGap = corridorFitDominantGap(fitCluster);
      html += '<div class="msg-item" style="margin-top:12px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Corridor Boundary / Violations</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Read-only boundary diagnostics live in the dedicated corridor-fit surface below and remain separate from policy authority.</div></div><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorBoundarySurface((selectedCluster.proto_cluster_id || ''))}) + '>Open Boundary Surface</button></div>';
      html += '<div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-top:10px">';
      html += '<div><strong>Fit Status</strong><br><span style="color:' + corridorFitStatusColor(fitCluster.fit_status) + '">' + esc(String(fitCluster.fit_status || 'UNDER_EVIDENCED').toLowerCase()) + '</span></div>';
      html += '<div><strong>Catalog Range</strong><br>' + esc(corridorFirstNonEmpty(((fitCluster.catalog_range_check || {}).display_name), ((fitCluster.catalog_range_check || {}).catalog_key), 'not surfaced')) + '</div>';
      html += '<div><strong>Out-of-Range Metrics</strong><br>' + esc(String(corridorFitViolationGaps(fitCluster).length)) + '</div>';
      html += '<div><strong>Dominant Violation</strong><br>' + esc(corridorFirstNonEmpty(dominantGap && dominantGap.metric, fitCluster.fit_status === 'NEAR_BOUNDARY' ? 'boundary proximity' : 'none')) + '</div>';
      html += '<div><strong>Fit Score</strong><br>' + esc(String(fitCluster.fit_score || 0)) + '</div>';
      html += '</div>';
      html += '<div style="margin-top:10px;font-size:11px;color:var(--muted);line-height:1.5">' + esc(fitCluster.summary || 'Boundary diagnostics stay read-only and operator-facing only.') + '</div></div>';
    } else {
      html += '<div class="msg-item" style="margin-top:12px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Corridor Boundary / Violations</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Boundary diagnostics will appear once the corridor-fit read-side and selected proto-cluster detail are loaded.</div></div><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorBoundarySurface((selectedCluster.proto_cluster_id || ''))}) + '>Open Boundary Surface</button></div></div>';
    }
  }
  if (corridorReadinessSnapshotEventCache && corridorReadinessSnapshotEventCache.event_id) {
    const snapshotPayload = parseJSON(corridorReadinessSnapshotEventCache.payload_json);
    const snapshotWorkspace = snapshotPayload.workspace || {};
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Corridor Readiness Snapshot Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Persisted corridor-readiness report for replay and operator inspection.</div></div><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((corridorReadinessSnapshotEventCache.event_id))}) + '>Open Latest Corridor Snapshot</button></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Generated</strong><br>' + esc(timeAgo((snapshotPayload.generated_at || corridorReadinessSnapshotEventCache.created_at || ''))) + '</div>';
    html += '<div><strong>Ready</strong><br>' + esc(String(snapshotWorkspace.ready_count || 0)) + '</div>';
    html += '<div><strong>Borderline</strong><br>' + esc(String(snapshotWorkspace.borderline_count || 0)) + '</div>';
    html += '<div><strong>Under-Evidenced</strong><br>' + esc(String(snapshotWorkspace.under_evidenced_count || 0)) + '</div>';
    html += '<div><strong>Mixed</strong><br>' + esc(String(snapshotWorkspace.mixed_count || 0)) + '</div>';
    html += '<div><strong>Stale Lookup Basis</strong><br>' + esc(String(snapshotWorkspace.stale_basis_count || 0)) + '</div>';
    html += '</div></div>';
  }
  el.innerHTML = html;
}

async function showCorridorReadinessClusterDetail(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) {
    renderCorridorReadinessState();
    return;
  }
  try {
    const response = await rpc('workspace.instrumentation.corridor.cluster', {
      workspace_id: WS_ID,
      proto_cluster_id: clusterID
    });
    corridorReadinessDetailCache[clusterID] = response.detail || null;
  } catch (e) {
    console.error('workspace.instrumentation.corridor.cluster', e);
    corridorReadinessDetailCache[clusterID] = null;
  }
  renderCorridorReadinessState();
}

async function showCorridorFitClusterDetail(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) {
    renderCorridorReadinessState();
    return;
  }
  try {
    const response = await rpc('workspace.instrumentation.corridor.fit.cluster', {
      workspace_id: WS_ID,
      proto_cluster_id: clusterID
    });
    corridorFitDetailCache[clusterID] = response.detail || null;
  } catch (e) {
    console.error('workspace.instrumentation.corridor.fit.cluster', e);
    corridorFitDetailCache[clusterID] = null;
  }
  renderCorridorReadinessState();
}

async function loadCorridorReadiness() {
  try {
    const response = await rpc('workspace.instrumentation.corridor.report', corridorReadinessParams());
    corridorReadinessReportCache = response.report || null;
    corridorReadinessDetailCache = {};
    renderCorridorReadinessState();
    const selected = controlPolicySelectedClusterID || ((((corridorReadinessReportCache || {}).clusters || [])[0] || {}).proto_cluster_id || '');
    if (selected) await showCorridorReadinessClusterDetail(selected);
  } catch (e) {
    console.error('workspace.instrumentation.corridor.report', e);
    corridorReadinessReportCache = null;
    corridorReadinessDetailCache = {};
    renderCorridorReadinessState();
  }
}

async function loadCorridorFit() {
  try {
    const response = await rpc('workspace.instrumentation.corridor.fit.report', corridorFitParams());
    corridorFitReportCache = response.report || null;
    corridorFitDetailCache = {};
    renderCorridorReadinessState();
    const selected = controlPolicySelectedClusterID || ((((corridorFitReportCache || {}).clusters || [])[0] || {}).proto_cluster_id || '');
    if (selected) await showCorridorFitClusterDetail(selected);
  } catch (e) {
    console.error('workspace.instrumentation.corridor.fit.report', e);
    corridorFitReportCache = null;
    corridorFitDetailCache = {};
    renderCorridorReadinessState();
  }
}

async function createCorridorReadinessSnapshot() {
  const btn = document.getElementById('corridor-readiness-snapshot-btn');
  const originalText = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Recording...';
  }
  try {
    const response = await rpc('workspace.instrumentation.corridor.snapshot', corridorReadinessParams({
      proto_cluster_id: controlPolicySelectedClusterID || '',
      actor_id: 'dashboard',
      limit: 40
    }));
    corridorReadinessReportCache = response.report || corridorReadinessReportCache;
    corridorReadinessSnapshotEventCache = response.event || corridorReadinessSnapshotEventCache;
    renderCorridorReadinessState();
    if (controlPolicySelectedClusterID) await showCorridorReadinessClusterDetail(controlPolicySelectedClusterID);
    await loadRuntimeEvents();
    toast('Corridor readiness snapshot recorded');
  } catch (e) {
    console.error('workspace.instrumentation.corridor.snapshot', e);
    toast('Corridor snapshot failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Record Snapshot';
    }
  }
}

async function createCorridorFitSnapshot() {
  const btn = document.getElementById('corridor-fit-snapshot-btn');
  const originalText = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Recording...';
  }
  try {
    const response = await rpc('workspace.instrumentation.corridor.fit.snapshot', corridorFitParams({
      proto_cluster_id: controlPolicySelectedClusterID || '',
      actor_id: 'dashboard',
      limit: 40
    }));
    corridorFitReportCache = response.report || corridorFitReportCache;
    corridorFitSnapshotEventCache = response.event || corridorFitSnapshotEventCache;
    renderCorridorReadinessState();
    if (controlPolicySelectedClusterID) await showCorridorFitClusterDetail(controlPolicySelectedClusterID);
    await loadRuntimeEvents();
    toast('Corridor fit snapshot recorded');
  } catch (e) {
    console.error('workspace.instrumentation.corridor.fit.snapshot', e);
    toast('Corridor fit snapshot failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Record Fit Snapshot';
    }
  }
}

function syncControlStateSnapshotFromRuntimeEvents() {
  controlStateSnapshotEventCache = (runtimeEventsCache || [])
    .filter(item => String(item.event_type || '').toLowerCase() === 'cluster.control_state_snapshot')
    .sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at))[0] || null;
  renderControlPolicyScaffoldState();
}

function controlStateReportParams(extra = {}) {
  const params = {
    workspace_id: WS_ID,
    limit: 40
  };
  Object.keys(extra || {}).forEach(key => {
    if (extra[key] !== undefined && extra[key] !== null && extra[key] !== '') params[key] = extra[key];
  });
  return params;
}

function findControlStateCluster(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  if (!clusterID) return null;
  return ((controlStateReportCache && controlStateReportCache.clusters) || []).find(item => String(item.proto_cluster_id || '').trim() === clusterID) ||
    (((controlStateDetailCache || {})[clusterID] || {}).state) ||
    null;
}

async function showControlStateClusterDetail(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  controlPolicySelectedClusterID = clusterID;
  document.getElementById('control-policy-selected').textContent = controlPolicySelectedClusterID ? ('selected ' + controlPolicySelectedClusterID) : 'none';
  if (!clusterID) {
    renderCorridorReadinessState();
    renderCorridorAuthorityState();
    renderControlPolicyScaffoldState();
    return;
  }
  try {
    const response = await rpc('workspace.instrumentation.control.state.cluster', {
      workspace_id: WS_ID,
      proto_cluster_id: clusterID
    });
    controlStateDetailCache[clusterID] = response.detail || null;
  } catch (e) {
    console.error('workspace.instrumentation.control.state.cluster', e);
    controlStateDetailCache[clusterID] = null;
  }
  await showCorridorReadinessClusterDetail(clusterID);
  await showCorridorFitClusterDetail(clusterID);
  renderCorridorAuthorityState();
  renderControlPolicyScaffoldState();
}

function controlStateHeuristicProfile(cluster, state) {
  const detail = state || {};
  const corridor = (cluster && cluster.corridor) || {};
  return String(detail.heuristic_profile || detail.corridor_profile || corridor.heuristic_profile || corridor.profile || 'integration');
}

function controlStateStabilizedHint(state) {
  const detail = state || {};
  return String(detail.stabilized_mode_hint || detail.current_mode || 'STEADY');
}

function controlStateCandidateHint(state) {
  const detail = state || {};
  return String(detail.candidate_mode_hint || detail.candidate_mode || 'STEADY');
}

function controlStateStabilityStreak(state) {
  const detail = state || {};
  return Number(detail.stability_streak || detail.candidate_streak || 0);
}

function controlStateDominantSignal(state) {
  const detail = state || {};
  return String(detail.dominant_signal_kind || detail.dominant_violation_kind || 'none');
}

function isControlStateStabilizationEventType(eventType) {
  const normalized = String(eventType || '').toLowerCase();
  return normalized === 'cluster.control_state_stabilized' || normalized === 'cluster.control_mode_transitioned';
}

function displayRuntimeEventType(eventType) {
  if (isControlStateStabilizationEventType(eventType)) return 'cluster.control_state_stabilized';
  return String(eventType || '');
}

function renderControlPolicyScaffoldState() {
  const badge = document.getElementById('control-policy-scaffold-state');
  const el = document.getElementById('control-policy-scaffold-summary');
  if (!badge || !el) return;
  const report = controlStateReportCache;
  const items = (report && report.clusters) || [];
  const selectedID = controlPolicySelectedClusterID || ((items[0] || {}).proto_cluster_id || '');
  const selectedDetail = selectedID ? ((controlStateDetailCache || {})[selectedID] || null) : null;
  const selectedCluster = (selectedDetail && selectedDetail.state) || findControlStateCluster(selectedID);
  if (!report) {
    badge.textContent = 'no data';
    el.innerHTML = '<div class="empty">Operator-facing control-state approximation will appear once advisory evidence loads.</div>';
    return;
  }
  const workspace = report.workspace || {};
  const modeCounts = workspace.stabilized_hint_counts || workspace.mode_counts || {};
  const candidateCounts = workspace.candidate_hint_counts || workspace.candidate_counts || {};
  items.forEach(item => {
    const state = item.state || {};
    const stabilizedHint = controlStateStabilizedHint(state);
    const candidateHint = controlStateCandidateHint(state);
    modeCounts[stabilizedHint] = Number(modeCounts[stabilizedHint] || 0);
    candidateCounts[candidateHint] = Number(candidateCounts[candidateHint] || 0);
  });
  const modeSummary = Object.entries(modeCounts)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 3)
    .map(entry => entry[0] + ' ' + entry[1]);
  const candidateSummary = Object.entries(candidateCounts)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 3)
    .map(entry => entry[0] + ' ' + entry[1]);
  const topClusterID = workspace.highest_pressure_cluster_id || (items[0] && items[0].proto_cluster_id) || '';
  badge.textContent = selectedCluster ? ('approx epoch ' + String((((selectedCluster || {}).state || {}).epoch) || 0)) : 'approximation';
  let html = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px">';
  [
    ['Visible', String(items.length)],
    ['Hot', String(workspace.hot_cluster_count || 0)],
    ['Non-Steady Hints', String(workspace.non_steady_hint_count || workspace.non_steady_count || 0)],
    ['Stabilizing', String(workspace.stabilizing_count || workspace.transitioning_count || 0)],
    ['Confirmed', String(workspace.confirmed_tension_count || 0)],
    ['Pending', String(workspace.pending_tension_count || 0)]
  ].forEach(card => {
    html += '<div class="msg-item" style="margin:0"><strong>' + esc(card[0]) + '</strong><div style="margin-top:4px">' + esc(card[1]) + '</div></div>';
  });
  html += '<div class="msg-item" style="margin:0"><strong>Top Pressure</strong><div style="margin-top:4px">' + esc(String(workspace.highest_pressure_score || 0)) + '</div></div>';
  html += '</div>';
  html += '<div style="display:flex;justify-content:space-between;gap:12px;margin-top:12px;font-size:12px;color:var(--muted)">';
  html += '<span>Operator-facing approximation over persisted hysteresis state and advisory evidence. Manual tick only; no automatic state mutation.</span>';
  if (topClusterID) {
    html += '<span><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();openControlScaffold((topClusterID))}) + ' style="color:var(--accent2)">Open highest-pressure cluster</a></span>';
  } else {
    html += '<span>No control-state approximation is currently selected.</span>';
  }
  html += '</div>';
  if (modeSummary.length) {
    html += '<div style="margin-top:12px"><strong>Stabilized Hint Mix</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">';
    html += modeSummary.map(item => '<span class="tool-badge kind">' + esc(item) + '</span>').join('');
    html += '</div></div>';
  }
  if (candidateSummary.length) {
    html += '<div style="margin-top:12px"><strong>Candidate Hint Mix</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">';
    html += candidateSummary.map(item => '<span class="tool-badge kind">' + esc(item) + '</span>').join('');
    html += '</div></div>';
  }
  if (selectedCluster) {
    const state = selectedCluster.state || {};
    const heuristicProfile = controlStateHeuristicProfile(selectedCluster, state);
    const stabilizedHint = controlStateStabilizedHint(state);
    const candidateHint = controlStateCandidateHint(state);
    const stabilityStreak = controlStateStabilityStreak(state);
    const dominantSignal = controlStateDominantSignal(state);
    const dominantSignalScore = Number(state.dominant_signal_score || state.dominant_violation_score || 0);
    const hints = state.operator_hints || {};
    const deviation = state.signal_deviation_vector || state.violation_vector || {};
    const violation = deviation;
    const lastBasisAt = String(state.last_basis_at || 'n/a');
    const lastStabilizedAt = String(state.last_stabilized_at || state.last_transition_at || '').trim();
    const lastTickAt = String(state.last_tick_at || '').trim();
    const relatedEvents = ((selectedDetail && selectedDetail.events) || []).slice();
    html += '<div class="msg-item" style="margin-top:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>Selected Cluster Approximation</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(selectedCluster.proto_cluster_id || 'cluster') + ' | stabilized hint ' + esc(stabilizedHint) + ' | candidate hint ' + esc(candidateHint) + '</div></div><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((selectedCluster.proto_cluster_id))}) + '>Tensions</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorBoundarySurface((selectedCluster.proto_cluster_id))}) + '>Boundary</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((selectedCluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showControlPolicyClusterDetail((selectedCluster.proto_cluster_id))}) + '>Advisory</button>' + (relatedEvents.length ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((relatedEvents[0].event_id))}) + '>Latest Approximation Event</button>' : '') + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Observation Epoch</strong><br>' + esc(String(state.epoch || 0)) + '</div>';
    html += '<div><strong>Hint Streak</strong><br>' + esc(String(stabilityStreak)) + '</div>';
    html += '<div><strong>Dominant Signal</strong><br>' + esc(dominantSignal) + '</div>';
    html += '<div><strong>Advisory Profile Heuristic</strong><br>' + esc(heuristicProfile) + '</div>';
    html += '<div><strong>Last Basis</strong><br>' + esc(lastBasisAt) + '</div>';
    html += '</div>';
    const approximationMeta = [];
    if (lastStabilizedAt) approximationMeta.push('last stabilization ' + timeAgo(lastStabilizedAt));
    else if (lastTickAt) approximationMeta.push('last manual tick ' + timeAgo(lastTickAt));
    if (dominantSignalScore) approximationMeta.push('signal score ' + String(dominantSignalScore));
      html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Priority Focus</strong><br>' + esc(String(hints.priority_focus || 'n/a')) + '</div>';
    html += '<div><strong>Fanout Hint</strong><br>' + esc(String(hints.fanout_cap || 0)) + '</div>';
    html += '<div><strong>Review Hint</strong><br>' + esc(String(hints.review_depth || 0)) + '</div>';
    html += '<div><strong>Context Hint</strong><br>' + esc(String(hints.context_cap || 0)) + '</div>';
    html += '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Review Deviation</strong><br>' + esc(String(violation.review || 0)) + '</div>';
    html += '<div><strong>Throughput Deviation</strong><br>' + esc(String(violation.throughput || 0)) + '</div>';
    html += '<div><strong>Centralization Deviation</strong><br>' + esc(String(violation.centralization || 0)) + '</div>';
    html += '<div><strong>Novelty Gap</strong><br>' + esc(String(deviation.novelty_gap || 0)) + '</div>';
      html += '</div></div>';
    html += '<div style="margin-top:10px;font-size:11px;color:var(--muted);line-height:1.5">Operator-facing approximation only: this scaffold summarizes persisted hysteresis hints and advisory evidence, not committed policy outputs.' + (approximationMeta.length ? '<div style="margin-top:6px">' + esc(approximationMeta.join(' | ')) + '</div>' : '') + '</div></div>';
  }
  if (controlStateSnapshotEventCache && controlStateSnapshotEventCache.event_id) {
    html += '<div style="margin-top:12px"><button class="participant-btn" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((controlStateSnapshotEventCache.event_id))}) + '>Open Latest Control-State Snapshot</button></div>';
  }
  el.innerHTML = html;
}

async function loadControlStateScaffold() {
  try {
    const response = await rpc('workspace.instrumentation.control.state.report', controlStateReportParams());
    controlStateReportCache = response.report || null;
    controlStateDetailCache = {};
    renderControlPolicyScaffoldState();
    const selected = controlPolicySelectedClusterID || ((((controlStateReportCache || {}).clusters || [])[0] || {}).proto_cluster_id || '');
    if (selected) await showControlStateClusterDetail(selected);
  } catch (e) {
    console.error('workspace.instrumentation.control.state.report', e);
    controlStateReportCache = null;
    controlStateDetailCache = {};
    renderControlPolicyScaffoldState();
  }
}

async function tickControlStateScaffold() {
  const btn = document.getElementById('control-state-tick-btn');
  const originalText = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Ticking...';
  }
  try {
    const response = await rpc('workspace.instrumentation.control.state.tick', {
      workspace_id: WS_ID,
      proto_cluster_id: controlPolicySelectedClusterID || '',
      actor_id: 'dashboard'
    });
    controlStateReportCache = (response.result || {}).report || controlStateReportCache;
    controlStateDetailCache = {};
    await loadRuntimeEvents();
    await loadControlStateScaffold();
    toast('Control-state tick recorded');
  } catch (e) {
    console.error('workspace.instrumentation.control.state.tick', e);
    toast('Control-state tick failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Tick';
    }
  }
}

async function createControlStateSnapshot() {
  const btn = document.getElementById('control-state-snapshot-btn');
  const originalText = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Recording...';
  }
  try {
    const response = await rpc('workspace.instrumentation.control.state.snapshot', {
      workspace_id: WS_ID,
      proto_cluster_id: controlPolicySelectedClusterID || '',
      actor_id: 'dashboard',
      limit: 40
    });
    controlStateReportCache = response.report || controlStateReportCache;
    controlStateSnapshotEventCache = response.event || controlStateSnapshotEventCache;
    await loadRuntimeEvents();
    renderControlPolicyScaffoldState();
    toast('Control-state snapshot recorded');
  } catch (e) {
    console.error('workspace.instrumentation.control.state.snapshot', e);
    toast('Control-state snapshot failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Record Snapshot';
    }
  }
}

function renderControlPolicyDetail(item) {
  const badge = document.getElementById('control-policy-detail-state');
  const el = document.getElementById('control-policy-detail');
  if (!badge || !el) return;
  if (!item) {
    badge.textContent = 'none selected';
    el.innerHTML = '<div class="empty">Select a control cluster to inspect advisory signals, suggested controls, and recent runtime events.</div>';
    return;
  }
  if (item.error && !item.cluster && !item.proto_cluster_id) {
    badge.textContent = 'error';
    el.innerHTML = '<div class="empty">' + esc(item.error || 'Failed to load advisory cluster detail') + '</div>';
    return;
  }
  const cluster = item.cluster || item;
  const tensions = item.tensions || [];
  const relatedEvents = item.related_events || [];
  const unified = item.unified_control || null;
  const metrics = cluster.metrics || {};
  const signals = cluster.signals || {};
  const controls = cluster.suggested_controls || {};
  const advisoryControls = (unified && (unified.advisory_controls || unified.suggested_controls)) || controls || {};
  const candidateControls = (unified && unified.candidate_controls) || advisoryControls || {};
  const effectiveControls = (unified && unified.effective_controls) || candidateControls || {};
  const effectiveControlsAudit = (unified && unified.effective_controls_audit) || null;
  const band = controlPolicyMode(cluster);
  badge.textContent = band + ' / ' + String(cluster.confirmed_tension_count || 0) + ' confirmed';
  let html = '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:12px">';
  html += '<div><div style="font-size:16px;font-weight:700;color:var(--text)">' + esc(cluster.proto_cluster_id || 'control-cluster') + '</div>';
  html += '<div style="font-size:12px;color:var(--muted);margin-top:4px">Advisory report from backend proto-cluster metrics plus confirmed tensions. Advisory, candidate, and effective controls remain inspectability-only surfaces here; they do not self-apply policy.</div></div>';
  html += '<div style="display:flex;gap:8px;flex-wrap:wrap">' +
    '<button class="btn-accent" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((cluster.proto_cluster_id))}) + '>Open Tensions</button>' +
    '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>Open Proto-Cluster</button>' +
    (relatedEvents.length ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((relatedEvents[0].event_id))}) + '>Open Latest Event</button>' : '') +
  '</div></div>';
  const basisWarnings = [];
  if (cluster.metrics_missing) {
    basisWarnings.push('Proto-cluster metrics are missing from the current instrumentation window; this advisory cluster is tension-derived.');
  }
  if (cluster.basis_stale) {
    basisWarnings.push(cluster.last_tension_basis_at
      ? 'Confirmed tension basis predates the latest cluster activity; refresh tensions before treating this advisory view as current.'
      : 'Confirmed tension basis timestamp is unavailable; treat this advisory view as stale.');
  }
  if (basisWarnings.length) {
    html += '<div class="msg-item" style="margin-bottom:12px;border-color:var(--yellow)"><strong>Basis Warning</strong><div style="margin-top:6px;font-size:12px;color:var(--muted);line-height:1.5">' + basisWarnings.map(text => esc(text)).join('<br>') + '</div></div>';
  }
  html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-bottom:12px">';
  html += '<div class="msg-item" style="margin:0"><strong>Band</strong><div style="margin-top:4px;color:' + controlPolicyModeColor(band) + ';font-weight:700">' + esc(band) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Pressure</strong><div style="margin-top:4px">' + esc(String(signals.pressure_score || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Operator Focus</strong><div style="margin-top:4px">' + esc(String(controls.priority_focus || 'throughput')) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Fanout Hint</strong><div style="margin-top:4px">' + esc(String(controls.fanout_cap || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Review Hint</strong><div style="margin-top:4px">' + esc(String(controls.review_depth || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Merge Caution</strong><div style="margin-top:4px">' + esc(String(controls.merge_threshold || 0)) + '</div></div>';
  html += '</div>';
  html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-bottom:12px">';
  html += '<div class="msg-item" style="margin:0"><strong>Events</strong><div style="margin-top:4px">' + esc(String(metrics.event_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Queues</strong><div style="margin-top:4px">' + esc(String(metrics.open_queue_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Confirmed</strong><div style="margin-top:4px">' + esc(String(cluster.confirmed_tension_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Pending</strong><div style="margin-top:4px">' + esc(String(cluster.pending_tension_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Blocker Density</strong><div style="margin-top:4px">' + esc(instrumentationPercent(metrics.blocker_density || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Centralization</strong><div style="margin-top:4px">' + esc(instrumentationPercent(metrics.communication_centralization || 0)) + '</div></div>';
  html += '</div>';
  if (unified) {
    html += '<div class="msg-item" style="margin-bottom:12px">';
    html += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start"><div><strong>Unified Arbitration</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Read-side arbitration combines control-state, memory coherence, and governed hints in control-order without applying live mutation.</div></div><div style="font-size:11px;color:var(--muted)">' + esc(timeAgo(unified.generated_at || '', unified.time_authority || null)) + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:8px;margin-top:8px">';
    html += '<div><strong>Mode</strong><br>' + esc(unifiedControlModeLabel(unified.control_mode || 'n/a')) + '</div>';
    html += '<div><strong>Candidate</strong><br>' + esc(unifiedControlModeLabel(unified.candidate_mode || 'n/a')) + '</div>';
    html += '<div><strong>Coherence</strong><br>' + esc(String(unified.memory_coherence_band || 'STABLE')) + '</div>';
    html += '<div><strong>RSP Risk</strong><br>' + esc(String(unified.rsp_risk_band || 'n/a')) + ' | ' + esc(Number(unified.rsp_risk_score || 0).toFixed(2)) + '</div>';
    html += '<div><strong>Cooldown</strong><br>' + esc(unified.cooldown_active ? 'active' : 'clear') + '</div>';
    html += '<div><strong>Advisory Only</strong><br>' + esc(unified.advisory_only ? 'yes' : 'no') + '</div>';
    html += '</div>';
    html += '<div style="margin-top:10px"><strong>Capability Flags</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">' + renderControlCapabilityFlags(unified.capability_flags || {}) + '</div></div>';
    const effectiveControlState = !effectiveControlsAudit ? 'unscoped'
      : !effectiveControlsAudit.found ? 'candidate_only'
      : effectiveControlsAudit.pending ? 'pending'
      : effectiveControlsAudit.expired ? 'expired'
      : effectiveControlsAudit.live ? (effectiveControlsAudit.scope_source || 'live')
      : 'stored_not_live';
    const effectiveControlSummary = !effectiveControlsAudit ? 'No effective-control audit surfaced.'
      : !effectiveControlsAudit.found ? 'No persisted effective-controls record is active; candidate controls remain inspect-only.'
      : effectiveControlsAudit.pending ? 'Persisted effective-controls exist but are still pending activation at generated_at.'
      : effectiveControlsAudit.expired ? 'Persisted effective-controls expired; candidate controls remain inspect-only until a new live record exists.'
      : effectiveControlsAudit.scope_source === 'workspace_fallback' ? 'Live persisted controls currently resolve from workspace fallback, not a proto-cluster-local record.'
      : 'Live persisted controls currently resolve from the active proto-cluster scope.';
    html += '<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div class="msg-item" style="margin:0"><strong>Advisory Controls</strong><div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(['focus ' + String(advisoryControls.priority_focus || 'throughput'), 'fanout ' + String(advisoryControls.fanout_cap || 0), 'review ' + String(advisoryControls.review_depth || 0), 'context ' + String(advisoryControls.context_cap || 0), 'merge ' + String(advisoryControls.merge_threshold || 0)].join(' | ')) + '</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Candidate Controls</strong><div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(['focus ' + String(candidateControls.priority_focus || 'throughput'), 'fanout ' + String(candidateControls.fanout_cap || 0), 'review ' + String(candidateControls.review_depth || 0), 'context ' + String(candidateControls.context_cap || 0), 'merge ' + String(candidateControls.merge_threshold || 0)].join(' | ')) + '</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Effective Controls</strong><div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(['focus ' + String(effectiveControls.priority_focus || 'throughput'), 'fanout ' + String(effectiveControls.fanout_cap || 0), 'review ' + String(effectiveControls.review_depth || 0), 'context ' + String(effectiveControls.context_cap || 0), 'merge ' + String(effectiveControls.merge_threshold || 0)].join(' | ')) + '</div></div>';
    html += '</div>';
    html += '<div style="margin-top:10px"><strong>Effective Control State</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Scope-aware audit over persisted effective-controls resolution. This remains inspectability only and does not grant proto-cluster authority to workspace fallback.</div><div style="margin-top:6px">' + esc('state ' + effectiveControlState + ' | scope ' + String((effectiveControlsAudit && effectiveControlsAudit.scope_source) || 'none') + ' | epoch ' + String((effectiveControlsAudit && effectiveControlsAudit.epoch) || 0) + ' | actor ' + String((effectiveControlsAudit && effectiveControlsAudit.actor_id) || 'n/a')) + '</div><div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(effectiveControlSummary) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Effective Control Basis</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Per-control inspectability over the current effective controls, derived from the current delta-bearing applied-action traces; inspectability only, not a second arbiter or control authority.</div><div style="margin-top:6px">' + renderUnifiedEffectiveControlBasis(unified.effective_control_basis || []) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Cooldown Basis</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Current mode/candidate-mode cooldown context over the unified-control read-side; inspectability only, not control authority or policy proof.</div><div style="margin-top:6px">' + renderUnifiedControlCooldownBasis(unified.cooldown_basis || null) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Effective Control Basis Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Compact rollup over the current per-control basis entries; inspectability only, not control authority or policy proof.</div><div style="margin-top:6px">' + renderUnifiedControlBasisSummary(unified.effective_control_basis_summary || null) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Contradiction Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Compact rollup over the current arbitration contradiction markers; inspectability only, not policy authority or proof of correctness.</div><div style="margin-top:6px">' + renderUnifiedControlContradictionSummary(unified.contradiction_summary || null) + '</div></div>';
    html += '<div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div class="msg-item" style="margin:0"><strong>Applied Actions</strong><div style="margin-top:6px">' + renderControlActionBadgeRow((unified.applied_actions || []).map(action => unifiedControlAuditSummaryKeyLabel(action)), 'No applied actions recorded.', 'positive') + '</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Suppressed Hints</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(unified.suppressed_hints || [], 'No suppressed hints recorded.', 'warning') + '</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Contradictions</strong><div style="margin-top:6px">' + renderControlActionBadgeRow(unified.contradictions || [], 'No contradictions recorded.', 'danger') + '</div></div>';
    html += '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div class="msg-item" style="margin:0"><strong>Applied Trace</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Audit-visible structured traces over the current read-side arbitration outputs; inspectability only, not durable execution history.</div><div style="margin-top:6px">' + renderUnifiedControlActionAudit(unified.applied_action_audit || []) + '</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Suppression Trace</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Structured suppression refs over current governed-hint intake, without claiming immutable audit retention.</div><div style="margin-top:6px">' + renderUnifiedControlSuppressedHintAudit(unified.suppressed_hint_audit || []) + '</div></div>';
    html += '</div>';
    html += '<div style="margin-top:10px"><strong>Audit Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Read-side rollup over the current applied/suppressed audit traces; inspectability only, not execution history or policy authority.</div><div style="margin-top:6px">' + renderUnifiedControlAuditSummary(unified.audit_summary || null) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Trace Coverage</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Coverage rollup over the current structured audit trace fields; inspectability only, not trace completeness or authority.</div><div style="margin-top:6px">' + renderUnifiedControlAuditCoverage(unified.audit_coverage || null) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Governed Hint Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Bounded count rollups over already surfaced governed-hint fields and advisory outcomes; inspectability only.</div><div style="margin-top:6px">' + renderGovernedHintSummary(unified.governed_hint_summary || null) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Governed Hint Evidence</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Evidence that entered arbitration through governed-hint intake, including ttl, actuation class, and attached refs.</div><div style="margin-top:6px">' + renderGovernedHintEvidence(unified.governed_hints || []) + '</div></div>';
    html += '<div style="margin-top:10px"><strong>Governed Hint Outcomes</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Joined advisory intake outcomes over existing hint, applied-trace, and suppression-trace surfaces; inspectability only, not execution history.</div><div style="margin-top:6px">' + renderGovernedHintOutcomes(unified.governed_hint_outcomes || []) + '</div></div>';
    html += '</div>';
  } else if (item.unified_control_error) {
    html += '<div class="msg-item" style="margin-bottom:12px;border-color:var(--yellow)"><strong>Unified Arbitration Unavailable</strong><div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(item.unified_control_error) + '</div></div>';
  }
  [['Tasks', cluster.task_ids], ['Sessions', cluster.session_ids], ['Agents', cluster.agent_ids], ['Docs', cluster.doc_keys], ['Artifacts', cluster.artifact_refs]].forEach(section => {
    if (!section[1] || !section[1].length) return;
    html += '<div style="margin-bottom:12px"><strong>' + esc(section[0]) + '</strong><div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">';
    html += section[1].map(value => {
      if (section[0] === 'Tasks') return '<span class="tool-badge kind"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();switchTab('tasks');setTimeout(()=>showTaskDetail((value),(value)),100)}) + ' style="color:var(--accent2)">' + esc(value) + '</a></span>';
      if (section[0] === 'Sessions') return '<span class="tool-badge kind"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();openSessionFromMemory((value))}) + ' style="color:var(--accent2)">' + esc(value) + '</a></span>';
      if (section[0] === 'Docs') return '<span class="tool-badge kind"><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showDoc((value))}) + ' style="color:var(--accent2)">' + esc(value) + '</a></span>';
      return '<span class="tool-badge kind">' + esc(value) + '</span>';
    }).join('');
    html += '</div></div>';
  });
  html += '<div style="margin-bottom:12px"><strong>Active Tensions</strong><div style="margin-top:6px">';
  if (tensions.length) {
    html += tensions.map(tension =>
      '<div class="msg-item" style="margin-bottom:6px">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(tension.title || tension.tension_id) + '</strong><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showTensionDetail((tension.tension_id))}) + '>Open</button></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([tension.tension_type || 'tension', tension.review_status || '', tension.lifecycle_state || '', tension.summary || '', 'surface ' + String(tension.surface_score || 0)].filter(Boolean).join(' | ')) + '</div>' +
      '</div>'
    ).join('');
  } else {
    html += '<div class="empty">No active tensions are currently linked to this advisory control cluster.</div>';
  }
  html += '</div></div>';

  const forecasts = item.forecasts || [];
  if (forecasts.length) {
    html += '<div style="margin-bottom:12px"><strong>Predictive Layer (Damped Holt)</strong><div style="margin-top:6px">';
    html += forecasts.map(f =>
      '<div class="msg-item" style="margin-bottom:6px">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(f.metric_name) + '</strong><span style="font-family:var(--font-mono);font-size:11px">Agent: ' + esc(f.agent_id) + '</span></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px;font-family:var(--font-mono)">' +
          'L<sub>k</sub>=' + Number(f.l_k || 0).toFixed(2) + ' | ' +
          'b<sub>k</sub>=' + Number(f.b_k || 0).toFixed(2) + ' | ' +
          '&sigma;<sub>k</sub>=' + Number(f.sigma_k || 0).toFixed(2) + '<br>' +
          '&alpha;=' + Number(f.alpha_k || 0).toFixed(3) + ' | ' +
          '&beta;=' + Number(f.beta_k || 0).toFixed(3) + ' | ' +
          'y<sub>new</sub>=' + Number(f.last_y || 0).toFixed(2) +
        '</div>' +
      '</div>'
    ).join('');
    html += '</div></div>';
  }
  if (relatedEvents.length) {
    html += '<div style="margin-bottom:12px"><strong>Recent Runtime Events</strong><div style="margin-top:6px">';
    html += relatedEvents.map(event =>
      '<div class="msg-item" style="margin-bottom:6px">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(event.event_type || event.event_id) + '</strong><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((event.event_id))}) + '>Open</button></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([event.entity_type || '', event.entity_id || '', timeAgo(event.created_at)].filter(Boolean).join(' | ')) + '</div>' +
      '</div>'
    ).join('');
    html += '</div></div>';
  } else {
    html += '<div class="empty">No recent runtime events matched this advisory control cluster.</div>';
  }
  if (item.error) {
    html += '<div class="msg-item" style="margin-top:12px;border-color:var(--yellow)"><strong>Fallback Detail</strong><div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(item.error) + '</div></div>';
  }
  el.innerHTML = html;
}

async function showControlPolicyClusterDetail(protoClusterID) {
  const clusterID = String(protoClusterID || '').trim();
  const detailSeq = ++controlPolicyDetailSeq;
  controlPolicySelectedClusterID = clusterID;
  document.getElementById('control-policy-selected').textContent = controlPolicySelectedClusterID ? ('selected ' + controlPolicySelectedClusterID) : 'none';
  if (!clusterID) {
    renderControlPolicyDetail(null);
    renderCorridorReadinessState();
    renderCorridorAuthorityState();
    renderCorridorOwnershipState();
    renderControlPolicyScaffoldState();
    return;
  }
  const summaryCluster = findControlPolicyCluster(clusterID);
  let detail = (controlPolicyDetailCache || {})[clusterID] || null;
  if (!detail) {
    try {
      const response = await rpc('workspace.instrumentation.control.cluster', controlPolicyReportParams({
        proto_cluster_id: clusterID,
        limit: 20
      }));
      if (detailSeq !== controlPolicyDetailSeq || controlPolicySelectedClusterID !== clusterID) return;
      detail = response.detail || null;
      if (detail) {
        detail.forecasts = response.forecasts || [];
      }
    } catch (e) {
      if (detailSeq !== controlPolicyDetailSeq || controlPolicySelectedClusterID !== clusterID) return;
      if (!summaryCluster) {
        renderControlPolicyDetail({error: e.message || 'Failed to load advisory control cluster detail'});
        renderControlPolicyScaffoldState();
        return;
      }
      detail = {
        cluster: summaryCluster,
        tensions: relatedTensionsForProtoCluster(clusterID),
        error: e.message || 'Using cached advisory summary because cluster detail failed'
      };
    }
  }
  if (!detail && summaryCluster) {
    detail = {
      cluster: summaryCluster,
      tensions: relatedTensionsForProtoCluster(clusterID)
    };
  }
  if (detail && !detail.unified_control && !detail.unified_control_error) {
    try {
      const unifiedResponse = await rpc('workspace.instrumentation.unified.control.report', controlPolicyReportParams({
        proto_cluster_id: clusterID,
        frontier_limit: 5
      }));
      if (detailSeq !== controlPolicyDetailSeq || controlPolicySelectedClusterID !== clusterID) return;
      detail.unified_control = unifiedResponse.report || null;
    } catch (e) {
      if (detailSeq !== controlPolicyDetailSeq || controlPolicySelectedClusterID !== clusterID) return;
      detail.unified_control_error = e.message || 'Failed to load unified arbitration detail';
    }
  }
  if (detail && detail.cluster) {
    detail.related_events = controlPolicyClusterEvents(
      clusterID,
      detail.cluster.task_ids || [],
      detail.cluster.session_ids || [],
      detail.cluster.agent_ids || [],
      dedupeStrings([].concat(detail.cluster.confirmed_tension_ids || [], detail.cluster.pending_tension_ids || [], (detail.tensions || []).map(item => item.tension_id)))
    );
    controlPolicyDetailCache[clusterID] = detail;
  }
  renderControlPolicyDetail(detail);
  await showCorridorReadinessClusterDetail(clusterID);
  await showCorridorOwnershipClusterDetail(clusterID);
  await showCorridorFitClusterDetail(clusterID);
  renderCorridorAuthorityState();
  if (clusterID && !((controlStateDetailCache || {})[clusterID])) {
    await showControlStateClusterDetail(clusterID);
    return;
  }
  renderControlPolicyScaffoldState();
}

async function loadControlPolicyOverlay() {
  const loadSeq = ++controlPolicyLoadSeq;
  const selectedClusterID = controlPolicySelectedClusterID;
  const hadSelectedDetail = !!((controlPolicyDetailCache || {})[selectedClusterID] || {}).cluster;
  try {
    const responses = await Promise.allSettled([
      rpc('workspace.instrumentation.control.report', controlPolicyReportParams()),
      rpc('workspace.rsp.capability.get', {workspace_id: WS_ID}),
      rpc('workspace.rsp.belief.report', {workspace_id: WS_ID, limit: 8}),
      rpc('workspace.rsp.forecast.report', {workspace_id: WS_ID}),
      rpc('workspace.rsp.telemetry.dump', {workspace_id: WS_ID, limit: 16})
    ]);
    if (loadSeq !== controlPolicyLoadSeq) return;
    if (!responses.length || responses[0].status !== 'fulfilled') {
      throw (responses[0] && responses[0].reason) || new Error('Failed to load advisory control report');
    }
    const response = responses[0].value;
    rspCapabilityFlagsCache = responses[1] && responses[1].status === 'fulfilled' ? responses[1].value : null;
    rspBeliefReportCache = responses[2] && responses[2].status === 'fulfilled' ? responses[2].value : null;
    rspForecastReportCache = responses[3] && responses[3].status === 'fulfilled' ? responses[3].value : null;
    rspTelemetryDumpCache = responses[4] && responses[4].status === 'fulfilled' ? responses[4].value : null;
    controlPolicyReportCache = response.report || null;
    controlPolicyDetailCache = {};
    const filtered = filteredControlPolicyClusters();
    controlPolicyClustersCache = filtered.items || [];
    renderControlPolicySummary(controlPolicyReportCache, controlPolicyClustersCache);
    renderControlPolicyClusters(controlPolicyClustersCache);
    renderControlPolicySnapshotState();
    renderControlPolicyScaffoldState();
    const selectedExistsInReport = selectedClusterID && ((controlPolicyReportCache && controlPolicyReportCache.clusters) || []).some(item => String(item.proto_cluster_id || '').trim() === selectedClusterID);
    if (!selectedClusterID) {
      controlPolicySelectedClusterID = controlPolicyClustersCache.length ? controlPolicyClustersCache[0].proto_cluster_id : '';
    } else if (!selectedExistsInReport && !hadSelectedDetail) {
      controlPolicySelectedClusterID = controlPolicyClustersCache.length ? controlPolicyClustersCache[0].proto_cluster_id : '';
    } else {
      controlPolicySelectedClusterID = selectedClusterID;
    }
    await loadCorridorReadiness();
    await loadCorridorAuthority();
    await loadCorridorOwnership();
    await loadCorridorFit();
    await loadControlStateScaffold();
    await showControlPolicyClusterDetail(controlPolicySelectedClusterID);
  } catch (e) {
    if (loadSeq !== controlPolicyLoadSeq) return;
    console.error('loadControlPolicyOverlay', e);
    controlPolicyReportCache = null;
    rspCapabilityFlagsCache = null;
    rspBeliefReportCache = null;
    rspForecastReportCache = null;
    rspTelemetryDumpCache = null;
    controlPolicyClustersCache = [];
    controlPolicyDetailCache = {};
    controlPolicySelectedClusterID = '';
    document.getElementById('control-policy-generated-at').textContent = 'error';
    document.getElementById('control-policy-list-count').textContent = '0';
    document.getElementById('control-policy-summary').innerHTML = '<div class="empty">' + esc(e.message || 'Failed to load advisory control report') + '</div>';
    document.getElementById('control-policy-cluster-list').innerHTML = '<div class="empty">' + esc(e.message || 'Failed to load advisory control clusters') + '</div>';
    corridorReadinessReportCache = null;
    corridorAuthorityReportCache = null;
    corridorAuthorityDetailCache = {};
    corridorOwnershipReportCache = null;
    corridorOwnershipDetailCache = {};
    corridorFitReportCache = null;
    corridorFitDetailCache = {};
    renderControlPolicySnapshotState();
    renderCorridorReadinessState();
    renderCorridorAuthorityState();
    renderCorridorOwnershipState();
    renderControlPolicyScaffoldState();
    renderControlPolicyDetail(null);
  }
}

async function createControlPolicySnapshot() {
  const btn = document.getElementById('control-policy-snapshot-btn');
  const originalText = btn ? btn.textContent : '';
  const selectedClusterID = controlPolicySelectedClusterID;
  const hadSelectedDetail = !!((controlPolicyDetailCache || {})[selectedClusterID] || {}).cluster;
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Recording...';
  }
  try {
    const response = await rpc('workspace.instrumentation.control.snapshot', controlPolicyReportParams({
      actor_id: 'dashboard',
      limit: 50
    }));
    controlPolicyReportCache = response.report || controlPolicyReportCache;
    controlPolicySnapshotEventCache = response.event || controlPolicySnapshotEventCache;
    controlPolicyDetailCache = {};
    const filtered = filteredControlPolicyClusters();
    controlPolicyClustersCache = filtered.items || [];
    renderControlPolicySummary(controlPolicyReportCache, controlPolicyClustersCache);
    renderControlPolicyClusters(controlPolicyClustersCache);
    renderControlPolicySnapshotState();
    renderControlPolicyScaffoldState();
    const selectedExistsInReport = selectedClusterID && ((controlPolicyReportCache && controlPolicyReportCache.clusters) || []).some(item => String(item.proto_cluster_id || '').trim() === selectedClusterID);
    if (!selectedClusterID) {
      controlPolicySelectedClusterID = controlPolicyClustersCache.length ? controlPolicyClustersCache[0].proto_cluster_id : '';
    } else if (!selectedExistsInReport && !hadSelectedDetail) {
      controlPolicySelectedClusterID = controlPolicyClustersCache.length ? controlPolicyClustersCache[0].proto_cluster_id : '';
    } else {
      controlPolicySelectedClusterID = selectedClusterID;
    }
    await loadCorridorReadiness();
    await loadCorridorOwnership();
    await showControlPolicyClusterDetail(controlPolicySelectedClusterID);
    await loadRuntimeEvents();
    toast('Advisory control snapshot recorded');
  } catch (e) {
    console.error('workspace.instrumentation.control.snapshot', e);
    toast('Control snapshot failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Record Snapshot';
    }
  }
}

async function createUnifiedControlSnapshot() {
  const btn = document.getElementById('unified-control-snapshot-btn');
  const originalText = btn ? btn.textContent : '';
  const clusterID = String(controlPolicySelectedClusterID || '').trim();
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Recording...';
  }
  try {
    const response = await rpc('workspace.instrumentation.unified.control.snapshot', controlPolicyReportParams({
      actor_id: 'dashboard',
      proto_cluster_id: clusterID || undefined,
      frontier_limit: 5
    }));
    unifiedControlSnapshotEventCache = response.event || unifiedControlSnapshotEventCache;
    renderUnifiedControlSnapshotState();
    await loadRuntimeEvents();
    toast('Unified advisory snapshot recorded');
  } catch (e) {
    console.error('workspace.instrumentation.unified.control.snapshot', e);
    toast('Unified snapshot failed: ' + e.message);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Record Unified Snapshot';
    }
  }
}

async function loadRuntimeEvents() {
  try {
    const r = await rpc('workspace.events.list', {workspace_id: WS_ID, limit: 50});
    const query = String((document.getElementById('runtime-event-filter') || {}).value || '').trim().toLowerCase();
    const allItems = r.items || [];
    runtimeEventsCache = allItems;
    syncInstrumentationSnapshotFromRuntimeEvents();
    syncTensionStateFromRuntimeEvents();
    syncControlPolicySnapshotFromRuntimeEvents();
    syncUnifiedControlSnapshotFromRuntimeEvents();
    syncCorridorReadinessSnapshotFromRuntimeEvents();
    syncCorridorOwnershipSnapshotFromRuntimeEvents();
    syncCorridorFitSnapshotFromRuntimeEvents();
    syncControlStateSnapshotFromRuntimeEvents();
    await loadControlPolicyOverlay();
    await loadControlStateScaffold();
    let items = allItems;
    if (query) {
      items = items.filter(item => {
        const haystack = [
          item.event_id,
          item.event_type,
          item.entity_type,
          item.entity_id,
          item.actor_type,
          item.actor_id,
          item.agent_id,
          item.session_id,
          item.task_id,
          item.payload_json
        ].join(' ').toLowerCase();
        return haystack.includes(query);
      });
    }
    document.getElementById('events-count').textContent = items.length;
    const el = document.getElementById('runtime-events-list');
    if (!items.length) {
      el.innerHTML = '<div class="empty">No runtime events recorded.</div>';
      return;
    }
    el.innerHTML = items.map(item => {
      let titleHtml = esc(displayRuntimeEventType(item.event_type) || item.event_id);
      if (String(item.event_type || '').startsWith('rsp.motif.')) {
        titleHtml += '<span class="badge" style="background:var(--red);color:white;margin-left:8px;font-size:10px">' + esc(item.event_type.split('.').pop().toUpperCase()) + ' DETECTED</span>';
      }
      return '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((item.event_id))}) + '>' +
        '<div class="action-title">' + titleHtml + '</div>' +
        '<div class="action-meta">' +
          '<span>'+esc(item.entity_type || 'entity')+'</span>' +
          '<span>'+esc(item.entity_id || '-')+'</span>' +
          '<span>'+timeAgo(item.created_at)+'</span>' +
        '</div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc([item.actor_id || item.agent_id || '', item.session_id || '', item.task_id || ''].filter(Boolean).join(' · ') || 'No actor/session/task context')+'</div>' +
      '</div>';
    }).join('');
  } catch (e) {
    console.error('loadRuntimeEvents', e);
    runtimeEventsCache = [];
    syncInstrumentationSnapshotFromRuntimeEvents();
    syncTensionStateFromRuntimeEvents();
    syncControlPolicySnapshotFromRuntimeEvents();
    syncUnifiedControlSnapshotFromRuntimeEvents();
    syncCorridorReadinessSnapshotFromRuntimeEvents();
    syncCorridorOwnershipSnapshotFromRuntimeEvents();
    syncCorridorFitSnapshotFromRuntimeEvents();
    syncControlStateSnapshotFromRuntimeEvents();
    await loadControlPolicyOverlay();
    await loadControlStateScaffold();
    document.getElementById('events-count').textContent = '0';
    document.getElementById('runtime-events-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load runtime events')+'</div>';
  }
  loadOperatorInbox();
}

function corridorRuntimeSnapshotEventType(eventType) {
  const normalized = String(eventType || '').toLowerCase().trim();
  if (!normalized.startsWith('cluster.corridor_')) return '';
  return normalized.endsWith('_snapshot') ? normalized : '';
}

function corridorRuntimeSnapshotCountEntries(workspace = {}) {
  return Object.entries(workspace || {})
    .filter(entry => String(entry[0] || '').endsWith('_count') && Number.isFinite(Number(entry[1])))
    .sort((left, right) => {
      const leftValue = Number(left[1] || 0);
      const rightValue = Number(right[1] || 0);
      if (rightValue !== leftValue) return rightValue - leftValue;
      return String(left[0] || '').localeCompare(String(right[0] || ''));
    })
    .slice(0, 6);
}

function corridorRuntimeSnapshotLabel(key) {
  return String(key || '')
    .replace(/_count$/, '')
    .split('_')
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function renderGenericCorridorSnapshotDetail(item, payloadObj) {
  const workspace = payloadObj.workspace || {};
  const clusters = Array.isArray(payloadObj.clusters) ? payloadObj.clusters : [];
  const countEntries = corridorRuntimeSnapshotCountEntries(workspace);
  const typedEvent = String(payloadObj.typed_event_type || item.event_type || 'CORRIDOR_SNAPSHOT');
  const subtype = typedEvent
    .replace(/^CORRIDOR_/, '')
    .replace(/_SNAPSHOT$/, '')
    .replace(/_/g, ' ')
    .toLowerCase()
    .trim() || 'corridor';
  let html = '<div style="margin-bottom:12px"><strong>Corridor Snapshot Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Read-only ' + esc(subtype) + ' corridor snapshot payload mirrored from the persisted runtime event.</div><div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:6px">';
  html += '<div class="msg-item" style="margin:0"><strong>Generated</strong><div style="margin-top:4px">' + esc(timeAgo(payloadObj.generated_at || item.created_at)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Captured Clusters</strong><div style="margin-top:4px">' + esc(String(clusters.length || payloadObj.captured_cluster_count || 0)) + '</div></div>';
  html += '<div class="msg-item" style="margin:0"><strong>Source Clusters</strong><div style="margin-top:4px">' + esc(String(payloadObj.source_cluster_count || clusters.length || 0)) + '</div></div>';
  countEntries.forEach(entry => {
    html += '<div class="msg-item" style="margin:0"><strong>' + esc(corridorRuntimeSnapshotLabel(entry[0])) + '</strong><div style="margin-top:4px">' + esc(String(entry[1] || 0)) + '</div></div>';
  });
  html += '</div></div>';
  if (clusters.length) {
    html += '<div style="margin-bottom:12px"><strong>Captured Clusters</strong><div style="margin-top:6px">';
    html += clusters.map(cluster =>
      '<div class="msg-item" style="margin-bottom:6px">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(cluster.proto_cluster_id || 'corridor-cluster') + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorSurface((cluster.proto_cluster_id))}) + '>Corridor</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((cluster.proto_cluster_id))}) + '>Control</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((cluster.proto_cluster_id))}) + '>Tensions</button></div></div>' +
        '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([cluster.resolution_kind || 'entity', cluster.summary || payloadObj.summary || 'Read-only corridor snapshot payload'].filter(Boolean).join(' | ')) + '</div>' +
      '</div>'
    ).join('');
    html += '</div></div>';
  }
  return html;
}

function showRuntimeEventDetail(eventId) {
  const item = runtimeEventsCache.find(x => x.event_id === eventId);
  if (!item) return;
  let payload = item.payload_json || '';
  const payloadObj = payload ? parseJSON(payload) : {};
  let prettyPayload = payload;
  if (payload) {
    try {
      prettyPayload = JSON.stringify(JSON.parse(payload), null, 2);
    } catch (e) {
      prettyPayload = payload;
    }
  }
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Event Type</strong><br>'+esc(item.event_type || '')+'</div>';
  html += '<div><strong>Created</strong><br>'+esc(timeAgo(item.created_at))+'</div>';
  if (item.pack_mode) html += '<div><strong>Pack Mode</strong><br>'+esc(String(item.pack_mode).toLowerCase())+'</div>';
  if (item.task_id) html += '<div><strong>Task</strong><br>'+esc(item.task_id)+'</div>';
  html += '<div><strong>Entity</strong><br>'+esc(item.entity_type || 'entity')+' / '+esc(item.entity_id || '-')+'</div>';
  html += '<div><strong>Actor</strong><br>'+esc(item.actor_type || '-')+' / '+esc(item.actor_id || item.agent_id || '-')+'</div>';
  if (item.session_id) html += '<div><strong>Session</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();openSessionFromMemory((item.session_id))}) + ' style="color:var(--accent)">'+esc(item.session_id)+'</a></div>';
  if (item.task_id) html += '<div><strong>Task</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tasks');setTimeout(()=>showTaskDetail((item.task_id),(item.task_id)),100)}) + ' style="color:var(--accent)">'+esc(item.task_id)+'</a></div>';
  html += '</div>';
  const linkedTensionID = String((payloadObj && payloadObj.tension_id) || (item.entity_type === 'tension' ? item.entity_id : '') || '').trim();
  const relatedTensions = relatedTensionsForRuntimeEvent(item, payloadObj);
  const relatedSegments = corridorSegmentEntries(
    payloadObj && payloadObj.segment_refs,
    payloadObj && payloadObj.segments,
    payloadObj && payloadObj.doc_segment_refs,
    payloadObj && payloadObj.doc_segments,
    payloadObj && payloadObj.artifact_segment_refs,
    payloadObj && payloadObj.artifact_segments,
    payloadObj && payloadObj.docs,
    payloadObj && payloadObj.artifacts
  );
  if (linkedTensionID || relatedTensions.length || (payloadObj && payloadObj.proto_cluster_id)) {
    html += '<div style="margin-bottom:12px"><strong>Related Tensions</strong><div style="margin-top:6px">';
    if (linkedTensionID) {
      html += '<div style="display:flex;gap:8px;align-items:center;margin-bottom:8px"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionFromRuntimeEvent((linkedTensionID))}) + '>Open Direct Tension</button>' + ((payloadObj && payloadObj.proto_cluster_id) ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorSurface((payloadObj.proto_cluster_id))}) + '>Open Corridor Surface</button>' : '') + '</div>';
    } else if (payloadObj && payloadObj.proto_cluster_id) {
      html += '<div style="display:flex;gap:8px;align-items:center;margin-bottom:8px"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorSurface((payloadObj.proto_cluster_id))}) + '>Open Corridor Surface</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((payloadObj.proto_cluster_id))}) + '>Open Cluster Tensions</button></div>';
    }
    html += renderTensionLinkList(relatedTensions, 'No related tensions are currently cached for this event.');
    html += '</div></div>';
  }
  if (relatedSegments.length) {
    html += '<div style="margin-bottom:12px"><strong>Related Segments</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Read-only artifact/doc segment context for operator inspection only.</div><div style="margin-top:6px">' + renderSegmentBadgeRow('Segments', relatedSegments) + '</div></div>';
  }
  if (String(item.event_type || '').toLowerCase() === 'cluster.metric_snapshot' && payloadObj && Object.keys(payloadObj).length) {
    const workspace = payloadObj.workspace || {};
    const replay = payloadObj.replay || {};
    const clusters = payloadObj.clusters || [];
    html += '<div style="margin-bottom:12px"><strong>Snapshot Summary</strong><div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:6px">';
    html += '<div class="msg-item" style="margin:0"><strong>Generated</strong><div style="margin-top:4px">'+esc(timeAgo(payloadObj.generated_at))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Clusters</strong><div style="margin-top:4px">'+esc(String(workspace.total_clusters || clusters.length || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Total Events</strong><div style="margin-top:4px">'+esc(String(replay.total_events || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Blocked</strong><div style="margin-top:4px">'+esc(String(workspace.blocked_cluster_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Duplicate-Prone</strong><div style="margin-top:4px">'+esc(String(workspace.duplicate_prone_cluster_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Top Agent</strong><div style="margin-top:4px">'+esc(workspace.top_agent_by_activity || 'n/a')+' | '+esc(instrumentationPercent(workspace.top_agent_activity_share || 0))+'</div></div>';
    html += '</div></div>';
    if (clusters.length) {
      html += '<div style="margin-bottom:12px"><strong>Captured Clusters</strong><div style="margin-top:6px">';
      html += clusters.map(cluster => {
        const metrics = cluster.metrics || {};
        return '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(cluster.proto_cluster_id || 'proto-cluster') + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((cluster.proto_cluster_id))}) + '>Control</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((cluster.proto_cluster_id))}) + '>Tensions</button></div></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc((cluster.resolution_kind || 'entity') + ' | ' + String(metrics.event_count || 0) + ' events | blockers ' + String(metrics.blocker_signal_count || 0) + ' | dup ' + instrumentationPercent(metrics.duplication_index || 0)) + '</div>' +
        '</div>';
      }).join('');
      html += '</div></div>';
    }
  }
  if (String(item.event_type || '').toLowerCase() === 'cluster.control_advisory_snapshot' && payloadObj && Object.keys(payloadObj).length) {
    const workspace = payloadObj.workspace || {};
    const clusters = payloadObj.clusters || [];
    html += '<div style="margin-bottom:12px"><strong>Advisory Snapshot Summary</strong><div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:6px">';
    html += '<div class="msg-item" style="margin:0"><strong>Generated</strong><div style="margin-top:4px">'+esc(timeAgo(payloadObj.generated_at))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Hot</strong><div style="margin-top:4px">'+esc(String(workspace.hot_cluster_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Attention</strong><div style="margin-top:4px">'+esc(String(workspace.attention_cluster_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Confirmed</strong><div style="margin-top:4px">'+esc(String(workspace.confirmed_tension_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Pending</strong><div style="margin-top:4px">'+esc(String(workspace.pending_tension_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Pressure</strong><div style="margin-top:4px">'+esc(String(workspace.highest_pressure_score || 0))+'</div></div>';
    html += '</div></div>';
    if (clusters.length) {
      html += '<div style="margin-bottom:12px"><strong>Captured Clusters</strong><div style="margin-top:6px">';
      html += clusters.map(cluster =>
        '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(cluster.proto_cluster_id || 'control-cluster') + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((cluster.proto_cluster_id))}) + '>Control</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((cluster.proto_cluster_id))}) + '>Tensions</button></div></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([cluster.resolution_kind || 'entity', ((cluster.signals || {}).attention_band || ''), (((cluster.suggested_controls || {}).priority_focus) || '')].filter(Boolean).join(' | ')) + '</div>' +
        '</div>'
      ).join('');
      html += '</div></div>';
    }
  }
  if ((String(item.event_type || '').toLowerCase() === 'cluster.unified_control_advisory_snapshot' || String(item.event_type || '').toLowerCase() === 'cluster.unified_control_effective_snapshot') && payloadObj && Object.keys(payloadObj).length) {
    const report = payloadObj.report || {};
    const snapshotTimeAuthority = report.time_authority || null;
    html += '<div style="margin-bottom:12px"><strong>Unified Control Snapshot Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Bounded advisory/effective unified-control snapshot mirrored from the persisted runtime event; inspectability only, not a second arbiter, execution history, or rollback authority.</div><div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:6px">';
    html += '<div class="msg-item" style="margin:0"><strong>Generated</strong><div style="margin-top:4px">'+esc(timeAgo(report.generated_at || item.created_at || '', snapshotTimeAuthority))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Scope</strong><div style="margin-top:4px">'+esc(String(report.proto_cluster_id || payloadObj.workspace_id || item.workspace_id || 'workspace'))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Mode</strong><div style="margin-top:4px">'+esc(unifiedControlModeLabel(report.control_mode || report.candidate_mode || 'n/a'))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Risk</strong><div style="margin-top:4px">'+esc(String(report.rsp_risk_band || 'n/a'))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Hints</strong><div style="margin-top:4px">'+esc(String(payloadObj.governed_hint_count || (Array.isArray(report.governed_hints) ? report.governed_hints.length : 0) || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Applied</strong><div style="margin-top:4px">'+esc(String(payloadObj.applied_action_count || (Array.isArray(report.applied_actions) ? report.applied_actions.length : 0) || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Suppressed</strong><div style="margin-top:4px">'+esc(String(payloadObj.suppressed_hint_count || (Array.isArray(report.suppressed_hints) ? report.suppressed_hints.length : 0) || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Outcomes</strong><div style="margin-top:4px">'+esc(String(payloadObj.governed_hint_outcome_count || (Array.isArray(report.governed_hint_outcomes) ? report.governed_hint_outcomes.length : 0) || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Basis Fields</strong><div style="margin-top:4px">'+esc(String(payloadObj.effective_control_basis_field_count || ((report.effective_control_basis_summary || {}).field_count) || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Changed Fields</strong><div style="margin-top:4px">'+esc(String(payloadObj.effective_control_basis_changed_count || ((report.effective_control_basis_summary || {}).changed_field_count) || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Contradictions</strong><div style="margin-top:4px">'+esc(String(payloadObj.contradiction_count || ((report.contradiction_summary || {}).total_count) || 0))+'</div></div>';
    html += '</div></div>';
    if (payloadObj.summary || report.summary) {
      html += '<div style="margin-bottom:12px"><strong>Snapshot Summary</strong><div class="msg-item" style="margin-top:6px">' + esc(payloadObj.summary || report.summary || '') + '</div></div>';
    }
    html += '<div style="margin-bottom:12px"><strong>Cooldown Basis</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Current mode/candidate-mode cooldown context captured in this advisory snapshot; inspectability only.</div><div style="margin-top:6px">' + renderUnifiedControlCooldownBasis(report.cooldown_basis || null) + '</div></div>';
    html += '<div style="margin-bottom:12px"><strong>Effective Control Basis Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Compact rollup over the current per-control basis entries captured in this advisory snapshot; inspectability only.</div><div style="margin-top:6px">' + renderUnifiedControlBasisSummary(report.effective_control_basis_summary || null) + '</div></div>';
    html += '<div style="margin-bottom:12px"><strong>Contradiction Summary</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Compact rollup over the current contradiction markers captured in this advisory snapshot; inspectability only.</div><div style="margin-top:6px">' + renderUnifiedControlContradictionSummary(report.contradiction_summary || null) + '</div></div>';
  }
  if (String(item.event_type || '').toLowerCase() === 'cluster.corridor_readiness_snapshot' && payloadObj && Object.keys(payloadObj).length) {
    const workspace = payloadObj.workspace || {};
    const clusters = payloadObj.clusters || [];
    html += '<div style="margin-bottom:12px"><strong>Corridor Readiness Snapshot Summary</strong><div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:6px">';
    html += '<div class="msg-item" style="margin:0"><strong>Generated</strong><div style="margin-top:4px">'+esc(timeAgo(payloadObj.generated_at || item.created_at))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Ready</strong><div style="margin-top:4px">'+esc(String(workspace.ready_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Borderline</strong><div style="margin-top:4px">'+esc(String(workspace.borderline_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Under-Evidenced</strong><div style="margin-top:4px">'+esc(String(workspace.under_evidenced_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Mixed</strong><div style="margin-top:4px">'+esc(String(workspace.mixed_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Stale Lookup Basis</strong><div style="margin-top:4px">'+esc(String(workspace.stale_basis_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Dominant Task Class</strong><div style="margin-top:4px">'+esc(String(workspace.dominant_task_class || 'unknown').toLowerCase())+' | '+esc(String(workspace.dominant_task_class_hits || 0))+'</div></div>';
    html += '</div></div>';
    if (clusters.length) {
      html += '<div style="margin-bottom:12px"><strong>Captured Clusters</strong><div style="margin-top:6px">';
      html += clusters.map(cluster => {
        const authorityApproximation = corridorAuthorityApproximation(cluster);
        const authorityFreshness = corridorAuthorityBasisFreshnessApproximation(cluster);
        return '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(cluster.proto_cluster_id || 'corridor-cluster') + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorSurface((cluster.proto_cluster_id))}) + '>Corridor</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((cluster.proto_cluster_id))}) + '>Control</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((cluster.proto_cluster_id))}) + '>Tensions</button></div></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([cluster.resolution_kind || 'entity', 'task class ' + String(corridorTaskClassValue(cluster)).toLowerCase(), 'source ' + String(corridorTaskClassSource(cluster)).toLowerCase(), 'authority ' + String(authorityApproximation).toLowerCase(), 'authority basis ' + authorityFreshness, 'catalog ' + corridorCatalogApproximation(cluster), 'lookup ' + corridorLookupApproximation(cluster), 'readiness ' + String(cluster.corridor_readiness || 'UNDER_EVIDENCED').toLowerCase(), cluster.summary || ''].filter(Boolean).join(' | ')) + '</div>' +
        '</div>';
      }).join('');
      html += '</div></div>';
    }
  }
  if (String(item.event_type || '').toLowerCase() === 'cluster.corridor_fit_snapshot' && payloadObj && Object.keys(payloadObj).length) {
    const workspace = payloadObj.workspace || {};
    const clusters = payloadObj.clusters || [];
    html += '<div style="margin-bottom:12px"><strong>Corridor Fit Snapshot Summary</strong><div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:6px">';
    html += '<div class="msg-item" style="margin:0"><strong>Generated</strong><div style="margin-top:4px">'+esc(timeAgo(payloadObj.generated_at || item.created_at))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>In Corridor</strong><div style="margin-top:4px">'+esc(String(workspace.in_corridor_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Near Boundary</strong><div style="margin-top:4px">'+esc(String(workspace.near_boundary_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Out of Corridor</strong><div style="margin-top:4px">'+esc(String(workspace.out_of_corridor_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Under-Evidenced</strong><div style="margin-top:4px">'+esc(String(workspace.under_evidenced_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Stale Basis</strong><div style="margin-top:4px">'+esc(String(workspace.stale_basis_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Dominant Catalog</strong><div style="margin-top:4px">'+esc(String(workspace.dominant_catalog_key || 'none').toLowerCase())+' | '+esc(String(workspace.dominant_catalog_key_hits || 0))+'</div></div>';
    html += '</div></div>';
    if (clusters.length) {
      html += '<div style="margin-bottom:12px"><strong>Captured Clusters</strong><div style="margin-top:6px">';
      html += clusters.map(cluster =>
        '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(cluster.proto_cluster_id || 'corridor-fit-cluster') + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorSurface((cluster.proto_cluster_id))}) + '>Corridor</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openCorridorBoundarySurface((cluster.proto_cluster_id))}) + '>Boundary</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((cluster.proto_cluster_id))}) + '>Control</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((cluster.proto_cluster_id))}) + '>Tensions</button></div></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([cluster.resolution_kind || 'entity', 'fit ' + String(cluster.fit_status || 'UNDER_EVIDENCED').toLowerCase(), 'catalog ' + corridorFirstNonEmpty((cluster.catalog_range_check || {}).catalog_key, (cluster.corridor_lookup || {}).catalog_key, 'none'), cluster.summary || ''].filter(Boolean).join(' | ')) + '</div>' +
        '</div>'
      ).join('');
      html += '</div></div>';
    }
  }
  const genericCorridorSnapshotType = corridorRuntimeSnapshotEventType(item.event_type);
  if (genericCorridorSnapshotType &&
      genericCorridorSnapshotType !== 'cluster.corridor_readiness_snapshot' &&
      genericCorridorSnapshotType !== 'cluster.corridor_fit_snapshot' &&
      payloadObj && Object.keys(payloadObj).length) {
    html += renderGenericCorridorSnapshotDetail(item, payloadObj);
  }
  if (String(item.event_type || '').toLowerCase() === 'cluster.control_state_snapshot' && payloadObj && Object.keys(payloadObj).length) {
    const workspace = payloadObj.workspace || {};
    const clusters = payloadObj.clusters || [];
    html += '<div style="margin-bottom:12px"><strong>Control-State Snapshot Summary</strong><div style="display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:6px">';
    html += '<div class="msg-item" style="margin:0"><strong>Generated</strong><div style="margin-top:4px">'+esc(timeAgo(payloadObj.generated_at))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Hot</strong><div style="margin-top:4px">'+esc(String(workspace.hot_cluster_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Non-Steady Hints</strong><div style="margin-top:4px">'+esc(String(workspace.non_steady_hint_count || workspace.non_steady_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Stabilizing</strong><div style="margin-top:4px">'+esc(String(workspace.stabilizing_count || workspace.transitioning_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Confirmed</strong><div style="margin-top:4px">'+esc(String(workspace.confirmed_tension_count || 0))+'</div></div>';
    html += '<div class="msg-item" style="margin:0"><strong>Pending</strong><div style="margin-top:4px">'+esc(String(workspace.pending_tension_count || 0))+'</div></div>';
    html += '</div></div>';
    if (clusters.length) {
      html += '<div style="margin-bottom:12px"><strong>Captured Clusters</strong><div style="margin-top:6px">';
      html += clusters.map(cluster =>
        '<div class="msg-item" style="margin-bottom:6px">' +
          '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>' + esc(cluster.proto_cluster_id || 'control-state-cluster') + '</strong><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((cluster.proto_cluster_id))}) + '>Control</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((cluster.proto_cluster_id))}) + '>Proto-Cluster</button><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((cluster.proto_cluster_id))}) + '>Tensions</button></div></div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc([cluster.resolution_kind || 'entity', 'stabilized hint ' + controlStateStabilizedHint(cluster.state || {}), 'candidate hint ' + controlStateCandidateHint(cluster.state || {}), cluster.summary || ''].filter(Boolean).join(' | ')) + '</div>' +
        '</div>'
      ).join('');
      html += '</div></div>';
    }
  }
  if ((String(item.event_type || '').toLowerCase() === 'cluster.control_state_ticked' || isControlStateStabilizationEventType(item.event_type)) && payloadObj && payloadObj.proto_cluster_id) {
    html += '<div style="margin-bottom:12px"><strong>Stabilization Links</strong><div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:6px">';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openControlScaffold((payloadObj.proto_cluster_id))}) + '>Open Scaffold</button>';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((payloadObj.proto_cluster_id))}) + '>Proto-Cluster</button>';
    html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((payloadObj.proto_cluster_id))}) + '>Tensions</button>';
    html += '</div></div>';
  }
  html += '<div style="margin-bottom:12px"><strong>Payload</strong><pre>'+esc(prettyPayload || 'No payload recorded.')+'</pre></div>';
  openModal('Event ' + esc(displayRuntimeEventType(item.event_type) || item.event_id), html);
}

async function loadCompaction() {
  try {
    const [candidateResp, snapshotResp] = await Promise.all([
      rpc('workspace.compaction.candidates', {workspace_id: WS_ID, limit: 20}),
      rpc('workspace.compaction.snapshots', {workspace_id: WS_ID, limit: 20})
    ]);
    compactionCandidatesCache = candidateResp.items || [];
    compactionSnapshotsCache = snapshotResp.items || [];
    document.getElementById('compaction-candidate-count').textContent = compactionCandidatesCache.length + ' candidates';
    document.getElementById('compaction-snapshot-count').textContent = compactionSnapshotsCache.length + ' snapshots';

    const candidateEl = document.getElementById('compaction-candidates-list');
    if (!compactionCandidatesCache.length) {
      candidateEl.innerHTML = '<div class="empty">No sessions currently exceed compaction thresholds.</div>';
    } else {
      candidateEl.innerHTML = compactionCandidatesCache.map(item =>
        '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showSessionDetail((item.session_id))}) + '>' +
          '<div class="action-title">'+esc(item.agent_id || 'agent')+' · '+esc(item.session_id)+'</div>' +
          '<div class="action-meta">' +
            '<span class="action-status '+esc(item.status || 'ACTIVE')+'">'+esc(item.status || 'ACTIVE')+'</span>' +
            '<span>'+esc(String(item.message_count || 0))+' msgs</span>' +
            '<span>'+esc(String(item.total_tokens || item.message_tokens || 0))+' tok</span>' +
          '</div>' +
          '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc((item.task_id ? ('Task ' + item.task_id + ' · ') : '') + 'Last message ' + timeAgo(item.last_message_at || item.started_at))+'</div>' +
        '</div>'
      ).join('');
    }

    const snapshotEl = document.getElementById('compaction-snapshots-list');
    if (!compactionSnapshotsCache.length) {
      snapshotEl.innerHTML = '<div class="empty">No compaction snapshots recorded yet.</div>';
    } else {
      snapshotEl.innerHTML = compactionSnapshotsCache.map(item =>
        '<div class="action-card" ' + dashboardAction(function(dashboardEvent){showCompactionSnapshotDetail((item.snapshot_id))}) + '>' +
          '<div class="action-title">'+esc(item.agent_id || 'agent')+' · '+esc(item.trigger_kind || 'compaction')+'</div>' +
          '<div class="action-meta">' +
            '<span>'+esc(item.session_id || '-')+'</span>' +
            '<span>'+esc(String(item.total_tokens || 0))+' tok</span>' +
            '<span>'+esc(String(item.pack_mode || 'complete').toLowerCase())+'</span>' +
            '<span>'+timeAgo(item.created_at)+'</span>' +
          '</div>' +
          (item.summary_text ? '<div style="font-size:11px;color:var(--muted);margin-top:6px">'+esc(item.summary_text.length > 120 ? item.summary_text.substring(0, 120) + '...' : item.summary_text)+'</div>' : '') +
        '</div>'
      ).join('');
    }
  } catch (e) {
    console.error('loadCompaction', e);
    document.getElementById('compaction-candidates-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load compaction data')+'</div>';
    document.getElementById('compaction-snapshots-list').innerHTML = '<div class="empty">'+esc(e.message || 'Failed to load compaction data')+'</div>';
  }
  loadOperatorInbox();
}

function showCompactionSnapshotDetail(snapshotId) {
  const item = compactionSnapshotsCache.find(x => x.snapshot_id === snapshotId);
  if (!item) return;
  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
  html += '<div><strong>Trigger</strong><br>'+esc(item.trigger_kind || 'compaction')+'</div>';
  html += '<div><strong>Token Budget</strong><br>'+esc(String(item.token_budget || 0))+'</div>';
  html += '<div><strong>Message Count</strong><br>'+esc(String(item.message_count_before || 0))+' → '+esc(String(item.message_count_after || 0))+'</div>';
  html += '<div><strong>Message Tokens</strong><br>'+esc(String(item.message_tokens_before || 0))+' → '+esc(String(item.message_tokens_after || 0))+'</div>';
  html += '<div><strong>Total Tokens</strong><br>'+esc(String(item.total_tokens || 0))+'</div>';
  html += '<div><strong>Created</strong><br>'+esc(timeAgo(item.created_at))+'</div>';
  if (item.session_id) html += '<div><strong>Session</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();openSessionFromMemory((item.session_id))}) + ' style="color:var(--accent)">'+esc(item.session_id)+'</a></div>';
  if (item.summary_workspace_memory) html += '<div><strong>Summary Memory</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showMemoryDetail((item.summary_workspace_memory))}) + ' style="color:var(--accent)">'+esc(item.summary_workspace_memory)+'</a></div>';
  if (item.episode_pack_id) html += '<div><strong>Episode Pack</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();showEpisodePackDetail((item.episode_pack_id))}) + ' style="color:var(--accent)">'+esc(item.episode_pack_id)+'</a></div>';
  html += '</div>';
  if (item.summary_text) {
    html += '<div style="margin-bottom:12px"><strong>Summary Text</strong><pre>'+esc(item.summary_text)+'</pre></div>';
  }
  openModal('Compaction ' + esc(item.snapshot_id), html);
}

async function showEpisodePackDetail(packId) {
  openModal('Episode Pack ' + esc(packId), '<div class="empty">Loading...</div>');
  try {
    const item = await rpc('workspace.episode.pack.get', {workspace_id: WS_ID, pack_id: packId});
    let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
    html += '<div><strong>Type</strong><br>'+esc(String(item.pack_type || 'compaction').toLowerCase())+'</div>';
    html += '<div><strong>Mode</strong><br>'+esc(String(item.pack_mode || 'complete').toLowerCase())+'</div>';
    html += '<div><strong>Session</strong><br>'+esc(item.session_id || '-')+'</div>';
    html += '<div><strong>Agent</strong><br>'+esc(item.agent_id || '-')+'</div>';
    html += '<div><strong>Trigger</strong><br>'+esc(item.trigger_kind || '-')+'</div>';
    html += '<div><strong>Created</strong><br>'+esc(timeAgo(item.created_at))+'</div>';
    if (item.task_id) html += '<div><strong>Task</strong><br>'+esc(item.task_id)+'</div>';
    if (item.lineage_session_id && item.lineage_session_id !== item.session_id) html += '<div><strong>Lineage Session</strong><br>'+esc(item.lineage_session_id)+'</div>';
    if (item.compaction_snapshot_id) html += '<div><strong>Snapshot</strong><br>'+esc(item.compaction_snapshot_id)+'</div>';
    if (item.lifecycle_event_id) html += '<div><strong>Lifecycle Event</strong><br>'+esc(item.lifecycle_event_id)+'</div>';
    if (item.summary_workspace_memory) html += '<div><strong>Summary Memory</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showMemoryDetail((item.summary_workspace_memory))}) + ' style="color:var(--accent)">'+esc(item.summary_workspace_memory)+'</a></div>';
    if (item.canonical_memory_id) html += '<div><strong>Canonical Memory Node</strong><br>'+esc(item.canonical_memory_id)+'</div>';
    html += '</div>';
    if (item.narrative_summary) html += '<div style="margin-bottom:12px"><strong>Narrative Summary</strong><pre>'+esc(item.narrative_summary)+'</pre></div>';
    if (item.open_loops && item.open_loops.length) html += '<div style="margin-bottom:12px"><strong>Open Loops</strong><pre>'+esc(item.open_loops.join('\n'))+'</pre></div>';
    if (item.failure_repair_chain && item.failure_repair_chain.length) html += '<div style="margin-bottom:12px"><strong>Failure / Repair</strong><pre>'+esc(item.failure_repair_chain.join('\n'))+'</pre></div>';
    if (item.provenance_refs && item.provenance_refs.length) html += '<div style="margin-bottom:12px"><strong>Provenance Refs</strong><pre>'+esc(item.provenance_refs.join('\n'))+'</pre></div>';
    openModal('Episode Pack ' + esc(packId), html);
  } catch (e) {
    openModal('Episode Pack ' + esc(packId), '<div class="empty">'+esc(e.message || 'Failed to load episode pack')+'</div>');
  }
}

async function loadTasks() {
  try {
    const r = await rpc('workspace.tasks.list', {workspace_id:WS_ID});
    const tasks = r.tasks || [];
    _cachedTasks = tasks;
    updateGraphTaskFocusOptions();
    document.getElementById('tasks-count').textContent = tasks.length;
    document.getElementById('s-tasks').textContent = tasks.length;
    const cols = {PENDING:[],CLAIMED:[],BLOCKED:[],COMPLETED:[]};
    const cancelled = [];
    tasks.forEach(t => {
      let s = (t.claim_status || t.status || 'PENDING').toUpperCase();
      if (s==='RELEASED') s='PENDING';
      if (s==='CANCELLED' || s==='FAILED') { cancelled.push(t); return; }
      if (!cols[s]) s='PENDING';
      cols[s].push(t);
    });
    ['pending','claimed','blocked','completed'].forEach(s => {
      const items = cols[s.toUpperCase()]||[];
      document.getElementById('cnt-'+s).textContent = '('+items.length+')';
      const el = document.getElementById('col-'+s);
      if (!items.length) { el.innerHTML='<div class="empty">—</div>'; return; }
      el.innerHTML = items.map(t => {
        const tags = t.tags||[];
        const hasTaskClassEvidence = !!corridorFirstNonEmpty(t.task_class, t.task_class_hint);
        const taskClass = corridorTaskClassValue(t);
        const taskClassSource = corridorTaskClassSource(t);
        const who = t.claim_agent_id ? '→ '+t.claim_agent_id : t.owner_user_id||'';
        return '<div class="task-card" data-project="'+esc(t.project_id||'')+'">'+
          '<div class="task-title">'+esc(t.title||t.task_id)+'</div>' +
          '<div class="task-meta"><span class="priority-'+esc(t.priority||'')+'">'+esc(t.priority||'')+'</span> · '+esc(who)+'</div>' +
          (hasTaskClassEvidence ? '<div class="task-meta">class '+esc(String(taskClass).toLowerCase())+' | source '+esc(String(taskClassSource).toLowerCase())+'</div>' : '') +
          (tags.length?'<div class="task-tags">'+tags.map(tg=>'<span class="task-tag">'+esc(tg)+'</span>').join('')+'</div>':'') +
        '</div>';
      }).join('');
      bindTaskDetailElements(el, items, '.task-card');
    });
    // Cancelled section
    const cancelledSection = document.getElementById('cancelled-section');
    const cancelledList = document.getElementById('cancelled-list');
    if (cancelled.length) {
      cancelledSection.style.display = '';
      document.getElementById('cnt-cancelled').textContent = cancelled.length;
      cancelledList.innerHTML = cancelled.map(t => {
        const st = (t.claim_status || t.status || '').toUpperCase();
        const reason = t.close_reason || '';
        const hasTaskClassEvidence = !!corridorFirstNonEmpty(t.task_class, t.task_class_hint);
        const taskClass = corridorTaskClassValue(t);
        const taskClassSource = corridorTaskClassSource(t);
        return '<div class="cancelled-item">'+
          '<div style="flex:1;min-width:0"><span class="ci-title">'+esc(t.title||t.task_id)+'</span>'+
          (reason ? '<div style="font-size:10px;color:var(--muted);font-style:italic;margin-top:2px">'+esc(reason)+'</div>' : '') +
          (hasTaskClassEvidence ? '<div style="font-size:10px;color:var(--muted);margin-top:2px">class '+esc(String(taskClass).toLowerCase())+' | source '+esc(String(taskClassSource).toLowerCase())+'</div>' : '') +
          '</div>'+
          '<span class="ci-meta">'+esc(st)+' · '+esc(t.priority||'')+'</span>'+
        '</div>';
      }).join('');
      bindTaskDetailElements(cancelledList, cancelled, '.cancelled-item');
    } else {
      cancelledSection.style.display = 'none';
      cancelledList.innerHTML = '';
    }
  } catch(e) { console.error('loadTasks', e); }
}

let _cachedTasks = [];
function taskClaimLifecycleEvents(taskId) {
  const normalizedTaskID = String(taskId || '').trim();
  if (!normalizedTaskID) return [];
  const relevantTypes = new Set(['task.claimed', 'task.blocked', 'task.released', 'task.completed']);
  return (runtimeEventsCache || [])
    .filter(item => String(item.task_id || '').trim() === normalizedTaskID && relevantTypes.has(String(item.event_type || '').trim()))
    .sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at));
}

function taskBlockingActions(taskId) {
  const normalizedTaskID = String(taskId || '').trim();
  if (!normalizedTaskID) return [];
  return (actionsCache || [])
    .filter(action => String(action.task_id || '').trim() === normalizedTaskID && !!action.blocking)
    .sort((left, right) => controlPolicyTimeValue(right.created_at) - controlPolicyTimeValue(left.created_at));
}

function taskClaimLifecycleTone(eventType) {
  const normalized = String(eventType || '').trim().toLowerCase();
  if (normalized === 'task.blocked') return 'background:rgba(214,162,60,.14);color:var(--yellow)';
  if (normalized === 'task.claimed') return 'background:rgba(91,159,224,.14);color:var(--blue)';
  if (normalized === 'task.completed') return 'background:#16a34a22;color:#16a34a';
  if (normalized === 'task.released') return 'background:#64748b22;color:var(--muted)';
  return 'background:#64748b22;color:var(--muted)';
}

async function showTaskDetail(taskId, titleHint) {
  openModal(titleHint || taskId, '<div class="empty">Loading...</div>');
  try {
    // Get claim info from cached workspace task list
    const cached = _cachedTasks.find(t => t.task_id === taskId) || {};
    const [r, authorityResponse] = await Promise.all([
      rpc('task.status', {workspace_id: WS_ID, task_id:taskId}),
      rpc('workspace.instrumentation.corridor.authority.task', corridorAuthorityParams({task_id: taskId})).catch(() => null)
    ]);
    // Use claim_status if available, fallback to task status
    const displayStatus = cached.claim_status || r.status || 'PENDING';
    const authorityDetail = authorityResponse && authorityResponse.detail ? authorityResponse.detail : null;
    if (authorityDetail && authorityDetail.task && authorityDetail.task.task_id) {
      corridorAuthorityDetailCache[taskId] = authorityDetail;
    }
    const taskClassEvidence = Object.assign({}, cached || {}, r || {}, (authorityDetail && authorityDetail.task) || {});
    const hasTaskClassEvidence = !!corridorFirstNonEmpty(taskClassEvidence.task_class, taskClassEvidence.task_class_hint);
    const taskAuthority = corridorAuthorityApproximation(taskClassEvidence, authorityDetail);
    const taskAuthorityFreshness = corridorAuthorityBasisFreshnessApproximation(taskClassEvidence, authorityDetail);

    let html = '<div style="font-size:11px;color:var(--muted);margin-bottom:10px">ID: <code style="background:var(--surface);padding:2px 6px;border-radius:4px">'+esc(taskId)+'</code></div>';
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:13px;margin-bottom:12px">';
    html += '<div><strong>Status</strong><br><span class="priority-'+esc(displayStatus)+'">'+esc(displayStatus)+'</span></div>';
    html += '<div><strong>Priority</strong><br><span class="priority-'+esc(r.priority)+'">'+esc(r.priority)+'</span></div>';
    html += '<div><strong>Owner</strong><br>'+esc(r.owner_user_id)+'</div>';
    if (cached.claim_agent_id) {
      html += '<div><strong>Assigned to</strong><br>'+esc(cached.claim_agent_id)+'</div>';
    } else {
      html += '<div><strong>Kind</strong><br>'+esc(r.task_kind)+' / '+esc(r.task_template)+'</div>';
    }
    html += '<div><strong>Created</strong><br>'+timeAgo(r.created_at)+'</div>';
    html += '<div><strong>Updated</strong><br>'+timeAgo(cached.claim_updated_at||r.updated_at)+'</div>';
    if (hasTaskClassEvidence) {
      html += '<div><strong>Task Class Evidence</strong><br><span style="color:'+corridorTaskClassColor(corridorTaskClassValue(taskClassEvidence))+'">'+esc(String(corridorTaskClassValue(taskClassEvidence)).toLowerCase())+'</span></div>';
      html += '<div><strong>Task Class Source</strong><br>'+esc(String(corridorTaskClassSource(taskClassEvidence)).toLowerCase())+'</div>';
    }
    if (taskAuthority !== 'not surfaced' || taskAuthorityFreshness !== 'no task-metadata lookup basis') {
      html += '<div><strong>Task-First Corridor Authority</strong><br>'+esc(String(taskAuthority).toLowerCase())+'</div>';
      html += '<div><strong>Authority Basis Freshness</strong><br>'+esc(taskAuthorityFreshness)+'</div>';
    }
    html += '</div>';
    if (cached.claim_summary) html += '<div style="margin-bottom:8px;padding:8px;background:var(--surface);border-radius:8px;font-size:12px"><strong>Claim note:</strong> '+esc(cached.claim_summary)+'</div>';
    if (r.description) html += '<div style="margin-bottom:8px"><strong>Description:</strong> '+esc(r.description)+'</div>';
    const tags = (cached.tags || r.tags || []);
    if (tags.length) {
      html += '<div style="margin-bottom:8px"><strong>Tags:</strong> ' + tags.map(tg => '<span class="task-tag">'+esc(tg)+'</span>').join(' ') + '</div>';
    }
    // Related actions cross-link
    const relatedActions = (actionsCache||[]).filter(a => a.task_id === taskId);
    const blockingActions = taskBlockingActions(taskId);
    const claimEvents = taskClaimLifecycleEvents(taskId);
    if (relatedActions.length) {
      html += '<div style="margin-bottom:8px"><strong>Actions:</strong> ';
      html += relatedActions.map(a => {
        const icon = a.status === 'PENDING' ? '' : a.status === 'COMPLETED' ? '✓' : '✗';
        return '<a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('actions');setTimeout(()=>showActionDetail((a.action_id)),100)}) + ' style="color:var(--accent);font-size:12px;margin-right:8px">'+icon+' '+esc(a.title)+'</a>';
      }).join('');
      html += '</div>';
    }
    html += '<div class="msg-item" style="margin:12px 0">';
    html += '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start"><div><strong>Claim Lock History</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">Operator-facing lifecycle of claim state and blocking human actions; read-side evidence only.</div></div>' + (claimEvents.length ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((claimEvents[0].event_id || ''))}) + '>Latest Runtime Event</button>' : '') + '</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:10px">';
    html += '<div><strong>Claim Status</strong><br>' + esc(displayStatus) + '</div>';
    html += '<div><strong>Assigned Agent</strong><br>' + esc(cached.claim_agent_id || 'unclaimed') + '</div>';
    html += '<div><strong>Claim Updated</strong><br>' + esc(timeAgo(cached.claim_updated_at || r.updated_at || '')) + '</div>';
    html += '<div><strong>Blocking Actions</strong><br>' + esc(String(blockingActions.length)) + '</div>';
    html += '</div>';
    html += '<div style="margin-top:12px"><strong>Lifecycle Events</strong><div style="margin-top:6px">';
    if (claimEvents.length) {
      html += claimEvents.slice(0, 6).map(event => {
        const payload = parseJSON(event.payload_json);
        const detailParts = [
          payload.reason || '',
          payload.summary || '',
          event.agent_id || payload.agent_id || ''
        ].filter(Boolean);
        return '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>' + esc(displayRuntimeEventType(event.event_type) || event.event_type || 'task event') + '</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(detailParts.join(' | ') || 'Task claim lifecycle event') + '</div></div><div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap"><span class="tool-badge kind" style="' + taskClaimLifecycleTone(event.event_type) + '">' + esc(String(payload.claim_status || event.event_type || '').replace('task.', '').toUpperCase()) + '</span><button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){showRuntimeEventDetail((event.event_id || ''))}) + '>Open Runtime Event</button></div></div><div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(timeAgo(event.created_at || '')) + '</div></div>';
      }).join('');
    } else {
      html += '<div class="empty">No claim lifecycle runtime events are currently visible for this task.</div>';
    }
    html += '</div></div>';
    html += '<div style="margin-top:12px"><strong>Blocking Actions</strong><div style="margin-top:6px">';
    if (blockingActions.length) {
      html += blockingActions.slice(0, 6).map(action => {
        const linkedRebaseQueue = findLinkedRebaseQueueForAction(action.action_id);
        const linkedRebasePayload = linkedRebaseQueue ? queueRebaseFollowupPayload(linkedRebaseQueue) : null;
        const linkedRebaseSummary = linkedRebaseQueue ? queueRebaseFollowupSummary(linkedRebaseQueue) : '';
        const canStartRebase = !!(linkedRebasePayload && String(action.status || '').toUpperCase() === 'PENDING' && String(linkedRebasePayload.rebase_workflow_state || '').trim().toLowerCase() !== 'in_progress');
        const canPauseRebase = !!(linkedRebasePayload && String(action.status || '').toUpperCase() === 'PENDING' && String(linkedRebasePayload.rebase_workflow_state || '').trim().toLowerCase() === 'in_progress');
        const statusTone = action.status === 'PENDING'
          ? 'background:rgba(214,162,60,.14);color:var(--yellow)'
          : (action.status === 'COMPLETED'
            ? 'background:#16a34a22;color:#16a34a'
            : 'background:rgba(224,106,106,.14);color:var(--red)');
        const detail = [
          action.assigned_to ? ('assigned ' + action.assigned_to) : '',
          action.description || '',
          action.resolution_comment ? ('resolution ' + action.resolution_comment) : '',
          linkedRebaseSummary ? ('rebase ' + linkedRebaseSummary) : '',
          linkedRebasePayload && linkedRebasePayload.action_started_by ? ('started by ' + linkedRebasePayload.action_started_by) : '',
          linkedRebasePayload && linkedRebasePayload.action_paused_by ? ('paused by ' + linkedRebasePayload.action_paused_by) : ''
        ].filter(Boolean).join(' | ');
        return '<div class="msg-item" style="margin-bottom:6px"><div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div><strong>' + esc(action.title || action.action_id) + '</strong><div style="font-size:11px;color:var(--muted);margin-top:4px">' + esc(detail || 'Blocking human action attached to this task.') + '</div></div><div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap"><span class="tool-badge kind" style="' + statusTone + '">' + esc(String(action.status || 'PENDING')) + '</span>' + (linkedRebasePayload ? '<span class="tool-badge kind">REBASE ' + esc(String(linkedRebasePayload.rebase_workflow_state || 'queued').replaceAll('_', ' ')) + '</span>' : '') + (canStartRebase ? '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();startAction((action.action_id || ''))}) + '>Start Rebase</button>' : '') + (canPauseRebase ? '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();pauseAction((action.action_id || ''))}) + '>Pause Rebase</button>' : '') + '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){closeModal();switchTab('actions');setTimeout(()=>showActionDetail((action.action_id || '')),100)}) + '>Open Action</button></div></div><div style="font-size:11px;color:var(--muted);margin-top:6px">' + esc(['created ' + timeAgo(action.created_at || ''), action.resolved_at ? 'resolved ' + timeAgo(action.resolved_at) : 'awaiting resolution', linkedRebasePayload && linkedRebasePayload.rebase_workflow_step ? ('workflow ' + linkedRebasePayload.rebase_workflow_step) : ''].filter(Boolean).join(' | ')) + '</div></div>';
      }).join('');
    } else {
      html += '<div class="empty">No blocking actions are currently attached to this task.</div>';
    }
    html += '</div></div>';
    html += '</div>';
    if (r.nodes && r.nodes.length > 0) {
      html += renderDag(r.nodes);
    } else if (r.node_counts && Object.keys(r.node_counts).length > 0) {
      html += '<hr style="border-color:var(--border);margin:12px 0"><strong>Nodes:</strong><br>';
      Object.entries(r.node_counts).forEach(([k,v]) => {
        html += '<span class="task-tag" style="font-size:11px;padding:3px 8px;margin:2px">'+esc(k)+': '+v+'</span>';
      });
    }
    // Delete button
    const ds = displayStatus.toUpperCase();
    if (ds !== 'RESOLVED' && ds !== 'CANCELLED' && ds !== 'FAILED') {
      html += '<hr style="border-color:var(--border);margin:14px 0">';
      html += '<button class="btn-danger" style="background:var(--red);border:none;color:#fff;padding:6px 16px;border-radius:6px;font-size:12px;cursor:pointer;font-weight:600;font-family:var(--font)" ' + dashboardAction(function(dashboardEvent){deleteTask((taskId),(r.title||taskId))}) + '>';
      html += 'Delete Task</button>';
    }
    document.getElementById('modal-body').innerHTML = html;
  } catch(e) {
    document.getElementById('modal-body').innerHTML = '<div class="empty">'+esc(e.message)+'</div>';
  }
}

// ── DAG Visualization ──
function renderDag(nodes) {
  if (!nodes || !nodes.length) return '';
  // 1. Topological sort / layering
  const nodeMap = new Map();
  nodes.forEach(n => nodeMap.set(n.node_id, {...n, layer: 0}));

  let changed = true;
  let iters = 0;
  while(changed && iters < 100) {
    changed = false;
    iters++;
    for (const n of nodes) {
      const currentLayer = nodeMap.get(n.node_id).layer;
      let maxDepLayer = -1;
      if (n.depends_on) {
        for (const dep of n.depends_on) {
          const dNode = nodeMap.get(dep);
          if (dNode && dNode.layer > maxDepLayer) maxDepLayer = dNode.layer;
        }
      }
      if (maxDepLayer + 1 > currentLayer) {
        nodeMap.get(n.node_id).layer = maxDepLayer + 1;
        changed = true;
      }
    }
  }

  const layers = [];
  nodeMap.forEach(n => {
    while (layers.length <= n.layer) layers.push([]);
    layers[n.layer].push(n);
  });

  let html = '<div style="margin:16px 0;padding:12px;background:var(--bg);border:1px solid var(--border);border-radius:8px;overflow-x:auto">';
  html += '<div style="font-size:11px;color:var(--muted);text-transform:uppercase;font-weight:600;margin-bottom:12px;letter-spacing:0.5px">Task Graph Visualization</div>';
  html += '<div style="display:flex;gap:24px;align-items:center;min-width:max-content;padding-bottom:8px">';

  layers.forEach((layerNodes, i) => {
    html += '<div style="display:flex;flex-direction:column;gap:12px;position:relative">';
    layerNodes.forEach(n => {
        const st = (n.status || 'PENDING').toUpperCase();
        let bg = 'var(--surface)';
        let border = '1px solid var(--border)';
        let color = 'var(--text)';

        if (st === 'RESOLVED' || st === 'COMPLETED') {
            bg = 'rgba(78, 166, 116, 0.1)';
            border = '1px solid var(--green)';
        } else if (st === 'FAILED') {
            bg = 'rgba(224, 106, 106, 0.1)';
            border = '1px solid var(--red)';
        } else if (st === 'RUNNING' || st === 'CLAIMED') {
            bg = 'rgba(56, 189, 248, 0.1)';
            border = '1px solid var(--accent)';
        } else if (st === 'BLOCKED') {
            bg = 'var(--bg)';
            color = 'var(--muted)';
            border = '1px dashed var(--border)';
        } else if (st === 'PENDING') {
            bg = 'var(--surface)';
        }

        html += '<div style="padding:10px 14px;border-radius:8px;background:'+bg+';border:'+border+';color:'+color+';font-size:12px;min-width:140px;box-shadow:0 2px 4px rgba(0,0,0,0.05);position:relative;backdrop-filter:blur(4px)">';
        html += '<div style="font-weight:600;font-family:monospace;margin-bottom:4px;color:var(--text)">'+esc(n.node_id)+'</div>';
        html += '<div style="display:flex;align-items:center;justify-content:space-between"><span style="font-size:10px;font-weight:700;opacity:0.8">'+esc(st)+'</span>';
        if (n.attempt_count > 0) {
            html += '<span style="font-size:10px;opacity:0.6">Try '+n.attempt_count+'</span>';
        }
        html += '</div>';

        if (n.depends_on && n.depends_on.length > 0) {
            html += '<div style="font-size:9px;margin-top:8px;color:var(--muted);border-top:1px solid rgba(128,128,128,0.2);padding-top:6px;line-height:1.4">';
            html += 'Deps: <span style="font-family:monospace">'+esc(n.depends_on.join(', '))+'</span>';
            html += '</div>';
        }
        html += '</div>';
    });
    html += '</div>';

    // Add arrow separator
    if (i < layers.length - 1) {
       html += '<div style="color:var(--muted);font-size:16px;opacity:0.5">→</div>';
    }
  });

  html += '</div></div>';
  return html;
}

// ── Create/Delete Tasks ──
function toggleCreateTask() {
  document.getElementById('create-task-form').classList.toggle('open');
}

async function submitNewTask() {
  const title = document.getElementById('ct-title').value.trim();
  const desc = document.getElementById('ct-desc').value.trim();
  const priority = document.getElementById('ct-priority').value;
  const kind = document.getElementById('ct-kind').value;
  const template = document.getElementById('ct-template').value;
  const taskClass = (document.getElementById('ct-task-class') || {}).value || '';
  const tagsRaw = document.getElementById('ct-tags').value.trim();
  const tags = tagsRaw ? tagsRaw.split(',').map(t => t.trim()).filter(Boolean) : [];
  const projectId = (document.getElementById('ct-project')||{}).value || '';
  const statusEl = document.getElementById('ct-status');

  if (!title) { statusEl.textContent='Title is required'; statusEl.className='msg-status err'; return; }

  try {
    const taskId = 'task-'+Date.now();
    await rpc('task.submit', {
      task_id: taskId,
      owner_user_id: 'developer',
      priority: priority,
      title: title,
      description: desc,
      task_kind: kind,
      task_template: template,
      task_class: taskClass || undefined,
      workspace_id: WS_ID,
      linked_by: 'dashboard',
      tags: tags,
      project_id: projectId
    });
    statusEl.textContent = '✓ Created: ' + taskId;
    statusEl.className = 'msg-status ok';
    document.getElementById('ct-title').value = '';
    document.getElementById('ct-desc').value = '';
    document.getElementById('ct-task-class').value = '';
    document.getElementById('ct-tags').value = '';
    toast('✓ Task created: ' + title);
    setTimeout(() => { statusEl.textContent = ''; }, 4000);
    loadTasks();
    if (activeTabPanelId() === 'panel-graph') {
      triggerGraphSync(80);
    }
  } catch(e) {
    statusEl.textContent = '✗ ' + e.message;
    statusEl.className = 'msg-status err';
  }
}

let _deleteTaskId = '';
let _deleteTaskTitle = '';

function deleteTask(taskId, title) {
  _deleteTaskId = taskId;
  _deleteTaskTitle = title;
  document.getElementById('delete-confirm-text').textContent = 'Delete task: "' + title + '"? This will mark it as cancelled.';
  document.getElementById('delete-reason').value = '';
  document.getElementById('delete-confirm').classList.add('open');
  closeModal();
  syncDashboardOverlayLock();
}

function cancelDelete() {
  document.getElementById('delete-confirm').classList.remove('open');
  _deleteTaskId = '';
  syncDashboardOverlayLock();
}

async function confirmDeleteTask() {
  if (!_deleteTaskId) return;
  const reason = document.getElementById('delete-reason').value.trim();
  try {
    await rpc('task.close', {
      workspace_id: WS_ID,
      task_id: _deleteTaskId,
      actor_id: 'developer',
      resolution: 'CANCELLED',
      reason: reason || 'Deleted from dashboard by developer'
    });
    toast('Task deleted: ' + _deleteTaskTitle);
    document.getElementById('delete-confirm').classList.remove('open');
    _deleteTaskId = '';
    syncDashboardOverlayLock();
    loadTasks();
    if (activeTabPanelId() === 'panel-graph') {
      triggerGraphSync(80);
    }
  } catch(e) {
    toast('Error: ' + e.message);
  }
}

// ── Activity Feed ──
async function loadFeed() {
  try {
    const r = await rpc('workspace.updates.list', {workspace_id:WS_ID, limit:20});
    const items = (r.updates || []).slice().reverse();
    document.getElementById('feed-count').textContent = items.length;
    const el = document.getElementById('feed-list');
    if (!items.length) { el.innerHTML = '<div class="empty">No recent activity</div>'; return; }
    el.innerHTML = items.map(ev => {
      const icon = ev.update_type==='progress'?'':ev.update_type==='blocker'?'':ev.update_type==='result'?'✓':'';
      const cls = ev.update_type==='progress'?'task':ev.update_type==='blocker'?'msg':'doc';
      return '<div class="feed-item">' +
        '<div class="feed-icon '+cls+'">'+icon+'</div>' +
        '<div class="feed-content">' +
          '<div class="feed-main"><span class="feed-actor">'+esc(ev.agent_name||ev.agent_id||'system')+'</span> '+esc(ev.summary||ev.update_type)+'</div>' +
          '<div class="feed-time">'+timeAgo(ev.created_at)+'</div>' +
        '</div></div>';
    }).join('');
  } catch(e) { console.error('loadFeed', e); }
}

// ── Interactions (real runtime interactions, from runtime_events) ──
let _interactionsCache = [];
const IX_NOISE_AGENTS = new Set(['telegram-bridge']);
function _ixParse(p){ try { return typeof p === 'string' ? JSON.parse(p) : (p || {}); } catch(e){ return {}; } }
function classifyInteraction(ev){
  const t = String(ev.event_type || '');
  if (t.indexOf('agent.request') === 0 || t === 'agent.response') return 'ask';
  if (t.indexOf('tool.call') === 0) return 'tool';
  if (t.indexOf('execution') === 0 || t.indexOf('workspace.execution') === 0) return 'execution';
  if (t.indexOf('session') !== -1) return 'session';
  if (t.indexOf('task') !== -1 || t.indexOf('node.') === 0) return 'task';
  return 'other';
}
function _ixIsNoise(ev){
  const a = String(ev.agent_id || ev.actor_id || '');
  if (IX_NOISE_AGENTS.has(a) || /-bridge$/.test(a)) return true;
  if (String(ev.event_type || '') === 'authority.renewed') return true;
  const pj = String(ev.payload_json || '');
  if (pj.indexOf('inbound_poll') !== -1 || pj.indexOf('telegram_inbound') !== -1) return true;
  return false;
}
async function loadMessages(){
  try {
    const r = await rpc('workspace.events.list', {workspace_id: WS_ID, limit: 200});
    _interactionsCache = r.items || [];
    renderInteractions();
  } catch(e){
    console.error('loadInteractions', e);
    const el = document.getElementById('msgs-list');
    if (el) el.innerHTML = '<div class="empty" style="padding:16px">'+esc(e.message || 'Failed to load interactions')+'</div>';
  }
}
function renderInteractions(){
  const kindF = (document.getElementById('ix-kind-filter') || {}).value || '';
  const q = String((document.getElementById('ix-search') || {}).value || '').trim().toLowerCase();
  const hideNoise = !!(document.getElementById('ix-hide-noise') || {}).checked;
  const items = (_interactionsCache || []).filter(function(ev){
    if (hideNoise && _ixIsNoise(ev)) return false;
    const k = classifyInteraction(ev);
    if (k === 'other') return false;
    if (kindF && k !== kindF) return false;
    if (q && [ev.event_type, ev.actor_id, ev.agent_id, ev.task_id, ev.session_id, ev.payload_json].join(' ').toLowerCase().indexOf(q) === -1) return false;
    return true;
  });
  const cnt = document.getElementById('msgs-count'); if (cnt) cnt.textContent = items.length;
  const sMsgs = document.getElementById('s-msgs'); if (sMsgs) sMsgs.textContent = items.length;
  const el = document.getElementById('msgs-list');
  if (!el) return;
  if (!items.length){ el.innerHTML = '<div class="empty" style="padding:18px">No interactions yet'+(hideNoise?' (infrastructure noise hidden)':'')+'.</div>'; return; }
  el.innerHTML = items.map(renderInteractionRow).join('');
}
function renderInteractionRow(ev){
  const kind = classifyInteraction(ev);
  const p = _ixParse(ev.payload_json);
  const actor = ev.agent_id || ev.actor_id || p.agent_id || 'system';
  const et = String(ev.event_type || '');
  const isErr = et.indexOf('denied') !== -1 || et.indexOf('blocked') !== -1 || et.indexOf('failed') !== -1 ||
    String(p.status || p.outcome || '').toUpperCase() === 'FAILED';
  const action = (typeof displayRuntimeEventType === 'function' ? displayRuntimeEventType(et) : et) || et;
  let detail = '';
  if (kind === 'ask'){
    const to = p.to_agent_id || p.to || '';
    if (p.response) detail = 'answered: ' + p.response;
    else if (p.payload) detail = (to ? ('→ ' + to + ': ') : '') + p.payload;
    else detail = to ? ('→ ' + to) : '';
  } else {
    detail = p.summary || p.title || p.outcome || p.error || p.detail || '';
  }
  if (!detail){
    detail = ev.task_id ? ('task ' + ev.task_id) : (ev.session_id ? ('session ' + ev.session_id) : (ev.entity_id || ''));
  }
  detail = String(detail);
  const rel = timeAgo(ev.created_at).split(' · ')[0];
  return '<div class="ix-row '+kind+(isErr?' err':'')+'">'+
    '<span class="ix-kind '+kind+'">'+(kind==='ask'?'ASK':esc(kind))+'</span>'+
    '<span class="ix-actor" title="'+esc(actor)+'">'+esc(actor)+'</span>'+
    '<span class="ix-action" title="'+esc(action)+'">'+esc(action)+'</span>'+
    '<span class="ix-detail" title="'+esc(detail)+'">'+esc(detail)+'</span>'+
    '<span class="ix-time" title="'+esc(timeAgo(ev.created_at))+'">'+esc(rel)+'</span>'+
  '</div>';
}

// ── Tools & MCP ──
async function loadTools() {
  try {
    const r = await rpc('tool.list', {workspace_id:WS_ID});
    const tools = r.tools || [];
    document.getElementById('tools-count').textContent = tools.length;
    const el = document.getElementById('tools-list');
    if (!tools.length) { el.innerHTML='<div class="empty">No tools registered</div>'; }
    else {
      el.innerHTML = tools.map(t => {
        const statusCls = (t.status||'').toLowerCase()==='active'?'active':(t.status||'').toLowerCase()==='blocked'?'blocked':'planned';
        return '<div class="tool-card" ' + dashboardAction(function(dashboardEvent){showToolDetail((t.tool_id||t.name))}) + '>' +
          '<div class="tool-name">'+esc(t.name||t.tool_id)+'</div>' +
          '<div class="tool-desc">'+esc(t.description||'No description')+'</div>' +
          '<div class="tool-badges">' +
            '<span class="tool-badge '+statusCls+'">'+esc(t.status||'ACTIVE')+'</span>' +
            '<span class="tool-badge kind">'+esc(t.kind||t.runtime||'TOOL')+'</span>' +
          '</div>' +
        '</div>';
      }).join('');
    }
  } catch(e) { console.error('loadTools', e); document.getElementById('tools-list').innerHTML='<div class="empty">'+esc(e.message)+'</div>'; }

  // MCP servers
  try {
    const r = await rpc('mcp.server.list', {workspace_id:WS_ID});
    const servers = r.servers || [];
    _mcpCache = servers;
    document.getElementById('mcp-count').textContent = servers.length;
    const el = document.getElementById('mcp-list');
    if (!servers.length) { el.innerHTML='<div class="empty">No MCP servers registered</div>'; }
    else {
      el.innerHTML = servers.map(s => {
        const envObj = parseJSON(s.env_json);
        const envCount = Object.keys(envObj).length;
        return '<div class="tool-card" ' + dashboardAction(function(dashboardEvent){showMcpDetail((s.server_id||s.name))}) + '>' +
          '<div class="tool-name">'+esc(s.display_name||s.name||s.server_id)+'</div>' +
          '<div class="tool-desc">'+esc(s.transport||'stdio')+' · '+esc(s.command||s.url||'')+'</div>' +
          '<div class="tool-badges">' +
            '<span class="tool-badge active">'+esc(s.status||'ACTIVE')+'</span>' +
            '<span class="tool-badge kind">'+esc(s.transport||'stdio')+'</span>' +
            (envCount ? '<span class="tool-badge kind">'+envCount+' env vars</span>' : '') +
            (s.tools_count ? '<span class="tool-badge kind">'+s.tools_count+' tools</span>' : '') +
          '</div>' +
        '</div>';
      }).join('');
    }
  } catch(e) { console.error('loadMcpServers', e); document.getElementById('mcp-list').innerHTML='<div class="empty">'+esc(e.message)+'</div>'; }
}

let _mcpCache = [];
function parseJSON(s) { try { return JSON.parse(s||'{}'); } catch { return {}; } }
function parseJSONArr(s) { try { return JSON.parse(s||'[]'); } catch { return []; } }

function maskValue(v) {
  if (!v || v.length < 8) return '••••••';
  return v.substring(0, 4) + '••••' + v.substring(v.length - 4);
}

function showMcpDetail(serverId) {
  const s = _mcpCache.find(x => x.server_id === serverId);
  if (!s) { openModal('MCP: '+serverId, '<div class="empty">Server not found</div>'); return; }

  let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px;font-size:13px">';
  html += '<div><strong>Server ID</strong><br><code style="background:var(--surface);padding:2px 6px;border-radius:4px;font-size:11px">'+esc(s.server_id)+'</code></div>';
  html += '<div><strong>Status</strong><br><span class="tool-badge active">'+esc(s.status||'ACTIVE')+'</span></div>';
  html += '<div><strong>Transport</strong><br>'+esc(s.transport||'stdio')+'</div>';
  html += '<div><strong>Registered By</strong><br>'+esc(s.registered_by||'—')+'</div>';
  if (s.url) html += '<div><strong>URL</strong><br><code style="font-size:11px">'+esc(s.url)+'</code></div>';
  if (s.command) html += '<div><strong>Command</strong><br><code style="font-size:11px">'+esc(s.command)+'</code></div>';
  html += '<div><strong>Created</strong><br>'+timeAgo(s.created_at)+'</div>';
  html += '<div><strong>Updated</strong><br>'+timeAgo(s.updated_at)+'</div>';
  html += '</div>';

  // Args
  const args = parseJSONArr(s.args_json);
  if (args.length) {
    html += '<div style="margin-bottom:10px"><strong>Arguments:</strong><br>';
    html += '<code style="font-size:11px;background:var(--surface);padding:4px 8px;border-radius:4px;display:inline-block;margin-top:4px">'+esc(args.join(' '))+'</code></div>';
  }

  // Env vars (API keys etc.)
  const env = parseJSON(s.env_json);
  const envKeys = Object.keys(env);
  if (envKeys.length) {
    html += '<div style="margin-bottom:10px"><strong>Environment Variables:</strong>';
    html += '<div style="margin-top:6px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;font-size:12px">';
    envKeys.forEach(k => {
      const isKey = k.toLowerCase().includes('key') || k.toLowerCase().includes('token') || k.toLowerCase().includes('secret');
      html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--border)">';
      html += '<code style="color:var(--accent)">'+esc(k)+'</code>';
      html += '<span id="env-'+esc(k)+'" style="font-family:monospace;font-size:11px;color:var(--muted)">'+(isKey ? maskValue(env[k]) : esc(env[k]))+'</span>';
      if (isKey) {
        html += '<button ' + dashboardAction(function(dashboardEvent){toggleEnvValue((k),(env[k]))}) + ' style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--muted);font-size:10px;padding:2px 6px;cursor:pointer;margin-left:6px">show</button>';
      }
      html += '</div>';
    });
    html += '</div></div>';
  }

  // Headers
  const headers = parseJSON(s.headers_json);
  const hdrKeys = Object.keys(headers);
  if (hdrKeys.length) {
    html += '<div style="margin-bottom:10px"><strong>Headers:</strong>';
    html += '<div style="margin-top:6px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;font-size:12px">';
    hdrKeys.forEach(k => {
      html += '<div style="padding:3px 0"><code>'+esc(k)+':</code> <span style="color:var(--muted)">'+esc(headers[k])+'</span></div>';
    });
    html += '</div></div>';
  }

  openModal(''+esc(s.display_name||s.server_id), html);
}

function toggleEnvValue(key, value) {
  const el = document.getElementById('env-'+key);
  if (!el) return;
  if (el.dataset.shown === '1') {
    el.textContent = maskValue(value);
    el.dataset.shown = '0';
  } else {
    el.textContent = value;
    el.dataset.shown = '1';
  }
}

function showToolDetail(toolId) {
  openModal(''+toolId, '<div class="empty">Loading...</div>');
  rpc('tool.status', {tool_id:toolId, workspace_id:WS_ID}).then(r => {
    let html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px">';
    html += '<div><strong>Tool ID</strong><br><code>'+esc(r.tool_id||toolId)+'</code></div>';
    html += '<div><strong>Status</strong><br>'+esc(r.status||'ACTIVE')+'</div>';
    html += '<div><strong>Kind</strong><br>'+esc(r.kind||r.runtime||'-')+'</div>';
    html += '<div><strong>Access</strong><br>'+esc(r.access_level||'WORKSPACE')+'</div>';
    html += '</div>';
    if (r.description) html += '<div style="margin-bottom:8px"><strong>Description:</strong> '+esc(r.description)+'</div>';
    if (r.script_path) html += '<div style="margin-bottom:8px"><strong>Path:</strong> <code>'+esc(r.script_path)+'</code></div>';
    html += '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:14px">';
    html += '<button style="padding:6px 16px;background:rgba(224,106,106,0.1);color:var(--red);border:1px solid rgba(224,106,106,0.3);border-radius:6px;cursor:pointer;font-family:var(--font);font-size:12px" ' + dashboardAction(function(dashboardEvent){removeToolRegistryEntry((r.tool_id||toolId))}) + '>Remove</button>';
    html += '</div>';
    document.getElementById('modal-body').innerHTML = html;
  }).catch(e => {
    document.getElementById('modal-body').innerHTML = '<div class="empty">'+esc(e.message)+'</div>';
  });
}

async function removeToolRegistryEntry(toolId) {
  const btn = event.target;
  if (btn.dataset.confirm !== 'yes') {
    btn.dataset.confirm = 'yes';
    btn.textContent = 'Confirm?';
    btn.style.background = 'rgba(224,106,106,0.3)';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Remove'; btn.style.background = 'rgba(224,106,106,0.1)'; }, 3000);
    return;
  }
  try {
    await rpc('tool.remove', {workspace_id: WS_ID, tool_id: toolId, removed_by: 'dashboard'});
    closeModal();
    toast('Tool removed: ' + toolId);
    loadTools();
  } catch(e) { toast('Error: ' + e.message); }
}

function showMcpDetail(serverId) {
  const s = _mcpCache.find(x => x.server_id === serverId);
  if (!s) { openModal('MCP: '+serverId, '<div class="empty">Server not found</div>'); return; }
  _editingMcpId = serverId;
  renderMcpView(s);
}

let _editingMcpId = null;

function renderMcpView(s) {
  let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
  html += '<span style="font-size:11px;color:var(--muted)">ID: <code>'+esc(s.server_id)+'</code></span>';
  html += '<button onclick="renderMcpEdit()" style="background:var(--accent);border:none;color:#fff;padding:4px 12px;border-radius:6px;font-size:11px;cursor:pointer;font-family:var(--font)">Edit</button>';
  html += '</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px;font-size:13px">';
  html += '<div><strong>Display Name</strong><br>'+esc(s.display_name)+'</div>';
  html += '<div><strong>Status</strong><br><span class="tool-badge active">'+esc(s.status||'ACTIVE')+'</span></div>';
  html += '<div><strong>Transport</strong><br>'+esc(s.transport||'stdio')+'</div>';
  html += '<div><strong>Registered By</strong><br>'+esc(s.registered_by||'—')+'</div>';
  if (s.url) html += '<div><strong>URL</strong><br><code style="font-size:11px">'+esc(s.url)+'</code></div>';
  if (s.command) html += '<div><strong>Command</strong><br><code style="font-size:11px">'+esc(s.command)+'</code></div>';
  html += '<div><strong>Created</strong><br>'+timeAgo(s.created_at)+'</div>';
  html += '<div><strong>Updated</strong><br>'+timeAgo(s.updated_at)+'</div>';
  html += '</div>';
  const args = parseJSONArr(s.args_json);
  if (args.length) {
    html += '<div style="margin-bottom:10px"><strong>Arguments:</strong><br>';
    html += '<code style="font-size:11px;background:var(--surface);padding:4px 8px;border-radius:4px;display:inline-block;margin-top:4px">'+esc(args.join(' '))+'</code></div>';
  }
  const env = parseJSON(s.env_json);
  const envKeys = Object.keys(env);
  if (envKeys.length) {
    html += '<div style="margin-bottom:10px"><strong>Environment Variables:</strong>';
    html += '<div style="margin-top:6px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;font-size:12px">';
    envKeys.forEach(k => {
      const isKey = k.toLowerCase().includes('key') || k.toLowerCase().includes('token') || k.toLowerCase().includes('secret');
      html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--border)">';
      html += '<code style="color:var(--accent)">'+esc(k)+'</code>';
      html += '<span id="env-'+esc(k)+'" style="font-family:monospace;font-size:11px;color:var(--muted)">'+(isKey?maskValue(env[k]):esc(env[k]))+'</span>';
      if (isKey) html += '<button ' + dashboardAction(function(dashboardEvent){toggleEnvValue((k),(env[k]))}) + ' style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--muted);font-size:10px;padding:2px 6px;cursor:pointer;margin-left:6px">show</button>';
      html += '</div>';
    });
    html += '</div></div>';
  }
  const headers = parseJSON(s.headers_json);
  const hdrKeys = Object.keys(headers);
  if (hdrKeys.length) {
    html += '<div style="margin-bottom:10px"><strong>Headers:</strong>';
    html += '<div style="margin-top:6px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;font-size:12px">';
    hdrKeys.forEach(k => {
      html += '<div style="padding:3px 0"><code>'+esc(k)+':</code> <span style="color:var(--muted)">'+esc(headers[k])+'</span></div>';
    });
    html += '</div></div>';
  }
  openModal(''+esc(s.display_name||s.server_id), html);
}

function renderMcpEdit() {
  const s = _mcpCache.find(x => x.server_id === _editingMcpId);
  if (!s) return;
  const env = parseJSON(s.env_json);
  const args = parseJSONArr(s.args_json);
  const headers = parseJSON(s.headers_json);
  const iS = 'width:100%;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:6px 8px;color:var(--text);font-size:12px;font-family:var(--font);box-sizing:border-box';
  const lS = 'font-size:11px;font-weight:600;color:var(--muted);margin-bottom:4px;display:block';
  let html = '<div id="mcp-edit-status" style="font-size:11px;margin-bottom:8px"></div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px">';
  html += '<div><label style="'+lS+'">Display Name</label><input id="mcp-e-name" value="'+esc(s.display_name)+'" style="'+iS+'"></div>';
  html += '<div><label style="'+lS+'">Transport</label><select id="mcp-e-transport" style="'+iS+'">';
  ['stdio','streamable-http','sse'].forEach(t => { html += '<option value="'+t+'"'+(s.transport===t?' selected':'')+'>'+t+'</option>'; });
  html += '</select></div>';
  html += '<div><label style="'+lS+'">URL</label><input id="mcp-e-url" value="'+esc(s.url||'')+'" placeholder="https://..." style="'+iS+'"></div>';
  html += '<div><label style="'+lS+'">Command</label><input id="mcp-e-cmd" value="'+esc(s.command||'')+'" placeholder="npx, node..." style="'+iS+'"></div>';
  html += '</div>';
  html += '<div style="margin-bottom:14px"><label style="'+lS+'">Arguments (space-separated)</label>';
  html += '<input id="mcp-e-args" value="'+esc(args.join(' '))+'" placeholder="-y firecrawl-mcp" style="'+iS+'"></div>';
  html += '<div style="margin-bottom:14px"><label style="'+lS+'">Environment Variables</label>';
  html += '<div id="mcp-env-rows" style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px">';
  let ei = 0;
  Object.entries(env).forEach(([k,v]) => { html += mcpKvRow('env', k, v, ei++); });
  html += '</div>';
  html += '<button onclick="addMcpKvRow(\'env\')" style="margin-top:6px;background:none;border:1px dashed var(--border);border-radius:6px;color:var(--accent);font-size:11px;padding:4px 10px;cursor:pointer;font-family:var(--font)">+ Add Variable</button></div>';
  html += '<div style="margin-bottom:14px"><label style="'+lS+'">Headers</label>';
  html += '<div id="mcp-hdr-rows" style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px">';
  let hi = 0;
  Object.entries(headers).forEach(([k,v]) => { html += mcpKvRow('hdr', k, v, hi++); });
  html += '</div>';
  html += '<button onclick="addMcpKvRow(\'hdr\')" style="margin-top:6px;background:none;border:1px dashed var(--border);border-radius:6px;color:var(--accent);font-size:11px;padding:4px 10px;cursor:pointer;font-family:var(--font)">+ Add Header</button></div>';
  html += '<div style="display:flex;gap:8px;justify-content:flex-end">';
  html += '<button ' + dashboardAction(function(dashboardEvent){showMcpDetail((s.server_id))}) + ' style="background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:6px 16px;font-size:12px;cursor:pointer;font-family:var(--font)">Cancel</button>';
  html += '<button onclick="saveMcpEdit()" style="background:var(--accent);border:none;color:#fff;padding:6px 16px;border-radius:6px;font-size:12px;cursor:pointer;font-weight:600;font-family:var(--font)">Save</button>';
  html += '</div>';
  openModal('Edit: '+esc(s.display_name||s.server_id), html);
}
let _mcpKvIdx = 100;
function mcpKvRow(prefix, k, v, i) {
  const id = 'mcp-'+prefix+'-'+(_mcpKvIdx++);
  return '<div id="'+id+'" style="display:flex;gap:6px;align-items:center;margin-bottom:4px">' +
    '<input class="mcp-'+prefix+'-key" value="'+esc(k)+'" placeholder="KEY" style="flex:1;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--accent);font-size:11px;font-family:monospace">' +
    '<input class="mcp-'+prefix+'-val" value="'+esc(v)+'" placeholder="value" style="flex:2;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--text);font-size:11px;font-family:monospace">' +
    '<button onclick="this.parentElement.remove()" style="background:none;border:none;color:var(--red);cursor:pointer;font-size:14px;padding:0 4px">×</button>' +
  '</div>';
}
function addMcpKvRow(prefix) {
  document.getElementById('mcp-'+prefix+'-rows').insertAdjacentHTML('beforeend', mcpKvRow(prefix, '', '', 0));
}
async function saveMcpEdit() {
  const s = _mcpCache.find(x => x.server_id === _editingMcpId);
  if (!s) return;
  const statusEl = document.getElementById('mcp-edit-status');
  statusEl.textContent = 'Saving...'; statusEl.style.color = 'var(--accent)';
  const envObj = {};
  document.querySelectorAll('#mcp-env-rows > div').forEach(row => {
    const k = row.querySelector('.mcp-env-key')?.value?.trim();
    const v = row.querySelector('.mcp-env-val')?.value?.trim();
    if (k) envObj[k] = v || '';
  });
  const hdrObj = {};
  document.querySelectorAll('#mcp-hdr-rows > div').forEach(row => {
    const k = row.querySelector('.mcp-hdr-key')?.value?.trim();
    const v = row.querySelector('.mcp-hdr-val')?.value?.trim();
    if (k) hdrObj[k] = v || '';
  });
  const argsStr = document.getElementById('mcp-e-args').value.trim();
  const argsArr = argsStr ? argsStr.split(/\s+/) : [];
  try {
    await rpc('mcp.server.register', {
      server_id: s.server_id, workspace_id: s.workspace_id,
      display_name: document.getElementById('mcp-e-name').value.trim() || s.display_name,
      transport: document.getElementById('mcp-e-transport').value,
      url: document.getElementById('mcp-e-url').value.trim(),
      command: document.getElementById('mcp-e-cmd').value.trim(),
      args_json: JSON.stringify(argsArr),
      env_json: JSON.stringify(envObj),
      headers_json: JSON.stringify(hdrObj),
      registered_by: currentProfileId()
    });
    toast('✓ MCP server updated');
    closeModal();
    loadTools();
  } catch(e) { statusEl.textContent = 'Error: '+e.message; statusEl.style.color = 'var(--red)'; }
}

function renderMcpView(s) {
  let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
  html += '<span style="font-size:11px;color:var(--muted)">ID: <code>'+esc(s.server_id)+'</code></span>';
  html += '<div style="display:flex;gap:8px">';
  html += '<button onclick="renderMcpEdit()" style="background:var(--accent);border:none;color:#fff;padding:4px 12px;border-radius:6px;font-size:11px;cursor:pointer;font-family:var(--font)">Edit</button>';
  html += '<button ' + dashboardAction(function(dashboardEvent){removeMcpServer((s.server_id),(s.display_name||s.server_id))}) + ' style="background:rgba(224,106,106,0.1);color:var(--red);border:1px solid rgba(224,106,106,0.3);padding:4px 12px;border-radius:6px;font-size:11px;cursor:pointer;font-family:var(--font)">Delete</button>';
  html += '</div>';
  html += '</div>';
  html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px;font-size:13px">';
  html += '<div><strong>Display Name</strong><br>'+esc(s.display_name)+'</div>';
  html += '<div><strong>Status</strong><br><span class="tool-badge active">'+esc(s.status||'ACTIVE')+'</span></div>';
  html += '<div><strong>Transport</strong><br>'+esc(s.transport||'stdio')+'</div>';
  html += '<div><strong>Registered By</strong><br>'+esc(s.registered_by||'-')+'</div>';
  if (s.url) html += '<div><strong>URL</strong><br><code style="font-size:11px">'+esc(s.url)+'</code></div>';
  if (s.command) html += '<div><strong>Command</strong><br><code style="font-size:11px">'+esc(s.command)+'</code></div>';
  html += '<div><strong>Created</strong><br>'+timeAgo(s.created_at)+'</div>';
  html += '<div><strong>Updated</strong><br>'+timeAgo(s.updated_at)+'</div>';
  html += '</div>';
  const args = parseJSONArr(s.args_json);
  if (args.length) {
    html += '<div style="margin-bottom:10px"><strong>Arguments:</strong><br>';
    html += '<code style="font-size:11px;background:var(--surface);padding:4px 8px;border-radius:4px;display:inline-block;margin-top:4px">'+esc(args.join(' '))+'</code></div>';
  }
  const env = parseJSON(s.env_json);
  const envKeys = Object.keys(env);
  if (envKeys.length) {
    html += '<div style="margin-bottom:10px"><strong>Environment Variables:</strong>';
    html += '<div style="margin-top:6px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;font-size:12px">';
    envKeys.forEach(k => {
      const isKey = k.toLowerCase().includes('key') || k.toLowerCase().includes('token') || k.toLowerCase().includes('secret');
      html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--border)">';
      html += '<code style="color:var(--accent)">'+esc(k)+'</code>';
      html += '<span id="env-'+esc(k)+'" style="font-family:monospace;font-size:11px;color:var(--muted)">'+(isKey?maskValue(env[k]):esc(env[k]))+'</span>';
      if (isKey) html += '<button ' + dashboardAction(function(dashboardEvent){toggleEnvValue((k),(env[k]))}) + ' style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--muted);font-size:10px;padding:2px 6px;cursor:pointer;margin-left:6px">show</button>';
      html += '</div>';
    });
    html += '</div></div>';
  }
  const headers = parseJSON(s.headers_json);
  const hdrKeys = Object.keys(headers);
  if (hdrKeys.length) {
    html += '<div style="margin-bottom:10px"><strong>Headers:</strong>';
    html += '<div style="margin-top:6px;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;font-size:12px">';
    hdrKeys.forEach(k => {
      html += '<div style="padding:3px 0"><code>'+esc(k)+':</code> <span style="color:var(--muted)">'+esc(headers[k])+'</span></div>';
    });
    html += '</div></div>';
  }
  openModal('MCP '+esc(s.display_name||s.server_id), html);
}

async function removeMcpServer(serverId, title) {
  const btn = event.target;
  if (btn.dataset.confirm !== 'yes') {
    btn.dataset.confirm = 'yes';
    btn.textContent = 'Confirm?';
    btn.style.background = 'rgba(224,106,106,0.3)';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete'; btn.style.background = 'rgba(224,106,106,0.1)'; }, 3000);
    return;
  }
  try {
    await rpc('mcp.server.remove', {server_id: serverId});
    closeModal();
    toast('MCP server removed: ' + title);
    loadTools();
  } catch(e) { toast('Error: ' + e.message); }
}

function toggleAddMcp() {
  document.getElementById('add-mcp-form').classList.toggle('open');
}

async function submitMcpRequest() {
  const title = document.getElementById('mcp-title').value.trim();
  const desc = document.getElementById('mcp-desc').value.trim();
  const statusEl = document.getElementById('mcp-status');
  if (!title) { statusEl.textContent='Enter a tool name'; statusEl.className='msg-status err'; return; }
  if (!desc) { statusEl.textContent='Describe the tool'; statusEl.className='msg-status err'; return; }
  try {
    const taskId = 'human-mcp-'+Date.now();
    await rpc('task.submit', {
      task_id: taskId,
      owner_user_id: currentProfileId(),
      priority: 'CRITICAL',
      title: '[HUMAN REQUEST] Add MCP: '+title,
      description: 'Human-requested MCP integration.\n\n'+desc,
      task_kind: 'EXECUTION',
      workspace_id: WS_ID,
      linked_by: 'dashboard',
      tags: ['human-request','mcp','integration','critical']
    });
    statusEl.textContent='✓ Task created: '+taskId; statusEl.className='msg-status ok';
    document.getElementById('mcp-title').value = '';
    document.getElementById('mcp-desc').value = '';
    toast('Critical task created: '+title);
    setTimeout(()=>{statusEl.textContent='';},4000);
    loadTasks();
    loadTools();
    if (activeTabPanelId() === 'panel-graph') {
      triggerGraphSync(80);
    }
  } catch(e) { statusEl.textContent='✗ '+e.message; statusEl.className='msg-status err'; }
}

// ── Actions (Action Required) ──
let actionsCache = [];
async function loadActions() {
  try {
    const r = await rpc('action.list', {workspace_id:WS_ID});
    actionsCache = r.actions || [];
    const pending = actionsCache.filter(a => a.status === 'PENDING');
    const resolved = actionsCache.filter(a => a.status !== 'PENDING');

    // Badge on tab
    const badge = document.getElementById('actions-badge');
    if (pending.length) { badge.style.display=''; badge.textContent=pending.length; }
    else { badge.style.display='none'; }

    document.getElementById('actions-count').textContent = pending.length;
    const el = document.getElementById('actions-list');
    if (!pending.length) { el.innerHTML='<div class="empty">No pending actions — all clear! </div>'; }
    else {
      el.innerHTML = pending.map(a => renderActionCard(a)).join('');
    }

    document.getElementById('resolved-actions-count').textContent = resolved.length;
    const rel = document.getElementById('resolved-actions-list');
    if (!resolved.length) { rel.innerHTML='<div class="empty">No resolved actions yet</div>'; }
    else {
      rel.innerHTML = resolved.map(a => renderActionCard(a, true)).join('');
    }
  } catch(e) { console.error('loadActions', e); }
}

function renderActionCard(a, isResolved) {
  const linkedRebaseQueue = findLinkedRebaseQueueForAction(a.action_id);
  const linkedRebasePayload = linkedRebaseQueue ? queueRebaseFollowupPayload(linkedRebaseQueue) : null;
  const linkedRebaseBadges = linkedRebaseQueue ? queueRebaseFollowupBadges(linkedRebaseQueue) : [];
  const linkedRebaseSummary = linkedRebaseQueue
    ? [queueRebaseFollowupSummary(linkedRebaseQueue), linkedRebasePayload && linkedRebasePayload.action_started_by ? ('started by ' + linkedRebasePayload.action_started_by) : '', linkedRebasePayload && linkedRebasePayload.action_paused_by ? ('paused by ' + linkedRebasePayload.action_paused_by) : ''].filter(Boolean).join(' | ')
    : '';
  return '<div class="action-card'+(isResolved?' resolved':'')+'" ' + dashboardAction(function(dashboardEvent){showActionDetail((a.action_id))}) + '>' +
    '<div class="action-title">'+esc(a.title)+'</div>' +
    '<div class="action-meta">' +
      '<span class="action-status '+esc(a.status)+'">'+esc(a.status)+'</span>' +
      '<span>'+esc(a.task_title||a.task_id)+'</span>' +
      (a.assigned_to ? '<span>'+esc(a.assigned_to)+'</span>' : '<span>—</span>') +
      '<span>'+timeAgo(a.created_at)+'</span>' +
    '</div>' +
    (linkedRebaseBadges.length ? '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px">' + linkedRebaseBadges.slice(0, 4).map(badge => '<span class="tool-badge kind">'+esc(String(badge || '').replaceAll('_', ' '))+'</span>').join('') + '</div>' : '') +
    (linkedRebaseSummary ? '<div style="font-size:10px;color:var(--muted);margin-top:4px">Rebase workflow: '+esc(linkedRebaseSummary)+'</div>' : '') +
    (a.resolution_comment ? '<div style="font-size:10px;color:var(--muted);margin-top:4px;font-style:italic">'+esc(a.resolution_comment)+'</div>' : '') +
  '</div>';
}

let currentActionId = null;
let currentResolveType = null;

async function showActionDetail(actionId) {
  const a = actionsCache.find(x => x.action_id === actionId);
  if (!a) return;
  currentActionId = actionId;
  const linkedRebaseQueue = findLinkedRebaseQueueForAction(actionId);
  const linkedRebasePayload = linkedRebaseQueue ? queueRebaseFollowupPayload(linkedRebaseQueue) : null;

  let html = '<div class="action-detail-grid">';
  html += '<div><strong>Action ID</strong>'+esc(a.action_id)+'</div>';
  html += '<div><strong>Status</strong><span class="action-status '+esc(a.status)+'">'+esc(a.status)+'</span></div>';
  html += '<div><strong>Linked Task</strong><a href="#" class="action-open-task" style="color:var(--accent)">'+esc(a.task_title||a.task_id)+'</a></div>';
  html += '<div><strong>Assigned To</strong>'+(a.assigned_to ? esc(a.assigned_to) : '— (anyone)')+'</div>';
  html += '<div><strong>Requesting Agent</strong>'+esc(a.agent_id||'—')+'</div>';
  html += '<div><strong>Created</strong>'+timeAgo(a.created_at)+'</div>';
  html += '</div>';

  if (a.description) {
    html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px;font-size:12px;margin-bottom:14px;white-space:pre-wrap">'+esc(a.description)+'</div>';
  }

  if (linkedRebasePayload) {
    html += '<div style="margin-bottom:14px"><strong>Linked Rebase Workflow</strong><div class="msg-item" style="margin-top:6px">';
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;font-size:12px">';
    if (linkedRebasePayload.rebase_workflow_state) html += '<div><strong>Workflow State</strong><br>'+esc(linkedRebasePayload.rebase_workflow_state)+'</div>';
    if (linkedRebasePayload.rebase_workflow_step) html += '<div><strong>Workflow Step</strong><br>'+esc(linkedRebasePayload.rebase_workflow_step)+'</div>';
    if (linkedRebasePayload.rebase_plan_class) html += '<div><strong>Rebase Plan</strong><br>'+esc(linkedRebasePayload.rebase_plan_class)+'</div>';
    if (linkedRebasePayload.conflict_safe_class) html += '<div><strong>Conflict Safety</strong><br>'+esc(linkedRebasePayload.conflict_safe_class)+'</div>';
    if (linkedRebasePayload.action_assigned_to) html += '<div><strong>Workflow Assignee</strong><br>'+esc(linkedRebasePayload.action_assigned_to)+'</div>';
    if (linkedRebasePayload.action_started_by) html += '<div><strong>Started By</strong><br>'+esc(linkedRebasePayload.action_started_by)+'</div>';
    if (linkedRebasePayload.action_paused_by) html += '<div><strong>Paused By</strong><br>'+esc(linkedRebasePayload.action_paused_by)+'</div>';
    if (linkedRebasePayload.repair_tension_id) html += '<div><strong>Repair Tension</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();switchTab('tensions');setTimeout(()=>showTensionDetail((linkedRebasePayload.repair_tension_id)),100)}) + ' style="color:var(--accent)">'+esc(linkedRebasePayload.repair_tension_id)+'</a></div>';
    html += '</div></div></div>';
  }

  if (a.status !== 'PENDING' && a.resolution_comment) {
    html += '<div style="margin-bottom:14px;font-size:12px"><strong>Resolution comment:</strong> <em>'+esc(a.resolution_comment)+'</em></div>';
  }

  // Action buttons (only for pending)
  if (a.status === 'PENDING') {
    const canStartRebase = !!(linkedRebasePayload && String(linkedRebasePayload.rebase_workflow_state || '').trim().toLowerCase() !== 'in_progress');
    const canPauseRebase = !!(linkedRebasePayload && String(linkedRebasePayload.rebase_workflow_state || '').trim().toLowerCase() === 'in_progress');
    html += '<div class="action-btn-row">';
    if (canStartRebase) html += '<button class="btn-session-primary" ' + dashboardAction(function(dashboardEvent){startAction((a.action_id))}) + '>▶ Start Rebase Work</button>';
    if (canPauseRebase) html += '<button class="btn-session-muted" ' + dashboardAction(function(dashboardEvent){pauseAction((a.action_id))}) + '>⏸ Pause Rebase</button>';
    html += '<button class="btn-complete" ' + dashboardAction(function(dashboardEvent){resolveAction((a.action_id),'COMPLETED')}) + '>✓ Action Completed</button>';
    html += '<button class="btn-fail" ' + dashboardAction(function(dashboardEvent){resolveAction((a.action_id),'FAILED')}) + '>✗ Action Failed</button>';
    html += '</div>';
  }

  // Chat section
  html += '<div class="action-chat">';
  html += '<strong style="font-size:11px;text-transform:uppercase;color:var(--muted);letter-spacing:.5px">Chat with Agent</strong>';
  html += '<div class="action-chat-messages" id="action-chat-msgs"><div class="empty">Loading chat...</div></div>';
  if (a.status === 'PENDING') {
    html += '<div class="action-chat-input">';
    html += '<input id="action-chat-input" placeholder="Ask agent for clarification..." onkeydown="if(event.key===\'Enter\')sendActionChat()">';
    html += '<button onclick="sendActionChat()">Send</button>';
    html += '</div>';
  }
  html += '</div>';

  openModal(''+a.title, html);
  bindTaskDetailElements(document.getElementById('modal-body'), [{task_id:a.task_id,title:a.task_title||a.task_id}], '.action-open-task', () => {
    closeModal();
    switchTab('tasks');
  });
  loadActionChat(actionId);
}

async function loadActionChat(actionId) {
  try {
    const r = await rpc('action.chat.list', {action_id:actionId});
    const msgs = r.messages || [];
    const el = document.getElementById('action-chat-msgs');
    if (!el) return;
    if (!msgs.length) { el.innerHTML='<div class="empty" style="padding:8px">No messages yet</div>'; return; }
    el.innerHTML = msgs.map(m => {
      const isHuman = m.from_id.startsWith('human') || m.from_id === 'developer' || m.from_id === 'dashboard';
      return '<div class="action-chat-msg '+(isHuman?'from-human':'from-agent')+'">' +
        esc(m.content) +
        '<div class="chat-meta">'+esc(m.from_id)+' · '+timeAgo(m.created_at)+'</div>' +
      '</div>';
    }).join('');
    el.scrollTop = el.scrollHeight;
  } catch(e) { console.error('loadActionChat', e); }
}

async function sendActionChat() {
  const input = document.getElementById('action-chat-input');
  if (!input) return;
  const text = input.value.trim();
  if (!text || !currentActionId) return;
  input.value = '';
  try {
    await rpc('action.chat.send', {action_id:currentActionId, from_id:currentProfileId(), content:text});
    loadActionChat(currentActionId);
  } catch(e) { toast('Chat error: '+e.message); }
}

async function startAction(actionId) {
  const comment = await dashboardPrompt('Optional start note for this action:', '');
  if (comment === null) return;
  try {
    await rpc('action.start', {
      action_id: actionId,
      started_by: currentProfileId(),
      comment: comment
    });
    toast('▶ Action started');
    closeModal();
    await Promise.all([loadActions(), loadOperatorQueue(), loadRuntimeEvents()]);
    setTimeout(()=>showActionDetail(actionId), 100);
  } catch(e) { toast('Error: '+e.message); }
}

async function pauseAction(actionId) {
  const comment = await dashboardPrompt('Optional pause note for this action:', '');
  if (comment === null) return;
  try {
    await rpc('action.pause', {
      action_id: actionId,
      paused_by: currentProfileId(),
      comment: comment
    });
    toast('⏸ Action paused');
    closeModal();
    await Promise.all([loadActions(), loadOperatorQueue(), loadRuntimeEvents()]);
    setTimeout(()=>showActionDetail(actionId), 100);
  } catch(e) { toast('Error: '+e.message); }
}

function resolveAction(actionId, resolution) {
  currentActionId = actionId;
  currentResolveType = resolution;
  const overlay = document.getElementById('resolve-overlay');
  const btn = document.getElementById('resolve-confirm-btn');
  if (resolution === 'COMPLETED') {
    document.getElementById('resolve-title').textContent = '✓ Complete Action';
    document.getElementById('resolve-subtitle').textContent = 'Add a completion comment (optional)';
    document.getElementById('resolve-comment').placeholder = 'What was done...';
    btn.textContent = 'Mark Completed';
    btn.className = 'btn-complete';
  } else {
    document.getElementById('resolve-title').textContent = '✗ Fail Action';
    document.getElementById('resolve-subtitle').textContent = 'Explain why the action failed';
    document.getElementById('resolve-comment').placeholder = 'Reason for failure...';
    btn.textContent = 'Mark Failed';
    btn.className = 'btn-fail';
  }
  document.getElementById('resolve-comment').value = '';
  overlay.classList.add('open');
  syncDashboardOverlayLock();
}

function cancelResolve() {
  document.getElementById('resolve-overlay').classList.remove('open');
  currentResolveType = null;
  syncDashboardOverlayLock();
}

async function confirmResolveAction() {
  if (!currentActionId || !currentResolveType) return;
  const comment = document.getElementById('resolve-comment').value.trim();
  try {
    await rpc('action.resolve', {
      action_id: currentActionId,
      resolution: currentResolveType,
      comment: comment,
      resolved_by: currentProfileId()
    });
    toast((currentResolveType==='COMPLETED'?'✓':'✗')+' Action resolved');
    cancelResolve();
    closeModal();
    loadActions();
    loadTasks();
  } catch(e) { toast('Error: '+e.message); }
}

// ── Broadcast ──

// ── Modal ──
function dashboardOverlayIsOpen(id) {
  const el = document.getElementById(id);
  return !!(el && el.classList.contains('open'));
}
function syncDashboardOverlayLock() {
  document.body.style.overflow = (
    dashboardOverlayIsOpen('modal') ||
    dashboardOverlayIsOpen('dialog-modal') ||
    dashboardOverlayIsOpen('delete-confirm') ||
    dashboardOverlayIsOpen('resolve-overlay')
  ) ? 'hidden' : '';
}
function dashboardCloseTopOverlay() {
  if (dashboardOverlayIsOpen('resolve-overlay')) {
    cancelResolve();
    return true;
  }
  if (_dashboardDialogState || dashboardOverlayIsOpen('dialog-modal')) {
    cancelDashboardDialog();
    return true;
  }
  if (dashboardOverlayIsOpen('delete-confirm')) {
    cancelDelete();
    return true;
  }
  if (dashboardOverlayIsOpen('modal')) {
    closeModal();
    return true;
  }
  return false;
}
function openModal(title, body) {
  document.getElementById('modal-title').textContent = title;
  document.getElementById('modal-body').innerHTML = body;
  document.getElementById('modal').classList.add('open');
  syncDashboardOverlayLock();
}
function closeModal() {
  document.getElementById('modal').classList.remove('open');
  syncDashboardOverlayLock();
}
function dashboardPromptShouldUseTextarea(message, defaultValue) {
  const text = String(message || '').toLowerCase();
  const initial = String(defaultValue || '');
  if (initial.length > 120 || initial.indexOf('\n') !== -1) return true;
  return ['body', 'details', 'description', 'summary', 'content', 'json', 'note', 'reason', 'evidence', 'verification'].some(function(token) {
    return text.indexOf(token) !== -1;
  });
}
function dashboardDialogElements() {
  return {
    overlay: document.getElementById('dialog-modal'),
    title: document.getElementById('dialog-title'),
    subtitle: document.getElementById('dialog-subtitle'),
    input: document.getElementById('dialog-input'),
    textarea: document.getElementById('dialog-textarea'),
    confirm: document.getElementById('dialog-confirm'),
    cancel: document.getElementById('dialog-cancel')
  };
}
function closeDashboardDialog() {
  const els = dashboardDialogElements();
  if (els.overlay) els.overlay.classList.remove('open');
  if (els.input) els.input.value = '';
  if (els.textarea) els.textarea.value = '';
  syncDashboardOverlayLock();
}
function resolveDashboardDialog(result) {
  const state = _dashboardDialogState;
  _dashboardDialogState = null;
  closeDashboardDialog();
  if (state && typeof state.resolve === 'function') state.resolve(result);
}
function cancelDashboardDialog() {
  resolveDashboardDialog(null);
}
function confirmDashboardDialog() {
  if (!_dashboardDialogState) return;
  const els = dashboardDialogElements();
  const value = _dashboardDialogState.multiline ? (els.textarea ? els.textarea.value : '') : (els.input ? els.input.value : '');
  resolveDashboardDialog(value);
}
function openDashboardDialog(options) {
  const opts = options || {};
  const els = dashboardDialogElements();
  _dashboardDialogState = {
    resolve: opts.resolve,
    multiline: !!opts.multiline,
    showInput: opts.showInput !== false
  };
  if (els.title) els.title.textContent = String(opts.title || 'Input Required');
  if (els.subtitle) {
    els.subtitle.textContent = String(opts.message || '');
    els.subtitle.style.display = opts.message ? '' : 'none';
  }
  if (els.confirm) els.confirm.textContent = String(opts.confirmLabel || 'OK');
  if (els.cancel) {
    els.cancel.textContent = String(opts.cancelLabel || 'Cancel');
    els.cancel.style.display = opts.showCancel === false ? 'none' : '';
  }
  if (opts.showInput === false) {
    if (els.input) els.input.style.display = 'none';
    if (els.textarea) els.textarea.style.display = 'none';
  } else if (opts.multiline) {
    if (els.input) els.input.style.display = 'none';
    if (els.textarea) {
      els.textarea.style.display = '';
      els.textarea.value = String(opts.defaultValue || '');
      els.textarea.placeholder = String(opts.placeholder || '');
      setTimeout(function() {
        els.textarea.focus();
        els.textarea.select();
      }, 0);
    }
  } else {
    if (els.textarea) els.textarea.style.display = 'none';
    if (els.input) {
      els.input.style.display = '';
      els.input.value = String(opts.defaultValue || '');
      els.input.placeholder = String(opts.placeholder || '');
      setTimeout(function() {
        els.input.focus();
        els.input.select();
      }, 0);
    }
  }
  if (els.overlay) els.overlay.classList.add('open');
  syncDashboardOverlayLock();
}
function dashboardPrompt(message, defaultValue, options) {
  const opts = options || {};
  return new Promise(function(resolve) {
    openDashboardDialog({
      title: opts.title || 'Input Required',
      message: message,
      defaultValue: defaultValue,
      placeholder: opts.placeholder || '',
      confirmLabel: opts.confirmLabel || 'OK',
      cancelLabel: opts.cancelLabel || 'Cancel',
      multiline: typeof opts.multiline === 'boolean' ? opts.multiline : dashboardPromptShouldUseTextarea(message, defaultValue),
      resolve: resolve
    });
  });
}
function dashboardAlert(message, title) {
  return new Promise(function(resolve) {
    openDashboardDialog({
      title: title || 'Notice',
      message: message,
      confirmLabel: 'OK',
      showCancel: false,
      showInput: false,
      multiline: false,
      resolve: function() { resolve(true); }
    });
  });
}
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    if (dashboardCloseTopOverlay()) {
      e.preventDefault();
      return;
    }
    closeProfileMenu();
    if (activeTabPanelId() === 'panel-graph' && _graphCamPrev) {
      graphCinematicRelease(); // ease the camera back from a node focus
    }
  }
  if (e.key === 'Enter' && _dashboardDialogState && !_dashboardDialogState.multiline) {
    e.preventDefault();
    confirmDashboardDialog();
  }
  if ((e.key === 'Enter' && (e.metaKey || e.ctrlKey)) && _dashboardDialogState && _dashboardDialogState.multiline) {
    e.preventDefault();
    confirmDashboardDialog();
  }
});
document.addEventListener('click', e => { if (!e.target.closest('.profile-wrap')) closeProfileMenu(); });

// ── Filters ──
let activeFilter = 'all';
function filterTasks(priority, btn) {
  activeFilter = priority;
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  applyFilters();
}
function filterTasksBySearch() { applyFilters(); }
function applyFilters() {
  const q = (document.getElementById('task-search').value||'').toLowerCase();
  document.querySelectorAll('.task-card').forEach(card => {
    const title = (card.querySelector('.task-title')||{}).textContent||'';
    const meta = (card.querySelector('.task-meta')||{}).textContent||'';
    const tags = (card.querySelector('.task-tags')||{}).textContent||'';
    const text = (title+' '+meta+' '+tags).toLowerCase();
    const matchPriority = activeFilter==='all' || text.includes(activeFilter);
    const matchSearch = !q || text.includes(q);
    card.style.display = (matchPriority && matchSearch) ? '' : 'none';
  });
}

// ── Refresh ──
let _refreshInFlight = false;
let _refreshPending = false;
let _refreshRetryTimer = null;

function setHeaderRefreshTimestamp() {
  const el = document.getElementById('last-refresh');
  if (el) el.textContent = new Date().toLocaleTimeString();
}

function setHeaderConnectionState(isConnected) {
  const dot = document.getElementById('sse-dot');
  const status = document.getElementById('conn-status');
  if (dot) dot.className = 'sse-dot ' + (isConnected ? 'connected' : 'disconnected');
  if (status) status.textContent = isConnected ? 'Live' : 'Reconnecting...';
}

function activeTabPanelId() {
  return document.querySelector('.tab-panel.active')?.id || '';
}

function scheduleRefreshRetry() {
  if (_refreshRetryTimer) return;
  _refreshRetryTimer = setTimeout(function() {
    _refreshRetryTimer = null;
    if (!TOKEN || activeTabPanelId() === 'panel-graph') return;
    refresh().catch(function(err) { console.error('refresh retry', err); });
  }, 3000);
}

async function refresh() {
  if (_refreshInFlight) {
    _refreshPending = true;
    return;
  }
  _refreshInFlight = true;
  try {
    setHeaderRefreshTimestamp();
    await loadAgents();
    await loadHumans();
    await Promise.all([loadSessions(), loadDocs(), loadMemory(), loadTasks(), loadFeed(), loadMessages(), loadTools(), loadActions(), loadOperatorQueue(), loadClaims(), loadExecutionRuns(), loadPolicies(), loadRuntimeEvents(), loadInstrumentation(), loadTensions(), loadCompaction(), loadVault(), loadProjects(), loadLimitGroups(), loadWorkspaceSecurity(), loadSecurityLogs(), loadNews(), loadRPCMethodCount()]);
  } finally {
    _refreshInFlight = false;
    const shouldRetry = _refreshPending && activeTabPanelId() !== 'panel-graph';
    _refreshPending = false;
    if (shouldRetry) scheduleRefreshRetry();
  }
}

// ── Projects ──
let projectsCache = [];
async function loadProjects() {
  try {
    const r = await rpc('project.list', {workspace_id:WS_ID});
    projectsCache = r.projects || [];
    document.getElementById('projects-count').textContent = projectsCache.length;
    document.getElementById('s-projects').textContent = projectsCache.length;

    // Update project filter dropdown in Tasks tab
    const filter = document.getElementById('task-project-filter');
    if (filter) {
      const prevVal = filter.value;
      filter.innerHTML = '<option value="">All Projects</option>' + projectsCache.map(p => '<option value="'+esc(p.project_id)+'">'+esc(p.title)+' ('+p.task_count+')</option>').join('');
      filter.value = prevVal;
    }

    // Update project dropdown in create task form
    const ctProject = document.getElementById('ct-project');
    if (ctProject) {
      const cpv = ctProject.value;
      ctProject.innerHTML = '<option value="">— No project —</option>' + projectsCache.map(p => '<option value="'+esc(p.project_id)+'">'+esc(p.title)+'</option>').join('');
      ctProject.value = cpv;
    }

    // Render project cards filtered by active tab
    const el = document.getElementById('projects-list');
    const activeTab = document.querySelector('.project-status-tab.active');
    const filterStatus = activeTab ? activeTab.dataset.status : 'ACTIVE';
    const filtered = projectsCache.filter(p => p.status === filterStatus);

    // Update counts on tabs
    const activeCount = projectsCache.filter(p => p.status === 'ACTIVE').length;
    const archivedCount = projectsCache.filter(p => p.status === 'ARCHIVED').length;
    document.querySelectorAll('.project-status-tab').forEach(tab => {
      if (tab.dataset.status === 'ACTIVE') tab.textContent = 'Active (' + activeCount + ')';
      else tab.textContent = 'Archived (' + archivedCount + ')';
    });

    if (!filtered.length) { el.innerHTML = '<div class="empty">' + (filterStatus === 'ACTIVE' ? 'No active projects. Create one to group your tasks!' : 'No archived projects.') + '</div>'; return; }
    el.innerHTML = filtered.map(p => {
      const isActive = p.status === 'ACTIVE';
      const statusBg = isActive ? 'rgba(78,166,116,0.15)' : 'rgba(139,138,135,0.15)';
      const statusColor = isActive ? 'var(--green)' : 'var(--muted)';
      const statusIcon = isActive ? '' : '';
      return '<div class="card" style="margin-bottom:10px;cursor:pointer;padding:14px 16px;transition:border-color 0.2s" onmouseover="this.style.borderColor=\'var(--accent)\'" onmouseout="this.style.borderColor=\'var(--border)\'" ' + dashboardAction(function(dashboardEvent){showProjectDetail((p.project_id))}) + '>' +
        '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px">' +
          '<div style="flex:1;min-width:0">' +
            '<div style="font-weight:600;font-size:14px;margin-bottom:2px">'+esc(p.title)+'</div>' +
            '<div style="font-family:monospace;font-size:11px;color:var(--muted)">'+esc(p.project_id)+'</div>' +
            (p.description ? '<div style="color:var(--muted);font-size:11px;margin-top:6px;line-height:1.4">'+esc(p.description)+'</div>' : '') +
          '</div>' +
          '<div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px;flex-shrink:0">' +
            '<span style="padding:2px 8px;border-radius:4px;font-size:10px;font-weight:600;background:'+statusBg+';color:'+statusColor+'">'+statusIcon+' '+esc(p.status)+'</span>' +
            '<span style="font-size:11px;color:var(--muted)">'+p.task_count+' tasks</span>' +
          '</div>' +
        '</div>' +
        '<div style="display:flex;gap:12px;margin-top:8px;font-size:10px;color:var(--muted);border-top:1px solid var(--border);padding-top:8px">' +
          '<span>'+esc(p.created_by)+'</span>' +
          '<span>'+timeAgo(p.created_at)+'</span>' +
        '</div>' +
      '</div>';
    }).join('');
  } catch(e) { console.error('loadProjects', e); }
}

function toggleCreateProject() {
  document.getElementById('create-project-form').classList.toggle('open');
}

async function submitNewProject() {
  const id = document.getElementById('cp-id').value.trim();
  const title = document.getElementById('cp-title').value.trim();
  const desc = document.getElementById('cp-desc').value.trim();
  const statusEl = document.getElementById('cp-status');
  const actorID = currentProfileId();
  if (!actorID) { statusEl.textContent = 'Select a profile before creating projects'; statusEl.className = 'msg-status err'; return; }
  if (!id || !title) { statusEl.textContent = '! ID and Title are required'; statusEl.className = 'msg-status err'; return; }
  try {
    statusEl.textContent = 'Creating...';
    statusEl.className = 'msg-status';
    await rpc('project.create', {project_id:id, workspace_id:WS_ID, title:title, description:desc, created_by:actorID});
    statusEl.textContent = '✓ Created!';
    statusEl.className = 'msg-status ok';
    document.getElementById('cp-id').value = '';
    document.getElementById('cp-title').value = '';
    document.getElementById('cp-desc').value = '';
    document.getElementById('create-project-form').classList.remove('open');
    await loadProjects();
  } catch(e) {
    const msg = (e && e.message) ? e.message : 'Unknown error';
    statusEl.textContent = '✗ ' + msg;
    statusEl.className = 'msg-status err';
  }
}

function projectCoordinationArray(value) {
  return Array.isArray(value) ? value : [];
}

function projectCoordinationStatusTone(state) {
  const s = String(state || '').toUpperCase();
  if (['READY','ACTIVE','SATISFIED','ACCEPTED','COMPLETED','DONE'].includes(s)) return 'background:rgba(78,166,116,.14);color:var(--green)';
  if (['CLAIMED','READY_FOR_REVIEW','PROPOSED','PENDING','CREATED'].includes(s)) return 'background:rgba(168,85,247,.14);color:var(--accent)';
  if (['BLOCKED','FAILED','REJECTED','STALE'].includes(s)) return 'background:rgba(224,106,106,.14);color:var(--red)';
  if (['CANCELED','CANCELLED','ARCHIVED','ABANDONED'].includes(s)) return 'background:rgba(139,138,135,.16);color:var(--muted)';
  return 'background:rgba(139,138,135,.14);color:var(--muted)';
}

function projectCoordinationBadge(value) {
  const label = String(value || 'unknown');
  return '<span style="display:inline-flex;align-items:center;min-height:20px;padding:2px 7px;border-radius:999px;font-size:10px;font-weight:700;'+projectCoordinationStatusTone(label)+'">'+esc(label)+'</span>';
}

function projectCoordinationCountTile(label, value) {
  return '<div style="border:1px solid var(--border);border-radius:8px;padding:8px;background:rgba(19,24,33,.55)"><div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em">'+esc(label)+'</div><div style="font-size:18px;font-weight:700;margin-top:2px">'+esc(String(value || 0))+'</div></div>';
}

function projectCoordinationNumber(value, fallback) {
  const parsed = Number(value);
  if (Number.isFinite(parsed)) return parsed;
  return fallback;
}

function projectCoordinationJSArg(value) {
  return esc(String(value || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\r/g, '\\r').replace(/\n/g, '\\n'));
}

function projectCoordinationPathset(item) {
  if (Array.isArray(item.pathset) && item.pathset.length) return item.pathset.map(x => String(x || '').trim()).filter(Boolean);
  const raw = String(item.pathset_json || '').trim();
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed.map(x => String(x || '').trim()).filter(Boolean);
    if (parsed && Array.isArray(parsed.paths)) return parsed.paths.map(x => String(x || '').trim()).filter(Boolean);
  } catch(e) {}
  return [];
}

function projectTaskTagList(task) {
  if (Array.isArray(task.tags)) return task.tags.map(x => String(x || '').trim()).filter(Boolean);
  if (typeof task.tags === 'string') return task.tags.split(',').map(x => x.trim()).filter(Boolean);
  return [];
}

function projectPatchQueueFollowupTasks(projectId, tasks) {
  const normalizedProject = String(projectId || '').trim();
  return projectCoordinationArray(tasks).filter(task => {
    if (String(task.project_id || '').trim() !== normalizedProject) return false;
    const tags = projectTaskTagList(task).map(t => t.toLowerCase());
    if (tags.includes('patch-queue')) return true;
    const text = String((task.title || '') + ' ' + (task.description || '')).toLowerCase();
    return text.includes('integration candidate') && (text.includes('patch queue') || text.includes('decision follow-up'));
  });
}

function renderProjectMiniTaskList(tasks, emptyText) {
  const list = projectCoordinationArray(tasks).slice(0, 8);
  if (!list.length) return '<div class="empty" style="margin-top:8px">'+esc(emptyText || 'No tasks')+'</div>';
  return '<div style="display:grid;gap:6px;margin-top:8px">'+list.map(task => {
    const status = task.claim_status || task.status || 'PENDING';
    const tags = projectTaskTagList(task);
    return '<div class="task-card project-open-task" style="margin:0">' +
      '<div class="task-title">'+esc(task.title || task.task_id)+'</div>' +
      '<div class="task-meta">'+projectCoordinationBadge(status)+' <span style="margin-left:6px">'+esc(task.project_lane || 'unlaned')+'</span></div>' +
      (tags.length ? '<div class="task-tags">'+tags.slice(0, 6).map(tag => '<span class="task-tag">'+esc(tag)+'</span>').join('')+'</div>' : '') +
    '</div>';
  }).join('') + '</div>';
}

function renderProjectPatchQueueItems(projectId, items) {
	const list = projectCoordinationArray(items);
	if (!list.length) return '<div class="empty" style="margin-top:8px">No patch queue items yet.</div>';
	return '<div style="display:grid;gap:8px;margin-top:8px">' + list.slice(0, 10).map(item => {
		const paths = projectCoordinationPathset(item);
		const state = item.state || 'UNKNOWN';
		const hasReviewerAdvisory = !!String(item.reviewer_advisory_digest || '').trim();
		const hasOperatorEnablement = !!String(item.operator_enablement_digest || '').trim();
		const canOperatorEnable = String(state || '').toUpperCase() === 'CLAIMED' && hasReviewerAdvisory && !hasOperatorEnablement && !!String(item.claim_token || '').trim();
		let html = '<div style="border:1px solid var(--border);border-radius:8px;padding:10px;background:rgba(19,24,33,.55)">';
		html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start"><div style="min-width:0">';
		html += '<div style="font-weight:700;font-size:12px">'+esc(item.branch_id || item.item_id || 'patch item')+'</div>';
    html += '<div style="font-family:monospace;font-size:10px;color:var(--muted);overflow-wrap:anywhere">'+esc(item.queue_id || '')+' / '+esc(item.item_id || '')+'</div>';
    html += '</div>'+projectCoordinationBadge(state)+'</div>';
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:7px;margin-top:8px;font-size:11px;color:var(--muted)">';
    html += '<div><strong>Repo</strong><br>'+esc(item.repo_id || '')+'</div>';
    html += '<div><strong>Claimed by</strong><br>'+esc(item.claimed_by || '-')+'</div>';
    html += '<div><strong>Head</strong><br><code style="font-size:10px">'+esc(item.head_sha || '-')+'</code></div>';
		html += '<div><strong>Review doc</strong><br>'+esc(item.review_doc_key || '-')+'</div>';
		html += '</div>';
		html += '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:8px;font-size:10px">';
		html += projectCoordinationBadge(hasReviewerAdvisory ? 'reviewer advisory' : 'review pending');
		html += projectCoordinationBadge(hasOperatorEnablement ? 'operator enabled' : 'operator pending');
		html += '</div>';
		if (paths.length) html += '<div style="margin-top:8px;font-size:10px;color:var(--muted)">scope: '+paths.slice(0, 6).map(esc).join(', ')+'</div>';
		if (item.decision_summary) html += '<div style="margin-top:8px;font-size:11px;line-height:1.45;color:var(--text)">'+esc(item.decision_summary)+'</div>';
		if (canOperatorEnable) {
			html += '<div style="margin-top:10px;display:flex;justify-content:flex-end">';
			html += '<button class="btn-accent" style="padding:6px 12px;font-size:11px" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();enableProjectPatchQueueOperator((projectId),(item.queue_id),(item.item_id),(item.claim_token))}) + '>Enable Operator Gate</button>';
			html += '</div>';
		}
		html += '</div>';
		return html;
	}).join('') + '</div>';
}

async function enableProjectPatchQueueOperator(projectId, queueId, itemId, claimToken) {
	const actorID = currentProfileId();
	if (!actorID || currentProfileType() !== 'human') {
		toast('Select a human operator profile first');
		return;
	}
	if (!String(claimToken || '').trim()) {
		toast('Claim token is missing for this patch queue item');
		return;
	}
	const reason = await dashboardPrompt('Operator enablement reason:', 'Reviewed advisory and enabling the final operator gate.', {
		title: 'Enable Patch Queue Item',
		confirmLabel: 'Enable'
	});
	if (reason === null) return;
	try {
		await rpc('operator.patch_queue.enable', {
			workspace_id: WS_ID,
			project_id: projectId,
			actor_id: actorID,
			queue_id: queueId,
			item_id: itemId,
			claim_token: claimToken,
			reason: String(reason || '').trim()
		});
		toast('Operator enablement recorded');
		await showProjectDetail(projectId);
		await loadProjects();
	} catch (e) {
		toast('Operator enablement failed: ' + (e.message || e));
	}
}

function mergeProjectCoordinationTasks(tasks) {
	const incoming = projectCoordinationArray(tasks).filter(task => task && task.task_id);
	if (!incoming.length) return;
  const byTaskID = new Map();
  (_cachedTasks || []).forEach(task => {
    const taskID = String((task && task.task_id) || '').trim();
    if (taskID) byTaskID.set(taskID, task);
  });
  incoming.forEach(task => byTaskID.set(String(task.task_id || '').trim(), task));
  _cachedTasks = Array.from(byTaskID.values());
}

function renderProjectCoordinationPanel(projectId, coordinationResult) {
  if (coordinationResult && coordinationResult._error) {
    return '<div style="margin-bottom:14px;border:1px solid rgba(224,106,106,.24);border-radius:8px;padding:10px;background:rgba(224,106,106,.08);font-size:12px;color:var(--red)">Coordination snapshot failed: '+esc(coordinationResult._error)+'</div>';
  }
  const coordination = coordinationResult && coordinationResult.coordination ? coordinationResult.coordination : coordinationResult;
  if (!coordination || !coordination.project) {
    return '<div style="margin-bottom:14px;border:1px solid var(--border);border-radius:8px;padding:10px;background:rgba(19,24,33,.55);font-size:12px;color:var(--muted)">No coordination snapshot is available yet.</div>';
  }
  const roles = projectCoordinationArray(coordination.roles);
  const repos = projectCoordinationArray(coordination.repositories);
  const checkouts = projectCoordinationArray(coordination.checkouts);
  const branches = projectCoordinationArray(coordination.branches);
  const patchItems = projectCoordinationArray(coordination.patch_queue_items);
  const projectTasks = projectCoordinationArray(coordination.tasks).filter(task => String(task.project_id || '').trim() === String(projectId || '').trim());
  const followups = projectPatchQueueFollowupTasks(projectId, projectTasks);
  const activeRoles = roles.filter(role => String(role.status || '').toUpperCase() === 'ACTIVE');
  const readyRepos = repos.filter(repo => String(repo.repo_status || repo.status || '').toUpperCase() === 'READY');
  const activeCheckouts = checkouts.filter(co => String(co.derived_status || co.status || '').toUpperCase() === 'ACTIVE');
  const openPatchItems = patchItems.filter(item => !['ACCEPTED','REJECTED','BLOCKED','CANCELED','CANCELLED'].includes(String(item.state || '').toUpperCase()));
  let html = '<div style="margin-bottom:14px;border:1px solid var(--border);border-radius:8px;padding:12px;background:rgba(19,24,33,.55)">';
  html += '<div style="display:flex;justify-content:space-between;gap:8px;align-items:center;margin-bottom:10px"><strong>Project Coordination</strong><span style="font-size:10px;color:var(--muted)">version '+esc(coordination.coordination_version || 'n/a')+'</span></div>';
  html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:8px;margin-bottom:12px">';
  html += projectCoordinationCountTile('open tasks', projectCoordinationNumber(coordination.open_task_count, projectTasks.length));
  html += projectCoordinationCountTile('active roles', activeRoles.length);
  html += projectCoordinationCountTile('ready repos', readyRepos.length);
  html += projectCoordinationCountTile('active checkouts', activeCheckouts.length);
  html += projectCoordinationCountTile('branches', branches.length);
  html += projectCoordinationCountTile('patch queue', patchItems.length);
  html += '</div>';
  if (coordination.profile || coordination.gate_status || coordination.strategic_lead) {
    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px;font-size:11px;color:var(--muted);margin-bottom:12px">';
    if (coordination.profile) html += '<div><strong>Phase</strong><br>'+projectCoordinationBadge(coordination.profile.current_phase || 'unknown')+'</div>';
    if (coordination.gate_status) html += '<div><strong>Implementation gate</strong><br>'+projectCoordinationBadge(coordination.gate_status.implementation_gate_state || coordination.gate_status.gate_state || 'unknown')+'</div>';
    if (coordination.strategic_lead) html += '<div><strong>Strategic lead</strong><br>'+esc(coordination.strategic_lead.agent_id || '-')+' '+projectCoordinationBadge(coordination.strategic_lead.status || '')+'</div>';
    html += '</div>';
  }
  if (repos.length) {
    html += '<div style="margin-top:10px"><strong style="font-size:12px">Repositories</strong><div style="display:grid;gap:6px;margin-top:6px">';
    html += repos.slice(0, 5).map(repo => '<div style="font-size:11px;color:var(--muted);display:flex;justify-content:space-between;gap:8px"><span style="overflow-wrap:anywhere">'+esc(repo.repo_id || repo.name || 'repo')+' '+(repo.remote_url ? '<code style="font-size:10px">'+esc(repo.remote_url)+'</code>' : '')+'</span>'+projectCoordinationBadge(repo.repo_status || repo.status || 'unknown')+'</div>').join('');
    html += '</div></div>';
  }
  if (branches.length) {
    html += '<div style="margin-top:10px"><strong style="font-size:12px">Branches</strong><div style="display:grid;gap:6px;margin-top:6px">';
    html += branches.slice(0, 8).map(branch => '<div style="font-size:11px;color:var(--muted);display:flex;justify-content:space-between;gap:8px"><span style="overflow-wrap:anywhere">'+esc(branch.branch_name || branch.branch_id || 'branch')+' <span style="color:var(--muted)">by '+esc(branch.agent_id || '-')+'</span></span>'+projectCoordinationBadge(branch.status || 'unknown')+'</div>').join('');
    html += '</div></div>';
  }
	html += '<div style="margin-top:12px"><strong style="font-size:12px">Patch Queue</strong>';
	if (openPatchItems.length) html += '<span style="font-size:10px;color:var(--muted);margin-left:6px">'+openPatchItems.length+' open</span>';
	html += renderProjectPatchQueueItems(projectId, patchItems) + '</div>';
  html += '<div style="margin-top:12px"><strong style="font-size:12px">Patch Queue Follow-up Tasks</strong>';
  html += renderProjectMiniTaskList(followups, 'No validation/revision follow-up tasks are visible yet.') + '</div>';
  html += '</div>';
  return html;
}

async function showProjectDetail(projectId) {
  openModal('Project', '<div class="empty">Loading...</div>');
  try {
    const [p, coordinationResult] = await Promise.all([
      rpc('project.get', {workspace_id: WS_ID, project_id: projectId}),
      rpc('project.coordination.get', {workspace_id: WS_ID, project_id: projectId}).catch(e => ({_error: e.message || 'project.coordination.get failed'}))
    ]);
    const coordination = coordinationResult && coordinationResult.coordination ? coordinationResult.coordination : coordinationResult;
    if (coordination && !coordinationResult._error) mergeProjectCoordinationTasks(coordination.tasks);

    let html = '';
    // ID
    html += '<div style="font-size:11px;color:var(--muted);margin-bottom:14px">ID: <code style="background:var(--surface);padding:2px 6px;border-radius:4px;font-size:12px">'+esc(p.project_id)+'</code></div>';
    html += renderProjectCoordinationPanel(p.project_id, coordinationResult);

    // Title
    html += '<div style="margin-bottom:14px">';
    html += '<label style="font-size:11px;color:var(--muted);display:block;margin-bottom:4px;font-weight:600">Title</label>';
    html += '<input id="pdetail-title" value="'+esc(p.title)+'" style="width:100%;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:14px;font-weight:600;font-family:var(--font);box-sizing:border-box">';
    html += '</div>';

    // Status
    html += '<div style="margin-bottom:14px">';
    html += '<label style="font-size:11px;color:var(--muted);display:block;margin-bottom:4px;font-weight:600">Status</label>';
    html += '<select id="pdetail-status" style="width:100%;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:12px;font-family:var(--font);box-sizing:border-box">';
    html += '<option value="ACTIVE"'+(p.status==='ACTIVE'?' selected':'')+'>ACTIVE</option>';
    html += '<option value="ARCHIVED"'+(p.status==='ARCHIVED'?' selected':'')+'>ARCHIVED</option>';
    html += '</select>';
    html += '</div>';

    // Description
    html += '<div style="margin-bottom:14px">';
    html += '<label style="font-size:11px;color:var(--muted);display:block;margin-bottom:4px;font-weight:600">Description</label>';
    html += '<textarea id="pdetail-desc" rows="3" style="width:100%;padding:8px 12px;border-radius:6px;border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:12px;font-family:var(--font);resize:vertical;box-sizing:border-box">'+esc(p.description)+'</textarea>';
    html += '</div>';

    // Save All button + feedback
    html += '<div style="display:flex;gap:8px;align-items:center;margin-bottom:14px">';
    html += '<button class="btn-accent" style="padding:6px 20px;font-size:12px" ' + dashboardAction(function(dashboardEvent){saveAllProjectFields((p.project_id))}) + '>Save Changes</button>';
    html += '<span id="pdetail-feedback" style="font-size:11px"></span>';
    html += '</div>';

    // Meta info
    html += '<hr style="border-color:var(--border);margin:12px 0">';
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px;color:var(--muted);margin-bottom:14px">';
    html += '<div><strong>Created by</strong><br>'+esc(p.created_by)+'</div>';
    html += '<div><strong>Tasks</strong><br><span style="font-size:16px;font-weight:600;color:var(--text)">'+p.task_count+'</span></div>';
    html += '<div><strong>Created</strong><br>'+timeAgo(p.created_at)+'</div>';
    html += '<div><strong>Updated</strong><br>'+timeAgo(p.updated_at)+'</div>';
    html += '</div>';

    // Action buttons
    html += '<button class="btn-accent" style="padding:8px 20px;font-size:12px;width:100%;margin-bottom:8px" ' + dashboardAction(function(dashboardEvent){closeModal();switchTab('tasks');document.getElementById('task-project-filter').value=(p.project_id);filterTasksByProject()}) + '>View Tasks ('+p.task_count+')</button>';
    html += '<button style="padding:8px 20px;font-size:12px;width:100%;background:rgba(224,106,106,0.1);color:var(--red);border:1px solid rgba(224,106,106,0.3);border-radius:6px;cursor:pointer;font-family:var(--font);transition:background 0.2s" onmouseover="this.style.background=\'rgba(224,106,106,0.2)\'" onmouseout="this.style.background=\'rgba(224,106,106,0.1)\'" ' + dashboardAction(function(dashboardEvent){deleteProject((p.project_id),(p.title))}) + '>Delete Project</button>';

    const modalBody = document.getElementById('modal-body');
    modalBody.innerHTML = html;
    const projectTasks = coordination ? projectCoordinationArray(coordination.tasks).filter(task => String(task.project_id || '').trim() === String(p.project_id || '').trim()) : [];
    bindTaskDetailElements(modalBody, projectPatchQueueFollowupTasks(p.project_id, projectTasks).slice(0, 8), '.project-open-task');
  } catch(e) {
    document.getElementById('modal-body').innerHTML = '<div class="empty">'+esc(e.message||'Failed to load project')+'</div>';
  }
}

async function saveAllProjectFields(projectId) {
  const fb = document.getElementById('pdetail-feedback');
  const actorID = currentProfileId();
  if (!actorID) {
    fb.textContent = 'Select a profile before updating projects';
    fb.style.color = 'var(--red)';
    return;
  }
  try {
    fb.textContent = 'Saving...';
    fb.style.color = 'var(--muted)';
    await rpc('project.update', {
      workspace_id: WS_ID,
      project_id: projectId,
      title: document.getElementById('pdetail-title').value.trim(),
      description: document.getElementById('pdetail-desc').value,
      status: document.getElementById('pdetail-status').value,
      actor_id: actorID
    });
    fb.textContent = '✓ Saved!';
    fb.style.color = 'var(--green)';
    await loadProjects();
  } catch(e) {
    fb.textContent = '✗ ' + (e.message || 'Save failed');
    fb.style.color = 'var(--red)';
  }
}

let currentProjectStatusTab = 'ACTIVE';
function switchProjectTab(status) {
  currentProjectStatusTab = status;
  document.querySelectorAll('.project-status-tab').forEach(t => t.classList.toggle('active', t.dataset.status === status));
  loadProjects();
}

async function deleteProject(projectId, title) {
  const btn = event.target;
  if (btn.dataset.confirm !== 'yes') {
    btn.dataset.confirm = 'yes';
    btn.textContent = '! Click again to confirm deletion';
    btn.style.background = 'rgba(224,106,106,0.3)';
    btn.style.fontWeight = '700';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete Project'; btn.style.background = 'rgba(224,106,106,0.1)'; btn.style.fontWeight = ''; }, 4000);
    return;
  }
  btn.textContent = 'Deleting...';
  btn.style.pointerEvents = 'none';
  const actorID = currentProfileId();
  if (!actorID) {
    btn.textContent = 'Select a profile before deleting projects';
    btn.style.pointerEvents = '';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete Project'; btn.style.background = 'rgba(224,106,106,0.1)'; btn.style.fontWeight = ''; }, 3000);
    return;
  }
  try {
    await rpc('project.delete', {workspace_id: WS_ID, project_id: projectId, actor_id: actorID});
    closeModal();
    await loadProjects();
  } catch(e) {
    btn.textContent = '✗ ' + (e.message || 'Failed');
    btn.style.pointerEvents = '';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete Project'; btn.style.background = 'rgba(224,106,106,0.1)'; btn.style.fontWeight = ''; }, 3000);
  }
}

function filterTasksByProject() {
  const projectId = document.getElementById('task-project-filter').value;
  document.querySelectorAll('.task-card').forEach(card => {
    if (!projectId) { card.style.display = ''; return; }
    card.style.display = card.dataset.project === projectId ? '' : 'none';
  });
}

// ── SSE Real-time ──
function disconnectSSE() {
  if (sseAbortController) {
    try { sseAbortController.abort(); } catch (e) {}
    sseAbortController = null;
  }
  if (sse) {
    try { sse.close(); } catch (e) {}
    sse = null;
  }
}

let refreshDebounceTimer = null;
let lastLiveRefreshAt = 0;
function debouncedRefresh() {
  if (refreshDebounceTimer) clearTimeout(refreshDebounceTimer);
  refreshDebounceTimer = setTimeout(() => {
    if (activeTabPanelId() !== 'panel-graph') {
      const now = Date.now();
      if (now-lastLiveRefreshAt < 3000) return;
      lastLiveRefreshAt = now;
      refresh();
    }
  }, 1500);
}

const LIVE_SSE_EVENTS = new Set(['agent.message','agent.request','agent.request.claimed','agent.response','action.created','action.started','action.paused','action.resolved','action.chat','task.created','task.closed','task.claimed','task.blocked','task.completed','task.released','node.claimed','node.released','node.completed','agent.update','agent.deleted','workspace.change','project.created','project.updated','project.deleted','project.lead.claimed','project.lead.renewed','project.lead.released','project.lead.transferred','project.lead.changed','project.role.assigned','project.role.released','project.role.changed','governance.challenge.raised','governance.challenge.defended','governance.vote.cast','governance.challenge.resolved','governance.challenge.changed','project.repository.upserted','project.repository.changed','project.checkout.registered','project.checkout.changed','project.branch.registered','project.branch.changed','project.patch_queue.submitted','project.patch_queue.claimed','project.patch_queue.released','project.patch_queue.accepted','project.patch_queue.rejected','project.patch_queue.blocked','project.patch_queue.canceled','project.patch_queue.changed','agent.session.start','agent.session.status','agent.session.blocked','agent.session.decision_needed','agent.session.keepalive','agent.session.end','agent.session.takeover','workspace.memory.recorded','workspace.memory.removed','workspace.memory.restored','workspace.memory.node.touched','workspace_artifact.created','workspace.ops.updated','workspace.ops.resolved','workspace.ops.escalated','workspace.claim.written','workspace.claim.review_requested','workspace.claim.confirmed','workspace.claim.disputed','workspace.claim.superseded','workspace.claim.stale','workspace.claim.review_escalated','workspace.claim.archived','workspace.execution.run','workspace.execution.step','workspace.policy.put','tool.call.executed','tool.call.denied','tool.call.approval_required','cluster.metric_snapshot','limits.group.created','limits.group.updated','limits.group.deleted','vault.entry.created','vault.entry.updated','vault.entry.deleted','vault.entry.read','vault.entries.listed','news.published','news.deleted','cluster.control_advisory_snapshot','cluster.unified_control_advisory_snapshot','cluster.unified_control_effective_snapshot','cluster.corridor_readiness_snapshot','cluster.corridor_fit_snapshot','cluster.corridor_basis_snapshot','cluster.corridor_ownership_snapshot','cluster.control_state_snapshot','cluster.control_state_ticked','cluster.control_state_stabilized','tension.refreshed','tension.detected','tension.updated','tension.confirmed','tension.discarded','tension.archived','tension.resolved','tension.dormant','tension.active','tension.recovered','tension.emergent','tension.condensed','tension.dependency.added','tension.dependency.removed','tension.agent.attached','tension.agent.detached']);

function handleSSEPayload(evtType, data) {
  if (!LIVE_SSE_EVENTS.has(evtType)) return;
  if (data && typeof data.summary === 'string' && data.summary.trim()) toast(data.summary.trim(), evtType);
  if (activeTabPanelId() === 'panel-graph') {
    if (GRAPH_RELEVANT_SSE_EVENTS.has(evtType)) {
      graphNoteActivityFromEvent(evtType, data); // ripple where the work happens
      applyGraphLiveHint(evtType, data);
      triggerGraphSync(evtType === 'agent.update' ? 900 : (evtType === 'task.created' ? 140 : 250));
    }
  } else {
    debouncedRefresh();
  }
}

function scheduleSSERetry() {
  setHeaderConnectionState(false);
  disconnectSSE();
  setTimeout(connectSSE, sseRetryMs);
  sseRetryMs = Math.min(sseRetryMs * 2, 30000);
}

// Background tabs can have their SSE fetch-stream silently suspended by the
// browser without firing an error, so on resume we revive the stream if it is
// disconnected or has gone quiet (no events/keep-alive) for too long.
const SSE_STALE_MS = 45000;
function ensureLiveConnection() {
  if (!TOKEN) return;
  const stale = !sseAbortController || (Date.now() - lastSSEActivityAt > SSE_STALE_MS);
  if (stale) {
    sseRetryMs = 1000;
    connectSSE();
  }
}

async function connectSSE() {
  if (!TOKEN) return;
  disconnectSSE();
  const controller = new AbortController();
  sseAbortController = controller;
  sse = { close: () => controller.abort() };
  const url = window.location.origin + '/events?workspace_id='+encodeURIComponent(WS_ID);

  try {
    const resp = await fetch(url, {
      method: 'GET',
      headers: {
        'Authorization': 'Bearer ' + TOKEN,
        'Accept': 'text/event-stream',
        'Cache-Control': 'no-cache'
      },
      credentials: 'same-origin',
      signal: controller.signal
    });
    if (!resp.ok) throw new Error('SSE HTTP ' + resp.status);
    if (!resp.body) throw new Error('SSE body unavailable');
    if (sseAbortController !== controller) return;

    setHeaderConnectionState(true);
    sseRetryMs = 1000;
    lastSSEActivityAt = Date.now();

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let currentType = '';
    let dataLines = [];

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      lastSSEActivityAt = Date.now();
      buffer += decoder.decode(value, { stream: true });

      let newlineIdx = buffer.indexOf('\n');
      while (newlineIdx !== -1) {
        let line = buffer.slice(0, newlineIdx);
        buffer = buffer.slice(newlineIdx + 1);
        if (line.endsWith('\r')) line = line.slice(0, -1);

        if (line === '') {
          if (currentType && dataLines.length > 0) {
            try {
              handleSSEPayload(currentType, JSON.parse(dataLines.join('\n')));
            } catch (err) {
              console.error('SSE parse', err);
            }
          }
          currentType = '';
          dataLines = [];
          newlineIdx = buffer.indexOf('\n');
          continue;
        }

        if (line.startsWith(':')) {
          newlineIdx = buffer.indexOf('\n');
          continue;
        }
        if (line.startsWith('event:')) {
          currentType = line.slice(6).trim();
        } else if (line.startsWith('data:')) {
          dataLines.push(line.slice(5).trimStart());
        }
        newlineIdx = buffer.indexOf('\n');
      }
    }

    throw new Error('SSE connection closed');
  } catch (err) {
    if (controller.signal.aborted || sseAbortController !== controller) return;
    console.error('SSE stream', err);
    scheduleSSERetry();
  }
}


// Keyboard accessibility: make the tab bar focusable + Enter/Space activatable.
(function(){
  document.querySelectorAll('.tabs .tab').forEach(function(t){
    t.setAttribute('tabindex','0');
    t.setAttribute('role','tab');
    t.addEventListener('keydown', function(e){
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); t.click(); }
    });
  });
})();

// ── Command palette (Cmd/Ctrl+K) ──
let _cmdkItems = [], _cmdkFiltered = [], _cmdkSel = 0;
function buildCmdkItems(){
  const items = [];
  document.querySelectorAll('.tabs .tab').forEach(function(t){
    const name = t.dataset.tab;
    const label = (t.childNodes[0] ? t.childNodes[0].textContent : t.textContent).trim();
    if (name) items.push({kind:'tab', label: label, hint:'', run:function(){ switchTab(name); }});
  });
  try { (agentsCache||[]).forEach(function(a){ items.push({kind:'agent', label:(a.display_name||a.agent_id), hint:String(a.agent_id||''), run:function(){ switchTab('overview'); showAgentDetail(a.agent_id); }}); }); } catch(e){}
  try { (projectsCache||[]).forEach(function(p){ items.push({kind:'project', label:(p.title||p.project_id), hint:'', run:function(){ switchTab('projects'); }}); }); } catch(e){}
  return items;
}
function renderCmdk(){
  const list = document.getElementById('cmdk-list');
  if (!_cmdkFiltered.length){ list.innerHTML = '<div class="cmdk-empty">No matches</div>'; return; }
  if (_cmdkSel >= _cmdkFiltered.length) _cmdkSel = _cmdkFiltered.length - 1;
  if (_cmdkSel < 0) _cmdkSel = 0;
  list.innerHTML = _cmdkFiltered.map(function(it,i){
    return '<div class="cmdk-item'+(i===_cmdkSel?' active':'')+'" data-i="'+i+'" role="option">'+
      '<span class="cmdk-kind">'+esc(it.kind)+'</span><span class="cmdk-label">'+esc(it.label)+'</span>'+
      (it.hint?'<span class="cmdk-hint">'+esc(it.hint)+'</span>':'')+'</div>';
  }).join('');
  Array.prototype.forEach.call(list.querySelectorAll('.cmdk-item'), function(el){
    el.addEventListener('mousemove', function(){ const i=parseInt(el.dataset.i,10); if(i!==_cmdkSel){ _cmdkSel=i; highlightCmdk(); } });
    el.addEventListener('click', function(){ runCmdk(parseInt(el.dataset.i,10)); });
  });
}
function highlightCmdk(){
  Array.prototype.forEach.call(document.querySelectorAll('#cmdk-list .cmdk-item'), function(el){
    el.classList.toggle('active', parseInt(el.dataset.i,10) === _cmdkSel);
  });
}
function moveCmdk(d){
  if(!_cmdkFiltered.length) return;
  _cmdkSel = (_cmdkSel + d + _cmdkFiltered.length) % _cmdkFiltered.length;
  highlightCmdk();
  const a = document.querySelector('#cmdk-list .cmdk-item.active'); if (a) a.scrollIntoView({block:'nearest'});
}
function filterCmdk(q){
  q = String(q||'').trim().toLowerCase();
  _cmdkFiltered = (!q ? _cmdkItems : _cmdkItems.filter(function(it){ return (it.label+' '+it.hint+' '+it.kind).toLowerCase().indexOf(q) !== -1; })).slice(0, 60);
  _cmdkSel = 0;
  renderCmdk();
}
function openCmdPalette(){
  if (document.body.classList.contains('auth-locked')) return;
  _cmdkItems = buildCmdkItems();
  document.getElementById('cmd-palette').classList.add('open');
  const inp = document.getElementById('cmdk-input');
  inp.value = ''; filterCmdk('');
  setTimeout(function(){ inp.focus(); }, 0);
}
function closeCmdPalette(){ document.getElementById('cmd-palette').classList.remove('open'); }
function runCmdk(i){
  const it = _cmdkFiltered[i];
  closeCmdPalette();
  if (it && typeof it.run === 'function') it.run();
}
(function(){
  const inp = document.getElementById('cmdk-input');
  if (inp){
    inp.addEventListener('input', function(e){ filterCmdk(e.target.value); });
    inp.addEventListener('keydown', function(e){
      if (e.key === 'ArrowDown'){ e.preventDefault(); moveCmdk(1); }
      else if (e.key === 'ArrowUp'){ e.preventDefault(); moveCmdk(-1); }
      else if (e.key === 'Enter'){ e.preventDefault(); runCmdk(_cmdkSel); }
      else if (e.key === 'Escape'){ e.preventDefault(); closeCmdPalette(); }
    });
  }
  document.addEventListener('keydown', function(e){
    if ((e.metaKey || e.ctrlKey) && (e.key === 'k' || e.key === 'K')){
      e.preventDefault();
      const ov = document.getElementById('cmd-palette');
      if (ov.classList.contains('open')) closeCmdPalette(); else openCmdPalette();
    }
  });
})();

// ── Vault ──
let _vaultCache = [];
async function loadVault() {
  try {
    const r = await rpc('vault.list', {workspace_id:WS_ID});
    const entries = r.entries || [];
    _vaultCache = entries;
    document.getElementById('vault-count').textContent = entries.length;
    const el = document.getElementById('vault-list');
    if (!entries.length) { el.innerHTML='<div class="empty">No vault entries. Click + Add Entry to create one.</div>'; return; }
    el.innerHTML = entries.map(e => {
      const fields = parseJSON(e.fields_json);
      const count = Object.keys(fields).length;
      return '<div class="tool-card" ' + dashboardAction(function(dashboardEvent){showVaultDetail((e.entry_id))}) + '>'+
        '<div class="tool-name">'+esc(e.title)+'</div>'+
        '<div class="tool-desc">'+esc(e.description||'No description')+'</div>'+
        '<div class="tool-badges">'+
          '<span class="tool-badge kind">'+count+' fields</span>'+
          '<span class="tool-badge active">'+timeAgo(e.updated_at)+'</span>'+
        '</div>'+
      '</div>';
    }).join('');
  } catch(e) { console.error('loadVault', e); }
  loadVaultAudit();
}

async function loadVaultAudit() {
  try {
    const r = await rpc('vault.audit', {workspace_id:WS_ID, limit:30});
    const entries = r.entries || [];
    document.getElementById('audit-count').textContent = entries.length;
    const el = document.getElementById('vault-audit-list');
    if (!entries.length) { el.innerHTML='<div class="empty">No audit events yet</div>'; return; }
    el.innerHTML = entries.map(e => {
      const icons = {read:'',create:'',update:'',delete:''};
      const colors = {read:'var(--muted)',create:'var(--green)',update:'var(--accent)',delete:'var(--red)'};
      const icon = icons[e.action]||'';
      const color = colors[e.action]||'var(--muted)';
      return '<div style="display:flex;align-items:center;gap:10px;padding:6px 0;border-bottom:1px solid var(--border);font-size:12px">'+
        '<span style="font-size:16px">'+icon+'</span>'+
        '<div style="flex:1;min-width:0">'+
          '<span style="color:'+color+';font-weight:600">'+esc(e.action)+'</span> '+
          '<span style="color:var(--text)">'+esc(e.entry_title||e.entry_id)+'</span>'+
          '<span style="color:var(--muted);margin-left:6px">by <strong>'+esc(e.actor)+'</strong></span>'+
        '</div>'+
        '<span style="color:var(--muted);font-size:10px;white-space:nowrap">'+timeAgo(e.created_at)+'</span>'+
      '</div>';
    }).join('');
  } catch(e) { console.error('loadVaultAudit', e); }
}

function toggleCreateVault() { document.getElementById('create-vault-form').classList.toggle('open'); }

let _cvFieldIdx = 0;
function addCvField() {
  const id = 'cvf-'+(_cvFieldIdx++);
  document.getElementById('cv-fields').insertAdjacentHTML('beforeend',
    '<div id="'+id+'" style="display:flex;gap:6px;align-items:center;margin-bottom:4px">'+
    '<input class="cv-key" placeholder="Key (e.g. login)" style="flex:1;background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--accent);font-size:11px;font-family:monospace">'+
    '<input class="cv-val" placeholder="Value" style="flex:2;background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:4px 6px;color:var(--text);font-size:11px;font-family:monospace">'+
    '<button onclick="this.parentElement.remove()" style="background:none;border:none;color:var(--red);cursor:pointer;font-size:14px;padding:0 4px">×</button>'+
    '</div>');
}

async function submitNewVault() {
  const title = document.getElementById('cv-title').value.trim();
  const desc = document.getElementById('cv-desc').value.trim();
  const statusEl = document.getElementById('cv-status');
  if (!title) { statusEl.textContent = 'Title required'; statusEl.style.color='var(--red)'; return; }
  const fields = {};
  document.querySelectorAll('#cv-fields > div').forEach(row => {
    const k = row.querySelector('.cv-key')?.value?.trim();
    const v = row.querySelector('.cv-val')?.value?.trim();
    if (k) fields[k] = v || '';
  });
  try {
    const actorID = currentProfileId();
    if (!actorID) { statusEl.textContent = 'Select a profile before creating vault entries'; statusEl.style.color='var(--red)'; toast('Select a profile before creating vault entries'); return; }
    await rpc('vault.create', { workspace_id:WS_ID, title, description:desc, fields_json:JSON.stringify(fields), created_by:actorID });
    toast('✓ Vault entry created');
    document.getElementById('cv-title').value = '';
    document.getElementById('cv-desc').value = '';
    document.getElementById('cv-fields').innerHTML = '';
    document.getElementById('create-vault-form').classList.remove('open');
    loadVault();
  } catch(e) { statusEl.textContent = 'Error: '+e.message; statusEl.style.color='var(--red)'; }
}

async function showVaultDetail(entryId) {
  const cached = _vaultCache.find(x => x.entry_id === entryId);
  if (!cached) return;
  _editingVaultId = entryId;
  // Call vault.get to trigger audit log read event
  try { await rpc('vault.get', {workspace_id:WS_ID, entry_id:entryId, actor:currentProfileId() || ''}); } catch(e) {}
  renderVaultView(cached);
}
let _editingVaultId = null;

function renderVaultView(e) {
  const fields = parseJSON(e.fields_json);
  let html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">';
  html += '<span style="font-size:11px;color:var(--muted)">ID: <code>'+esc(e.entry_id)+'</code> · Created by '+esc(e.created_by||'—')+' · '+timeAgo(e.created_at)+'</span>';
  html += '<div style="display:flex;gap:6px">';
  html += '<button onclick="renderVaultEdit()" style="background:var(--accent);border:none;color:#fff;padding:6px 14px;border-radius:6px;font-size:12px;cursor:pointer;font-family:var(--font)">Edit</button>';
  html += '<button id="vault-del-btn" onclick="deleteVaultEntry(this)" style="background:var(--red);border:none;color:#fff;padding:6px 14px;border-radius:6px;font-size:12px;cursor:pointer;font-family:var(--font)">Delete</button>';
  html += '</div></div>';
  if (e.description) html += '<div style="margin-bottom:12px;color:var(--muted);font-size:12px">'+esc(e.description)+'</div>';
  const keys = Object.keys(fields);
  if (keys.length) {
    html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:10px">';
    keys.forEach(k => {
      const isSecret = k.toLowerCase().includes('key') || k.toLowerCase().includes('password') || k.toLowerCase().includes('secret') || k.toLowerCase().includes('token') || k.toLowerCase().includes('pin') || k.toLowerCase().includes('cvv');
      html += '<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--border)">';
      html += '<code style="color:var(--accent);font-size:12px">'+esc(k)+'</code>';
      html += '<span id="vf-'+esc(k)+'" style="font-family:monospace;font-size:12px;color:var(--text)">'+(isSecret ? maskValue(fields[k]) : esc(fields[k]))+'</span>';
      if (isSecret) html += '<button ' + dashboardAction(function(dashboardEvent){toggleVaultField((k),(fields[k]))}) + ' style="background:none;border:1px solid var(--border);border-radius:4px;color:var(--muted);font-size:10px;padding:2px 6px;cursor:pointer;margin-left:6px">show</button>';
      html += '</div>';
    });
    html += '</div>';
  } else {
    html += '<div class="empty">No fields. Click Edit to add.</div>';
  }
  openModal(''+esc(e.title), html);
}

function toggleVaultField(key, value) {
  const el = document.getElementById('vf-'+key);
  if (!el) return;
  if (el.dataset.shown === '1') { el.textContent = maskValue(value); el.dataset.shown = '0'; }
  else { el.textContent = value; el.dataset.shown = '1'; }
}

function renderVaultEdit() {
  const e = _vaultCache.find(x => x.entry_id === _editingVaultId);
  if (!e) return;
  const fields = parseJSON(e.fields_json);
  const iS = 'width:100%;background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:6px 8px;color:var(--text);font-size:12px;font-family:var(--font);box-sizing:border-box';
  const lS = 'font-size:11px;font-weight:600;color:var(--muted);margin-bottom:4px;display:block';
  let html = '<div id="vault-edit-status" style="font-size:11px;margin-bottom:8px"></div>';
  html += '<div style="margin-bottom:10px"><label style="'+lS+'">Title</label><input id="ve-title" value="'+esc(e.title)+'" style="'+iS+'"></div>';
  html += '<div style="margin-bottom:10px"><label style="'+lS+'">Description</label><input id="ve-desc" value="'+esc(e.description||'')+'" style="'+iS+'"></div>';
  html += '<div style="margin-bottom:10px"><label style="'+lS+'">Fields</label>';
  html += '<div id="mcp-ve-rows" style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px">';
  Object.entries(fields).forEach(([k,v]) => { html += mcpKvRow('ve', k, v, 0); });
  html += '</div>';
  html += '<button onclick="addMcpKvRow(\'ve\')" style="margin-top:6px;background:none;border:1px dashed var(--border);border-radius:6px;color:var(--accent);font-size:11px;padding:4px 10px;cursor:pointer;font-family:var(--font)">+ Add Field</button></div>';
  html += '<div style="display:flex;gap:8px;justify-content:flex-end">';
  html += '<button ' + dashboardAction(function(dashboardEvent){showVaultDetail((e.entry_id))}) + ' style="background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:6px 16px;font-size:12px;cursor:pointer;font-family:var(--font)">Cancel</button>';
  html += '<button onclick="saveVaultEdit()" style="background:var(--accent);border:none;color:#fff;padding:6px 16px;border-radius:6px;font-size:12px;cursor:pointer;font-weight:600;font-family:var(--font)">Save</button>';
  html += '</div>';
  openModal('Edit: '+esc(e.title), html);
}

async function saveVaultEdit() {
  const statusEl = document.getElementById('vault-edit-status');
  statusEl.textContent = 'Saving...'; statusEl.style.color = 'var(--accent)';
  const fields = {};
  document.querySelectorAll('#mcp-ve-rows > div').forEach(row => {
    const k = row.querySelector('.mcp-ve-key')?.value?.trim();
    const v = row.querySelector('.mcp-ve-val')?.value?.trim();
    if (k) fields[k] = v || '';
  });
  try {
    const actorID = currentProfileId();
    if (!actorID) { statusEl.textContent = 'Select a profile before saving vault entries'; statusEl.style.color = 'var(--red)'; toast('Select a profile before saving vault entries'); return; }
    await rpc('vault.update', {
      workspace_id: WS_ID,
      entry_id: _editingVaultId,
      title: document.getElementById('ve-title').value.trim(),
      description: document.getElementById('ve-desc').value.trim(),
      fields_json: JSON.stringify(fields),
      actor: actorID
    });
    toast('✓ Vault entry updated');
    closeModal();
    loadVault();
  } catch(e) { statusEl.textContent = 'Error: '+e.message; statusEl.style.color = 'var(--red)'; }
}

async function deleteVaultEntry(btn) {
  if (btn && btn.dataset.confirmed !== '1') {
    btn.textContent = 'Sure? Click again';
    btn.dataset.confirmed = '1';
    btn.style.background = '#b91c1c';
    setTimeout(() => { if(btn) { btn.textContent = 'Delete'; btn.dataset.confirmed = '0'; btn.style.background = 'var(--red)'; } }, 3000);
    return;
  }
  try {
    const actorID = currentProfileId();
    if (!actorID) { toast('Select a profile before deleting vault entries'); return; }
    await rpc('vault.delete', { workspace_id: WS_ID, entry_id: _editingVaultId, actor: actorID });
    toast('Vault entry deleted');
    closeModal();
    loadVault();
  } catch(e) { toast('Error: '+e.message); }
}

// ── RPC Logs ──
let _rpcMethodsLoaded = false;
let _rpcLogCursor = 0;
let _rpcLogsHasMore = true;
let _rpcLogsLoading = false;

function renderLogEntry(e) {
  const isErr = e.status === 'error';
  const lat = Number(e.latency_ms || 0);
  const latColor = lat > 500 ? 'var(--red)' : lat > 100 ? 'var(--yellow)' : 'var(--faint)';
  const rel = timeAgo(e.created_at).split(' · ')[0];
  return '<div class="rpc-log-row'+(isErr?' err':'')+'">'+
    '<span class="rpc-log-badge '+(isErr?'err':'ok')+'">'+(isErr?'ERR':'OK')+'</span>'+
    '<span class="rpc-log-method" title="'+esc(e.method)+'">'+esc(e.method)+'</span>'+
    '<span class="rpc-log-actor" title="'+esc(e.actor||'')+'">'+(e.actor?esc(e.actor):'—')+'</span>'+
    '<span class="rpc-log-lat" style="color:'+latColor+'">'+lat+'<span style="color:var(--faint);font-size:9px;margin-left:1px">ms</span></span>'+
    '<span class="rpc-log-msg" title="'+esc(e.error_msg||'')+'">'+(isErr?esc(e.error_msg||''):'')+'</span>'+
    '<span class="rpc-log-time" title="'+esc(timeAgo(e.created_at))+'">'+esc(rel)+'</span>'+
  '</div>';
}

async function loadRpcLogs(append) {
  if (_rpcLogsLoading) return;
  if (append && !_rpcLogsHasMore) return;
  _rpcLogsLoading = true;
  try {
    const method = document.getElementById('logs-method-filter')?.value || '';
    const status = document.getElementById('logs-status-filter')?.value || '';
    const params = {limit:50, method, status};
    if (append && _rpcLogCursor > 0) params.before_id = _rpcLogCursor;
    const r = await rpc('rpc.logs.list', params);
    const entries = r.entries || [];
    const stats = r.stats || {};
    _rpcLogsHasMore = r.has_more || false;
    if (entries.length) _rpcLogCursor = entries[entries.length - 1].id;
    // Stats bar
    const _t = Number(stats.total_24h || 0), _e = Number(stats.errors_24h || 0);
    const _rate = _t > 0 ? Math.round(_e / _t * 100) : 0;
    document.getElementById('logs-stats').innerHTML =
      '<span class="rpc-log-stat"><b>'+_t+'</b> 24h total</span>'+
      '<span class="rpc-log-stat err"><b>'+_e+'</b> errors ('+_rate+'%)</span>'+
      '<span class="rpc-log-stat"><b>'+(Number(stats.avg_latency_ms)||0)+'ms</b> avg latency</span>';
    // Populate method filter (once)
    if (!_rpcMethodsLoaded && entries.length) {
      const methods = [...new Set(entries.map(e => e.method))].sort();
      const sel = document.getElementById('logs-method-filter');
      methods.forEach(m => {
        if (!sel.querySelector('option[value="'+m+'"]')) {
          sel.insertAdjacentHTML('beforeend', '<option value="'+esc(m)+'">'+esc(m)+'</option>');
        }
      });
      _rpcMethodsLoaded = true;
    }
    const el = document.getElementById('logs-list');
    if (!append) {
      if (!entries.length) { el.innerHTML='<div class="empty">No log entries</div>'; return; }
      el.innerHTML = entries.map(renderLogEntry).join('');
    } else {
      el.insertAdjacentHTML('beforeend', entries.map(renderLogEntry).join(''));
    }
    // Remove old sentinel and add new one if more
    const oldS = document.getElementById('logs-sentinel');
    if (oldS) oldS.remove();
    if (_rpcLogsHasMore) {
      el.insertAdjacentHTML('beforeend', '<div id="logs-sentinel" style="padding:12px;text-align:center;color:var(--muted);font-size:11px">Loading more...</div>');
      observeLogsSentinel();
    }
  } catch(e) {
    console.error('loadRpcLogs', e);
    const msg = esc(e && e.message ? e.message : 'Failed to load log entries');
    const el = document.getElementById('logs-list');
    if (append) {
      const oldS = document.getElementById('logs-sentinel');
      if (oldS) oldS.remove();
      _rpcLogsHasMore = false;
      if (el) {
        el.insertAdjacentHTML('beforeend', '<div class="empty" style="margin-top:8px;border:1px solid rgba(224,106,106,.24);background:rgba(224,106,106,.08);color:var(--red)">Failed to load more log entries: '+msg+'</div>');
      }
    } else {
      const statsEl = document.getElementById('logs-stats');
      if (statsEl) {
        statsEl.innerHTML = '<span class="rpc-log-stat"><b>0</b> 24h total</span><span class="rpc-log-stat err"><b>0</b> errors</span><span class="rpc-log-stat"><b>0ms</b> avg latency</span>';
      }
      if (el) {
        el.innerHTML = '<div class="empty">'+msg+'</div>';
      }
    }
  }
  _rpcLogsLoading = false;
}

function resetRpcLogs() {
  _rpcLogCursor = 0;
  _rpcLogsHasMore = true;
  loadRpcLogs(false);
}

let _logsObserver;
function observeLogsSentinel() {
  const sentinel = document.getElementById('logs-sentinel');
  if (!sentinel) return;
  if (_logsObserver) _logsObserver.disconnect();
  _logsObserver = new IntersectionObserver(entries => {
    if (entries[0].isIntersecting) loadRpcLogs(true);
  }, {rootMargin: '200px'});
  _logsObserver.observe(sentinel);
}

// ── Limit Groups ──
let _limitGroupsCache = [];
async function loadLimitGroups() {
  try {
    const r = await rpc('limits.group.list', {workspace_id:WS_ID});
    _limitGroupsCache = r.groups || [];
    document.getElementById('limit-groups-count').textContent = _limitGroupsCache.length;
    const el = document.getElementById('limit-groups-list');
    if (!_limitGroupsCache.length) { el.innerHTML = '<div class="empty">No limit groups. Click New Group to create one.</div>'; return; }
    el.innerHTML = _limitGroupsCache.map(g => {
      const agentCount = (g.agents||[]).length;
      const dailyPct = g.daily_limit > 0 ? Math.round(g.daily_remaining / g.daily_limit * 100) : 0;
      const weeklyPct = g.weekly_limit > 0 ? Math.round(g.weekly_remaining / g.weekly_limit * 100) : 0;
      const dailyColor = dailyPct > 50 ? 'var(--green)' : dailyPct > 20 ? 'var(--yellow)' : 'var(--red)';
      const weeklyColor = weeklyPct > 50 ? 'var(--green)' : weeklyPct > 20 ? 'var(--yellow)' : 'var(--red)';
      return '<div class="tool-card" ' + dashboardAction(function(dashboardEvent){showLimitGroupDetail((g.group_id))}) + '>' +
        '<div class="tool-name">'+esc(g.title)+'</div>' +
        '<div class="tool-desc">'+esc(g.owner_name||'No owner')+(g.subscription_tier?' · '+esc(g.subscription_tier):'')+'</div>' +
        '<div style="margin:6px 0">' +
          '<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin-bottom:2px"><span>Daily</span><span>'+g.daily_remaining+'%</span></div>' +
          '<div style="background:var(--border);border-radius:3px;height:6px;overflow:hidden"><div style="width:'+g.daily_remaining+'%;height:100%;background:'+dailyColor+';border-radius:3px;transition:width .3s"></div></div>' +
          '<div style="display:flex;justify-content:space-between;font-size:9px;color:var(--muted);margin:4px 0 2px"><span>Weekly</span><span>'+g.weekly_remaining+'%</span></div>' +
          '<div style="background:var(--border);border-radius:3px;height:6px;overflow:hidden"><div style="width:'+g.weekly_remaining+'%;height:100%;background:'+weeklyColor+';border-radius:3px;transition:width .3s"></div></div>' +
        '</div>' +
        '<div class="tool-badges">' +
          '<span class="tool-badge kind">'+agentCount+' agent'+(agentCount!==1?'s':'')+'</span>' +
          (g.subscription_tier ? '<span class="tool-badge active">'+esc(g.subscription_tier)+'</span>' : '') +
          (g.last_reported_at ? '<span class="tool-badge kind">'+timeAgo(g.last_reported_at)+'</span>' : '') +
        '</div>' +
      '</div>';
    }).join('');
  } catch(e) { console.error('loadLimitGroups', e); }
  populateStatsGroupSelect();
}

function toggleCreateLimitGroup() {
  document.getElementById('create-limit-group-form').classList.toggle('open');
}

async function submitNewLimitGroup() {
  const id = document.getElementById('clg-id').value.trim();
  const title = document.getElementById('clg-title').value.trim();
  const owner = document.getElementById('clg-owner').value.trim();
  const tier = document.getElementById('clg-tier').value.trim();
  const statusEl = document.getElementById('clg-status');
  if (!id || !title) { statusEl.textContent = 'Group ID and Title are required'; statusEl.style.color = 'var(--red)'; return; }
  const actorID = currentProfileId();
  if (!actorID) { statusEl.textContent = 'Select a profile before changing limit groups'; statusEl.style.color = 'var(--red)'; return; }
  try {
    await rpc('limits.group.create', {
      group_id: id, workspace_id: WS_ID, title: title,
      owner_name: owner, subscription_tier: tier,
      daily_limit: 100, weekly_limit: 100,
      actor_id: actorID
    });
    statusEl.textContent = '✓ Created!';
    statusEl.style.color = 'var(--green)';
    document.getElementById('clg-id').value = '';
    document.getElementById('clg-title').value = '';
    document.getElementById('clg-owner').value = '';
    document.getElementById('clg-tier').value = '';
    toast('✓ Limit group created: ' + title);
    setTimeout(() => { statusEl.textContent = ''; }, 3000);
    loadLimitGroups();
  } catch(e) { statusEl.textContent = '✗ ' + e.message; statusEl.style.color = 'var(--red)'; }
}

async function showLimitGroupDetail(groupId) {
  const g = _limitGroupsCache.find(x => x.group_id === groupId);
  if (!g) return;
  openModal(''+esc(g.title), '<div class="empty">Loading...</div>');
  try {
    const group = await rpc('limits.group.get', {workspace_id: WS_ID, group_id: groupId});
    let html = '';
    // Editable fields
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:14px">';
    html += '<div><label style="font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:3px">Title</label><input id="lg-title" value="'+esc(group.title)+'" style="width:100%;background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:7px 10px;font-size:12px;font-family:var(--font);outline:none"></div>';
    html += '<div><label style="font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:3px">Owner</label><input id="lg-owner" value="'+esc(group.owner_name)+'" style="width:100%;background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:7px 10px;font-size:12px;font-family:var(--font);outline:none"></div>';
    html += '<div><label style="font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:3px">Subscription Tier</label><input id="lg-tier" value="'+esc(group.subscription_tier)+'" style="width:100%;background:var(--surface);border:1px solid var(--border);border-radius:6px;color:var(--text);padding:7px 10px;font-size:12px;font-family:var(--font);outline:none"></div>';
    html += '<div><label style="font-size:10px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;display:block;margin-bottom:3px">Group ID</label><code style="background:var(--surface);padding:7px 10px;border-radius:6px;font-size:11px;display:block;border:1px solid var(--border)">'+esc(group.group_id)+'</code></div>';
    html += '</div>';
    // Remaining bars (percentage mode)
    html += renderLimitBars(group.daily_remaining, group.weekly_remaining);
    if (group.last_reported_at) html += '<div style="font-size:10px;color:var(--muted);margin:4px 0 10px">Last reported: '+timeAgo(group.last_reported_at)+'</div>';
    // Agent membership
    html += '<hr style="border-color:var(--border);margin:14px 0">';
    html += '<strong style="font-size:12px">Agents in this group:</strong>';
    html += '<div id="lg-agents-list" style="margin-top:8px;display:flex;flex-direction:column;gap:4px">';
    const groupAgents = group.agents || [];
    agentsCache.forEach(a => {
      const checked = groupAgents.includes(a.agent_id) ? 'checked' : '';
      html += '<label style="display:flex;align-items:center;gap:8px;padding:4px 8px;border-radius:6px;background:var(--surface);cursor:pointer;font-size:12px">';
      html += '<input type="checkbox" class="lg-agent-cb" value="'+esc(a.agent_id)+'" '+checked+' style="accent-color:var(--accent)">';
      html += '<span class="agent-dot '+(a.is_online?'online':'offline')+'" style="width:8px;height:8px"></span>';
      html += esc(a.display_name || a.agent_id);
      html += '</label>';
    });
    html += '</div>';
    // Save / Delete buttons
    html += '<div style="display:flex;gap:8px;margin-top:14px">';
    html += '<button class="btn-accent" style="flex:1;padding:8px" ' + dashboardAction(function(dashboardEvent){saveLimitGroup((groupId))}) + ' id="lg-save-btn">Save Changes</button>';
    html += '<button style="padding:8px 16px;background:rgba(224,106,106,0.1);color:var(--red);border:1px solid rgba(224,106,106,0.3);border-radius:6px;cursor:pointer;font-family:var(--font);font-size:12px" ' + dashboardAction(function(dashboardEvent){deleteLimitGroup((groupId),(group.title))}) + '>Delete</button>';
    html += '</div>';
    html += '<span id="lg-save-status" style="font-size:11px;margin-top:6px;display:block"></span>';
    document.getElementById('modal-body').innerHTML = html;
  } catch(e) {
    document.getElementById('modal-body').innerHTML = '<div class="empty">'+esc(e.message||'Failed')+"</div>";
  }
}

async function saveLimitGroup(groupId) {
  const fb = document.getElementById('lg-save-status');
  try {
    fb.textContent = 'Saving...';
    fb.style.color = 'var(--muted)';
    const actorID = currentProfileId();
    if (!actorID) { fb.textContent = 'Select a profile before changing limit groups'; fb.style.color = 'var(--red)'; return; }
    const agentIds = [...document.querySelectorAll('.lg-agent-cb:checked')].map(cb => cb.value);
    await rpc('limits.group.update', {
      workspace_id: WS_ID,
      group_id: groupId,
      title: document.getElementById('lg-title').value.trim(),
      owner_name: document.getElementById('lg-owner').value.trim(),
      subscription_tier: document.getElementById('lg-tier').value.trim(),
      agent_ids: agentIds,
      actor_id: actorID
    });
    fb.textContent = '✓ Saved!';
    fb.style.color = 'var(--green)';
    await loadLimitGroups();
  } catch(e) {
    fb.textContent = '✗ ' + (e.message || 'Save failed');
    fb.style.color = 'var(--red)';
  }
}

async function deleteLimitGroup(groupId, title) {
  const btn = event.target;
  if (btn.dataset.confirm !== 'yes') {
    btn.dataset.confirm = 'yes';
    btn.textContent = '! Confirm?';
    btn.style.background = 'rgba(224,106,106,0.3)';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete'; btn.style.background = 'rgba(224,106,106,0.1)'; }, 3000);
    return;
  }
  try {
    const actorID = currentProfileId();
    if (!actorID) { toast('Select a profile before changing limit groups'); return; }
    await rpc('limits.group.delete', {workspace_id: WS_ID, group_id: groupId, actor_id: actorID});
    closeModal();
    toast('Limit group deleted: ' + title);
    loadLimitGroups();
  } catch(e) { toast('Error: ' + e.message); }
}

function renderLimitBars(dailyRem, weeklyRem) {
  let html = '<div style="margin-top:8px">';
  const dc = dailyRem > 50 ? 'var(--green)' : dailyRem > 20 ? 'var(--yellow)' : 'var(--red)';
  const wc = weeklyRem > 50 ? 'var(--green)' : weeklyRem > 20 ? 'var(--yellow)' : 'var(--red)';
  html += '<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-bottom:2px"><span>Daily</span><span style="font-weight:600;color:'+dc+'">'+dailyRem+'%</span></div>';
  html += '<div style="background:var(--border);border-radius:4px;height:8px;overflow:hidden"><div style="width:'+dailyRem+'%;height:100%;background:'+dc+';border-radius:4px;transition:width .3s"></div></div>';
  html += '<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin:6px 0 2px"><span>Weekly</span><span style="font-weight:600;color:'+wc+'">'+weeklyRem+'%</span></div>';
  html += '<div style="background:var(--border);border-radius:4px;height:8px;overflow:hidden"><div style="width:'+weeklyRem+'%;height:100%;background:'+wc+';border-radius:4px;transition:width .3s"></div></div>';
  html += '</div>';
  return html;
}

// ── Stats Chart ──
let _statsMode = 'weekly';
let _statsGranHours = 24;
let _statsData = [];

function populateStatsGroupSelect() {
  const sel = document.getElementById('stats-group-select');
  if (!sel) return;
  const prev = sel.value;
  sel.innerHTML = _limitGroupsCache.map(g => '<option value="'+esc(g.group_id)+'">'+esc(g.title)+'</option>').join('');
  if (prev && [...sel.options].some(o => o.value === prev)) sel.value = prev;
  if (sel.value) loadLimitStats();
}

function setStatsMode(mode, btn) {
  _statsMode = mode;
  document.querySelectorAll('.stats-mode-btn[data-mode]').forEach(b => b.classList.toggle('active', b.dataset.mode === mode));
  renderStatsChart();
}

function setStatsGranularity(hours, btn) {
  _statsGranHours = hours;
  document.querySelectorAll('.stats-mode-btn[data-gran]').forEach(b => b.classList.toggle('active', parseInt(b.dataset.gran) === hours));
  renderStatsChart();
}

async function loadLimitStats() {
  const groupId = document.getElementById('stats-group-select')?.value;
  if (!groupId) return;
  try {
    const r = await rpc('limits.snapshots', {group_id: groupId, days: 7});
    _statsData = r.snapshots || [];
    renderStatsChart();
  } catch(e) {
    document.getElementById('stats-chart').innerHTML = '<div class="empty">'+esc(e.message)+'</div>';
  }
}

function renderStatsChart() {
  const chartEl = document.getElementById('stats-chart');
  const labelsEl = document.getElementById('stats-x-labels');
  if (!_statsData.length) {
    chartEl.innerHTML = '<div class="empty">No data yet. Agents report limits via limits.report.</div>';
    labelsEl.innerHTML = '';
    return;
  }

  // Bucket data by granularity
  const bucketMs = _statsGranHours * 3600 * 1000;
  const now = Date.now();
  const weekAgo = now - 7 * 24 * 3600 * 1000;
  const bucketCount = Math.ceil((7 * 24) / _statsGranHours);

  // Initialize buckets
  const buckets = [];
  for (let i = 0; i < bucketCount; i++) {
    const start = weekAgo + i * bucketMs;
    buckets.push({start, value: null, label: ''});
  }

  // Fill buckets with last snapshot value in each
  const field = _statsMode === 'daily' ? 'daily_remaining' : 'weekly_remaining';
  _statsData.forEach(s => {
    const t = new Date(s.reported_at).getTime();
    const idx = Math.floor((t - weekAgo) / bucketMs);
    if (idx >= 0 && idx < bucketCount) {
      buckets[idx].value = s[field];
    }
  });

  // Forward-fill nulls with last known value
  let lastVal = null;
  for (let i = 0; i < buckets.length; i++) {
    if (buckets[i].value !== null) lastVal = buckets[i].value;
    else buckets[i].value = lastVal;
  }

  // Generate labels
  buckets.forEach(b => {
    const d = new Date(b.start);
    if (_statsGranHours >= 24) {
      b.label = (d.getMonth()+1)+'/'+d.getDate();
    } else {
      b.label = d.getDate()+' '+String(d.getHours()).padStart(2,'0')+':00';
    }
  });

  // Only show buckets that have data (value !== null)
  const visible = buckets.filter(b => b.value !== null);
  if (!visible.length) {
    chartEl.innerHTML = '<div class="empty">No data in this range</div>';
    labelsEl.innerHTML = '';
    return;
  }

  const barW = Math.max(8, Math.floor(480 / visible.length) - 2);
  chartEl.innerHTML = visible.map(b => {
    const v = Math.max(0, Math.min(100, b.value));
    const c = v > 50 ? 'var(--green)' : v > 20 ? 'var(--yellow)' : 'var(--red)';
    return '<div title="'+b.label+': '+v+'%" style="width:'+barW+'px;min-width:'+barW+'px;height:'+v+'%;background:'+c+';border-radius:3px 3px 0 0;transition:height .3s;cursor:default"></div>';
  }).join('');

  // Show every Nth label to avoid clutter
  const labelEvery = Math.max(1, Math.ceil(visible.length / 10));
  labelsEl.innerHTML = visible.map((b,i) => {
    const show = i % labelEvery === 0 || i === visible.length - 1;
    return '<span style="width:'+barW+'px;min-width:'+barW+'px;text-align:center;white-space:nowrap">'+(show ? b.label : '')+'</span>';
  }).join('');
}

// ── News Feed ──
let _newsCache = [];

async function loadNews() {
  try {
    const r = await rpc('news.list', {workspace_id: WS_ID});
    _newsCache = r.items || [];
    document.getElementById('news-count').textContent = _newsCache.length;
    const el = document.getElementById('news-list');
    if (!_newsCache.length) { el.innerHTML = '<div class="empty">No news yet. Be the first to publish!</div>'; return; }
    el.innerHTML = _newsCache.map(n => {
      const preview = (n.content || '').slice(0, 120) + (n.content && n.content.length > 120 ? '...' : '');
      const authorIcon = n.author_type === 'human' ? '' : '';
      return '<div class="tool-card" style="cursor:pointer" ' + dashboardAction(function(dashboardEvent){showNewsDetail((n.news_id))}) + '>'+
        '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:4px">'+
        '<div class="tool-name" style="margin:0">'+esc(n.title)+'</div>'+
        '<span style="font-size:9px;color:var(--muted);white-space:nowrap;margin-left:8px">'+timeAgo(n.created_at)+'</span>'+
        '</div>'+
        '<div class="tool-desc" style="margin-bottom:6px">'+esc(preview)+'</div>'+
        '<div class="tool-badges">'+
        '<span class="tool-badge kind">'+authorIcon+' '+esc(n.author_id)+'</span>'+
        (n.author_type === 'human' ? '<span class="tool-badge active">human</span>' : '')+
        '</div>'+
      '</div>';
    }).join('');
  } catch(e) { console.error('loadNews', e); }
}

function toggleCreateNews() {
  document.getElementById('create-news-form').classList.toggle('open');
}

async function submitNews() {
  const title = document.getElementById('cn-title').value.trim();
  const content = document.getElementById('cn-content').value.trim();
  const author = currentProfileId() || document.getElementById('cn-author').value.trim();
  const statusEl = document.getElementById('cn-status');
  if (!title) { statusEl.textContent = 'Title is required'; statusEl.style.color = 'var(--red)'; return; }
  if (!author) { statusEl.textContent = 'Select a profile before publishing news'; statusEl.style.color = 'var(--red)'; toast('Select a profile before publishing news'); return; }
  try {
    await rpc('news.publish', {
      workspace_id: WS_ID, title: title, content: content,
      author_id: author, author_type: currentProfileType()
    });
    statusEl.textContent = '✓ Published!';
    statusEl.style.color = 'var(--green)';
    document.getElementById('cn-title').value = '';
    document.getElementById('cn-content').value = '';
    toast('✓ News published: ' + title);
    setTimeout(() => { statusEl.textContent = ''; }, 3000);
    loadNews();
  } catch(e) { statusEl.textContent = '✗ ' + e.message; statusEl.style.color = 'var(--red)'; }
}

function showNewsDetail(newsId) {
  const n = _newsCache.find(x => x.news_id === newsId);
  if (!n) return;
  const authorIcon = n.author_type === 'human' ? '' : '';
  let html = '';
  html += '<div style="margin-bottom:12px">';
  html += '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">';
  html += '<span style="font-size:18px">'+authorIcon+'</span>';
  html += '<span style="font-weight:600;color:var(--text)">'+esc(n.author_id)+'</span>';
  html += '<span style="font-size:10px;color:var(--muted)">'+timeAgo(n.created_at)+'</span>';
  if (n.author_type === 'human') html += '<span style="background:rgba(78,166,116,.15);color:var(--green);padding:1px 6px;border-radius:4px;font-size:9px;font-weight:600">human</span>';
  html += '</div>';
  html += '</div>';
  // Content
  html += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:12px;font-size:12px;line-height:1.6;white-space:pre-wrap;max-height:400px;overflow-y:auto;margin-bottom:14px">'+esc(n.content || 'No content.')+'</div>';
  // Delete
  html += '<div style="display:flex;gap:8px;justify-content:flex-end">';
  html += '<button style="padding:6px 16px;background:rgba(224,106,106,0.1);color:var(--red);border:1px solid rgba(224,106,106,0.3);border-radius:6px;cursor:pointer;font-family:var(--font);font-size:12px" ' + dashboardAction(function(dashboardEvent){deleteNews((newsId),(n.title))}) + '>Delete</button>';
  html += '</div>';
  openModal(''+esc(n.title), html);
}

async function deleteNews(newsId, title) {
  const btn = event.target;
  if (btn.dataset.confirm !== 'yes') {
    btn.dataset.confirm = 'yes';
    btn.textContent = '! Confirm?';
    btn.style.background = 'rgba(224,106,106,0.3)';
    setTimeout(() => { btn.dataset.confirm = ''; btn.textContent = 'Delete'; btn.style.background = 'rgba(224,106,106,0.1)'; }, 3000);
    return;
  }
  try {
    const actorID = currentProfileId();
    if (!actorID) { toast('Select a profile before deleting news'); return; }
    await rpc('news.delete', {workspace_id: WS_ID, news_id: newsId, actor_id: actorID});
    closeModal();
    toast('News deleted: ' + title);
    loadNews();
  } catch(e) { toast('Error: ' + e.message); }
}

// ── Truth Graph Logic ──
let _graphLibLoaded = false;
let _graphInstance = null;
let _graphData = { nodes: [], links: [] };
let _graphSnapshotStats = {};
let _graphFocusNode = null;
let _graphLastTimeAuth = '';
let _graphRequestedFocusID = '';
let _graphLoadedMode = 'SYSTEM';
let _graphLoadedFocus = '';
let _graphSyncTimer = null;
let _graphSyncInFlight = false;
let _graphSyncPending = false;
let _graphExternalPollTimer = null;
let _graphLayoutSettings = { repulsion: null, linkDistance: null, gravity: null };
let _graphIntroFrame = 0;
let _graphIntroUntil = 0;
let _graphHoverNodeId = '';
let _graphPointerHoverNodeId = '';
let _graphHoverAffinityNeighbors = new Set();
let _graphHoverDistanceByNode = new Map();
let _graphDragNodeId = '';
let _graphMemoryOverlayAnchorIds = new Set();
let _graphBlockingActionNodeIds = new Set();
let _graphBlockedNodeIds = new Set();
let _graphBlockedClaimTaskIds = new Set();
let _graphColdTensionIds = new Set();
let _graphLabelRects = [];

// ── Graph cinematics: ripples, sparks, heat, dust, comets, camera, replay ──
let _graphNodeByRef = new Map();      // live node objects by ref id (rebuilt per snapshot)
let _graphClusterMembers = new Map(); // hub ref -> [member node objects] for halos
let _graphFxRipples = [];             // {id, t0, kind: 'ripple'|'spark'}
let _graphFxHeat = new Map();         // nodeId -> wall-clock ms of last activity
let _graphHoverEaseStart = 0;         // hover focus ease-in anchor
let _graphCamPrev = null;             // camera state before cinematic focus
let _graphReplay = null;              // active timelapse replay state
const GRAPH_FX_MAX_RIPPLES = 36;
const GRAPH_FX_RIPPLE_MS = 950;
const GRAPH_FX_SPARK_MS = 760;
const GRAPH_HEAT_TTL_MS = 5 * 60 * 1000;

function graphHashSeed(value) {
  let h = 2166136261;
  const str = String(value || '');
  for (let i = 0; i < str.length; i++) { h ^= str.charCodeAt(i); h = Math.imul(h, 16777619); }
  return (h >>> 0) / 4294967295;
}

function graphSpawnEffect(nodeID, kind) {
  nodeID = String(graphRefId(nodeID) || '').trim();
  if (!nodeID) return;
  _graphFxRipples.push({ id: nodeID, t0: performance.now(), kind: kind === 'spark' ? 'spark' : 'ripple' });
  if (_graphFxRipples.length > GRAPH_FX_MAX_RIPPLES) _graphFxRipples.splice(0, _graphFxRipples.length - GRAPH_FX_MAX_RIPPLES);
  graphNoteNodeHeat(nodeID);
}

function graphNoteNodeHeat(nodeID) {
  nodeID = String(graphRefId(nodeID) || '').trim();
  if (!nodeID) return;
  _graphFxHeat.set(nodeID, Date.now());
  if (_graphFxHeat.size > 600) {
    const cutoff = Date.now() - GRAPH_HEAT_TTL_MS;
    _graphFxHeat.forEach(function(ts, key) { if (ts < cutoff) _graphFxHeat.delete(key); });
  }
}

// 0..1 "recency warmth": bright right after activity, cools over ~5 minutes
function graphNodeHeat(node) {
  const ts = _graphFxHeat.get(String(graphRefId(node && node.id) || ''));
  if (!ts) return 0;
  const age = Date.now() - ts;
  if (age >= GRAPH_HEAT_TTL_MS) return 0;
  return Math.pow(1 - age / GRAPH_HEAT_TTL_MS, 1.6);
}

// Route live (or replayed) runtime traffic into node effects.
function graphNoteActivityFromEvent(evtType, data) {
  if (!data || !_graphInstance) return;
  const et = String(evtType || '');
  const isResolve = et === 'task.completed' || et === 'task.closed' || et === 'node.completed';
  ['task_id', 'agent_id', 'session_id', 'node_id', 'action_id'].forEach(function(field) {
    const ref = String(data[field] || '').trim();
    if (!ref || !_graphNodeByRef.has(ref)) return;
    graphSpawnEffect(ref, isResolve && field === 'task_id' ? 'spark' : 'ripple');
  });
}

function graphHoverEaseK() {
  if (!graphHoverHasAffinityFocus()) return 0;
  if (!_graphHoverEaseStart) return 1;
  const t = Math.min(1, (performance.now() - _graphHoverEaseStart) / 280);
  return 1 - Math.pow(1 - t, 3); // cubic ease-out
}
// blend a hover falloff factor toward 1 while the ease-in is still running
function graphHoverEased(factor) {
  const k = graphHoverEaseK();
  return 1 + (factor - 1) * k;
}

function graphCinematicFocus(node) {
  if (!_graphInstance || !node || !Number.isFinite(node.x) || !Number.isFinite(node.y)) return;
  if (!_graphCamPrev) {
    _graphCamPrev = { center: _graphInstance.centerAt(), zoom: _graphInstance.zoom() };
  }
  const targetZoom = Math.max(_graphInstance.zoom(), 1.45);
  // The frosted panels are absolute overlays on top of the canvas, so the
  // raw canvas center is NOT the visual center - aim for the middle of the
  // unobstructed band instead (only panels that cover canvas mid-height count).
  let dx = 0;
  const container = document.getElementById('graph-container');
  if (container) {
    const crect = container.getBoundingClientRect();
    const midY = crect.top + crect.height / 2;
    let leftCover = 0;
    let rightCover = 0;
    const probe = function(el, side) {
      if (!el || el.offsetParent === null) return;
      const r = el.getBoundingClientRect();
      if (!r.width || r.top > midY || r.bottom < midY) return;
      if (side === 'left') leftCover = Math.max(leftCover, r.right - crect.left);
      else rightCover = Math.max(rightCover, crect.right - r.left);
    };
    probe(document.getElementById('graph-toolbar'), 'left');
    probe(document.querySelector('.graph-inspector-panel'), 'right');
    probe(document.querySelector('.graph-display-settings-overlay'), 'right');
    dx = ((leftCover - rightCover) / 2) / targetZoom;
  }
  _graphInstance.centerAt(node.x - dx, node.y, 700);
  _graphInstance.zoom(targetZoom, 700);
}

function graphCinematicRelease() {
  if (!_graphInstance || !_graphCamPrev) return;
  const prev = _graphCamPrev;
  _graphCamPrev = null;
  if (prev.center && Number.isFinite(prev.center.x)) _graphInstance.centerAt(prev.center.x, prev.center.y, 600);
  if (Number.isFinite(prev.zoom)) _graphInstance.zoom(prev.zoom, 600);
}

// ── Timelapse replay: re-run the last hour of runtime events as ripples ──
async function toggleGraphReplay() {
  if (_graphReplay) { stopGraphReplay(); return; }
  const btn = document.getElementById('graph-replay-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Loading...'; }
  try {
    const r = await rpc('workspace.events.list', { workspace_id: WS_ID, limit: 400 });
    const cutoff = Date.now() - 60 * 60 * 1000;
    const events = (r.items || [])
      .map(function(ev) { return { ev: ev, ts: Date.parse(ev.created_at || '') || 0 }; })
      .filter(function(item) {
        if (item.ts < cutoff) return false;
        const ev = item.ev;
        return ['task_id', 'agent_id', 'session_id', 'node_id', 'action_id'].some(function(field) {
          const ref = String(ev[field] || '').trim();
          return ref && _graphNodeByRef.has(ref);
        });
      })
      .sort(function(a, b) { return a.ts - b.ts; });
    if (!events.length) {
      toast('No replayable runtime events in the last hour', 'event');
      return;
    }
    const t0 = events[0].ts;
    const span = Math.max(1000, events[events.length - 1].ts - t0);
    const duration = Math.min(45000, Math.max(12000, events.length * 160));
    _graphReplay = { events: events, idx: 0, t0: t0, span: span, duration: duration, startedAt: performance.now() };
    _graphReplay.timer = setInterval(graphReplayTick, 50);
    const bar = document.getElementById('graph-replay-bar');
    if (bar) bar.style.display = '';
    if (btn) btn.textContent = 'Stop replay';
  } catch (e) {
    console.error('graph replay', e);
    toast('Replay failed: ' + (e.message || e), 'event');
  } finally {
    if (btn) { btn.disabled = false; if (!_graphReplay) btn.textContent = 'Replay 1h'; }
  }
}

function graphReplayTick() {
  const rp = _graphReplay;
  if (!rp) return;
  const prog = Math.min(1, (performance.now() - rp.startedAt) / rp.duration);
  const fill = document.getElementById('graph-replay-fill');
  if (fill) fill.style.width = (prog * 100).toFixed(1) + '%';
  const chip = document.getElementById('graph-replay-chip');
  if (chip) {
    const virtualTs = rp.t0 + prog * rp.span;
    const agoMin = Math.max(0, Math.round((Date.now() - virtualTs) / 60000));
    chip.textContent = 'replay · T−' + agoMin + 'm';
  }
  const virtual = rp.t0 + prog * rp.span;
  while (rp.idx < rp.events.length && rp.events[rp.idx].ts <= virtual) {
    const item = rp.events[rp.idx++];
    graphNoteActivityFromEvent(item.ev.event_type, item.ev);
  }
  if (prog >= 1) stopGraphReplay();
}

function stopGraphReplay() {
  const rp = _graphReplay;
  _graphReplay = null;
  if (rp && rp.timer) clearInterval(rp.timer);
  const bar = document.getElementById('graph-replay-bar');
  if (bar) bar.style.display = 'none';
  const btn = document.getElementById('graph-replay-btn');
  if (btn) { btn.disabled = false; btn.textContent = 'Replay 1h'; }
}
let _graphInspectorDismissedNodeID = '';
let _graphAtlasHistory = [];
let _graphAtlasHistoryLabels = {};
const GRAPH_NODE_INTRO_MS = 650;
const GRAPH_EXTERNAL_POLL_MS = 1200;
const GRAPH_CONTROLS_PANEL_KEY = 'rhizome.graph.controls.open';
const GRAPH_DISPLAY_SETTINGS_KEY = 'rhizome.graph.display.open';
const GRAPH_RELEVANT_SSE_EVENTS = new Set([
  'task.created',
  'task.closed',
  'task.claimed', 'task.blocked', 'task.completed', 'task.released',
  'agent.request.claimed',
  'node.claimed', 'node.released', 'node.completed',
  'agent.update', 'agent.deleted',
  'agent.session.start', 'agent.session.status', 'agent.session.blocked',
  'agent.session.decision_needed', 'agent.session.end', 'agent.session.takeover',
  'workspace.memory.recorded', 'workspace.memory.removed', 'workspace.memory.restored', 'workspace.memory.node.touched', 'workspace_artifact.created',
  'workspace.change',
  'workspace.ops.updated', 'workspace.ops.resolved', 'workspace.ops.escalated',
  'workspace.claim.written', 'workspace.claim.review_requested', 'workspace.claim.confirmed',
  'workspace.claim.disputed', 'workspace.claim.superseded', 'workspace.claim.stale',
  'workspace.claim.review_escalated', 'workspace.claim.archived',
  'cluster.metric_snapshot', 'cluster.control_advisory_snapshot', 'cluster.unified_control_advisory_snapshot', 'cluster.unified_control_effective_snapshot',
  'cluster.corridor_readiness_snapshot', 'cluster.corridor_fit_snapshot',
  'cluster.corridor_basis_snapshot', 'cluster.corridor_ownership_snapshot',
  'cluster.control_state_snapshot', 'cluster.control_state_ticked', 'cluster.control_state_stabilized',
  'governance.challenge.raised', 'governance.challenge.defended',
  'governance.vote.cast', 'governance.challenge.resolved',
  'governance.challenge.changed',
  'tension.refreshed', 'tension.detected', 'tension.updated',
  'tension.confirmed', 'tension.discarded', 'tension.archived',
  'tension.resolved', 'tension.dormant', 'tension.active', 'tension.recovered',
  'tension.emergent', 'tension.condensed',
  'tension.dependency.added', 'tension.dependency.removed',
  'tension.agent.attached', 'tension.agent.detached'
]);

function graphControlsPanelOpen() {
  return localStorage.getItem(GRAPH_CONTROLS_PANEL_KEY) !== '0';
}

function graphDisplaySettingsOpen() {
  return localStorage.getItem(GRAPH_DISPLAY_SETTINGS_KEY) === '1';
}

function syncGraphControlsUI() {
  const controlsBody = document.getElementById('graph-controls-body');
  const controlsToggle = document.getElementById('graph-controls-toggle');
  const controlsIcon = document.getElementById('graph-controls-toggle-icon');
  const controlsOpen = graphControlsPanelOpen();
  if (controlsBody) controlsBody.classList.toggle('is-collapsed', !controlsOpen);
  if (controlsIcon) controlsIcon.classList.toggle('is-collapsed', !controlsOpen);
  if (controlsToggle) {
    const label = controlsOpen ? 'Collapse graph panel' : 'Expand graph panel';
    controlsToggle.setAttribute('aria-label', label);
    controlsToggle.setAttribute('title', label);
  }

  const displayPanel = document.getElementById('graph-display-settings-panel');
  const displayBody = document.getElementById('graph-display-settings-body');
  const displayCopy = document.getElementById('graph-display-settings-copy');
  const displayOpen = graphDisplaySettingsOpen();
  if (displayBody) displayBody.classList.toggle('is-collapsed', !displayOpen);
  if (displayPanel) displayPanel.classList.toggle('panel-collapsed', !displayOpen);
  if (displayCopy) displayCopy.classList.toggle('is-hidden', !displayOpen);
}

function toggleGraphControlsPanel(forceOpen) {
  const next = typeof forceOpen === 'boolean' ? forceOpen : !graphControlsPanelOpen();
  localStorage.setItem(GRAPH_CONTROLS_PANEL_KEY, next ? '1' : '0');
  syncGraphControlsUI();
}

function toggleGraphDisplaySettings(forceOpen) {
  const next = typeof forceOpen === 'boolean' ? forceOpen : !graphDisplaySettingsOpen();
  localStorage.setItem(GRAPH_DISPLAY_SETTINGS_KEY, next ? '1' : '0');
  syncGraphControlsUI();
}

function graphSelectedMode() {
  const select = document.getElementById('graph-mode-select');
  return select ? String(select.value || 'SYSTEM') : 'SYSTEM';
}

function graphModeSupportsFocus(mode) {
  return mode === 'TASK_FOCUS' || mode === 'CONTROL' || mode === 'MEMORY_ATLAS';
}

function graphNodeRefID(node) {
  if (!node || typeof node !== 'object') return '';
  return String(node.ref_id || node.id || '').trim();
}

function graphNodeFocusKey(node) {
  if (!node || typeof node !== 'object') return '';
  return String(graphNodeRefID(node) || node.id || '').trim();
}

function dismissGraphInspector() {
  const panel = document.getElementById('graph-inspector');
  _graphInspectorDismissedNodeID = graphNodeFocusKey(_graphFocusNode);
  if (panel) panel.style.display = 'none';
}

function graphCurrentFocusID() {
  const mode = graphSelectedMode();
  return graphModeSupportsFocus(mode) ? String(_graphRequestedFocusID || '').trim() : '';
}

function graphFocusLabel(taskID) {
  taskID = String(taskID || '').trim();
  if (!taskID) return '';
  const match = (_graphData.nodes || []).find(function(node) {
    return node.type === 'task' && graphNodeRefID(node) === taskID;
  });
  if (match) return String(match.label || taskID);
  const cached = (_cachedTasks || []).find(function(task) {
    return String(task.task_id || '').trim() === taskID;
  });
  if (cached) return String(cached.title || taskID);
  return taskID;
}

function graphTaskFocusOptionStatus(task) {
  return String((task && (task.claim_status || task.status)) || 'PENDING').trim().toUpperCase() || 'PENDING';
}

function graphTaskFocusOptionLabel(task) {
  const title = String((task && (task.title || task.task_id)) || '').trim();
  const status = graphTaskFocusOptionStatus(task);
  const assignee = String((task && task.claim_agent_id) || '').trim();
  return title + ' - ' + status + (assignee ? (' - ' + assignee) : '');
}

function updateGraphTaskFocusOptions() {
  const wrap = document.getElementById('graph-task-focus-picker-wrap');
  const select = document.getElementById('graph-task-focus-select');
  if (!wrap || !select) return;
  const mode = _graphLoadedMode || graphSelectedMode();
  wrap.style.display = mode === 'TASK_FOCUS' ? '' : 'none';
  if (mode !== 'TASK_FOCUS') return;
  const focusID = String(_graphLoadedFocus || _graphRequestedFocusID || '').trim();
  const tasks = (_cachedTasks || []).slice().sort(function(left, right) {
    const leftOpen = ['CLAIMED', 'BLOCKED', 'PENDING', 'RUNNING'].includes(graphTaskFocusOptionStatus(left)) ? 0 : 1;
    const rightOpen = ['CLAIMED', 'BLOCKED', 'PENDING', 'RUNNING'].includes(graphTaskFocusOptionStatus(right)) ? 0 : 1;
    if (leftOpen !== rightOpen) return leftOpen - rightOpen;
    return String(left.title || left.task_id || '').localeCompare(String(right.title || right.task_id || ''));
  });
  let html = '<option value="">Select task...</option>';
  tasks.forEach(function(task) {
    const taskID = String(task.task_id || '').trim();
    if (!taskID) return;
    html += '<option value="' + esc(taskID) + '"' + (taskID === focusID ? ' selected' : '') + '>' + esc(graphTaskFocusOptionLabel(task)) + '</option>';
  });
  select.innerHTML = html;
  if (focusID && !tasks.some(function(task) { return String(task.task_id || '').trim() === focusID; })) {
    select.value = '';
  } else if (focusID) {
    select.value = focusID;
  }
}

function handleGraphTaskFocusSelection() {
  const select = document.getElementById('graph-task-focus-select');
  const taskID = select ? String(select.value || '').trim() : '';
  _graphRequestedFocusID = taskID;
  _graphLoadedFocus = taskID;
  updateGraphFocusUI();
  initGraphData();
}

function graphControlBandRank(cluster) {
  const band = String((cluster && cluster.attention_band) || '').trim().toUpperCase();
  if (band === 'HOT') return 0;
  if (band === 'WATCH') return 1;
  if (band === 'ELEVATED') return 2;
  return 3;
}

function graphProtoClusterKindLabel(kind) {
  kind = String(kind || '').trim();
  if (kind === 'task') return 'Task Cluster';
  if (kind === 'session') return 'Session Cluster';
  if (kind === 'workspace_doc' || kind === 'doc') return 'Doc Cluster';
  if (kind === 'artifact') return 'Artifact Cluster';
  if (kind === 'source') return 'Source Cluster';
  if (kind === 'claim') return 'Claim Cluster';
  return 'Proto-Cluster';
}

function graphHumanizeProtoClusterID(clusterID, maxDetail) {
  clusterID = String(clusterID || '').trim();
  if (!clusterID) return '';
  const cut = clusterID.indexOf(':');
  if (cut < 0) return clusterID;
  const kind = clusterID.slice(0, cut).trim();
  let tail = clusterID.slice(cut + 1).trim();
  const kindLabel = graphProtoClusterKindLabel(kind);
  if (!tail) return kindLabel;
  const parts = tail.split('/').map(function(part) { return String(part || '').trim(); }).filter(Boolean);
  if (kind === 'source') {
    tail = parts.length > 1 ? parts.slice(1).join('/') : (parts[0] || tail);
  } else {
    tail = parts.length > 1 ? parts[parts.length - 1] : (parts[0] || tail);
  }
  tail = String(tail || '').trim();
  if (!tail) return kindLabel;
  const limit = Number(maxDetail || 0);
  if (limit > 0 && tail.length > limit) {
    tail = tail.slice(0, Math.max(1, limit - 3)) + '...';
  }
  return kindLabel + ': ' + tail;
}

function graphControlClusterDisplayLabel(cluster) {
  const clusterID = String((cluster && cluster.proto_cluster_id) || '').trim();
  const explicit = String((cluster && cluster.label) || '').trim();
  if (explicit && explicit !== clusterID) return explicit;
  return graphHumanizeProtoClusterID(clusterID, 20) || explicit || clusterID;
}

function graphControlClusterByID(clusterID) {
  clusterID = String(clusterID || '').trim();
  if (!clusterID) return null;
  return graphControlClusterCandidates().find(function(cluster) {
    return String(cluster.proto_cluster_id || '').trim() === clusterID;
  }) || null;
}

function graphNodeNeighborTypeCounts(nodeID) {
  const counts = { task: 0, session: 0, agent: 0, tension: 0, proto_cluster: 0 };
  nodeID = graphRefId(nodeID);
  if (!nodeID) return counts;
  const nodeByID = new Map((_graphData.nodes || []).map(function(node) { return [graphRefId(node.id), node]; }));
  const seen = new Set();
  (_graphData.links || []).forEach(function(link) {
    const sourceID = graphRefId(link.source);
    const targetID = graphRefId(link.target);
    let otherID = '';
    if (sourceID === nodeID) otherID = targetID;
    else if (targetID === nodeID) otherID = sourceID;
    if (!otherID || seen.has(otherID)) return;
    seen.add(otherID);
    const node = nodeByID.get(otherID);
    const type = String((node && node.type) || '').trim();
    if (type && Object.prototype.hasOwnProperty.call(counts, type)) {
      counts[type] += 1;
    }
  });
  return counts;
}

function graphControlClusterScopeSummary(clusterID) {
  const cluster = graphControlClusterByID(clusterID);
  if (!cluster) return '';
  const bits = [];
  const band = String(cluster.attention_band || '').trim();
  const pressure = Number(cluster.pressure_score || 0);
  const mode = String(cluster.current_mode || '').trim();
  if (band) bits.push(band);
  if (pressure > 0) bits.push('pressure ' + pressure);
  if (mode) bits.push(mode);
  const summary = String(cluster.summary || '').trim();
  if (summary) bits.push(summary);
  return bits.join(' · ');
}

function graphControlClusterCandidates() {
  const byID = new Map();
  function mergeCluster(raw) {
    const clusterID = String((raw && (raw.proto_cluster_id || raw.id || raw.ref_id)) || '').trim();
    if (!clusterID) return;
    const existing = byID.get(clusterID) || {
      proto_cluster_id: clusterID,
      attention_band: '',
      pressure_score: 0,
      current_mode: '',
      summary: '',
      label: '',
    };
    const pressure = Number((raw && (raw.pressure_score || raw.cluster_pressure_score || raw.max_pressure_score)) || existing.pressure_score || 0);
    const next = {
      proto_cluster_id: clusterID,
      attention_band: String((raw && (raw.attention_band || raw.status || raw.current_attention_band)) || existing.attention_band || '').trim(),
      pressure_score: Number.isFinite(pressure) ? pressure : existing.pressure_score || 0,
      current_mode: String((raw && (raw.current_mode || raw.mode)) || existing.current_mode || '').trim(),
      summary: String((raw && raw.summary) || existing.summary || '').trim(),
      label: String((raw && raw.label) || existing.label || '').trim(),
    };
    byID.set(clusterID, next);
  }

  ((_graphData.nodes || []).filter(function(node) { return node.type === 'proto_cluster'; }) || []).forEach(function(node) {
    mergeCluster({
      proto_cluster_id: graphNodeRefID(node),
      attention_band: node.status,
      label: node.label,
    });
  });
  (((controlStateReportCache || {}).clusters) || []).forEach(mergeCluster);
  (((corridorOwnershipReportCache || {}).clusters) || []).forEach(mergeCluster);
  (controlPolicyClustersCache || []).forEach(mergeCluster);
  (instrumentationClustersCache || []).forEach(mergeCluster);

  return Array.from(byID.values()).sort(function(left, right) {
    const bandDelta = graphControlBandRank(left) - graphControlBandRank(right);
    if (bandDelta !== 0) return bandDelta;
    const pressureDelta = Number(right.pressure_score || 0) - Number(left.pressure_score || 0);
    if (pressureDelta !== 0) return pressureDelta;
    return String(left.proto_cluster_id || '').localeCompare(String(right.proto_cluster_id || ''));
  });
}

function graphControlFocusLabel(clusterID) {
  clusterID = String(clusterID || '').trim();
  if (!clusterID) return '';
  const graphNode = (_graphData.nodes || []).find(function(node) {
    return node.type === 'proto_cluster' && graphNodeRefID(node) === clusterID;
  });
  if (graphNode) return String(graphNode.label || graphHumanizeProtoClusterID(clusterID, 20) || clusterID);
  const candidate = graphControlClusterCandidates().find(function(cluster) {
    return String(cluster.proto_cluster_id || '').trim() === clusterID;
  });
  if (!candidate) return graphHumanizeProtoClusterID(clusterID, 20) || clusterID;
  return graphControlClusterDisplayLabel(candidate);
}

function graphControlClusterOptionLabel(cluster) {
  const bits = [graphControlClusterDisplayLabel(cluster)];
  const band = String((cluster && cluster.attention_band) || '').trim();
  const pressure = Number((cluster && cluster.pressure_score) || 0);
  const mode = String((cluster && cluster.current_mode) || '').trim();
  if (band) bits.push(band);
  if (pressure > 0) bits.push('pressure ' + pressure);
  if (mode) bits.push(mode);
  return bits.filter(Boolean).join(' - ');
}

function updateGraphControlFocusOptions() {
  const wrap = document.getElementById('graph-control-focus-picker-wrap');
  const select = document.getElementById('graph-control-focus-select');
  if (!wrap || !select) return;
  const mode = _graphLoadedMode || graphSelectedMode();
  wrap.style.display = mode === 'CONTROL' ? '' : 'none';
  if (mode !== 'CONTROL') return;
  const focusID = String(_graphLoadedFocus || _graphRequestedFocusID || '').trim();
  const clusters = graphControlClusterCandidates();
  let html = '<option value="">All clusters</option>';
  clusters.forEach(function(cluster) {
    const clusterID = String(cluster.proto_cluster_id || '').trim();
    if (!clusterID) return;
    html += '<option value="' + esc(clusterID) + '"' + (clusterID === focusID ? ' selected' : '') + '>' + esc(graphControlClusterOptionLabel(cluster)) + '</option>';
  });
  select.innerHTML = html;
  if (focusID && clusters.some(function(cluster) { return String(cluster.proto_cluster_id || '').trim() === focusID; })) {
    select.value = focusID;
  } else {
    select.value = '';
  }
}

function graphIsMemoryAtlasMode() {
  return (_graphLoadedMode || graphSelectedMode()) === 'MEMORY_ATLAS';
}

function graphMemoryAtlasQuery() {
  const input = document.getElementById('graph-memory-atlas-query');
  return input ? String(input.value || '').trim() : '';
}

function graphMemoryAtlasLayer() {
  const select = document.getElementById('graph-memory-atlas-layer');
  return select ? String(select.value || '').trim() : '';
}

function graphMemoryAtlasLifecycle() {
  const select = document.getElementById('graph-memory-atlas-lifecycle');
  return select ? String(select.value || '').trim() : '';
}

function graphMemoryAtlasOriginKind() {
  const select = document.getElementById('graph-memory-atlas-origin');
  return select ? String(select.value || '').trim() : '';
}

function graphMemoryAtlasEpistemicStatus() {
  const select = document.getElementById('graph-memory-atlas-epistemic');
  return select ? String(select.value || '').trim() : '';
}

function graphMemoryAtlasDepth() {
  const select = document.getElementById('graph-memory-atlas-depth');
  const value = select ? parseInt(select.value || '1', 10) : 1;
  if (value === 2) return 2;
  return 1;
}

function graphMemoryAtlasCanonicalOnly() {
  const toggle = document.getElementById('graph-memory-atlas-canonical');
  return !!(toggle && toggle.checked);
}

function graphMemoryAtlasIncludeArchived() {
  const toggle = document.getElementById('graph-memory-atlas-archived');
  return !!(toggle && toggle.checked);
}

function graphMemoryAtlasShowAnchors() {
  const toggle = document.getElementById('graph-memory-atlas-anchors');
  return !!(toggle && toggle.checked);
}

function graphMemoryAtlasFocusLabel(memoryID) {
  memoryID = String(memoryID || '').trim();
  if (!memoryID) return '';
  const graphNode = (_graphData.nodes || []).find(function(node) {
    return node.type === 'memory_node' && graphNodeRefID(node) === memoryID;
  });
  if (graphNode && graphNode.label) return String(graphNode.label);
  return memoryID;
}

function graphRememberMemoryAtlasLabels(nodes) {
  if (!Array.isArray(nodes)) return;
  nodes.forEach(function(node) {
    if (!node || node.type !== 'memory_node') return;
    const memoryID = String(node.ref_id || node.id || '').trim();
    const label = String(node.label || memoryID).trim();
    if (memoryID && label) {
      _graphAtlasHistoryLabels[memoryID] = label;
    }
  });
}

function graphMemoryAtlasHistoryLabel(memoryID) {
  memoryID = String(memoryID || '').trim();
  if (!memoryID) return '';
  return _graphAtlasHistoryLabels[memoryID] || graphMemoryAtlasFocusLabel(memoryID);
}

function graphCurrentMemoryAtlasLens() {
  const layer = graphMemoryAtlasLayer();
  const lifecycle = graphMemoryAtlasLifecycle();
  const origin = graphMemoryAtlasOriginKind();
  const epistemic = graphMemoryAtlasEpistemicStatus();
  const canonical = graphMemoryAtlasCanonicalOnly();
  const archived = graphMemoryAtlasIncludeArchived();
  if (!layer && !lifecycle && !origin && !epistemic && !canonical && !archived) return 'all';
  if (lifecycle === 'ACTIVE' && !layer && !origin && !epistemic && !canonical) return 'active';
  if (lifecycle === 'DORMANT' && archived && !layer && !origin && !epistemic) return 'dormant';
  if (layer === 'PROCEDURAL' && !lifecycle && !origin && !epistemic) return 'procedural';
  if (layer === 'IDENTITY' && !lifecycle && !origin && !epistemic) return 'identity';
  if (epistemic === 'DISPUTED') return 'disputed';
  if (canonical && !origin) return 'canonical';
  if (!canonical && origin === 'knowledge_claim') return 'derived';
  return '';
}

function graphRenderMemoryAtlasHistory() {
  const wrap = document.getElementById('graph-memory-atlas-nav');
  const list = document.getElementById('graph-memory-atlas-history');
  const backBtn = document.getElementById('graph-atlas-back-btn');
  const mode = _graphLoadedMode || graphSelectedMode();
  if (!wrap || !list || !backBtn) return;
  const history = _graphAtlasHistory.filter(Boolean);
  const show = mode === 'MEMORY_ATLAS' && history.length > 0;
  wrap.style.display = show ? '' : 'none';
  backBtn.style.display = show ? '' : 'none';
  if (!show) {
    list.innerHTML = '';
    return;
  }
  list.innerHTML = history.slice(-6).reverse().map(function(memoryID) {
    return '<button type="button" class="graph-atlas-history-chip" ' + dashboardAction(function(dashboardEvent){graphEnterMemoryAtlasFocus((memoryID))}) + '>' + esc(graphMemoryAtlasHistoryLabel(memoryID)) + '</button>';
  }).join('');
}

function graphPushMemoryAtlasHistory(memoryID) {
  memoryID = String(memoryID || '').trim();
  const current = String(_graphLoadedFocus || _graphRequestedFocusID || '').trim();
  if (!current || current === memoryID) return;
  _graphAtlasHistory = _graphAtlasHistory.filter(function(entry) { return String(entry || '').trim() !== current; });
  _graphAtlasHistory.push(current);
  if (_graphAtlasHistory.length > 10) {
    _graphAtlasHistory = _graphAtlasHistory.slice(_graphAtlasHistory.length - 10);
  }
}

function graphGoBackMemoryAtlasFocus() {
  while (_graphAtlasHistory.length > 0) {
    const previous = String(_graphAtlasHistory.pop() || '').trim();
    if (!previous) continue;
    graphEnterMemoryAtlasFocus(previous, { skipHistory: true });
    return;
  }
  graphShowMemoryAtlasOverview();
}

function graphApplyMemoryAtlasLens(lens) {
  const layer = document.getElementById('graph-memory-atlas-layer');
  const lifecycle = document.getElementById('graph-memory-atlas-lifecycle');
  const origin = document.getElementById('graph-memory-atlas-origin');
  const epistemic = document.getElementById('graph-memory-atlas-epistemic');
  const canonical = document.getElementById('graph-memory-atlas-canonical');
  const archived = document.getElementById('graph-memory-atlas-archived');
  if (layer) layer.value = '';
  if (lifecycle) lifecycle.value = '';
  if (origin) origin.value = '';
  if (epistemic) epistemic.value = '';
  if (canonical) canonical.checked = false;
  if (archived) archived.checked = false;
  switch (String(lens || '').trim()) {
    case 'active':
      if (lifecycle) lifecycle.value = 'ACTIVE';
      break;
    case 'dormant':
      if (lifecycle) lifecycle.value = 'DORMANT';
      if (archived) archived.checked = true;
      break;
    case 'procedural':
      if (layer) layer.value = 'PROCEDURAL';
      break;
    case 'identity':
      if (layer) layer.value = 'IDENTITY';
      break;
    case 'disputed':
      if (epistemic) epistemic.value = 'DISPUTED';
      if (archived) archived.checked = true;
      break;
    case 'canonical':
      if (canonical) canonical.checked = true;
      break;
    case 'derived':
      if (origin) origin.value = 'knowledge_claim';
      break;
  }
  handleGraphMemoryAtlasFilterChange();
}

function updateGraphMemoryAtlasControls() {
  const wrap = document.getElementById('graph-memory-atlas-controls-wrap');
  const layerLabel = document.getElementById('graph-memory-atlas-layer-label');
  const lifecycleLabel = document.getElementById('graph-memory-atlas-lifecycle-label');
  const originLabel = document.getElementById('graph-memory-atlas-origin-label');
  const epistemicLabel = document.getElementById('graph-memory-atlas-epistemic-label');
  const depthLabel = document.getElementById('graph-memory-atlas-depth-label');
  const mode = _graphLoadedMode || graphSelectedMode();
  if (wrap) wrap.style.display = mode === 'MEMORY_ATLAS' ? '' : 'none';
  const layer = graphMemoryAtlasLayer();
  const lifecycle = graphMemoryAtlasLifecycle();
  const origin = graphMemoryAtlasOriginKind();
  const epistemic = graphMemoryAtlasEpistemicStatus();
  if (layerLabel) {
    layerLabel.textContent = layer ? (layer.charAt(0) + layer.slice(1).toLowerCase()) : 'All';
  }
  if (lifecycleLabel) {
    lifecycleLabel.textContent = lifecycle ? (lifecycle.charAt(0) + lifecycle.slice(1).toLowerCase()) : 'All';
  }
  if (originLabel) {
    originLabel.textContent = origin ? origin.replace(/_/g, ' ') : 'Mixed';
  }
  if (epistemicLabel) {
    epistemicLabel.textContent = epistemic ? (epistemic.charAt(0) + epistemic.slice(1).toLowerCase()) : 'All';
  }
  if (depthLabel) {
    depthLabel.textContent = graphMemoryAtlasDepth() + '-hop';
  }
  document.querySelectorAll('[data-atlas-lens]').forEach(function(button) {
    button.classList.toggle('active', button.getAttribute('data-atlas-lens') === graphCurrentMemoryAtlasLens());
  });
  graphRenderMemoryAtlasHistory();
}

function handleGraphMemoryAtlasSearch() {
  const select = document.getElementById('graph-mode-select');
  if (select) select.value = 'MEMORY_ATLAS';
  _graphLoadedMode = 'MEMORY_ATLAS';
  _graphRequestedFocusID = '';
  _graphLoadedFocus = '';
  updateGraphFocusUI();
  triggerGraphSync(0);
}

function handleGraphMemoryAtlasFilterChange() {
  updateGraphMemoryAtlasControls();
  if (!graphIsMemoryAtlasMode()) return;
  triggerGraphSync(0);
}

function graphEnterMemoryAtlasFocus(memoryID, options) {
  memoryID = String(memoryID || '').trim();
  if (!memoryID) return;
  const opts = options || {};
  _graphAtlasHistoryLabels[memoryID] = graphMemoryAtlasFocusLabel(memoryID);
  if (!opts.skipHistory) {
    graphPushMemoryAtlasHistory(memoryID);
  }
  const select = document.getElementById('graph-mode-select');
  if (select) select.value = 'MEMORY_ATLAS';
  _graphLoadedMode = 'MEMORY_ATLAS';
  _graphRequestedFocusID = memoryID;
  _graphLoadedFocus = memoryID;
  updateGraphFocusUI();
  triggerGraphSync(0);
}

function graphShowMemoryAtlasOverview() {
  const select = document.getElementById('graph-mode-select');
  if (select) select.value = 'MEMORY_ATLAS';
  _graphLoadedMode = 'MEMORY_ATLAS';
  _graphRequestedFocusID = '';
  _graphLoadedFocus = '';
  updateGraphFocusUI();
  triggerGraphSync(0);
}

function graphMemoryOverlayCounts() {
  const counts = { nodes: 0, edges: 0 };
  (_graphData.nodes || []).forEach(function(node) {
    if (node.type === 'memory_node') counts.nodes += 1;
  });
  (_graphData.links || []).forEach(function(link) {
    const sourceID = String(graphRefId(link.source) || '').trim();
    const targetID = String(graphRefId(link.target) || '').trim();
    if (sourceID.indexOf('memory:') === 0 || targetID.indexOf('memory:') === 0) {
      counts.edges += 1;
    }
  });
  return counts;
}

function graphFormatMetricValue(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(2) : '';
}

function handleGraphControlFocusSelection() {
  const select = document.getElementById('graph-control-focus-select');
  const clusterID = select ? String(select.value || '').trim() : '';
  controlPolicySelectedClusterID = clusterID;
  _graphRequestedFocusID = clusterID;
  _graphLoadedFocus = clusterID;
  updateGraphFocusUI();
  initGraphData();
}

function graphAutoSeedFocusFromTasks() {
  if (_graphRequestedFocusID) return _graphRequestedFocusID;
  const preferred = (_cachedTasks || []).find(function(task) {
    const status = graphTaskFocusOptionStatus(task);
    return status === 'CLAIMED' || status === 'BLOCKED' || status === 'RUNNING' || status === 'PENDING';
  });
  return preferred ? String(preferred.task_id || '').trim() : '';
}

function graphAutoSeedFocusFromClusters() {
  if (_graphRequestedFocusID) return _graphRequestedFocusID;
  const selected = String(controlPolicySelectedClusterID || '').trim();
  if (selected) return selected;
  const preferred = graphControlClusterCandidates()[0];
  return preferred ? String(preferred.proto_cluster_id || '').trim() : '';
}

function updateGraphFocusUI() {
  const mode = _graphLoadedMode || graphSelectedMode();
  const focusID = graphModeSupportsFocus(mode) ? String(_graphLoadedFocus || _graphRequestedFocusID || '').trim() : '';
  const backBtn = document.getElementById('graph-back-to-system');
  const atlasOverviewBtn = document.getElementById('graph-atlas-overview-btn');
  const atlasBackBtn = document.getElementById('graph-atlas-back-btn');
  const affinityWrap = document.getElementById('graph-affinity-controls-wrap');
  const meta = document.getElementById('graph-focus-meta');
  if (backBtn) backBtn.style.display = mode !== 'SYSTEM' ? '' : 'none';
  if (atlasOverviewBtn) atlasOverviewBtn.style.display = mode === 'MEMORY_ATLAS' && focusID ? '' : 'none';
  if (atlasBackBtn) atlasBackBtn.style.display = mode === 'MEMORY_ATLAS' && _graphAtlasHistory.length ? '' : 'none';
  if (affinityWrap) affinityWrap.style.display = mode === 'MEMORY_ATLAS' ? 'none' : '';
  updateGraphTaskFocusOptions();
  updateGraphControlFocusOptions();
  updateGraphMemoryAtlasControls();
  if (!meta) return;
  if (mode === 'SYSTEM') {
    meta.style.display = 'none';
    meta.textContent = '';
    return;
  }
  meta.style.display = '';
  if (mode === 'TASK_FOCUS') {
    meta.textContent = focusID
      ? ('Focused task: ' + graphFocusLabel(focusID))
      : 'Choose a task from the list below, or click a task node and use Focus Task Graph.';
    return;
  }
  if (mode === 'MEMORY_OVERLAY') {
    const counts = graphMemoryOverlayCounts();
    if (!counts.nodes) {
      meta.textContent = 'Memory overlay: no canonical workspace memory has been recorded yet, so this mode currently has nothing distinct to draw.';
      return;
    }
    meta.textContent = 'Memory overlay: canonical memory attached to active tasks, sessions, and agents' +
      (counts.nodes ? (' · ' + counts.nodes + ' memory nodes') : '') +
      (counts.edges ? (' · ' + counts.edges + ' memory links') : '') +
      '.';
    return;
  }
  if (mode === 'MEMORY_ATLAS') {
    const query = graphMemoryAtlasQuery();
    const layer = graphMemoryAtlasLayer();
    const lifecycle = graphMemoryAtlasLifecycle();
    const origin = graphMemoryAtlasOriginKind();
    const epistemic = graphMemoryAtlasEpistemicStatus();
    const depth = graphMemoryAtlasDepth();
    const anchors = graphMemoryAtlasShowAnchors();
    const canonical = graphMemoryAtlasCanonicalOnly();
    if (focusID) {
      meta.textContent = 'Memory atlas focus: ' + graphMemoryAtlasFocusLabel(focusID) +
        ' - ' + depth + '-hop neighborhood' +
        (layer ? (' - layer ' + layer.toLowerCase()) : '') +
        (lifecycle ? (' - ' + lifecycle.toLowerCase()) : '') +
        (origin ? (' - ' + origin.replace(/_/g, ' ')) : '') +
        (epistemic ? (' - ' + epistemic.toLowerCase()) : '') +
        (canonical ? ' - canonical only' : '') +
        (anchors ? ' - anchors on' : '');
      return;
    }
    meta.textContent = 'Memory atlas overview: budgeted canonical memory map' +
      (query ? (' - search "' + query + '"') : '') +
      (layer ? (' - layer ' + layer.toLowerCase()) : '') +
      (lifecycle ? (' - ' + lifecycle.toLowerCase()) : '') +
      (origin ? (' - ' + origin.replace(/_/g, ' ')) : '') +
      (epistemic ? (' - ' + epistemic.toLowerCase()) : '') +
      (canonical ? ' - canonical only' : '') +
      (anchors ? ' - anchors on' : '') +
      ' - use search or click a memory node to center the atlas.';
    return;
  }
  if (focusID) {
    const detail = graphControlClusterScopeSummary(focusID);
    meta.textContent = 'Focused cluster: ' + graphControlFocusLabel(focusID) + (detail ? (' · ' + detail) : '');
    return;
  }
  const clusters = graphControlClusterCandidates();
  const hotCount = clusters.filter(function(cluster) { return String(cluster.attention_band || '').trim().toUpperCase() === 'HOT'; }).length;
  const peakPressure = clusters.reduce(function(best, cluster) {
    return Math.max(best, Number(cluster.pressure_score || 0));
  }, 0);
  meta.textContent = 'Control overview: cluster-centered pressure map' +
    (clusters.length ? (' · ' + clusters.length + ' clusters') : '') +
    (hotCount ? (' · ' + hotCount + ' hot') : '') +
    (peakPressure > 0 ? (' · peak pressure ' + peakPressure) : '') +
    '. Use the cluster picker below to isolate one proto-cluster.';
}

function graphEnterTaskFocus(taskID) {
  taskID = String(taskID || '').trim();
  if (!taskID) return;
  _graphRequestedFocusID = taskID;
  const select = document.getElementById('graph-mode-select');
  if (select) select.value = 'TASK_FOCUS';
  _graphLoadedMode = 'TASK_FOCUS';
  _graphLoadedFocus = taskID;
  updateGraphFocusUI();
  triggerGraphSync(0);
}

function graphEnterControlFocus(clusterID) {
  clusterID = String(clusterID || '').trim();
  if (!clusterID) return;
  controlPolicySelectedClusterID = clusterID;
  _graphRequestedFocusID = clusterID;
  const select = document.getElementById('graph-mode-select');
  if (select) select.value = 'CONTROL';
  _graphLoadedMode = 'CONTROL';
  _graphLoadedFocus = clusterID;
  const controlSelect = document.getElementById('graph-control-focus-select');
  if (controlSelect) controlSelect.value = clusterID;
  updateGraphFocusUI();
  triggerGraphSync(0);
}

function graphShowControlOverview() {
  controlPolicySelectedClusterID = '';
  _graphRequestedFocusID = '';
  _graphLoadedMode = 'CONTROL';
  _graphLoadedFocus = '';
  const select = document.getElementById('graph-mode-select');
  if (select) select.value = 'CONTROL';
  const controlSelect = document.getElementById('graph-control-focus-select');
  if (controlSelect) controlSelect.value = '';
  updateGraphFocusUI();
  triggerGraphSync(0);
}

function graphReturnToSystem() {
  _graphRequestedFocusID = '';
  _graphLoadedMode = 'SYSTEM';
  _graphLoadedFocus = '';
  const select = document.getElementById('graph-mode-select');
  if (select) select.value = 'SYSTEM';
  const focusSelect = document.getElementById('graph-task-focus-select');
  if (focusSelect) focusSelect.value = '';
  const controlSelect = document.getElementById('graph-control-focus-select');
  if (controlSelect) controlSelect.value = '';
  updateGraphFocusUI();
  triggerGraphSync(0);
}

function handleGraphModeChange() {
  const mode = graphSelectedMode();
  if (_graphLoadedMode && _graphLoadedMode !== mode) {
    _graphRequestedFocusID = '';
    _graphLoadedFocus = '';
  }
  if (mode === 'TASK_FOCUS') {
    if (!_graphRequestedFocusID && _graphFocusNode && _graphFocusNode.type === 'task') {
      _graphRequestedFocusID = graphNodeRefID(_graphFocusNode);
    }
    if (!_graphRequestedFocusID) {
      _graphRequestedFocusID = graphAutoSeedFocusFromTasks();
    }
    _graphLoadedFocus = _graphRequestedFocusID;
  } else if (mode === 'CONTROL') {
    if (!_graphRequestedFocusID && _graphFocusNode && _graphFocusNode.type === 'proto_cluster') {
      _graphRequestedFocusID = graphNodeRefID(_graphFocusNode);
    }
    if (!_graphRequestedFocusID) {
      _graphRequestedFocusID = graphAutoSeedFocusFromClusters();
    }
    _graphLoadedFocus = _graphRequestedFocusID;
  } else if (mode === 'MEMORY_ATLAS') {
    if (!_graphRequestedFocusID && _graphFocusNode && _graphFocusNode.type === 'memory_node') {
      _graphRequestedFocusID = graphNodeRefID(_graphFocusNode);
    }
    _graphLoadedFocus = _graphRequestedFocusID;
  } else {
    _graphRequestedFocusID = '';
    _graphLoadedFocus = '';
  }
  _graphLoadedMode = mode;
  updateGraphFocusUI();
  initGraphData();
}

function triggerGraphSync(delay) {
  if (_graphSyncTimer) clearTimeout(_graphSyncTimer);
  _graphSyncTimer = setTimeout(() => {
    if (_graphSyncInFlight) {
      _graphSyncPending = true;
      return;
    }
    initGraphData();
  }, typeof delay === 'number' ? delay : 500);
}

function stopGraphExternalPolling() {
  if (_graphExternalPollTimer) {
    clearInterval(_graphExternalPollTimer);
    _graphExternalPollTimer = null;
  }
}

function startGraphExternalPolling() {
  if (_graphExternalPollTimer) return;
  _graphExternalPollTimer = setInterval(() => {
    if (activeTabPanelId() !== 'panel-graph' || document.visibilityState === 'hidden') {
      stopGraphExternalPolling();
      return;
    }
    // Catch external writers that mutate sqlite directly without publishing SSE.
    triggerGraphSync(0);
  }, GRAPH_EXTERNAL_POLL_MS);
}

function graphRefId(ref) {
  return ref && typeof ref === 'object' ? ref.id : ref;
}

function graphHash(text) {
  let hash = 0;
  const value = String(text || '');
  for (let i = 0; i < value.length; i += 1) {
    hash = ((hash << 5) - hash) + value.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function graphSeedOffset(seed, axis, span) {
  const hash = graphHash(String(seed) + ':' + axis);
  return ((hash % 1000) / 999 - 0.5) * span;
}

function graphNowMs() {
  return window.performance && typeof window.performance.now === 'function' ? window.performance.now() : Date.now();
}

function graphEaseOutCubic(t) {
  const clamped = Math.max(0, Math.min(1, t));
  return 1 - Math.pow(1 - clamped, 3);
}

function graphMarkNodeIntro(node, bornAt) {
  if (!node) return;
  node.__graphBornAt = typeof bornAt === 'number' ? bornAt : graphNowMs();
}

function graphNodeIntroProgress(node) {
  if (!node || typeof node.__graphBornAt !== 'number') return 1;
  const elapsed = graphNowMs() - node.__graphBornAt;
  if (elapsed >= GRAPH_NODE_INTRO_MS) {
    delete node.__graphBornAt;
    return 1;
  }
  return graphEaseOutCubic(elapsed / GRAPH_NODE_INTRO_MS);
}

function graphScheduleIntroFrames(durationMs) {
  if (!_graphInstance || typeof _graphInstance.refresh !== 'function') return;
  _graphIntroUntil = Math.max(_graphIntroUntil || 0, graphNowMs() + Math.max(durationMs || 0, 180));
  if (_graphIntroFrame) return;
  const pump = () => {
    _graphIntroFrame = 0;
    if (activeTabPanelId() !== 'panel-graph' || !_graphInstance || typeof _graphInstance.refresh !== 'function') {
      _graphIntroUntil = 0;
      return;
    }
    _graphInstance.refresh();
    if (graphNowMs() < _graphIntroUntil) {
      _graphIntroFrame = requestAnimationFrame(pump);
    } else {
      _graphIntroUntil = 0;
    }
  };
  _graphIntroFrame = requestAnimationFrame(pump);
}

function graphCentroid(nodesById) {
  let x = 0;
  let y = 0;
  let count = 0;
  nodesById.forEach(node => {
    if (Number.isFinite(node.x) && Number.isFinite(node.y)) {
      x += node.x;
      y += node.y;
      count += 1;
    }
  });
  if (!count) return { x: 0, y: 0 };
  return { x: x / count, y: y / count };
}

function graphFindAnchor(nodeId, edges, oldNodes) {
  for (const edge of (edges || [])) {
    const sourceId = graphRefId(edge.source);
    const targetId = graphRefId(edge.target);
    let neighborId = null;
    if (sourceId === nodeId) neighborId = targetId;
    if (targetId === nodeId) neighborId = sourceId;
    if (!neighborId) continue;
    const anchor = oldNodes.get(neighborId);
    if (anchor && Number.isFinite(anchor.x) && Number.isFinite(anchor.y)) {
      return anchor;
    }
  }
  return null;
}

function seedGraphNodePosition(node, edges, oldNodes, centroid) {
  const anchor = graphFindAnchor(node.id, edges, oldNodes) || centroid;
  node.x = anchor.x + graphSeedOffset(node.id, 'x', 36);
  node.y = anchor.y + graphSeedOffset(node.id, 'y', 36);
  node.vx = 0;
  node.vy = 0;
}

function graphLinkKey(link) {
  return graphRefId(link.source) + '|' + graphRefId(link.target) + '|' + (link.label || '');
}

function graphShowAffinityLinks() {
  const toggle = document.getElementById('graph-toggle-affinity');
  return !toggle || !!toggle.checked;
}

function graphAffinityThreshold() {
  const slider = document.getElementById('graph-affinity-threshold');
  const value = slider ? parseFloat(slider.value) : 0.35;
  return Number.isFinite(value) ? value : 0.35;
}

function graphUpdateAffinityThresholdLabel() {
  const label = document.getElementById('graph-affinity-threshold-val');
  if (label) {
    label.textContent = graphAffinityThreshold().toFixed(2);
  }
}

function graphAffinityPassesThreshold(link) {
  if (!link || link.semantics !== 'affinity') return true;
  return Number(link.strength || 0) >= graphAffinityThreshold();
}

function graphCurrentHoverNodeID() {
  return String(_graphDragNodeId || _graphPointerHoverNodeId || '').trim();
}

function graphSetHoverNode(node) {
  _graphPointerHoverNodeId = node ? graphRefId(node.id) : '';
  const nodeId = graphCurrentHoverNodeID();
  if (_graphHoverNodeId === nodeId) return;
  if (nodeId) _graphHoverEaseStart = performance.now(); // smooth focus ease-in
  _graphHoverNodeId = nodeId;
  _graphHoverAffinityNeighbors = new Set();
  _graphHoverDistanceByNode = new Map();
  if (nodeId) {
    const visible = graphBuildVisibleData();
    const adjacency = new Map();
    function connect(left, right) {
      if (!left || !right) return;
      if (!adjacency.has(left)) adjacency.set(left, new Set());
      adjacency.get(left).add(right);
    }
    (visible.links || []).forEach(function(link) {
      const sourceId = graphRefId(link.source);
      const targetId = graphRefId(link.target);
      connect(sourceId, targetId);
      connect(targetId, sourceId);
    });
    const queue = [nodeId];
    _graphHoverDistanceByNode.set(nodeId, 0);
    while (queue.length) {
      const currentId = queue.shift();
      const currentDist = Number(_graphHoverDistanceByNode.get(currentId) || 0);
      const neighbors = adjacency.get(currentId) || new Set();
      neighbors.forEach(function(neighborId) {
        if (!neighborId || _graphHoverDistanceByNode.has(neighborId)) return;
        const nextDist = currentDist + 1;
        _graphHoverDistanceByNode.set(neighborId, nextDist);
        if (nextDist === 1) {
          _graphHoverAffinityNeighbors.add(neighborId);
        }
        queue.push(neighborId);
      });
    }
  }
  if (_graphInstance && typeof _graphInstance.refresh === 'function') {
    _graphInstance.refresh();
  }
}

function graphSetDragHoverNode(node) {
  _graphDragNodeId = node ? graphRefId(node.id) : '';
  const nodeId = graphCurrentHoverNodeID();
  if (_graphHoverNodeId === nodeId) return;
  _graphHoverNodeId = '';
  graphSetHoverNode(_graphPointerHoverNodeId ? { id: _graphPointerHoverNodeId } : null);
}

function graphClearHoverState() {
  _graphDragNodeId = '';
  _graphPointerHoverNodeId = '';
  if (!_graphHoverNodeId && _graphHoverDistanceByNode.size === 0 && _graphHoverAffinityNeighbors.size === 0) return;
  _graphHoverNodeId = '';
  _graphHoverAffinityNeighbors = new Set();
  _graphHoverDistanceByNode = new Map();
  if (_graphInstance && typeof _graphInstance.refresh === 'function') {
    _graphInstance.refresh();
  }
}

function graphHoverHighlightsLink(link) {
  return graphHoverDistanceForLink(link) === 0;
}

function graphHoverHasAffinityFocus() {
  return !!_graphHoverNodeId && _graphHoverDistanceByNode.size > 0;
}

function graphHoverDistanceForNode(node) {
  const nodeID = graphRefId(node && node.id);
  if (!nodeID || !graphHoverHasAffinityFocus()) return null;
  if (!_graphHoverDistanceByNode.has(nodeID)) return null;
  return Number(_graphHoverDistanceByNode.get(nodeID));
}

function graphHoverDistanceForLink(link) {
  if (!link || !graphHoverHasAffinityFocus()) return null;
  const sourceID = graphRefId(link.source);
  const targetID = graphRefId(link.target);
  const sourceDist = _graphHoverDistanceByNode.has(sourceID) ? Number(_graphHoverDistanceByNode.get(sourceID)) : null;
  const targetDist = _graphHoverDistanceByNode.has(targetID) ? Number(_graphHoverDistanceByNode.get(targetID)) : null;
  if (sourceDist === null && targetDist === null) return null;
  if (sourceDist === null) return targetDist;
  if (targetDist === null) return sourceDist;
  return Math.min(sourceDist, targetDist);
}

function graphHoverNodeFalloff(node) {
  const distance = graphHoverDistanceForNode(node);
  if (distance === null) return graphHoverHasAffinityFocus() ? 0.08 : 1;
  if (distance <= 0) return 1;
  if (distance === 1) return 0.96;
  if (distance === 2) return 0.42;
  if (distance === 3) return 0.22;
  return 0.12;
}

function graphHoverLinkFalloff(link) {
  const distance = graphHoverDistanceForLink(link);
  if (distance === null) return graphHoverHasAffinityFocus() ? 0.05 : 1;
  if (distance <= 0) return 1;
  if (distance === 1) return 0.42;
  if (distance === 2) return 0.2;
  return 0.1;
}

function graphIsMemoryOverlayMode() {
  return (_graphLoadedMode || graphSelectedMode()) === 'MEMORY_OVERLAY';
}

function graphIsMemoryContextMode() {
  const mode = _graphLoadedMode || graphSelectedMode();
  return mode === 'MEMORY_OVERLAY' || mode === 'MEMORY_ATLAS';
}

function graphLinkTouchesMemory(link) {
  if (!link) return false;
  const sourceID = String(graphRefId(link.source) || '').trim();
  const targetID = String(graphRefId(link.target) || '').trim();
  return sourceID.indexOf('memory:') === 0 || targetID.indexOf('memory:') === 0;
}

function graphNodeStatusIs(node, status) {
  return String((node && node.status) || '').trim().toUpperCase() === String(status || '').trim().toUpperCase();
}

function graphBlockingPulse() {
  return 0.5 + 0.5 * Math.sin(Date.now() / 210);
}

function graphMemoryMetric(node, field) {
  const value = Number(node && node[field]);
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

function graphMemoryNodeRadius(node, scale) {
  const base = 5.5 * (scale || 1);
  const importance = graphMemoryMetric(node, 'importance');
  return base + importance * 5.2 * (scale || 1);
}

function graphMemoryNodeGlow(node) {
  return 0.1 + graphMemoryMetric(node, 'activation') * 0.26;
}

function graphMemoryNodeRings(node) {
  if (!node || node.type !== 'memory_node') return [];
  const rings = [];
  const drift = graphMemoryMetric(node, 'drift');
  if (drift >= 0.2) {
    rings.push({ color: '#ffb86b', width: 1.2, offset: 2.2 + drift * 1.4, alpha: 0.28 + drift * 0.28 });
  }
  if (node.protect || node.retention_prunable === false && node.retention_band === 'PRUNABLE') {
    rings.push({ color: '#7dd3fc', width: 1.1, offset: 4.2, alpha: 0.36 });
  }
  if (node.recovery_candidate) {
    rings.push({ color: '#fb7185', width: 1.25, offset: 6, alpha: 0.4 });
  }
  if (node.unresolved || String(node.epistemic_status || '').trim().toUpperCase() === 'DISPUTED') {
    rings.push({ color: '#f87171', width: 1.15, offset: 7.8, alpha: 0.34 });
  }
  return rings;
}

function graphNodeIsBlockingAction(node) {
  if (!node || node.type !== 'action') return false;
  const nodeID = graphRefId(node.id);
  return !!nodeID && _graphBlockingActionNodeIds.has(nodeID);
}

function graphNodeIsBlocked(node) {
  if (!node) return false;
  const nodeID = graphRefId(node.id);
  if (!nodeID) return false;
  return _graphBlockedNodeIds.has(nodeID);
}

function graphLinkIsBlocking(link) {
  if (!link) return false;
  const sourceID = graphRefId(link.source);
  const targetID = graphRefId(link.target);
  return String(link.label || '') === 'blocked_by_action' && _graphBlockingActionNodeIds.has(targetID) && _graphBlockedNodeIds.has(sourceID);
}

function graphLinkTouchesBlockedChain(link) {
  if (!link || graphLinkIsBlocking(link)) return false;
  const sourceID = graphRefId(link.source);
  const targetID = graphRefId(link.target);
  return _graphBlockedNodeIds.has(sourceID) || _graphBlockedNodeIds.has(targetID);
}

function graphRecomputeBlockerState() {
  const blockingActions = new Set();
  const blockedNodes = new Set();
  const nodeByID = new Map((_graphData.nodes || []).map(function(node) {
    return [graphRefId(node.id), node];
  }));

  (_graphData.links || []).forEach(function(link) {
    if (String(link.label || '') !== 'blocked_by_action') return;
    const sourceID = graphRefId(link.source);
    const targetID = graphRefId(link.target);
    const actionNode = nodeByID.get(targetID);
    if (!sourceID || !actionNode || actionNode.type !== 'action' || !graphNodeStatusIs(actionNode, 'PENDING')) return;
    blockingActions.add(targetID);
    blockedNodes.add(sourceID);
  });

  let changed = true;
  while (changed) {
    changed = false;
    (_graphData.links || []).forEach(function(link) {
      const sourceID = graphRefId(link.source);
      const targetID = graphRefId(link.target);
      if (!sourceID || !targetID) return;
      if (String(link.label || '') === 'works_on_task' && blockedNodes.has(targetID) && !blockedNodes.has(sourceID)) {
        blockedNodes.add(sourceID);
        changed = true;
      }
      if (String(link.label || '') === 'contains_node' && blockedNodes.has(sourceID) && !blockedNodes.has(targetID)) {
        blockedNodes.add(targetID);
        changed = true;
      }
    });
  }

  _graphBlockingActionNodeIds = blockingActions;
  _graphBlockedNodeIds = blockedNodes;

  // A BLOCKED claim arrives as a claims_task edge with "solid" semantics
  // (CLAIMED ones are "animated") - task status itself never says BLOCKED.
  const blockedClaims = new Set();
  (_graphData.links || []).forEach(function(link) {
    if (String(link.label || '') !== 'claims_task') return;
    if (link.semantics === 'animated') return;
    blockedClaims.add(graphRefId(link.target));
  });
  _graphBlockedClaimTaskIds = blockedClaims;

  // Tensions with no currently-live agent attached are "cold": they keep
  // their alert shape but drop to ink so live alerts stand out.
  const coldTensions = new Set();
  (_graphData.nodes || []).forEach(function(node) {
    if (node && node.type === 'tension') coldTensions.add(graphRefId(node.id));
  });
  if (coldTensions.size) {
    (_graphData.links || []).forEach(function(link) {
      const sourceID = graphRefId(link.source);
      const targetID = graphRefId(link.target);
      let tensionID = '';
      let other = null;
      if (coldTensions.has(sourceID)) { tensionID = sourceID; other = nodeByID.get(targetID); }
      else if (coldTensions.has(targetID)) { tensionID = targetID; other = nodeByID.get(sourceID); }
      if (!tensionID || !other) return;
      if (graphAgentIsLive(other)) coldTensions.delete(tensionID);
    });
  }
  _graphColdTensionIds = coldTensions;

  // live node index for effects + per-hub constellation membership for halos
  _graphNodeByRef = nodeByID;
  const members = new Map();
  const owner = new Map();
  (_graphData.nodes || []).forEach(function(node) {
    if (node && (node.type === 'agent' || node.type === 'human')) {
      const ref = graphRefId(node.id);
      members.set(ref, [node]);
      owner.set(ref, ref);
    }
  });
  if (members.size) {
    let grew = true;
    let guard = 0;
    while (grew && guard < 6) {
      grew = false;
      guard += 1;
      (_graphData.links || []).forEach(function(link) {
        if (!_graphLinkIsContainment(link)) return;
        const sourceID = graphRefId(link.source);
        const targetID = graphRefId(link.target);
        const sourceHub = owner.get(sourceID);
        const targetHub = owner.get(targetID);
        if (sourceHub && !targetHub) {
          const node = nodeByID.get(targetID);
          if (node) { owner.set(targetID, sourceHub); members.get(sourceHub).push(node); grew = true; }
        } else if (targetHub && !sourceHub) {
          const node = nodeByID.get(sourceID);
          if (node) { owner.set(sourceID, targetHub); members.get(targetHub).push(node); grew = true; }
        }
      });
    }
  }
  _graphClusterMembers = members;
}

function graphAgentIsLive(node) {
  if (!node || (node.type !== 'agent' && node.type !== 'human')) return false;
  if (node.is_online === true || node.active === true) return true;
  const st = String(node.status || '').trim().toUpperCase();
  return st === 'ACTIVE' || st === 'RUNNING';
}

function graphTaskHasBlockedClaim(node) {
  return !!node && node.type === 'task' && _graphBlockedClaimTaskIds.has(graphRefId(node.id));
}

// "Inactive" = a finished/abandoned branch of the tree: completed sessions,
// resolved/cancelled tasks, offline hubs. Used to fade both edges and labels
// so the live part of the workspace reads at a glance.
function graphNodeIsInactive(node) {
  if (!node || typeof node !== 'object') return false;
  const st = String(node.status || '').trim().toUpperCase();
  if (node.type === 'session') return st === 'STOPPED' || st === 'COMPLETED' || st === 'FAILED' || st === 'CANCELLED';
  if (node.type === 'task') return st === 'COMPLETED' || st === 'RESOLVED' || st === 'CANCELLED' || st === 'FAILED';
  if (node.type === 'agent' || node.type === 'human') return st === 'OFFLINE' || st === 'PHANTOM';
  if (node.type === 'action' || node.type === 'dag_node') return st === 'COMPLETED' || st === 'FAILED' || st === 'CANCELLED';
  if (node.type === 'tension') return _graphColdTensionIds.has(graphRefId(node.id));
  return false;
}

function graphLinkInactiveFactor(link) {
  if (!link) return 1;
  return (graphNodeIsInactive(link.source) || graphNodeIsInactive(link.target)) ? 0.42 : 1;
}

function graphNodeVisualAlpha(node) {
  let alpha = 1;
  if (graphIsMemoryContextMode()) {
    if (node && node.type === 'memory_node') {
      alpha = graphIsMemoryAtlasMode() ? (0.56 + graphMemoryMetric(node, 'activation') * 0.44) : 1;
    } else {
      alpha = _graphMemoryOverlayAnchorIds.has(graphRefId(node && node.id))
        ? (graphIsMemoryAtlasMode() ? 0.56 : 0.5)
        : (graphIsMemoryAtlasMode() ? 0.14 : 0.18);
    }
  }
  if (graphHoverHasAffinityFocus()) {
    alpha *= graphHoverEased(graphHoverNodeFalloff(node));
  }
  if (graphNodeIsBlocked(node) && !graphNodeIsHoveredOrFocused(node)) {
    alpha *= 0.34;
  }
  if (graphNodeIsBlockingAction(node)) {
    alpha = Math.max(alpha, 0.96);
  }
  return alpha;
}

function graphLabelVisualAlpha(node) {
  let alpha = 1;
  if (graphIsMemoryContextMode()) {
    if (node && node.type === 'memory_node') {
      alpha = 1;
    } else {
      alpha = _graphMemoryOverlayAnchorIds.has(graphRefId(node && node.id))
        ? (graphIsMemoryAtlasMode() ? 0.58 : 0.62)
        : (graphIsMemoryAtlasMode() ? 0.16 : 0.2);
    }
  }
  if (graphHoverHasAffinityFocus()) {
    alpha *= graphHoverEased(Math.max(0.12, graphHoverNodeFalloff(node)));
  }
  if (graphNodeIsBlocked(node) && !graphNodeIsHoveredOrFocused(node)) {
    alpha *= 0.4;
  }
  if (graphNodeIsInactive(node) && !graphNodeIsHoveredOrFocused(node)) {
    alpha *= 0.55;
  }
  if (graphNodeIsBlockingAction(node)) {
    alpha = Math.max(alpha, 0.92);
  }
  return alpha;
}

function graphNodeIsHoveredOrFocused(node) {
  const nodeID = graphRefId(node && node.id);
  if (!nodeID) return false;
  if (_graphHoverNodeId && _graphHoverNodeId === nodeID) return true;
  return !!(_graphFocusNode && graphRefId(_graphFocusNode.id) === nodeID);
}

function graphNodeInHoverNeighborhood(node) {
  const nodeID = graphRefId(node && node.id);
  if (!nodeID || !graphHoverHasAffinityFocus()) return false;
  return _graphHoverAffinityNeighbors.has(nodeID);
}

function graphMemoryAnchorIDs(memoryNodeID) {
  const ids = [];
  const seen = new Set();
  memoryNodeID = String(memoryNodeID || '').trim();
  if (!memoryNodeID) return ids;
  (_graphData.links || []).forEach(function(link) {
    if (!graphLinkTouchesMemory(link)) return;
    const sourceID = String(graphRefId(link.source) || '').trim();
    const targetID = String(graphRefId(link.target) || '').trim();
    let anchorID = '';
    if (sourceID === memoryNodeID && targetID.indexOf('memory:') !== 0) anchorID = targetID;
    if (targetID === memoryNodeID && sourceID.indexOf('memory:') !== 0) anchorID = sourceID;
    if (!anchorID || seen.has(anchorID)) return;
    seen.add(anchorID);
    ids.push(anchorID);
  });
  return ids;
}

function graphMemoryAnchorCentroid(node) {
  if (!node || node.type !== 'memory_node' || !graphIsMemoryContextMode()) return null;
  const nodeByID = new Map((_graphData.nodes || []).map(function(entry) { return [graphRefId(entry.id), entry]; }));
  const anchors = graphMemoryAnchorIDs(graphRefId(node.id))
    .map(function(anchorID) { return nodeByID.get(anchorID); })
    .filter(function(anchor) { return anchor && Number.isFinite(anchor.x) && Number.isFinite(anchor.y); });
  if (!anchors.length) return null;
  let x = 0;
  let y = 0;
  anchors.forEach(function(anchor) {
    x += anchor.x;
    y += anchor.y;
  });
  return { x: x / anchors.length, y: y / anchors.length };
}

function graphNodeLabelText(node) {
  let label = String((node && (node.label || node.id)) || '').trim();
  if (!label) return '';
  if (!graphIsMemoryContextMode()) return label;
  if (node.type === 'session' && !graphNodeIsHoveredOrFocused(node) && !graphIsMemoryAtlasMode()) return '';
  if (graphIsMemoryAtlasMode() && node.type !== 'memory_node' && !graphNodeIsHoveredOrFocused(node)) {
    if (!_graphMemoryOverlayAnchorIds.has(graphRefId(node.id))) return '';
  }
  if (graphIsMemoryAtlasMode() && node.type === 'session') return label;
  if (node.type === 'session') return 'Session';
  return label;
}

// Level-of-detail label alpha for the normal (non-memory) graph mode:
// hub names anchor the map at any zoom, alert/cluster labels arrive early,
// task labels fade in as you approach, micro-nodes only up close.
function graphLabelZoomAlpha(node, globalScale) {
  if (!node) return 0;
  if (graphNodeIsHoveredOrFocused(node)) return 1;
  function ramp(a, b) { return Math.max(0, Math.min(1, (globalScale - a) / (b - a))); }
  const t = node.type;
  if (t === 'agent' || t === 'human') return 1;
  if (t === 'session') return 0; // generic labels: tooltip/hover only
  if (t === 'tension' || t === 'proto_cluster') return ramp(0.3, 0.55);
  if (t === 'dag_node' || t === 'action' || t === 'queue_item') return ramp(1.05, 1.5);
  return ramp(0.55, 1.0); // tasks, memory, everything else
}

function graphLabelRectCollides(rect) {
  for (let i = 0; i < _graphLabelRects.length; i++) {
    const o = _graphLabelRects[i];
    if (rect.x < o.x + o.w && rect.x + rect.w > o.x && rect.y < o.y + o.h && rect.y + rect.h > o.y) return true;
  }
  return false;
}

function graphShouldRenderLabel(node, globalScale) {
  if (!node) return false;
  if (!graphIsMemoryContextMode()) {
    return graphLabelZoomAlpha(node, globalScale) > 0.03;
  }
  if (graphIsMemoryAtlasMode()) {
    if (node.type === 'memory_node') return globalScale > 0.3;
    if (graphNodeIsHoveredOrFocused(node)) return globalScale > 0.3;
    return _graphMemoryOverlayAnchorIds.has(graphRefId(node.id)) && globalScale > 0.55;
  }
  if (node.type === 'memory_node') return globalScale > 0.34;
  if (node.type === 'session') return graphNodeIsHoveredOrFocused(node) && globalScale > 0.34;
  return globalScale > 0.5;
}

function graphNodeLabelPlacement(node, radius, fontSize) {
  const placement = { x: 0, y: radius + 2, align: 'center' };
  if (!graphIsMemoryContextMode() || !node || node.type !== 'memory_node') return placement;
  const anchor = graphMemoryAnchorCentroid(node);
  if (!anchor || !Number.isFinite(node.x) || !Number.isFinite(node.y)) return placement;
  let dx = node.x - anchor.x;
  let dy = node.y - anchor.y;
  const dist = Math.sqrt(dx * dx + dy * dy) || 1;
  dx /= dist;
  dy /= dist;
  const radial = radius + 10;
  placement.x = dx * (Math.abs(dx) > 0.6 ? radial : radial * 0.35);
  placement.y = dy * radial + (dy < 0 ? (-fontSize - 6) : 2);
  if (Math.abs(dx) > 0.58) {
    placement.align = dx >= 0 ? 'left' : 'right';
  }
  return placement;
}

// Classify an edge by its endpoint node types so the layout can give structural
// (containment) links short, rigid springs and cross-group links long, loose
// ones — this is what pulls each agent's constellation into a tidy cluster
// while letting separate groups drift apart.
function _graphLinkEndType(end) { return (end && typeof end === 'object') ? (end.type || '') : ''; }
function _graphLinkIsContainment(link) {
  const st = _graphLinkEndType(link.source), tt = _graphLinkEndType(link.target);
  if (link.semantics === 'warning' || st === 'tension' || tt === 'tension') return false;
  if (link.semantics === 'affinity') return false;
  if (st === 'proto_cluster' || tt === 'proto_cluster') return false;
  if ((st === 'agent' || st === 'human') && (tt === 'agent' || tt === 'human')) return false;
  return true;
}
function graphLinkStructuralFactor(link) {
  const st = _graphLinkEndType(link.source), tt = _graphLinkEndType(link.target);
  if (link.semantics === 'warning' || st === 'tension' || tt === 'tension') return 0.85;
  if (link.semantics === 'affinity') return 1.05;
  if (st === 'proto_cluster' || tt === 'proto_cluster') return 0.9;
  if ((st === 'agent' || st === 'human') && (tt === 'agent' || tt === 'human')) return 1.1;
  return 0.34; // containment (agent→session→task→step): tight
}
function graphLinkStrengthValue(link) {
  const st = _graphLinkEndType(link.source), tt = _graphLinkEndType(link.target);
  if (link.semantics === 'warning' || st === 'tension' || tt === 'tension') return 0.45;
  if (link.semantics === 'affinity') return 0.12;
  if (st === 'proto_cluster' || tt === 'proto_cluster') return 0.3;
  if ((st === 'agent' || st === 'human') && (tt === 'agent' || tt === 'human')) return 0.18;
  return 0.9; // structural links rigid → tidy constellations
}

function graphLinkDistanceValue(link) {
  const base = parseInt(document.getElementById('st-linkdist').value, 10) || 80;
  if (!graphIsMemoryContextMode()) return base * graphLinkStructuralFactor(link);
  if (!graphLinkTouchesMemory(link)) return Math.max(base * 0.92, 48);
  const sourceID = String(graphRefId(link && link.source) || '').trim();
  const targetID = String(graphRefId(link && link.target) || '').trim();
  const memoryToMemory = sourceID.indexOf('memory:') === 0 && targetID.indexOf('memory:') === 0;
  if (graphIsMemoryAtlasMode()) {
    if (memoryToMemory) return Math.max(base * 0.44, 20);
    return Math.max(base * 0.34, 22);
  }
  if (memoryToMemory) return Math.max(base * 0.5, 24);
  return Math.max(base * 0.42, 26);
}

function graphVisibleLinks(links) {
  const showAffinity = graphShowAffinityLinks();
  return (links || []).filter(function(link) {
    if (!showAffinity && link.hidden_by_default) return false;
    return graphAffinityPassesThreshold(link);
  });
}

function graphBuildVisibleData() {
  const mode = _graphLoadedMode || graphSelectedMode();
  const links = graphVisibleLinks(_graphData.links || []);
  const connected = new Set();
  links.forEach(function(link) {
    connected.add(graphRefId(link.source));
    connected.add(graphRefId(link.target));
  });
  if (mode === 'MEMORY_OVERLAY' || mode === 'MEMORY_ATLAS') {
    const keep = new Set();
    const memoryAnchors = new Set();
    (_graphData.nodes || []).forEach(function(node) {
      if (node.type === 'memory_node') {
        keep.add(graphRefId(node.id));
      }
    });
    links.forEach(function(link) {
      const sourceID = graphRefId(link.source);
      const targetID = graphRefId(link.target);
      if (String(sourceID || '').indexOf('memory:') === 0 || String(targetID || '').indexOf('memory:') === 0) {
        keep.add(sourceID);
        keep.add(targetID);
        if (String(sourceID || '').indexOf('memory:') !== 0) memoryAnchors.add(sourceID);
        if (String(targetID || '').indexOf('memory:') !== 0) memoryAnchors.add(targetID);
      }
    });
    _graphMemoryOverlayAnchorIds = memoryAnchors;
    return {
      nodes: (_graphData.nodes || []).filter(function(node) {
        return keep.has(graphRefId(node.id));
      }),
      links: links.filter(function(link) {
        return keep.has(graphRefId(link.source)) && keep.has(graphRefId(link.target));
      }),
    };
  }
  _graphMemoryOverlayAnchorIds = new Set();
  return {
    nodes: (_graphData.nodes || []).filter(function(node) {
      return node.type !== 'proto_cluster' || connected.has(node.id);
    }),
    links: links,
  };
}

function _legacyUpdateGraphStatsLine(extra) {
  const suffix = extra ? ' · ' + extra : '';
  document.getElementById('graph-stats').textContent = 'Nodes: ' + _graphData.nodes.length + ' / Edges: ' + _graphData.links.length + suffix;
}

function updateGraphStatsLine(extra) {
  const visibleData = graphBuildVisibleData();
  const visibleEdges = (visibleData.links || []).length;
  const totalEdges = (_graphData.links || []).length;
  const visibleNodes = (visibleData.nodes || []).length;
  const totalNodes = (_graphData.nodes || []).length;
  const edgeText = visibleEdges === totalEdges ? String(totalEdges) : (String(visibleEdges) + ' of ' + String(totalEdges));
  const nodeText = visibleNodes === totalNodes ? String(totalNodes) : (String(visibleNodes) + ' of ' + String(totalNodes));
  let detail = extra || '';
  if (!detail) {
    const potentialDetail = graphShowAffinityLinks() ? ('potential >= ' + graphAffinityThreshold().toFixed(2)) : 'potential off';
    if ((_graphLoadedMode || graphSelectedMode()) === 'TASK_FOCUS') {
      const focusID = String(_graphLoadedFocus || _graphRequestedFocusID || '').trim();
      detail = focusID ? ('task focus: ' + graphFocusLabel(focusID) + ' - ' + potentialDetail) : ('task focus - select a task - ' + potentialDetail);
    } else if ((_graphLoadedMode || graphSelectedMode()) === 'MEMORY_ATLAS') {
      const focusID = String(_graphLoadedFocus || _graphRequestedFocusID || '').trim();
      const query = graphMemoryAtlasQuery();
      const layer = graphMemoryAtlasLayer();
      const lifecycle = graphMemoryAtlasLifecycle();
      const origin = graphMemoryAtlasOriginKind();
      const epistemic = graphMemoryAtlasEpistemicStatus();
      const depth = graphMemoryAtlasDepth();
      const anchors = graphMemoryAtlasShowAnchors();
      const canonical = graphMemoryAtlasCanonicalOnly();
      const stats = _graphSnapshotStats || {};
      const focusBits = [];
      const overviewBits = [];
      if (layer) focusBits.push(layer.toLowerCase());
      if (lifecycle) focusBits.push(lifecycle.toLowerCase());
      if (origin) focusBits.push(origin.replace(/_/g, ' '));
      if (epistemic) focusBits.push(epistemic.toLowerCase());
      if (canonical) focusBits.push('canonical only');
      if (anchors) focusBits.push('anchors on');
      if (Number(stats.lineage_edge_count || 0) > 0) focusBits.push(String(stats.lineage_edge_count) + ' lineage');
      if (Number(stats.anchor_node_count || 0) > 0) focusBits.push(String(stats.anchor_node_count) + ' anchors');
      if (Number(stats.frontier_hops || 0) > 0) focusBits.push(String(stats.frontier_hops) + ' expansion hops');
      if (query) overviewBits.push('search "' + query + '"');
      if (layer) overviewBits.push(layer.toLowerCase());
      if (lifecycle) overviewBits.push(lifecycle.toLowerCase());
      if (origin) overviewBits.push(origin.replace(/_/g, ' '));
      if (epistemic) overviewBits.push(epistemic.toLowerCase());
      if (canonical) overviewBits.push('canonical only');
      if (anchors) overviewBits.push('anchors on');
      if (Number(stats.seed_count || 0) > 0) overviewBits.push(String(stats.seed_count) + ' seeds');
      if (Number(stats.frontier_hops || 0) > 0) overviewBits.push(String(stats.frontier_hops) + ' expansion hops');
      if (stats.seed_source_counts && typeof stats.seed_source_counts === 'object') {
        const sourceKinds = Object.keys(stats.seed_source_counts).filter(function(key) {
          return Number(stats.seed_source_counts[key] || 0) > 0;
        });
        if (sourceKinds.length === 1) {
          overviewBits.push(sourceKinds[0].replace(/_/g, ' ') + ' seeds');
        } else if (sourceKinds.length > 1) {
          overviewBits.push(String(sourceKinds.length) + ' source types');
        }
      }
      detail = focusID
        ? ('memory atlas: ' + graphMemoryAtlasFocusLabel(focusID) + ' - ' + depth + '-hop' + (focusBits.length ? (' - ' + focusBits.join(' - ')) : ''))
        : ('memory atlas overview' + (overviewBits.length ? (' - ' + overviewBits.join(' - ')) : ''));
    } else if ((_graphLoadedMode || graphSelectedMode()) === 'MEMORY_OVERLAY') {
      const counts = graphMemoryOverlayCounts();
      if (!counts.nodes) {
        detail = 'memory overlay - no canonical workspace memory recorded yet';
      } else {
        detail = 'memory overlay' +
          (counts.nodes ? (' · ' + counts.nodes + ' memory nodes') : '') +
          (counts.edges ? (' · ' + counts.edges + ' memory links') : '') +
          ' - ' + potentialDetail;
      }
    } else if ((_graphLoadedMode || graphSelectedMode()) === 'CONTROL') {
      const focusID = String(_graphLoadedFocus || _graphRequestedFocusID || '').trim();
      if (focusID) {
        const controlDetail = graphControlClusterScopeSummary(focusID);
        detail = 'control: ' + graphControlFocusLabel(focusID) + (controlDetail ? (' · ' + controlDetail) : '') + ' - ' + potentialDetail;
      } else {
        const clusters = graphControlClusterCandidates();
        const hotCount = clusters.filter(function(cluster) { return String(cluster.attention_band || '').trim().toUpperCase() === 'HOT'; }).length;
        const peakPressure = clusters.reduce(function(best, cluster) { return Math.max(best, Number(cluster.pressure_score || 0)); }, 0);
        detail = 'control overview' +
          (clusters.length ? (' · ' + clusters.length + ' clusters') : '') +
          (hotCount ? (' · ' + hotCount + ' hot') : '') +
          (peakPressure > 0 ? (' · peak pressure ' + peakPressure) : '') +
          ' - ' + potentialDetail;
      }
    } else {
      detail = potentialDetail;
    }
  }
  const suffix = detail ? ' - ' + detail : '';
  document.getElementById('graph-stats').textContent = 'Nodes: ' + nodeText + ' / Edges: ' + edgeText + suffix;
}

function graphDecodePayloadJSON(payloadJSON) {
  if (typeof payloadJSON !== 'string' || !payloadJSON.trim()) return null;
  try {
    return JSON.parse(payloadJSON);
  } catch (err) {
    return null;
  }
}

function applyGraphLiveHint(evtType, eventData) {
  if (graphSelectedMode() !== 'SYSTEM') return false;
  if (evtType !== 'task.created' || !_graphInstance || !_graphInstance.hasLoadedData) return false;
  const payload = graphDecodePayloadJSON(eventData && eventData.payload_json);
  if (!payload || !payload.task_id) return false;
  if ((_graphData.nodes || []).some(node => node.id === payload.task_id)) return false;

  const oldNodes = new Map((_graphData.nodes || []).map(node => [node.id, node]));
  const centroid = graphCentroid(oldNodes);
  const hintedNode = {
    id: payload.task_id,
    label: payload.title || payload.task_id,
    status: payload.status || 'PENDING',
    type: 'task',
    author: payload.owner_user_id || '',
    created_at: payload.created_at || '',
  };
  seedGraphNodePosition(hintedNode, _graphData.links || [], oldNodes, centroid);
  graphMarkNodeIntro(hintedNode);
  _graphData.nodes = (_graphData.nodes || []).concat(hintedNode);
  _graphInstance.graphData(graphBuildVisibleData());
  updateGraphStatsLine('live');
  graphScheduleIntroFrames(GRAPH_NODE_INTRO_MS);
  return true;
}

function checkGraphTabActive() {
  const t = document.querySelector('.tab-panel.active');
  if (t && t.id === 'panel-graph') {
    syncGraphControlsUI();
    if (document.visibilityState !== 'hidden') {
      startGraphExternalPolling();
    }
    if (!_graphLibLoaded) {
      _graphLibLoaded = true;
      const scriptURL = new URL('assets/force-graph.min.js', window.location.href).href;
      document.getElementById('graph-stats').textContent = 'Fetching engine from ' + scriptURL + '...';

      // Self-check
      fetch(scriptURL, { method: 'HEAD' })
        .then(r => {
           if(!r.ok) throw new Error('HTTP ' + r.status);
           const script = document.createElement('script');
           script.src = scriptURL;
           script.onload = () => { initGraphData(); };
           script.onerror = () => {
             document.getElementById('graph-stats').textContent = 'Error: Script execution failed (CSP/MIME issue).';
             _graphLibLoaded = false;
           };
           document.head.appendChild(script);
        })
        .catch(err => {
           document.getElementById('graph-stats').textContent = 'Error: Asset unreachable at ' + scriptURL + ' (' + err.message + ')';
           _graphLibLoaded = false;
        });
    } else {
      if (!_graphInstance) initGraphData();
    }
  } else {
    if (_graphSyncTimer) clearTimeout(_graphSyncTimer);
    stopGraphExternalPolling();
  }
}

// Hook into switchTab, override it if it exists or use standard logic
const _origSwitchTab = window.switchTab;
window.switchTab = function(tabName) {
  if (_origSwitchTab) _origSwitchTab(tabName);

  // Standard logic if not intercepted
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabName));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.id === 'panel-' + tabName));
  localStorage.setItem('active_tab', tabName);

  checkGraphTabActive();
  if (tabName !== 'graph') {
    stopGraphExternalPolling();
    debouncedRefresh();
  }
};

document.addEventListener('visibilitychange', function() {
  const onGraph = activeTabPanelId() === 'panel-graph';
  if (document.visibilityState === 'hidden') {
    if (onGraph) stopGraphExternalPolling();
    return;
  }
  // Became visible: revive the live stream first, then pull fresh state.
  ensureLiveConnection();
  if (onGraph) {
    startGraphExternalPolling();
    triggerGraphSync(0);
  } else {
    renderAgents();               // instant presence correction from cache
    if (TOKEN) refresh().catch(err => console.error('visibility refresh', err));
  }
});

async function initGraphData() {
  if (!window.ForceGraph) {
     if (_graphLibLoaded) document.getElementById('graph-stats').textContent = 'Waiting for force-graph to load...';
     return;
  }
  if (_graphSyncInFlight) {
    _graphSyncPending = true;
    return;
  }
  _graphSyncInFlight = true;
  const mode = graphSelectedMode();
  const focusID = mode === 'TASK_FOCUS'
    ? String(_graphRequestedFocusID || _graphLoadedFocus || '').trim()
    : mode === 'CONTROL'
      ? String(_graphRequestedFocusID || _graphLoadedFocus || '').trim()
      : mode === 'MEMORY_ATLAS'
        ? String(_graphRequestedFocusID || _graphLoadedFocus || '').trim()
    : '';
  try {
    if (!_graphInstance) {
      document.getElementById('graph-stats').textContent = 'Loading projection...';
    }
    if (mode === 'TASK_FOCUS' && !focusID) {
      _graphLoadedMode = 'TASK_FOCUS';
      _graphLoadedFocus = '';
      _graphData = { nodes: [], links: [] };
      graphRecomputeBlockerState();
      if (_graphInstance) {
        _graphInstance.graphData(graphBuildVisibleData());
        _graphInstance.hasLoadedData = true;
      }
      updateGraphFocusUI();
      updateGraphStatsLine('task focus - select a task node');
      return;
    }
    const snap = mode === 'MEMORY_ATLAS'
      ? await rpc('workspace.memory.graph.atlas', {
          workspace_id: WS_ID,
          center_memory_id: focusID || undefined,
          query: graphMemoryAtlasQuery() || undefined,
          memory_type: undefined,
          memory_layer: graphMemoryAtlasLayer() || undefined,
          visibility: undefined,
          epistemic_status: graphMemoryAtlasEpistemicStatus() || undefined,
          lifecycle_state: graphMemoryAtlasLifecycle() || undefined,
          origin_kind: graphMemoryAtlasOriginKind() || undefined,
          include_anchors: graphMemoryAtlasShowAnchors(),
          include_archived: graphMemoryAtlasIncludeArchived(),
          canonical_only: graphMemoryAtlasCanonicalOnly(),
          depth: graphMemoryAtlasDepth(),
          limit_nodes: 80,
          limit_edges: 140,
          min_importance: undefined,
          min_activation: undefined
        })
      : await rpc('workspace.graph.snapshot', {
          workspace_id: WS_ID, mode: mode, focus_id: focusID || undefined, limit: 1000
        });

    // Keep object identity for existing nodes and seed new nodes near their anchors.
    let topoChanged = false;
    let paintChanged = false;
    let addedNodeCount = 0;
    let addedLinkCount = 0;
    const graphNodeSyncFields = ['ref_id', 'label', 'status', 'type', 'author', 'created_at', 'summary', 'memory_type', 'memory_layer', 'visibility', 'epistemic_status', 'lifecycle_state', 'canonical_authority', 'surface_authority', 'surface_role', 'compatibility_only', 'origin_kind', 'origin_id', 'source_kind', 'source_id', 'agent_id', 'session_id', 'task_id', 'confidence', 'importance', 'activation', 'drift', 'semantic_lineage_id', 'retention_band', 'retention_prunable', 'protect', 'unresolved', 'recovery_candidate', 'recovery_guard_reason', 'recovery_trigger_count'];
    const oldNodes = new Map((_graphData.nodes || []).map(n => [n.id, n]));
    const oldLinks = new Map((_graphData.links || []).map(l => [graphLinkKey(l), l]));
    const centroid = graphCentroid(oldNodes);
    if ((_graphData.nodes || []).length !== (snap.nodes || []).length) topoChanged = true;
    const newNodes = (snap.nodes || []).map(n => {
       const old = oldNodes.get(n.id);
       if (old) {
          if (old.status !== n.status) {
            graphNoteNodeHeat(old.id);
            const newStatus = String(n.status || '').toUpperCase();
            if (old.type === 'task' && (newStatus === 'RESOLVED' || newStatus === 'COMPLETED')) {
              graphSpawnEffect(old.id, 'spark'); // quiet resolve celebration
            }
          }
          if (graphNodeSyncFields.some(function(field) { return old[field] !== n[field]; })) {
            paintChanged = true;
          }
          graphNodeSyncFields.forEach(function(field) {
            old[field] = n[field];
          });
           return old;
       }
       topoChanged = true;
       addedNodeCount += 1;
       seedGraphNodePosition(n, snap.edges || [], oldNodes, centroid);
       graphMarkNodeIntro(n);
       return n;
    });
    if ((_graphData.links || []).length !== (snap.edges || []).length) topoChanged = true;
    const newLinks = (snap.edges || []).map(l => {
       const key = graphLinkKey(l);
       const old = oldLinks.get(key);
       if (old) {
          if (
            old.label !== l.label ||
            old.semantics !== l.semantics ||
            old.authority !== l.authority ||
            old.strength !== l.strength ||
            old.fit_score !== l.fit_score ||
            old.semantic_distance !== l.semantic_distance ||
            old.evidence_count !== l.evidence_count ||
            old.source_model !== l.source_model ||
            old.hidden_by_default !== l.hidden_by_default
          ) {
            paintChanged = true;
          }
          old.label = l.label;
          old.semantics = l.semantics;
          old.authority = l.authority;
          old.strength = l.strength;
          old.fit_score = l.fit_score;
          old.semantic_distance = l.semantic_distance;
          old.evidence_count = l.evidence_count;
          old.source_model = l.source_model;
          old.hidden_by_default = l.hidden_by_default;
          return old;
       }
       topoChanged = true;
       addedLinkCount += 1;
       return l;
    });
    _graphData.nodes = newNodes;
    _graphData.links = newLinks;
    _graphLoadedMode = String(snap.mode || mode || 'SYSTEM');
    _graphLoadedFocus = String(snap.focus || focusID || '').trim();
    _graphRequestedFocusID = graphModeSupportsFocus(_graphLoadedMode) ? _graphLoadedFocus : '';
    if (_graphLoadedMode === 'MEMORY_ATLAS') {
      graphRememberMemoryAtlasLabels(snap.nodes || []);
    }
    _graphSnapshotStats = (snap && snap.stats) || {};
    _graphLastTimeAuth = typeof snap.time_authority === 'string'
      ? snap.time_authority
      : ((snap.time_authority && snap.time_authority.reference_at) || '');
    graphRecomputeBlockerState();

    updateGraphFocusUI();
    updateGraphStatsLine();
    var nodeScale = parseFloat(document.getElementById('st-nodesize').value) || 1.0;

      // Ink + violet design code: monochrome by default, violet marks "live/active",
      // semantic red/amber reserved for alerts. Shape (drawn below) carries category.
      var INK = {
        bg:'#07070b', bright:'#e8e6e3', mid:'#8b8a87', dim:'#4a4a50', done:'#5c5b63',
        hubFill:'#15151c', violet:'#a855f7', red:'#e06a6a', amber:'#d6a23c'
      };
      function _graphNodeActive(n) {
        if (n.is_online === true || n.active === true) return true;
        var st = String(n.status || '').trim().toUpperCase();
        return st === 'ACTIVE' || st === 'RUNNING';
      }
      function _nodeColor(n) {
        var st = String(n.status || '').trim().toUpperCase();
        if (n.type === 'agent' || n.type === 'human') {
          if (st === 'PHANTOM' || st === 'OFFLINE') return INK.dim;
          return _graphNodeActive(n) ? INK.violet : INK.bright;
        }
        if (n.type === 'session') {
          if (st === 'STOPPED' || st === 'COMPLETED') return INK.dim;
          if (st === 'BLOCKED') return INK.red;
          return st === 'ACTIVE' ? INK.violet : INK.mid;
        }
        if (n.type === 'task') {
          if (st === 'BLOCKED' || graphTaskHasBlockedClaim(n)) return INK.red;
          if (st === 'RUNNING') return INK.violet;
          if (st === 'COMPLETED' || st === 'RESOLVED') return INK.done; // solid grey = done
          if (st === 'CANCELLED' || st === 'FAILED') return INK.dim;
          return INK.bright; // UNASSIGNED/PENDING
        }
        if (n.type === 'dag_node') return st === 'BLOCKED' ? INK.red : INK.mid;
        if (n.type === 'action') {
          if (graphNodeIsBlockingAction(n)) return INK.red;
          if (st === 'COMPLETED' || st === 'FAILED') return INK.dim;
          return INK.mid;
        }
        if (n.type === 'queue_item') return INK.mid; // hollow-square shape carries "waiting"
        if (n.type === 'proto_cluster') {
          if (st === 'HOT') return INK.red;
          if (st === 'WATCH') return INK.amber;
          return INK.mid;
        }
        if (n.type === 'tension') {
          if (_graphColdTensionIds.has(graphRefId(n.id))) return INK.dim; // no live agent attached
          return st === 'ACTIVE' ? INK.amber : INK.red; // tension IS the alert
        }
        if (n.type === 'memory_node') {
          var lifecycle = String(n.lifecycle_state || n.status || '').trim().toUpperCase();
          if (lifecycle === 'ARCHIVED' || lifecycle === 'DORMANT' || lifecycle === 'SUPERSEDED') return INK.dim;
          var epistemic = String(n.epistemic_status || '').trim().toUpperCase();
          if (epistemic === 'DISPUTED' || epistemic === 'RETRACTED') return INK.red;
          // diamond shape carries "memory"; keep it inky, identity a touch brighter
          return String(n.memory_layer || '').trim().toUpperCase() === 'IDENTITY' ? INK.bright : INK.mid;
        }
        return INK.mid;
      }
      function _nodeRadius(n) {
        if (n.type === 'agent' || n.type === 'human') return 12;
        if (n.type === 'tension') return 9;
        if (n.type === 'proto_cluster') return 11;
        if (n.type === 'task') return 4.4;
        if (n.type === 'dag_node') return 3.4;
        if (n.type === 'action') return 3;
        if (n.type === 'queue_item') return 3.8;
        if (n.type === 'session') return 4.2;
        if (n.type === 'memory_node') return graphMemoryNodeRadius(n, 1);
        return 4.4;
      }
      // Shape primitives + dispatcher (shape = ontological category).
      function _graphSquare(ctx, x, y, h, fill, stroke, lw) {
        ctx.beginPath(); ctx.rect(x - h, y - h, 2 * h, 2 * h);
        if (fill) { ctx.fillStyle = fill; ctx.fill(); }
        if (stroke) { ctx.strokeStyle = stroke; ctx.lineWidth = lw; ctx.stroke(); }
      }
      function _graphDiamond(ctx, x, y, h, fill) {
        ctx.save(); ctx.translate(x, y); ctx.rotate(Math.PI / 4);
        ctx.beginPath(); ctx.rect(-h, -h, 2 * h, 2 * h); ctx.fillStyle = fill; ctx.fill();
        ctx.restore();
      }
      function _graphTriangle(ctx, x, y, r, fill, stroke, lw) {
        ctx.beginPath();
        for (var i = 0; i < 3; i++) {
          var a = -Math.PI / 2 + i * 2.0943951;
          var px = x + r * Math.cos(a), py = y + r * Math.sin(a);
          if (i) ctx.lineTo(px, py); else ctx.moveTo(px, py);
        }
        ctx.closePath();
        if (fill) { ctx.fillStyle = fill; ctx.fill(); }
        if (stroke) { ctx.strokeStyle = stroke; ctx.lineWidth = lw; ctx.stroke(); }
      }
      function _graphDrawNodeShape(node, ctx, x, y, r, col, globalScale) {
        var t = node.type;
        if (t === 'agent' || t === 'human') {
          // hub: filled dark disc + colored ring (name renders beside, like other nodes)
          ctx.beginPath(); ctx.arc(x, y, r, 0, 6.2831853); ctx.fillStyle = INK.hubFill; ctx.fill();
          ctx.strokeStyle = col;
          ctx.lineWidth = _graphNodeActive(node)
            ? 2 + 0.35 * Math.sin(performance.now() / 740 + graphHashSeed(node.id) * 6.283)
            : 2;
          ctx.stroke();
          if (t === 'human') { ctx.beginPath(); ctx.arc(x, y, r + 3, 0, 6.2831853); ctx.strokeStyle = col; ctx.lineWidth = 1; ctx.stroke(); }
          return;
        }
        if (t === 'session') { ctx.beginPath(); ctx.arc(x, y, r, 0, 6.2831853); ctx.strokeStyle = col; ctx.lineWidth = 1.3; ctx.stroke(); return; }
        if (t === 'task') {
          var tst = String(node.status || '').toUpperCase();
          var hollow = tst === 'CANCELLED' || tst === 'FAILED'; // terminal-but-not-done stays hollow
          _graphSquare(ctx, x, y, r, hollow ? null : col, col, 1.2); return;
        }
        if (t === 'dag_node' || t === 'action') { _graphSquare(ctx, x, y, r, col, null, 0); return; }
        if (t === 'queue_item') { _graphSquare(ctx, x, y, r, null, col, 1.2); return; }
        if (t === 'proto_cluster') { ctx.save(); ctx.setLineDash([4, 4]); ctx.lineDashOffset = -((performance.now() / 55) % 8); ctx.beginPath(); ctx.arc(x, y, r, 0, 6.2831853); ctx.strokeStyle = col; ctx.lineWidth = 1.1; ctx.stroke(); ctx.restore(); return; }
        if (t === 'tension') { _graphTriangle(ctx, x, y, r + 1, col, col === INK.dim ? col : INK.bright, 0.8); return; }
        if (t === 'memory_node') { _graphDiamond(ctx, x, y, r, col); ctx.beginPath(); ctx.arc(x, y, r + 2.4, 0, 6.2831853); ctx.strokeStyle = col; ctx.lineWidth = 0.7; ctx.stroke(); return; }
        ctx.beginPath(); ctx.arc(x, y, r, 0, 6.2831853); ctx.fillStyle = col; ctx.fill();
      }
      function _nodeNeedsBackgroundMask(n) {
        if (n.status === 'PHANTOM' || n.status === 'CANCELLED' || n.status === 'OFFLINE') return true;
        if (n.type === 'task' && n.status === 'FAILED') return true;
        if (n.type === 'session' && (n.status === 'STOPPED' || n.status === 'COMPLETED')) return true;
        if (n.type === 'memory_node' && (n.status === 'ARCHIVED' || n.status === 'DORMANT' || n.status === 'SUPERSEDED')) return true;
        return false;
      }
    if (!_graphInstance) {
      _graphInstance = ForceGraph()
        (document.getElementById('graph-container'))
        .backgroundColor('#07070b')
        .autoPauseRedraw(false) // breathing/dust/comets/ripples need a live frame loop
        .nodeId('id')
        .nodeLabel(function(n) {
          if (n.type === 'memory_node') {
            const parts = ['memory'];
            if (n.memory_type) parts.push(n.memory_type);
            if (n.memory_layer) parts.push(n.memory_layer);
            return n.label + ' [' + parts.join(' · ') + ']' + (n.status ? ' (' + n.status + ')' : '');
          }
          var statusText = n.status ? String(n.status) : '';
          if (n.type === 'task' && graphTaskHasBlockedClaim(n) && statusText.toUpperCase() !== 'BLOCKED') {
            statusText = (statusText ? statusText + ' · ' : '') + 'claim BLOCKED';
          }
          return n.label + ' [' + n.type + ']' + (statusText ? ' (' + statusText + ')' : '');
        })
        .linkSource('source')
        .linkTarget('target')
        .linkLabel(function(link) {
          if (link.semantics === 'affinity') {
            var pull = Math.round(Number(link.strength || 0) * 100);
            if (link.source_model === 'attachment') {
              var fit = Math.round(Number(link.fit_score || 0) * 100);
              return 'candidate_for | fit ' + fit + '% | pull ' + pull + '%';
            }
            if (link.source_model === 'surface') {
              return 'surfaces | surface ' + pull + '%' + (link.evidence_count ? ' | evidence ' + String(link.evidence_count) : '');
            }
            if (link.source_model === 'control') {
              return 'pressure_on | pressure ' + pull + '%';
            }
            return (link.label || 'affinity') + ' | score ' + pull + '%';
          }
          return link.label;
        })
        .linkColor(function(link) {
          var overlayAlpha = graphIsMemoryContextMode() ? (graphLinkTouchesMemory(link) ? 1 : 0.24) : 1;
          var blockedAlpha = graphLinkTouchesBlockedChain(link) ? 0.22 : 1;
          var hoverAlpha = graphHoverHasAffinityFocus() ? graphHoverEased(graphHoverLinkFalloff(link)) : 1;
          if (link.semantics === 'affinity') {
            var alpha = 0.08 + 0.28 * Math.max(0, Math.min(1, Number(link.strength || 0)));
            if (graphHoverHasAffinityFocus()) {
              alpha *= hoverAlpha;
              if (graphHoverHighlightsLink(link)) {
                alpha = Math.min(0.68, alpha + 0.22);
              }
            }
            alpha *= overlayAlpha * blockedAlpha;
            if (link.source_model === 'surface') return 'rgba(94, 234, 212, ' + alpha + ')';
            if (link.source_model === 'control') return 'rgba(255, 166, 87, ' + alpha + ')';
            return 'rgba(244, 208, 63, ' + alpha + ')';
          }
          var inactiveAlpha = graphLinkInactiveFactor(link);
          if (link.semantics === 'warning') return 'rgba(255, 123, 114, ' + (0.78 * overlayAlpha * hoverAlpha * inactiveAlpha) + ')';
          // a non-animated claims_task edge is a BLOCKED claim - tie it to its red task
          if (String(link.label || '') === 'claims_task' && link.semantics !== 'animated') {
            return 'rgba(224, 106, 106, ' + (0.5 * overlayAlpha * blockedAlpha * hoverAlpha) + ')';
          }
          if (link.semantics === 'muted') return 'rgba(139, 148, 158, ' + (0.24 * overlayAlpha * blockedAlpha * hoverAlpha * inactiveAlpha) + ')';
          if (link.semantics === 'dashed') return 'rgba(139, 148, 158, ' + (0.26 * overlayAlpha * blockedAlpha * hoverAlpha * inactiveAlpha) + ')';
          if (link.semantics === 'animated') return 'rgba(88, 166, 255, ' + (0.38 * overlayAlpha * blockedAlpha * hoverAlpha * inactiveAlpha) + ')';
          // structural ink (runs_session / works_on_task): neutral, fades on dead branches
          return 'rgba(176, 174, 170, ' + (0.3 * overlayAlpha * blockedAlpha * hoverAlpha * inactiveAlpha) + ')';
        })
        .linkWidth(function(link) {
          if (link.semantics === 'affinity') {
            var base = 0.6 + 1.8 * Math.max(0, Math.min(1, Number(link.strength || 0)));
            if (graphHoverHasAffinityFocus()) {
              var hoverDistance = graphHoverDistanceForLink(link);
              if (hoverDistance === null) return 0.14;
              if (hoverDistance === 0) return base + 1.2;
              if (hoverDistance === 1) return Math.max(0.32, base * 0.56);
              if (hoverDistance === 2) return Math.max(0.22, base * 0.32);
              return 0.14;
            }
            return base;
          }
          if (graphHoverHasAffinityFocus()) {
            var hoverDistance = graphHoverDistanceForLink(link);
            if (hoverDistance === null) return 0.18;
            if (hoverDistance === 0) {
              if (link.semantics === 'warning') return 2.8;
              if (link.semantics === 'muted') return 1.25;
              return link.semantics === 'solid' ? 1.9 : 1.35;
            }
            if (hoverDistance === 1) {
              if (link.semantics === 'warning') return 1.3;
              return link.semantics === 'solid' ? 0.95 : 0.74;
            }
            if (hoverDistance === 2) return 0.34;
            return 0.18;
          }
          var thin = graphLinkInactiveFactor(link) < 1 ? 0.75 : 1;
          if (link.semantics === 'warning') return 2.2 * thin;
          if (link.semantics === 'muted') return 0.9 * thin;
          return (link.semantics === 'solid' ? 1.5 : 1.1) * thin;
        })
        .linkCurvature(function(link) {
          // structural spokes stay straight; cross-cluster ties arc gently
          if (_graphLinkIsContainment(link)) return 0;
          var sign = graphHashSeed(graphLinkKey(link)) > 0.5 ? 1 : -1;
          if (link.semantics === 'warning') return 0.2 * sign;
          if (link.semantics === 'affinity') return 0.28 * sign;
          return 0.16 * sign;
        })
        .linkDirectionalArrowLength(function(link) { return link.semantics === 'affinity' ? 0 : 4; })
        .linkDirectionalArrowRelPos(function(link) { return link.semantics === 'affinity' ? 0 : 1; })
        .linkDirectionalParticles(function(link) {
          if (graphLinkInactiveFactor(link) < 1) return 0; // dead branches do not flow
          if (link.semantics === 'warning') return 2;
          return 0; // 'animated' claims now flow as custom comets (onRenderFramePost)
        })
        .linkDirectionalParticleWidth(function(link) { return link.semantics === 'warning' ? 2.6 : (link.semantics === 'animated' ? 2 : 0); })
        .linkDirectionalParticleSpeed(function(link) { return link.semantics === 'warning' ? 0.006 : (link.semantics === 'animated' ? 0.004 : 0); })
        .linkDirectionalParticleColor(function(link) { return link && link.semantics === 'warning' ? 'rgba(255, 123, 114, 0.92)' : 'rgba(88, 166, 255, 0.6)'; })
        .onNodeHover(function(node) {
          graphSetHoverNode(node || null);
          document.getElementById('graph-container').style.cursor = node ? 'pointer' : 'default';
          if (node) {
            updateGraphStatsLine('hover focus');
          } else {
            updateGraphStatsLine();
          }
        })
        .onNodeDrag(function(node) {
          _graphPointerHoverNodeId = '';
          graphSetDragHoverNode(node || null);
          document.getElementById('graph-container').style.cursor = 'grabbing';
          updateGraphStatsLine('drag focus');
        })
        .onNodeDragEnd(function() {
          graphClearHoverState();
          document.getElementById('graph-container').style.cursor = 'default';
          updateGraphStatsLine();
        })
        .onRenderFramePre(function(ctx, globalScale) {
          var nowMs = performance.now();
          // cluster halos: a soft tinted field behind each hub constellation
          if (!graphIsMemoryContextMode()) {
            _graphClusterMembers.forEach(function(memberNodes, hubRef) {
              if (!memberNodes || memberNodes.length < 2) return;
              var hub = _graphNodeByRef.get(hubRef);
              if (!hub || !Number.isFinite(hub.x)) return;
              var cx = 0, cy = 0, count = 0;
              for (var mi = 0; mi < memberNodes.length; mi++) {
                var m = memberNodes[mi];
                if (!Number.isFinite(m.x) || !Number.isFinite(m.y)) continue;
                cx += m.x; cy += m.y; count += 1;
              }
              if (count < 2) return;
              cx /= count; cy /= count;
              var maxD = 0;
              for (var mj = 0; mj < memberNodes.length; mj++) {
                var mm = memberNodes[mj];
                if (!Number.isFinite(mm.x)) continue;
                var dx = mm.x - cx, dy = mm.y - cy;
                var dist = Math.sqrt(dx * dx + dy * dy);
                if (dist > maxD) maxD = dist;
              }
              var radius = Math.min(420, maxD + 30);
              if (radius < 40) return;
              var live = graphAgentIsLive(hub);
              var grad = ctx.createRadialGradient(cx, cy, radius * 0.12, cx, cy, radius);
              var tint = live ? '168,85,247' : '150,148,145';
              grad.addColorStop(0, 'rgba(' + tint + ',' + (live ? 0.05 : 0.028) + ')');
              grad.addColorStop(1, 'rgba(' + tint + ',0)');
              ctx.beginPath();
              ctx.arc(cx, cy, radius, 0, 6.2831853);
              ctx.fillStyle = grad;
              ctx.fill();
            });
          }

          // comets: a bright head with a fading tail flowing along live claim
          // edges - drawn under links/nodes so they read as edge traffic
          var cometCount = 0;
          var links = _graphData.links || [];
          for (var li = 0; li < links.length && cometCount < 80; li++) {
            var link = links[li];
            if (link.semantics !== 'animated') continue;
            var s = link.source, t = link.target;
            if (!s || !t || typeof s !== 'object' || typeof t !== 'object') continue;
            if (!Number.isFinite(s.x) || !Number.isFinite(t.x)) continue;
            if (graphLinkInactiveFactor(link) < 1) continue;
            var hoverA = graphHoverHasAffinityFocus() ? graphHoverEased(graphHoverLinkFalloff(link)) : 1;
            if (hoverA < 0.12) continue;
            cometCount += 1;
            var seed = graphHashSeed(graphLinkKey(link));
            var phase = ((nowMs / 3800) * (0.75 + seed * 0.5) + seed) % 1;
            var tail = Math.max(0, phase - 0.13);
            var hx = s.x + (t.x - s.x) * phase, hy = s.y + (t.y - s.y) * phase;
            var tx = s.x + (t.x - s.x) * tail, ty = s.y + (t.y - s.y) * tail;
            var grad = ctx.createLinearGradient(tx, ty, hx, hy);
            grad.addColorStop(0, 'rgba(124,140,255,0)');
            grad.addColorStop(1, 'rgba(150,160,255,' + (0.55 * hoverA) + ')');
            ctx.beginPath();
            ctx.moveTo(tx, ty);
            ctx.lineTo(hx, hy);
            ctx.strokeStyle = grad;
            ctx.lineWidth = 1.1;
            ctx.stroke();
            ctx.beginPath();
            ctx.arc(hx, hy, 1.5, 0, 6.2831853);
            ctx.fillStyle = 'rgba(178,186,255,' + (0.85 * hoverA) + ')';
            ctx.fill();
          }

          // reserve hub label rects first so agent names always win the
          // collision contest against task labels drawn earlier in node order
          _graphLabelRects = [];
          if (graphIsMemoryContextMode()) return;
          var scale = window._graphNodeScale || 1.0;
          var fontSize = Math.min(14, Math.max(3, 12 / globalScale));
          ctx.save();
          ctx.font = fontSize + 'px Inter, system-ui, sans-serif';
          (_graphData.nodes || []).forEach(function(node) {
            if (!node || (node.type !== 'agent' && node.type !== 'human')) return;
            if (!Number.isFinite(node.x) || !Number.isFinite(node.y)) return;
            var label = graphNodeLabelText(node);
            if (!label) return;
            if (label.length > 40) label = label.substring(0, 37) + '...';
            var tw = ctx.measureText(label).width;
            var pad = fontSize * 0.15;
            _graphLabelRects.push({ x: node.x - tw / 2 - pad, y: node.y + 12 * scale + 2, w: tw + pad * 2, h: fontSize + pad * 2 });
          });
          ctx.restore();
        })
        .onRenderFramePost(function(ctx, globalScale) {
          var nowMs = performance.now();
          // activity ripples + resolve sparks
          if (_graphFxRipples.length) {
            var alive = [];
            for (var ri = 0; ri < _graphFxRipples.length; ri++) {
              var fx = _graphFxRipples[ri];
              var ttl = fx.kind === 'spark' ? GRAPH_FX_SPARK_MS : GRAPH_FX_RIPPLE_MS;
              var age = nowMs - fx.t0;
              if (age >= ttl) continue;
              alive.push(fx);
              var node = _graphNodeByRef.get(fx.id);
              if (!node || !Number.isFinite(node.x) || !Number.isFinite(node.y)) continue;
              var e = age / ttl;
              var eased = 1 - Math.pow(1 - e, 2);
              if (fx.kind === 'spark') {
                // quiet resolve: a faint flash and five drifting ink motes
                ctx.save();
                ctx.globalAlpha = (1 - e) * 0.22;
                ctx.beginPath();
                ctx.arc(node.x, node.y, 3 + eased * 7, 0, 6.2831853);
                ctx.fillStyle = '#e8e6e3';
                ctx.fill();
                ctx.globalAlpha = (1 - e) * 0.75;
                ctx.fillStyle = '#e8e6e3';
                var sparkSeed = graphHashSeed(fx.id) * 6.283;
                for (var k = 0; k < 5; k++) {
                  var ang = sparkSeed + k * 1.2566;
                  var dist = 4 + eased * 15;
                  var size = 1.5 * (1 - e);
                  ctx.fillRect(node.x + Math.cos(ang) * dist - size / 2, node.y + Math.sin(ang) * dist - size / 2, size, size);
                }
                ctx.restore();
              } else {
                ctx.save();
                ctx.globalAlpha = (1 - e) * 0.32;
                ctx.beginPath();
                ctx.arc(node.x, node.y, 5 + eased * 34, 0, 6.2831853);
                ctx.strokeStyle = '#a855f7';
                ctx.lineWidth = 1.2 * (1 - e * 0.6);
                ctx.stroke();
                ctx.restore();
              }
            }
            _graphFxRipples = alive;
          }
        })
        .nodePointerAreaPaint(function(node, color, ctx) {
          var scale = window._graphNodeScale || 1.0;
          var r = _nodeRadius(node) * scale;
          var pad = (node.status !== 'PHANTOM' && node.status !== 'CANCELLED' && node.status !== 'FAILED' && node.status !== 'OFFLINE') ? 2 : 0;
          ctx.fillStyle = color;
          ctx.beginPath();
          ctx.arc(node.x, node.y, r + pad, 0, 2 * Math.PI, false);
          ctx.fill();
        })
        .nodeCanvasObject(function(node, ctx, globalScale) {
          var scale = window._graphNodeScale || 1.0;
          var intro = graphNodeIntroProgress(node);
          var r = _nodeRadius(node) * scale * (0.68 + intro * 0.32);
          var col = _nodeColor(node);
          var nodeAlpha = graphNodeVisualAlpha(node);
          var labelAlpha = graphLabelVisualAlpha(node);
          var blockerPulse = graphNodeIsBlockingAction(node) ? graphBlockingPulse() : 0;

          if (graphNodeIsBlockingAction(node)) {
            ctx.save();
            ctx.globalAlpha = (0.16 + blockerPulse * 0.24) * nodeAlpha;
            ctx.beginPath();
            ctx.arc(node.x, node.y, r + 4.5 + blockerPulse * 3.2, 0, 2 * Math.PI, false);
            ctx.fillStyle = '#ff7b72';
            ctx.fill();
            ctx.restore();
          }

          // soft glow kept only for live hubs + memory emphasis; crisp ink otherwise
          if (node.status !== 'PHANTOM' && node.status !== 'CANCELLED' && node.status !== 'FAILED' && node.status !== 'OFFLINE') {
            // live hubs breathe: slow desynchronized glow pulse per agent
            var breath = 0.5 + 0.5 * Math.sin(performance.now() / 740 + graphHashSeed(node.id) * 6.283);
            var heat = graphNodeHeat(node);
            var glowA = graphNodeIsBlockingAction(node) ? (0.22 + blockerPulse * 0.18)
              : (node.type === 'memory_node' ? graphMemoryNodeGlow(node)
              : (((node.type === 'agent' || node.type === 'human') && _graphNodeActive(node)) ? (0.07 + breath * 0.07) : 0));
            // recency warmth: freshly-touched nodes carry a faint warm halo that cools off
            if (heat > 0.03 && node.type !== 'agent' && node.type !== 'human') {
              ctx.save();
              ctx.globalAlpha = heat * 0.16 * nodeAlpha;
              ctx.beginPath();
              ctx.arc(node.x, node.y, r + 2.6 + heat * 2, 0, 2 * Math.PI, false);
              ctx.fillStyle = '#efe9df';
              ctx.fill();
              ctx.restore();
            }
            if (glowA > 0) {
              ctx.save();
              ctx.globalAlpha = glowA * nodeAlpha;
              ctx.beginPath();
              ctx.arc(node.x, node.y, r + 2 + (1 - intro) * 2.5, 0, 2 * Math.PI, false);
              ctx.fillStyle = col;
              ctx.fill();
              ctx.restore();
            }
          }

          // shape (shape = category; ink/violet fill = state)
          ctx.save();
          ctx.globalAlpha *= (0.35 + intro * 0.65) * nodeAlpha;
          if (node.status === 'PHANTOM' || node.status === 'CANCELLED') {
             ctx.setLineDash([2, 2]);
             ctx.globalAlpha = 0.6 * nodeAlpha;
          }
          if (_nodeNeedsBackgroundMask(node)) {
            // Only translucent/inert node fills need a background mask.
            ctx.beginPath();
            ctx.arc(node.x, node.y, r + 0.6, 0, 2 * Math.PI, false);
            ctx.fillStyle = INK.bg;
            ctx.fill();
          }
          _graphDrawNodeShape(node, ctx, node.x, node.y, r, col, globalScale);
          ctx.restore();

          if (node.type === 'memory_node') {
            graphMemoryNodeRings(node).forEach(function(ring) {
              ctx.save();
              ctx.globalAlpha = ring.alpha * nodeAlpha;
              ctx.beginPath();
              ctx.arc(node.x, node.y, r + ring.offset, 0, 2 * Math.PI, false);
              ctx.strokeStyle = ring.color;
              ctx.lineWidth = ring.width;
              ctx.stroke();
              ctx.restore();
            });
          }

          // label - LOD: hubs anchor, the rest fades in with zoom and yields
          // to already-placed labels instead of stacking on top of them
          if (graphShouldRenderLabel(node, globalScale)) {
            var zoomAlpha = graphIsMemoryContextMode() ? 1 : graphLabelZoomAlpha(node, globalScale);
            if (zoomAlpha <= 0.03) return;
            var label = graphNodeLabelText(node);
            if (!label) return;
            if (label.length > 40) label = label.substring(0, 37) + '...';
            var fontSize = Math.min(14, Math.max(3, (graphIsMemoryOverlayMode() && node.type !== 'memory_node' ? 10.5 : 12) / globalScale));
            var placement = graphNodeLabelPlacement(node, r, fontSize);
            ctx.font = fontSize + 'px Inter, system-ui, sans-serif';
            ctx.textAlign = placement.align;
            ctx.textBaseline = 'top';
            var tw = ctx.measureText(label).width;
            var pad = fontSize * 0.15;
            var labelX = node.x + placement.x;
            var labelY = node.y + placement.y;
            var rectX = labelX - tw/2 - pad;
            if (placement.align === 'left') rectX = labelX - pad;
            if (placement.align === 'right') rectX = labelX - tw - pad;
            var isHub = node.type === 'agent' || node.type === 'human';
            if (!graphIsMemoryContextMode() && !isHub && !graphNodeIsHoveredOrFocused(node)) {
              var rect = { x: rectX, y: labelY, w: tw + pad * 2, h: fontSize + pad * 2 };
              if (graphLabelRectCollides(rect)) return;
              _graphLabelRects.push(rect);
            }
            ctx.save();
            ctx.globalAlpha = (0.35 + intro * 0.65) * labelAlpha * zoomAlpha;
            ctx.fillStyle = 'rgba(10,10,13,0.78)';
            ctx.fillRect(rectX, labelY, tw + pad*2, fontSize + pad*2);
            ctx.fillStyle = '#d8d6d3';
            ctx.fillText(label, labelX, labelY + pad);
            ctx.restore();
          }
        })
        .warmupTicks(24)
        .cooldownTicks(75)
        .onNodeClick(function(node) {
          _graphInspectorDismissedNodeID = '';
          _graphFocusNode = node;
          updateGraphInspector();       // open the inspector first so the camera
          graphCinematicFocus(node);    // can account for the space it covers
        })
        .onBackgroundClick(function() {
          graphCinematicRelease();
        });

      var initRep = parseInt(document.getElementById('st-repulsion').value, 10) || -200;
      var initLd = parseInt(document.getElementById('st-linkdist').value, 10) || 80;

      // Custom D3 Force to provide radial gravity (pulling components back to center)
      function createGravityForce() {
        var nodes;
        function force(alpha) {
          var grav = parseInt(document.getElementById('st-gravity').value, 10) * 0.001;
          if(!nodes) return;
          for(var i=0, n=nodes.length; i<n; ++i) {
            if(isNaN(nodes[i].x) || isNaN(nodes[i].y)) continue;
            nodes[i].vx -= nodes[i].x * grav * alpha;
            nodes[i].vy -= nodes[i].y * grav * alpha;
          }
        }
        force.initialize = function(_nodes) { nodes = _nodes; };
        return force;
      }

      function createMemoryOverlayForce() {
        var nodes;
        function force(alpha) {
          if (!graphIsMemoryContextMode() || !nodes || !nodes.length) return;
          var nodeByID = new Map();
          var memoryNodes = [];
          nodes.forEach(function(node) {
            nodeByID.set(graphRefId(node.id), node);
            if (node.type === 'memory_node') memoryNodes.push(node);
          });
          var anchorMap = new Map();
          (_graphData.links || []).forEach(function(link) {
            if (!graphLinkTouchesMemory(link)) return;
            var sourceID = String(graphRefId(link.source) || '').trim();
            var targetID = String(graphRefId(link.target) || '').trim();
            if (sourceID.indexOf('memory:') === 0 && targetID.indexOf('memory:') !== 0) {
              if (!anchorMap.has(sourceID)) anchorMap.set(sourceID, []);
              anchorMap.get(sourceID).push(targetID);
            }
            if (targetID.indexOf('memory:') === 0 && sourceID.indexOf('memory:') !== 0) {
              if (!anchorMap.has(targetID)) anchorMap.set(targetID, []);
              anchorMap.get(targetID).push(sourceID);
            }
          });
          var groups = new Map();
          memoryNodes.forEach(function(node) {
            var nodeID = graphRefId(node.id);
            var anchorIDs = (anchorMap.get(nodeID) || []).filter(Boolean);
            if (!anchorIDs.length || !Number.isFinite(node.x) || !Number.isFinite(node.y)) return;
            var anchors = anchorIDs.map(function(anchorID) { return nodeByID.get(anchorID); }).filter(function(anchor) {
              return anchor && Number.isFinite(anchor.x) && Number.isFinite(anchor.y);
            });
            if (!anchors.length) return;
            var cx = 0;
            var cy = 0;
            anchors.forEach(function(anchor) {
              cx += anchor.x;
              cy += anchor.y;
            });
            cx /= anchors.length;
            cy /= anchors.length;
            node.vx += (cx - node.x) * 0.16 * alpha;
            node.vy += (cy - node.y) * 0.16 * alpha;
            var signature = anchorIDs.slice().sort().join('|');
            if (!groups.has(signature)) groups.set(signature, []);
            groups.get(signature).push(node);
          });
          groups.forEach(function(group) {
            for (var i = 0; i < group.length; i += 1) {
              for (var j = i + 1; j < group.length; j += 1) {
                var left = group[i];
                var right = group[j];
                if (!Number.isFinite(left.x) || !Number.isFinite(left.y) || !Number.isFinite(right.x) || !Number.isFinite(right.y)) continue;
                var dx = right.x - left.x;
                var dy = right.y - left.y;
                var dist = Math.sqrt(dx * dx + dy * dy) || 1;
                var target = 54;
                if (dist >= target) continue;
                var push = (target - dist) / target * 0.12 * alpha;
                dx /= dist;
                dy /= dist;
                left.vx -= dx * push;
                left.vy -= dy * push;
                right.vx += dx * push;
                right.vy += dy * push;
              }
            }
          });
        }
        force.initialize = function(_nodes) { nodes = _nodes; };
        return force;
      }

      // Collision force so sized nodes (esp. big agent hubs) never overlap.
      // Uniform-grid broad phase keeps it ~O(n) even for large graphs.
      function createNodeCollideForce() {
        var nodes;
        function radiusOf(n) {
          var s = window._graphNodeScale || 1.0;
          var r = _nodeRadius(n) * s;
          // count the drawn border so strokes never visually touch, plus a small gap
          if (n.type === 'human') return r + 3 + 0.5 + 4;  // outer ring (r+3) + half-stroke + gap
          if (n.type === 'agent') return r + 1 + 4;        // disc border (2px) half + gap
          return r + 0.6 + 1.4;                            // small nodes: ~half stroke + tiny gap
        }
        function force(alpha) {
          if (!nodes || !nodes.length) return;
          var i, n, rad = new Array(nodes.length), maxR = 0;
          for (i = 0; i < nodes.length; i++) { rad[i] = radiusOf(nodes[i]); if (rad[i] > maxR) maxR = rad[i]; }
          var cell = Math.max(8, maxR * 2);
          var grid = new Map();
          for (i = 0; i < nodes.length; i++) {
            n = nodes[i];
            if (!Number.isFinite(n.x) || !Number.isFinite(n.y)) continue;
            var k = Math.floor(n.x / cell) + ':' + Math.floor(n.y / cell);
            var b = grid.get(k); if (!b) { b = []; grid.set(k, b); } b.push(i);
          }
          var strength = 0.5;
          for (i = 0; i < nodes.length; i++) {
            n = nodes[i];
            if (!Number.isFinite(n.x) || !Number.isFinite(n.y)) continue;
            var cx = Math.floor(n.x / cell), cy = Math.floor(n.y / cell);
            for (var gx = cx - 1; gx <= cx + 1; gx++) {
              for (var gy = cy - 1; gy <= cy + 1; gy++) {
                var bucket = grid.get(gx + ':' + gy); if (!bucket) continue;
                for (var bi = 0; bi < bucket.length; bi++) {
                  var j = bucket[bi]; if (j <= i) continue;
                  var m = nodes[j];
                  if (!Number.isFinite(m.x) || !Number.isFinite(m.y)) continue;
                  var dx = m.x - n.x, dy = m.y - n.y;
                  var dist = Math.sqrt(dx * dx + dy * dy) || 0.01;
                  var min = rad[i] + rad[j];
                  if (dist < min) {
                    var push = (min - dist) / dist * strength * alpha;
                    dx *= push; dy *= push;
                    n.vx -= dx; n.vy -= dy; m.vx += dx; m.vy += dy;
                  }
                }
              }
            }
          }
        }
        force.initialize = function(_nodes) { nodes = _nodes; };
        return force;
      }

      // Cluster-cohesion: pull every node of an agent's constellation (incl.
      // deep descendants like task->step that only the parent spring reaches)
      // gently toward that agent's hub, so each cluster stays compact and round
      // and reads as a distinct group. Membership is computed by walking only
      // containment edges out from each hub; cached until the node count changes.
      function createClusterCohesionForce() {
        var nodes;
        var hubOf = null;   // nodeId -> hub node
        var lastN = -1;
        function endId(e) { return (e && typeof e === 'object') ? e.id : e; }
        function recompute() {
          hubOf = new Map();
          var byId = new Map(nodes.map(function(n) { return [n.id, n]; }));
          var adj = new Map();
          function add(a, b) { if (!adj.has(a)) adj.set(a, []); adj.get(a).push(b); }
          (_graphData.links || []).forEach(function(l) {
            if (!_graphLinkIsContainment(l)) return;
            var s = endId(l.source), t = endId(l.target);
            if (s == null || t == null) return;
            add(s, t); add(t, s);
          });
          nodes.forEach(function(h) {
            if (h.type !== 'agent' && h.type !== 'human') return;
            if (hubOf.has(h.id)) return;
            var queue = [h.id]; hubOf.set(h.id, h);
            while (queue.length) {
              var cur = queue.shift();
              var nbrs = adj.get(cur) || [];
              for (var k = 0; k < nbrs.length; k++) {
                var nb = nbrs[k], nbNode = byId.get(nb);
                if (!nbNode || hubOf.has(nb)) continue;
                if (nbNode.type === 'agent' || nbNode.type === 'human') continue; // stop at other hubs
                hubOf.set(nb, h); queue.push(nb);
              }
            }
          });
          lastN = nodes.length;
        }
        function force(alpha) {
          if (!nodes || !nodes.length) return;
          if (!hubOf || nodes.length !== lastN) recompute();
          var strength = 0.07;
          for (var i = 0; i < nodes.length; i++) {
            var n = nodes[i], hub = hubOf.get(n.id);
            if (!hub || hub === n) continue;
            if (!Number.isFinite(n.x) || !Number.isFinite(hub.x)) continue;
            n.vx += (hub.x - n.x) * strength * alpha;
            n.vy += (hub.y - n.y) * strength * alpha;
          }
        }
        force.initialize = function(_nodes) { nodes = _nodes; hubOf = null; lastN = -1; };
        return force;
      }

      _graphInstance.d3Force('gravity', createGravityForce());
      _graphInstance.d3Force('memoryOverlay', createMemoryOverlayForce());
      _graphInstance.d3Force('collide', createNodeCollideForce());
      _graphInstance.d3Force('clusterCohesion', createClusterCohesionForce());
      _graphInstance.d3Force('charge').strength(initRep);
      _graphInstance.d3Force('link').distance(function(link) { return graphLinkDistanceValue(link); }).strength(function(link) { return graphLinkStrengthValue(link); });
      _graphLayoutSettings.repulsion = initRep;
      _graphLayoutSettings.linkDistance = initLd;
      _graphLayoutSettings.gravity = parseInt(document.getElementById('st-gravity').value, 10) || 50;
    }
    if (_graphFocusNode) {
      _graphFocusNode = newNodes.find(n => n.id === _graphFocusNode.id) || null;
      updateGraphInspector();
    }
    const hadLoadedData = !!_graphInstance.hasLoadedData;
    if (!hadLoadedData || topoChanged) {
        _graphInstance.graphData(graphBuildVisibleData());
        _graphInstance.hasLoadedData = true;
        if (hadLoadedData && topoChanged) {
          const removedNodeCount = Math.max(0, oldNodes.size - newNodes.length);
          const removedLinkCount = Math.max(0, oldLinks.size - newLinks.length);
          const shouldReheat =
            removedNodeCount > 0 ||
            removedLinkCount > 0 ||
            addedNodeCount > 2 ||
            addedLinkCount > 4;
          if (shouldReheat) {
            _graphInstance.d3ReheatSimulation();
          }
        }
        if (addedNodeCount > 0) {
          graphScheduleIntroFrames(GRAPH_NODE_INTRO_MS);
        }
    } else if (paintChanged && typeof _graphInstance.refresh === 'function') {
        _graphInstance.refresh();
    }
  } catch(err) {
    document.getElementById('graph-stats').textContent = 'Err: ' + err.message;
  } finally {
    _graphSyncInFlight = false;
    if (_graphSyncPending && document.querySelector('.tab-panel.active')?.id === 'panel-graph') {
      _graphSyncPending = false;
      triggerGraphSync(120);
    }
  }
}

window._graphNodeScale = 1.0;
function updateGraphSettings() {
  if (!_graphInstance) return;
  const rep = parseInt(document.getElementById('st-repulsion').value, 10);
  const ld = parseInt(document.getElementById('st-linkdist').value, 10);
  const ns = parseFloat(document.getElementById('st-nodesize').value);
  const gravRaw = parseInt(document.getElementById('st-gravity').value, 10) || 0;
  const grav = gravRaw * 0.001;

  document.getElementById('st-repulsion-val').textContent = rep;
  document.getElementById('st-linkdist-val').textContent = ld;
  document.getElementById('st-nodesize-val').textContent = ns.toFixed(1) + 'x';
  document.getElementById('st-gravity-val').textContent = grav.toFixed(3);

  window._graphNodeScale = ns;

  _graphInstance.d3Force('charge').strength(rep);
  _graphInstance.d3Force('link').distance(function(link) { return graphLinkDistanceValue(link); }).strength(function(link) { return graphLinkStrengthValue(link); });

  const layoutChanged =
    _graphLayoutSettings.repulsion !== rep ||
    _graphLayoutSettings.linkDistance !== ld ||
    _graphLayoutSettings.gravity !== gravRaw;

  _graphLayoutSettings.repulsion = rep;
  _graphLayoutSettings.linkDistance = ld;
  _graphLayoutSettings.gravity = gravRaw;

  if (layoutChanged) {
    _graphInstance.d3ReheatSimulation();
  } else if (typeof _graphInstance.refresh === 'function') {
    _graphInstance.refresh();
  }
}

function updateGraphVisibility() {
  graphUpdateAffinityThresholdLabel();
  graphClearHoverState();
  updateGraphFocusUI();
  if (!_graphInstance) {
    updateGraphStatsLine();
    return;
  }
  _graphInstance.graphData(graphBuildVisibleData());
  _graphInstance.hasLoadedData = true;
  updateGraphStatsLine();
}

async function showMemoryGraphNodeDetail(memoryId) {
  memoryId = String(memoryId || '').trim();
  if (!memoryId) return;
  openModal('Memory Node ' + esc(memoryId), '<div class="empty">Loading...</div>');
  try {
    const detail = await rpc('workspace.memory.graph.get', {
      workspace_id: WS_ID,
      memory_id: memoryId
    });
    const node = (detail && detail.node) || {};
    const drift = detail && detail.drift_report ? detail.drift_report : null;
    let html = '<div class="grid cols-2" style="margin-bottom:12px">';
    html += '<div><strong>Memory ID</strong><br><code style="font-size:10px">' + esc(node.memory_id || memoryId) + '</code></div>';
    if (node.memory_type) html += '<div><strong>Type</strong><br>' + esc(node.memory_type) + '</div>';
    if (node.memory_layer) html += '<div><strong>Layer</strong><br>' + esc(node.memory_layer) + '</div>';
    if (node.epistemic_status) html += '<div><strong>Epistemic</strong><br>' + esc(node.epistemic_status) + '</div>';
    if (node.lifecycle_state) html += '<div><strong>Lifecycle</strong><br>' + esc(node.lifecycle_state) + '</div>';
    if (node.canonical_authority) html += '<div><strong>Canonical Authority</strong><br>' + esc(node.canonical_authority) + '</div>';
    if (node.surface_authority || node.surface_role) html += '<div><strong>Surface Boundary</strong><br>' + esc([node.surface_authority, node.surface_role].filter(Boolean).join(' / ')) + '</div>';
    if (node.origin_kind || node.origin_id) html += '<div><strong>Origin</strong><br>' + esc([node.origin_kind, node.origin_id].filter(Boolean).join(' · ')) + '</div>';
    if (node.source_kind || node.source_id) html += '<div><strong>Source</strong><br>' + esc([node.source_kind, node.source_id].filter(Boolean).join(' · ')) + '</div>';
    if (node.semantic_lineage_id) html += '<div><strong>Semantic Lineage</strong><br><code style="font-size:10px">' + esc(node.semantic_lineage_id) + '</code></div>';
    if (Number.isFinite(Number(node.importance))) html += '<div><strong>Importance</strong><br>' + esc(graphFormatMetricValue(node.importance)) + '</div>';
    if (Number.isFinite(Number(node.confidence))) html += '<div><strong>Confidence</strong><br>' + esc(graphFormatMetricValue(node.confidence)) + '</div>';
    if (Number.isFinite(Number(node.activation))) html += '<div><strong>Activation</strong><br>' + esc(graphFormatMetricValue(node.activation)) + '</div>';
    if (Number.isFinite(Number(node.drift))) html += '<div><strong>Drift</strong><br>' + esc(graphFormatMetricValue(node.drift)) + '</div>';
    if (node.retention_band) html += '<div><strong>Retention</strong><br>' + esc(node.retention_band + (node.retention_prunable ? ' - prunable' : '')) + '</div>';
    if (node.protect) html += '<div><strong>Protected</strong><br>yes</div>';
    if (node.unresolved) html += '<div><strong>Unresolved</strong><br>yes</div>';
    if (node.recovery_candidate) html += '<div><strong>Recovery Candidate</strong><br>' + esc('yes' + (Number(node.recovery_trigger_count || 0) > 0 ? (' - triggers ' + String(node.recovery_trigger_count || 0)) : '')) + '</div>';
    if (node.recovery_guard_reason) html += '<div><strong>Recovery Guard</strong><br>' + esc(node.recovery_guard_reason) + '</div>';
    if (drift && Number.isFinite(Number(drift.effective_drift))) html += '<div><strong>Effective Drift</strong><br>' + esc(graphFormatMetricValue(drift.effective_drift)) + '</div>';
    html += '</div>';
    if (node.title) html += '<div style="margin-bottom:10px"><strong>Title</strong><br>' + esc(node.title) + '</div>';
    if (node.summary) html += '<div style="margin-bottom:10px"><strong>Summary</strong><br>' + esc(node.summary) + '</div>';
    if (node.body) html += '<div style="margin-bottom:10px"><strong>Body</strong><div class="empty" style="margin-top:6px">' + esc(node.body) + '</div></div>';
    const links = [];
    if (node.task_id) links.push('<div><strong>Task</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showTaskDetail((node.task_id),(node.task_id))}) + '>' + esc(node.task_id) + '</a></div>');
    if (node.session_id) links.push('<div><strong>Session</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showSessionDetail((node.session_id))}) + '>' + esc(node.session_id) + '</a></div>');
    if (node.agent_id) links.push('<div><strong>Agent</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showAgentDetail((node.agent_id))}) + '>' + esc(resolveAgentName(node.agent_id) || node.agent_id) + '</a></div>');
    if (node.origin_kind === 'workspace_memory' && node.origin_id) links.push('<div><strong>Backing Memory</strong><br><a href="#" ' + dashboardAction(function(dashboardEvent){dashboardEvent.preventDefault();closeModal();showMemoryDetail((node.origin_id))}) + '>' + esc(node.origin_id) + '</a></div>');
    if (links.length) html += '<div class="grid cols-2" style="margin-top:12px">' + links.join('') + '</div>';
    const outboundCount = ((detail && detail.outbound_edges) || []).length;
    const inboundCount = ((detail && detail.inbound_edges) || []).length;
    html += '<div style="margin-top:12px;font-size:11px;color:var(--muted)">Graph neighborhood: ' + esc(String(outboundCount)) + ' outbound · ' + esc(String(inboundCount)) + ' inbound edges</div>';
    openModal('Memory Node ' + esc(node.title || node.memory_id || memoryId), html);
  } catch (e) {
    openModal('Memory Node ' + esc(memoryId), '<div class="empty">' + esc((e && e.message) || 'Failed to load memory node') + '</div>');
  }
}

function updateGraphInspector() {
  const panel = document.getElementById('graph-inspector');
  const body = document.getElementById('graph-inspector-body');
  const actions = document.getElementById('graph-inspector-actions');
  if (!_graphFocusNode) {
    _graphInspectorDismissedNodeID = '';
    panel.style.display = 'none';
    if (actions) actions.innerHTML = '';
    return;
  }
  const focusNodeKey = graphNodeFocusKey(_graphFocusNode);
  if (focusNodeKey && _graphInspectorDismissedNodeID === focusNodeKey) {
    panel.style.display = 'none';
    return;
  }
  panel.style.display = 'flex';
  const n = _graphFocusNode;
  const refID = graphNodeRefID(n);
  const focusMode = _graphLoadedMode || graphSelectedMode();
  const focusID = String(_graphLoadedFocus || _graphRequestedFocusID || '').trim();
  const controlCluster = n.type === 'proto_cluster' ? graphControlClusterByID(refID || n.id) : null;
  const neighborCounts = n.type === 'proto_cluster' ? graphNodeNeighborTypeCounts(n.id) : null;
  body.innerHTML =
    '<div style="margin-bottom:8px"><strong>Node ID:</strong> <br><code style="font-size:10px;background:var(--card);padding:2px">' + esc(n.id) + '</code></div>' +
    ((refID && refID !== n.id) ? '<div style="margin-bottom:8px"><strong>Entity ID:</strong> <br><code style="font-size:10px;background:var(--card);padding:2px">' + esc(refID) + '</code></div>' : '') +
    ((n.type === 'proto_cluster' && refID) ? '<div style="margin-bottom:8px"><strong>Proto-Cluster ID:</strong> <br><code style="font-size:10px;background:var(--card);padding:2px">' + esc(refID) + '</code></div>' : '') +
    '<div style="margin-bottom:8px"><strong>Type:</strong> ' + esc(n.type) + '</div>' +
    '<div style="margin-bottom:8px"><strong>Label:</strong> ' + esc(n.label) + '</div>' +
    (n.status ? '<div style="margin-bottom:8px"><strong>Status:</strong> ' + esc(n.status) + '</div>' : '') +
    ((n.type === 'memory_node' && n.memory_type) ? '<div style="margin-bottom:8px"><strong>Memory Type:</strong> ' + esc(String(n.memory_type || '').trim()) + '</div>' : '') +
    ((n.type === 'memory_node' && n.memory_layer) ? '<div style="margin-bottom:8px"><strong>Memory Layer:</strong> ' + esc(String(n.memory_layer || '').trim()) + '</div>' : '') +
    ((n.type === 'memory_node' && n.epistemic_status) ? '<div style="margin-bottom:8px"><strong>Epistemic:</strong> ' + esc(String(n.epistemic_status || '').trim()) + '</div>' : '') +
    ((n.type === 'memory_node' && n.canonical_authority) ? '<div style="margin-bottom:8px"><strong>Canonical Authority:</strong> ' + esc(String(n.canonical_authority || '').trim()) + '</div>' : '') +
    ((n.type === 'memory_node' && (n.surface_authority || n.surface_role)) ? '<div style="margin-bottom:8px"><strong>Surface Boundary:</strong> ' + esc([String(n.surface_authority || '').trim(), String(n.surface_role || '').trim()].filter(Boolean).join(' / ')) + '</div>' : '') +
    ((n.type === 'memory_node' && (n.origin_kind || n.origin_id)) ? '<div style="margin-bottom:8px"><strong>Origin:</strong> ' + esc([String(n.origin_kind || '').trim(), String(n.origin_id || '').trim()].filter(Boolean).join(' · ')) + '</div>' : '') +
    ((n.type === 'memory_node' && (n.source_kind || n.source_id)) ? '<div style="margin-bottom:8px"><strong>Source:</strong> ' + esc([String(n.source_kind || '').trim(), String(n.source_id || '').trim()].filter(Boolean).join(' · ')) + '</div>' : '') +
    ((n.type === 'memory_node' && Number.isFinite(Number(n.importance))) ? '<div style="margin-bottom:8px"><strong>Importance:</strong> ' + esc(graphFormatMetricValue(n.importance)) + '</div>' : '') +
    ((n.type === 'memory_node' && Number.isFinite(Number(n.activation))) ? '<div style="margin-bottom:8px"><strong>Activation:</strong> ' + esc(graphFormatMetricValue(n.activation)) + '</div>' : '') +
    ((n.type === 'memory_node' && Number.isFinite(Number(n.drift))) ? '<div style="margin-bottom:8px"><strong>Drift:</strong> ' + esc(graphFormatMetricValue(n.drift)) + '</div>' : '') +
    ((n.type === 'memory_node' && n.semantic_lineage_id) ? '<div style="margin-bottom:8px"><strong>Semantic Lineage:</strong> <br><code style="font-size:10px;background:var(--card);padding:2px">' + esc(String(n.semantic_lineage_id || '').trim()) + '</code></div>' : '') +
    ((n.type === 'memory_node' && Number.isFinite(Number(n.confidence))) ? '<div style="margin-bottom:8px"><strong>Confidence:</strong> ' + esc(graphFormatMetricValue(n.confidence)) + '</div>' : '') +
    ((n.type === 'memory_node' && n.retention_band) ? '<div style="margin-bottom:8px"><strong>Retention:</strong> ' + esc(String(n.retention_band || '').trim()) + (n.retention_prunable ? ' / prunable' : '') + '</div>' : '') +
    ((n.type === 'memory_node' && n.protect) ? '<div style="margin-bottom:8px"><strong>Protected:</strong> yes</div>' : '') +
    ((n.type === 'memory_node' && n.unresolved) ? '<div style="margin-bottom:8px"><strong>Unresolved:</strong> yes</div>' : '') +
    ((n.type === 'memory_node' && n.recovery_candidate) ? '<div style="margin-bottom:8px"><strong>Recovery Candidate:</strong> yes' + (Number(n.recovery_trigger_count || 0) > 0 ? (' / triggers ' + esc(String(n.recovery_trigger_count || 0))) : '') + '</div>' : '') +
    ((n.type === 'memory_node' && n.recovery_guard_reason) ? '<div style="margin-bottom:8px"><strong>Recovery Guard:</strong> ' + esc(String(n.recovery_guard_reason || '').trim()) + '</div>' : '') +
    ((n.type === 'memory_node' && n.summary) ? '<div style="margin-bottom:8px"><strong>Summary:</strong> ' + esc(String(n.summary || '').trim()) + '</div>' : '') +
    ((n.type === 'proto_cluster' && controlCluster) ? '<div style="margin-bottom:8px"><strong>Cluster Pressure:</strong> ' + esc(String(controlCluster.pressure_score || 0)) + '</div>' : '') +
    ((n.type === 'proto_cluster' && controlCluster && controlCluster.current_mode) ? '<div style="margin-bottom:8px"><strong>Current Mode:</strong> ' + esc(String(controlCluster.current_mode || '').trim()) + '</div>' : '') +
    ((n.type === 'proto_cluster' && controlCluster && controlCluster.summary) ? '<div style="margin-bottom:8px"><strong>Summary:</strong> ' + esc(String(controlCluster.summary || '').trim()) + '</div>' : '') +
    ((n.type === 'proto_cluster' && neighborCounts) ? '<div style="margin-bottom:8px"><strong>Neighborhood:</strong> ' + esc('tasks ' + neighborCounts.task + ' · sessions ' + neighborCounts.session + ' · tensions ' + neighborCounts.tension + ' · agents ' + neighborCounts.agent) + '</div>' : '') +
    ((focusMode === 'TASK_FOCUS' && focusID) ? '<div style="margin-bottom:8px"><strong>Focused Task:</strong> ' + esc(graphFocusLabel(focusID)) + '</div>' : '') +
    ((focusMode === 'CONTROL' && focusID) ? '<div style="margin-bottom:8px"><strong>Focused Cluster:</strong> ' + esc(graphControlFocusLabel(focusID)) + '</div>' : '') +
    ((focusMode === 'MEMORY_ATLAS' && focusID) ? '<div style="margin-bottom:8px"><strong>Atlas Focus:</strong> ' + esc(graphMemoryAtlasFocusLabel(focusID)) + '</div>' : '') +
    '<div style="margin-top:14px;font-size:10px;color:var(--muted)">Use Graph Controls to expand scope</div>';
  if (!actions) return;
  let actionHTML = '';
  const taskDetailItems = [];
  if (n.type === 'task' && refID) {
    actionHTML += '<button class="hdr-btn graph-open-task" style="width:100%">Open Task</button>';
    taskDetailItems.push({task_id:refID,title:n.label||refID});
    if (focusMode === 'TASK_FOCUS' && focusID === refID) {
      actionHTML += '<button class="hdr-btn" style="width:100%" onclick="graphReturnToSystem()">Back to System</button>';
    } else {
      actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){graphEnterTaskFocus((refID))}) + '>Focus Task Graph</button>';
    }
  }
  if (n.type === 'session' && refID) {
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){switchTab('overview');setTimeout(()=>showSessionDetail((refID)),100)}) + '>Open Session</button>';
  }
  if (n.type === 'agent' && refID) {
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){showAgentDetail((refID))}) + '>Open Agent</button>';
  }
  if (n.type === 'action' && refID) {
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){showActionDetail((refID))}) + '>Open Action</button>';
  }
  if (n.type === 'tension' && refID) {
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){showTensionDetail((refID))}) + '>Open Tension</button>';
  }
  if (n.type === 'proto_cluster' && refID) {
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){openControlScaffold((refID))}) + '>Open Control Scaffold</button>';
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){openTensionsForProtoCluster((refID))}) + '>Open Tensions</button>';
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){showProtoClusterDetail((refID))}) + '>Open Proto-Cluster</button>';
    if (focusMode === 'CONTROL' && focusID === refID) {
      actionHTML += '<button class="hdr-btn" style="width:100%" onclick="graphShowControlOverview()">Show All Clusters</button>';
    } else {
      actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){graphEnterControlFocus((refID))}) + '>Focus Control Graph</button>';
    }
  }
  if (n.type === 'memory_node' && refID) {
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){showMemoryGraphNodeDetail((refID))}) + '>Open Memory Node</button>';
    if (String(n.origin_kind || '').trim() === 'workspace_memory' && String(n.origin_id || '').trim()) {
      actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){showMemoryDetail((String(n.origin_id || '').trim()))}) + '>Open Backing Memory</button>';
    }
    if (String(n.task_id || '').trim()) {
      actionHTML += '<button class="hdr-btn graph-open-task" style="width:100%">Open Linked Task</button>';
      taskDetailItems.push({task_id:String(n.task_id || '').trim(),title:String(n.task_id || '').trim()});
    }
    if (focusMode === 'MEMORY_ATLAS' && focusID === refID) {
      actionHTML += '<button class="hdr-btn" style="width:100%" onclick="graphShowMemoryAtlasOverview()">Show Atlas Overview</button>';
    } else {
      actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){graphEnterMemoryAtlasFocus((refID))}) + '>Center Here</button>';
    }
    actionHTML += '<button class="hdr-btn" style="width:100%" ' + dashboardAction(function(dashboardEvent){(function(){var depth=document.getElementById('graph-memory-atlas-depth');if(depth)depth.value='2';handleGraphMemoryAtlasFilterChange();graphEnterMemoryAtlasFocus((refID));})()}) + '>Expand Neighborhood</button>';
    if (focusMode === 'MEMORY_ATLAS' && _graphAtlasHistory.length) {
      actionHTML += '<button class="hdr-btn" style="width:100%" onclick="graphGoBackMemoryAtlasFocus()">Back to Previous Focus</button>';
    }
  }
  if (n.type === 'dag_node' && focusMode === 'TASK_FOCUS' && focusID) {
    actionHTML += '<button class="hdr-btn graph-open-task" style="width:100%">Open Parent Task</button>';
    taskDetailItems.push({task_id:focusID,title:graphFocusLabel(focusID)});
  }
  if (!actionHTML) {
    actionHTML = '<div style="font-size:11px;color:var(--muted)">No direct actions yet for this node.</div>';
  }
  actions.innerHTML = actionHTML;
  bindTaskDetailElements(actions, taskDetailItems, '.graph-open-task');
}

syncWorkspaceInputs();
syncAuthChrome();
fillAuthWorkspaceDefaults();
syncGraphControlsUI();
if (TOKEN) {
  startApp();
} else {
  openAuthShell('human-login');
}
</script>
<div id="legal-source" style="position:fixed;right:12px;bottom:10px;z-index:9999;max-width:min(520px,calc(100vw - 24px));padding:7px 10px;border:1px solid var(--border);border-radius:7px;background:var(--surface);color:var(--muted);font-size:10px;box-shadow:0 4px 20px rgba(0,0,0,.24)">
  <strong style="color:var(--text)">Legal &amp; source</strong> · Copyright © Rhizome Project contributors · No warranty · <a href="https://github.com/Rhizome-Project/rhizome-runtime/blob/main/LICENSE" target="_blank" rel="noopener noreferrer">AGPL-3.0-only</a> · <a href="{{RHIZOME_SOURCE_URL}}" target="_blank" rel="noopener noreferrer">Corresponding source for this build</a>
</div>
</body>
</html>` + ""
