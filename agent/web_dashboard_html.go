package main

import (
	"html"
	"net/url"
	"os"
	"strings"
)

const defaultManagerWebSourceURL = "https://github.com/Rhizome-Project/rhizome-runtime"

func managerWebDashboardHTML() string {
	return strings.Join([]string{
		"<!doctype html>",
		"<html lang=\"en\"><head>",
		"<meta charset=\"utf-8\">",
		"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
		"<title>rhizome-bot web</title>",
		"<link rel=\"icon\" href=\"data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22 fill=%22%234aa8ff%22>[]</text></svg>\">",
		"<style>", managerWebDashboardStyles(), "</style>",
		"</head><body>",
		"<div class=\"shell\">",

		"<aside class=\"sidebar\" id=\"sidebar\">",
		"<div class=\"brand\"><div><h1 onclick=\"goHome()\">rhizome-bot</h1><div class=\"sub\" id=\"sidebar-subtitle\">local manager dashboard</div></div></div>",
		"<div class=\"sidebar-meta\" id=\"defaults-summary\"></div>",
		"<div class=\"sidebar-section-title\">Managed Agents</div>",
		"<div class=\"agent-list\" id=\"agent-list\"></div>",
		"</aside>",

		"<main class=\"main\">",
		"<div class=\"topbar\" id=\"topbar\">",
		"<nav class=\"nav-tabs\" id=\"nav-tabs\">",
		"<button class=\"nav-tab active\" data-tab=\"overview\" onclick=\"switchTab('overview')\">Overview</button>",
		"<button class=\"nav-tab\" data-tab=\"providers\" onclick=\"switchTab('providers')\">Providers</button>",
		"<button class=\"nav-tab\" data-tab=\"settings\" onclick=\"switchTab('settings')\">Settings</button>",
		"</nav>",
		"</div>",

		"<div class=\"tab-panel active\" id=\"panel-overview\">",
		"<div class=\"overview-header\"><h2>All Agents</h2><div class=\"overview-actions\"><button class=\"btn-add\" onclick=\"showOnboardModal()\">+ New Agent</button><span class=\"sub\" id=\"summary-line\">loading...</span></div></div>",
		"<div class=\"overview-stats\" id=\"overview-stats\"></div>",
		"<div class=\"overview-grid\" id=\"overview-grid\"></div>",
		"</div>",

		"<div class=\"tab-panel\" id=\"panel-settings\">",
		"<section class=\"card\"><h2>Global Defaults</h2><form id=\"defaults-form\" class=\"field-grid\"></form></section>",
		"</div>",

		"<div class=\"tab-panel\" id=\"panel-providers\">",
		"<div id=\"providers-panel\"></div>",
		"</div>",

		"<div class=\"agent-page\" id=\"agent-page\">",
		"<div class=\"agent-page-topbar\">",
		"<button class=\"btn-back\" onclick=\"closeAgentPage()\"><- Overview</button>",
		"<div class=\"agent-header-info\"><div class=\"agent-detail-name\" id=\"agent-page-name\">Agent Details</div><div class=\"small\" id=\"agent-page-id\"></div></div>",
		"</div>",

		"<div class=\"agent-sub-tabs\">",
		"<button class=\"sub-tab active\" data-subtab=\"info\" onclick=\"switchAgentSubTab('info')\">Info</button>",
		"<button class=\"sub-tab\" data-subtab=\"controls\" onclick=\"switchAgentSubTab('controls')\">Controls</button>",
		"<button class=\"sub-tab\" data-subtab=\"inbox\" onclick=\"switchAgentSubTab('inbox')\">Inbox</button>",
		"<button class=\"sub-tab\" data-subtab=\"activity\" onclick=\"switchAgentSubTab('activity')\">Activity</button>",
		"<button class=\"sub-tab\" data-subtab=\"runtime\" onclick=\"switchAgentSubTab('runtime')\">Runtime</button>",
		"<button class=\"sub-tab\" data-subtab=\"logs\" onclick=\"switchAgentSubTab('logs')\">Logs</button>",
		"<button class=\"sub-tab\" data-subtab=\"settings\" onclick=\"switchAgentSubTab('settings')\">Settings</button>",
		"</div>",

		"<div class=\"agent-sub-panel active\" id=\"agent-panel-info\"></div>",

		"<div class=\"agent-sub-panel\" id=\"agent-panel-controls\">",
		"<div class=\"controls-layout\">",
		"<div class=\"controls-sidebar\">",
		"<div style=\"padding:1rem; border-bottom:1px solid var(--border)\"><button class=\"btn-primary btn-compact\" id=\"local-chat-new-button\" style=\"width:100%\" onclick=\"createNewLocalChat()\">+ New Inspect Chat</button></div>",
		"<div class=\"chat-list\" id=\"local-chats-list\"></div>",
		"</div>",
		"<div class=\"controls-main\">",
		"<div class=\"controls-tabs-header\">",
		"<button class=\"ctl-tab active\" data-ctltab=\"chat\" onclick=\"switchControlsTab('chat')\">Inspect Chat</button>",
		"<button class=\"ctl-tab\" data-ctltab=\"legacy\" onclick=\"switchControlsTab('legacy')\">Task & Tension</button>",
		"</div>",
		"<div class=\"ctl-panel active\" id=\"ctl-panel-chat\">",
		"<div id=\"local-chat-contract-banner\" style=\"padding:0.85rem 1rem; border-bottom:1px solid var(--border); background:rgba(255,255,255,0.03)\"></div>",
		"<div class=\"chat-messages\" id=\"local-chat-messages\"></div>",
		"<div class=\"chat-input-area\">",
		"<form id=\"local-chat-form\" style=\"display:flex;flex-direction:column;gap:0.5rem;\">",
		"<div style=\"display:flex;gap:0.5rem;\">",
		"<textarea id=\"local-chat-input\" placeholder=\"Send an inspect message...\" rows=\"1\"></textarea>",
		"<button class=\"btn-primary btn-compact\" id=\"local-chat-send-button\" type=\"submit\" style=\"align-self:flex-end; padding:0.6rem 1rem\">Send</button>",
		"</div>",
		"</form>",
		"</div>",
		"</div>",
		"<div class=\"ctl-panel\" id=\"ctl-panel-legacy\">",
		"<div id=\"control-panel\" style=\"padding:1.5rem; overflow-y:auto; flex:1\"></div>",
		"</div>",
		"</div>",
		"</div>",
		"</div>",

		"<div class=\"agent-sub-panel\" id=\"agent-panel-inbox\">",
		"<div class=\"inbox-container\" id=\"inbox-panel\"></div>",
		"</div>",

		"<div class=\"agent-sub-panel\" id=\"agent-panel-activity\">",
		"<div id=\"activity-panel\"></div>",
		"</div>",

		"<div class=\"agent-sub-panel\" id=\"agent-panel-runtime\">",
		"<div class=\"dual-grid\">",
		"<section class=\"card\"><h2>Runtime Snapshot</h2><div id=\"runtime-panel\"></div></section>",
		"<section class=\"card\"><h2>Workspace Catalog</h2><div id=\"catalog-panel\"></div></section>",
		"</div>",
		"</div>",

		"<div class=\"agent-sub-panel\" id=\"agent-panel-logs\">",
		"<section class=\"card\"><div class=\"card-head\"><h2>Agent Logs</h2></div><div id=\"logs-panel\"></div></section>",
		"</div>",

		"<div class=\"agent-sub-panel\" id=\"agent-panel-settings\">",
		"<div id=\"settings-panel\"></div>",
		"</div>",
		"</div>",

		"<div class=\"modal-overlay\" id=\"onboard-modal\" onclick=\"if(event.target===this)closeOnboardModal()\">",
		"<div class=\"modal\"><div class=\"modal-head\"><h2>Onboard New Agent</h2><button class=\"modal-close\" onclick=\"closeOnboardModal()\">x</button></div>",
		"<div class=\"modal-body\"><form id=\"onboard-form\" class=\"field-grid\"></form></div></div>",
		"</div>",

		"<div class=\"modal-overlay\" id=\"provider-modal\" onclick=\"if(event.target===this)closeProviderModal()\">",
		"<div class=\"modal\"><div class=\"modal-head\"><h2 id=\"provider-modal-title\">Add Provider</h2><button class=\"modal-close\" onclick=\"closeProviderModal()\">x</button></div>",
		"<div class=\"modal-body\"><form id=\"provider-form\" class=\"field-grid\"></form></div></div>",
		"</div>",

		"<div class=\"modal-overlay\" id=\"fs-modal-overlay\">",
		"<div class=\"modal\" style=\"max-width:600px; display:flex; flex-direction:column; padding:0; height:70vh; max-height:600px;\">",
		"<div class=\"modal-head\" style=\"padding:1.5rem; border-bottom:1px solid var(--border);\"><h2>Select Folder</h2><button class=\"modal-close\" id=\"fs-close-top\">x</button></div>",
		"<div style=\"display:flex; padding:0.75rem 1.5rem; gap:0.5rem; background:rgba(0,0,0,0.2); border-bottom:1px solid rgba(255,255,255,0.05);\">",
		"<button class=\"btn-ghost btn-compact\" id=\"fs-btn-up\">Up</button>",
		"<input type=\"text\" id=\"fs-path-input\" spellcheck=\"false\" placeholder=\"Enter path manually...\" style=\"flex:1; background:rgba(0,0,0,0.3); border:1px solid var(--border); color:var(--text); padding:0.4rem 0.6rem; border-radius:6px; font-family:var(--mono); font-size:0.8rem;\">",
		"<button class=\"btn-ghost btn-compact\" id=\"fs-btn-go\">Go</button>",
		"</div>",
		"<div id=\"fs-list\" style=\"flex:1; overflow-y:auto; padding:0.5rem;\"></div>",
		"<div style=\"padding:1rem 1.5rem; border-top:1px solid var(--border); display:flex; justify-content:flex-end; gap:0.75rem; background:rgba(255,255,255,0.02);\">",
		"<button type=\"button\" class=\"btn-ghost\" id=\"fs-close-bottom\">Cancel</button>",
		"<button type=\"button\" class=\"btn-primary\" id=\"fs-btn-select\">Select Folder</button>",
		"</div>",
		"</div>",
		"</div>",

		"</main></div>",
		"<div id=\"legal-source\" style=\"position:fixed;right:12px;bottom:10px;z-index:9999;max-width:min(520px,calc(100vw - 24px));padding:7px 10px;border:1px solid var(--border);border-radius:7px;background:var(--panel);color:var(--muted);font-size:10px;box-shadow:0 4px 20px rgba(0,0,0,.24)\">",
		"<strong style=\"color:var(--text)\">Legal &amp; source</strong> &middot; Copyright &copy; Rhizome Project contributors &middot; No warranty &middot; <a href=\"https://github.com/Rhizome-Project/rhizome-runtime/blob/main/LICENSE\" target=\"_blank\" rel=\"noopener noreferrer\">AGPL-3.0-only</a> &middot; <a href=\"", html.EscapeString(managerWebSourceURL()), "\" target=\"_blank\" rel=\"noopener noreferrer\">Corresponding source for this build</a>",
		"</div>",
		"<div id=\"toast\" class=\"toast\"></div>",
		"<script>",
		managerWebDashboardScriptCore(),
		managerWebDashboardScriptRenderers(),
		"</script>",
		"</body></html>",
	}, "")
}

func managerWebSourceURL() string {
	raw := strings.TrimSpace(os.Getenv("RHIZOME_SOURCE_URL"))
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return defaultManagerWebSourceURL
	}
	return parsed.String()
}
