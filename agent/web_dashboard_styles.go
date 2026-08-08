package main

func managerWebDashboardStyles() string {
	return `
:root{--bg:#0b0d10;--panel:#131821;--panel2:#181f2b;--text:#eef3ff;--muted:#8a9bb8;--accent:#4aa8ff;--accent2:#57c38c;--border:#222d3d;--danger:#ff6a6a;--warn:#f2b84b;--mono:"JetBrains Mono","SFMono-Regular",Consolas,monospace;--sans:"Inter","Segoe UI",sans-serif;--radius:12px;--card-bg:rgba(19,24,33,.88)}
*{box-sizing:border-box;margin:0;padding:0}html,body{min-height:100%;background:var(--bg);color:var(--text);font-family:var(--sans);font-size:14px;line-height:1.5}
button,input,textarea,select{font:inherit;color:inherit;background:var(--panel2);border:1px solid var(--border);border-radius:8px;outline:none}
input,textarea,select{width:100%;padding:.6rem .75rem;transition:border-color .15s}input:focus,textarea:focus,select:focus{border-color:var(--accent)}
textarea{min-height:100px;resize:vertical}
pre{margin:0;padding:.9rem 1rem;border-radius:10px;background:#0d121a;border:1px solid var(--border);overflow:auto;white-space:pre-wrap;word-break:break-word;font-family:var(--mono);font-size:12px;line-height:1.6}
code{font-family:var(--mono)}a{color:var(--accent);text-decoration:none}

/* ── Buttons ── */
button{cursor:pointer;padding:.5rem .85rem;transition:all .15s;font-weight:500;font-size:.82rem}
button:hover{border-color:var(--accent);transform:translateY(-1px)}
button:active{transform:translateY(0)}
.btn-primary{background:linear-gradient(135deg,#2c8f66,#2466c7);border-color:transparent;color:#fff}
.btn-primary:hover{box-shadow:0 4px 14px rgba(36,102,199,.3)}
.btn-warn{background:rgba(79,59,12,.6);color:var(--warn);border-color:rgba(242,184,75,.25)}
.btn-danger{background:rgba(79,32,32,.6);color:var(--danger);border-color:rgba(255,106,106,.2)}
.btn-ghost{background:transparent;border-color:var(--border);color:var(--muted)}
.btn-ghost:hover{color:var(--text)}
.btn-add{background:linear-gradient(135deg,var(--accent),var(--accent2));border:none;color:#fff;padding:.55rem 1.1rem;border-radius:10px;font-weight:600;font-size:.85rem;letter-spacing:.02em}
.btn-add:hover{box-shadow:0 4px 16px rgba(74,168,255,.25);transform:translateY(-1px)}
.btn-back{background:transparent;border:1px solid var(--border);color:var(--muted);padding:.5rem 1rem;border-radius:8px;font-size:.82rem}
.btn-back:hover{color:var(--text);border-color:var(--accent)}

/* ── Shell layout ── */
.shell{display:grid;grid-template-columns:280px 1fr;min-height:100vh}
.main{padding:0;overflow:auto;position:relative}

/* ── Sidebar ── */
.sidebar{background:linear-gradient(180deg,rgba(13,16,22,.97) 0%,rgba(10,13,16,.99) 100%);border-right:1px solid var(--border);padding:1.2rem;position:sticky;top:0;height:100vh;overflow-y:auto;display:flex;flex-direction:column;gap:.5rem}
.brand{margin-bottom:.5rem}
.brand h1{font-size:1.05rem;font-weight:700;cursor:pointer;background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;transition:opacity .2s}
.brand h1:hover{opacity:.8}
.sub{color:var(--muted);font-size:.78rem}
.sidebar-meta{font-size:.72rem;color:var(--muted);padding:.4rem .6rem;background:rgba(255,255,255,.03);border-radius:8px;line-height:1.5}
.sidebar-section-title{font-size:.68rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-top:.6rem;padding:0 .2rem}

/* Agent list in sidebar */
.agent-list{display:flex;flex-direction:column;gap:6px}
.agent-item{padding:.7rem .8rem;border:1px solid var(--border);border-radius:10px;background:rgba(19,24,33,.7);cursor:pointer;transition:all .15s;position:relative}
.agent-item:hover{border-color:rgba(74,168,255,.4);background:rgba(25,32,45,.9)}
.agent-item.selected{background:linear-gradient(135deg,rgba(30,55,85,.95),rgba(20,42,65,.95));border-color:rgba(74,168,255,.5)}
.agent-item-name{font-weight:600;font-size:.85rem;display:flex;justify-content:space-between;align-items:center;gap:.5rem}
.agent-item-meta{font-size:.72rem;color:var(--muted);margin-top:.2rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}

/* Badge */
.badge{display:inline-flex;align-items:center;padding:.15rem .5rem;border-radius:999px;font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em}
.badge.running{color:#a7f0c5;background:rgba(31,78,57,.4);border:1px solid rgba(45,128,92,.4)}
.badge.stopped{color:#ffcf8d;background:rgba(98,71,26,.35);border:1px solid rgba(140,101,48,.4)}
.badge.error{color:#ffb5b5;background:rgba(111,34,34,.35);border:1px solid rgba(133,67,67,.4)}
.badge.unknown{color:var(--muted);background:rgba(138,155,184,.1);border:1px solid var(--border)}
.badge.forming{color:#a7c5f0;background:rgba(31,61,78,.4);border:1px solid rgba(45,98,128,.4)}
.badge.active{color:#a7f0c5;background:rgba(31,78,57,.4);border:1px solid rgba(45,128,92,.4)}
.badge.dormant{color:var(--muted);background:rgba(138,155,184,.1);border:1px solid var(--border)}
.badge.dissolved{color:#ffb5b5;background:rgba(111,34,34,.35);border:1px solid rgba(133,67,67,.4)}

/* ── Topbar / Nav ── */
.topbar{padding:1rem 1.5rem .5rem;display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap}
.nav-tabs{display:flex;gap:3px;background:rgba(19,24,33,.6);padding:4px;border-radius:10px}
.nav-tab{background:transparent;border:1px solid transparent;color:var(--muted);padding:.45rem .9rem;border-radius:7px;font-size:.78rem;font-weight:600;letter-spacing:.02em;transition:all .18s}
.nav-tab:hover{color:var(--text);background:rgba(255,255,255,.04);transform:none}
.nav-tab.active{background:rgba(74,168,255,.12);color:var(--accent);border-color:rgba(74,168,255,.25)}
.message{font-size:.78rem;color:var(--muted);text-align:right;min-height:1rem}.message.error{color:#ffb5b5}

/* ── Tab panels ── */
.tab-panel{display:none;padding:1rem 1.5rem 2rem}
.tab-panel.active{display:block;animation:fadeUp .2s ease}
@keyframes fadeUp{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}

/* ── Cards ── */
.card{background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius);padding:1.2rem;margin-bottom:1rem}
.card h2{font-size:.78rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:1rem}
.card-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem}
.card-head h2{margin-bottom:0}

/* ── Grids ── */
.dual-grid{display:grid;grid-template-columns:1.5fr 1fr;gap:1rem}
.field-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:.75rem}
.field{display:flex;flex-direction:column;gap:.3rem}
.field label{font-size:.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;font-weight:600}
.field.full{grid-column:1/-1}
.split{display:grid;grid-template-columns:1fr 1fr;gap:1rem}
.logs{display:grid;grid-template-columns:1fr 1fr;gap:1rem}

/* ── KVS ── */
.kvs{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:8px}
.kv{background:rgba(13,18,26,.7);border:1px solid var(--border);border-radius:9px;padding:.6rem .75rem}
.kv .label{font-size:.65rem;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);margin-bottom:.25rem;font-weight:600}
.kv .value{font-family:var(--mono);font-size:.8rem;word-break:break-word;line-height:1.4}

/* ── Overview ── */
.overview-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:1.2rem}
.overview-header h2{font-size:1.1rem;font-weight:700;color:var(--text)}
.overview-actions{display:flex;align-items:center;gap:1rem}
.overview-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:1.4rem}
.stat-card{background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius);padding:.9rem 1rem;text-align:center;transition:border-color .2s}
.stat-card:hover{border-color:rgba(74,168,255,.2)}
.stat-value{font-size:1.6rem;font-weight:700;background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.stat-label{font-size:.65rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-top:.25rem;font-weight:600}

.overview-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px}
.agent-card{background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius);padding:1rem 1.1rem;cursor:pointer;transition:all .18s;position:relative;overflow:hidden}
.agent-card:hover{border-color:rgba(74,168,255,.4);transform:translateY(-2px);box-shadow:0 6px 20px rgba(0,0,0,.2)}
.agent-card::before{content:"";position:absolute;top:0;left:0;right:0;height:2px}
.agent-card.st-running::before{background:linear-gradient(90deg,var(--accent2),var(--accent))}
.agent-card.st-stopped::before{background:var(--warn)}
.agent-card.st-error::before{background:var(--danger)}
.agent-card-top{display:flex;justify-content:space-between;align-items:flex-start;gap:.5rem;margin-bottom:.65rem}
.agent-card-name{font-size:.95rem;font-weight:700}
.agent-card-id{font-size:.72rem;color:var(--muted);font-family:var(--mono);margin-top:.15rem}
.agent-card-grid{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-top:.5rem}
.agent-card-field{display:flex;flex-direction:column}
.agent-card-field .af-label{font-size:.62rem;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);font-weight:600}
.agent-card-field .af-value{font-family:var(--mono);font-size:.78rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.agent-card-actions{display:flex;gap:6px;margin-top:.75rem}
.agent-card-actions button{padding:.35rem .65rem;font-size:.72rem;border-radius:7px}

.provider-header h2{font-size:1.1rem;font-weight:700;color:var(--text)}
.providers-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px}
.provider-card{background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius);padding:1rem 1.1rem;transition:all .18s;position:relative;overflow:hidden;cursor:pointer}
.provider-card:hover{border-color:rgba(74,168,255,.4);transform:translateY(-2px);box-shadow:0 6px 20px rgba(0,0,0,.2)}
.provider-card::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--accent),var(--accent2))}
.provider-card-top{display:flex;justify-content:space-between;align-items:flex-start;gap:.5rem;margin-bottom:.65rem}
.provider-card-name{font-size:.95rem;font-weight:700}
.provider-card-id{font-size:.72rem;color:var(--muted);font-family:var(--mono);margin-top:.15rem}
.provider-card-grid{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-top:.5rem}
.provider-card-field{display:flex;flex-direction:column}
.provider-card-field .af-label{font-size:.62rem;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);font-weight:600}
.provider-card-field .af-value{font-family:var(--mono);font-size:.78rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.provider-card-actions{display:flex;gap:6px;margin-top:.75rem}
.provider-card-actions button{padding:.35rem .65rem;font-size:.72rem;border-radius:7px}

/* ── Agent Detail Page ── */
.agent-page{display:none;padding:1rem 1.5rem 2rem;animation:fadeUp .2s ease}
.agent-page.open{display:block}
.agent-page-topbar{display:flex;align-items:center;gap:1rem;margin-bottom:1.2rem}
.agent-header-info{display:flex;align-items:baseline;gap:.75rem}
.agent-detail-name{font-size:1.2rem;font-weight:700}
.agent-detail-actions{display:flex;gap:8px;flex-wrap:wrap}
.agent-detail-actions button{padding:.5rem 1rem;font-size:.82rem;border-radius:8px}
.section-gap{margin-top:1.2rem}
.section-gap h3{font-size:.85rem;font-weight:600;color:var(--text);margin-bottom:.6rem}
.danger-zone{margin-top:1.4rem;padding-top:1rem;border-top:1px solid var(--border)}

/* ── Sub-tabs ── */
.agent-sub-tabs{display:flex;gap:8px;border-bottom:1px solid var(--border);margin-bottom:1.5rem;overflow-x:auto}
.sub-tab{background:transparent;border:none;border-bottom:2px solid transparent;border-radius:0;color:var(--muted);padding:.6rem 1rem;font-size:.82rem;font-weight:600;white-space:nowrap;transition:all .2s;cursor:pointer}
.sub-tab:hover{color:var(--text)}
.sub-tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.agent-sub-panel{display:none;animation:fadeUp .2s ease}
.agent-sub-panel.active{display:block}

.inbox-container{display:grid;grid-template-columns:280px 1fr;gap:0;height:65vh;min-height:500px;background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.inbox-sidebar{border-right:1px solid var(--border);display:flex;flex-direction:column;background:rgba(13,18,26,.4);min-height:0}
.inbox-search{padding:1rem;border-bottom:1px solid var(--border)}
.inbox-search input{background:rgba(255,255,255,.05);border:1px solid var(--border);border-radius:8px;padding:.55rem .75rem;font-size:.8rem}

/* ── Local Chat Delete Button ── */
.chat-item-actions{display:flex;align-items:center;gap:.4rem;align-self:center;opacity:0;transform:translateX(4px);transition:opacity .15s ease,transform .15s ease;margin-left:auto}
.chat-item:hover .chat-item-actions,.chat-item.active .chat-item-actions{opacity:1;transform:translateX(0)}
.chat-item-action{appearance:none;border:1px solid var(--border);background:rgba(255,255,255,.03);color:var(--muted);border-radius:999px;padding:.38rem .78rem;cursor:pointer;font-size:.72rem;font-weight:700;line-height:1;transition:all .15s ease}
.chat-item-action:hover{color:var(--text);border-color:rgba(255,255,255,.16);background:rgba(255,255,255,.06)}
.chat-item-action-archive{color:#f2c37a;border-color:rgba(242,184,75,.24);background:rgba(79,59,12,.24)}
.chat-item-action-archive:hover{color:#ffe1ad;border-color:rgba(242,184,75,.38);background:rgba(96,71,18,.34)}
.chat-item-action-delete{color:#ff9a9a;border-color:rgba(255,106,106,.18);background:rgba(79,32,32,.22)}
.chat-item-action-delete:hover{color:#ffd0d0;border-color:rgba(255,106,106,.34);background:rgba(108,39,39,.32)}

/* ── Controls Layout ── */
.controls-layout{display:flex;height:100%;min-height:0;background:var(--card-bg)}
.controls-sidebar{width:260px;border-right:1px solid var(--border);display:flex;flex-direction:column;background:rgba(10,13,16,.4);flex-shrink:0}
.controls-main{flex:1;display:flex;flex-direction:column;min-width:0;min-height:0}
.controls-tabs-header{display:flex;border-bottom:1px solid var(--border);background:var(--panel2)}
.ctl-tab{padding:.8rem 1.25rem;border:none;background:transparent;color:var(--muted);font-weight:600;font-size:.85rem;cursor:pointer;position:relative;transition:all .2s ease;user-select:none}
.ctl-tab:hover{color:var(--text)}
.ctl-tab.active{color:var(--text)}
.ctl-tab.active::after{content:"";position:absolute;bottom:0;left:0;right:0;height:2px;background:var(--accent)}
.ctl-panel{display:none;flex:1;min-height:0;flex-direction:column}
.ctl-panel.active{display:flex}
.chat-input-area{padding:1rem 1.5rem;border-top:1px solid var(--border);background:rgba(19,24,33,.8)}
#local-chat-input{min-height:44px;max-height:200px;font-size:0.9rem;border-radius:12px;padding:0.75rem 1rem;background:var(--panel2);border:1px solid var(--border);color:var(--text);font-family:inherit}
#local-chat-input:disabled{opacity:0.5;cursor:not-allowed}

/* ── Inbox / Chat Items ── */
.chat-list{flex:1;overflow-y:auto;padding:.5rem;display:flex;flex-direction:column;gap:.25rem}
.chat-item{display:flex;gap:12px;padding:12px;border-radius:12px;cursor:pointer;transition:all .15s ease;align-items:flex-start;border:1px solid transparent}
.chat-item:hover{background:rgba(255,255,255,.04);border-color:rgba(255,255,255,.06)}
.chat-item.active{background:rgba(74,168,255,.12);border:1px solid rgba(74,168,255,.25)}
.chat-item-avatar{width:36px;height:36px;border-radius:50%;background:linear-gradient(135deg,var(--border),var(--muted));display:flex;align-items:center;justify-content:center;font-weight:700;font-size:.85rem;color:#fff;flex-shrink:0}
.chat-item.broadcast .chat-item-avatar{background:linear-gradient(135deg,var(--accent),var(--accent2))}
.chat-item-info{flex:1;overflow:hidden}
.chat-item-main{flex:1;min-width:0;display:flex;flex-direction:column;gap:.2rem}
.chat-item-top{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:.2rem}
.chat-item-name{font-size:.82rem;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chat-item-time{font-size:.65rem;color:var(--muted);flex-shrink:0}
.chat-item-preview{font-size:.7rem;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chat-item-tags{display:flex;flex-wrap:wrap;gap:.35rem;margin-top:.15rem}
.chat-item-tag{display:inline-flex;align-items:center;padding:.18rem .45rem;border-radius:999px;font-size:.62rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;border:1px solid var(--border);color:var(--muted);background:rgba(255,255,255,.03)}
.chat-item-tag-privileged{color:#9fc3ff;border-color:rgba(74,168,255,.24);background:rgba(20,42,65,.36)}
.chat-item-tag-retained{color:#f2c37a;border-color:rgba(242,184,75,.24);background:rgba(79,59,12,.24)}
.chat-item-tag-archived{color:#bac8e2;border-color:rgba(138,155,184,.22);background:rgba(255,255,255,.05)}

.inbox-main{display:flex;flex-direction:column;background:rgba(10,13,16,.4);min-height:0}
.chat-header{padding:1rem 1.5rem;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:1rem;background:rgba(19,24,33,.8)}
.chat-header-title{font-weight:600;font-size:.95rem}
.chat-messages{flex:1;overflow-y:auto;padding:1.5rem;display:flex;flex-direction:column;gap:1rem;min-height:0}
.msg{display:flex;flex-direction:column;max-width:85%}
.msg-in{align-self:flex-start}
.msg-out{align-self:flex-end}
.msg-meta{font-size:.65rem;color:var(--muted);margin-bottom:.3rem;display:flex;gap:.5rem}
.msg-out .msg-meta{justify-content:flex-end}
.msg-bubble{padding:.75rem 1rem;border-radius:12px;font-size:.85rem;line-height:1.5;word-break:break-word}
.msg-in .msg-bubble{background:var(--panel2);border:1px solid var(--border);border-bottom-left-radius:4px}
.msg-out .msg-bubble{background:linear-gradient(135deg,#1e3755,#142a41);border:1px solid rgba(74,168,255,.3);color:#eef3ff;border-bottom-right-radius:4px}
.msg-system{align-self:center;background:rgba(255,255,255,.05);padding:.4rem .8rem;font-size:.7rem;color:var(--muted);border-radius:999px;margin:.5rem 0}

/* ── Activity Stats ── */
.activity-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:1rem;margin-bottom:1.5rem}
.activity-card{background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius);padding:1.2rem}
.ac-title{font-size:.72rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.5rem;font-weight:600}
.ac-val{font-size:1.8rem;font-weight:700;color:var(--text)}
.ac-val.highlight{background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.timeline{display:flex;flex-direction:column;gap:1rem;margin-top:1.5rem}
.tl-item{display:grid;grid-template-columns:120px 1fr;gap:1.5rem;position:relative}
.tl-item::before{content:"";position:absolute;left:130px;top:20px;bottom:-1rem;width:2px;background:var(--border)}
.tl-item:last-child::before{display:none}
.tl-time{font-size:.75rem;color:var(--muted);text-align:right;padding-top:.4rem;font-family:var(--mono)}
.tl-content{background:var(--card-bg);border:1px solid var(--border);border-radius:var(--radius);padding:1rem}
.tl-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:.5rem}
.tl-title{font-weight:600;font-size:.85rem}
.tl-body{font-size:.8rem;color:var(--muted)}

/* ── Modal ── */
.modal-overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);backdrop-filter:blur(4px);z-index:500;justify-content:center;align-items:flex-start;padding:3rem 1rem;overflow-y:auto}
.modal-overlay.open{display:flex}
.modal{background:var(--panel);border:1px solid var(--border);border-radius:16px;width:min(680px,95vw);box-shadow:0 16px 48px rgba(0,0,0,.4);animation:modalIn .2s ease}
@keyframes modalIn{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:none}}
.modal-head{display:flex;justify-content:space-between;align-items:center;padding:1rem 1.3rem;border-bottom:1px solid var(--border)}
.modal-head h2{font-size:.95rem;font-weight:700;color:var(--text)}
.modal-close{background:transparent;border:none;color:var(--muted);font-size:1.1rem;cursor:pointer;padding:.3rem}
.modal-close:hover{color:var(--text);transform:none}
.modal-body{padding:1.3rem}

/* ── Toolbar for control buttons ── */
.toolbar{display:flex;gap:8px;margin-bottom:1rem;flex-wrap:wrap}
.toolbar button{padding:.5rem 1rem;border-radius:8px;font-size:.82rem}

/* Misc */
.provider-form-actions{display:flex;flex-direction:row;flex-wrap:wrap;gap:.75rem;align-items:center;justify-content:flex-start}
.provider-form-actions .btn-compact{min-width:140px;max-width:none}
.provider-toggle-row{display:flex;flex-direction:column;gap:.65rem;margin-top:.2rem}
.provider-toggle{display:flex;align-items:flex-start;gap:.75rem;padding:.85rem .95rem;border:1px solid var(--border);border-radius:10px;background:rgba(255,255,255,.02);cursor:pointer;min-height:auto;text-transform:none;letter-spacing:normal;color:var(--text);font-size:inherit;font-weight:400}
.provider-toggle:hover{border-color:rgba(74,168,255,.24);background:rgba(74,168,255,.035)}
.provider-toggle input{margin:.15rem 0 0;flex:0 0 auto;width:16px;height:16px}
.provider-toggle-copy{display:flex;flex-direction:column;gap:.16rem}
.provider-toggle-title{font-size:.82rem;font-weight:700;color:var(--text);text-transform:none;letter-spacing:normal}
.provider-toggle-sub{font-size:.75rem;line-height:1.35;color:var(--muted);text-transform:none;letter-spacing:normal;font-weight:500}
.empty{padding:1.2rem;border:1px dashed var(--border);border-radius:12px;color:var(--muted);text-align:center;font-size:.85rem}
.list{display:flex;flex-direction:column;gap:6px}
.stack{display:flex;flex-direction:column;gap:.75rem}
.mono{font-family:var(--mono)}.small{font-size:.78rem;color:var(--muted)}
.btn-compact{width:auto;max-width:240px;padding:.55rem 1.2rem}
.btn-sm{padding:.35rem .7rem;font-size:.75rem;border-radius:7px}

/* ── Quick nav ── */
.fs-item{display:flex;align-items:center;gap:0.75rem;padding:0.5rem 1rem;border-radius:6px;cursor:pointer;user-select:none}
.fs-item:hover{background:rgba(255,255,255,0.05)}
.fs-item.selected{background:rgba(74,168,255,0.15);border:1px solid rgba(74,168,255,0.3)}
.fs-icon{font-size:1.2rem;opacity:0.7}
.fs-item-name{font-size:0.9rem;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.quick-nav{display:flex;gap:6px;margin-top:.75rem}

/* ── Workdir preview ── */
.workdir-preview{font-size:.75rem;color:var(--muted);padding:.5rem .75rem;background:rgba(13,18,26,.6);border:1px solid var(--border);border-radius:8px}

/* ── Toast ── */
.toast{position:fixed;bottom:1.5rem;left:50%;transform:translateX(-50%) translateY(20px);padding:.65rem 1.4rem;border-radius:10px;font-size:.82rem;font-weight:500;background:rgba(19,24,33,.95);border:1px solid var(--border);color:var(--text);box-shadow:0 8px 24px rgba(0,0,0,.3);backdrop-filter:blur(8px);opacity:0;transition:all .25s ease;pointer-events:none;z-index:600;white-space:nowrap;max-width:90vw}
.toast.show{opacity:1;transform:translateX(-50%) translateY(0);pointer-events:auto}
.toast.error{border-color:rgba(255,106,106,.3);color:var(--danger);background:rgba(40,16,16,.95)}

/* Responsive */
@media(max-width:1100px){.shell{grid-template-columns:1fr}.sidebar{position:static;height:auto;border-right:0;border-bottom:1px solid var(--border);flex-direction:row;flex-wrap:wrap;gap:.5rem}.agent-list{flex-direction:row;flex-wrap:wrap}.agent-item{flex:1;min-width:200px}.dual-grid{grid-template-columns:1fr}.split,.logs{grid-template-columns:1fr}.overview-stats{grid-template-columns:repeat(2,1fr)}.overview-grid,.providers-grid{grid-template-columns:1fr}}
@media(max-width:600px){.overview-stats{grid-template-columns:1fr}.nav-tabs{flex-wrap:wrap}}
`
}
