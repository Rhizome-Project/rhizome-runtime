package main

func managerWebDashboardScriptCore() string {
	return `
(function(){
var state={overview:null,detail:null,activity:null,inbox:{messages:[],agentMap:{},count:0},selectedAgentId:localStorage.getItem("rhizome-bot.selected")||"",activeTab:localStorage.getItem("rhizome-bot.tab")||"overview",activeAgentSubTab:"info",agentPageOpen:false,overviewTimer:null,detailTimer:null,busy:false,dirty:{defaults:false,onboard:false,providers:false,agent:false,ask:false,task:false,tension:false},onboardSyncFolder:true,providerEditingId:"",localChats:[],activeLocalChatId:"",activeLocalChatSession:null,localChatContract:null,localChatSending:false};
window.__lastControlResponse=null;
function esc(v){return String(v==null?"":v).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;").replace(/'/g,"&#39;")}
var dashboardActionSequence=0,dashboardActions=new Map(),dashboardActionPruneQueued=false;
function dashboardAction(callback){
if(typeof callback!=="function"){return""}
var actionID="dashboard-action-"+(++dashboardActionSequence).toString(36);dashboardActions.set(actionID,callback);
if(!dashboardActionPruneQueued){dashboardActionPruneQueued=true;var enqueue=typeof queueMicrotask==="function"?queueMicrotask:function(fn){Promise.resolve().then(fn)};enqueue(function(){dashboardActionPruneQueued=false;var live=new Set();Array.prototype.forEach.call(document.querySelectorAll("[data-dashboard-action]"),function(element){var id=element.getAttribute("data-dashboard-action");if(id){live.add(id)}});dashboardActions.forEach(function(_,id){if(!live.has(id)){dashboardActions.delete(id)}})})}
return'data-dashboard-action="'+actionID+'"'}
document.addEventListener("click",function(event){var target=event.target&&event.target.closest?event.target.closest("[data-dashboard-action]"):null;if(!target){return}var callback=dashboardActions.get(target.getAttribute("data-dashboard-action")||"");if(typeof callback==="function"){callback(event,target)}});
function fmt(v,f){if(v===null||v===undefined||v===""){return f||"\u2014"}if(typeof v==="boolean"){return v?"true":"false"}return String(v)}
function errText(err){if(!err){return"request failed"}if(typeof err==="string"){return err}return err.message||String(err)}
function inspectLabel(v,fallback){var s=String(v||fallback||"").replace(/_/g," ");return s||fallback||"unknown"}

/* ── Toast notifications ── */
var toastTimer=null;
function toast(text,type){
var el=document.getElementById("toast");if(!el){return}
if(toastTimer){clearTimeout(toastTimer)}
el.textContent=text;el.className="toast "+(type||"info")+" show";
toastTimer=setTimeout(function(){el.classList.remove("show")},4000);
}
function setMessage(text,isError){toast(text,isError?"error":"info")}

function kv(label,value){return '<div class="kv"><div class="label">'+esc(label)+'</div><div class="value">'+esc(fmt(value))+'</div></div>'}
function bindClick(id,fn){var n=document.getElementById(id);if(n){n.addEventListener("click",fn)}}
function markDirty(key,value){state.dirty[key]=value!==false}
function isDirty(key){return !!state.dirty[key]}
function localChatContractAvailable(contract){return !!(contract&&contract.availability==="available")}
function localChatExecutionStateLabel(contract){
if(!contract||!contract.execution_state){return"idle"}
return inspectLabel(contract.execution_state,"idle")}
function localChatExecutionStateReasonLabel(contract){
if(!contract||!contract.execution_state_reason){return""}
return inspectLabel(contract.execution_state_reason,"execution state reason")}
function localChatSendReady(contract){return !!(contract&&contract.availability==="available"&&contract.execution_state==="idle")}
function localChatToolScopeLabel(contract){
if(!contract){return"unknown"}
if(contract.shell_allowed){return"trusted local inspect"}
if(contract.mutation_allowed){return"local inspect with bounded mutation"}
return"read-only local inspect"}
function localChatOverridePolicyLabel(contract){
if(!contract||!contract.override_policy){return""}
if(contract.override_policy==="per_send_required"){return"read-only by default; mutation requires explicit operator override per send, and shell also requires explicit mutation acknowledgement"}
return inspectLabel(contract.override_policy,"override policy")}
function localChatBackendLabel(contract){
if(!contract||!contract.auth_backend){return"unavailable"}
if(contract.auth_backend==="manager_runtime"){return"manager runtime"}
return inspectLabel(contract.auth_backend,"unavailable")}
function localChatUnavailableLabel(contract){
if(!contract||!contract.unavailable_reason){return"inspect chat unavailable"}
return inspectLabel(contract.unavailable_reason,"inspect chat unavailable")}
function localChatBlockedLabel(contract){
if(!contract){return"inspect chat unavailable"}
if(contract.execution_state==="busy"){return"manager inspect busy"}
if(contract.execution_state==="saturated"){return"manager inspect saturated"}
return localChatUnavailableLabel(contract)}
function localChatPrivilegedHistoryLabel(session){
if(!session||!session.has_privileged_turns){return""}
var scopePart=""
if(session.last_privileged_tool_scope){scopePart=" ("+inspectLabel(session.last_privileged_tool_scope,"privileged scope")+")"}
if(session.last_override_mode){
if(session.last_override_mode==="legacy_privileged_turn"){return"Privileged history present from earlier inspect turns"+scopePart}
return"Last privileged turn: "+inspectLabel(session.last_override_mode,"privileged turn")+scopePart}
return"Privileged history present"}
function localChatPrivilegedReasonLabel(session){
if(!session||!session.last_override_reason){return""}
return"Latest override reason: "+String(session.last_override_reason)}
function localChatSessionModeLabel(session){
if(!session||!session.session_mode){return""}
if(session.session_mode==="trusted_local_inspect"){return"trusted inspect"}
if(session.session_mode==="archived_retained_inspect"){return"archived retained inspect"}
if(session.session_mode==="privileged_quarantined_inspect"){return"privileged quarantined inspect"}
return inspectLabel(session.session_mode,"read only inspect")}
function localChatSessionSendPolicyLabel(session){
if(!session||!session.send_policy){return""}
if(session.send_policy==="default_trusted"){return"trusted inspect stays active for follow-up"}
if(session.send_policy==="archived_retained_history_only"){return"this inspect chat is archived for retained audit; use a new inspect chat for any follow-up"}
if(session.send_policy==="history_only_after_privileged_turn"){return"this inspect chat is history only after a privileged turn; use a new inspect chat for any follow-up"}
return inspectLabel(session.send_policy,"default read only")}
function localChatArchiveStateLabel(session){
if(!session||!session.archive_state){return""}
if(session.archive_state==="retained_active"){return"retained and active"}
if(session.archive_state==="retained_archived"){return"retained and archived"}
return inspectLabel(session.archive_state,"active")}
function localChatSessionArchived(session){
return !!(session&&session.archive_state==="retained_archived")}
function localChatRetentionModeLabel(session){
if(!session||!session.retention_mode){return""}
if(session.retention_mode==="audit_retained_privileged_history"){return"retained for privileged-history audit"}
if(session.retention_mode==="audit_retained_legacy_manager_inspect_history"){return"retained for legacy manager-inspect audit"}
return inspectLabel(session.retention_mode,"normal retention")}
function localChatDeletePolicyLabel(session){
if(!session||!session.delete_policy){return""}
if(session.delete_policy==="delete_blocked_audit_retention"){return"normal delete blocked for audit retention"}
if(session.delete_policy==="delete_blocked_legacy_audit_retention"){return"normal delete blocked for legacy inspect retention"}
return inspectLabel(session.delete_policy,"normal delete allowed")}
function localChatDeleteBlockedMessage(session){
if(!session||!localChatSessionDeleteBlocked(session)){return"Delete blocked: privileged history requires audit retention."}
if(session.delete_blocked_reason==="legacy_manager_inspect_history_requires_retention"){return"Delete blocked: legacy manager-inspect history requires audit retention."}
return"Delete blocked: privileged history requires audit retention."}
function localChatSessionIsHistoryOnly(session){
return !!(session&&(session.send_policy==="history_only_after_privileged_turn"||session.send_policy==="archived_retained_history_only"))}
function localChatSendBlockedMessage(session){
if(!session){return"Inspect chat unavailable."}
if(session.send_policy==="archived_retained_history_only"){return"This inspect chat is archived for retained audit; start a new inspect chat for follow-up."}
if(session.send_policy==="history_only_after_privileged_turn"){return"This inspect chat is history only after a privileged turn; start a new inspect chat for follow-up."}
return"Inspect chat unavailable."}
function localChatSessionDeleteBlocked(session){
return !!(session&&session.delete_policy&&session.delete_policy!=="normal_delete_allowed")}
function localChatSessionCanArchive(session){
return !!(session&&session.delete_policy&&session.delete_policy!=="normal_delete_allowed"&&session.archive_state!=="retained_archived")}
function localChatArchiveBlockedMessage(session){
if(!session){return"Archive unavailable for this inspect chat."}
if(localChatSessionArchived(session)){return"This retained inspect chat is already archived."}
if(localChatSessionDeleteBlocked(session)){return"Archive available for retained inspect chats."}
return"Only retained inspect chats can be archived."}
function localChatSessionSummaryByID(chatID){
if(state.activeLocalChatSession&&state.activeLocalChatSession.chat_id===chatID){return state.activeLocalChatSession}
if(!state.localChats||!chatID){return null}
for(var i=0;i<state.localChats.length;i++){if(state.localChats[i]&&state.localChats[i].chat_id===chatID){return state.localChats[i]}}
return null}
function activeLocalChatSessionSummary(){
if(!state.activeLocalChatId){return null}
var summary=localChatSessionSummaryByID(state.activeLocalChatId)
if(summary){return summary}
return null}
function localChatExecutionSummary(execution){
if(!execution){return""}
if(execution.snapshot_status==="legacy_partial"){return"Manager Inspect legacy execution snapshot unavailable"}
var parts=[]
if(execution.execution_identity){parts.push(inspectLabel(execution.execution_identity,"manager process"))}
if(execution.service_identity_mode){parts.push(inspectLabel(execution.service_identity_mode,"shared manager identity"))}
if(execution.tool_scope){parts.push(inspectLabel(execution.tool_scope,"inspect tool scope"))}
if(execution.override_mode&&execution.override_mode!=="default_trusted_shell"&&execution.override_mode!=="default_trusted_mutation"){parts.push(inspectLabel(execution.override_mode,"default read only"))}
if(execution.auth_backend){parts.push(inspectLabel(execution.auth_backend,"auth backend"))}
if(execution.workspace_persona_mode&&execution.workspace_persona_mode!=="none"){parts.push(inspectLabel(execution.workspace_persona_mode,"workspace persona mode"))}
if(typeof execution.shell_allowed==="boolean"){parts.push(execution.shell_allowed?"shell allowed":"no shell")}
if(typeof execution.mutation_allowed==="boolean"){parts.push(execution.mutation_allowed?"mutation allowed":"read only")}
return parts.join(" • ")}
function localChatToolsUsedSummary(execution){
if(!execution||!execution.tools_used||!execution.tools_used.length){return""}
return execution.tools_used.map(function(tool){var status=tool&&tool.status==="error"?"error":"ok";return inspectLabel(tool&&tool.name,"tool")+" ("+status+")"}).join(", ")}
function localChatOverrideReasonSummary(execution){
if(!execution||!execution.override_reason){return""}
return"Override reason: "+String(execution.override_reason)}
function renderLocalChatControlsState(){
var contract=state.localChatContract||(state.detail&&state.detail.local_chat_contract)||null;
var activeSession=activeLocalChatSessionSummary();
var available=localChatContractAvailable(contract);
var sendReady=localChatSendReady(contract);
var textarea=document.getElementById("local-chat-input");
var sendButton=document.getElementById("local-chat-send-button");
var newButton=document.getElementById("local-chat-new-button");
if(activeSession&&localChatSessionIsHistoryOnly(activeSession)){sendReady=false}
if(textarea){
textarea.disabled=!sendReady||state.localChatSending;
if(sendReady){textarea.placeholder="Send an inspect message..."}
else if(activeSession&&localChatSessionIsHistoryOnly(activeSession)){textarea.placeholder=localChatSendBlockedMessage(activeSession)}
else{textarea.placeholder="Inspect chat unavailable: "+localChatBlockedLabel(contract)}
}
if(sendButton){sendButton.disabled=!sendReady||state.localChatSending}
if(newButton){newButton.disabled=!available}
}
function renderLocalChatContractBanner(){
var node=document.getElementById("local-chat-contract-banner");
if(!node){return}
var contract=state.localChatContract||(state.detail&&state.detail.local_chat_contract)||null;
if(!contract){
node.innerHTML='<div class="small">Inspect chat contract unavailable.</div>';
renderLocalChatControlsState();
return
}
var mode=inspectLabel(contract.channel_mode,"manager mediated inspect");
var runtimeRelation=inspectLabel(contract.runtime_relation,"not live managed runtime");
var executionIdentity=inspectLabel(contract.execution_identity,"manager process");
var serviceIdentityMode=inspectLabel(contract.service_identity_mode,"shared manager process identity");
var transcriptScope=inspectLabel(contract.transcript_scope,"manager owned");
var availability=inspectLabel(contract.availability,"unavailable");
var executionState=localChatExecutionStateLabel(contract);
var statusLine='Availability: <strong>'+esc(availability)+'</strong>';
if(localChatContractAvailable(contract)){
statusLine+=' via <strong>'+esc(localChatBackendLabel(contract))+'</strong>';
}else if(contract.unavailable_reason){
statusLine+=' because <strong>'+esc(localChatUnavailableLabel(contract))+'</strong>';
}
statusLine+=' with execution state <strong>'+esc(executionState)+'</strong>';
if(contract.execution_state_reason){
statusLine+=' because <strong>'+esc(localChatExecutionStateReasonLabel(contract))+'</strong>';
}
var overrideLine=""
if(contract.override_policy){
overrideLine='<div class="small" style="margin-top:0.35rem">Override policy: <strong>'+esc(localChatOverridePolicyLabel(contract))+'</strong>.'
if(contract.override_can_mutation||contract.override_can_shell){
overrideLine+=' Allowed per-send override: <strong>'+esc([contract.override_can_mutation?"mutation":null,contract.override_can_shell?"shell":null].filter(Boolean).join(" + "))+'</strong>.'
}
overrideLine+='</div>'
}
var historyLine=""
var activeSession=activeLocalChatSessionSummary()
if(activeSession&&activeSession.has_privileged_turns){
historyLine='<div class="small" style="margin-top:0.35rem">History: <strong>'+esc(localChatPrivilegedHistoryLabel(activeSession))+'</strong>.</div>'
if(activeSession.last_override_reason){
historyLine+='<div class="small" style="margin-top:0.35rem">Reason: <strong>'+esc(localChatPrivilegedReasonLabel(activeSession))+'</strong>.</div>'
}
}
var sessionModeLine=""
if(activeSession&&activeSession.session_mode){
if(activeSession.session_mode!=="trusted_local_inspect"&&activeSession.session_mode!=="read_only_inspect"){
sessionModeLine='<div class="small" style="margin-top:0.35rem">Session mode: <strong>'+esc(localChatSessionModeLabel(activeSession))+'</strong>.</div>'
}
if(activeSession.send_policy&&localChatSessionIsHistoryOnly(activeSession)){
sessionModeLine+='<div class="small" style="margin-top:0.35rem">Session send policy: <strong>'+esc(localChatSessionSendPolicyLabel(activeSession))+'</strong>.</div>'
}
if(activeSession.retention_mode){
sessionModeLine+='<div class="small" style="margin-top:0.35rem">Retention: <strong>'+esc(localChatRetentionModeLabel(activeSession))+'</strong>.</div>'
}
if(activeSession.archive_state){
sessionModeLine+='<div class="small" style="margin-top:0.35rem">Archive state: <strong>'+esc(localChatArchiveStateLabel(activeSession))+'</strong>.</div>'
if(activeSession.archived_at){
sessionModeLine+='<div class="small" style="margin-top:0.35rem">Archived at: <strong>'+esc(new Date(activeSession.archived_at).toLocaleString())+'</strong>.</div>'
}
}
if(activeSession.delete_policy){
sessionModeLine+='<div class="small" style="margin-top:0.35rem">Delete policy: <strong>'+esc(localChatDeletePolicyLabel(activeSession))+'</strong>.</div>'
}
}
node.innerHTML='<div><strong>'+esc(mode)+'</strong></div>'
+'<div class="small" style="margin-top:0.35rem">Runs via <strong>'+esc(executionIdentity)+'</strong> under <strong>'+esc(serviceIdentityMode)+'</strong>, stays <strong>'+esc(runtimeRelation)+'</strong>, stores transcripts under <strong>'+esc(transcriptScope)+'</strong>, exposes <strong>'+esc(localChatToolScopeLabel(contract))+'</strong>, and currently reports '+statusLine+'.</div>'
+overrideLine
+historyLine
+sessionModeLine;
renderLocalChatControlsState();
}
function resetAgentInteractionDirty(){state.dirty.agent=false;state.dirty.ask=false;state.dirty.task=false;state.dirty.tension=false}
function resetAllDirty(){state.dirty.defaults=false;state.dirty.onboard=false;state.dirty.providers=false;resetAgentInteractionDirty()}
function currentRows(){return state.overview&&Array.isArray(state.overview.agents)?state.overview.agents:[]}
function selectedRow(){var rows=currentRows();for(var i=0;i<rows.length;i++){if(rows[i].record&&rows[i].record.agent_id===state.selectedAgentId){return rows[i]}}return null}
function shouldRefreshDetail(){return !!(state.selectedAgentId&&(state.agentPageOpen||state.activeTab!=="overview"))}
async function api(path,options){var resp=await fetch(path,Object.assign({headers:{"Content-Type":"application/json"}},options||{}));var text=await resp.text();var data=null;if(text){try{data=JSON.parse(text)}catch(_){data={raw:text}}}if(!resp.ok){var err=new Error(data&&data.error?data.error:"http "+resp.status);err.payload=data;err.status=resp.status;throw err}return data}

/* ── Top-level Tab switching ── */
function switchTab(tabName){
  state.activeTab=tabName;state.agentPageOpen=false;
  localStorage.setItem("rhizome-bot.tab",tabName);
  var panels=document.querySelectorAll(".tab-panel");
  for(var i=0;i<panels.length;i++){panels[i].classList.remove("active")}
  var target=document.getElementById("panel-"+tabName);
  if(target){target.classList.add("active")}
  var tabs=document.querySelectorAll(".nav-tab");
  for(var i=0;i<tabs.length;i++){tabs[i].classList.toggle("active",tabs[i].getAttribute("data-tab")===tabName)}
  document.getElementById("agent-page").classList.remove("open");
  var topbar=document.getElementById("topbar");if(topbar){topbar.style.display=""}
}
window.switchTab=switchTab;

/* ── Agent Sub-tab switching ── */
function switchAgentSubTab(subTabName){
  state.activeAgentSubTab=subTabName;
  var panels=document.querySelectorAll(".agent-sub-panel");
  for(var i=0;i<panels.length;i++){panels[i].classList.remove("active")}
  var target=document.getElementById("agent-panel-"+subTabName);
  if(target){target.classList.add("active")}
  var tabs=document.querySelectorAll(".sub-tab");
  for(var i=0;i<tabs.length;i++){tabs[i].classList.toggle("active",tabs[i].getAttribute("data-subtab")===subTabName)}
  if(subTabName==="controls"){fetchLocalChats();if(state.agentPageOpen&&state.detail){return}}
  if(state.agentPageOpen&&state.detail&&(subTabName==="info"||subTabName==="settings"||subTabName==="runtime"||subTabName==="logs")){return}
  if(state.agentPageOpen&&subTabName==="activity"&&state.activity&&state.activity.agent_id===state.selectedAgentId){renderActivityPanel();return}
  if(state.agentPageOpen&&subTabName==="inbox"&&state.inbox&&state.inbox.agent_id===state.selectedAgentId){renderInboxPanel();return}
  if(state.agentPageOpen&&state.detail&&subTabName==="activity"){
    renderActivityPanel();
    api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/activity").then(async function(activityPayload){
      await applyOverviewPayload(activityPayload,true,true);
      applyDetailPayload(activityPayload);
      state.activity=activityPayload;
      renderActivityPanel();
    }).catch(function(err){handleDashboardReadError(err,true)});
    return
  }
  if(state.agentPageOpen&&state.detail&&subTabName==="inbox"){
    var currentCh=state.inbox&&state.inbox.channel?state.inbox.channel:"";
    renderInboxPanel();
    api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/messages").then(async function(fetched){
      await applyOverviewPayload(fetched,true,true);
      applyDetailPayload(fetched);
      state.inbox=fetched;
      state.inbox.channel=currentCh;
      renderInboxPanel();
    }).catch(function(err){handleDashboardReadError(err,true)});
    return
  }
  if(state.agentPageOpen){refreshDetail(true).catch(function(err){handleDashboardReadError(err,true)})}
}
window.switchAgentSubTab=switchAgentSubTab;

/* ── Agent detail page ── */
function openAgentPage(agentId){
  state.selectedAgentId=agentId;state.agentPageOpen=true;
  localStorage.setItem("rhizome-bot.selected",agentId);
  resetAgentInteractionDirty();
  state.localChats=[];state.activeLocalChatId="";state.activeLocalChatSession=null;state.localChatContract=null;
  var panels=document.querySelectorAll(".tab-panel");
  for(var i=0;i<panels.length;i++){panels[i].classList.remove("active")}
  var topbar=document.getElementById("topbar");if(topbar){topbar.style.display="none"}
  document.getElementById("agent-page").classList.add("open");
  document.getElementById("agent-page-id").textContent=agentId;
  switchAgentSubTab("info");
  renderOverview();
}
window.openAgentPage=openAgentPage;

function closeAgentPage(){
  state.agentPageOpen=false;
  state.localChats=[];state.activeLocalChatId="";state.activeLocalChatSession=null;state.localChatContract=null;
  document.getElementById("agent-page").classList.remove("open");
  var topbar=document.getElementById("topbar");if(topbar){topbar.style.display=""}
  switchTab("overview");
}
window.closeAgentPage=closeAgentPage;

function goHome(){closeAgentPage();switchTab("overview")}
window.goHome=goHome;

/* ── Onboard modal ── */
function showOnboardModal(){state.onboardSyncFolder=true;document.getElementById("onboard-modal").classList.add("open");renderOnboardForm()}
function closeOnboardModal(){document.getElementById("onboard-modal").classList.remove("open")}
window.showOnboardModal=showOnboardModal;
window.closeOnboardModal=closeOnboardModal;
function showProviderModal(providerId){
var nextId=providerId||"";
var modal=document.getElementById("provider-modal");
var isOpen=modal&&modal.classList.contains("open");
if(isOpen&&isDirty("providers")&&state.providerEditingId!==nextId){
if(!window.confirm("Discard unsaved provider changes?")){return}
}
state.providerEditingId=nextId;
state.dirty.providers=false;
document.getElementById("provider-modal").classList.add("open");
renderProviderModal();
}
function closeProviderModal(force){
var modal=document.getElementById("provider-modal");
if(!force&&modal&&modal.classList.contains("open")&&isDirty("providers")){
if(!window.confirm("Discard unsaved provider changes?")){return}
}
document.getElementById("provider-modal").classList.remove("open");
state.providerEditingId="";
state.dirty.providers=false;
renderProviderModal();
}
window.showProviderModal=showProviderModal;
window.closeProviderModal=closeProviderModal;

function extractOverviewFromPayload(payload){
if(!payload||!payload.defaults||!payload.create_default){return null}
return{command:payload.command||"",defaults:payload.defaults,agents:Array.isArray(payload.agents)?payload.agents:[],providers:Array.isArray(payload.providers)?payload.providers:[],provider_catalog:Array.isArray(payload.provider_catalog)?payload.provider_catalog:[],providers_error:payload.providers_error||"",create_default:payload.create_default}
}
function applyDetailPayload(payload){
if(!payload){return false}
var canCreateDetail=!!(payload.record||payload.local_runtime||payload.effective_identity||payload.profile||payload.logs)
if(!state.detail&&!canCreateDetail){
if(payload.local_chat_contract){state.localChatContract=payload.local_chat_contract;renderLocalChatContractBanner();return true}
return false
}
state.detail=state.detail||{}
var applied=false
if(payload.record){state.detail.record=payload.record;applied=true}
if(payload.process){state.detail.process=payload.process;applied=true}
if(payload.local_runtime){state.detail.local_runtime=payload.local_runtime;applied=true}
if(payload.effective_identity){state.detail.effective_identity=payload.effective_identity;applied=true}
if(payload.profile){state.detail.profile=payload.profile;applied=true}
if(payload.live){state.detail.live=payload.live;applied=true}
if(payload.catalog){state.detail.catalog=payload.catalog;applied=true}
if(payload.logs){state.detail.logs=payload.logs;applied=true}
if(payload.local_chat_contract){state.detail.local_chat_contract=payload.local_chat_contract;state.localChatContract=payload.local_chat_contract;applied=true}
if(applied){renderDetail()}
return applied
}
async function applyOverviewPayload(payload,keepMessage,skipDetailRefresh){
var overview=extractOverviewFromPayload(payload);if(!overview){return false}
var previousSelected=state.selectedAgentId;state.overview=overview;var rows=currentRows();if(!state.selectedAgentId||!rows.some(function(row){return row.record.agent_id===state.selectedAgentId})){state.selectedAgentId=rows.length?rows[0].record.agent_id:""}if(previousSelected!==state.selectedAgentId){resetAgentInteractionDirty()}if(state.selectedAgentId){localStorage.setItem("rhizome-bot.selected",state.selectedAgentId)}else{localStorage.removeItem("rhizome-bot.selected")}renderOverview();renderOverviewTab();renderDefaultsForm();renderProvidersTab();renderProviderModal();
if(shouldRefreshDetail()){if(!skipDetailRefresh){await refreshDetail(keepMessage)}}else if(!state.agentPageOpen){state.detail=null;renderDetail()}return true
}
async function hydrateDashboardError(err,refreshDetailAfter){
if(!err||!err.payload){return false}
var appliedOverview=await applyOverviewPayload(err.payload,true,true);var appliedDetail=applyDetailPayload(err.payload);
if(appliedOverview&&refreshDetailAfter&&shouldRefreshDetail()&&!appliedDetail){await refreshDetail(true)}
return !!(appliedOverview||appliedDetail)
}
async function handleDashboardReadError(err,refreshDetailAfter){
try{await hydrateDashboardError(err,refreshDetailAfter)}catch(hydrationErr){console.error("dashboard read error hydrate error",hydrationErr)}
handleError(err)
}
async function refreshOverview(keepMessage,skipDetailRefresh){var payload=await api("/api/overview");if(!await applyOverviewPayload(payload,keepMessage,!!skipDetailRefresh)){throw new Error("overview response missing dashboard context")}}
async function refreshDetail(keepMessage){
  if(!state.selectedAgentId){state.detail=null;state.activity=null;state.inbox={messages:[],agentMap:{},count:0};renderDetail();return}
  var detailPayload=await api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"?lines=80");
  await applyOverviewPayload(detailPayload,true,true);
  state.detail=detailPayload;
  if(state.detail&&state.detail.local_chat_contract){state.localChatContract=state.detail.local_chat_contract}
  if(state.activeAgentSubTab==="activity"||state.activeAgentSubTab==="runtime"){
    try{
      var activityPayload=await api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/activity");
      await applyOverviewPayload(activityPayload,true,true);
      applyDetailPayload(activityPayload);
      state.activity=activityPayload
    }catch(e){console.warn(e)}
  }
  if(state.activeAgentSubTab==="inbox"){
    var currentCh=state.inbox.channel;
    try{
      var fetched=await api("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/messages");
      await applyOverviewPayload(fetched,true,true);
      applyDetailPayload(fetched);
      state.inbox=fetched;
      state.inbox.channel=currentCh;
    }catch(e){console.warn(e)}
  }
  renderLocalChatContractBanner();
  renderDetail();
  if(!keepMessage){setMessage("")}
}
async function postJSON(path,payload,successMessage,refreshDetailAfter,customRefresh){try{if(state.busy){return}state.busy=true;setMessage("working...");var result=await api(path,{method:"POST",body:JSON.stringify(payload||{})});window.__lastControlResponse=result;if(typeof customRefresh==="function"){await customRefresh(result)}else{var appliedOverview=await applyOverviewPayload(result,true,true);var appliedDetail=applyDetailPayload(result);if(!appliedOverview){await refreshOverview(true)}else if(refreshDetailAfter&&shouldRefreshDetail()&&!appliedDetail){await refreshDetail(true)}}setMessage(successMessage||result.message||"done")}catch(err){await hydrateDashboardError(err,refreshDetailAfter);handleError(err)}finally{state.busy=false}}
function sendControl(method,payload,message){if(!state.selectedAgentId){setMessage("select an agent first",true);return}postJSON("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/control",{method:method,payload:payload},message||"control request sent",true,async function(result){window.__lastControlResponse=result;var appliedOverview=await applyOverviewPayload(result,true,true);var appliedDetail=applyDetailPayload(result);if(!appliedOverview){if(shouldRefreshDetail()){await refreshDetail(true)}}else if(!appliedDetail&&shouldRefreshDetail()){await refreshDetail(true)}})}
function runProcessAction(action,agentId){var aid=agentId||state.selectedAgentId;if(!aid){setMessage("select an agent first",true);return}postJSON("/api/agents/"+encodeURIComponent(aid)+"/process",{action:action},action+" completed",true)}
window.runProcessAction=runProcessAction;
function handleError(err){console.error(err);setMessage(errText(err),true)}
function getLastResponseText(){return window.__lastControlResponse?JSON.stringify(window.__lastControlResponse,null,2):"no live control response yet"}
function updateOnboardPreview(){var parent=document.getElementById("onboard-parent_dir");var folder=document.getElementById("onboard-folder_name");var preview=document.getElementById("onboard-preview");if(!parent||!folder||!preview){return}var pv=(parent.value||"").trim();var fv=(folder.value||"").trim();if(!pv||!fv){preview.textContent="workdir preview unavailable";return}var sep=pv.indexOf("\\")>=0?"\\":"/";preview.textContent=pv.replace(/[\\\/]+$/,"")+sep+fv}

/* ── Inbox Utilities ── */
window.setInboxChannel=function(ch){state.inbox.channel=ch;renderInboxPanel()};


/* ── Onboard field auto-sync: agent_id → folder_name + display_name ── */
function setupOnboardSync(){
var agentIdField=document.getElementById("onboard-agent_id");
var folderField=document.getElementById("onboard-folder_name");
var displayField=document.getElementById("onboard-display_name");
if(!agentIdField||!folderField){return}

/* Track if user manually edited folder_name */
state.onboardSyncFolder=true;
folderField.addEventListener("input",function(){state.onboardSyncFolder=false;markDirty("onboard")});

/* agent_id → folder_name (if not manually edited) + display_name */
agentIdField.addEventListener("input",function(){
  var val=agentIdField.value;
  if(state.onboardSyncFolder){folderField.value=val;updateOnboardPreview()}
  if(displayField){displayField.value=val}
  markDirty("onboard");
});
}

function attachFormHandlers(){
document.getElementById("defaults-form").addEventListener("submit",async function(event){event.preventDefault();var form=event.currentTarget;var entries=Array.prototype.slice.call(form.elements).filter(function(node){return node.name}).filter(function(node){return node.name!=="workspace_password"||String(node.value||"").trim()!==""}).map(function(node){return{field:node.name,value:node.value||""}});try{state.busy=true;setMessage("saving defaults...");var lastResult=null;for(var i=0;i<entries.length;i++){lastResult=await api("/api/defaults",{method:"POST",body:JSON.stringify(entries[i])})}markDirty("defaults",false);var applied=lastResult?await applyOverviewPayload(lastResult,true,true):false;if(!applied){await refreshOverview(true)}setMessage("defaults saved ✓")}catch(err){await hydrateDashboardError(err,false);handleError(err)}finally{state.busy=false}});
document.getElementById("onboard-form").addEventListener("submit",async function(event){event.preventDefault();var payload={};Array.prototype.forEach.call(event.currentTarget.elements,function(node){if(node.name){payload[node.name]=node.value||""}});try{state.busy=true;setMessage("registering agent...");var result=await api("/api/onboard",{method:"POST",body:JSON.stringify(payload)});if(result&&result.record&&result.record.agent_id){state.selectedAgentId=result.record.agent_id}resetAllDirty();closeOnboardModal();var appliedOverview=await applyOverviewPayload(result,true,true);var appliedDetail=applyDetailPayload(result);if(!appliedOverview){await refreshOverview(true)}else if(!appliedDetail&&shouldRefreshDetail()){await refreshDetail(true)}setMessage("agent registered ✓")}catch(err){await hydrateDashboardError(err,false);handleError(err)}finally{state.busy=false}});
}
function bindAgentPanel(){
var panel=document.getElementById("agent-panel-info");if(!panel){return}
Array.prototype.forEach.call(panel.querySelectorAll("[data-process]"),function(button){button.addEventListener("click",function(){runProcessAction(button.getAttribute("data-process"))})});
var refreshButton=panel.querySelector("[data-refresh='detail']");if(refreshButton){refreshButton.addEventListener("click",function(){refreshDetail(true).then(function(){setMessage("detail refreshed ✓")}).catch(function(err){handleDashboardReadError(err,true)})})}
var editForm=document.getElementById("edit-agent-form");if(editForm){Array.prototype.forEach.call(editForm.querySelectorAll("input,textarea"),function(node){node.addEventListener("input",function(){markDirty("agent")})});editForm.addEventListener("submit",function(event){event.preventDefault();markDirty("agent",false);var payload={display_name:(document.getElementById("edit-display-name").value||"").trim(),workdir:(document.getElementById("edit-workdir").value||"").trim(),role:(document.getElementById("edit-role").value||"").trim(),tags:(document.getElementById("edit-tags").value||"").trim(),soul_prompt:(document.getElementById("edit-soul-prompt").value||"").trim()},groupField=document.getElementById("edit-group-id");if(groupField&&groupField.name){payload.group_id=(groupField.value||"").trim()}postJSON("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/edit",payload,"agent updated ✓",false)})}
bindClick("remove-agent",function(){var row=selectedRow();if(!row){return}if(!confirm("Remove "+row.record.agent_id+" from registry?")){return}postJSON("/api/agents/"+encodeURIComponent(state.selectedAgentId)+"/edit",{remove:true},"agent removed",false,async function(result){state.selectedAgentId="";state.detail=null;closeAgentPage();var applied=await applyOverviewPayload(result,true,true);if(!applied){await refreshOverview(true)}})})
}
function bindControlPanel(){
renderLocalChatContractBanner();
var chatForm=document.getElementById("local-chat-form");if(chatForm&&!chatForm.dataset.bound){chatForm.dataset.bound="true";chatForm.addEventListener("submit",function(e){e.preventDefault();var inp=document.getElementById("local-chat-input");var v=(inp.value||"").trim();if(v)sendLocalChatMessage(v);});var inp=document.getElementById("local-chat-input");if(inp){inp.addEventListener("keydown",function(e){if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();var v=(inp.value||"").trim();if(v)sendLocalChatMessage(v);}});}}
var askForm=document.getElementById("ask-form");if(askForm){Array.prototype.forEach.call(askForm.querySelectorAll("textarea,input"),function(node){node.addEventListener("input",function(){markDirty("ask")})});askForm.addEventListener("submit",function(event){event.preventDefault();var prompt=(document.getElementById("ask-prompt").value||"").trim();if(!prompt){setMessage("prompt is empty",true);return}markDirty("ask",false);sendControl("model.ask",prompt,"prompt sent ✓")})}
var taskForm=document.getElementById("task-form");if(taskForm){Array.prototype.forEach.call(taskForm.querySelectorAll("input,select"),function(node){node.addEventListener("input",function(){markDirty("task")});node.addEventListener("change",function(){markDirty("task")})});taskForm.addEventListener("submit",function(event){event.preventDefault();var payload={task_id:(document.getElementById("switch-task-id").value||"").trim(),session_id:(document.getElementById("switch-session-id").value||"").trim(),reason:(document.getElementById("switch-task-reason").value||"").trim()};if(!payload.task_id){setMessage("task_id is required",true);return}markDirty("task",false);sendControl("runtime.switch_task",payload,"task switch requested ✓")})}
var tensionForm=document.getElementById("tension-form");if(tensionForm){Array.prototype.forEach.call(tensionForm.querySelectorAll("input,select"),function(node){node.addEventListener("input",function(){markDirty("tension")});node.addEventListener("change",function(){markDirty("tension")})});tensionForm.addEventListener("submit",function(event){event.preventDefault();var payload={tension_id:(document.getElementById("switch-tension-id").value||"").trim(),action:(document.getElementById("switch-tension-action").value||"").trim(),role:(document.getElementById("switch-tension-role").value||"").trim(),lifecycle_state:(document.getElementById("switch-tension-state").value||"").trim(),reason:(document.getElementById("switch-tension-reason").value||"").trim()};if(!payload.tension_id){setMessage("tension_id is required",true);return}markDirty("tension",false);sendControl("runtime.switch_tension",payload,"tension switch requested ✓")})}
bindClick("runtime-status",function(){sendControl("runtime.status",{reason:"web dashboard status"},"runtime.status sent ✓")});
bindClick("runtime-refresh",function(){sendControl("runtime.refresh",{reason:"web dashboard refresh"},"runtime.refresh sent ✓")});
bindClick("runtime-pause",function(){sendControl("runtime.pause",{reason:"web dashboard pause"},"paused ✓")});
bindClick("runtime-resume",function(){sendControl("runtime.resume",{reason:"web dashboard resume"},"resumed ✓")});
}
function switchControlsTab(tab){
var tabs=document.querySelectorAll('.ctl-tab');for(var i=0;i<tabs.length;i++){if(tabs[i].getAttribute('data-ctltab')===tab)tabs[i].classList.add('active');else tabs[i].classList.remove('active');}
var panels=document.querySelectorAll('.ctl-panel');for(var i=0;i<panels.length;i++){if(panels[i].id==='ctl-panel-'+tab)panels[i].classList.add('active');else panels[i].classList.remove('active');}
}
function fetchLocalChats(){
if(!state.selectedAgentId)return;api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats').then(function(res){applyLocalChatPayload(res).catch(function(err){console.error("fetchChats error",err);if(!applyLocalChatPayloadFallback(res,false)){setMessage("Inspect chats refresh partially failed: "+err.message,true)}})}).catch(function(err){hydrateLocalChatError(err,false).finally(function(){setMessage("Inspect chats refresh failed: "+err.message,true)})});}
function renderLocalChatsList(){
var list=document.getElementById('local-chats-list');if(!list)return;if(!state.localChats||state.localChats.length===0){list.innerHTML='<div class="empty">No inspect chats yet</div>';return;}
	var active=[];var archived=[];state.localChats.forEach(function(c){if(localChatSessionArchived(c)){archived.push(c)}else{active.push(c)}});
	function renderRow(c){var title=c.title||"New Inspect Chat";var date=new Date(c.updated_at).toLocaleString();var cl='chat-item';if(c.chat_id===state.activeLocalChatId)cl+=' active';
	var chips='',rowHints=[];
	if(c.has_privileged_turns){var privilegedLabel=localChatPrivilegedHistoryLabel(c);chips+='<span class="chat-item-tag chat-item-tag-privileged" title="Privileged history: '+esc(privilegedLabel)+'">Privileged</span>';rowHints.push('Privileged history: '+privilegedLabel)}
	if(localChatSessionDeleteBlocked(c)){var retentionLabel=localChatRetentionModeLabel(c),deleteBlockedLabel=localChatDeleteBlockedMessage(c),retainedTitle=deleteBlockedLabel+(retentionLabel?' Retention: '+retentionLabel+'.':'');chips+='<span class="chat-item-tag chat-item-tag-retained" title="'+esc(retainedTitle)+'">Retained</span>';if(retentionLabel){rowHints.push('Retention: '+retentionLabel)}rowHints.push(deleteBlockedLabel)}
	if(c.archive_state){var archiveLabel=localChatArchiveStateLabel(c);chips+='<span class="chat-item-tag chat-item-tag-archived" title="Archive: '+esc(archiveLabel)+'">'+esc(archiveLabel)+'</span>';rowHints.push('Archive: '+archiveLabel)}
	var meta='<div class="chat-item-name">'+esc(title)+'</div><div class="chat-item-time">'+esc(date)+'</div>'+(chips?'<div class="chat-item-tags">'+chips+'</div>':'');
	var actions='';
	if(localChatSessionCanArchive(c)){actions+='<button type="button" class="chat-item-action chat-item-action-archive" title="Archive retained inspect chat" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();archiveLocalChat((c.chat_id))}) + '>Archive</button>'}
	if(!localChatSessionDeleteBlocked(c)){actions+='<button type="button" class="chat-item-action chat-item-action-delete" title="Delete chat" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();deleteLocalChat((c.chat_id))}) + '>Delete</button>'}
	return '<div class="'+cl+'"'+(rowHints.length?' title="'+esc(rowHints.join(' | '))+'"':'')+' ' + dashboardAction(function(dashboardEvent){loadLocalChat((c.chat_id))}) + '><div class="chat-item-avatar" style="background:linear-gradient(135deg,#3b82f6,#2563eb)">#</div><div class="chat-item-main">'+meta+'</div>'+(actions?'<div class="chat-item-actions">'+actions+'</div>':'')+'</div>';}
	function renderSection(title,items){if(!items.length){return''}var html='<div class="chat-item-time" style="padding:0.85rem 1rem 0.35rem; text-transform:uppercase; letter-spacing:.08em">'+esc(title)+'</div>';items.forEach(function(c){html+=renderRow(c)});return html;}
	list.innerHTML=renderSection('Active Inspect Chats',active)+renderSection('Archived Retained Chats',archived);
}
function renderLocalChatSessionMessages(session){
var container=document.getElementById('local-chat-messages');if(!container)return;
if(!session){container.innerHTML='<div class="empty">Select or create an inspect chat</div>';return}
var html='';
if(!session.messages||session.messages.length===0){html='<div class="empty">Empty inspect chat</div>';}else{
session.messages.forEach(function(m){var cls=m.role==='user'?'msg-out':'msg-in';var name=m.role==='user'?'You':(m.origin==='manager_inspect'?'Manager Inspect':'Inspect');var time=new Date(m.timestamp).toLocaleTimeString();var executionSummary=m.origin==='manager_inspect'?localChatExecutionSummary(m.execution):"";var overrideReasonSummary=m.origin==='manager_inspect'?localChatOverrideReasonSummary(m.execution):"";var toolsSummary=m.origin==='manager_inspect'?localChatToolsUsedSummary(m.execution):"";
html+='<div class="msg '+cls+'"><div class="msg-meta"><span>'+esc(name)+'</span><span>'+esc(time)+'</span></div>'+(executionSummary?'<div class="msg-meta"><span>'+esc(executionSummary)+'</span></div>':'')+(overrideReasonSummary?'<div class="msg-meta"><span>'+esc(overrideReasonSummary)+'</span></div>':'')+(toolsSummary?'<div class="msg-meta"><span>Tools: '+esc(toolsSummary)+'</span></div>':'')+'<div class="msg-bubble">'+esc(m.content).replace(/\n/g,"<br>")+'</div></div>';});
}
container.innerHTML=html;container.scrollTop=container.scrollHeight;
}
function applyLocalChatPayloadFallback(payload,renderMessages){
var applied=false;
if(payload&&Array.isArray(payload.sessions)){state.localChats=payload.sessions;applied=true}
if(payload&&payload.contract){state.localChatContract=payload.contract;if(state.detail){state.detail.local_chat_contract=payload.contract}applied=true}
if(payload&&payload.session){state.activeLocalChatSession=payload.session;if(payload.session.chat_id){state.activeLocalChatId=payload.session.chat_id}applied=true}
else if(state.activeLocalChatId){var summary=activeLocalChatSessionSummary();state.activeLocalChatSession=summary?Object.assign({},state.activeLocalChatSession||{},summary):null}
if(applied){renderLocalChatContractBanner();renderLocalChatsList();if(renderMessages){renderLocalChatSessionMessages(state.activeLocalChatSession)}}
return applied
}
async function applyLocalChatPayload(payload){
var appliedOverview=await applyOverviewPayload(payload,true,true);var appliedDetail=applyDetailPayload(payload);
if(payload&&Array.isArray(payload.sessions)){state.localChats=payload.sessions}
if(payload&&payload.contract){state.localChatContract=payload.contract;if(state.detail){state.detail.local_chat_contract=payload.contract}}
if(payload&&payload.session){state.activeLocalChatSession=payload.session;if(payload.session.chat_id){state.activeLocalChatId=payload.session.chat_id}}
else if(state.activeLocalChatId){var summary=activeLocalChatSessionSummary();state.activeLocalChatSession=summary?Object.assign({},state.activeLocalChatSession||{},summary):null}
renderLocalChatContractBanner();renderLocalChatsList();
if(!appliedOverview){await refreshOverview(true)}else if(shouldRefreshDetail()&&!appliedDetail){await refreshDetail(true)}
return appliedOverview||appliedDetail
}
function hydrateLocalChatError(err,renderMessages){
if(!err||!err.payload){return Promise.resolve(false)}
return applyLocalChatPayload(err.payload).then(function(){
if(renderMessages){renderLocalChatSessionMessages(state.activeLocalChatSession)}
return true
}).catch(function(hydrationErr){console.error("localChat error hydrate error",hydrationErr);return applyLocalChatPayloadFallback(err.payload,renderMessages)})
}
function deleteLocalChat(id){
	var summary=localChatSessionSummaryByID(id);
	if(localChatSessionDeleteBlocked(summary)){setMessage(localChatDeleteBlockedMessage(summary),true);return}
	api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats/'+encodeURIComponent(id)).then(function(res){
		var session=res&&res.session?res.session:null;
		if(session){
			if(state.localChats){
				for(var i=0;i<state.localChats.length;i++){if(state.localChats[i]&&state.localChats[i].chat_id===id){state.localChats[i]=Object.assign({},state.localChats[i],session)}}
			}
			if(state.activeLocalChatId===id){state.activeLocalChatSession=Object.assign({},state.activeLocalChatSession||{},session)}
			renderLocalChatContractBanner();renderLocalChatsList();
			if(localChatSessionDeleteBlocked(session)){setMessage(localChatDeleteBlockedMessage(session),true);return null}
		}
		if(!confirm("Are you sure you want to delete this local chat history?"))return null;
		return api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats/'+encodeURIComponent(id), {method:'DELETE'});
	}).then(function(res){
		if(!res)return;
		if(state.activeLocalChatId===id){ state.activeLocalChatId=null; state.activeLocalChatSession=null; }
		applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat delete hydrate error",err);if(!applyLocalChatPayloadFallback(res,true)){var container=document.getElementById('local-chat-messages');if(container)container.innerHTML='<div class="empty">Select or create an inspect chat</div>';setMessage("Inspect chat delete partially failed: "+err.message,true)}});
	}).catch(function(err){hydrateLocalChatError(err,true).finally(function(){setMessage("Delete failed: "+err.message,true)})});
}
function archiveLocalChat(id){
	var summary=localChatSessionSummaryByID(id);
	if(localChatSessionArchived(summary)){setMessage(localChatArchiveBlockedMessage(summary),true);return}
	api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats/'+encodeURIComponent(id)).then(function(res){
		var session=res&&res.session?res.session:null;
		if(session){
			if(state.localChats){
				for(var i=0;i<state.localChats.length;i++){if(state.localChats[i]&&state.localChats[i].chat_id===id){state.localChats[i]=Object.assign({},state.localChats[i],session)}}
			}
			if(state.activeLocalChatId===id){state.activeLocalChatSession=Object.assign({},state.activeLocalChatSession||{},session)}
			renderLocalChatContractBanner();renderLocalChatsList();
			if(!localChatSessionCanArchive(session)){setMessage(localChatArchiveBlockedMessage(session),true);return null}
		}
		if(!confirm("Archive this retained inspect chat for audit retention? It will stay readable but no longer accept new sends."))return null;
		return api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats/'+encodeURIComponent(id)+'/archive', {method:'POST'});
	}).then(function(res){
		if(!res)return;
		applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat archive hydrate error",err);if(!applyLocalChatPayloadFallback(res,true)){setMessage("Inspect chat archive partially failed: "+err.message,true)}})
	}).catch(function(err){hydrateLocalChatError(err,true).finally(function(){setMessage("Archive failed: "+err.message,true)})});
}
function loadLocalChat(id){
state.activeLocalChatId=id;renderLocalChatsList();var container=document.getElementById('local-chat-messages');if(!container)return;container.innerHTML='<div class="empty">Loading...</div>';
api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats/'+encodeURIComponent(id)).then(function(res){
applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){if(!applyLocalChatPayloadFallback(res,true)){container.innerHTML='<div class="empty" style="color:#ff6a6a">Error: '+esc(err.message)+'</div>';setMessage("Inspect chat load partially failed: "+err.message,true)}});
}).catch(function(err){hydrateLocalChatError(err,true).then(function(applied){if(!applied){container.innerHTML='<div class="empty" style="color:#ff6a6a">Error: '+esc(err.message)+'</div>'}})});
}
function createNewLocalChat(){
var contract=state.localChatContract||(state.detail&&state.detail.local_chat_contract)||null;
if(contract&&!localChatContractAvailable(contract)){setMessage("Inspect chat unavailable: "+localChatUnavailableLabel(contract),true);return}
if(!state.selectedAgentId)return;api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats',{method:'POST'}).then(function(res){var session=res&&res.session?res.session:res;if(session&&session.chat_id){state.activeLocalChatId=session.chat_id}applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat create hydrate error",err);if(!applyLocalChatPayloadFallback(res,true)){setMessage("Inspect chat create partially failed: "+err.message,true)}})}).catch(function(err){hydrateLocalChatError(err,false).finally(function(){setMessage(err.message,true)})});
}
function sendLocalChatMessage(content){
var contract=state.localChatContract||(state.detail&&state.detail.local_chat_contract)||null;
var activeSession=activeLocalChatSessionSummary();
if(contract&&!localChatSendReady(contract)){setMessage("Inspect chat unavailable: "+localChatBlockedLabel(contract),true);return}
if(!state.selectedAgentId||state.localChatSending)return;
var inp=document.getElementById('local-chat-input');var btn=document.getElementById('local-chat-send-button');
var allowMutation=document.getElementById("local-chat-allow-mutation");
var allowShell=document.getElementById("local-chat-allow-shell");
var overrideReason=document.getElementById("local-chat-override-reason");
var overridePayload={allow_mutation:!!(allowMutation&&allowMutation.checked),allow_shell:!!(allowShell&&allowShell.checked),override_reason:String(overrideReason&&overrideReason.value||"").trim()};
if(overridePayload.allow_shell&&!overridePayload.allow_mutation){setMessage("Shell inspect also grants bounded mutation; confirm mutation too for this send.",true);if(allowMutation){allowMutation.checked=true;renderLocalChatControlsState()}return}
if(activeSession&&localChatSessionIsHistoryOnly(activeSession)){setMessage(localChatSendBlockedMessage(activeSession),true);return}
if((overridePayload.allow_mutation||overridePayload.allow_shell)&&!overridePayload.override_reason){setMessage("Override reason required for mutation or shell inspect send",true);if(overrideReason){overrideReason.focus()}return}
if(!(overridePayload.allow_mutation||overridePayload.allow_shell)&&overridePayload.override_reason){setMessage("Override reason requires mutation or shell inspect send",true);if(overrideReason){overrideReason.focus()}return}
state.localChatSending=true;inp.disabled=true;btn.disabled=true;
if(!state.activeLocalChatId){
api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats',{method:'POST'}).then(function(res){
var session=res&&res.session?res.session:res;
if(session&&session.chat_id){state.activeLocalChatId=session.chat_id}
applyLocalChatPayload(res).catch(function(err){console.error("localChat create-before-send hydrate error",err);if(!applyLocalChatPayloadFallback(res,false)){setMessage("Inspect chat bootstrap partially failed: "+err.message,true)}});
doSendMessage(content,inp,btn,overridePayload);
}).catch(function(err){hydrateLocalChatError(err,false).finally(function(){setMessage(err.message,true);state.localChatSending=false;inp.disabled=false;btn.disabled=false;});});return;
}
doSendMessage(content,inp,btn,overridePayload);
}
function doSendMessage(content,inp,btn,overridePayload){
var container=document.getElementById('local-chat-messages');var time=new Date().toLocaleTimeString();
var uidiv=document.createElement('div');uidiv.className='msg msg-out';uidiv.innerHTML='<div class="msg-meta"><span>You</span><span>'+esc(time)+'</span></div><div class="msg-bubble">'+esc(content).replace(/\n/g,"<br>")+'</div>';
if(container.querySelector('.empty'))container.innerHTML='';container.appendChild(uidiv);container.scrollTop=container.scrollHeight;
var typing=document.createElement('div');typing.className='msg msg-in';typing.innerHTML='<div class="msg-bubble" style="opacity:0.6">Manager Inspect is thinking...</div>';container.appendChild(typing);container.scrollTop=container.scrollHeight;
api('/api/agents/'+encodeURIComponent(state.selectedAgentId)+'/local_chats/'+encodeURIComponent(state.activeLocalChatId)+'/message',{method:'POST',body:JSON.stringify({content:content,allow_mutation:!!(overridePayload&&overridePayload.allow_mutation),allow_shell:!!(overridePayload&&overridePayload.allow_shell),override_reason:String(overridePayload&&overridePayload.override_reason||"")})}).then(function(res){
var allowMutation=document.getElementById("local-chat-allow-mutation");
var allowShell=document.getElementById("local-chat-allow-shell");
var overrideReason=document.getElementById("local-chat-override-reason");
if(allowMutation){allowMutation.checked=false}
if(allowShell){allowShell.checked=false}
if(overrideReason){overrideReason.value=""}
inp.value='';inp.disabled=false;btn.disabled=false;state.localChatSending=false;inp.focus();applyLocalChatPayload(res).then(function(){renderLocalChatSessionMessages(state.activeLocalChatSession)}).catch(function(err){console.error("localChat send hydrate error",err);if(!applyLocalChatPayloadFallback(res,true)){setMessage("Inspect chat send partially failed: "+err.message,true)}});
}).catch(function(err){typing.remove();uidiv.remove();state.localChatSending=false;hydrateLocalChatError(err,true).then(function(applied){if(!applied&&state.activeLocalChatId){fetchLocalChats();loadLocalChat(state.activeLocalChatId)}}).finally(function(){setMessage(err.message,true);if(inp){inp.focus();}});});
}

window.switchControlsTab=switchControlsTab;
window.createNewLocalChat=createNewLocalChat;
window.loadLocalChat=loadLocalChat;
window.deleteLocalChat=deleteLocalChat;
window.archiveLocalChat=archiveLocalChat;

function startPolling(){if(state.overviewTimer){clearInterval(state.overviewTimer)}if(state.detailTimer){clearInterval(state.detailTimer)}state.overviewTimer=setInterval(function(){refreshOverview(true,shouldRefreshDetail()).catch(function(err){handleDashboardReadError(err,false)})},5000);state.detailTimer=setInterval(function(){if(state.selectedAgentId&&(state.agentPageOpen||state.activeTab!=="overview")){refreshDetail(true).catch(function(err){handleDashboardReadError(err,true)})}},3000)}
async function boot(){attachFormHandlers();bindFSEvents();switchTab(state.activeTab);startPolling();
window.addEventListener("keydown", function(e) {
if(e.key==="Escape"){
var fsModal=document.getElementById("fs-modal-overlay");
if(fsModal&&fsModal.classList.contains("open")){closeFSPicker();return}
var obModal=document.getElementById("onboard-modal");
if(obModal&&obModal.classList.contains("open")){closeOnboardModal();return}
var providerModal=document.getElementById("provider-modal");
if(providerModal&&providerModal.classList.contains("open")){closeProviderModal();return}
}
});
try{await refreshOverview(false,false);setMessage("dashboard ready")}catch(err){await handleDashboardReadError(err,false)}
}

var fsPickerResolve=null,fsCurrentPath="",fsCurrentParent="",fsSelectedEntry="";
function openFSPicker(initial,cb){fsPickerResolve=cb;document.getElementById("fs-modal-overlay").classList.add("open");loadFSDir(initial||"")}
function closeFSPicker(){document.getElementById("fs-modal-overlay").classList.remove("open");fsPickerResolve=null}
function loadFSDir(path){
var list=document.getElementById("fs-list"),input=document.getElementById("fs-path-input");list.innerHTML='<div class="empty">Loading...</div>';
var url='/api/fs/list';if(path)url+='?dir='+encodeURIComponent(path);
api(url).then(function(res){
Promise.resolve(applyOverviewPayload(res,true,true)).catch(function(err){console.error("fs overview hydrate error",err)});
fsCurrentPath=res.path;fsCurrentParent=res.parent;fsSelectedEntry="";window.fsCurrentPath=fsCurrentPath;input.value=fsCurrentPath;var html='';
if(res.entries&&res.entries.length){
res.entries.sort(function(a,b){if(a.is_dir!==b.is_dir)return a.is_dir?-1:1;return a.name.localeCompare(b.name)});
res.entries.forEach(function(e){var icon=e.is_dir?"📁":"📄",opacity=e.is_dir?"1":"0.4";html+='<div class="fs-item" style="opacity:'+opacity+'" data-name="'+esc(e.name)+'" data-dir="'+(e.is_dir?'true':'false')+'"><div class="fs-icon">'+icon+'</div><div class="fs-item-name">'+esc(e.name)+'</div></div>'});
}else{html='<div class="empty">Folder is empty</div>'}
list.innerHTML=html;
Array.prototype.forEach.call(list.querySelectorAll(".fs-item"),function(node){node.addEventListener("click",function(){var isDir=node.getAttribute("data-dir")==="true";var name=node.getAttribute("data-name");if(isDir){loadFSDir(joinFSPath(fsCurrentPath,name))}else{selectFSEntry(name,false)}})});
}).catch(function(err){hydrateDashboardError(err,false).catch(function(hydrationErr){console.error("fs error hydrate error",hydrationErr)}).finally(function(){list.innerHTML='<div class="empty" style="color:#ff6a6a">Error: '+esc(err.message)+'</div>'})})
}
function joinFSPath(base,child){if(!base)return child;if(base.endsWith("\\")||base.endsWith("/"))return base+child;var sep=base.indexOf("\\")!==-1?"\\":"/";return base+sep+child}
function selectFSEntry(name,isDir){if(!isDir)return;var list=document.getElementById("fs-list"),items=list.querySelectorAll(".fs-item");for(var i=0;i<items.length;i++){if(items[i].getAttribute("data-name")===name){items[i].classList.add("selected")}else{items[i].classList.remove("selected")}}fsSelectedEntry=name}
function bindFSEvents(){
var up=document.getElementById("fs-btn-up");if(up)up.addEventListener("click",function(){if(fsCurrentParent)loadFSDir(fsCurrentParent)});
var go=document.getElementById("fs-btn-go");if(go)go.addEventListener("click",function(){loadFSDir(document.getElementById("fs-path-input").value)});
var inp=document.getElementById("fs-path-input");if(inp)inp.addEventListener("keyup",function(e){if(e.key==="Enter")loadFSDir(inp.value)});
var sel=document.getElementById("fs-btn-select");if(sel)sel.addEventListener("click",function(){if(fsPickerResolve){var fp=fsCurrentPath;if(fsSelectedEntry)fp=joinFSPath(fp,fsSelectedEntry);fsPickerResolve(fp);closeFSPicker()}});
var ct=document.getElementById("fs-close-top");if(ct)ct.addEventListener("click",closeFSPicker);
var cb=document.getElementById("fs-close-bottom");if(cb)cb.addEventListener("click",closeFSPicker);
}

window.openFSPicker=openFSPicker;window.closeFSPicker=closeFSPicker;window.selectFSEntry=selectFSEntry;window.loadFSDir=loadFSDir;window.joinFSPath=joinFSPath;

window.addEventListener("load",boot);
`
}
