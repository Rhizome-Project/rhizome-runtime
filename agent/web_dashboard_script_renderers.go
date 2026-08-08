package main

func managerWebDashboardScriptRenderers() string {
	return `
function renderOverview(){
var list=document.getElementById("agent-list"),defaultsSummary=document.getElementById("defaults-summary"),subtitle=document.getElementById("sidebar-subtitle"),overview=state.overview;
if(!overview){list.innerHTML='<div class="empty">overview unavailable</div>';defaultsSummary.textContent="";subtitle.textContent="local manager dashboard";return}
subtitle.textContent="local manager dashboard";
defaultsSummary.innerHTML="provider: <strong>"+esc(fmt(providerTitleFor(overview.defaults.default_provider_id,overview.defaults.llm_backend),"?"))+"</strong> &middot; model: <strong>"+esc(fmt(overview.defaults.model,"?"))+"</strong> &middot; providers: <strong>"+providerRows().length+"</strong><br>owner: <strong>"+esc(fmt(overview.defaults.owner_user_id,"?"))+"</strong>";
if(!overview.agents.length){list.innerHTML='<div class="empty">no agents registered</div>';return}
var html="";overview.agents.forEach(function(row){
var effective=row.effective_identity||{};
var selected=row.record.agent_id===state.selectedAgentId;
var badgeClass=row.process&&row.process.running?"running":(row.process&&row.process.state==="error"?"error":(row.process&&row.process.state==="stopped"?"stopped":"unknown"));
var stateLabel=row.process&&row.process.state?row.process.state:"unknown";
var displayName=effective.display_name||row.record.display_name||row.record.agent_id;
var effectiveAgentId=effective.agent_id||row.record.agent_id;
var roleLabel=effective.role||row.record.role||"agent";
var sourceLabel=(effective.source||"registry").replace(/_/g," ");
var meta=roleLabel+" &middot; "+effectiveAgentId+" &middot; "+sourceLabel;
if(effectiveAgentId!==row.record.agent_id){meta+=" &middot; registry:"+row.record.agent_id}
html+='<div class="agent-item'+(selected?" selected":"")+'" data-agent-id="'+esc(row.record.agent_id)+'">'
+'<div class="agent-item-name"><span>'+esc(displayName)+'</span><span class="badge '+badgeClass+'">'+esc(stateLabel)+'</span></div>'
+'<div class="agent-item-meta">'+esc(meta)+'</div>'
+'</div>';
});
list.innerHTML=html;
Array.prototype.forEach.call(list.querySelectorAll(".agent-item"),function(node){node.addEventListener("click",function(){openAgentPage(node.getAttribute("data-agent-id")||"")})});
}

function renderOverviewTab(){
var statsNode=document.getElementById("overview-stats");
var gridNode=document.getElementById("overview-grid");
var summaryNode=document.getElementById("summary-line");
var overview=state.overview;
if(!overview){if(statsNode){statsNode.innerHTML=""}if(gridNode){gridNode.innerHTML='<div class="empty">loading...</div>'}if(summaryNode){summaryNode.textContent=""}return}
var rows=currentRows();
var running=0,stopped=0,errored=0;
rows.forEach(function(row){if(row.process&&row.process.running){running++}else if(row.process&&row.process.state==="error"){errored++}else{stopped++}});
if(summaryNode){summaryNode.innerHTML=esc(rows.length+" agent"+(rows.length!==1?"s":""))+" &middot; "+esc(fmt(overview.defaults.workspace_id))+" &middot; "+esc(fmt(overview.defaults.host_url))}

if(statsNode){statsNode.innerHTML=
'<div class="stat-card"><div class="stat-value">'+rows.length+'</div><div class="stat-label">Total</div></div>'+
'<div class="stat-card"><div class="stat-value" style="'+(running>0?"color:#57c38c":"")+'">'+running+'</div><div class="stat-label">Running</div></div>'+
'<div class="stat-card"><div class="stat-value" style="'+(stopped>0?"color:#f2b84b":"")+'">'+stopped+'</div><div class="stat-label">Stopped</div></div>'+
'<div class="stat-card"><div class="stat-value" style="'+(errored>0?"color:#ff6a6a":"")+'">'+errored+'</div><div class="stat-label">Errors</div></div>'}

if(!gridNode){return}
if(!rows.length){gridNode.innerHTML='<div class="empty">No agents yet. Click <strong>＋ New Agent</strong> to register your first agent.</div>';return}
var html="";
rows.forEach(function(row){
var r=row.record,p=row.process||{},effective=row.effective_identity||{};
var stClass=p.running?"st-running":(p.state==="error"?"st-error":"st-stopped");
var badgeClass=p.running?"running":(p.state==="error"?"error":(p.state==="stopped"?"stopped":"unknown"));
var stateLabel=p.state||"unknown";
var displayName=effective.display_name||r.display_name||r.agent_id;
var effectiveAgentId=effective.agent_id||r.agent_id;
var roleLabel=effective.role||r.role;
var workspaceLabel=effective.workspace_id||r.workspace_id;
var sourceLabel=(effective.source||"registry").replace(/_/g," ");
var idLine=effectiveAgentId;
if(effectiveAgentId!==r.agent_id){idLine+=' · registry:'+r.agent_id}
idLine+=' · '+sourceLabel;
html+='<div class="agent-card '+stClass+'" data-agent-id="'+esc(r.agent_id)+'">'
+'<div class="agent-card-top"><div><div class="agent-card-name">'+esc(displayName)+'</div><div class="agent-card-id">'+esc(idLine)+'</div></div><span class="badge '+badgeClass+'">'+esc(stateLabel)+'</span></div>'
+'<div class="agent-card-grid">'
+'<div class="agent-card-field"><span class="af-label">Role</span><span class="af-value">'+esc(fmt(roleLabel))+'</span></div>'
+'<div class="agent-card-field"><span class="af-label">Provider</span><span class="af-value">'+esc(fmt(providerTitleFor(r.provider_id,r.llm_backend)))+'</span></div>'
+'<div class="agent-card-field"><span class="af-label">Model</span><span class="af-value">'+esc(fmt(r.model))+'</span></div>'
+'<div class="agent-card-field"><span class="af-label">Workspace</span><span class="af-value">'+esc(fmt(workspaceLabel))+'</span></div>'
+'</div>'
+'<div class="agent-card-field" style="margin-top:.4rem"><span class="af-label">Workdir</span><span class="af-value" title="'+esc(fmt(r.workdir))+'">'+esc(fmt(r.workdir))+'</span></div>'
+'<div class="agent-card-actions">'
+(p.running
?'<button class="btn-warn" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();runProcessAction('stop',(r.agent_id))}) + '>◼ Stop</button>'
:'<button class="btn-primary" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();runProcessAction('start',(r.agent_id))}) + '>▶ Start</button>')
+'<button class="btn-ghost" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();runProcessAction('restart',(r.agent_id))}) + '>↻ Restart</button>'
+'</div></div>';
});
gridNode.innerHTML=html;
Array.prototype.forEach.call(gridNode.querySelectorAll(".agent-card"),function(card){
card.addEventListener("click",function(){openAgentPage(card.getAttribute("data-agent-id")||"")});
});
}

function renderDefaultsForm(){
var form=document.getElementById("defaults-form"),defaults=state.overview?state.overview.defaults:{};if(isDirty("defaults")&&form.children.length){return}
var providerId=firstNonEmptyValue(defaults.default_provider_id),providersError=state.overview&&state.overview.providers_error?state.overview.providers_error:"",modelValue=firstNonEmptyValue(defaults.model_override,!providerId?defaults.model:""),providerHint=providersError?'<div class="field full"><div class="danger-zone">Provider registry error: '+esc(providersError)+'</div></div>':(providerRows().length?'':'<div class="field full"><div class="small">No providers configured yet. Add one in the Providers tab, or leave this empty to keep legacy/default behavior.</div></div>'),html='';
html+='<div class="field full"><label for="default-default_parent_dir">Default Parent Dir</label><input id="default-default_parent_dir" name="default_parent_dir" value="'+esc(fmt(defaults.default_parent_dir,""))+'"><button type="button" class="btn-ghost" style="padding:0.4rem; border:1px solid rgba(255,255,255,0.1); margin-top:0.4rem; width:fit-content" onclick="openFSPicker(document.getElementById(\'default-default_parent_dir\').value, function(p){var it=document.getElementById(\'default-default_parent_dir\');it.value=p;it.dispatchEvent(new Event(\'input\'))})">Browse Folder...</button></div>';
html+='<div class="field"><label for="default-host_url">Host URL</label><input id="default-host_url" name="host_url" value="'+esc(fmt(defaults.host_url,""))+'"></div>';
html+='<div class="field"><label for="default-workspace_id">Workspace ID</label><input id="default-workspace_id" name="workspace_id" value="'+esc(fmt(defaults.workspace_id,""))+'"></div>';
html+='<div class="field"><label for="default-workspace_password">Workspace Password</label><input id="default-workspace_password" name="workspace_password" value="" placeholder="leave blank to keep current"></div>';
html+='<div class="field"><label for="default-owner_user_id">Owner User ID</label><input id="default-owner_user_id" name="owner_user_id" value="'+esc(fmt(defaults.owner_user_id,""))+'"></div>';
html+='<div class="field"><label for="default-default_provider_id">Default Provider</label><select id="default-default_provider_id" name="default_provider_id">'+providerOptionsHTML(providerId,true,"(no explicit provider)")+'</select></div>';
html+=providerModelInputHTML("default-model_override","model_override","Model Override",providerId,modelValue,"leave blank to use provider/default model","field");
html+='<div class="field"><label for="default-role">Role</label><input id="default-role" name="role" value="'+esc(fmt(defaults.role,""))+'"></div>';
html+='<div class="field full"><label for="default-capabilities">Capabilities (CSV)</label><input id="default-capabilities" name="capabilities" value="'+esc(fmt(Array.isArray(defaults.capabilities)?defaults.capabilities.join(", "):defaults.capabilities,""))+'"></div>';
html+=providerHint;
html+='<div class="field full" style="justify-content:flex-start"><button class="btn-primary btn-compact" type="submit">Save Defaults</button></div>';
form.innerHTML=html;
syncProviderFormFields({providerFieldId:"default-default_provider_id",modelFieldId:"default-model_override",fallbackProviderId:defaults.llm_backend,fallbackGroup:defaults.group_id,fallbackModel:defaults.model,allowLegacyFallback:true});
Array.prototype.forEach.call(form.querySelectorAll("input,select"),function(node){var eventName=node.tagName==="SELECT"?"change":"input";node.addEventListener(eventName,function(){markDirty("defaults");if(node.id==="default-default_provider_id"){syncProviderFormFields({providerFieldId:"default-default_provider_id",modelFieldId:"default-model_override",fallbackProviderId:defaults.llm_backend,fallbackGroup:defaults.group_id,fallbackModel:defaults.model,allowLegacyFallback:true})}})});
}

function providerRows(){return state.overview&&Array.isArray(state.overview.providers)?state.overview.providers:[]}
function providerById(providerId){var rows=providerRows();for(var i=0;i<rows.length;i++){if(rows[i]&&rows[i].provider_id===providerId){return rows[i]}}return null}
function providerCatalogRows(){return state.overview&&Array.isArray(state.overview.provider_catalog)?state.overview.provider_catalog:[]}
function providerCatalogByID(optionId){var rows=providerCatalogRows();for(var i=0;i<rows.length;i++){if(rows[i]&&rows[i].id===optionId){return rows[i]}}return null}
function providerCatalogLegacyValue(channelType,driver){channelType=firstNonEmptyValue(channelType);driver=firstNonEmptyValue(driver);return channelType&&driver?"__legacy__:"+channelType+":"+driver:""}
function parseProviderCatalogLegacyValue(value){value=firstNonEmptyValue(value);if(value.indexOf("__legacy__:")!==0){return null}var raw=value.slice("__legacy__:".length),idx=raw.indexOf(":");if(idx<=0){return null}var channelType=raw.slice(0,idx),driver=raw.slice(idx+1);if(!channelType||!driver){return null}return{id:value,label:"Legacy Implementation",description:"This provider uses a legacy implementation that is not in the current supported catalog.",channel_type:channelType,driver:driver,is_legacy:true}}
function providerCatalogOptionForProvider(provider){if(!provider){return null}var channelType=firstNonEmptyValue(provider.channel_type).toLowerCase(),driver=firstNonEmptyValue(provider.driver).toLowerCase(),rows=providerCatalogRows();for(var i=0;i<rows.length;i++){var row=rows[i];if(!row){continue}if(firstNonEmptyValue(row.channel_type).toLowerCase()===channelType&&firstNonEmptyValue(row.driver).toLowerCase()===driver){return row}}return null}
function providerCatalogSelectionMeta(value){return providerCatalogByID(value)||parseProviderCatalogLegacyValue(value)}
function providerCatalogSelection(provider){var matched=providerCatalogOptionForProvider(provider);if(matched){return matched.id}var legacy=providerCatalogLegacyValue(provider&&provider.channel_type,provider&&provider.driver);if(legacy){return legacy}var rows=providerCatalogRows();return rows.length?firstNonEmptyValue(rows[0].id):""}
function providerCatalogOptionLabel(option){if(!option){return""}var details=[];if(firstNonEmptyValue(option.channel_type)){details.push(firstNonEmptyValue(option.channel_type))}if(firstNonEmptyValue(option.driver)){details.push(firstNonEmptyValue(option.driver))}return details.length?firstNonEmptyValue(option.label,option.id)+" ("+details.join(" · ")+")":firstNonEmptyValue(option.label,option.id)}
function providerCatalogOptionsHTML(selectedValue){selectedValue=firstNonEmptyValue(selectedValue);var rows=providerCatalogRows(),html="",seen=false;if(!rows.length){html='<option value="">(no implementations registered)</option>';if(selectedValue){var legacyOnly=providerCatalogSelectionMeta(selectedValue);html+='<option value="'+esc(selectedValue)+'" selected>'+esc(providerCatalogOptionLabel(legacyOnly)||selectedValue)+'</option>'}return html}rows.forEach(function(option){if(!option){return}var optionId=firstNonEmptyValue(option.id);if(!optionId){return}if(optionId===selectedValue){seen=true}html+='<option value="'+esc(optionId)+'"'+(optionId===selectedValue?' selected':'')+'>'+esc(providerCatalogOptionLabel(option))+'</option>'});if(selectedValue&&!seen){var legacy=providerCatalogSelectionMeta(selectedValue);html+='<option value="'+esc(selectedValue)+'" selected>'+esc(providerCatalogOptionLabel(legacy)||selectedValue)+'</option>'}return html}
function splitCSV(value){return String(value||"").split(",").map(function(part){return part.trim()}).filter(Boolean).filter(function(part,index,arr){return arr.indexOf(part)===index})}
function formatProviderHeaderLines(headers){if(!headers){return""}return Object.keys(headers).sort().map(function(key){return key+": "+headers[key]}).join("\n")}
function parseProviderHeaderLines(raw){var lines=String(raw||"").split(/\r?\n/),out={};lines.forEach(function(line){var trimmed=line.trim();if(!trimmed){return}var idx=trimmed.indexOf(":");if(idx<=0){return}var key=trimmed.slice(0,idx).trim(),value=trimmed.slice(idx+1).trim();if(key&&value){out[key]=value}});return out}
function firstNonEmptyValue(){for(var i=0;i<arguments.length;i++){var value=arguments[i];if(value===null||value===undefined){continue}value=String(value).trim();if(value){return value}}return""}
function providerTitleFor(providerId,fallbackValue){providerId=firstNonEmptyValue(providerId);var provider=providerById(providerId);if(provider){return firstNonEmptyValue(provider.provider_id,provider.title,fallbackValue)}return firstNonEmptyValue(providerId,fallbackValue)}
function providerGroupFor(providerId,fallbackValue){providerId=firstNonEmptyValue(providerId);var provider=providerById(providerId);if(provider){return firstNonEmptyValue(provider.provider_id,provider.group_id,fallbackValue)}return firstNonEmptyValue(providerId,fallbackValue)}
function providerIDFor(providerId,fallbackValue){providerId=firstNonEmptyValue(providerId);var provider=providerById(providerId);if(provider){return firstNonEmptyValue(provider.provider_id,fallbackValue)}return firstNonEmptyValue(providerId,fallbackValue)}
function providerModelsFor(providerId,fallbackValue){var provider=providerById(providerId),models=[];function pushModel(value){value=firstNonEmptyValue(value);if(value&&models.indexOf(value)===-1){models.push(value)}}if(provider&&Array.isArray(provider.models)){provider.models.forEach(pushModel)}if(provider){pushModel(provider.default_model)}pushModel(fallbackValue);return models}
function providerOptionLabel(provider){if(!provider){return""}var primary=providerTitleFor(provider.provider_id,provider.provider_id),details=[],label=firstNonEmptyValue(provider.title);if(label&&label!==primary){details.push(label)}details.push(firstNonEmptyValue(provider.channel_type,"?"));details.push(firstNonEmptyValue(provider.driver,"?"));if(provider.enabled===false){details.push("disabled")}return details.length?primary+" ("+details.join(" · ")+")":primary}
function providerOptionsHTML(selectedProviderId,includeEmpty,emptyLabel){selectedProviderId=firstNonEmptyValue(selectedProviderId);var html="",seen=false;if(includeEmpty){html+='<option value="">'+esc(firstNonEmptyValue(emptyLabel,"(none)"))+'</option>'}providerRows().forEach(function(provider){if(!provider){return}var providerId=firstNonEmptyValue(provider.provider_id);if(!providerId){return}if(provider.enabled===false&&providerId!==selectedProviderId){return}if(providerId===selectedProviderId){seen=true}html+='<option value="'+esc(providerId)+'"'+(providerId===selectedProviderId?' selected':'')+'>'+esc(providerOptionLabel(provider))+'</option>'});if(selectedProviderId&&!seen){html+='<option value="'+esc(selectedProviderId)+'" selected>'+esc(providerTitleFor(selectedProviderId,selectedProviderId))+' (missing)</option>'}return html}
function providerModelInputHTML(fieldId,name,label,providerId,value,placeholder,fieldClass){var listId=fieldId+"-models",provider=providerById(providerId),effectivePlaceholder=firstNonEmptyValue(placeholder,provider&&provider.default_model?"leave blank to use "+provider.default_model:"","leave blank to use provider/default model"),html='<div class="'+esc(fieldClass||"field")+'"><label for="'+esc(fieldId)+'">'+esc(label)+'</label><input id="'+esc(fieldId)+'" name="'+esc(name)+'" list="'+esc(listId)+'" value="'+esc(fmt(value,""))+'" placeholder="'+esc(effectivePlaceholder)+'"><datalist id="'+esc(listId)+'">';providerModelsFor(providerId,value).forEach(function(model){html+='<option value="'+esc(model)+'"></option>'});html+='</datalist></div>';return html}
function syncProviderFormFields(config){if(!config){return}var providerNode=document.getElementById(config.providerFieldId);if(!providerNode){return}var providerId=firstNonEmptyValue(providerNode.value),provider=providerById(providerId),groupNode=document.getElementById(config.groupFieldId),providerIdNode=document.getElementById(config.providerIdFieldId),modelNode=document.getElementById(config.modelFieldId);if(groupNode){groupNode.value=providerGroupFor(providerId,config.fallbackGroup)}if(providerIdNode){providerIdNode.value=providerIDFor(providerId,config.fallbackProviderId)}if(modelNode){var listId=modelNode.getAttribute("list"),listNode=listId?document.getElementById(listId):null,placeholder=firstNonEmptyValue(provider&&provider.default_model?"leave blank to use "+provider.default_model:"",config.placeholder,config.allowLegacyFallback?"leave blank to use current/default model":"leave blank to use provider/default model");if(listNode){listNode.innerHTML=providerModelsFor(providerId,modelNode.value||config.fallbackModel).map(function(model){return'<option value="'+esc(model)+'"></option>'}).join("")}modelNode.placeholder=placeholder}}

function providerStatusBadge(provider){return provider&&provider.enabled!==false?'<span class="badge active">enabled</span>':'<span class="badge dormant">disabled</span>'}
function renderProvidersTab(){
var node=document.getElementById("providers-panel"),providers=providerRows();
if(!node){return}
var providersError=state.overview&&state.overview.providers_error?state.overview.providers_error:"";
var html='<div class="overview-header provider-header"><div><h2>Providers</h2><div class="sub">Reusable API keys and bridges for agent runtime selection.</div></div><div class="overview-actions"><button class="btn-add" type="button" onclick="showProviderModal()">+ New Provider</button><span class="sub">'+esc(providers.length+' provider'+(providers.length!==1?'s':''))+'</span></div></div>';
if(providersError){html+='<div class="danger-zone" style="margin-bottom:1rem">Provider registry error: '+esc(providersError)+'</div>'}
  if(!providers.length){html+='<div class="empty">No providers yet. Add one from the registered implementation catalog, such as OpenRouter API, Qwen Code, or the Codex bridge.</div>';node.innerHTML=html;return}
html+='<div class="providers-grid">';
providers.forEach(function(provider){
var headline=firstNonEmptyValue(provider.provider_id,provider.title,"(no provider)");
var subtitleParts=[];
if(provider.title&&provider.title!==headline){subtitleParts.push(provider.title)}
if(!subtitleParts.length){subtitleParts.push(provider.provider_id||headline)}
var connectionLabel=provider.channel_type==="api"?firstNonEmptyValue(provider.api&&provider.api.base_url,"default endpoint"):firstNonEmptyValue(provider.bridge&&provider.bridge.command,provider.bridge&&provider.bridge.executable,"default bridge");
html+='<div class="provider-card" data-provider-id="'+esc(provider.provider_id)+'">'
+'<div class="provider-card-top"><div><div class="provider-card-name">'+esc(headline)+'</div><div class="provider-card-id">'+esc(subtitleParts.join(" \u00b7 "))+'</div></div>'+providerStatusBadge(provider)+'</div>'
+'<div class="provider-card-grid">'
+'<div class="provider-card-field"><span class="af-label">Type</span><span class="af-value">'+esc(fmt(provider.channel_type))+'</span></div>'
+'<div class="provider-card-field"><span class="af-label">Driver</span><span class="af-value">'+esc(fmt(provider.driver))+'</span></div>'
+'<div class="provider-card-field"><span class="af-label">Default Model</span><span class="af-value">'+esc(fmt(provider.default_model))+'</span></div>'
+'<div class="provider-card-field"><span class="af-label">Models</span><span class="af-value">'+esc(fmt((provider.models||[]).length||0))+'</span></div>'
+'</div>'
+'<div class="provider-card-field" style="margin-top:.45rem"><span class="af-label">Connection</span><span class="af-value" title="'+esc(connectionLabel)+'">'+esc(connectionLabel)+'</span></div>'
+'<div class="provider-card-field" style="margin-top:.45rem"><span class="af-label">Provider ID</span><span class="af-value">'+esc(fmt(provider.provider_id))+'</span></div>'
+'<div class="provider-card-actions">'
+'<button class="btn-primary" type="button" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();showProviderModal((provider.provider_id))}) + '>Edit</button>'
+'<button class="btn-warn" type="button" ' + dashboardAction(function(dashboardEvent){dashboardEvent.stopPropagation();deleteProvider((provider.provider_id))}) + '>Delete</button>'
+'</div></div>';
});
html+='</div>';
node.innerHTML=html;
Array.prototype.forEach.call(node.querySelectorAll(".provider-card"),function(card){
card.addEventListener("click",function(){showProviderModal(card.getAttribute("data-provider-id")||"")});
});
}

function renderProviderModal(){
var modal=document.getElementById("provider-modal"),titleNode=document.getElementById("provider-modal-title"),form=document.getElementById("provider-form");
if(!modal||!titleNode||!form){return}
var isOpen=modal.classList.contains("open");
if(!isOpen&&!state.providerEditingId&&!form.children.length){return}
var selectedProvider=providerById(state.providerEditingId);
if(state.providerEditingId&&!selectedProvider){
titleNode.textContent="Provider Missing";
form.innerHTML='<div class="field full"><div class="danger-zone">Provider '+esc(state.providerEditingId)+' no longer exists. Close this dialog and pick a different provider from the list.</div></div><div class="field full provider-form-actions"><button type="button" class="btn-primary btn-compact" onclick="closeProviderModal(true)">Close</button></div>';
return
}
if(isDirty("providers")&&isOpen&&form.children.length){return}
  var defaultImplementation=providerCatalogRows()[0]||null;
  var editing=selectedProvider||{channel_type:defaultImplementation?defaultImplementation.channel_type:"bridge",driver:defaultImplementation?defaultImplementation.driver:"codex",enabled:true,api:{},bridge:{use_managed_home:true}};
  var isEditing=!!(state.providerEditingId&&editing&&editing.provider_id);
  var implementationValue=providerCatalogSelection(editing),implementationMeta=providerCatalogSelectionMeta(implementationValue),providerIDPlaceholder=firstNonEmptyValue(implementationMeta&&implementationMeta.suggested_provider_id,"codex-bridge","provider-main"),titlePlaceholder=firstNonEmptyValue(implementationMeta&&implementationMeta.suggested_title,"secondary label for humans"),implementationDescription="Pick one of the supported provider implementations.";
  if(implementationMeta){
  implementationDescription=firstNonEmptyValue(implementationMeta.description,"");
  implementationDescription+=(implementationDescription?" ":"")+"Type: "+firstNonEmptyValue(implementationMeta.channel_type,"?")+" · Driver: "+firstNonEmptyValue(implementationMeta.driver,"?");
  if(implementationMeta.is_legacy){implementationDescription="Legacy implementation. "+implementationDescription}
  }
  titleNode.textContent=isEditing?"Edit Provider":"Add Provider";
  var html=''
  +'<div class="field"><label for="provider-id">Provider ID</label><input id="provider-id" name="provider_id" value="'+esc(editing.provider_id||'')+'" '+(isEditing?'readonly':'')+' placeholder="'+esc(providerIDPlaceholder)+'"></div>'
  +'<div class="field"><label for="provider-title">Display Label (optional)</label><input id="provider-title" name="title" value="'+esc(editing.title||'')+'" placeholder="'+esc(titlePlaceholder)+'"></div>'
  +'<div class="field full"><label for="provider-implementation">Implementation</label><select id="provider-implementation" name="implementation_id">'+providerCatalogOptionsHTML(implementationValue)+'</select><div class="small" id="provider-implementation-description">'+esc(implementationDescription)+'</div></div>'
  +'<div class="field"><label for="provider-default-model">Default Model</label><input id="provider-default-model" name="default_model" value="'+esc(editing.default_model||'')+'" placeholder="gpt-5.4"></div>'
  +'<div class="field full"><label for="provider-models">Models (CSV)</label><input id="provider-models" name="models" value="'+esc((editing.models||[]).join(", "))+'" placeholder="gpt-5.4, gpt-5.4-mini"></div>'
+'<div class="field" id="provider-api-base-wrap"><label for="provider-api-base">API Base URL</label><input id="provider-api-base" name="api_base_url" value="'+esc(editing.api&&editing.api.base_url||'')+'" placeholder="https://api.example.com/v1"></div>'
+'<div class="field full" id="provider-api-headers-wrap"><label for="provider-api-headers">Public Headers</label><textarea id="provider-api-headers" name="api_public_headers" rows="3" placeholder="X-Env: prod\nX-Tenant: main">'+esc(formatProviderHeaderLines(editing.api&&editing.api.public_headers))+'</textarea></div>'
+'<div class="field" id="provider-bridge-executable-wrap"><label for="provider-bridge-executable">Bridge Executable</label><input id="provider-bridge-executable" name="bridge_executable" value="'+esc(editing.bridge&&editing.bridge.executable||'')+'" placeholder="codex"></div>'
+'<div class="field" id="provider-bridge-command-wrap"><label for="provider-bridge-command">Bridge Command</label><input id="provider-bridge-command" name="bridge_command" value="'+esc(editing.bridge&&editing.bridge.command||'')+'" placeholder="codex"></div>'
+'<div class="field full provider-toggle-row">'
+'<label class="provider-toggle"><input type="checkbox" name="bridge_use_managed_home" '+((editing.bridge&&editing.bridge.use_managed_home)!==false?'checked':'')+'><span class="provider-toggle-copy"><span class="provider-toggle-title">Use Managed Home</span><span class="provider-toggle-sub">Isolate CLI home/config for this provider.</span></span></label>'
+'<label class="provider-toggle"><input type="checkbox" name="enabled" '+(editing.enabled!==false?'checked':'')+'><span class="provider-toggle-copy"><span class="provider-toggle-title">Enabled</span><span class="provider-toggle-sub">Show in pickers and allow new bindings.</span></span></label>'
+'</div>'
+'<div class="field full"><div class="small">Provider ID is the stable runtime handle used by agents and defaults. Display label is optional and only helps humans scan the list faster.</div></div>'
+'<div class="field full provider-form-actions"><button class="btn-primary btn-compact" type="submit">'+(isEditing?'Save Provider':'Add Provider')+'</button><button type="button" class="btn-ghost btn-compact" onclick="closeProviderModal()">'+(isEditing?'Cancel':'Close')+'</button></div>';
  form.innerHTML=html;
  form.onsubmit=submitProviderForm;
  Array.prototype.forEach.call(form.querySelectorAll("input,textarea,select"),function(node){
  var eventName=node.tagName==="SELECT"?"change":"input";
  node.addEventListener(eventName,function(){markDirty("providers");if(node.name==="implementation_id"){toggleProviderChannelFields()}});
  });
  toggleProviderChannelFields();
  }

function toggleProviderChannelFields(){
  var implementationNode=document.getElementById("provider-implementation"),implementation=implementationNode?providerCatalogSelectionMeta(implementationNode.value):null,channel=firstNonEmptyValue(implementation&&implementation.channel_type,"bridge"),isAPI=channel!=="bridge",descriptionNode=document.getElementById("provider-implementation-description"),providerIDNode=document.getElementById("provider-id"),providerTitleNode=document.getElementById("provider-title");
  if(descriptionNode){
  var description="Pick one of the supported provider implementations.";
  if(implementation){
  description=firstNonEmptyValue(implementation.description,"");
  description+=(description?" ":"")+"Type: "+firstNonEmptyValue(implementation.channel_type,"?")+" · Driver: "+firstNonEmptyValue(implementation.driver,"?");
  if(implementation.is_legacy){description="Legacy implementation. "+description}
  }
  descriptionNode.textContent=description
  }
  if(providerIDNode&&!providerIDNode.value){providerIDNode.placeholder=firstNonEmptyValue(implementation&&implementation.suggested_provider_id,"codex-bridge","provider-main")}
  if(providerTitleNode&&!providerTitleNode.value){providerTitleNode.placeholder=firstNonEmptyValue(implementation&&implementation.suggested_title,"secondary label for humans")}
  ["provider-api-base-wrap","provider-api-headers-wrap"].forEach(function(id){var node=document.getElementById(id);if(node){node.style.display=isAPI?"":"none";Array.prototype.forEach.call(node.querySelectorAll("input,textarea,select"),function(field){field.disabled=!isAPI})}});
  ["provider-bridge-executable-wrap","provider-bridge-command-wrap"].forEach(function(id){var node=document.getElementById(id);if(node){node.style.display=isAPI?"none":"";Array.prototype.forEach.call(node.querySelectorAll("input,textarea,select"),function(field){field.disabled=isAPI})}});
  }

async function submitProviderForm(event){
  event.preventDefault();
  var form=event.target,fd=new FormData(form),implementationValue=firstNonEmptyValue(fd.get("implementation_id")),implementation=providerCatalogSelectionMeta(implementationValue);
  if(!implementation){setMessage("choose a supported implementation",true);return}
  var channel=firstNonEmptyValue(implementation.channel_type),driver=firstNonEmptyValue(implementation.driver);
  if(!channel||!driver){setMessage("selected implementation is incomplete",true);return}
  var payload={
  provider_id:String(fd.get("provider_id")||"").trim(),
  title:String(fd.get("title")||"").trim(),
  channel_type:channel,
  driver:driver,
  group_id:String(fd.get("provider_id")||"").trim(),
  default_model:String(fd.get("default_model")||"").trim(),
  models:splitCSV(fd.get("models")||""),
enabled:fd.get("enabled")==="on",
api:channel==="api"?{base_url:String(fd.get("api_base_url")||"").trim(),public_headers:parseProviderHeaderLines(fd.get("api_public_headers")||"")}:{},
bridge:channel==="bridge"?{executable:String(fd.get("bridge_executable")||"").trim(),command:String(fd.get("bridge_command")||"").trim(),use_managed_home:fd.get("bridge_use_managed_home")==="on"}:{}
};
try{
var result=await api("/api/providers",{method:"POST",body:JSON.stringify(payload)});
state.dirty.providers=false;
state.providerEditingId="";
closeProviderModal(true);
var applied=await applyOverviewPayload(result,true,false);
setMessage(result&&result.message?result.message:"saved provider");
if(!applied){await refreshOverview(true)}
}catch(err){await hydrateDashboardError(err,false);setMessage(err.message,true)}
}

async function deleteProvider(providerId){
if(!providerId){return}
if(!window.confirm("Remove provider "+providerId+"?")){return}
try{
var result=await api("/api/providers/"+encodeURIComponent(providerId),{method:"DELETE"});
if(state.providerEditingId===providerId){closeProviderModal(true)}
state.dirty.providers=false;
var applied=await applyOverviewPayload(result,true,false);
setMessage(result&&result.message?result.message:"removed provider");
if(!applied){await refreshOverview(true)}
}catch(err){await hydrateDashboardError(err,false);setMessage(err.message,true)}
}
window.deleteProvider=deleteProvider;

function renderOnboardForm(){
var form=document.getElementById("onboard-form"),defaults=state.overview?state.overview.defaults:{},cd=state.overview?state.overview.create_default:{};if(isDirty("onboard")&&form.children.length){return}
var providerId=firstNonEmptyValue(defaults.default_provider_id),providersError=state.overview&&state.overview.providers_error?state.overview.providers_error:"",modelValue=firstNonEmptyValue(defaults.model_override,!providerId?defaults.model:""),providerHint=providersError?'<div class="field full"><div class="danger-zone">Provider registry error: '+esc(providersError)+'</div></div>':(providerRows().length?'':'<div class="field full"><div class="small">No providers configured yet. New agents will fall back to existing defaults until you add one.</div></div>'),html="";
html+='<div class="field"><label for="onboard-agent_id">Agent ID</label><input id="onboard-agent_id" name="agent_id" value="'+esc(fmt(cd.folder_name,""))+'"></div>';
html+='<div class="field"><label for="onboard-folder_name">Folder Name</label><input id="onboard-folder_name" name="folder_name" value="'+esc(fmt(cd.folder_name,""))+'" placeholder="auto-synced from Agent ID"></div>';
html+='<div class="field"><label for="onboard-display_name">Display Name</label><input id="onboard-display_name" name="display_name" value="'+esc(fmt(cd.folder_name,""))+'"></div>';
html+='<div class="field full"><label for="onboard-parent_dir">Parent Dir</label><input id="onboard-parent_dir" name="parent_dir" value="'+esc(fmt(cd.parent_dir||defaults.default_parent_dir,""))+'"><button type="button" class="btn-ghost" style="padding:0.4rem; border:1px solid rgba(255,255,255,0.1); margin-top:0.4rem; width:fit-content" onclick="openFSPicker(document.getElementById(\'onboard-parent_dir\').value, function(p){var it=document.getElementById(\'onboard-parent_dir\');it.value=p;it.dispatchEvent(new Event(\'input\'))})">Browse Folder...</button></div>';
html+='<div class="field"><label for="onboard-owner_user_id">Owner</label><input id="onboard-owner_user_id" name="owner_user_id" value="'+esc(fmt(defaults.owner_user_id,""))+'"></div>';
html+='<div class="field"><label for="onboard-provider_id">Provider</label><select id="onboard-provider_id" name="provider_id">'+providerOptionsHTML(providerId,true,"(use defaults / legacy backend)")+'</select></div>';
html+='<div class="field"><label for="onboard-host_url">Host URL</label><input id="onboard-host_url" name="host_url" value="'+esc(fmt(defaults.host_url,""))+'"></div>';
html+='<div class="field"><label for="onboard-workspace_id">Workspace ID</label><input id="onboard-workspace_id" name="workspace_id" value="'+esc(fmt(defaults.workspace_id,""))+'"></div>';
html+='<div class="field"><label for="onboard-workspace_password">Workspace Password</label><input id="onboard-workspace_password" name="workspace_password" value="" placeholder="leave blank for current default"></div>';
html+='<div class="field"><label for="onboard-role">Role</label><input id="onboard-role" name="role" value="'+esc(fmt(defaults.role||"generalist",""))+'"></div>';
html+='<div class="field"><label for="onboard-primary_specialization">Primary Spec</label><input id="onboard-primary_specialization" name="primary_specialization" value="'+esc(fmt(defaults.role||"generalist",""))+'"></div>';
html+='<div class="field"><label for="onboard-secondary_specializations">Secondary Specs</label><input id="onboard-secondary_specializations" name="secondary_specializations" value=""></div>';
html+='<div class="field full"><label for="onboard-domain_scope">Domain Scope</label><input id="onboard-domain_scope" name="domain_scope" value=""></div>';
html+='<div class="field full"><label for="onboard-mission">Mission</label><input id="onboard-mission" name="mission" value=""></div>';
html+=providerModelInputHTML("onboard-model","model","Model",providerId,modelValue,"leave blank to use provider/default model","field");
html+=providerHint;
html+='<div class="field full"><div class="workdir-preview mono" id="onboard-preview">'+esc(fmt(cd.workdir,""))+'</div></div><div class="field full" style="justify-content:flex-start"><button class="btn-primary btn-compact" type="submit">Register Agent</button></div>';form.innerHTML=html;updateOnboardPreview();
["onboard-parent_dir","onboard-folder_name"].forEach(function(id){var node=document.getElementById(id);if(node){node.addEventListener("input",updateOnboardPreview)}});
setupOnboardSync();
syncProviderFormFields({providerFieldId:"onboard-provider_id",modelFieldId:"onboard-model",fallbackProviderId:defaults.llm_backend,fallbackGroup:defaults.group_id,fallbackModel:defaults.model,allowLegacyFallback:true});
Array.prototype.forEach.call(form.querySelectorAll("input,select"),function(node){var eventName=node.tagName==="SELECT"?"change":"input";node.addEventListener(eventName,function(){markDirty("onboard");if(node.id==="onboard-provider_id"){syncProviderFormFields({providerFieldId:"onboard-provider_id",modelFieldId:"onboard-model",fallbackProviderId:defaults.llm_backend,fallbackGroup:defaults.group_id,fallbackModel:defaults.model,allowLegacyFallback:true})}})});
}

function renderDetail(){renderAgentPanelInfo();renderControlPanel();renderInboxPanel();renderActivityPanel();renderRuntimePanel();renderCatalogPanel();renderLogsPanel();renderAgentPanelSettings()}

function renderAgentPanelInfo(){
var node=document.getElementById("agent-panel-info"),row=selectedRow();
if(!row){node.innerHTML='<div class="empty">select an agent from the sidebar</div>';return}
if(isDirty("agent")&&node.children.length){return}
var detail=state.detail,process=detail?detail.process:row.process,record=detail?detail.record:row.record,effective=detail&&detail.effective_identity?detail.effective_identity:{},runtime=detail&&detail.local_runtime?detail.local_runtime:{},profile=detail&&detail.profile?detail.profile:{};
	var html='<div class="agent-detail-actions">'+(process&&process.running?'<button data-process="stop" class="btn-warn">◼ Stop</button>':'<button data-process="start" class="btn-primary">▶ Start</button>')+'<button data-process="restart" class="btn-ghost">↻ Restart</button><button data-refresh="detail" class="btn-ghost">Refresh</button></div>';
	html+='<div class="section-gap"><section class="card"><h2>Confirmed Executor Identity</h2><div class="small">Current executor truth for runtime and live control. Source: '+esc(fmt(effective.source,"bootstrap_local_runtime"))+'</div><div class="kvs">'
	+kv("Agent ID",effective.agent_id||record.agent_id)+kv("Display Name",effective.display_name||record.display_name)+kv("Workspace",effective.workspace_id||record.workspace_id)
	+kv("Role",effective.role||record.role)+kv("Owner",effective.owner_user_id||record.owner_user_id)+kv("Protocol",effective.protocol_version)
	+'</div></section></div>';
	html+='<div class="section-gap"><div class="kvs">'
	+kv("Provider",providerTitleFor(firstNonEmptyValue(runtime.provider_id,record.provider_id),record.llm_backend))+kv("Model",record.model)+kv("Host",record.host_url)
	+kv("PID",process&&process.pid?process.pid:null)+kv("Workdir",record.workdir)+kv("Bootstrap Display",runtime.display_name||record.display_name)
	+kv("Bootstrap Workspace",runtime.workspace_id||record.workspace_id)+kv("Profile Role",profile.role||record.role)+kv("Provider ID",providerIDFor(firstNonEmptyValue(runtime.provider_id,record.provider_id),record.llm_backend))
	+'</div><div class="small" style="margin-top:.6rem">Bootstrap and profile fields can diverge from confirmed executor identity until a fresh registration updates them.</div></div>';

	html+='<div class="section-gap" id="centralization-warning" style="display:none"><div class="danger-zone">⚠️ <strong>Centralization Penalty Warning:</strong> Agent is approaching the stewardship limit (3+ clusters). Coalition creation/stewardship may be blocked.</div></div>';

	html+='<div class="dual-grid" style="margin-top:2.5rem;"><section class="card"><h2>Active Coalitions</h2><div class="empty">No active coalitions (tools initialized)</div></section><section class="card"><h2>Reviewer Mesh Routes</h2><div class="empty">No active reviewer routing requests</div></section></div>';

	node.innerHTML=html;bindAgentPanel();

}

function renderAgentPanelSettings(){
	var node=document.getElementById("settings-panel"),row=selectedRow();
	if(!row){node.innerHTML='<div class="empty">select an agent from the sidebar</div>';return}
	if(isDirty("agent")&&node.children.length){return}
	var detail=state.detail,process=detail?detail.process:row.process,record=detail?detail.record:row.record,effective=detail&&detail.effective_identity?detail.effective_identity:{};

	// Create local runtime config form
	var localProfile = detail&&detail.local_runtime?detail.local_runtime:{};
	var runtimeProviderId=firstNonEmptyValue(localProfile.provider_id,record.provider_id,state.overview&&state.overview.defaults?state.overview.defaults.default_provider_id:"");
	var runtimeModelValue=firstNonEmptyValue(localProfile.model_override,!runtimeProviderId?localProfile.model:"",!runtimeProviderId?record.model:"");
	var providersError=state.overview&&state.overview.providers_error?state.overview.providers_error:"";
	var providerHint=providersError?'<div class="field full"><div class="danger-zone">Provider registry error: '+esc(providersError)+'</div></div>':(providerRows().length?'':'<div class="field full"><div class="small">No providers configured yet. This agent will keep using its current legacy backend until you add one.</div></div>');
	var identityHtml='<div class="section-gap"><section class="card"><h2>Confirmed Executor Identity</h2><div class="small">These values drive runtime and live control. The editable forms below change bootstrap and profile fields only until a new registration confirms different identity.</div><div class="kvs">'
	+kv("Source",effective.source)+kv("Agent ID",effective.agent_id||record.agent_id)+kv("Display Name",effective.display_name||record.display_name)
	+kv("Workspace",effective.workspace_id||record.workspace_id)+kv("Role",effective.role||record.role)+kv("Owner",effective.owner_user_id||record.owner_user_id)
	+'</div></section></div>';
	var rtHtml='<div class="section-gap"><section class="card"><h2>Local Runtime Config (Bootstrap)</h2><form id="edit-runtime-form" class="field-grid">';
	rtHtml+='<div class="field"><label for="edit-runtime-provider">Provider</label><select id="edit-runtime-provider" name="provider_id">'+providerOptionsHTML(runtimeProviderId,true,"(use current / legacy backend)")+'</select></div>';
	rtHtml+=providerModelInputHTML("edit-runtime-model","model","Model",runtimeProviderId,runtimeModelValue,"leave blank to use provider/default model","field");
	rtHtml+='<div class="field full"><div class="small">Provider secrets are not editable here yet. This screen only binds the agent to a saved provider and optional model override.</div></div>';
	rtHtml+='<div class="field"><label>Planner Interval (sec)</label><input type="number" name="planner_sec" value="'+(localProfile.planner_sec||0)+'" placeholder="0"></div>';
	rtHtml+='<div class="field"><label>Watchdog Interval (sec)</label><input type="number" name="watchdog_sec" value="'+(localProfile.watchdog_sec||0)+'" placeholder="0"></div>';
	rtHtml+=providerHint;
	rtHtml+='<div class="field full" style="justify-content:flex-start"><button class="btn-primary btn-compact" type="submit">Save Runtime Config</button></div>';
	rtHtml+='</form></section></div>';

	var regHtml='<div class="section-gap"><section class="card"><h2>Bootstrap Profile & Registry</h2><div class="small">Edits here update local bootstrap and profile metadata. They do not overwrite confirmed executor identity until a fresh registration succeeds.</div><form id="edit-agent-form" class="field-grid">';
	var prof=detail&&detail.profile?detail.profile:{};
	regHtml+='<div class="field"><label for="edit-display-name">Bootstrap Display Name</label><input id="edit-display-name" name="display_name" value="'+esc(fmt(localProfile.display_name||record.display_name,""))+'"></div>';
	regHtml+='<div class="field"><label for="edit-provider-id">Provider ID</label><input id="edit-provider-id" value="'+esc(fmt(providerIDFor(runtimeProviderId,record.llm_backend),""))+'" readonly placeholder="selected provider"></div>';
	regHtml+='<div class="field"><label for="edit-role">Profile / Registry Role</label><input id="edit-role" name="role" value="'+esc(fmt(prof.role||record.role,""))+'" placeholder="generalist"></div>';
	regHtml+='<div class="field"><label for="edit-tags">Capabilities (Tags)</label><input id="edit-tags" name="tags" value="'+esc(fmt(prof.secondary_specializations?(prof.secondary_specializations.join(', ')):'', ""))+'" placeholder="frontend, rust, ui..."></div>';
	regHtml+='<div class="field full"><label for="edit-soul-prompt">Local Text Prompt (SOUL.md / Mission)</label><textarea id="edit-soul-prompt" name="soul_prompt" rows="3" placeholder="Enter custom agent behavior constraints...">'+esc(fmt(prof.mission,""))+'</textarea></div>';
	regHtml+='<div class="field full"><label for="edit-workdir">Workdir</label><div style="display:flex;gap:0.5rem"><input id="edit-workdir" name="workdir" value="'+esc(fmt(record.workdir,""))+'" style="flex:1"><button type="button" class="btn-ghost" style="padding:0.4rem; border:1px solid rgba(255,255,255,0.1)" onclick="openFSPicker(document.getElementById(\'edit-workdir\').value, function(p){var it=document.getElementById(\'edit-workdir\');it.value=p;it.dispatchEvent(new Event(\'input\'))})">Browse Folder...</button></div></div>';
	regHtml+='<div class="field full" style="justify-content:flex-start"><button class="btn-primary btn-compact" type="submit">Save Profile & Registry</button></div>';
	regHtml+='</form></section></div>';

	var dangerHtml='<div class="danger-zone" style="margin-top:2rem"><button id="remove-agent" class="btn-danger">Remove From Registry</button><div class="small" style="margin-top:.3rem">allowed only when the process is stopped</div></div>';

	node.innerHTML=identityHtml + rtHtml + regHtml + dangerHtml;

	// Bind Settings Tab Forms
	var rtForm=document.getElementById("edit-runtime-form");
	if(rtForm){
		Array.prototype.forEach.call(rtForm.querySelectorAll("input,select"),function(field){
			var eventName=field.tagName==="SELECT"?"change":"input";
			field.addEventListener(eventName,function(){
				markDirty("agent");
				if(field.id==="edit-runtime-provider"){
					syncProviderFormFields({providerFieldId:"edit-runtime-provider",modelFieldId:"edit-runtime-model",fallbackProviderId:record.llm_backend,fallbackGroup:firstNonEmptyValue(localProfile.group_id,record.group_id),fallbackModel:firstNonEmptyValue(localProfile.model,record.model),allowLegacyFallback:true});
					var registryProviderField=document.getElementById("edit-provider-id");
					if(registryProviderField){registryProviderField.value=providerIDFor(field.value,record.llm_backend)}
				}
			});
		});
		syncProviderFormFields({providerFieldId:"edit-runtime-provider",modelFieldId:"edit-runtime-model",fallbackProviderId:record.llm_backend,fallbackGroup:firstNonEmptyValue(localProfile.group_id,record.group_id),fallbackModel:firstNonEmptyValue(localProfile.model,record.model),allowLegacyFallback:true});
		var registryProviderField=document.getElementById("edit-provider-id");
		if(registryProviderField){registryProviderField.value=providerIDFor(runtimeProviderId,record.llm_backend)}
		rtForm.onsubmit=async function(e){
			e.preventDefault();
			try{
				var res=await api('/api/agents/'+encodeURIComponent(record.agent_id)+'/settings',{method:'POST',body:JSON.stringify(Object.fromEntries(new FormData(e.target)))});
				markDirty("agent",false);
				var appliedOverview=await applyOverviewPayload(res,true,true);
				var appliedDetail=applyDetailPayload(res);
				setMessage(res.message);
				if(!appliedOverview){await refreshOverview(true)}else if(shouldRefreshDetail()&&!appliedDetail){await refreshDetail(true)}
			}catch(err){await hydrateDashboardError(err,true);setMessage(err.message,true)}
		};
	}
	bindAgentPanel(); // Rebind Reg Edit form since it uses #edit-agent-form which bindAgentPanel selects
}

function renderControlPanel(){
var node=document.getElementById("control-panel");if(!state.selectedAgentId){node.innerHTML='<div class="empty">select an agent to send model and runtime requests</div>';return}if((isDirty("ask")||isDirty("task")||isDirty("tension"))&&node.children.length){return}
var detail=state.detail||{},process=detail.process||null;
var isRunning=process&&process.running;
var chatInp=document.getElementById("local-chat-input"),chatBtn=document.querySelector("#local-chat-form button");
if(chatInp&&chatBtn&&!state.localChatSending){chatInp.disabled=false;chatBtn.disabled=false;chatInp.placeholder="Send a message...";}

var tasks=detail.catalog&&Array.isArray(detail.catalog.tasks)?detail.catalog.tasks:[],tensions=detail.catalog&&Array.isArray(detail.catalog.tensions)?detail.catalog.tensions:[],taskOpts='<option value="">select task</option>',tensionOpts='<option value="">select tension</option>';
tasks.forEach(function(t){taskOpts+='<option value="'+esc(t.task_id)+'">'+esc(t.task_id+" \u2014 "+(t.title||t.status||"task"))+"</option>"});tensions.forEach(function(t){tensionOpts+='<option value="'+esc(t.tension_id)+'">'+esc(t.tension_id+" \u2014 "+(t.title||t.tension_type||"tension"))+"</option>"});
var html='<div class="toolbar"><button id="runtime-status" class="btn-ghost">runtime.status</button><button id="runtime-refresh" class="btn-ghost">runtime.refresh</button><button id="runtime-pause" class="btn-warn">pause</button><button id="runtime-resume" class="btn-primary">resume</button></div>';
html+='<div class="split"><form id="ask-form" class="stack"><div class="field full"><label for="ask-prompt">Model Ask</label><textarea id="ask-prompt" placeholder="Ask the live agent anything."></textarea></div><div><button class="btn-primary btn-compact" type="submit">Send Prompt</button></div></form>';
html+='<form id="task-form" class="stack"><div class="field"><label for="switch-task-id">Switch Task</label><select id="switch-task-id">'+taskOpts+'</select></div><div class="field"><label for="switch-session-id">Session ID (optional)</label><input id="switch-session-id" placeholder="session-123"></div><div class="field"><label for="switch-task-reason">Reason</label><input id="switch-task-reason" value="web dashboard task switch"></div><div><button class="btn-primary btn-compact" type="submit">Switch Task</button></div></form></div>';
html+='<div class="section-gap"><h3>Tension Switch</h3></div><form id="tension-form" class="field-grid"><div class="field"><label for="switch-tension-id">Tension</label><select id="switch-tension-id">'+tensionOpts+'</select></div><div class="field"><label for="switch-tension-action">Action</label><select id="switch-tension-action"><option value="focus">focus</option><option value="detach">detach</option><option value="lifecycle">lifecycle</option></select></div><div class="field"><label for="switch-tension-role">Role</label><input id="switch-tension-role" placeholder="reviewer"></div><div class="field"><label for="switch-tension-state">Lifecycle State</label><select id="switch-tension-state"><option value="ACTIVE">ACTIVE</option><option value="IN_REVIEW">IN_REVIEW</option><option value="RESOLVED">RESOLVED</option></select></div><div class="field full"><label for="switch-tension-reason">Reason</label><input id="switch-tension-reason" value="web dashboard tension switch"></div><div class="field full" style="justify-content:flex-start"><button class="btn-primary btn-compact" type="submit">Switch Tension</button></div></form>';
html+='<div class="section-gap"><h3>Last Response</h3><pre id="control-response">'+esc(getLastResponseText())+'</pre></div>';node.innerHTML=html;bindControlPanel();
}

function renderInboxPanel(){
var node=document.getElementById("inbox-panel");if(!state.inbox){node.innerHTML='<div class="empty">loading inbox...</div>';return}
var oldList=node.querySelector(".chat-list"),oldMsgs=node.querySelector(".chat-messages");
var listScroll=oldList?oldList.scrollTop:0;
var msgsScroll=oldMsgs?oldMsgs.scrollTop:-1,msgsHeight=oldMsgs?oldMsgs.scrollHeight:-1,msgsClient=oldMsgs?oldMsgs.clientHeight:-1;
var msgsAtBottom=msgsScroll===-1||(msgsScroll+msgsClient>=msgsHeight-10);

var msgs=state.inbox.messages||[],agentMap=state.inbox.agentMap||{},activeChannel=state.inbox.channel||"";
var chats={},bcasts=[];

msgs.forEach(function(m){
    if(m.metadata_json){try{var meta=JSON.parse(m.metadata_json);if(meta.author_user_id&&(m.from_agent_id==="telegram-bridge"||m.from_agent_id==="human-input"||m.channel==="human-input")){m.from_agent_id=meta.author_user_id;if(meta.author_name)agentMap[meta.author_user_id]=meta.author_name;}}catch(e){}}

    var isMe=state.selectedAgentId;
    var isTargetEmpty=!m.to_agent_id||m.to_agent_id===""||m.to_agent_id==="workspace"||m.to_agent_id==="all";

    if(!isTargetEmpty){
        if(m.from_agent_id!==isMe&&m.to_agent_id!==isMe)return;
        var partner=(m.from_agent_id===isMe)?m.to_agent_id:m.from_agent_id;
        if(!chats[partner])chats[partner]=[];
        chats[partner].push(m);
    }else{
        if(m.channel==="task-suggestion"||m.channel==="news"||m.channel==="heartbeat")return;
        bcasts.push(m);
    }
});

var html='<div class="inbox-sidebar"><div class="chat-list">';
html+='<div class="chat-item broadcast '+(activeChannel===""||activeChannel==="workspace.broadcast"?"active":"")+'" onclick="setInboxChannel(\'workspace.broadcast\')"><div class="chat-item-avatar">B</div><div class="chat-item-info"><div class="chat-item-top"><div class="chat-item-name">Broadcasts</div></div><div class="chat-item-preview">System wide messages</div></div></div>';
Object.keys(chats).forEach(function(partner){
    var displayName=agentMap[partner]||partner;
    var initial=displayName.substring(0,2).toUpperCase();
    var lastMsg=chats[partner][0];
    html+='<div class="chat-item '+(activeChannel===partner?"active":"")+'" ' + dashboardAction(function(dashboardEvent){setInboxChannel((partner))}) + '><div class="chat-item-avatar">'+esc(initial)+'</div><div class="chat-item-info"><div class="chat-item-top"><div class="chat-item-name">'+esc(displayName)+'</div></div><div class="chat-item-preview">'+(lastMsg?esc(lastMsg.content):"")+'</div></div></div>';
});
html+='</div></div><div class="inbox-main"><div class="chat-header"><div class="chat-header-title">'+esc(agentMap[activeChannel]||activeChannel||"workspace.broadcast")+'</div></div><div class="chat-messages">';

var displayMsgs=activeChannel&&activeChannel!=="workspace.broadcast"?(chats[activeChannel]?chats[activeChannel].slice().reverse():[]):bcasts.slice().reverse();
if(displayMsgs.length===0){html+='<div class="msg-system">No messages</div>'}else{
	displayMsgs.forEach(function(m){var isOut=m.from_agent_id===state.selectedAgentId;var name=agentMap[m.from_agent_id]||m.from_agent_id;html+='<div class="msg '+(isOut?"msg-out":"msg-in")+'"><div class="msg-meta"><span>'+esc(name)+'</span><span>'+esc(new Date(m.created_at).toLocaleTimeString())+'</span></div><div class="msg-bubble">'+esc(m.content)+'</div></div>'});
}
html+='</div></div>';
node.innerHTML=html;

var newList=node.querySelector(".chat-list"),newMsgs=node.querySelector(".chat-messages");
if(newList){newList.scrollTop=listScroll}
if(newMsgs){if(msgsAtBottom){newMsgs.scrollTop=newMsgs.scrollHeight}else{newMsgs.scrollTop=msgsScroll}}
}

function renderActivityPanel(){
var node=document.getElementById("activity-panel");if(!state.activity){node.innerHTML='<div class="empty">loading activity...</div>';return}
var a=state.activity,proc=a.process||{},live=a.live||{},cat=a.catalog||{};
var isUp=proc.running,uptime="0s";
var tasksCount=cat.tasks?cat.tasks.length:0;
var tr=cat.tensions?cat.tensions.length:0;

var html='<div class="activity-grid">';
html+='<div class="activity-card"><div class="ac-title">Uptime Status</div><div class="ac-val '+(isUp?"highlight":"")+'">'+(isUp?"Running":"Stopped")+'</div></div>';
html+='<div class="activity-card"><div class="ac-title">Active Task</div><div class="ac-val" style="font-size:1.1rem;word-break:break-all">'+(live.active_task_id?esc(live.active_task_id):"None")+'</div></div>';
html+='<div class="activity-card"><div class="ac-title">Catalog Tasks</div><div class="ac-val">'+tasksCount+'</div></div>';
html+='<div class="activity-card"><div class="ac-title">Active Tensions</div><div class="ac-val">'+tr+'</div></div></div>';

html+='<section class="card"><h2>Recent Events Timeline (Simulated)</h2><div class="timeline">';
if(live.active_task_id){
	html+='<div class="tl-item"><div class="tl-time">active</div><div class="tl-content"><div class="tl-header"><span class="tl-title">Processing Task</span></div><div class="tl-body">'+esc(live.active_task_id)+'</div></div></div>';
}
if(proc.running){
	html+='<div class="tl-item"><div class="tl-time">live</div><div class="tl-content"><div class="tl-header"><span class="tl-title">Process Attached</span></div><div class="tl-body">PID '+esc(proc.pid)+' running</div></div></div>';
}
if(!live.active_task_id&&!proc.running){
	html+='<div class="empty" style="margin-left:140px">no recent activity</div>';
}
html+='</div></section>';
node.innerHTML=html;
}

function renderRuntimePanel(){
var node=document.getElementById("runtime-panel");if(!state.detail){node.innerHTML='<div class="empty">select an agent to view runtime snapshot</div>';return}
var d=state.detail,live=d.live||{},profile=d.profile||{},runtime=d.local_runtime||{},effective=d.effective_identity||{};
var html='<div class="kvs">'+kv("Status",live.status)+kv("Summary",live.summary)+kv("Paused",live.paused)+kv("Attachable",live.attachable)+kv("Active Task",live.active_task_id)+kv("Active Session",live.active_session_id)+kv("Focus Task",live.focus_task_id)+kv("Focus Tension",live.focus_tension_id)+kv("Last Action",live.last_action)+kv("Trigger",live.trigger)+kv("Runtime Mode",runtime.mode)+kv("RPC Endpoint",runtime.rpc_endpoint)+kv("Identity Source",effective.source)+kv("Effective Agent",effective.agent_id)+kv("Effective Workspace",effective.workspace_id)+'</div>';
html+='<div class="section-gap"><h3>Mission</h3><pre>'+esc(profile.mission||"mission not set")+'</pre></div><div class="section-gap"><h3>Effective / Profile / Runtime JSON</h3><div class="split"><pre>'+esc(JSON.stringify(effective,null,2))+'</pre><pre>'+esc(JSON.stringify(profile,null,2))+'</pre><pre>'+esc(JSON.stringify(runtime,null,2))+'</pre></div></div>';node.innerHTML=html;
}

function renderCatalogPanel(){
var node=document.getElementById("catalog-panel");if(!state.detail){node.innerHTML='<div class="empty">catalog unavailable</div>';return}
var catalog=state.detail.catalog||{},html="";if(catalog.error){html+='<div class="small">catalog warning: '+esc(catalog.error)+'</div>'}html+="<h3>Tasks</h3>";
if(catalog.tasks&&catalog.tasks.length){html+='<div class="list">';catalog.tasks.forEach(function(t){html+='<button type="button" class="btn-ghost catalog-task" style="text-align:left;width:100%" data-task-id="'+esc(t.task_id)+'"><strong>'+esc(t.title||t.task_id)+'</strong><div class="small">'+esc(t.task_id+" \u2014 "+fmt(t.status)+" \u2014 "+fmt(t.priority))+'</div></button>'});html+="</div>"}else{html+='<div class="empty">no task choices</div>'}
html+='<div class="section-gap"><h3>Tensions</h3>';
if(catalog.tensions&&catalog.tensions.length){html+='<div class="list">';catalog.tensions.forEach(function(t){html+='<button type="button" class="btn-ghost catalog-tension" style="text-align:left;width:100%" data-tension-id="'+esc(t.tension_id)+'"><strong>'+esc(t.title||t.tension_id)+'</strong><div class="small">'+esc(t.tension_id+" \u2014 "+fmt(t.tension_type)+" \u2014 "+fmt(t.review_status))+'</div></button>'});html+="</div>"}else{html+='<div class="empty">no tension choices</div>'}html+="</div>";node.innerHTML=html;
Array.prototype.forEach.call(node.querySelectorAll(".catalog-task"),function(btn){btn.addEventListener("click",function(){var f=document.getElementById("switch-task-id");if(f){f.value=btn.getAttribute("data-task-id")||"";switchAgentSubTab("controls")}})});
Array.prototype.forEach.call(node.querySelectorAll(".catalog-tension"),function(btn){btn.addEventListener("click",function(){var f=document.getElementById("switch-tension-id");if(f){f.value=btn.getAttribute("data-tension-id")||"";switchAgentSubTab("controls")}})});
}

function renderLogsPanel(){
var node=document.getElementById("logs-panel");if(!state.detail){node.innerHTML='<div class="empty">select an agent to view its logs</div>';return}

var pres=node.querySelectorAll("pre");
var oScroll=pres[0]?pres[0].scrollTop:-1,oHeight=pres[0]?pres[0].scrollHeight:-1,oClient=pres[0]?pres[0].clientHeight:-1;
var eScroll=pres[1]?pres[1].scrollTop:-1,eHeight=pres[1]?pres[1].scrollHeight:-1,eClient=pres[1]?pres[1].clientHeight:-1;
var outAtBtm=oScroll===-1||(oScroll+oClient>=oHeight-10);
var errAtBtm=eScroll===-1||(eScroll+eClient>=eHeight-10);

var logs=state.detail.logs||{},stdout=Array.isArray(logs.stdout)?logs.stdout.join("\n"):"",stderr=Array.isArray(logs.stderr)?logs.stderr.join("\n"):"";
node.innerHTML='<div class="logs"><div><h3>stdout</h3><div class="small mono">'+esc(fmt(logs.log_out_path,""))+'</div><pre>'+esc(stdout||"(empty)")+'</pre></div><div class="section-gap"><h3>stderr</h3><div class="small mono">'+esc(fmt(logs.log_err_path,""))+'</div><pre>'+esc(stderr||"(empty)")+'</pre></div></div>';

var nPres=node.querySelectorAll("pre");
if(nPres[0]){if(outAtBtm){nPres[0].scrollTop=nPres[0].scrollHeight}else{nPres[0].scrollTop=oScroll}}
if(nPres[1]){if(errAtBtm){nPres[1].scrollTop=nPres[1].scrollHeight}else{nPres[1].scrollTop=eScroll}}
}
})();
`
}
