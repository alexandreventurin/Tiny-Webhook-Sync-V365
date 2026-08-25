const state = {
  page: "home",
  subtab: "",
  period: "today",
  start: "",
  end: "",
  summary: null,
  auth: { tokens: [], health: {} },
  cache: { queue: {}, origin: {}, synced: {}, cancelled: null },
  filters: {
    accounts: [], types: [], statuses: [], actions: [], date: "", hour: "",
    receivedDate: "", plannedDate: "", shipping: [], reasons: [],
  },
  query: "",
  webhookLimit: 10,
  pageSizes: {},
  selectedCancelled: new Set(),
  cardsCollapsed: false,
  loading: 0,
};

const $ = (id) => document.getElementById(id);
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (char) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[char]));
const fmt = (value) => value === null || value === undefined || value === "" ? "-" : String(value);
const dateTime = (value) => value ? new Date(value).toLocaleString("pt-BR") : "-";
const dateOnly = (value) => value ? String(value).split(" ")[0] : "-";
const textOf = (value) => JSON.stringify(value || {}).toLocaleLowerCase("pt-BR");
const selectedValues = (id) => [...$(id).selectedOptions].map((option) => option.value).filter(Boolean);
const safeJson = (value) => { try { return JSON.parse(value); } catch (_) { return {}; } };

function setLoading(active, label = "Carregando painel...") {
  state.loading = Math.max(0, state.loading + (active ? 1 : -1));
  $("loaderText").textContent = label;
  $("loader").classList.toggle("open", state.loading > 0);
}

async function apiJson(url, options = {}, label = "Carregando dados...", silent = false) {
  if (!silent) setLoading(true, label);
  try {
    const response = await fetch(url, options);
    if (response.status === 401) {
      location.href = "/login?next=/dashboard";
      throw new Error("Sessão expirada");
    }
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || `Falha na solicitação (${response.status})`);
    return data;
  } finally {
    if (!silent) setLoading(false);
  }
}

function showToast(message, timeout = 5000) {
  $("toast").textContent = message;
  $("toast").classList.add("open");
  window.clearTimeout(showToast.timer);
  showToast.timer = window.setTimeout(() => $("toast").classList.remove("open"), timeout);
}

function periodParams() {
  const params = new URLSearchParams({ period: state.period });
  if (state.period === "custom" && state.start && state.end) {
    params.set("start", state.start);
    params.set("end", state.end);
  }
  return params;
}

function periodKey() {
  return `${state.period}:${state.start}:${state.end}`;
}

function badge(label, type = "muted") {
  return `<span class="badge ${type}">${esc(label)}</span>`;
}

function statusText(status) {
  if (status === "concluido") return "Concluído";
  if (status === "aguardando") return "Aguardando";
  if (status === "erro") return "Erro";
  return fmt(status).replaceAll("_", " ");
}

function taskBadge(status) {
  return badge(statusText(status), status === "concluido" ? "ok" : status === "erro" ? "bad" : "info");
}

function table(headers, rows) {
  if (!rows.length) return '<div class="empty">Nenhum registro encontrado.</div>';
  return `<table><thead><tr>${headers.map((header) => `<th>${header}</th>`).join("")}</tr></thead><tbody>${rows.join("")}</tbody></table>`;
}

function erpLink(id, html) {
  if (!id) return html;
  return `<a class="erp-link" href="https://erp.olist.com/vendas#edit/${encodeURIComponent(id)}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${html}</a>`;
}

function trackingLink(code) {
  if (!code) return "-";
  const href = `https://rejuderme.com.br/pages/rastreio?code=${encodeURIComponent(code)}`;
  return `<a class="tracking-link" href="${href}" target="_blank" rel="noopener" onclick="event.stopPropagation()">${esc(code)}</a>`;
}

function flowCell(item) {
  return `<div class="flow"><div class="flow-line"><span class="flow-dot in"></span><span>${dateTime(item.webhook_received_at)}</span></div><div class="flow-line"><span class="flow-dot out"></span><span>${dateTime(item.transfer_scheduled_at)}</span></div></div>`;
}

function shippingCell(item, compare = false) {
  const shippingClass = compare && item.forma_envio_divergent ? "danger-text" : "";
  const trackingClass = compare && item.codigo_rastreamento_divergent ? "danger-text" : "";
  const shipping = item.forma_envio || item.forma_frete || "-";
  const freight = item.forma_envio && item.forma_frete && item.forma_envio !== item.forma_frete
    ? `<div class="small ${shippingClass}">${esc(item.forma_frete)}</div>` : "";
  return `<div class="${shippingClass}">${esc(shipping)}</div>${freight}<div class="small mono ${trackingClass}">${trackingLink(item.codigo_rastreamento)}</div>`;
}

function reasonLabel(reason) {
  const labels = {
    cliente_sem_nome: "Cliente sem nome",
    cliente_sem_cpf_cnpj: "CPF/CNPJ ausente",
    detalhes_pendentes: "Detalhes ainda não carregados",
    sem_itens: "Itens não carregados",
    sem_detalhes_do_pedido: "Pedido sem detalhes",
    produto_sem_mapeamento: "Produto sem mapeamento",
    erro_criacao_c: "Falha ao criar em C",
    deposito_nao_exportavel: "Depósito/envio não exportável",
    ja_marcado_v365: "Já marcado v365",
    pedido_excluido: "Pedido excluído",
  };
  if (String(reason).startsWith("status_nao_exportavel:")) {
    return `Situação não permitida: ${String(reason).split(":")[1].replaceAll("_", " ")}`;
  }
  return labels[reason] || String(reason).replaceAll("_", " ");
}

function loadMoreButton(onclick, shown, total) {
  if (shown >= total) return "";
  return `<div class="load-more"><button onclick="${onclick}">Exibir mais 20 (${shown}/${total})</button></div>`;
}

async function loadSummary(silent = false) {
  const data = await apiJson(`/admin/orders-panel/summary?${periodParams()}`, {}, "Carregando resumo...", silent);
  state.summary = data;
  $("periodToggle").textContent = data.period.label;
  renderSummary();
}

function metric(label, value, tone, action = "", actionLabel = "") {
  const button = action ? `<button data-card-action="${action}">${esc(actionLabel)}</button>` : "";
  return `<div class="metric ${tone}"><div><div class="value">${esc(value ?? 0)}</div><div class="label">${esc(label)}</div></div>${button}</div>`;
}

function renderSummary() {
  const cards = state.summary?.cards || {};
  $("summary").classList.toggle("collapsed", state.cardsCollapsed);
  $("summary").innerHTML = [
    metric("Execuções concluídas", cards.export_completed, "green"),
    metric("Agendados para exportar", cards.scheduled_export, "blue"),
    metric("Precisam de ajuste", cards.needs_adjustment, "orange", "refresh-adjustments", "Atualizar"),
    metric("Não exportar", cards.do_not_export, "lilac"),
    metric("Sincronizados", cards.synced, "green"),
    metric("Com erro de sincronismo", cards.sync_errors, "red", "retry-errors", "Retentar"),
    metric("Divergentes", cards.divergent, "orange", "sync-divergences", "Sincronizar"),
    metric("Cancelados para analisar", cards.cancelled_pending, "yellow", "open-cancelled", "Abrir lista"),
  ].join("");
  $("collapseCards").textContent = state.cardsCollapsed ? "Mostrar cards" : "Recolher cards";
  document.querySelectorAll("[data-card-action]").forEach((button) => {
    button.onclick = () => runCardAction(button.dataset.cardAction);
  });
}

async function runCardAction(action) {
  if (action === "refresh-adjustments") return refreshAdjustments();
  if (action === "retry-errors") return openErrorModal();
  if (action === "sync-divergences") return openDivergenceModal();
  if (action === "open-cancelled") return openPage("cancelled", true, "pending");
}

async function loadAuth() {
  try {
    const [tokens, health] = await Promise.all([
      apiJson("/admin/tokens", {}, "Verificando conexões...", true),
      apiJson("/admin/tokens/health", {}, "Verificando conexões...", true),
    ]);
    state.auth.tokens = tokens.tokens || [];
    state.auth.health = health || {};
  } catch (_) {
    state.auth = { tokens: [], health: {} };
  }
  renderAuth();
}

function renderAuth() {
  const tokens = { A: null, B: null };
  state.auth.tokens.forEach((token) => { tokens[token.account] = token; });
  const labels = { A: "Rejuderme", B: "V.365" };
  const paths = { A: "/auth/rj/start", B: "/auth/v365/start" };
  $("authMini").innerHTML = ["A", "B"].map((account) => {
    const token = tokens[account];
    const health = state.auth.health?.[account];
    let css = "missing";
    let status = "Sem conexão";
    if (token) {
      const valid = token.expires_at && new Date(token.expires_at) > new Date();
      if (!valid) { css = "expired"; status = "Expirado"; }
      else if (health && health.ok === false) { css = "expired"; status = health.status_code === 401 ? "Token inválido" : "Falha"; }
      else { css = "ok"; status = "Conectado"; }
    }
    return `<div class="auth-mini-card ${css}"><span class="auth-mini-dot"></span><div class="auth-mini-text"><div class="auth-mini-name">Tiny ${esc(labels[account])}</div><div class="auth-mini-detail">${esc(status)}</div></div><a class="auth-mini-link" href="${paths[account]}" target="_blank">Reconectar</a></div>`;
  }).join("");
}

function queueStatus(item) {
  const raw = item.task_status_raw || item.status;
  if (raw === "done") return "concluido";
  if (["failed", "dead", "waiting_sku"].includes(raw)) return "erro";
  return "aguardando";
}

function filterKey() {
  return JSON.stringify(state.filters);
}

async function loadQueue(force = false, silent = false, light = false) {
  const key = `${periodKey()}:${state.subtab}:${filterKey()}:${state.webhookLimit}:${state.pageSizes[`queue:${state.subtab}`] || 20}`;
  if (state.cache.queue[key] && !force) return state.cache.queue[key];
  const params = periodParams();
  if (state.subtab === "webhooks") {
    params.set("limit", String(state.webhookLimit));
    if (light) params.set("include_meta", "false");
    if (state.filters.accounts.length) params.set("accounts", state.filters.accounts.join(","));
    if (state.filters.types.length) params.set("types", state.filters.types.join(","));
    if (state.filters.statuses.length) params.set("situations", state.filters.statuses.join(","));
    if (state.filters.actions.length) params.set("actions", state.filters.actions.join(","));
    const data = await apiJson(`/admin/orders-panel/webhooks?${params}`, {}, "Carregando webhooks...", silent);
    const previous = state.cache.queue.current || {};
    state.cache.queue[key] = {
      events: data.items || [],
      items: [],
      groups: [],
      counts: Object.keys(data.counts || {}).length ? data.counts : (previous.counts || {}),
      total: data.total || 0,
      options: Object.keys(data.options || {}).length ? data.options : (previous.options || {}),
    };
  } else {
    const limit = state.pageSizes[`queue:${state.subtab}`] || 20;
    params.set("limit", String(limit));
    params.set("statuses", state.subtab === "executed" ? "concluido" : state.subtab === "future" ? "aguardando" : "erro");
    if (state.filters.types.length) params.set("types", state.filters.types.join(","));
    if (state.filters.accounts.length) params.set("accounts", state.filters.accounts.join(","));
    const data = await apiJson(`/admin/orders-panel/queue-data?${params}`, {}, "Carregando fila de execuções...", silent);
    state.cache.queue[key] = { items: data.queue?.items || [], groups: data.queue?.groups || [], events: [], counts: data.counts || {}, total: data.counts?.[state.subtab] || 0 };
  }
  state.cache.queue.current = state.cache.queue[key];
  return state.cache.queue[key];
}

function queueGroupRow(group) {
  const start = group.window_start ? new Date(group.window_start).toLocaleString("pt-BR") : "Sem horário";
  return `<tr class="queue-group"><td colspan="9"><div class="queue-meta"><span class="queue-chip time">${esc(start)} <span class="queue-plus">+10 min</span></span><span class="queue-chip exec">Execuções: A ${esc(group.planned_a || 0)} / ${esc(group.success_a || 0)} · C ${esc(group.planned_c || 0)} / ${esc(group.success_c || 0)}</span><span class="queue-chip limit">Limites restantes API: A ${esc(group.remaining_a || 0)} | C ${esc(group.remaining_c || 0)}</span></div></td></tr>`;
}

function queueMatches(item) {
  if (state.query && !textOf(item).includes(state.query.toLocaleLowerCase("pt-BR"))) return false;
  if (state.filters.date) {
    const value = item.scheduled_at || item.updated_at || item.created_at;
    if (!value || new Date(value).toISOString().slice(0, 10) !== state.filters.date) return false;
  }
  if (state.filters.hour) {
    const value = item.scheduled_at || item.updated_at || item.created_at;
    if (!value || new Date(value).toTimeString().slice(0, 5) !== state.filters.hour) return false;
  }
  return true;
}

function renderQueue() {
  const data = state.cache.queue.current || { items: [], events: [], groups: [], counts: {}, total: 0 };
  const tabs = [["webhooks", "Webhooks"], ["executed", "Sucesso"], ["future", "Futuros"], ["error", "Com erro"]];
  $("subtabs").innerHTML = tabs.map(([key, label]) => `<button class="subtab ${state.subtab === key ? "active" : ""}" data-subtab="${key}">${label} (${data.counts?.[key] ?? 0})</button>`).join("");
  bindSubtabs("queue");
  if (state.subtab === "webhooks") return renderWebhooks(data);
  populateQueueFilters(data.items);
  const groups = Object.fromEntries((data.groups || []).map((group) => [group.window_key, group]));
  const items = (data.items || []).filter(queueMatches);
  let lastGroup = "";
  const rows = [];
  items.forEach((item) => {
    if (item.window_key && item.window_key !== lastGroup) {
      rows.push(queueGroupRow(groups[item.window_key] || {}));
      lastGroup = item.window_key;
    }
    rows.push(`<tr data-a="${esc(item.venda_a_id || "")}" data-c="${esc(item.venda_c_id || "")}"><td>${erpLink(item.venda_a_id, `<strong>${esc(item.numero || item.venda_a_id || "-")}</strong><div class="small mono">ID A ${esc(item.venda_a_id || "-")}</div>`)}</td><td>${erpLink(item.venda_c_id, `<strong>${esc(item.nota_fiscal_destino ? `NF ${item.nota_fiscal_destino}` : "NF -")}</strong><div class="small mono">ID C ${esc(item.venda_c_id || "-")}</div>`)}</td><td>${dateTime(item.scheduled_at || item.updated_at)}</td><td>${esc(item.cliente || "-")}<div class="small">${esc(item.cpf_cnpj || "-")}</div></td><td>${esc(item.ecommerce_nome || "-")}<div class="small mono">${esc(item.numero_ecommerce || "-")}</div></td><td>${esc(item.task_label || item.job_type || "-")}</td><td>${taskBadge(queueStatus(item))}</td><td>${esc(item.attempts || 0)}</td><td class="small">${esc(item.last_error || "")}</td></tr>`);
  });
  const shown = data.items.length;
  $("tableWrap").innerHTML = table(["Origem", "Destino", "Previsto/Executado", "Cliente", "Ecommerce", "Tarefa", "Status", "Tentativas", "Erro"], rows) + loadMoreButton("loadMoreQueue()", shown, data.total || shown);
  bindOrderRows();
  $("listTools").innerHTML = "";
}

function renderWebhooks(data) {
  const events = (data.events || []).filter(queueMatches);
  populateWebhookFilters(data.options || {});
  $("tableWrap").innerHTML = table(["Recebido", "Conta", "Tipo", "Pedido", "Situação", "Nota fiscal", "Ação"], events.map((event) => `<tr><td>${dateTime(event.created_at)}</td><td>${badge(event.source === "A" ? "Rejuderme" : "V.365", event.source === "A" ? "info" : "muted")}</td><td>${esc(event.topic || "-")}</td><td class="mono">${erpLink(event.venda_id, esc(event.venda_id || "-"))}</td><td>${esc(event.codigo_situacao || "-")}</td><td class="mono">${esc(event.id_nota_fiscal || "-")}</td><td>${badge(event.action_result || "-", "muted")}</td></tr>`));
  $("listTools").innerHTML = `<label for="webhookLimit">Exibir</label><select id="webhookLimit"><option>10</option><option>20</option><option>50</option><option>100</option></select>`;
  $("webhookLimit").value = String(state.webhookLimit);
  $("webhookLimit").onchange = async (event) => { state.webhookLimit = Number(event.target.value); state.cache.queue = {}; await loadQueue(true); renderQueue(); };
}

async function loadMoreQueue() {
  const key = `queue:${state.subtab}`;
  state.pageSizes[key] = (state.pageSizes[key] || 20) + 20;
  state.cache.queue = {};
  await loadQueue(true);
  renderQueue();
}

async function loadOrigin(force = false, append = false) {
  const category = state.subtab || "ready";
  const key = `${periodKey()}:${category}`;
  const current = state.cache.origin[key];
  if (current && !force && !append) { state.cache.origin.current = current; return current; }
  const params = periodParams();
  params.set("category", category);
  params.set("limit", "20");
  params.set("offset", String(append && current ? current.items.length : 0));
  const data = await apiJson(`/admin/orders-panel/origin-list?${params}`, {}, "Carregando pedidos na origem...");
  const merged = append && current ? { ...data, items: [...current.items, ...(data.items || [])], offset: 0 } : data;
  merged.has_more = merged.items.length < merged.total;
  state.cache.origin[key] = merged;
  state.cache.origin.current = merged;
  return merged;
}

function originMatches(item) {
  if (state.query && !textOf(item).includes(state.query.toLocaleLowerCase("pt-BR"))) return false;
  if (state.filters.statuses.length && !state.filters.statuses.includes(item.situacao_label || item.situacao_normalized || "")) return false;
  if (state.filters.receivedDate && (!item.webhook_received_at || new Date(item.webhook_received_at).toISOString().slice(0, 10) !== state.filters.receivedDate)) return false;
  if (state.filters.plannedDate && (!item.transfer_scheduled_at || new Date(item.transfer_scheduled_at).toISOString().slice(0, 10) !== state.filters.plannedDate)) return false;
  if (state.filters.shipping.length && !state.filters.shipping.includes(item.forma_envio || item.forma_frete || "")) return false;
  if (state.filters.reasons.length && !state.filters.reasons.some((reason) => (item.reasons || []).includes(reason))) return false;
  return true;
}

function renderOrigin() {
  const data = state.cache.origin.current || { items: [], total: 0 };
  const cards = state.summary?.cards || {};
  const counts = { ready: cards.scheduled_export || 0, needs_adjustment: cards.needs_adjustment || 0, do_not_export: cards.do_not_export || 0 };
  const tabs = [["ready", "Prontos para exportação"], ["needs_adjustment", "Precisam de ajuste"], ["do_not_export", "Não exportar"]];
  $("subtabs").innerHTML = tabs.map(([key, label]) => `<button class="subtab ${state.subtab === key ? "active" : ""}" data-subtab="${key}">${label} (${counts[key]})</button>`).join("");
  bindSubtabs("origin");
  populateOriginFilters(data.items || []);
  const items = (data.items || []).filter(originMatches);
  const rows = items.map((item) => `<tr data-a="${esc(item.venda_a_id)}"><td>${erpLink(item.venda_a_id, `<strong>${esc(item.numero || item.venda_a_id)}</strong><div class="small mono">ID A ${esc(item.venda_a_id)}</div>`)}</td><td>${esc(dateOnly(item.data || item.data_hora))}</td><td>${flowCell(item)}</td><td>${esc(item.cliente || "-")}<div class="small">${esc(item.cpf_cnpj || "-")}</div></td><td>${esc(item.ecommerce_nome || "-")}<div class="small mono">${esc(item.numero_ecommerce || "-")}</div></td><td>${badge(item.situacao_label || item.situacao_normalized || "sem status", item.export_category === "valid" ? "ok" : item.export_category === "do_not_export" ? "bad" : "warn")}</td><td>${shippingCell(item)}</td><td>${esc(item.itens ?? "-")}</td><td><div class="reason">${(item.reasons || []).length ? item.reasons.map((reason) => badge(reasonLabel(reason), item.export_category === "do_not_export" ? "bad" : "warn")).join("") : badge("Pronto", "ok")}</div>${item.last_create_error ? `<div class="small">${esc(item.last_create_error)}</div>` : ""}</td></tr>`);
  $("tableWrap").innerHTML = table(["Pedido", "Data", "Recebido/Enviar", "Cliente", "Ecommerce", "Situação", "Envio / Rastreio", "Itens", "Motivos"], rows) + loadMoreButton("loadMoreOrigin()", data.items.length, data.total || data.items.length);
  bindOrderRows();
  $("listTools").innerHTML = "";
}

async function loadMoreOrigin() { await loadOrigin(true, true); renderOrigin(); }

async function loadSynced(force = false, append = false) {
  const status = state.subtab && state.subtab !== "all" ? state.subtab : "all";
  const key = `${periodKey()}:${status}`;
  const current = state.cache.synced[key];
  if (current && !force && !append) { state.cache.synced.current = current; return current; }
  const params = periodParams();
  params.set("limit", "20");
  params.set("offset", String(append && current ? current.items.length : 0));
  if (status !== "all") params.set("status", status);
  const data = await apiJson(`/admin/orders-panel/synced-list?${params}`, {}, "Carregando pedidos sincronizados...");
  const merged = append && current ? { ...data, items: [...current.items, ...(data.items || [])], status_counts: data.status_counts || current.status_counts } : data;
  merged.has_more = merged.items.length < merged.total;
  state.cache.synced[key] = merged;
  state.cache.synced.current = merged;
  return merged;
}

function syncedRows(items, selectable = false) {
  return items.map((item) => {
    const checked = state.selectedCancelled.has(String(item.venda_a_id)) ? "checked" : "";
    const selectCell = selectable ? `<td class="selectcol"><input type="checkbox" class="cancel-check" data-a="${esc(item.venda_a_id)}" ${checked} onclick="event.stopPropagation()"></td>` : "";
    return `<tr data-a="${esc(item.venda_a_id || "")}" data-c="${esc(item.venda_c_id || "")}">${selectCell}<td>${erpLink(item.venda_a_id, `<strong>${esc(item.numero || item.venda_a_id || "-")}</strong><div class="small mono">ID A ${esc(item.venda_a_id || "-")}</div>`)}<div>${badge(item.situacao_a_label || item.situacao_label || "sem status", "info")}</div></td><td>${erpLink(item.venda_c_id, `<strong>${esc(item.nota_fiscal_destino ? `NF ${item.nota_fiscal_destino}` : "NF -")}</strong><div class="small mono">ID C ${esc(item.venda_c_id || "-")}</div>`)}<div>${badge(item.situacao_destino_label || "em aberto", "muted")}</div></td><td>${esc(dateOnly(item.data))}</td><td>${esc(item.cliente || "-")}<div class="small">${esc(item.cpf_cnpj || "-")}</div></td><td>${esc(item.ecommerce_nome || "-")}<div class="small mono">${esc(item.numero_ecommerce || "-")}</div></td><td>${shippingCell(item, true)}</td><td>${dateTime(item.last_sync_at || item.updated_at)}</td><td>${item.divergence_count === null ? badge("-", "muted") : badge(item.divergence_count, item.divergence_count ? "warn" : "ok")}</td><td>${badge(item.latest_job_type ? `${item.latest_job_type}: ${item.last_job_status || "-"}` : item.last_job_status || "-", "muted")}</td></tr>`;
  });
}

function renderSynced() {
  const data = state.cache.synced.current || { items: [], total: 0, status_counts: [] };
  const statusCounts = data.status_counts || [];
  const allCount = statusCounts.reduce((sum, item) => sum + Number(item.count || 0), 0);
  const tabs = [["all", "Todos", allCount], ...statusCounts.map((item) => [item.key, item.label || statusText(item.key), item.count])];
  $("subtabs").innerHTML = tabs.map(([key, label, count]) => `<button class="subtab ${state.subtab === key ? "active" : ""}" data-subtab="${esc(key)}">${esc(label)} (${esc(count)})</button>`).join("");
  bindSubtabs("synced");
  const items = (data.items || []).filter((item) => !state.query || textOf(item).includes(state.query.toLocaleLowerCase("pt-BR")));
  $("tableWrap").innerHTML = table(["Origem", "Destino", "Data", "Cliente", "Ecommerce", "Envio / Rastreio", "Última sincronização", "Divergências", "Job"], syncedRows(items)) + loadMoreButton("loadMoreSynced()", data.items.length, data.total || data.items.length);
  bindOrderRows();
  $("listTools").innerHTML = "";
}

async function loadMoreSynced() { await loadSynced(true, true); renderSynced(); }

async function loadCancelled(force = false) {
  if (state.cache.cancelled && !force) return state.cache.cancelled;
  state.cache.cancelled = await apiJson("/admin/orders-panel/cancelled-list", {}, "Carregando cancelamentos...");
  return state.cache.cancelled;
}

function renderCancelled() {
  const data = state.cache.cancelled || { pending: [], reviewed: [] };
  const pending = data.pending || [];
  const reviewed = data.reviewed || [];
  $("subtabs").innerHTML = `<button class="subtab ${state.subtab === "pending" ? "active" : ""}" data-subtab="pending">Analisar (${pending.length})</button><button class="subtab ${state.subtab === "reviewed" ? "active" : ""}" data-subtab="reviewed">Conferidos (${reviewed.length})</button>`;
  bindSubtabs("cancelled");
  const source = (state.subtab === "reviewed" ? reviewed : pending).filter((item) => !state.query || textOf(item).includes(state.query.toLocaleLowerCase("pt-BR")));
  const key = `cancelled:${state.subtab}`;
  const size = state.pageSizes[key] || 20;
  const visible = source.slice(0, size);
  const selectable = state.subtab === "pending";
  const selectedCount = [...state.selectedCancelled].filter((id) => source.some((item) => String(item.venda_a_id) === id)).length;
  const reviewBar = selectable ? `<div class="reviewbar"><button id="confirmReviewBtn" ${selectedCount ? "" : "disabled"}>Confirmar análise (${selectedCount})</button></div>` : "";
  const headers = selectable ? ['<input id="selectAllCancelled" type="checkbox">', "Origem", "Destino", "Data", "Cliente", "Ecommerce", "Envio / Rastreio", "Última sincronização", "Divergências", "Job"] : ["Origem", "Destino", "Data", "Cliente", "Ecommerce", "Envio / Rastreio", "Última sincronização", "Divergências", "Job"];
  $("tableWrap").innerHTML = reviewBar + table(headers, syncedRows(visible, selectable)) + loadMoreButton("loadMoreCancelled()", visible.length, source.length);
  bindOrderRows();
  document.querySelectorAll(".cancel-check").forEach((checkbox) => { checkbox.onchange = () => { checkbox.checked ? state.selectedCancelled.add(String(checkbox.dataset.a)) : state.selectedCancelled.delete(String(checkbox.dataset.a)); renderCancelled(); }; });
  const selectAll = $("selectAllCancelled");
  if (selectAll) selectAll.onchange = () => { source.forEach((item) => selectAll.checked ? state.selectedCancelled.add(String(item.venda_a_id)) : state.selectedCancelled.delete(String(item.venda_a_id))); renderCancelled(); };
  const confirmButton = $("confirmReviewBtn");
  if (confirmButton) confirmButton.onclick = confirmCancelledReview;
  $("listTools").innerHTML = "";
}

function loadMoreCancelled() { const key = `cancelled:${state.subtab}`; state.pageSizes[key] = (state.pageSizes[key] || 20) + 20; renderCancelled(); }

async function confirmCancelledReview() {
  const ids = [...state.selectedCancelled];
  if (!ids.length || !window.confirm(`${ids.length} pedido(s) devem ser enviados para Conferidos?`)) return;
  await apiJson("/admin/orders-panel/cancelled/mark-reviewed", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ venda_a_ids: ids }) }, "Confirmando análise...");
  state.selectedCancelled.clear();
  state.cache.cancelled = null;
  await Promise.all([loadSummary(), loadCancelled(true)]);
  state.subtab = "reviewed";
  renderCancelled();
}

function bindOrderRows() {
  document.querySelectorAll("tr[data-a]").forEach((row) => {
    row.onclick = (event) => { if (!event.target.closest("a,input,button")) openOrderDetail(row.dataset.a, row.dataset.c); };
  });
}

async function openOrderDetail(vendaA, vendaC) {
  const params = new URLSearchParams();
  if (vendaA) params.set("venda_a_id", vendaA);
  if (vendaC) params.set("venda_c_id", vendaC);
  openModal(`Pedido A ${vendaA || "-"}`, '<div class="empty">Carregando pedido...</div>', "");
  try {
    const data = await apiJson(`/admin/orders-panel/detail?${params}`, {}, "Carregando pedido...");
    $("modalTitle").textContent = `Pedido A ${fmt(data.venda_a_id)}${data.venda_c_id ? ` / C ${data.venda_c_id}` : ""}`;
    $("modalBody").innerHTML = `<div class="diff-row"><strong>Campo</strong><strong>Origem</strong><strong>Destino</strong></div>${(data.fields || []).map((field) => `<div class="diff-row ${field.divergent ? "divergent" : ""}"><strong>${esc(field.label)}</strong><div>${esc(fmt(field.origin))}</div><div>${esc(fmt(field.destination))}</div></div>`).join("")}<strong>Total de divergências: ${esc(data.divergence_count || 0)}</strong>`;
  } catch (error) {
    $("modalBody").innerHTML = `<div class="empty">${esc(error.message)}</div>`;
  }
}

function openModal(title, body, footer) {
  $("modalTitle").textContent = title;
  $("modalBody").innerHTML = body;
  $("modalFoot").innerHTML = footer;
  $("modal").classList.add("open");
}

function closeModal() { $("modal").classList.remove("open"); }

async function refreshAdjustments() {
  const data = await apiJson("/admin/orders-panel/origin/refresh-adjustments", { method: "POST" }, "Enviando verificações para a fila...");
  showToast(`${data.queued || 0} pedido(s) enviados para conferência. O quadro será atualizado conforme as verificações terminarem.`);
  state.cache.origin = {};
  await loadSummary();
  pollSummaryAfterQueuedAction();
}

function pollSummaryAfterQueuedAction() {
  window.clearTimeout(pollSummaryAfterQueuedAction.timer);
  let remaining = 10;
  const tick = async () => {
    try { await loadSummary(true); } catch (_) { }
    remaining -= 1;
    if (remaining > 0) pollSummaryAfterQueuedAction.timer = window.setTimeout(tick, 60000);
  };
  pollSummaryAfterQueuedAction.timer = window.setTimeout(tick, 10000);
}

async function openErrorModal() {
  const data = await apiJson("/admin/orders-panel/errors/summary", {}, "Analisando erros...");
  if (!(data.types || []).length) { showToast("Não há erros pendentes."); return; }
  const body = `<p class="modal-note">A tarefa consulta novamente o Tiny antes de executar. Se o problema continuar, ela retorna para Com erro.</p>${(data.types || []).map((type) => `<label class="check-row"><input type="checkbox" class="error-check" value="${esc(type.job_type)}" ${type.checkable ? "" : "disabled"}><span><strong>${esc(type.label)}</strong><div class="small">${esc(type.count)} erro(s) · ${esc(type.checkable)} podem ser verificados</div>${type.sample_error ? `<div class="small">${esc(type.sample_error)}</div>` : ""}</span></label>`).join("")}`;
  openModal("Retentar erros de sincronismo", body, '<button id="cancelModal">Cancelar</button><button class="primary" id="confirmErrors" disabled>Enviar para fila</button>');
  $("cancelModal").onclick = closeModal;
  const confirmButton = $("confirmErrors");
  document.querySelectorAll(".error-check").forEach((checkbox) => { checkbox.onchange = () => { confirmButton.disabled = !document.querySelectorAll(".error-check:checked").length; }; });
  confirmButton.onclick = async () => {
    const jobTypes = [...document.querySelectorAll(".error-check:checked")].map((checkbox) => checkbox.value);
    const result = await apiJson("/admin/orders-panel/errors/retry", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ job_types: jobTypes }) }, "Verificando e reenfileirando...");
    closeModal();
    state.cache.queue = {};
    await loadSummary();
    showToast(`${result.queued || 0} tarefa(s) reenfileirada(s); ${result.still_blocked || 0} continuam aguardando correção.`);
  };
}

async function openDivergenceModal() {
  const data = await apiJson("/admin/orders-panel/divergences/types", {}, "Analisando divergências...");
  if (!(data.types || []).length) { showToast("Não há divergências pendentes."); return; }
  const body = (data.types || []).map((type) => `<label class="check-row"><input type="checkbox" class="divergence-check" value="${esc(type.key)}" ${type.syncable ? "" : "disabled"}><span><strong>${esc(type.label)}</strong><div class="small">${esc(type.count)} pedido(s)${type.syncable ? "" : " · exige análise manual"}</div></span></label>`).join("");
  openModal("Sincronizar divergências", body, '<button id="cancelModal">Cancelar</button><button class="primary" id="confirmDivergences" disabled>Enviar para fila</button>');
  $("cancelModal").onclick = closeModal;
  const confirmButton = $("confirmDivergences");
  document.querySelectorAll(".divergence-check").forEach((checkbox) => { checkbox.onchange = () => { confirmButton.disabled = !document.querySelectorAll(".divergence-check:checked").length; }; });
  confirmButton.onclick = async () => {
    const fieldKeys = [...document.querySelectorAll(".divergence-check:checked")].map((checkbox) => checkbox.value);
    const result = await apiJson("/admin/orders-panel/divergences/sync-selected", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ field_keys: fieldKeys }) }, "Criando tarefas de sincronização...");
    closeModal();
    state.cache.queue = {};
    showToast(`${result.created || 0} tarefa(s) enviada(s) para a fila.`);
    await loadSummary();
  };
}

function populateSelect(id, values, selected = []) {
  $(id).innerHTML = [...new Set(values.filter(Boolean))].sort((a, b) => String(a).localeCompare(String(b), "pt-BR")).map((value) => `<option value="${esc(value)}" ${selected.includes(String(value)) ? "selected" : ""}>${esc(statusText(value))}</option>`).join("");
}

function populateQueueFilters(items) {
  populateSelect("filterType", items.map((item) => item.job_type), state.filters.types);
  populateSelect("filterStatus", ["concluido", "aguardando", "erro"], state.filters.statuses);
  populateSelect("filterAction", [], state.filters.actions);
  [...$("filterAccount").options].forEach((option) => { option.selected = state.filters.accounts.includes(option.value); });
}

function populateWebhookFilters(options) {
  populateSelect("filterType", options.types || [], state.filters.types);
  populateSelect("filterStatus", options.situations || [], state.filters.statuses);
  populateSelect("filterAction", options.actions || [], state.filters.actions);
  [...$("filterAccount").options].forEach((option) => { option.selected = state.filters.accounts.includes(option.value); });
}

function populateOriginFilters(items) {
  populateSelect("filterType", [], []);
  populateSelect("filterStatus", items.map((item) => item.situacao_label || item.situacao_normalized), state.filters.statuses);
  populateSelect("filterShipping", items.map((item) => item.forma_envio || item.forma_frete), state.filters.shipping);
  populateSelect("filterReason", items.flatMap((item) => item.reasons || []), state.filters.reasons);
}

function updateFilterVisibility() {
  document.querySelectorAll(".origin-only").forEach((element) => { element.style.display = state.page === "origin" ? "grid" : "none"; });
  document.querySelectorAll(".webhook-only").forEach((element) => { element.style.display = state.page === "queue" && state.subtab === "webhooks" ? "grid" : "none"; });
}

function bindSubtabs(page) {
  document.querySelectorAll(".subtab").forEach((button) => {
    button.onclick = async () => { state.subtab = button.dataset.subtab; updateFilterVisibility(); await openPage(page, false, state.subtab); };
  });
}

async function openPage(page, force = false, subtab = null) {
  const previous = state.page;
  state.page = page;
  if (subtab) state.subtab = subtab;
  else if (page !== previous) state.subtab = page === "queue" ? "webhooks" : page === "origin" ? "ready" : page === "synced" ? "all" : page === "cancelled" ? "pending" : "";
  document.querySelectorAll(".menu").forEach((button) => button.classList.toggle("active", button.dataset.page === page && (!button.dataset.subtab || button.dataset.subtab === state.subtab)));
  const hasList = page !== "home";
  $("toolbar").classList.toggle("open", hasList);
  $("viewHead").classList.toggle("open", hasList);
  $("panel").classList.toggle("open", hasList);
  if (!hasList) { $("filters").classList.remove("open"); return; }
  updateFilterVisibility();
  if (page === "queue") { await loadQueue(force); renderQueue(); }
  else if (page === "origin") { await loadOrigin(force); renderOrigin(); }
  else if (page === "synced") { await loadSynced(force); renderSynced(); }
  else if (page === "cancelled") { await loadCancelled(force); renderCancelled(); }
}

function readFilters() {
  state.filters.accounts = selectedValues("filterAccount");
  state.filters.types = selectedValues("filterType");
  state.filters.statuses = selectedValues("filterStatus");
  state.filters.actions = selectedValues("filterAction");
  state.filters.date = $("filterDate").value;
  state.filters.hour = $("filterHour").value;
  state.filters.receivedDate = $("filterReceivedDate").value;
  state.filters.plannedDate = $("filterPlannedDate").value;
  state.filters.shipping = selectedValues("filterShipping");
  state.filters.reasons = selectedValues("filterReason");
}

async function applyFilters() {
  readFilters();
  if (state.page === "queue") { state.cache.queue = {}; await loadQueue(true); renderQueue(); }
  else if (state.page === "origin") renderOrigin();
  else if (state.page === "synced") renderSynced();
  else if (state.page === "cancelled") renderCancelled();
}

async function clearFilters() {
  state.query = "";
  state.filters = { accounts: [], types: [], statuses: [], actions: [], date: "", hour: "", receivedDate: "", plannedDate: "", shipping: [], reasons: [] };
  $("search").value = "";
  ["filterAccount", "filterType", "filterStatus", "filterAction", "filterShipping", "filterReason"].forEach((id) => [...$(id).options].forEach((option) => { option.selected = false; }));
  ["filterDate", "filterHour", "filterReceivedDate", "filterPlannedDate"].forEach((id) => { $(id).value = ""; });
  state.cache.queue = {};
  await openPage(state.page, true, state.subtab);
}

async function refreshCurrent() {
  await Promise.all([loadSummary(), loadAuth()]);
  if (state.page !== "home") await openPage(state.page, true, state.subtab);
}

async function changePeriod(period) {
  state.period = period;
  state.cache = { queue: {}, origin: {}, synced: {}, cancelled: null };
  state.pageSizes = {};
  document.querySelectorAll("[data-period]").forEach((button) => button.classList.toggle("active", button.dataset.period === period));
  await refreshCurrent();
}

$("periodToggle").onclick = () => $("periodControls").classList.toggle("open");
$("collapseCards").onclick = () => { state.cardsCollapsed = !state.cardsCollapsed; renderSummary(); };
$("refreshBtn").onclick = refreshCurrent;
$("resetBtn").onclick = async () => { state.cache = { queue: {}, origin: {}, synced: {}, cancelled: null }; state.pageSizes = {}; await clearFilters(); await openPage("home"); await loadSummary(); };
$("filtersBtn").onclick = () => $("filters").classList.toggle("open");
$("clearBtn").onclick = clearFilters;
$("applyFilters").onclick = applyFilters;
$("search").oninput = (event) => { state.query = event.target.value; if (state.page === "queue") renderQueue(); else if (state.page === "origin") renderOrigin(); else if (state.page === "synced") renderSynced(); else if (state.page === "cancelled") renderCancelled(); };
$("modalClose").onclick = closeModal;
$("modal").onclick = (event) => { if (event.target === $("modal")) closeModal(); };
document.querySelectorAll(".menu").forEach((button) => { button.onclick = () => openPage(button.dataset.page, false, button.dataset.subtab || null); });
document.querySelectorAll("[data-period]").forEach((button) => { button.onclick = () => changePeriod(button.dataset.period); });

async function maybeApplyCustomPeriod() {
  if (!state.start || !state.end || state.start > state.end) return;
  state.period = "custom";
  state.cache = { queue: {}, origin: {}, synced: {}, cancelled: null };
  document.querySelectorAll("[data-period]").forEach((button) => button.classList.remove("active"));
  await refreshCurrent();
}

$("customStart").onchange = async (event) => { state.start = event.target.value; await maybeApplyCustomPeriod(); };
$("customEnd").onchange = async (event) => { state.end = event.target.value; await maybeApplyCustomPeriod(); };

async function refreshWebhooks() {
  if (state.page !== "queue" || state.subtab !== "webhooks") return;
  try { await loadQueue(true, true, true); renderQueue(); } catch (_) { }
}

window.loadMoreQueue = loadMoreQueue;
window.loadMoreOrigin = loadMoreOrigin;
window.loadMoreSynced = loadMoreSynced;
window.loadMoreCancelled = loadMoreCancelled;

Promise.allSettled([loadSummary(true), loadAuth()]).then((results) => {
  if (results[0].status === "rejected") showToast("Não foi possível carregar o resumo. Use Atualizar para tentar novamente.");
}).finally(() => setLoading(false));
window.setInterval(refreshWebhooks, 10000);
