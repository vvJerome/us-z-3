// Live dashboard client. Polls /api/snapshot every 2s; renders in place.
const POLL_MS = 2000;
const STALE_MS = 4000;

const STATE_ORDER = [
  ["VALIDATED",        "text-emerald-300"],
  ["VALIDATING",       "text-sky-300"],
  ["NEEDS_ZUHAL",      "text-amber-300"],
  ["ZUHAL_VALIDATING", "text-amber-300"],
  ["DISCOVERED",       "text-slate-200"],
  ["VALIDATION_FAILED","text-rose-300"],
  ["DISCOVERY_FAILED", "text-rose-300"],
  ["COST_SKIPPED",     "text-rose-300"],
];

const fmt = (n) => n == null ? "—" : Number(n).toLocaleString();
const fmtPct = (n) => n == null ? "—" : `${n.toFixed(1)}%`;

let chart = null;
let runChart = null;
let runChartKey = "";
let lastFetchOk = 0;

function setIndicator(state, title) {
  const el = document.getElementById("indicator");
  if (!el) return;
  el.classList.remove("polling", "stale");
  if (state !== "fresh") el.classList.add(state);
  if (title) el.title = title;
}

function renderStates(states) {
  const g = document.getElementById("states-grid");
  g.innerHTML = STATE_ORDER.map(([s, cls]) => {
    const n = states[s] ?? 0;
    return `
      <div class="text-slate-400 text-sm">${s}</div>
      <div class="text-right font-semibold ${cls}">${fmt(n)}</div>
    `;
  }).join("");
}

function renderRate(rate, totals) {
  document.getElementById("rate-per-hour").textContent = fmt(rate.per_hour);
  const eta = rate.eta_hours;
  document.getElementById("rate-eta").textContent =
    eta == null ? "—" : (eta >= 48 ? `${(eta/24).toFixed(1)} d` : `${eta.toFixed(1)} h`);
  document.getElementById("total-records").textContent = fmt(totals.all);
  document.getElementById("total-pending").textContent = fmt(totals.pending);
}

function renderCost(cost, breakdown, totals) {
  document.getElementById("cost-spent").textContent = `$${cost.spent_usd.toFixed(2)}`;
  document.getElementById("cost-ceiling").textContent = cost.ceiling_usd ? `$${cost.ceiling_usd.toFixed(2)}` : "—";
  const pct = cost.pct ?? 0;
  document.getElementById("cost-pct").textContent = pct.toFixed(2);
  document.getElementById("cost-bar").style.width = `${Math.min(100, pct)}%`;

  const svcs = (breakdown && breakdown.services) || [];
  document.getElementById("cost-breakdown").innerHTML = svcs.map(s => `
    <div class="flex justify-between">
      <span class="text-slate-400 capitalize">${s.name}</span>
      <span class="num"><span class="text-slate-200">$${s.cost_usd.toFixed(2)}</span>
        <span class="text-slate-500"> (${fmt(s.calls)})</span></span>
    </div>
  `).join("");

  const all = totals?.all ?? 0;
  const pending = totals?.pending ?? 0;
  const processed = all - pending;
  let projected = "—";
  if (processed > 0 && all > 0 && cost.spent_usd > 0) {
    projected = `$${(cost.spent_usd * all / processed).toFixed(2)}`;
  }
  document.getElementById("cost-projected").textContent = projected;
}

function renderBackends(backends) {
  const order = [
    ["racknerd", "Racknerd"],
    ["bbops",    "bbops"],
    ["zuhal",    "Zuhal"],
  ];
  const container = document.getElementById("backends");
  container.innerHTML = order.map(([k, label]) => {
    const b = backends[k] || {};
    const total = b.total || 0;
    const verdictOrder = ["valid","catch_all","dual_valid","dual_catch_all","invalid","blocked","error","unknown","not_run","ms_valid"];
    const pills = verdictOrder
      .filter(v => b[v])
      .map(v => `<span class="pill pill-${v} mr-1">${v}: ${fmt(b[v])}</span>`)
      .join("");
    const errPct = b.error_pct ?? 0;
    const errCls = errPct > 70 ? "text-rose-300" : errPct > 40 ? "text-amber-300" : "text-emerald-300";
    return `
      <div>
        <div class="flex justify-between items-baseline text-sm mb-1">
          <span class="font-semibold">${label}</span>
          <span class="text-xs text-slate-400 num">
            ${fmt(total)} probes · err <span class="${errCls}">${fmtPct(errPct)}</span>
          </span>
        </div>
        <div class="text-xs">${pills || '<span class="text-slate-500">no data in window</span>'}</div>
      </div>
    `;
  }).join("");
}

function renderDiscovery(d) {
  document.getElementById("disc-dns").textContent = fmt(d.dns);
  document.getElementById("disc-serper").textContent = fmt(d.serper);
  document.getElementById("disc-failed").textContent = fmt(d.failed);
  const total = d.total_input || (d.dns + d.serper + d.failed) || 0;
  const p = (n) => total ? (n / total * 100) : 0;
  document.getElementById("disc-share-dns").style.width    = `${p(d.dns)}%`;
  document.getElementById("disc-share-serper").style.width = `${p(d.serper)}%`;
  document.getElementById("disc-share-failed").style.width = `${p(d.failed)}%`;
  document.getElementById("disc-pct-dns").textContent      = p(d.dns).toFixed(1);
  document.getElementById("disc-pct-serper").textContent   = p(d.serper).toFixed(1);
  document.getElementById("disc-pct-failed").textContent   = p(d.failed).toFixed(1);
  document.getElementById("disc-hit-rate").textContent     = (d.hit_rate_pct ?? 0).toFixed(1);
  document.getElementById("disc-total").textContent        = fmt(total);
}

function renderThroughputStats(series) {
  if (!series.length) {
    ["tp-peak","tp-avg","tp-15m","tp-trend"].forEach(id => document.getElementById(id).textContent = "—");
    return;
  }
  const counts = series.map(p => p.count);
  const peak = Math.max(...counts);
  const avg = counts.reduce((a,b) => a + b, 0) / counts.length;
  const last15 = counts.slice(-15).reduce((a,b) => a + b, 0);
  const prior30 = counts.slice(-45, -15);
  const recent15 = counts.slice(-15);
  const priorAvg = prior30.length ? prior30.reduce((a,b) => a + b, 0) / prior30.length : 0;
  const recentAvg = recent15.reduce((a,b) => a + b, 0) / recent15.length;
  let trend = "—";
  if (priorAvg > 0) {
    const delta = ((recentAvg - priorAvg) / priorAvg) * 100;
    const arrow = delta > 2 ? "↑" : delta < -2 ? "↓" : "→";
    const cls = delta > 2 ? "text-emerald-300" : delta < -2 ? "text-rose-300" : "text-slate-300";
    trend = `<span class="${cls}">${arrow} ${Math.abs(delta).toFixed(0)}%</span>`;
  }
  document.getElementById("tp-peak").textContent = fmt(peak);
  document.getElementById("tp-avg").textContent  = avg.toFixed(1);
  document.getElementById("tp-15m").textContent  = fmt(last15);
  document.getElementById("tp-trend").innerHTML  = trend;
}

const RUN_COLORS = [
  { key: "valid",       label: "valid",       color: "rgba(16,185,129,0.85)",  border: "rgba(16,185,129,1)" },
  { key: "catch_all",   label: "catch_all",   color: "rgba(245,158,11,0.80)",  border: "rgba(245,158,11,1)" },
  { key: "invalid",     label: "invalid",     color: "rgba(244,63,94,0.75)",   border: "rgba(244,63,94,1)" },
  { key: "errored",     label: "error",       color: "rgba(100,116,139,0.70)", border: "rgba(100,116,139,1)" },
  { key: "disc_failed", label: "discovery",   color: "rgba(127,29,29,0.65)",   border: "rgba(127,29,29,1)" },
];

function hourLabel(hour) {
  return hour ? hour.slice(5, 13).replace("T", " ") : "";
}

function renderRunHistory(rows) {
  if (!rows.length) {
    document.getElementById("run-started").textContent = "—";
    document.getElementById("run-elapsed").textContent = "—";
    document.getElementById("run-buckets").textContent = "—";
    return;
  }
  const last = rows[rows.length - 1];
  const key = `${rows.length}|${last.hour}|${last.valid}|${last.catch_all}|${last.invalid}|${last.errored}|${last.disc_failed}`;

  document.getElementById("run-started").textContent = rows[0].hour.replace("T", " ");
  document.getElementById("run-elapsed").textContent = `${rows.length}h`;
  document.getElementById("run-buckets").textContent = rows.length;

  if (runChartKey === key && runChart) return;
  runChartKey = key;

  const labels = rows.map(r => hourLabel(r.hour));

  if (runChart) {
    runChart.data.labels = labels;
    RUN_COLORS.forEach((s, i) => {
      runChart.data.datasets[i].data = rows.map(r => r[s.key] || 0);
    });
    runChart.update("none");
  } else {
    const ctx = document.getElementById("run-history-chart").getContext("2d");
    runChart = new Chart(ctx, {
      type: "line",
      data: {
        labels,
        datasets: RUN_COLORS.map(s => ({
          label: s.label,
          data: rows.map(r => r[s.key] || 0),
          backgroundColor: s.color,
          borderColor: s.border,
          borderWidth: 1,
          fill: true,
          pointRadius: 0,
          tension: 0.25,
        })),
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        interaction: { mode: "index", intersect: false },
        plugins: { legend: { display: false }, tooltip: { intersect: false } },
        scales: {
          x: { stacked: true, ticks: { color: "#64748b", maxTicksLimit: 12, autoSkip: true }, grid: { display: false } },
          y: { stacked: true, ticks: { color: "#64748b" }, grid: { color: "rgba(100,116,139,0.12)" }, beginAtZero: true },
        },
      },
    });
  }
}

function relativeTime(iso) {
  if (!iso) return "—";
  const t = Date.parse(iso.replace(" ", "T") + (iso.includes("Z") ? "" : "Z"));
  const s = Math.max(0, (Date.now() - t) / 1000);
  if (s < 60)  return `${Math.floor(s)}s`;
  if (s < 3600) return `${Math.floor(s/60)}m`;
  return `${Math.floor(s/3600)}h`;
}

function pill(v) {
  if (!v) return "—";
  return `<span class="pill pill-${v}">${v}</span>`;
}

function renderRecent(rows) {
  const tb = document.getElementById("recent-validated");
  tb.innerHTML = rows.map(r => `
    <tr class="border-t border-slate-800">
      <td class="text-slate-400">${r.unique_id || "—"}</td>
      <td class="truncate" style="max-width:240px">${r.candidate_email || "—"}</td>
      <td>${pill(r.racknerd_status)}</td>
      <td>${pill(r.bbops_status)}</td>
      <td>${pill(r.zuhal_status)}</td>
      <td class="text-slate-400">${relativeTime(r.updated_at)}</td>
    </tr>
  `).join("");
}

function renderErrors(rows) {
  const tb = document.getElementById("top-errors");
  if (!rows.length) {
    tb.innerHTML = `<tr><td colspan="3" class="text-slate-500 py-2">no recent errors</td></tr>`;
    return;
  }
  tb.innerHTML = rows.map(r => `
    <tr class="border-t border-slate-800">
      <td class="num text-right text-slate-300">${fmt(r.n)}</td>
      <td><span class="pill pill-error">${r.source}</span></td>
      <td class="text-slate-300">${(r.message || "").replace(/[<>&]/g, c => ({'<':'&lt;','>':'&gt;','&':'&amp;'}[c]))}</td>
    </tr>
  `).join("");
}

function renderThroughput(series) {
  const labels = series.map(p => p.minute);
  const data = series.map(p => p.count);
  if (chart) {
    chart.data.labels = labels;
    chart.data.datasets[0].data = data;
    chart.update("none");
    return;
  }
  const ctx = document.getElementById("throughput-chart").getContext("2d");
  chart = new Chart(ctx, {
    type: "bar",
    data: { labels, datasets: [{
      data, borderWidth: 0,
      backgroundColor: "rgba(34,211,238,0.75)",
    }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false }, tooltip: { intersect: false, mode: "index" } },
      scales: {
        x: { ticks: { color: "#64748b", maxTicksLimit: 8 }, grid: { display: false } },
        y: { ticks: { color: "#64748b" }, grid: { color: "rgba(100,116,139,0.15)" }, beginAtZero: true },
      },
    },
  });
}

async function tick() {
  setIndicator("polling", "fetching…");
  try {
    const r = await fetch("/api/snapshot", { cache: "no-store" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const s = await r.json();
    lastFetchOk = Date.now();
    document.getElementById("run-id").textContent = s.run_id || "—";
    document.getElementById("as-of").textContent = (s.as_of || "").replace("T", " ");
    document.getElementById("build-ms").textContent = s.build_ms ?? "—";
    document.getElementById("poll-status").textContent = "live";
    setIndicator("fresh", `live · ${s.as_of || ""}`);
    renderStates(s.states || {});
    renderRate(s.rate || {}, s.totals || {});
    renderCost(s.cost || { spent_usd: 0 }, s.cost_breakdown || { services: [] }, s.totals || {});
    renderBackends(s.backends || {});
    renderDiscovery(s.discovery || {});
    renderThroughput(s.throughput_60min || []);
    renderThroughputStats(s.throughput_60min || []);
    renderRunHistory(s.run_history || []);
    renderRecent(s.recent_validated || []);
    renderErrors(s.top_recent_errors || []);
  } catch (e) {
    document.getElementById("poll-status").textContent = "stale";
    setIndicator("stale", `fetch error: ${e.message || e}`);
  }
}

tick();
setInterval(tick, POLL_MS);
// Mark stale (dot only) when no successful fetch in >STALE_MS.
setInterval(() => {
  if (!lastFetchOk) return;
  const age = Date.now() - lastFetchOk;
  if (age > STALE_MS) setIndicator("stale", `snapshot ${(age/1000).toFixed(0)} s old`);
}, 1000);
