/* 信息 / 价格异动实时监控 · 前端（原生 JS + EventSource，无构建） */
'use strict';

const $ = (s, r) => (r || document).querySelector(s);
const $$ = (s, r) => Array.from((r || document).querySelectorAll(s));

let es = null;
let lastEventId = null;
let reconnectDelay = 1000;
let feedItems = [];
const MAX_FEED = 300;

/* ---------- 工具 ---------- */
function pad(n) { return String(n).padStart(2, '0'); }
function fmtTime(ms) {
  if (!ms) return '—';
  const d = new Date(ms);
  return pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + ' ' +
         pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
}
function fmtAgo(ms) {
  if (!ms) return '—';
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return s + 's 前';
  if (s < 3600) return Math.floor(s / 60) + 'm 前';
  return Math.floor(s / 3600) + 'h 前';
}
async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(path + ' -> ' + r.status);
  return r.json();
}
function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}

/* ---------- 标签页 ---------- */
$$('.tab').forEach(function (t) {
  t.addEventListener('click', function () {
    $$('.tab').forEach(function (x) { x.classList.remove('active'); });
    $$('.view').forEach(function (x) { x.classList.remove('active'); });
    t.classList.add('active');
    $('#view-' + t.dataset.view).classList.add('active');
    if (t.dataset.view === 'quotes') refreshQuotes();
    if (t.dataset.view === 'compare') { loadCompareChips().then(drawCompare); }
    if (t.dataset.view === 'tasks') loadTasks();
    if (t.dataset.view === 'health') refreshHealth();
    if (t.dataset.view === 'history') loadHistory();
    if (t.dataset.view === 'replay') loadReplayTasks();
  });
});

/* ---------- SSE ---------- */
function setConn(ok) {
  const d = $('#conn-dot');
  d.className = 'dot ' + (ok ? 'ok' : 'bad');
  d.title = ok ? 'SSE 已连接' : 'SSE 断开，重连中';
}

function connect() {
  if (es) { try { es.close(); } catch (e) {} }
  const url = '/stream' + (lastEventId ? '?last=' + encodeURIComponent(lastEventId) : '');
  es = new EventSource(url);
  es.onopen = function () { setConn(true); reconnectDelay = 1000; loadRecentAlerts(); };
  es.onerror = function () {
    setConn(false);
    try { es.close(); } catch (e) {}
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 1.7, 15000);
  };
  const handler = function (e) {
    if (e.lastEventId) lastEventId = e.lastEventId;
    let msg;
    try { msg = JSON.parse(e.data); } catch (err) { return; }
    if (msg.type === 'hello') return;
    if (msg.type === 'alert') pushAlert(msg.data);
  };
  es.addEventListener('alert', handler);
  es.addEventListener('message', handler);
}

/* ---------- 告警流 ---------- */
function sevClass(s) { return (s === 'P1' || s === 'P2' || s === 'P3') ? s : 'P2'; }

function pushAlert(a) {
  // 声音 / 桌面通知 / 右下角提示：与筛选无关，任何新告警都提醒
  if (a.status !== 'resolved') {
    beep(a.severity);
    notify(a);
    toast(a);
  }
  const filter = $('#sev-filter').value;
  if (filter && a.severity !== filter) return;
  const li = document.createElement('li');
  const isResolved = a.status === 'resolved';
  li.className = sevClass(a.severity) + (isResolved ? ' resolved' : '');
  const p = a.payload || {};
  const linkHtml = p.link
    ? '<a href="' + escapeHtml(p.link) + '" target="_blank" rel="noopener" style="color:var(--accent)">原文</a>'
    : '';
  li.innerHTML =
    '<div class="head"><span class="title">' + escapeHtml(a.title) + '</span>' +
    '<span><span class="badge ' + sevClass(a.severity) + '">' + a.severity + '</span> ' +
    '<span class="t">' + fmtTime(a.event_time) + '</span></span></div>' +
    '<div class="detail">' + escapeHtml(a.detail || '') + ' ' + linkHtml + '</div>' +
    '<div class="detail">任务：' + escapeHtml(p.task_name || a.task_id || '—') +
    ' · 标的：' + escapeHtml(p.subject_name || a.subject || '—') +
    ' · ' + (isResolved ? '已恢复' : '触发') + '</div>';
  const feed = $('#alert-feed');
  feed.prepend(li);
  feedItems.push(li);
  while (feedItems.length > MAX_FEED) { const old = feedItems.shift(); old.remove(); }
  if ($('#auto-scroll').checked) feed.scrollTop = 0;
  $('#feed-hint').textContent = '最近更新 ' + new Date().toLocaleTimeString();
}

$('#sev-filter').addEventListener('change', function () {
  $('#alert-feed').innerHTML = '';
  feedItems = [];
  historyLoaded = false;
  loadRecentAlerts();
});
$('#clear-feed').addEventListener('click', function () { $('#alert-feed').innerHTML = ''; feedItems = []; });


/* ---------- 实时行情 ---------- */
let lastQuotePrices = {};

function fmtNum(v, cls) {
  if (v == null) return '—';
  if (cls === 'fx') return v.toFixed(4);
  if (cls === 'futures') return v.toFixed(2);
  return v.toFixed(2);
}

// 压缩连续相同价格：收盘后快照价格不变，若按时间等距抽样会得到一条平线。
// 这里保留价格变化点，让图形反映真实的日内走势形态。
function compactSeries(series) {
  if (!series || series.length < 3) return series || [];
  const out = [series[0]];
  for (let i = 1; i < series.length - 1; i++) {
    const prev = series[i - 1][1], cur = series[i][1], next = series[i + 1][1];
    if (cur !== prev || cur !== next) out.push(series[i]);
  }
  out.push(series[series.length - 1]);
  return out;
}

function drawChart(canvas, series, up) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 280, h = 74;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  if (!series || series.length < 2) {
    ctx.strokeStyle = '#2a3140'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(0, h / 2); ctx.lineTo(w, h / 2); ctx.stroke();
    return;
  }
  series = compactSeries(series);
  if (series.length < 2) {
    ctx.strokeStyle = '#2a3140'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(0, h / 2); ctx.lineTo(w, h / 2); ctx.stroke();
    return;
  }
  const vals = series.map(p => p[1]);
  let min = Math.min.apply(null, vals), max = Math.max.apply(null, vals);
  if (max === min) { max += 0.001; min -= 0.001; }
  const pad = (max - min) * 0.15;
  min -= pad; max += pad;
  const x = i => (i / (series.length - 1)) * (w - 2) + 1;
  const y = v => h - 4 - ((v - min) / (max - min)) * (h - 8);

  const color = up ? '#ff4d4f' : '#52c41a';
  // 面积
  const grad = ctx.createLinearGradient(0, 0, 0, h);
  grad.addColorStop(0, up ? 'rgba(255,77,79,.28)' : 'rgba(82,196,26,.28)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  ctx.beginPath();
  ctx.moveTo(x(0), h);
  series.forEach(function (p, i) { ctx.lineTo(x(i), y(p[1])); });
  ctx.lineTo(x(series.length - 1), h);
  ctx.closePath();
  ctx.fillStyle = grad; ctx.fill();
  // 线
  ctx.beginPath();
  series.forEach(function (p, i) {
    if (i === 0) ctx.moveTo(x(i), y(p[1])); else ctx.lineTo(x(i), y(p[1]));
  });
  ctx.strokeStyle = color; ctx.lineWidth = 1.6; ctx.lineJoin = 'round'; ctx.stroke();
  // 末点
  const lx = x(series.length - 1), ly = y(vals[vals.length - 1]);
  ctx.beginPath(); ctx.arc(lx, ly, 2.6, 0, Math.PI * 2);
  ctx.fillStyle = color; ctx.fill();
  // 昨收参考线（若有 prev_close，这里用序列均值近似中线）
  ctx.strokeStyle = 'rgba(139,149,167,.25)';
  ctx.setLineDash([3, 3]); ctx.lineWidth = 1;
  const mid = (max + min) / 2;
  ctx.beginPath(); ctx.moveTo(0, y(mid)); ctx.lineTo(w, y(mid)); ctx.stroke();
  ctx.setLineDash([]);
}

async function refreshQuotes() {
  const win = $('#q-window').value;
  try {
    const res = await api('/api/quotes?window=' + win);
    const grid = $('#quote-grid');
    if (!res.items.length) {
      grid.innerHTML = '<p class="muted">暂无行情数据（等待采集器首次成功）</p>';
      return;
    }
    // 首次渲染：建卡片
    // 排序：实时在前的排前，然后按类别聚合（指数 → 期货 → 外盘 → 外汇）
    const ORDER = { index: 0, futures: 1, global: 2, fx: 3 };
    res.items.sort(function (a, b) {
      if (!!a.live !== !!b.live) return a.live ? -1 : 1;
      const oa = ORDER[a.class] != null ? ORDER[a.class] : 9;
      const ob = ORDER[b.class] != null ? ORDER[b.class] : 9;
      if (oa !== ob) return oa - ob;
      return 0;
    });
    if (!grid.dataset.built || grid.children.length !== res.items.length) {
      grid.innerHTML = res.items.map(function (q) {
        return '<div class="qcard flat" data-subject="' + escapeHtml(q.subject) + '">' +
          '<div class="qhead"><span class="qname">' + escapeHtml(q.name) + '</span>' +
          '<span class="qstatus"></span></div>' +
          '<div class="qprice">—</div><div class="qchg">—</div>' +
          '<div class="qmeta"></div>' +
          '<div class="qspan" hidden></div>' +
          '<canvas class="qchart"></canvas></div>';
      }).join('');
      grid.dataset.built = '1';
    }
    res.items.forEach(function (q) {
      const card = grid.querySelector('[data-subject="' + q.subject + '"]');
      if (!card) return;
      const chg = q.change_pct == null ? 0 : q.change_pct;
      const dir = chg > 0 ? 'up' : (chg < 0 ? 'down' : 'flat');
      card.className = 'qcard ' + dir;
      card.querySelector('.qprice').textContent = fmtNum(q.price, q.class);
      const sign = chg > 0 ? '+' : '';
      card.querySelector('.qchg').textContent = sign + chg.toFixed(2) + '%';
      // 实时状态灯：<30s 绿点「实时」，否则灰点「已收盘 / 未更新」
      const statusEl = card.querySelector('.qstatus');
      if (q.live) {
        statusEl.innerHTML = '<span class="live-dot"></span><span style="color:var(--ok)">实时</span>';
      } else {
        const mins = Math.round((q.stale_s || 0) / 60);
        const label = mins >= 60 ? Math.round(mins / 60) + ' 小时' : mins + ' 分钟';
        statusEl.innerHTML = '<span class="idle-dot"></span><span style="color:var(--muted)">已收盘 · ' +
          label + '前</span>';
      }
      card.classList.toggle('is-live', !!q.live);
      let staleTag = '';
      card.querySelector('.qmeta').innerHTML =
        '昨收 <b>' + fmtNum(q.prev_close, q.class) + '</b>' +
        (q.high != null ? ' 高 <b>' + fmtNum(q.high, q.class) + '</b>' : '') +
        (q.low != null ? ' 低 <b>' + fmtNum(q.low, q.class) + '</b>' : '') +
        ' 更新 <b>' + fmtTime(q.updated_at) + '</b>' + staleTag;
      // 窗口被自动放大时明确提示，避免「我选了5分钟为什么画了一小时」的困惑
      const spanEl = card.querySelector('.qspan');
      if (spanEl) {
        // 只在「扩展后的跨度明显大于所选窗口」时提示，否则刚启动时点少会误报
        const reallyExpanded = q.window_expanded && q.actual_span_s &&
                               q.actual_span_s > (q.requested_window_s || 0) * 1.5 &&
                               q.actual_span_s > 600;
        if (reallyExpanded) {
          const mins = Math.round(q.actual_span_s / 60);
          const spanTxt = mins >= 60 ? (mins / 60).toFixed(1).replace('.0', '') + ' 小时' : mins + ' 分钟';
          spanEl.textContent = '所选窗口内数据点不足，已扩展至近 ' + spanTxt + ' 显示';
          spanEl.hidden = false;
        } else {
          spanEl.hidden = true;
        }
      }
      drawChart(card.querySelector('.qchart'), q.series, chg >= 0);
      // 价格变化闪烁
      if (lastQuotePrices[q.subject] != null && lastQuotePrices[q.subject] !== q.price) {
        card.classList.remove('qflash');
        void card.offsetWidth;
        card.classList.add('qflash');
      }
      lastQuotePrices[q.subject] = q.price;
    });
    $('#q-updated').textContent = '最后刷新 ' + new Date().toLocaleTimeString();
  } catch (e) { /* 静默 */ }
}

$('#q-window').addEventListener('change', refreshQuotes);



/* ---------- 声音提醒 + 桌面通知 ---------- */
let audioCtx = null;
let userInteracted = false;
document.addEventListener('click', function () { userInteracted = true; }, { once: true });
document.addEventListener('keydown', function () { userInteracted = true; }, { once: true });

// 用 WebAudio 合成提示音，不依赖音频文件（离线可用）
function beep(severity) {
  if (!$('#q-sound').checked) return;
  try {
    if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
    if (audioCtx.state === 'suspended') audioCtx.resume();
    // P1 三连高音，P2 两连中音，P3 单短音
    const seq = severity === 'P1' ? [[880, 0], [1100, 0.18], [1320, 0.36]]
              : severity === 'P2' ? [[660, 0], [880, 0.16]]
              : [[520, 0]];
    seq.forEach(function (pair) {
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.type = 'sine';
      osc.frequency.value = pair[0];
      const t = audioCtx.currentTime + pair[1];
      gain.gain.setValueAtTime(0.0001, t);
      gain.gain.exponentialRampToValueAtTime(0.18, t + 0.01);
      gain.gain.exponentialRampToValueAtTime(0.0001, t + 0.14);
      osc.connect(gain); gain.connect(audioCtx.destination);
      osc.start(t); osc.stop(t + 0.16);
    });
  } catch (e) { /* 音频不可用则静默 */ }
}

function notify(a) {
  if (!$('#q-notify').checked) return;
  if (!('Notification' in window)) return;
  const show = function () {
    try {
      const n = new Notification('[盯盘] ' + a.severity + ' ' + a.title, {
        body: (a.detail || '') + '\n' + (a.payload && a.payload.task_name || ''),
        tag: a.alert_id,          // 同 id 只显示一次，避免刷屏
        requireInteraction: a.severity === 'P1',
        silent: true,             // 声音由 beep() 负责，避免双重响
      });
      n.onclick = function () { window.focus(); n.close(); };
      setTimeout(function () { try { n.close(); } catch (e) {} }, 15000);
    } catch (e) {}
  };
  if (Notification.permission === 'granted') show();
  else if (Notification.permission !== 'denied') {
    Notification.requestPermission().then(function (p) { if (p === 'granted') show(); });
  }
}

function toast(a) {
  const el = document.createElement('div');
  el.className = 'toast ' + (a.severity || 'P2');
  el.innerHTML = '<div class="tt">' + escapeHtml(a.severity) + ' · ' + escapeHtml(a.title) + '</div>' +
                 '<div class="muted" style="font-size:12px">' + escapeHtml(a.detail || '') + '</div>';
  document.body.appendChild(el);
  setTimeout(function () { el.remove(); }, 6000);
}

// 首次交互后请求通知权限，并把状态显示在开关旁
function updateNotifyState() {
  const el = $('#q-notify');
  if (!el) return;
  if (!('Notification' in window)) {
    el.disabled = true;
    el.parentElement.title = '当前浏览器不支持桌面通知';
    return;
  }
  const p = Notification.permission;
  if (p === 'denied') {
    el.checked = false;
    el.parentElement.title = '通知权限已被拒绝，请在浏览器地址栏左侧的锁图标里手动允许';
  } else if (p === 'granted') {
    el.parentElement.title = '桌面通知已开启';
  } else {
    el.parentElement.title = '点击页面任意位置以授权桌面通知';
  }
}
updateNotifyState();

document.addEventListener('click', function () {
  if ('Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission().then(function () {
      setTimeout(updateNotifyState, 200);
    });
  }
}, { once: true });

/* ---------- K 线（蜡烛图 + 均线 + 副图） ---------- */
let selectedSubject = null;

function drawSubChart(canvas, data, kind, bars) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 800, h = 90;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  if (!bars.length || kind === 'none') return;

  const PAD_L = 8, PAD_R = 56, PAD_T = 6, PAD_B = 4;
  const plotW = w - PAD_L - PAD_R, plotH = h - PAD_T - PAD_B;
  const x = i => PAD_L + (i + 0.5) * (plotW / bars.length);

  if (kind === 'vol') {
    const maxV = Math.max.apply(null, bars.map(b => b.v || 0)) || 1;
    const bw = Math.max(1, Math.min(9, (plotW / bars.length) * 0.66));
    const vy = v => PAD_T + plotH - (v / maxV) * plotH;
    bars.forEach(function (b, i) {
      const up = b.c >= b.o;
      const hh = Math.max(1, ((b.v || 0) / maxV) * plotH);
      ctx.fillStyle = up ? 'rgba(255,77,79,.65)' : 'rgba(82,196,26,.65)';
      ctx.fillRect(x(i) - bw / 2, PAD_T + plotH - hh, bw, hh);
    });
    // 成交量均线 VMA5 / VMA10
    const vmaColors = { vma5: '#faad14', vma10: '#4c8dff' };
    ['vma5', 'vma10'].forEach(function (k) {
      const arr = (data.vma && data.vma[k]) || [];
      ctx.beginPath();
      let started = false;
      arr.forEach(function (v, i) {
        if (v == null) return;
        const yy = vy(v);
        if (!started) { ctx.moveTo(x(i), yy); started = true; } else ctx.lineTo(x(i), yy);
      });
      if (started) { ctx.strokeStyle = vmaColors[k]; ctx.lineWidth = 1.2; ctx.stroke(); }
    });
    ctx.fillStyle = '#8b95a7'; ctx.font = '9px Consolas,monospace';
    ctx.fillText('VOL  VMA5', PAD_L, 10);
    return;
  }

  if (kind === 'boll') {
    // BOLL 画在主图上，副图留空提示
    ctx.fillStyle = '#8b95a7'; ctx.font = '11px sans-serif';
    ctx.fillText('BOLL 已叠加到主图（上轨/中轨/下轨）', PAD_L, h / 2);
    return;
  }

  if (kind === 'macd') {
    const hist = data.macd.hist, dif = data.macd.dif, dea = data.macd.dea;
    const vals = [].concat(hist.filter(v => v != null), dif.filter(v => v != null),
                            dea.filter(v => v != null), [0]);
    const mn = Math.min.apply(null, vals), mx = Math.max.apply(null, vals);
    const rng = (mx - mn) || 1;
    const y = v => PAD_T + (1 - (v - mn) / rng) * plotH;
    const zero = y(0);
    ctx.strokeStyle = '#232a35';
    ctx.beginPath(); ctx.moveTo(PAD_L, zero); ctx.lineTo(PAD_L + plotW, zero); ctx.stroke();
    const bw = Math.max(1, Math.min(7, (plotW / bars.length) * 0.6));
    hist.forEach(function (v, i) {
      if (v == null) return;
      ctx.fillStyle = v >= 0 ? 'rgba(255,77,79,.8)' : 'rgba(82,196,26,.8)';
      const y0 = y(v), top = Math.min(y0, zero), bh = Math.max(1, Math.abs(y0 - zero));
      ctx.fillRect(x(i) - bw / 2, top, bw, bh);
    });
    [['#faad14', dif], ['#4c8dff', dea]].forEach(function (pair) {
      ctx.beginPath();
      let started = false;
      pair[1].forEach(function (v, i) {
        if (v == null) return;
        if (!started) { ctx.moveTo(x(i), y(v)); started = true; } else ctx.lineTo(x(i), y(v));
      });
      if (started) { ctx.strokeStyle = pair[0]; ctx.lineWidth = 1.2; ctx.stroke(); }
    });
    ctx.fillStyle = '#8b95a7'; ctx.font = '9px Consolas,monospace';
    ctx.fillText('MACD', PAD_L, 10);
    return;
  }

  if (kind === 'kdj') {
    const ks = data.kdj.k, ds = data.kdj.d, js = data.kdj.j;
    const y = v => PAD_T + (1 - v / 100) * plotH;
    ctx.strokeStyle = '#232a35';
    [20, 50, 80].forEach(function (lv) {
      const yy = y(lv);
      ctx.beginPath(); ctx.moveTo(PAD_L, yy); ctx.lineTo(PAD_L + plotW, yy); ctx.stroke();
    });
    [['#faad14', ks], ['#4c8dff', ds], ['#b37feb', js]].forEach(function (pair) {
      ctx.beginPath();
      let started = false;
      pair[1].forEach(function (v, i) {
        if (v == null) return;
        const yy = y(Math.max(0, Math.min(100, v)));
        if (!started) { ctx.moveTo(x(i), yy); started = true; } else ctx.lineTo(x(i), yy);
      });
      if (started) { ctx.strokeStyle = pair[0]; ctx.lineWidth = 1.2; ctx.stroke(); }
    });
    ctx.fillStyle = '#8b95a7'; ctx.font = '9px Consolas,monospace';
    ctx.fillText('KDJ', PAD_L, 10);
  }
}

function drawKLine(canvas, data, title) {
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 800, h = 260;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const bars = data.bars || [];
  if (bars.length < 2) {
    ctx.fillStyle = '#8b95a7';
    ctx.font = '13px sans-serif';
    ctx.fillText('数据不足，等待采集积累…', 20, h / 2);
    return;
  }

  const PAD_L = 8, PAD_R = 56, PAD_T = 10, PAD_B = 22;
  const plotW = w - PAD_L - PAD_R;
  const plotH = h - PAD_T - PAD_B;

  // 价格范围（含均线）
  let min = Infinity, max = -Infinity;
  bars.forEach(b => { min = Math.min(min, b.l); max = Math.max(max, b.h); });
  ['ma5', 'ma10', 'ma20'].forEach(function (k) {
    (data.ma && data.ma[k] || []).forEach(function (v) {
      if (v != null) { min = Math.min(min, v); max = Math.max(max, v); }
    });
  });
  const pad = (max - min) * 0.08 || 0.01;
  min -= pad; max += pad;

  const x = i => PAD_L + (i + 0.5) * (plotW / bars.length);
  const y = v => PAD_T + (1 - (v - min) / (max - min)) * plotH;
  const bw = Math.max(1, Math.min(9, (plotW / bars.length) * 0.66));

  // 网格 + 右侧价格轴
  ctx.strokeStyle = '#232a35'; ctx.lineWidth = 1;
  ctx.fillStyle = '#8b95a7'; ctx.font = '10px Consolas,monospace';
  for (let i = 0; i <= 4; i++) {
    const v = min + (max - min) * i / 4;
    const yy = y(v);
    ctx.beginPath(); ctx.moveTo(PAD_L, yy); ctx.lineTo(PAD_L + plotW, yy); ctx.stroke();
    ctx.fillText(v.toFixed(v > 1000 ? 0 : 2), PAD_L + plotW + 5, yy + 3);
  }

  // 蜡烛
  bars.forEach(function (b, i) {
    const up = b.c >= b.o;
    const color = up ? '#ff4d4f' : '#52c41a';
    const xc = x(i);
    ctx.strokeStyle = color; ctx.fillStyle = color;
    // 影线
    ctx.beginPath();
    ctx.moveTo(xc, y(b.h)); ctx.lineTo(xc, y(b.l));
    ctx.lineWidth = 1; ctx.stroke();
    // 实体
    const yo = y(b.o), yc = y(b.c);
    const top = Math.min(yo, yc), bh = Math.max(1, Math.abs(yc - yo));
    ctx.fillRect(xc - bw / 2, top, bw, bh);
  });

  // BOLL 布林带（若选中）
  if (document.querySelector('#k-indicator') && document.querySelector('#k-indicator').value === 'boll') {
    const bl = data.boll || {};
    [['up', 'rgba(179,127,235,.7)'], ['mid', 'rgba(139,149,167,.8)'], ['low', 'rgba(179,127,235,.7)']]
      .forEach(function (pair) {
        const arr = bl[pair[0]] || [];
        ctx.beginPath();
        let started = false;
        arr.forEach(function (v, i) {
          if (v == null) return;
          if (!started) { ctx.moveTo(x(i), y(v)); started = true; } else ctx.lineTo(x(i), y(v));
        });
        if (started) {
          ctx.strokeStyle = pair[1]; ctx.lineWidth = 1.1;
          if (pair[0] !== 'mid') ctx.setLineDash([3, 3]);
          ctx.stroke();
          ctx.setLineDash([]);
        }
      });
  }

  // 均线
  const maColors = { ma5: '#faad14', ma10: '#4c8dff', ma20: '#b37feb' };
  ['ma5', 'ma10', 'ma20'].forEach(function (k) {
    const arr = (data.ma && data.ma[k]) || [];
    ctx.beginPath();
    let started = false;
    arr.forEach(function (v, i) {
      if (v == null) return;
      if (!started) { ctx.moveTo(x(i), y(v)); started = true; }
      else ctx.lineTo(x(i), y(v));
    });
    if (started) {
      ctx.strokeStyle = maColors[k]; ctx.lineWidth = 1.4; ctx.stroke();
    }
  });

  // 时间轴（首尾）
  ctx.fillStyle = '#8b95a7'; ctx.font = '10px sans-serif';
  const p2 = n => (n < 10 ? '0' : '') + n;
  const fmt = ts => {
    const d = new Date(ts);
    return p2(d.getHours()) + ':' + p2(d.getMinutes());
  };
  ctx.fillText(fmt(bars[0].t), PAD_L, h - 6);
  const lastTxt = fmt(bars[bars.length - 1].t);
  ctx.fillText(lastTxt, PAD_L + plotW - ctx.measureText(lastTxt).width, h - 6);

  // 副图
  const sub = document.querySelector('#k-sub');
  if (sub) drawSubChart(sub, data, document.querySelector('#k-indicator').value, bars);
}

async function loadKLine(subject, name) {
  const period = $('#q-period').value;
  const panel = $('#kline-panel');
  panel.hidden = false;
  $('#k-title').textContent = name;
  $('#k-meta').textContent = '加载中…';
  try {
    const d = await api('/api/kline?subject=' + encodeURIComponent(subject) +
                        '&period=' + period + '&limit=180');
    const bars = d.bars || [];
    $('#k-meta').textContent = bars.length + ' 根 · ' +
      ({60:'1分钟',300:'5分钟',900:'15分钟',3600:'1小时'}[period] || period);
    drawKLine($('#k-chart'), d, name);
  } catch (e) {
    $('#k-meta').textContent = '加载失败';
  }
}

document.addEventListener('click', function (e) {
  const card = e.target.closest('.qcard');
  if (!card) return;
  const subj = card.dataset.subject;
  const name = card.querySelector('.qname').textContent;
  document.querySelectorAll('.qcard').forEach(c => c.classList.remove('sel'));
  card.classList.add('sel');
  selectedSubject = subj;
  loadKLine(subj, name);
});

$('#k-close').addEventListener('click', function () {
  $('#kline-panel').hidden = true;
  document.querySelectorAll('.qcard').forEach(c => c.classList.remove('sel'));
  selectedSubject = null;
});

$('#q-notify').addEventListener('change', function () {
  if (this.checked && 'Notification' in window && Notification.permission === 'default') {
    Notification.requestPermission().then(updateNotifyState);
  } else if (this.checked && Notification.permission === 'denied') {
    alert('桌面通知权限已被拒绝。\n\n请点击浏览器地址栏左侧的锁图标 → 通知 → 允许，然后刷新页面。');
    this.checked = false;
  }
});

$('#k-indicator').addEventListener('change', function () {
  if (selectedSubject) {
    const card = document.querySelector('.qcard[data-subject="' + selectedSubject + '"]');
    loadKLine(selectedSubject, card ? card.querySelector('.qname').textContent : selectedSubject);
  }
});

$('#q-period').addEventListener('change', function () {
  if (selectedSubject) {
    const card = document.querySelector('.qcard[data-subject="' + selectedSubject + '"]');
    loadKLine(selectedSubject, card ? card.querySelector('.qname').textContent : selectedSubject);
  }
});


/* ---------- 多标的对比 ---------- */
let compareSelection = [];
let compareQuotes = [];

const COMPARE_COLORS = ['#ff4d4f', '#4c8dff', '#faad14', '#52c41a', '#b37feb',
                        '#13c2c2', '#eb2f96', '#fa8c16', '#a0d911', '#2f54eb'];

async function loadCompareChips() {
  try {
    const res = await api('/api/quotes?window=86400');
    compareQuotes = res.items;
    const box = $('#c-chips');
    box.innerHTML = res.items.map(function (q) {
      const on = compareSelection.indexOf(q.subject) >= 0;
      return '<span class="chip' + (on ? ' on' : '') + '" data-subject="' +
        escapeHtml(q.subject) + '">' + escapeHtml(q.name) + '</span>';
    }).join('');
  } catch (e) {}
}

$('#c-chips').addEventListener('click', function (e) {
  const chip = e.target.closest('.chip');
  if (!chip) return;
  const subj = chip.dataset.subject;
  const i = compareSelection.indexOf(subj);
  if (i >= 0) compareSelection.splice(i, 1);
  else {
    if (compareSelection.length >= 10) { alert('最多同时对比 10 个标的'); return; }
    compareSelection.push(subj);
  }
  chip.classList.toggle('on');
  drawCompare();
});

function drawCompare() {
  const canvas = $('#c-chart');
  const legend = $('#c-legend');
  if (!compareSelection.length) {
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    canvas.width = canvas.clientWidth * dpr;
    canvas.height = 380 * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, canvas.clientWidth, 380);
    ctx.fillStyle = '#8b95a7'; ctx.font = '13px sans-serif';
    ctx.fillText('点击上方标签选择要对比的标的（可多选）', 20, 190);
    legend.innerHTML = '';
    return;
  }
  api('/api/compare?subjects=' + encodeURIComponent(compareSelection.join(',')) +
      '&period=' + $('#c-period').value + '&limit=' + ($('#c-limit').value || 200))
    .then(function (data) {
      renderCompare(canvas, legend, data);
    }).catch(function () {});
}

function renderCompare(canvas, legend, data) {
  const items = (data.items || []).filter(x => x.series && x.series.length > 1);
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth || 900, h = 380;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  if (!items.length) {
    ctx.fillStyle = '#8b95a7'; ctx.font = '13px sans-serif';
    ctx.fillText('所选标的暂无足够数据', 20, h / 2);
    legend.innerHTML = '';
    return;
  }

  const PAD_L = 10, PAD_R = 56, PAD_T = 12, PAD_B = 22;
  const plotW = w - PAD_L - PAD_R, plotH = h - PAD_T - PAD_B;

  let mn = Infinity, mx = -Infinity;
  items.forEach(it => it.series.forEach(p => {
    mn = Math.min(mn, p[1]); mx = Math.max(mx, p[1]);
  }));
  const pad = (mx - mn) * 0.08 || 1;
  mn -= pad; mx += pad;

  const maxLen = Math.max.apply(null, items.map(it => it.series.length));
  const x = i => PAD_L + (i / (maxLen - 1)) * plotW;
  const y = v => PAD_T + (1 - (v - mn) / (mx - mn)) * plotH;

  // 网格
  ctx.strokeStyle = '#232a35'; ctx.lineWidth = 1;
  ctx.fillStyle = '#8b95a7'; ctx.font = '10px Consolas,monospace';
  for (let i = 0; i <= 4; i++) {
    const v = mn + (mx - mn) * i / 4, yy = y(v);
    ctx.beginPath(); ctx.moveTo(PAD_L, yy); ctx.lineTo(PAD_L + plotW, yy); ctx.stroke();
    ctx.fillText(v.toFixed(1), PAD_L + plotW + 5, yy + 3);
  }
  // 基准线 100
  const y100 = y(100);
  ctx.strokeStyle = 'rgba(139,149,167,.45)';
  ctx.setLineDash([4, 4]);
  ctx.beginPath(); ctx.moveTo(PAD_L, y100); ctx.lineTo(PAD_L + plotW, y100); ctx.stroke();
  ctx.setLineDash([]);

  // 各标的曲线
  items.forEach(function (it, idx) {
    const color = COMPARE_COLORS[idx % COMPARE_COLORS.length];
    ctx.beginPath();
    it.series.forEach(function (p, i) {
      if (i === 0) ctx.moveTo(x(i), y(p[1])); else ctx.lineTo(x(i), y(p[1]));
    });
    ctx.strokeStyle = color; ctx.lineWidth = 1.7; ctx.stroke();
  });

  // 图例
  legend.innerHTML = items.map(function (it, idx) {
    const color = COMPARE_COLORS[idx % COMPARE_COLORS.length];
    const chg = it.change_pct;
    const chgTxt = chg == null ? '' :
      ' <b style="color:' + (chg >= 0 ? '#ff4d4f' : '#52c41a') + '">' +
      (chg > 0 ? '+' : '') + chg.toFixed(2) + '%</b>';
    return '<span><i style="background:' + color + '"></i>' + escapeHtml(it.name) + chgTxt + '</span>';
  }).join('');
}

$('#c-refresh').addEventListener('click', drawCompare);
$('#c-clear').addEventListener('click', function () {
  compareSelection = [];
  document.querySelectorAll('#c-chips .chip').forEach(c => c.classList.remove('on'));
  drawCompare();
});
$('#c-period').addEventListener('change', drawCompare);

/* ---------- 历史告警回填 ----------
 * 只看新推送是不够的：打开页面时若之前已经发生过告警，用户会以为「没消息」。
 * 这里在首次连接时拉一次最近告警，保证看板与库内状态一致。
 */
let historyLoaded = false;
async function loadRecentAlerts() {
  if (historyLoaded) return;
  historyLoaded = true;
  try {
    const res = await api('/api/alerts?limit=50');
    const feed = $('#alert-feed');
    const items = res.items.slice().reverse();   // 旧的先渲染，新的在上
    items.forEach(function (a) {
      const filter = $('#sev-filter').value;
      if (filter && a.severity !== filter) return;
      const li = document.createElement('li');
      const isResolved = a.status === 'resolved';
      const isDigest = a.status === 'digest';
      li.className = sevClass(a.severity) + (isResolved ? ' resolved' : '');
      const p = a.payload || {};
      const linkHtml = p.link
        ? '<a href="' + escapeHtml(p.link) + '" target="_blank" rel="noopener" style="color:var(--accent)">原文</a>'
        : '';
      const tag = isResolved ? '已恢复' : (isDigest ? '摘要' : '触发');
      li.innerHTML =
        '<div class="head"><span class="title">' + escapeHtml(a.title) + '</span>' +
        '<span><span class="badge ' + sevClass(a.severity) + '">' + a.severity + '</span> ' +
        '<span class="t">' + fmtTime(a.event_time) + '</span></span></div>' +
        '<div class="detail">' + escapeHtml(a.detail || '') + ' ' + linkHtml + '</div>' +
        '<div class="detail">任务：' + escapeHtml(p.task_name || a.task_id || '—') +
        ' · 标的：' + escapeHtml(p.subject_name || a.subject || '—') + ' · ' + tag + '</div>';
      feed.prepend(li);
      feedItems.push(li);
    });
    if (items.length) {
      $('#feed-hint').textContent = '已加载最近 ' + items.length + ' 条历史告警，等待新告警…';
    }
  } catch (e) { /* 静默：历史加载失败不应阻断实时流 */ }
}

/* ---------- 状态轮询 ---------- */
async function refreshStatus() {
  try {
    const h = await api('/api/health');
    const hb = h.heartbeat;
    const mode = $('#mode-pill');
    mode.textContent = '数据模式：' + h.data_mode;
    mode.className = 'pill';

    const lat = hb.end_to_end_latency_ms;
    const age = hb.ingest_age_ms;
    const lp = $('#latency-pill');
    lp.textContent = '链路时延：' + (age == null ? '—' : (age / 1000).toFixed(1) + 's') +
      ' · 数据时延：' + (lat == null ? '—' : (lat / 1000).toFixed(1) + 's');
    lp.title = '链路时延=最新事件入库到现在的间隔；数据时延=数据源标注时间到现在的间隔（新闻天然滞后）';
    lp.className = 'pill' + (age == null ? '' : age > 60000 ? ' bad' : age > 15000 ? ' warn' : '');

    $('#uptime-pill').textContent = '运行：' + Math.floor(h.uptime_s / 60) + 'm' + (h.uptime_s % 60) + 's';

    const degraded = hb.degraded || [];
    const banner = $('#degrade-banner');
    if (degraded.length || hb.pipeline_state !== 'OK') {
      const parts = [];
      if (degraded.length) {
        const names = hb.sources.filter(function (s) { return degraded.indexOf(s.source) >= 0; })
          .map(function (s) { return s.source + '(' + s.state + '，最后成功 ' + fmtAgo(s.last_success) + ')'; });
        parts.push('数据源降级：' + names.join('、'));
      }
      if (hb.pipeline_state !== 'OK') parts.push('计算链路 ' + hb.pipeline_age_s + 's 无进展');
      banner.hidden = false;
      banner.textContent = '⚠ ' + parts.join('；') + ' —— 当前看到的「无告警」不代表正常';
    } else {
      banner.hidden = true;
    }

    const st = await api('/api/stats');
    $('#stat-events').textContent = st.events_total;
    $('#stat-alerts').textContent = st.alerts_total;
    $('#stat-p1').textContent = st.alerts_by_severity.P1 || 0;
    $('#stat-p2').textContent = st.alerts_by_severity.P2 || 0;
    $('#stat-p3').textContent = st.alerts_by_severity.P3 || 0;
    $('#stat-clients').textContent = st.pipeline.sse_clients;
    $('#stat-wm').textContent = fmtTime(st.pipeline.watermark.watermark);
    const supp = await api('/api/suppressions');
    $('#stat-supp').textContent = supp.items.reduce(function (a, b) { return a + b.count; }, 0);
    renderSupp(supp.items);
  } catch (e) {
    setConn(false);
  }
}

function renderSupp(items) {
  const tb = $('#supp-table tbody');
  tb.innerHTML = items.map(function (i) {
    return '<tr><td><code>' + i.fingerprint.slice(0, 10) + '</code></td><td>' +
      escapeHtml(i.task_id || '') + '</td><td>' + i.count + '</td><td>' +
      fmtTime(i.last_at) + '</td><td>' + escapeHtml(i.reason || '') + '</td></tr>';
  }).join('');
}

/* ---------- 任务 ---------- */
async function loadTasks() {
  const res = await api('/api/tasks');
  const tasks = res.tasks;
  const box = $('#task-list');
  box.innerHTML = tasks.map(function (t) {
    const rules = (t.config.rules || []).map(function (r) {
      return '<span class="tag">' + r.type +
        (r.threshold != null ? ' ' + r.threshold : '') +
        (r.window_s ? '/' + r.window_s + 's' : '') + ' ' + (r.severity || '') + '</span>';
    }).join('');
    const subj = (t.config.subjects || t.config.sources || []).join(', ');
    return '<div class="card"><div class="hdr"><div><b>' + escapeHtml(t.name) + '</b> ' +
      '<span class="tag">' + (t.type === 'price' ? '价格' : '信息') + '</span> ' +
      '<span class="tag">' + (t.enabled ? '启用' : '停用') + '</span> ' +
      '<span class="tag">' + escapeHtml(t.id) + '</span></div><div>' +
      '<button class="ghost" onclick="editTask(\'' + t.id + '\')">编辑</button> ' +
      '<button class="ghost" onclick="toggleTask(\'' + t.id + '\',' + (!t.enabled) + ')">' +
      (t.enabled ? '停用' : '启用') + '</button> ' +
      '<button class="danger" onclick="delTask(\'' + t.id + '\')">删除</button>' +
      '</div></div>' +
      '<div class="rules">标的/来源：' + escapeHtml(subj || '—') +
      ' · 冷却 ' + (t.config.cooldown_s || '默认') + 's</div>' +
      '<div class="rules">' + (rules || '无规则') + '</div></div>';
  }).join('') || '<p class="muted">暂无任务</p>';
  window.__tasks = tasks;
}

window.editTask = async function (id) {
  const t = (window.__tasks || []).find(function (x) { return x.id === id; });
  if (!t) return;
  $('#task-editor').hidden = false;
  $('#f-id').value = t.id;
  $('#f-name').value = t.name;
  $('#f-type').value = t.type;
  $('#f-enabled').checked = t.enabled;
  $('#f-subjects').value = (t.config.subjects || t.config.sources || []).join(',');
  $('#f-rules').value = JSON.stringify(t.config.rules || [], null, 2);
  $('#f-cooldown').value = t.config.cooldown_s || 300;
};
window.toggleTask = async function (id, enabled) {
  await api('/api/tasks/' + id + '/toggle', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled: enabled })
  });
  loadTasks();
};
window.delTask = async function (id) {
  if (!confirm('确认删除任务 ' + id + '？')) return;
  await api('/api/tasks/' + id + '/delete', { method: 'POST' });
  loadTasks();
};

$('#new-task').addEventListener('click', function () {
  $('#task-editor').hidden = false;
  $('#f-id').value = ''; $('#f-name').value = ''; $('#f-type').value = 'price';
  $('#f-enabled').checked = true; $('#f-subjects').value = '';
  $('#f-rules').value = JSON.stringify([{ id: 'r1', type: 'change_pct', threshold: 1.0, severity: 'P2', enabled: true }], null, 2);
  $('#f-cooldown').value = 300;
});
$('#cancel-task').addEventListener('click', function () { $('#task-editor').hidden = true; });
$('#save-task').addEventListener('click', async function () {
  let rules;
  try { rules = JSON.parse($('#f-rules').value); }
  catch (e) { $('#task-msg').textContent = '规则 JSON 解析失败：' + e.message; return; }
  const type = $('#f-type').value;
  const list = $('#f-subjects').value.split(',').map(function (s) { return s.trim(); }).filter(Boolean);
  const cfg = { rules: rules, cooldown_s: Number($('#f-cooldown').value) || 300 };
  if (type === 'price') cfg.subjects = list; else cfg.sources = list;
  const payload = {
    id: $('#f-id').value.trim() || undefined,
    name: $('#f-name').value.trim(),
    type: type, enabled: $('#f-enabled').checked, config: cfg
  };
  if (!payload.name) { $('#task-msg').textContent = '名称必填'; return; }
  try {
    await api('/api/tasks', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    $('#task-msg').textContent = '已保存';
    $('#task-editor').hidden = true;
    loadTasks();
  } catch (e) { $('#task-msg').textContent = '保存失败：' + e.message; }
});

/* ---------- 健康 ---------- */
async function refreshHealth() {
  const h = await api('/api/health');
  const hb = h.heartbeat;
  const tb = $('#health-table tbody');
  tb.innerHTML = hb.sources.map(function (s) {
    return '<tr><td>' + s.source + '</td><td class="st-' + s.state + '">' + s.state + '</td>' +
      '<td>' + s.interval_s + 's</td><td>' + fmtTime(s.last_success) + '</td>' +
      '<td>' + (s.age_s == null ? '—' : s.age_s + 's') + '</td>' +
      '<td>' + (s.latency_ms == null ? '—' : s.latency_ms + 'ms') + '</td>' +
      '<td>' + s.event_count + '</td><td class="muted">' + escapeHtml(s.last_error || '') + '</td></tr>';
  }).join('');
  $('#guard-row').innerHTML =
    '<div class="stat"><label>守卫存活</label><b>' + (hb.guard_alive ? 'YES' : 'NO') + '</b></div>' +
    '<div class="stat"><label>心跳次数</label><b>' + hb.tick_count + '</b></div>' +
    '<div class="stat"><label>最后心跳</label><b>' + fmtAgo(hb.last_tick) + '</b></div>' +
    '<div class="stat"><label>计算链路</label><b class="st-' + hb.pipeline_state + '">' + hb.pipeline_state + '</b></div>' +
    '<div class="stat"><label>链路停滞</label><b>' + hb.pipeline_age_s + 's</b></div>' +
    '<div class="stat"><label>链路时延</label><b>' +
    (hb.ingest_age_ms == null ? '—' : (hb.ingest_age_ms / 1000).toFixed(1) + 's') + '</b></div>' +
    '<div class="stat"><label>数据时延</label><b>' +
    (hb.end_to_end_latency_ms == null ? '—' : (hb.end_to_end_latency_ms / 1000).toFixed(1) + 's') + '</b></div>';
}

/* ---------- 历史 ---------- */
async function loadHistory() {
  const limit = $('#h-limit').value || 100;
  const res = await api('/api/events?limit=' + limit);
  const kind = $('#h-kind').value;
  const tb = $('#history-table tbody');
  const rows = res.items.filter(function (i) { return !kind || i.kind === kind; });
  tb.innerHTML = rows.map(function (i) {
    const p = i.payload || {};
    let content;
    if (i.kind === 'quote') {
      content = (p.name || '') + ' 现价 ' + p.price +
        (p.change_pct != null ? ' (' + (p.change_pct > 0 ? '+' : '') + p.change_pct + '%)' : '');
    } else if (i.kind === 'baseline') {
      content = (p.name || '') + ' ' + p.days + '日基线：高 ' + p.n_high +
        ' / 低 ' + p.n_low + ' / 昨收 ' + p.prev_close +
        (p.provider ? '（来源 ' + p.provider + '）' : '');
    } else {
      content = p.title || JSON.stringify(p).slice(0, 80);
    }
    return '<tr><td>' + i.seq + '</td><td>' + i.source + '</td><td>' + i.kind + '</td><td>' +
      escapeHtml(i.subject || '') + '</td><td>' + fmtTime(i.event_time) + '</td><td>' +
      escapeHtml(content) + '</td></tr>';
  }).join('') || '<tr><td colspan="6" class="muted">无数据</td></tr>';
}
$('#h-refresh').addEventListener('click', loadHistory);
$('#h-kind').addEventListener('change', loadHistory);

/* ---------- 回放 ---------- */
async function loadReplayTasks() {
  const res = await api('/api/tasks');
  $('#r-task').innerHTML = res.tasks.map(function (t) {
    return '<option value="' + t.id + '">' + escapeHtml(t.name) + '</option>';
  }).join('');
}
function parseLocal(s) {
  if (!s) return null;
  const m = s.trim().match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})$/);
  if (!m) return null;
  return new Date(+m[1], +m[2] - 1, +m[3], +m[4], +m[5]).getTime();
}
$('#r-run').addEventListener('click', async function () {
  const taskId = $('#r-task').value;
  const from = parseLocal($('#r-from').value), to = parseLocal($('#r-to').value);
  if (!from || !to) { $('#replay-result').innerHTML = '<p class="muted">请填写时间，格式 2026-11-12 09:30</p>'; return; }
  let newCfg = null;
  const raw = $('#r-config').value.trim();
  if (raw) {
    try { newCfg = JSON.parse(raw); }
    catch (e) { $('#replay-result').innerHTML = '<p class="muted">新规则 JSON 解析失败</p>'; return; }
  }
  $('#replay-result').innerHTML = '<p class="muted">回放中…</p>';
  try {
    const res = await api('/api/replay', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ task_id: taskId, from: from, to: to, new_config: newCfg })
    });
    if (res.error) { $('#replay-result').innerHTML = '<p class="muted">' + escapeHtml(res.error) + '</p>'; return; }
    const fmtHits = function (arr) {
      return arr.length ? arr.slice(0, 30).map(function (h) {
        return '<div class="detail">' + fmtTime(h.event_time) + ' · ' + escapeHtml(h.rule) +
          ' · ' + escapeHtml(h.title) + '</div>';
      }).join('') : '<div class="muted">无</div>';
    };
    $('#replay-result').innerHTML =
      '<div class="res"><h4>回放结果：扫描 ' + res.events_scanned + ' 条事件</h4>' +
      '<div class="diff"><div><b>当前规则</b>：命中 ' + res.old_hits + '</div>' +
      '<div><b>新规则</b>：命中 ' + res.new_hits + '</div></div>' +
      '<div class="diff"><div><b>仅旧规则命中 (' + res.only_old.length + ')</b>' + fmtHits(res.only_old) + '</div>' +
      '<div><b>仅新规则命中 (' + res.only_new.length + ')</b>' + fmtHits(res.only_new) + '</div></div></div>';
  } catch (e) {
    $('#replay-result').innerHTML = '<p class="muted">回放失败：' + escapeHtml(e.message) + '</p>';
  }
});

/* ---------- 启动 ---------- */
connect();
refreshQuotes();
loadRecentAlerts();
refreshStatus();
setInterval(function () {
  if ($('#q-autorefresh').checked && $('#view-quotes').classList.contains('active')) refreshQuotes();
}, 2000);
// K 线每 5 秒刷新（比卡片慢，避免频繁重绘）
setInterval(function () {
  if (selectedSubject && !$('#kline-panel').hidden && $('#view-quotes').classList.contains('active')) {
    const card = document.querySelector('.qcard[data-subject="' + selectedSubject + '"]');
    loadKLine(selectedSubject, card ? card.querySelector('.qname').textContent : selectedSubject);
  }
}, 5000);
setInterval(refreshStatus, 3000);
setInterval(function () { if ($('#view-health').classList.contains('active')) refreshHealth(); }, 5000);
window.addEventListener('resize', function () { if ($('#view-quotes').classList.contains('active')) refreshQuotes(); });