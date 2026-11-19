/* Whale Watch — 개인용 주식 + 13F 거물 추적 앱 */
'use strict';

const $ = (s, el = document) => el.querySelector(s);
const view = $('#view');
const FH = 'https://finnhub.io/api/v1';

/* ---------------- storage ---------------- */
const store = {
  get(k, d) { try { const v = localStorage.getItem('ww.' + k); return v == null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { localStorage.setItem('ww.' + k, JSON.stringify(v)); } catch {} },
};
const getKey = () => store.get('fhKey', '');
const getWatch = () => store.get('watch', ['INSM', 'AVGO', 'MU', 'GOOGL']);
const setWatch = (w) => store.set('watch', w);

/* ---------------- format ---------------- */
const fmtUsd = (v, d = 2) => v == null || isNaN(v) ? '–' : '$' + Number(v).toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
function fmtBig(v) {
  if (v == null || isNaN(v)) return '–';
  const a = Math.abs(v), s = v < 0 ? '-' : '';
  if (a >= 1e12) return s + '$' + (a / 1e12).toFixed(2) + 'T';
  if (a >= 1e9) return s + '$' + (a / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return s + '$' + (a / 1e6).toFixed(1) + 'M';
  if (a >= 1e3) return s + '$' + (a / 1e3).toFixed(0) + 'K';
  return s + '$' + a.toFixed(0);
}
function fmtSh(v) {
  const a = Math.abs(v);
  if (a >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (a >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  return String(v);
}
const pct = (v, d = 2) => (v > 0 ? '+' : '') + Number(v).toFixed(d) + '%';
const cls = (v) => v > 0 ? 'up' : v < 0 ? 'down' : '';
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
function ago(sec) {
  const d = Date.now() / 1000 - sec;
  if (d < 3600) return Math.max(1, Math.round(d / 60)) + '분 전';
  if (d < 86400) return Math.round(d / 3600) + '시간 전';
  return Math.round(d / 86400) + '일 전';
}
const qLabel = (p) => { if (!p) return ''; const [y, m] = p.split('-'); return `${y.slice(2)}년 ${Math.ceil(+m / 3)}분기`; };
function toast(msg) { const t = $('#toast'); t.textContent = msg; t.classList.add('show'); clearTimeout(toast.t); toast.t = setTimeout(() => t.classList.remove('show'), 2200); }

/* ---------------- Finnhub (rate-limited + cached) ---------------- */
const cache = new Map();
let lastCall = 0;
const queue = [];
let busy = false;
async function pump() {
  if (busy) return; busy = true;
  while (queue.length) {
    const wait = 220 - (Date.now() - lastCall);
    if (wait > 0) await new Promise(r => setTimeout(r, wait));
    lastCall = Date.now();
    const job = queue.shift();
    try { job.res(await job.run()); } catch (e) { job.rej(e); }
  }
  busy = false;
}
function fh(path, params = {}, ttl = 600) {
  const key = getKey();
  if (!key) return Promise.reject(new Error('NOKEY'));
  const qs = new URLSearchParams({ ...params, token: key }).toString();
  const ck = path + '?' + qs;
  const hit = cache.get(ck);
  if (hit && Date.now() - hit.t < ttl * 1000) return hit.p;
  const p = new Promise((res, rej) => {
    queue.push({
      res, rej, run: async () => {
        const r = await fetch(`${FH}${path}?${qs}`);
        if (r.status === 401 || r.status === 403) throw new Error('DENIED');
        if (r.status === 429) throw new Error('LIMIT');
        if (!r.ok) throw new Error('HTTP' + r.status);
        return r.json();
      }
    });
    pump();
  });
  cache.set(ck, { t: Date.now(), p });
  p.catch(() => cache.delete(ck));
  return p;
}

/* ---------------- realtime (WebSocket) ---------------- */
const live = { ws: null, subs: new Set(), handlers: new Set(), last: {} };
function wsConnect() {
  const key = getKey();
  if (!key || live.ws) return;
  try {
    const ws = new WebSocket('wss://ws.finnhub.io?token=' + key);
    live.ws = ws;
    ws.onopen = () => live.subs.forEach(s => ws.send(JSON.stringify({ type: 'subscribe', symbol: s })));
    ws.onmessage = (ev) => {
      let m; try { m = JSON.parse(ev.data); } catch { return; }
      if (m.type !== 'trade' || !m.data) return;
      const latest = {};
      for (const t of m.data) if (!latest[t.s] || t.t >= latest[t.s].t) latest[t.s] = t;
      for (const s in latest) { live.last[s] = latest[s].p; live.handlers.forEach(h => h(s, latest[s].p)); }
    };
    ws.onclose = () => { live.ws = null; setTimeout(() => { if (document.visibilityState === 'visible') wsConnect(); }, 5000); };
    ws.onerror = () => ws.close();
  } catch {}
}
function wsSub(symbols) {
  const want = new Set(symbols);
  for (const s of live.subs) if (!want.has(s) && live.ws?.readyState === 1) live.ws.send(JSON.stringify({ type: 'unsubscribe', symbol: s }));
  for (const s of want) if (!live.subs.has(s) && live.ws?.readyState === 1) live.ws.send(JSON.stringify({ type: 'subscribe', symbol: s }));
  live.subs = want;
  wsConnect();
}

/* ---------------- 13F data ---------------- */
let F13 = null;
async function load13F() {
  if (F13) return F13;
  const r = await fetch('data/13f.json', { cache: 'no-cache' });
  const d = await r.json();
  const byTicker = {}, filers = {};
  for (const f of d.filers) filers[f.cik] = f;
  for (const [cik, rows] of Object.entries(d.h)) {
    const f = filers[cik];
    for (const r of rows) {
      const [t, issuer, cusip, pc, sh, val, psh, pval] = r;
      const o = { f, t, issuer, cusip, pc, sh, val, psh, pval, st: status(sh, psh), w: f.total ? val / f.total : 0, pw: 0 };
      const k = t || ('#' + cusip);
      (byTicker[k] ||= []).push(o);
    }
  }
  F13 = { ...d, byTicker, filerMap: filers };
  return F13;
}
function status(sh, psh) {
  if (!psh && sh) return 'NEW';
  if (psh && !sh) return 'SOLD';
  if (sh > psh) return 'ADD';
  if (sh < psh) return 'REDUCE';
  return 'HOLD';
}
const ST = { NEW: '신규 매수', ADD: '추가 매수', REDUCE: '일부 매도', SOLD: '전량 매도', HOLD: '유지' };
function badge(o) {
  if (o.pc) return `<span class="b OPT">${o.pc === 'C' ? '콜옵션' : '풋옵션'}</span> <span class="b ${o.st}">${ST[o.st]}</span>`;
  let s = ST[o.st];
  if ((o.st === 'ADD' || o.st === 'REDUCE') && o.psh) s += ' ' + pct((o.sh - o.psh) / o.psh * 100, 0);
  return `<span class="b ${o.st}">${s}</span>`;
}

/* ---------------- router ---------------- */
const titleEl = $('#title'), backBtn = $('#backBtn');
backBtn.onclick = () => history.length > 1 ? history.back() : (location.hash = '#home');
let cleanup = [];
function route() {
  cleanup.forEach(fn => fn()); cleanup = [];
  const h = location.hash.slice(1) || 'home';
  const [page, arg] = h.split('-').length > 1 && (h.startsWith('stock-') || h.startsWith('whale-'))
    ? [h.slice(0, h.indexOf('-')), decodeURIComponent(h.slice(h.indexOf('-') + 1))] : [h, null];
  document.querySelectorAll('.tabbar a').forEach(a => a.classList.toggle('on',
    a.dataset.tab === page || (page === 'stock' && a.dataset.tab === 'search') || (page === 'whale' && a.dataset.tab === 'whales')));
  backBtn.hidden = !(page === 'stock' || page === 'whale');
  window.scrollTo(0, 0);
  ({ home: pageHome, search: pageSearch, whales: pageWhales, settings: pageSettings, stock: pageStock, whale: pageWhale }[page] || pageHome)(arg);
}
window.addEventListener('hashchange', route);
document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible') { wsConnect(); marketStatus(); } });

function needKeyCard() {
  return `<div class="card"><h2>🔑 Finnhub 키가 필요해요</h2>
    <p class="muted sm">실시간 주가·뉴스·애널리스트 의견은 Finnhub 무료 키로 불러와요. 거물 13F 데이터는 키 없이도 볼 수 있어요.</p>
    <a class="btn" href="#settings">키 입력하러 가기</a></div>`;
}
const spinner = () => '<div class="center"><span class="spin"></span></div>';

/* ---------------- market status ---------------- */
async function marketStatus() {
  const el = $('#mkt');
  try {
    const s = await fh('/stock/market-status', { exchange: 'US' }, 120);
    const map = { 'pre-market': '프리마켓', regular: '정규장', 'post-market': '애프터마켓' };
    el.className = 'mkt ' + (s.isOpen ? (s.session === 'regular' ? 'open' : 'ext') : '');
    el.innerHTML = `<i></i>${s.isOpen ? (map[s.session] || '장중') : '장 마감'}`;
  } catch { el.innerHTML = ''; }
}

/* ---------------- HOME: watchlist + market news ---------------- */
async function pageHome() {
  titleEl.textContent = '관심종목';
  const w = getWatch();
  if (!getKey()) { view.innerHTML = needKeyCard() + await whaleTeaser(); return; }
  view.innerHTML = `<div class="list" id="wl">${w.length ? w.map(s => `
      <a class="li" href="#stock-${encodeURIComponent(s)}" data-s="${esc(s)}">
        <div class="logo">${esc(s.slice(0, 4))}</div>
        <div class="grow"><div class="tk">${esc(s)}</div><div class="sm muted ell" data-name>&nbsp;</div></div>
        <div class="px num"><b data-p>–</b><span class="chip" data-c>–</span></div>
      </a>`).join('') : '<div class="empty">검색에서 ☆를 눌러 관심종목을 추가하세요</div>'}</div>
    <div id="teaser"></div>
    <div class="card"><h2>📰 시장 주요 뉴스</h2><div id="mnews">${spinner()}</div></div>`;

  const rows = {};
  view.querySelectorAll('#wl .li').forEach(el => rows[el.dataset.s] = el);
  const prev = {};
  const paint = (s, c, pc) => {
    const el = rows[s]; if (!el) return;
    const d = c - pc, dp = pc ? d / pc * 100 : 0;
    const pEl = el.querySelector('[data-p]');
    if (prev[s] != null && c !== prev[s]) { pEl.classList.remove('flash-up', 'flash-down'); void pEl.offsetWidth; pEl.classList.add(c > prev[s] ? 'flash-up' : 'flash-down'); }
    prev[s] = c;
    pEl.textContent = fmtUsd(c);
    const ch = el.querySelector('[data-c]'); ch.className = 'chip ' + cls(dp); ch.textContent = pct(dp);
  };
  const pcs = {};
  const refresh = () => w.forEach(s => fh('/quote', { symbol: s }, 20).then(q => { if (q.c) { pcs[s] = q.pc; paint(s, live.last[s] ?? q.c, q.pc); } }).catch(() => {}));
  refresh();
  const iv = setInterval(refresh, 30000);
  w.forEach(s => fh('/stock/profile2', { symbol: s }, 86400).then(p => {
    const el = rows[s]; if (!el || !p.name) return;
    el.querySelector('[data-name]').textContent = p.name;
    if (p.logo) el.querySelector('.logo').innerHTML = `<img src="${esc(p.logo)}" alt="" onerror="this.remove()">`;
  }).catch(() => {}));
  const h = (s, p) => { if (pcs[s]) paint(s, p, pcs[s]); };
  live.handlers.add(h); wsSub(w);
  cleanup.push(() => { clearInterval(iv); live.handlers.delete(h); });

  whaleTeaser().then(html => { const t = $('#teaser'); if (t) t.innerHTML = html; });

  fh('/news', { category: 'general' }, 300).then(list => {
    const el = $('#mnews'); if (!el) return;
    el.innerHTML = newsList(list.slice(0, 10)) || '<div class="empty">뉴스가 없어요</div>';
  }).catch(e => { const el = $('#mnews'); if (el) el.innerHTML = errMsg(e); });
}

async function whaleTeaser() {
  try {
    const d = await load13F();
    const { buys } = flows(d);
    return `<div class="card"><h2>🐋 이번 분기 거물들이 가장 많이 산 종목 <small>${qLabel(latestPeriod(d))}</small></h2>
      ${buys.slice(0, 5).map((x, i) => rankRow(x, i)).join('')}
      <a class="sm" style="color:var(--accent);display:block;margin-top:8px" href="#whales">거물 전체 보기 →</a></div>`;
  } catch { return ''; }
}

function newsList(list) {
  const seen = new Set();
  return list.filter(n => n.headline && !seen.has(n.headline) && seen.add(n.headline)).map(n => `
    <div class="news">
      ${n.image ? `<img src="${esc(n.image)}" alt="" loading="lazy" onerror="this.remove()">` : ''}
      <div class="grow">
        <a class="h" href="${esc(n.url)}" target="_blank" rel="noopener">${esc(n.headline)}</a>
        <div class="meta"><span>${esc(n.source)}</span><span>${ago(n.datetime)}</span>
          <a href="https://translate.google.com/translate?sl=auto&tl=ko&u=${encodeURIComponent(n.url)}" target="_blank" rel="noopener">번역</a></div>
      </div>
    </div>`).join('');
}
function errMsg(e) {
  const m = { NOKEY: 'Finnhub 키를 설정에서 입력해 주세요.', DENIED: '무료 플랜에서는 제공되지 않는 데이터예요.', LIMIT: '요청이 많아 잠시 후 다시 시도해 주세요.' }[e.message];
  return `<div class="note">${m || '불러오지 못했어요 (' + esc(e.message) + ')'}</div>`;
}

/* ---------------- SEARCH ---------------- */
function pageSearch() {
  titleEl.textContent = '종목 검색';
  view.innerHTML = `
    <label class="search"><svg viewBox="0 0 24 24"><circle cx="11" cy="11" r="6.5"/><path d="M16 16l4.5 4.5"/></svg>
      <input id="q" type="search" placeholder="티커 또는 회사명 (예: NVDA, Apple)" autocomplete="off" autocapitalize="characters"></label>
    <div id="res"></div>`;
  const q = $('#q'), res = $('#res');
  let tm;
  const recent = store.get('recent', []);
  const showRecent = () => {
    res.innerHTML = recent.length ? `<div class="sm muted" style="margin:4px 4px 8px">최근 본 종목</div><div class="list">${recent.map(s =>
      `<a class="li" href="#stock-${encodeURIComponent(s)}"><div class="logo">${esc(s.slice(0, 4))}</div><div class="grow tk">${esc(s)}</div></a>`).join('')}</div>` : '';
  };
  showRecent();
  q.addEventListener('input', () => {
    clearTimeout(tm);
    const v = q.value.trim();
    if (!v) return showRecent();
    tm = setTimeout(() => doSearch(v, res), 300);
  });
  q.addEventListener('keydown', e => { if (e.key === 'Enter') { const v = q.value.trim().toUpperCase(); if (/^[A-Z.]{1,6}$/.test(v)) location.hash = '#stock-' + v; } });
  setTimeout(() => q.focus(), 50);
}
async function doSearch(v, res) {
  const items = new Map();
  try {
    const d = await load13F();
    const V = v.toUpperCase();
    for (const [k, list] of Object.entries(d.byTicker)) {
      if (k.startsWith('#')) continue;
      if (k === V || k.startsWith(V) || list[0].issuer.toUpperCase().includes(V)) items.set(k, { s: k, n: list[0].issuer, w: list.filter(o => o.sh).length });
      if (items.size > 30) break;
    }
  } catch {}
  if (getKey()) {
    try {
      const r = await fh('/search', { q: v, exchange: 'US' }, 3600);
      for (const x of (r.result || []).slice(0, 15)) {
        if (x.symbol.includes(':')) continue;
        const prev = items.get(x.symbol);
        items.set(x.symbol, { s: x.symbol, n: x.description, w: prev?.w || 0 });
      }
    } catch {}
  }
  const V = v.toUpperCase();
  const arr = [...items.values()].sort((a, b) => (b.s === V) - (a.s === V) || (b.s.startsWith(V)) - (a.s.startsWith(V)) || b.w - a.w).slice(0, 25);
  res.innerHTML = arr.length ? `<div class="list">${arr.map(x => `
    <a class="li" href="#stock-${encodeURIComponent(x.s)}"><div class="logo">${esc(x.s.slice(0, 4))}</div>
      <div class="grow"><div class="tk">${esc(x.s)}</div><div class="sm muted ell">${esc(x.n)}</div></div>
      ${x.w ? `<span class="b HOLD">🐋 ${x.w}명 보유</span>` : ''}</a>`).join('')}</div>` : '<div class="empty">결과가 없어요</div>';
}

/* ---------------- STOCK DETAIL ---------------- */
async function pageStock(sym) {
  sym = (sym || '').toUpperCase();
  titleEl.textContent = sym;
  const rec = store.get('recent', []).filter(s => s !== sym); rec.unshift(sym); store.set('recent', rec.slice(0, 12));
  const inW = () => getWatch().includes(sym);
  view.innerHTML = `
    <div class="hero">
      <div class="row"><div class="logo" id="lg">${esc(sym.slice(0, 4))}</div>
        <div class="grow"><div class="name ell" id="nm">&nbsp;</div><div class="sm faint" id="ind"></div></div>
        <button class="star ${inW() ? 'on' : ''}" id="star" aria-label="관심종목"><svg viewBox="0 0 24 24"><path d="M12 3l2.6 5.6 6 .7-4.5 4.1 1.2 6L12 16.4 6.7 19.4l1.2-6L3.4 9.3l6-.7z" fill="currentColor"/></svg></button></div>
      <div class="big num" id="px">${getKey() ? '…' : '–'}</div>
      <div><span class="chg num" id="chg"></span><span class="live" id="lv" hidden><i></i>실시간</span></div>
    </div>
    ${getKey() ? '' : needKeyCard()}
    <div class="card" style="padding:8px"><div class="chart" id="chart"></div></div>
    <div class="card" id="statsCard" hidden><div class="stats" id="stats"></div></div>
    <div class="card"><h2>🐋 거물 보유 현황 <small id="whq"></small></h2><div id="wh">${spinner()}</div></div>
    <div class="card"><h2>🎯 애널리스트 평가</h2><div id="an">${getKey() ? spinner() : '<div class="note">키를 입력하면 볼 수 있어요</div>'}</div></div>
    <div class="card"><h2>📰 최근 뉴스 <small>최근 7일</small></h2><div id="nw">${getKey() ? spinner() : '<div class="note">키를 입력하면 볼 수 있어요</div>'}</div></div>`;

  $('#star').onclick = () => {
    let w = getWatch();
    if (w.includes(sym)) { w = w.filter(s => s !== sym); toast('관심종목에서 뺐어요'); }
    else { w.push(sym); toast('관심종목에 추가했어요'); }
    setWatch(w); $('#star').classList.toggle('on', w.includes(sym));
  };

  renderWhalesFor(sym);
  chart(sym, null);
  if (!getKey()) return;

  let pc = null, lastP = null;
  const paint = (c, isLive) => {
    if (!pc) return;
    const el = $('#px'); if (!el) return;
    if (lastP != null && c !== lastP) { el.classList.remove('flash-up', 'flash-down'); void el.offsetWidth; el.classList.add(c > lastP ? 'flash-up' : 'flash-down'); }
    lastP = c;
    el.textContent = fmtUsd(c);
    const d = c - pc, dp = d / pc * 100;
    const ch = $('#chg'); ch.className = 'chg num ' + cls(d);
    ch.textContent = `${d > 0 ? '▲' : d < 0 ? '▼' : ''} ${fmtUsd(Math.abs(d))} (${pct(dp)})`;
    if (isLive) $('#lv').hidden = false;
  };
  const q = () => fh('/quote', { symbol: sym }, 15).then(r => {
    if (!r.c && !r.pc) { $('#px').textContent = '–'; $('#chg').textContent = '시세를 찾을 수 없어요 (미국 상장 종목만 지원)'; return; }
    pc = r.pc; paint(live.last[sym] ?? r.c);
  }).catch(e => { $('#chg').innerHTML = errMsg(e); });
  q();
  const iv = setInterval(q, 30000);
  const h = (s, p) => { if (s === sym) paint(p, true); };
  live.handlers.add(h); wsSub([sym]);
  cleanup.push(() => { clearInterval(iv); live.handlers.delete(h); });

  fh('/stock/profile2', { symbol: sym }, 86400).then(p => {
    if (!p.name) return;
    $('#nm').textContent = p.name;
    $('#ind').textContent = [p.finnhubIndustry, p.country].filter(Boolean).join(' · ');
    if (p.logo) $('#lg').innerHTML = `<img src="${esc(p.logo)}" alt="" onerror="this.remove()">`;
    chart(sym, p.exchange);
    statsCard(sym, p);
  }).catch(() => statsCard(sym, {}));

  analyst(sym);

  const to = new Date(), from = new Date(Date.now() - 7 * 864e5);
  const d = (x) => x.toISOString().slice(0, 10);
  fh('/company-news', { symbol: sym, from: d(from), to: d(to) }, 600).then(list => {
    const el = $('#nw'); if (!el) return;
    el.innerHTML = newsList(list.slice(0, 20)) || '<div class="empty">최근 7일 뉴스가 없어요</div>';
  }).catch(e => { const el = $('#nw'); if (el) el.innerHTML = errMsg(e); });
}

function chart(sym, exchange) {
  const el = $('#chart'); if (!el) return;
  const ex = /NASDAQ/i.test(exchange || '') ? 'NASDAQ:' : /NEW YORK/i.test(exchange || '') ? 'NYSE:' : /AMEX|NYSE AMERICAN/i.test(exchange || '') ? 'AMEX:' : '';
  if (exchange === null && el.dataset.done) return;
  el.dataset.done = 1;
  const dark = matchMedia('(prefers-color-scheme: dark)').matches;
  el.innerHTML = '';
  const box = document.createElement('div'); box.className = 'tradingview-widget-container'; box.style.height = '100%';
  box.innerHTML = '<div class="tradingview-widget-container__widget" style="height:100%"></div>';
  const s = document.createElement('script');
  s.src = 'https://s3.tradingview.com/external-embedding/embed-widget-mini-symbol-overview.js';
  s.async = true;
  s.textContent = JSON.stringify({ symbol: ex + sym.replace('.', '-'), width: '100%', height: '100%', locale: 'kr', dateRange: '12M',
    colorTheme: dark ? 'dark' : 'light', isTransparent: true, autosize: true, largeChartUrl: '', chartOnly: false, noTimeScale: false });
  box.appendChild(s); el.appendChild(box);
}

async function statsCard(sym, p) {
  let m = {};
  try { m = (await fh('/stock/metric', { symbol: sym, metric: 'all' }, 3600)).metric || {}; } catch {}
  const items = [
    ['시가총액', p.marketCapitalization ? fmtBig(p.marketCapitalization * 1e6) : '–'],
    ['PER (TTM)', m.peTTM ? m.peTTM.toFixed(1) : '–'],
    ['PBR', m.pbQuarterly ? m.pbQuarterly.toFixed(1) : (m.pbAnnual ? m.pbAnnual.toFixed(1) : '–')],
    ['52주 최고', m['52WeekHigh'] ? fmtUsd(m['52WeekHigh']) : '–'],
    ['52주 최저', m['52WeekLow'] ? fmtUsd(m['52WeekLow']) : '–'],
    ['베타', m.beta ? m.beta.toFixed(2) : '–'],
  ];
  const el = $('#stats'); if (!el) return;
  el.innerHTML = items.map(([k, v]) => `<div class="stat"><span>${k}</span><b class="num">${v}</b></div>`).join('');
  $('#statsCard').hidden = false;
}

async function analyst(sym) {
  const el = $('#an');
  let html = '';
  try {
    const r = await fh('/stock/recommendation', { symbol: sym }, 3600);
    if (r.length) {
      const c = r[0];
      const tot = c.strongBuy + c.buy + c.hold + c.sell + c.strongSell;
      const score = tot ? (c.strongBuy * 5 + c.buy * 4 + c.hold * 3 + c.sell * 2 + c.strongSell) / tot : 0;
      const label = score >= 4.3 ? ['적극 매수', 'var(--buy)'] : score >= 3.6 ? ['매수', 'var(--buy)'] : score >= 2.6 ? ['중립', 'var(--hold)'] : ['매도', 'var(--sell)'];
      const segs = [['strongBuy', '적극매수', '#16a34a'], ['buy', '매수', '#4ade80'], ['hold', '보유', '#94a3b8'], ['sell', '매도', '#f87171'], ['strongSell', '적극매도', '#dc2626']];
      html += `<div class="row" style="margin-bottom:10px"><div class="consensus" style="color:${label[1]}">${label[0]}</div>
          <div class="sm muted" style="margin-left:auto">애널리스트 ${tot}명 · ${c.period.slice(0, 7)}</div></div>
        <div class="bar">${segs.map(([k, , col]) => c[k] ? `<span style="width:${c[k] / tot * 100}%;background:${col}"></span>` : '').join('')}</div>
        <div class="legend">${segs.map(([k, n, col]) => `<span><i style="background:${col}"></i>${n} ${c[k]}</span>`).join('')}</div>
        <div class="trend">${r.slice(0, 6).reverse().map(x => {
          const t = x.strongBuy + x.buy + x.hold + x.sell + x.strongSell || 1;
          return `<div><div class="col">${segs.map(([k, , col]) => `<span style="height:${x[k] / t * 100}%;background:${col}"></span>`).join('')}</div>${x.period.slice(5, 7)}월</div>`;
        }).join('')}</div>`;
    } else html += '<div class="note">애널리스트 의견 데이터가 없어요</div>';
  } catch (e) { html += errMsg(e); }

  try {
    const t = await fh('/stock/price-target', { symbol: sym }, 3600);
    if (t.targetMean) {
      const cur = Number(($('#px')?.textContent || '').replace(/[$,]/g, '')) || null;
      const lo = Math.min(t.targetLow, cur ?? t.targetLow), hi = Math.max(t.targetHigh, cur ?? t.targetHigh);
      const pos = (v) => ((v - lo) / (hi - lo || 1) * 100).toFixed(1) + '%';
      const up = cur ? (t.targetMean / cur - 1) * 100 : null;
      html += `<div style="margin-top:16px" class="row"><b>목표주가</b>
          <span class="sm muted" style="margin-left:auto">평균 ${fmtUsd(t.targetMean)}${up != null ? ` <span class="${cls(up)}">(${pct(up, 1)})</span>` : ''}</span></div>
        <div class="tgt">
          <div class="m" style="left:${pos(t.targetLow)}"><em>최저 ${fmtUsd(t.targetLow, 0)}</em></div>
          <div class="m" style="left:${pos(t.targetMean)}"><em>평균</em></div>
          <div class="m" style="left:${pos(t.targetHigh)}"><em>최고 ${fmtUsd(t.targetHigh, 0)}</em></div>
          ${cur ? `<div class="m cur" style="left:${pos(cur)}"><em>현재</em></div>` : ''}
        </div>`;
    }
  } catch (e) { if (e.message === 'DENIED') html += '<div class="note" style="margin-top:10px">목표주가는 Finnhub 유료 플랜에서만 제공돼요.</div>'; }

  try {
    const ud = await fh('/stock/upgrade-downgrade', { symbol: sym }, 3600);
    if (ud.length) {
      const act = { up: ['상향', 'up'], down: ['하향', 'down'], init: ['신규', ''], main: ['유지', ''], reit: ['재개', ''] };
      html += `<div style="margin-top:14px"><b>최근 투자의견 변경</b>${ud.slice(0, 6).map(x => `
        <div class="rank"><span class="grow ell">${esc(x.company)}</span>
          <span class="sm ${act[x.action]?.[1] || ''}">${act[x.action]?.[0] || esc(x.action)}</span>
          <span class="sm muted">${esc(x.fromGrade || '')}${x.fromGrade ? ' → ' : ''}${esc(x.toGrade)}</span>
          <span class="sm faint">${new Date(x.gradeTime * 1000).toISOString().slice(5, 10)}</span></div>`).join('')}</div>`;
    }
  } catch {}
  if (el) el.innerHTML = html;
}

async function renderWhalesFor(sym) {
  const el = $('#wh');
  try {
    const d = await load13F();
    const list = (d.byTicker[sym] || []).slice();
    $('#whq').textContent = qLabel(latestPeriod(d)) + ' 기준';
    if (!list.length) { el.innerHTML = '<div class="note">추적 중인 거물 중 이 종목을 보유했던 사람이 없어요.</div>'; return; }
    list.sort((a, b) => (!!a.pc - !!b.pc) || (b.val - a.val) || (b.pval - a.pval));
    const holders = list.filter(o => o.sh && !o.pc).length;
    const buys = list.filter(o => !o.pc && (o.st === 'NEW' || o.st === 'ADD')).length;
    const sells = list.filter(o => !o.pc && (o.st === 'SOLD' || o.st === 'REDUCE')).length;
    el.innerHTML = `<div class="row sm" style="gap:14px;margin-bottom:6px"><span>보유 <b>${holders}</b>명</span>
        <span class="up">매수 <b>${buys}</b></span><span class="down">매도 <b>${sells}</b></span></div>` +
      list.map(o => `
      <a class="wh" href="#whale-${o.f.cik}">
        <div><span class="n">${esc(o.f.name)}</span> <span class="sm faint">${esc(o.f.fund)}</span></div>
        <div class="r">${badge(o)}</div>
        <div class="sm muted num">${o.sh ? fmtSh(o.sh) + '주 · ' + fmtBig(o.val) : '이전 ' + fmtSh(o.psh) + '주'}${o.f.period !== latestPeriod(d) ? ` · <span class="faint">${qLabel(o.f.period)} 공시</span>` : ''}</div>
        <div class="r sm muted num">${o.sh && !o.pc ? '비중 ' + (o.w * 100).toFixed(2) + '%' : ''}</div>
      </a>`).join('');
  } catch { el.innerHTML = '<div class="note">13F 데이터를 불러오지 못했어요</div>'; }
}

/* ---------------- WHALES ---------------- */
function latestPeriod(d) {
  const c = {}; d.filers.forEach(f => c[f.period] = (c[f.period] || 0) + 1);
  return Object.entries(c).sort((a, b) => b[1] - a[1])[0]?.[0];
}
function flows(d) {
  const lp = latestPeriod(d);
  const agg = {};
  for (const [k, list] of Object.entries(d.byTicker)) {
    if (k.startsWith('#')) continue;
    for (const o of list) {
      if (o.pc || o.f.period !== lp) continue;
      const a = agg[k] ||= { k, n: o.issuer, buy: 0, sell: 0, newc: 0, soldc: 0 };
      if (o.st === 'NEW' || o.st === 'ADD') { a.buy++; if (o.st === 'NEW') a.newc++; }
      if (o.st === 'SOLD' || o.st === 'REDUCE') { a.sell++; if (o.st === 'SOLD') a.soldc++; }
    }
  }
  const arr = Object.values(agg);
  return {
    buys: arr.filter(a => a.buy).sort((a, b) => b.buy - a.buy || b.newc - a.newc).slice(0, 15),
    sells: arr.filter(a => a.sell).sort((a, b) => b.sell - a.sell || b.soldc - a.soldc).slice(0, 15),
  };
}
const rankRow = (x, i, sell) => `<a class="rank" href="#stock-${encodeURIComponent(x.k)}"><em>${i + 1}</em><b>${esc(x.k)}</b>
  <span class="sm muted ell grow">${esc(x.n)}</span><span class="cnt ${sell ? 'down' : 'up'}">${sell ? x.sell : x.buy}명${(sell ? x.soldc : x.newc) ? ` <span class="sm">(${sell ? '전량' : '신규'} ${sell ? x.soldc : x.newc})</span>` : ''}</span></a>`;

async function pageWhales() {
  titleEl.textContent = '거물 13F';
  view.innerHTML = spinner();
  let d;
  try { d = await load13F(); } catch { view.innerHTML = '<div class="empty">13F 데이터를 불러오지 못했어요</div>'; return; }
  const { buys, sells } = flows(d);
  const lp = latestPeriod(d);
  const tab = store.get('flowTab', 'buy');
  view.innerHTML = `
    <div class="card"><h2>📊 거물들의 매매 동향 <small>${qLabel(lp)} (직전 분기 대비)</small></h2>
      <div class="seg"><button data-t="buy" class="${tab === 'buy' ? 'on' : ''}">많이 산 종목</button><button data-t="sell" class="${tab === 'sell' ? 'on' : ''}">많이 판 종목</button></div>
      <div id="flow">${(tab === 'buy' ? buys : sells).map((x, i) => rankRow(x, i, tab === 'sell')).join('')}</div></div>
    <div class="list">${d.filers.slice().sort((a, b) => b.total - a.total).map(f => `
      <a class="li" href="#whale-${f.cik}">
        <div class="logo">🐋</div>
        <div class="grow"><div class="tk">${esc(f.name)}</div><div class="sm muted ell">${esc(f.fund)}</div></div>
        <div class="px"><b class="num">${fmtBig(f.total)}</b><span class="sm ${f.period === lp ? 'faint' : ''}" style="${f.period === lp ? '' : 'color:var(--warn)'}">${qLabel(f.period)} · ${f.n}종목</span></div>
      </a>`).join('')}</div>
    <p class="note">13F는 분기 말 기준 미국 상장주식 롱 포지션만 담고, 분기 종료 후 최대 45일 뒤에 공시돼요. 공매도·채권·해외주식은 포함되지 않아요. 데이터 갱신: ${esc(d.generated.replace('T', ' ').replace('Z', ' UTC'))}</p>`;
  view.querySelectorAll('.seg button').forEach(b => b.onclick = () => {
    store.set('flowTab', b.dataset.t);
    view.querySelectorAll('.seg button').forEach(x => x.classList.toggle('on', x === b));
    $('#flow').innerHTML = (b.dataset.t === 'buy' ? buys : sells).map((x, i) => rankRow(x, i, b.dataset.t === 'sell')).join('');
  });
}

async function pageWhale(cik) {
  view.innerHTML = spinner();
  const d = await load13F();
  const f = d.filerMap[cik];
  if (!f) { view.innerHTML = '<div class="empty">찾을 수 없어요</div>'; return; }
  titleEl.textContent = f.name;
  const rows = d.h[cik].map(r => {
    const [t, issuer, cusip, pc, sh, val, psh, pval] = r;
    return { t, issuer, cusip, pc, sh, val, psh, pval, st: status(sh, psh), w: f.total ? val / f.total : 0 };
  });
  let filter = 'ALL';
  const prevTotal = rows.filter(o => !o.pc).reduce((s, o) => s + o.pval, 0);
  const render = () => {
    const list = rows.filter(o => filter === 'ALL' ? o.sh : filter === 'BUY' ? (o.st === 'NEW' || o.st === 'ADD') : filter === 'SELL' ? (o.st === 'SOLD' || o.st === 'REDUCE') : o.st === filter);
    $('#hl').innerHTML = list.length ? list.slice(0, 200).map(o => `
      <a class="li" ${o.t ? `href="#stock-${encodeURIComponent(o.t)}"` : ''}>
        <div class="grow"><div class="row" style="gap:6px"><span class="tk">${esc(o.t || '—')}</span>${badge(o)}</div>
          <div class="sm muted ell">${esc(o.issuer)}</div></div>
        <div class="px num"><b>${o.sh ? fmtBig(o.val) : '–'}</b><span class="sm muted">${o.sh && !o.pc ? (o.w * 100).toFixed(2) + '%' : (o.sh ? '' : '이전 ' + fmtBig(o.pval))}</span></div>
      </a>`).join('') : '<div class="empty">해당 종목이 없어요</div>';
  };
  view.innerHTML = `
    <div class="card"><div class="muted sm">${esc(f.fund)} · ${esc(f.edgar)}</div>
      <div class="row" style="margin-top:6px"><div><div class="sm muted">${qLabel(f.period)} 포트폴리오</div><div class="consensus num">${fmtBig(f.total)}</div></div>
        <div style="margin-left:auto;text-align:right"><div class="sm muted">직전 분기 대비</div><div class="num ${cls(f.total - prevTotal)}"><b>${prevTotal ? pct((f.total / prevTotal - 1) * 100, 1) : '–'}</b></div></div></div>
      <div class="sm faint" style="margin-top:6px">${f.n}종목 · 공시일 ${esc(f.filed)}${f.prev ? ` · 비교 기준 ${qLabel(f.prev)}` : ''}</div>
      <a class="sm" style="color:var(--accent);display:block;margin-top:6px" target="_blank" rel="noopener" href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${f.cik}&type=13F-HR">SEC 원문 보기 →</a></div>
    <div class="seg">${[['ALL', '보유 전체'], ['BUY', '매수'], ['SELL', '매도'], ['NEW', '신규'], ['SOLD', '전량 매도']].map(([k, n]) => `<button data-f="${k}" class="${k === filter ? 'on' : ''}">${n}</button>`).join('')}</div>
    <div class="list" id="hl"></div>`;
  view.querySelectorAll('.seg button').forEach(b => b.onclick = () => { filter = b.dataset.f; view.querySelectorAll('.seg button').forEach(x => x.classList.toggle('on', x === b)); render(); });
  render();
}

/* ---------------- SETTINGS ---------------- */
function pageSettings() {
  titleEl.textContent = '설정';
  const w = getWatch();
  view.innerHTML = `
    <div class="card"><h2>🔑 Finnhub API 키</h2>
      <p class="sm muted">무료 가입 후 대시보드에 보이는 키를 붙여넣으세요. 키는 이 기기에만 저장돼요.</p>
      <input class="field" id="key" placeholder="API key" value="${esc(getKey())}" autocomplete="off" autocapitalize="off" spellcheck="false">
      <div class="row" style="margin-top:10px"><button class="btn" id="save">저장하고 테스트</button>
        <a class="btn ghost" href="https://finnhub.io/register" target="_blank" rel="noopener">키 발급받기</a></div>
      <div id="kr" class="sm" style="margin-top:8px"></div></div>
    <div class="card"><h2>⭐ 관심종목 <small>${w.length}개</small></h2>
      ${w.map((s, i) => `<div class="rank"><b>${esc(s)}</b><span class="grow"></span>
        <button class="del" data-up="${i}" ${i ? '' : 'disabled style="opacity:.3"'}>▲</button>
        <button class="del" data-rm="${i}">삭제</button></div>`).join('') || '<div class="note">없음</div>'}</div>
    <div class="card"><h2>📱 아이폰 홈 화면에 추가</h2>
      <p class="sm muted">Safari에서 이 페이지를 열고 하단 공유 버튼 → <b>홈 화면에 추가</b>. 홈 화면 앱은 Safari와 저장 공간이 따로라서, 앱으로 연 뒤에 키를 한 번 더 입력해 주세요.</p></div>`;
  $('#save').onclick = async () => {
    const k = $('#key').value.trim();
    store.set('fhKey', k); cache.clear();
    if (live.ws) live.ws.close();
    const kr = $('#kr');
    kr.innerHTML = '<span class="spin"></span>';
    try { const q = await fh('/quote', { symbol: 'AAPL' }, 0); kr.innerHTML = q.c ? `<span style="color:var(--buy)">✓ 연결됐어요 (AAPL ${fmtUsd(q.c)})</span>` : '응답은 왔지만 시세가 비어 있어요'; marketStatus(); }
    catch (e) { kr.innerHTML = errMsg(e); }
  };
  view.querySelectorAll('[data-rm]').forEach(b => b.onclick = () => { const w = getWatch(); w.splice(+b.dataset.rm, 1); setWatch(w); pageSettings(); });
  view.querySelectorAll('[data-up]').forEach(b => b.onclick = () => { const w = getWatch(), i = +b.dataset.up; if (i) { [w[i - 1], w[i]] = [w[i], w[i - 1]]; setWatch(w); pageSettings(); } });
}

/* ---------------- boot ---------------- */
if ('serviceWorker' in navigator) navigator.serviceWorker.register('sw.js').catch(() => {});
marketStatus();
route();
