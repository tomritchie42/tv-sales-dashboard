const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 3000;
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || 'change-me-to-a-secret';

// ── Data store ──────────────────────────────────────────────
const DATA_DIR  = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'consignments.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(DATA_FILE)) fs.writeFileSync(DATA_FILE, '[]');

function readData()  { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
function writeData(d){ fs.writeFileSync(DATA_FILE, JSON.stringify(d, null, 2)); }

// ── Helpers ─────────────────────────────────────────────────
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml' };

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

function json(res, data, status = 200) {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify(data));
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-auth-token');
}

// ── XML parser ──────────────────────────────────────────────
function parseXMLConsignments(xml) {
  const consignments = [];
  const blocks = xml.split('<ConsignmentNumber>').slice(1);
  blocks.forEach(block => {
    const get = tag => { const m = block.match(new RegExp('<' + tag + '>(.*?)</' + tag + '>', 's')); return m ? m[1].trim() : ''; };
    const conNum = block.split('</ConsignmentNumber>')[0]?.trim();
    consignments.push({
      ConsignmentNumber: conNum, ConsignmentDate: get('ConsignmentDate'),
      SenderName: get('SenderName'), SenderSuburb: get('SenderSuburb'), SenderState: get('SenderState'), SenderPostcode: get('SenderPostcode'),
      ReceiverName: get('ReceiverName'), ReceiverSuburb: get('ReceiverSuburb'), ReceiverState: get('ReceiverState'), ReceiverPostcode: get('ReceiverPostcode'),
      SenderReference: get('SenderReference'), ReceiverReference: get('ReceiverReference'), SpecialInstructions: get('SpecialInstructions'),
      ServiceType: get('ServiceType'), Status: get('Status'), Revenue: parseFloat(get('Revenue')) || 0,
      Weight: parseFloat(get('Weight')) || 0, Items: parseInt(get('Items')) || 1,
      DeliveryDate: get('DeliveryDate'), CustomerName: get('SenderName'),
    });
  });
  return consignments;
}

// ── Dashboard aggregation ───────────────────────────────────
function aggregateDashboard(period) {
  const raw = readData();
  const now = new Date();
  const cutoff = new Date(now); cutoff.setDate(cutoff.getDate() - period);
  const prevCutoff = new Date(cutoff); prevCutoff.setDate(prevCutoff.getDate() - period);

  const inPeriod = raw.filter(c => new Date(c.ConsignmentDate) >= cutoff);
  const inPrev = raw.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; });

  const totalRev = inPeriod.reduce((s, c) => s + (c.Revenue || 0), 0);
  const prevRev = inPrev.reduce((s, c) => s + (c.Revenue || 0), 0);
  const customerSet = new Set(inPeriod.map(c => c.SenderName || c.CustomerName).filter(Boolean));
  const prevCustomerSet = new Set(inPrev.map(c => c.SenderName || c.CustomerName).filter(Boolean));

  // Customer aggregation
  const customerMap = {};
  raw.forEach(c => {
    const name = c.SenderName || c.CustomerName || 'Unknown';
    if (!customerMap[name]) customerMap[name] = { name, consignments: [], totalRevenue: 0, dates: [] };
    customerMap[name].consignments.push(c);
    customerMap[name].totalRevenue += (c.Revenue || 0);
    if (c.ConsignmentDate) customerMap[name].dates.push(new Date(c.ConsignmentDate));
  });

  // At-risk
  const sevenAgo = new Date(now); sevenAgo.setDate(sevenAgo.getDate() - 7);
  const fourteenAgo = new Date(now); fourteenAgo.setDate(fourteenAgo.getDate() - 14);
  const atRiskInactive = [], atRiskVolumeDrop = [];

  Object.values(customerMap).forEach(cust => {
    const recent = cust.dates.filter(d => d >= sevenAgo);
    const prev = cust.dates.filter(d => d >= fourteenAgo && d < sevenAgo);
    if (recent.length === 0 && prev.length > 0) {
      atRiskInactive.push({ name: cust.name, lastOrderDate: new Date(Math.max(...cust.dates)).toISOString().split('T')[0], previousVolume: prev.length, totalRevenue: cust.totalRevenue });
    }
    const curP = cust.consignments.filter(c => new Date(c.ConsignmentDate) >= cutoff).length;
    const prevP = cust.consignments.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; }).length;
    if (prevP > 2 && curP > 0) {
      const drop = ((curP - prevP) / prevP) * 100;
      if (drop < -20) {
        const curR = cust.consignments.filter(c => new Date(c.ConsignmentDate) >= cutoff).reduce((s, c) => s + (c.Revenue || 0), 0);
        const prevR = cust.consignments.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; }).reduce((s, c) => s + (c.Revenue || 0), 0);
        atRiskVolumeDrop.push({ name: cust.name, dropPercent: drop.toFixed(1), currentVolume: curP, previousVolume: prevP, currentRevenue: curR, previousRevenue: prevR });
      }
    }
  });

  // Monthly trends
  const monthlyTrends = [];
  for (let i = 5; i >= 0; i--) {
    const ms = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const me = new Date(now.getFullYear(), now.getMonth() - i + 1, 0, 23, 59, 59);
    const mc = raw.filter(c => { const d = new Date(c.ConsignmentDate); return d >= ms && d <= me; });
    monthlyTrends.push({ label: ms.toLocaleString('en-AU', { month: 'short', year: '2-digit' }), revenue: mc.reduce((s, c) => s + (c.Revenue || 0), 0), consignments: mc.length });
  }

  // Service types
  const serviceTypes = {};
  inPeriod.forEach(c => { const st = c.ServiceType || 'Standard'; serviceTypes[st] = (serviceTypes[st] || 0) + (c.Revenue || 0); });

  // Customer list
  const customerList = Object.values(customerMap).map(cust => {
    const pc = cust.consignments.filter(c => new Date(c.ConsignmentDate) >= cutoff);
    const pp = cust.consignments.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; });
    const pr = pc.reduce((s, c) => s + (c.Revenue || 0), 0);
    const ppr = pp.reduce((s, c) => s + (c.Revenue || 0), 0);
    const lo = cust.dates.length > 0 ? new Date(Math.max(...cust.dates)).toISOString().split('T')[0] : 'Never';
    return { name: cust.name, periodRevenue: pr, prevPeriodRevenue: ppr, periodConsignments: pc.length, avgPerCon: pc.length > 0 ? (pr / pc.length).toFixed(2) : 0, lastOrder: lo, trend: ppr > 0 ? (((pr - ppr) / ppr) * 100).toFixed(1) : 'NEW' };
  }).sort((a, b) => b.periodRevenue - a.periodRevenue);

  // Lanes
  const laneMap = {};
  inPeriod.forEach(c => {
    const key = (c.SenderSuburb || 'Unknown') + '→' + (c.ReceiverSuburb || 'Unknown');
    if (!laneMap[key]) laneMap[key] = { origin: c.SenderSuburb || 'Unknown', dest: c.ReceiverSuburb || 'Unknown', cons: 0, rev: 0, customers: {} };
    laneMap[key].cons++; laneMap[key].rev += (c.Revenue || 0);
    const cn = c.SenderName || c.CustomerName || 'Unknown';
    laneMap[key].customers[cn] = (laneMap[key].customers[cn] || 0) + 1;
  });
  const lanes = Object.values(laneMap).sort((a, b) => b.cons - a.cons).slice(0, 10).map(l => ({ ...l, topCustomer: Object.entries(l.customers).sort((a, b) => b[1] - a[1])[0]?.[0] || 'Unknown' }));

  // Peak days
  const dayVol = [0, 0, 0, 0, 0, 0, 0];
  inPeriod.forEach(c => { dayVol[new Date(c.ConsignmentDate).getDay()]++; });
  const peakDays = [...dayVol.slice(1), dayVol[0]];

  // Zones
  const zoneMap = {};
  inPeriod.forEach(c => { const z = c.ReceiverState || 'Unknown'; if (!zoneMap[z]) zoneMap[z] = { zone: z, rev: 0, cons: 0 }; zoneMap[z].rev += (c.Revenue || 0); zoneMap[z].cons++; });

  return {
    generatedAt: now.toISOString(), period, totalStored: raw.length,
    kpis: { totalRevenue: totalRev, prevRevenue: prevRev, totalConsignments: inPeriod.length, prevConsignments: inPrev.length, activeCustomers: customerSet.size, avgRevenuePerCon: inPeriod.length > 0 ? (totalRev / inPeriod.length).toFixed(2) : 0, atRiskCount: atRiskInactive.length + atRiskVolumeDrop.length, newCustomers: [...customerSet].filter(c => !prevCustomerSet.has(c)).length },
    atRisk: { inactive: atRiskInactive, volumeDrop: atRiskVolumeDrop },
    monthlyTrends, serviceTypes,
    customers: { top10: customerList.slice(0, 10), bottom10: customerList.filter(c => c.periodRevenue > 0).slice(-10).reverse(), all: customerList },
    concentration: { top5Revenue: customerList.slice(0, 5).reduce((s, c) => s + c.periodRevenue, 0), totalRevenue: totalRev, top5: customerList.slice(0, 5).map(c => ({ name: c.name, revenue: c.periodRevenue })) },
    lanes, peakDays, zones: Object.values(zoneMap).sort((a, b) => b.rev - a.rev)
  };
}

// ── Server ──────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  cors(res);
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;

  // CORS preflight
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

  // ── API routes ──
  if (pathname === '/api/webhook' && req.method === 'POST') {
    const token = parsed.query.token || req.headers['x-auth-token'] || '';
    if (token !== WEBHOOK_TOKEN) return json(res, { error: 'Unauthorised' }, 401);

    try {
      const body = await readBody(req);
      let consignments = [];
      const ct = req.headers['content-type'] || '';

      if (ct.includes('xml') || body.trim().startsWith('<')) {
        consignments = parseXMLConsignments(body);
      } else {
        const parsed = JSON.parse(body);
        if (parsed.consignments) consignments = parsed.consignments;
        else if (Array.isArray(parsed)) consignments = parsed;
        else if (parsed.ConsignmentNumber) consignments = [parsed];
      }

      const existing = readData();
      const map = new Map(existing.map(c => [c.ConsignmentNumber, c]));
      consignments.forEach(c => { c._receivedAt = new Date().toISOString(); map.set(c.ConsignmentNumber, c); });
      writeData(Array.from(map.values()));

      console.log('[WEBHOOK] Received ' + consignments.length + ' consignment(s) — total: ' + map.size);
      return json(res, { success: true, received: consignments.length, total: map.size });
    } catch (err) {
      console.error('[WEBHOOK] Error:', err.message);
      return json(res, { error: err.message }, 400);
    }
  }

  if (pathname === '/api/dashboard' && req.method === 'GET') {
    const period = parseInt(parsed.query.period) || 30;
    return json(res, aggregateDashboard(period));
  }

  if (pathname === '/api/stats' && req.method === 'GET') {
    const raw = readData();
    return json(res, {
      totalConsignments: raw.length,
      uniqueCustomers: new Set(raw.map(c => c.SenderName || c.CustomerName).filter(Boolean)).size,
      oldestRecord: raw.length > 0 ? raw.reduce((min, c) => c.ConsignmentDate < min ? c.ConsignmentDate : min, raw[0].ConsignmentDate) : null,
      newestRecord: raw.length > 0 ? raw.reduce((max, c) => c.ConsignmentDate > max ? c.ConsignmentDate : max, raw[0].ConsignmentDate) : null
    });
  }

  if (pathname === '/api/import-csv' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const lines = body.split('\n').filter(l => l.trim());
      if (lines.length < 2) return json(res, { error: 'No data rows' }, 400);

      const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
      const existing = readData();
      const map = new Map(existing.map(c => [c.ConsignmentNumber, c]));
      let imported = 0;

const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 3000;
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || 'change-me-to-a-secret';

// ── Data store ──────────────────────────────────────────────
const DATA_DIR  = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'consignments.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(DATA_FILE)) fs.writeFileSync(DATA_FILE, '[]');

function readData()  { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
function writeData(d){ fs.writeFileSync(DATA_FILE, JSON.stringify(d, null, 2)); }

// ── Helpers ─────────────────────────────────────────────────
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml' };

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

function json(res, data, status = 200) {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify(data));
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-auth-token');
}

// ── XML parser ──────────────────────────────────────────────
function parseXMLConsignments(xml) {
  const consignments = [];
  const blocks = xml.split('<ConsignmentNumber>').slice(1);
  blocks.forEach(block => {
    const get = tag => { const m = block.match(new RegExp('<' + tag + '>(.*?)</' + tag + '>', 's')); return m ? m[1].trim() : ''; };
    const conNum = block.split('</ConsignmentNumber>')[0]?.trim();
    consignments.push({
      ConsignmentNumber: conNum, ConsignmentDate: get('ConsignmentDate'),
      SenderName: get('SenderName'), SenderSuburb: get('SenderSuburb'), SenderState: get('SenderState'), SenderPostcode: get('SenderPostcode'),
      ReceiverName: get('ReceiverName'), ReceiverSuburb: get('ReceiverSuburb'), ReceiverState: get('ReceiverState'), ReceiverPostcode: get('ReceiverPostcode'),
      SenderReference: get('SenderReference'), ReceiverReference: get('ReceiverReference'), SpecialInstructions: get('SpecialInstructions'),
      ServiceType: get('ServiceType'), Status: get('Status'), Revenue: parseFloat(get('Revenue')) || 0,
      Weight: parseFloat(get('Weight')) || 0, Items: parseInt(get('Items')) || 1,
      DeliveryDate: get('DeliveryDate'), CustomerName: get('SenderName'),
    });
  });
  return consignments;
}

// ── Dashboard aggregation ───────────────────────────────────
function aggregateDashboard(period) {
  const raw = readData();
  const now = new Date();
  const cutoff = new Date(now); cutoff.setDate(cutoff.getDate() - period);
  const prevCutoff = new Date(cutoff); prevCutoff.setDate(prevCutoff.getDate() - period);

  const inPeriod = raw.filter(c => new Date(c.ConsignmentDate) >= cutoff);
  const inPrev = raw.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; });

  const totalRev = inPeriod.reduce((s, c) => s + (c.Revenue || 0), 0);
  const prevRev = inPrev.reduce((s, c) => s + (c.Revenue || 0), 0);
  const customerSet = new Set(inPeriod.map(c => c.SenderName || c.CustomerName).filter(Boolean));
  const prevCustomerSet = new Set(inPrev.map(c => c.SenderName || c.CustomerName).filter(Boolean));

  // Customer aggregation
  const customerMap = {};
  raw.forEach(c => {
    const name = c.SenderName || c.CustomerName || 'Unknown';
    if (!customerMap[name]) customerMap[name] = { name, consignments: [], totalRevenue: 0, dates: [] };
    customerMap[name].consignments.push(c);
    customerMap[name].totalRevenue += (c.Revenue || 0);
    if (c.ConsignmentDate) customerMap[name].dates.push(new Date(c.ConsignmentDate));
  });

  // At-risk
  const sevenAgo = new Date(now); sevenAgo.setDate(sevenAgo.getDate() - 7);
  const fourteenAgo = new Date(now); fourteenAgo.setDate(fourteenAgo.getDate() - 14);
  const atRiskInactive = [], atRiskVolumeDrop = [];

  Object.values(customerMap).forEach(cust => {
    const recent = cust.dates.filter(d => d >= sevenAgo);
    const prev = cust.dates.filter(d => d >= fourteenAgo && d < sevenAgo);
    if (recent.length === 0 && prev.length > 0) {
      atRiskInactive.push({ name: cust.name, lastOrderDate: new Date(Math.max(...cust.dates)).toISOString().split('T')[0], previousVolume: prev.length, totalRevenue: cust.totalRevenue });
    }
    const curP = cust.consignments.filter(c => new Date(c.ConsignmentDate) >= cutoff).length;
    const prevP = cust.consignments.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; }).length;
    if (prevP > 2 && curP > 0) {
      const drop = ((curP - prevP) / prevP) * 100;
      if (drop < -20) {
        const curR = cust.consignments.filter(c => new Date(c.ConsignmentDate) >= cutoff).reduce((s, c) => s + (c.Revenue || 0), 0);
        const prevR = cust.consignments.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; }).reduce((s, c) => s + (c.Revenue || 0), 0);
        atRiskVolumeDrop.push({ name: cust.name, dropPercent: drop.toFixed(1), currentVolume: curP, previousVolume: prevP, currentRevenue: curR, previousRevenue: prevR });
      }
    }
  });

  // Monthly trends
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 3000;
const WEBHOOK_TOKEN = process.env.WEBHOOK_TOKEN || 'change-me-to-a-secret';

// ── Data store ──────────────────────────────────────────────
const DATA_DIR  = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'consignments.json');

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(DATA_FILE)) fs.writeFileSync(DATA_FILE, '[]');

function readData()  { return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); }
function writeData(d){ fs.writeFileSync(DATA_FILE, JSON.stringify(d, null, 2)); }

// ── Helpers ─────────────────────────────────────────────────
const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json', '.png': 'image/png', '.svg': 'image/svg+xml' };

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}

function json(res, data, status = 200) {
  res.writeHead(status, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify(data));
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-auth-token');
}

// ── XML parser ──────────────────────────────────────────────
function parseXMLConsignments(xml) {
  const consignments = [];
  const blocks = xml.split('<ConsignmentNumber>').slice(1);
  blocks.forEach(block => {
    const get = tag => { const m = block.match(new RegExp('<' + tag + '>(.*?)</' + tag + '>', 's')); return m ? m[1].trim() : ''; };
    const conNum = block.split('</ConsignmentNumber>')[0]?.trim();
    consignments.push({
      ConsignmentNumber: conNum, ConsignmentDate: get('ConsignmentDate'),
      SenderName: get('SenderName'), SenderSuburb: get('SenderSuburb'), SenderState: get('SenderState'), SenderPostcode: get('SenderPostcode'),
      ReceiverName: get('ReceiverName'), ReceiverSuburb: get('ReceiverSuburb'), ReceiverState: get('ReceiverState'), ReceiverPostcode: get('ReceiverPostcode'),
      SenderReference: get('SenderReference'), ReceiverReference: get('ReceiverReference'), SpecialInstructions: get('SpecialInstructions'),
      ServiceType: get('ServiceType'), Status: get('Status'), Revenue: parseFloat(get('Revenue')) || 0,
      Weight: parseFloat(get('Weight')) || 0, Items: parseInt(get('Items')) || 1,
      DeliveryDate: get('DeliveryDate'), CustomerName: get('SenderName'),
    });
  });
  return consignments;
}

// ── Dashboard aggregation ───────────────────────────────────
function aggregateDashboard(period) {
  const raw = readData();
  const now = new Date();
  const cutoff = new Date(now); cutoff.setDate(cutoff.getDate() - period);
  const prevCutoff = new Date(cutoff); prevCutoff.setDate(prevCutoff.getDate() - period);

  const inPeriod = raw.filter(c => new Date(c.ConsignmentDate) >= cutoff);
  const inPrev = raw.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; });

  const totalRev = inPeriod.reduce((s, c) => s + (c.Revenue || 0), 0);
  const prevRev = inPrev.reduce((s, c) => s + (c.Revenue || 0), 0);
  const customerSet = new Set(inPeriod.map(c => c.SenderName || c.CustomerName).filter(Boolean));
  const prevCustomerSet = new Set(inPrev.map(c => c.SenderName || c.CustomerName).filter(Boolean));

  // Customer aggregation
  const customerMap = {};
  raw.forEach(c => {
    const name = c.SenderName || c.CustomerName || 'Unknown';
    if (!customerMap[name]) customerMap[name] = { name, consignments: [], totalRevenue: 0, dates: [] };
    customerMap[name].consignments.push(c);
    customerMap[name].totalRevenue += (c.Revenue || 0);
    if (c.ConsignmentDate) customerMap[name].dates.push(new Date(c.ConsignmentDate));
  });

  // At-risk
  const sevenAgo = new Date(now); sevenAgo.setDate(sevenAgo.getDate() - 7);
  const fourteenAgo = new Date(now); fourteenAgo.setDate(fourteenAgo.getDate() - 14);
  const atRiskInactive = [], atRiskVolumeDrop = [];

  Object.values(customerMap).forEach(cust => {
    const recent = cust.dates.filter(d => d >= sevenAgo);
    const prev = cust.dates.filter(d => d >= fourteenAgo && d < sevenAgo);
    if (recent.length === 0 && prev.length > 0) {
      atRiskInactive.push({ name: cust.name, lastOrderDate: new Date(Math.max(...cust.dates)).toISOString().split('T')[0], previousVolume: prev.length, totalRevenue: cust.totalRevenue });
    }
    const curP = cust.consignments.filter(c => new Date(c.ConsignmentDate) >= cutoff).length;
    const prevP = cust.consignments.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; }).length;
    if (prevP > 2 && curP > 0) {
      const drop = ((curP - prevP) / prevP) * 100;
      if (drop < -20) {
        const curR = cust.consignments.filter(c => new Date(c.ConsignmentDate) >= cutoff).reduce((s, c) => s + (c.Revenue || 0), 0);
        const prevR = cust.consignments.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; }).reduce((s, c) => s + (c.Revenue || 0), 0);
        atRiskVolumeDrop.push({ name: cust.name, dropPercent: drop.toFixed(1), currentVolume: curP, previousVolume: prevP, currentRevenue: curR, previousRevenue: prevR });
      }
    }
  });

  // Monthly trends
  const monthlyTrends = [];
  for (let i = 5; i >= 0; i--) {
    const ms = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const me = new Date(now.getFullYear(), now.getMonth() - i + 1, 0, 23, 59, 59);
    const mc = raw.filter(c => { const d = new Date(c.ConsignmentDate); return d >= ms && d <= me; });
    monthlyTrends.push({ label: ms.toLocaleString('en-AU', { month: 'short', year: '2-digit' }), revenue: mc.reduce((s, c) => s + (c.Revenue || 0), 0), consignments: mc.length });
  }

  // Service types
  const serviceTypes = {};
  inPeriod.forEach(c => { const st = c.ServiceType || 'Standard'; serviceTypes[st] = (serviceTypes[st] || 0) + (c.Revenue || 0); });

  // Customer list
  const customerList = Object.values(customerMap).map(cust => {
    const pc = cust.consignments.filter(c => new Date(c.ConsignmentDate) >= cutoff);
    const pp = cust.consignments.filter(c => { const d = new Date(c.ConsignmentDate); return d >= prevCutoff && d < cutoff; });
    const pr = pc.reduce((s, c) => s + (c.Revenue || 0), 0);
    const ppr = pp.reduce((s, c) => s + (c.Revenue || 0), 0);
    const lo = cust.dates.length > 0 ? new Date(Math.max(...cust.dates)).toISOString().split('T')[0] : 'Never';
    return { name: cust.name, periodRevenue: pr, prevPeriodRevenue: ppr, periodConsignments: pc.length, avgPerCon: pc.length > 0 ? (pr / pc.length).toFixed(2) : 0, lastOrder: lo, trend: ppr > 0 ? (((pr - ppr) / ppr) * 100).toFixed(1) : 'NEW' };
  }).sort((a, b) => b.periodRevenue - a.periodRevenue);

  // Lanes
  const laneMap = {};
  inPeriod.forEach(c => {
    const key = (c.SenderSuburb || 'Unknown') + '→' + (c.ReceiverSuburb || 'Unknown');
    if (!laneMap[key]) laneMap[key] = { origin: c.SenderSuburb || 'Unknown', dest: c.ReceiverSuburb || 'Unknown', cons: 0, rev: 0, customers: {} };
    laneMap[key].cons++; laneMap[key].rev += (c.Revenue || 0);
    const cn = c.SenderName || c.CustomerName || 'Unknown';
    laneMap[key].customers[cn] = (laneMap[key].customers[cn] || 0) + 1;
  });
  const lanes = Object.values(laneMap).sort((a, b) => b.cons - a.cons).slice(0, 10).map(l => ({ ...l, topCustomer: Object.entries(l.customers).sort((a, b) => b[1] - a[1])[0]?.[0] || 'Unknown' }));

  // Peak days
  const dayVol = [0, 0, 0, 0, 0, 0, 0];
  inPeriod.forEach(c => { dayVol[new Date(c.ConsignmentDate).getDay()]++; });
  const peakDays = [...dayVol.slice(1), dayVol[0]];

  // Zones
  const zoneMap = {};
  inPeriod.forEach(c => { const z = c.ReceiverState || 'Unknown'; if (!zoneMap[z]) zoneMap[z] = { zone: z, rev: 0, cons: 0 }; zoneMap[z].rev += (c.Revenue || 0); zoneMap[z].cons++; });

  return {
    generatedAt: now.toISOString(), period, totalStored: raw.length,
    kpis: { totalRevenue: totalRev, prevRevenue: prevRev, totalConsignments: inPeriod.length, prevConsignments: inPrev.length, activeCustomers: customerSet.size, avgRevenuePerCon: inPeriod.length > 0 ? (totalRev / inPeriod.length).toFixed(2) : 0, atRiskCount: atRiskInactive.length + atRiskVolumeDrop.length, newCustomers: [...customerSet].filter(c => !prevCustomerSet.has(c)).length },
    atRisk: { inactive: atRiskInactive, volumeDrop: atRiskVolumeDrop },
    monthlyTrends, serviceTypes,
    customers: { top10: customerList.slice(0, 10), bottom10: customerList.filter(c => c.periodRevenue > 0).slice(-10).reverse(), all: customerList },
    concentration: { top5Revenue: customerList.slice(0, 5).reduce((s, c) => s + c.periodRevenue, 0), totalRevenue: totalRev, top5: customerList.slice(0, 5).map(c => ({ name: c.name, revenue: c.periodRevenue })) },
    lanes, peakDays, zones: Object.values(zoneMap).sort((a, b) => b.rev - a.rev)
  };
}

// ── Server ──────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  cors(res);
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;

  // CORS preflight
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }

  // ── API routes ──
  if (pathname === '/api/webhook' && req.method === 'POST') {
    const token = parsed.query.token || req.headers['x-auth-token'] || '';
    if (token !== WEBHOOK_TOKEN) return json(res, { error: 'Unauthorised' }, 401);

    try {
      const body = await readBody(req);
      let consignments = [];
      const ct = req.headers['content-type'] || '';

      if (ct.includes('xml') || body.trim().startsWith('<')) {
        consignments = parseXMLConsignments(body);
      } else {
        const parsed = JSON.parse(body);
        if (parsed.consignments) consignments = parsed.consignments;
        else if (Array.isArray(parsed)) consignments = parsed;
        else if (parsed.ConsignmentNumber) consignments = [parsed];
      }

      const existing = readData();
      const map = new Map(existing.map(c => [c.ConsignmentNumber, c]));
      consignments.forEach(c => { c._receivedAt = new Date().toISOString(); map.set(c.ConsignmentNumber, c); });
      writeData(Array.from(map.values()));

      console.log('[WEBHOOK] Received ' + consignments.length + ' consignment(s) — total: ' + map.size);
      return json(res, { success: true, received: consignments.length, total: map.size });
    } catch (err) {
      console.error('[WEBHOOK] Error:', err.message);
      return json(res, { error: err.message }, 400);
    }
  }

  if (pathname === '/api/dashboard' && req.method === 'GET') {
    const period = parseInt(parsed.query.period) || 30;
    return json(res, aggregateDashboard(period));
  }

  if (pathname === '/api/stats' && req.method === 'GET') {
    const raw = readData();
    return json(res, {
      totalConsignments: raw.length,
      uniqueCustomers: new Set(raw.map(c => c.SenderName || c.CustomerName).filter(Boolean)).size,
      oldestRecord: raw.length > 0 ? raw.reduce((min, c) => c.ConsignmentDate < min ? c.ConsignmentDate : min, raw[0].ConsignmentDate) : null,
      newestRecord: raw.length > 0 ? raw.reduce((max, c) => c.ConsignmentDate > max ? c.ConsignmentDate : max, raw[0].ConsignmentDate) : null
    });
  }

  if (pathname === '/api/import-csv' && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const lines = body.split('\n').filter(l => l.trim());
      if (lines.length < 2) return json(res, { error: 'No data rows' }, 400);

      const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
      const existing = readData();
      const map = new Map(existing.map(c => [c.ConsignmentNumber, c]));
      let imported = 0;

      for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',').map(v => v.trim().replace(/"/g, ''));
        const row = {}; headers.forEach((h, idx) => { row[h] = values[idx] || ''; });
        const c = {
          ConsignmentNumber: row.ConsignmentNumber || row.Connote || row['Con Note'] || 'IMPORT-' + i,
          ConsignmentDate: row.ConsignmentDate || row.Date || row['Created Date'] || '',
          SenderName: row.SenderName || row.Sender || row.Customer || row.Account || '',
          SenderSuburb: row.SenderSuburb || row['Sender Suburb'] || '',
          SenderState: row.SenderState || row['Sender State'] || '',
          ReceiverName: row.ReceiverName || row.Receiver || '',
          ReceiverSuburb: row.ReceiverSuburb || row['Receiver Suburb'] || row['Delivery Suburb'] || '',
          ReceiverState: row.ReceiverState || row['Receiver State'] || row['Delivery State'] || '',
          ServiceType: row.ServiceType || row.Service || row['Service Level'] || 'Standard',
          Status: row.Status || row['Consignment Status'] || '',
          Revenue: parseFloat(row.Revenue || row.Charge || row['Total Charge'] || row.Amount || 0),
          Weight: parseFloat(row.Weight || row['Total Weight'] || 0),
          Items: parseInt(row.Items || row['Item Count'] || 1),
          DeliveryDate: row.DeliveryDate || row['Delivery Date'] || '',
          CustomerName: row.SenderName || row.Sender || row.Customer || row.Account || '',
          _receivedAt: new Date().toISOString(), _source: 'csv-import'
        };
        map.set(c.ConsignmentNumber, c);
        imported++;
      }
      writeData(Array.from(map.values()));
      return json(res, { success: true, imported, total: map.size });
    } catch (err) {
      return json(res, { error: err.message }, 400);
    }
  }

  // ── Static files ──
  let filePath = pathname === '/' ? '/index.html' : pathname;
  filePath = path.join(__dirname, 'public', filePath);

  if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    return fs.createReadStream(filePath).pipe(res);
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

server.listen(PORT, () => {
  console.log('TV Sales Dashboard Server running on port ' + PORT);
  console.log('Dashboard: http://localhost:' + PORT);
  console.log('Webhook:   http://localhost:' + PORT + '/api/webhook?token=...');
});
