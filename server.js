const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const url   = require('url');

const PORT = 3000;
const STATE_FILE  = path.join(__dirname, 'state.json');
const SS_TOKEN    = process.env.SS_TOKEN;
const SS_SHEET_ID = process.env.SS_SHEET_ID || '3928541238349700';

// ── Snowflake connection settings
const SF_ACCOUNT   = process.env.SF_ACCOUNT   || 'RBI-RBI_USE1';
const SF_PAT       = process.env.SF_PAT;                          // REQUIRED: Personal Access Token
const SF_WAREHOUSE = process.env.SF_WAREHOUSE  || 'ANALYSIS_PLK';
const SF_DATABASE  = process.env.SF_DATABASE   || 'BRAND_PLK';
const SF_SCHEMA    = process.env.SF_SCHEMA     || 'TLOG';
const SF_ROLE      = process.env.SF_ROLE       || 'ANALYST_PLK';

// ── 1-hour in-memory cache for service mode data
let sfCache = { data: null, ts: 0 };
const SF_CACHE_MS = 60 * 60 * 1000; // 1 hour

// ── Service Mode SQL (from brand_plk.pmix.DAILY_SERVICE_MODE_COMPARABLE_SALES_AND_TRAFFIC)
const SERVICE_MODE_SQL = `
SELECT
    store_id,
    fz_name,
    division,
    dma,
    service_mode_category,
    SUM(cy_category_sales)     AS cy_category_sales,
    SUM(cy_store_total_sales)  AS cy_store_total_sales,
    ROUND(
      CASE WHEN SUM(cy_store_total_sales) > 0
           THEN SUM(cy_category_sales) / SUM(cy_store_total_sales) * 100
           ELSE NULL END, 2
    ) AS cy_sales_mix_pct
FROM brand_plk.pmix.DAILY_SERVICE_MODE_COMPARABLE_SALES_AND_TRAFFIC
WHERE country_code = 'US'
  AND date BETWEEN '2025-12-29' AND DATEADD(DAY, -1, CURRENT_DATE())
GROUP BY store_id, fz_name, division, dma, service_mode_category
ORDER BY store_id, service_mode_category
`.trim();

// ── Execute a SQL statement via Snowflake REST API v2 (PAT bearer auth + polling)
function querySnowflake(sql) {
  return new Promise((resolve, reject) => {
    const host = `${SF_ACCOUNT}.snowflakecomputing.com`;

    const body = JSON.stringify({
      statement: sql,
      timeout: 120,
      database: SF_DATABASE,
      schema: SF_SCHEMA,
      warehouse: SF_WAREHOUSE,
      role: SF_ROLE
    });

    const postOpts = {
      hostname: host,
      path: '/api/v2/statements',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${SF_PAT}`,
        'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
        'Accept': 'application/json',
        'Content-Length': Buffer.byteLength(body)
      }
    };

    // POST the statement
    const req = https.request(postOpts, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(raw); } catch(e) { return reject(new Error('Invalid JSON from Snowflake: ' + raw.slice(0,200))); }

        if (res.statusCode === 200) {
          // Synchronous result — collect all pages
          return collectPages(host, parsed).then(resolve).catch(reject);
        } else if (res.statusCode === 202) {
          // Async — poll until done
          const stmtHandle = parsed.statementHandle;
          if (!stmtHandle) return reject(new Error('No statementHandle in 202 response'));
          return pollStatement(host, stmtHandle).then(resolve).catch(reject);
        } else {
          return reject(new Error(`Snowflake POST ${res.statusCode}: ${raw.slice(0,300)}`));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ── Poll a pending statement until complete (max ~120s, 2s intervals)
function pollStatement(host, stmtHandle, attempts = 0) {
  return new Promise((resolve, reject) => {
    if (attempts > 60) return reject(new Error('Snowflake query timed out after polling'));

    const getOpts = {
      hostname: host,
      path: `/api/v2/statements/${stmtHandle}`,
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${SF_PAT}`,
        'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
        'Accept': 'application/json'
      }
    };

    const req = https.request(getOpts, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        let parsed;
        try { parsed = JSON.parse(raw); } catch(e) { return reject(new Error('Poll parse error')); }

        if (res.statusCode === 200) {
          return collectPages(host, parsed).then(resolve).catch(reject);
        } else if (res.statusCode === 202) {
          // Still running — wait 2s then retry
          setTimeout(() => pollStatement(host, stmtHandle, attempts + 1).then(resolve).catch(reject), 2000);
        } else {
          return reject(new Error(`Snowflake poll ${res.statusCode}: ${raw.slice(0,300)}`));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// ── Collect all result pages from a completed statement response
function collectPages(host, firstResponse) {
  return new Promise(async (resolve, reject) => {
    try {
      const allRows = [];
      const firstRows = (firstResponse.data || []);
      firstRows.forEach(r => allRows.push(r));

      const total = firstResponse.resultSetMetaData && firstResponse.resultSetMetaData.partitionInfo
        ? firstResponse.resultSetMetaData.partitionInfo.length
        : 1;

      // Pages 1..N (page 0 already included in first response)
      for (let p = 1; p < total; p++) {
        const stmtHandle = firstResponse.statementHandle;
        const rows = await fetchPage(host, stmtHandle, p);
        rows.forEach(r => allRows.push(r));
      }

      resolve({ rows: allRows, meta: firstResponse.resultSetMetaData });
    } catch(e) { reject(e); }
  });
}

// ── Fetch a single result page
function fetchPage(host, stmtHandle, partition) {
  return new Promise((resolve, reject) => {
    const getOpts = {
      hostname: host,
      path: `/api/v2/statements/${stmtHandle}?partition=${partition}`,
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${SF_PAT}`,
        'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
        'Accept': 'application/json'
      }
    };
    const req = https.request(getOpts, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try { resolve(JSON.parse(raw).data || []); }
        catch(e) { reject(new Error('Page parse error')); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// ── Pivot Snowflake rows into per-store objects for client
// Columns: [0]=store_id [1]=fz_name [2]=division [3]=dma [4]=service_mode_category
//          [5]=cy_category_sales [6]=cy_store_total_sales [7]=cy_sales_mix_pct (0-100)
function pivotServiceMode(rows) {
  const CAT_MAP = {
    'DRIVE THRU':  'dt_pct',
    'EAT IN':      'eatin_pct',
    'TAKE OUT':    'takeout_pct',
    'DELIVERY':    'delivery_pct'
  };
  const stores = {};

  rows.forEach(row => {
    const plk = parseInt(row[0]);
    if (isNaN(plk) || plk <= 0) return;
    const cat = String(row[4] || '').toUpperCase().trim();
    const pctKey = CAT_MAP[cat];
    if (!pctKey) return;

    if (!stores[plk]) stores[plk] = { plk };
    // cy_sales_mix_pct is already 0-100 — convert to 0-1 decimal for client
    const pct = parseFloat(row[7]);
    stores[plk][pctKey] = isNaN(pct) ? null : pct / 100;
  });

  return Object.values(stores);
}

const server = http.createServer(async (req, res) => {
  const parsed = url.parse(req.url, true);

  // ── CORS headers for all responses
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // ── POST /api/save-state  →  persist parsed SR/FR/DR to state.json
  if (req.method === 'POST' && parsed.pathname === '/api/save-state') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        JSON.parse(body); // validate before writing
        fs.writeFile(STATE_FILE, body, 'utf8', err => {
          if (err) {
            console.error('[state] Write error:', err.message);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: err.message }));
          } else {
            console.log('[state] Saved (' + (body.length / 1024).toFixed(0) + ' KB)');
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ok: true }));
          }
        });
      } catch(e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid JSON: ' + e.message }));
      }
    });
    return;
  }

  // ── GET /api/load-state  →  return saved state.json if it exists
  if (req.method === 'GET' && parsed.pathname === '/api/load-state') {
    fs.readFile(STATE_FILE, 'utf8', (err, data) => {
      if (err) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'No saved state' }));
      } else {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(data);
      }
    });
    return;
  }

  // ── /api/renovations  →  proxy to Smartsheet
  if (parsed.pathname === '/api/renovations') {
    const options = {
      hostname: 'api.smartsheet.com',
      path: `/2.0/sheets/${SS_SHEET_ID}`,
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${SS_TOKEN}`,
        'Accept': 'application/json'
      }
    };
    const proxy = https.request(options, (ssRes) => {
      let data = '';
      ssRes.on('data', chunk => data += chunk);
      ssRes.on('end', () => {
        res.writeHead(ssRes.statusCode, { 'Content-Type': 'application/json' });
        res.end(data);
      });
    });
    proxy.on('error', e => {
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    });
    proxy.end();
    return;
  }

  // ── /api/service-mode  →  Snowflake query (with 1-hour cache)
  if (parsed.pathname === '/api/service-mode') {
    if (!SF_PAT) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'SF_PAT environment variable not set' }));
      return;
    }

    // Serve from cache if fresh
    const now = Date.now();
    if (sfCache.data && (now - sfCache.ts) < SF_CACHE_MS) {
      res.writeHead(200, { 'Content-Type': 'application/json', 'X-Cache': 'HIT' });
      res.end(JSON.stringify({ stores: sfCache.data, cached: true, age: Math.round((now - sfCache.ts) / 1000) }));
      return;
    }

    try {
      console.log('[Snowflake] Running service mode query…');
      const { rows } = await querySnowflake(SERVICE_MODE_SQL);
      const stores = pivotServiceMode(rows);
      sfCache = { data: stores, ts: Date.now() };
      console.log(`[Snowflake] ✓ ${stores.length} stores returned`);
      res.writeHead(200, { 'Content-Type': 'application/json', 'X-Cache': 'MISS' });
      res.end(JSON.stringify({ stores, cached: false }));
    } catch(e) {
      console.error('[Snowflake] Error:', e.message);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }

  // ── /api/service-mode/refresh  →  bust cache and re-query
  if (parsed.pathname === '/api/service-mode/refresh') {
    sfCache = { data: null, ts: 0 };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, message: 'Cache cleared — next /api/service-mode call will re-query Snowflake' }));
    return;
  }

  // ── Serve static files
  let filePath = parsed.pathname === '/' ? '/index.html' : parsed.pathname;
  filePath = path.join(__dirname, filePath);

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const ext = path.extname(filePath);
    const types = { '.html':'text/html', '.js':'application/javascript',
                    '.css':'text/css', '.json':'application/json' };
    res.writeHead(200, { 'Content-Type': types[ext] || 'text/plain' });
    res.end(data);
  });
});

server.listen(PORT, () => {
  console.log(`\n✓ PLK Master FZ File running at http://localhost:${PORT}`);
  console.log(`  Smartsheet proxy:    http://localhost:${PORT}/api/renovations`);
  console.log(`  Service Mode (live): http://localhost:${PORT}/api/service-mode`);
  console.log(`  Snowflake PAT set:   ${SF_PAT ? 'YES' : 'NO ← set SF_PAT env var'}\n`);
});
