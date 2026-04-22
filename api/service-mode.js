const https = require('https');

const SF_ACCOUNT   = process.env.SF_ACCOUNT   || 'RBI-RBI_USE1';
const SF_PAT       = process.env.SF_PAT;
const SF_WAREHOUSE = process.env.SF_WAREHOUSE  || 'ANALYSIS_PLK';
const SF_DATABASE  = process.env.SF_DATABASE   || 'BRAND_PLK';
const SF_SCHEMA    = process.env.SF_SCHEMA     || 'TLOG';
const SF_ROLE      = process.env.SF_ROLE       || 'ANALYST_PLK';

const SERVICE_MODE_SQL = `
SELECT
    store_id,
    service_mode_category,
    SUM(cy_category_sales)    AS cy_category_sales,
    SUM(cy_store_total_sales) AS cy_store_total_sales
FROM brand_plk.pmix.DAILY_SERVICE_MODE_COMPARABLE_SALES_AND_TRAFFIC
WHERE country_code = 'US'
  AND date BETWEEN '2025-12-29' AND DATEADD(DAY, -1, CURRENT_DATE())
GROUP BY store_id, service_mode_category
ORDER BY store_id, service_mode_category
`.trim();

// ── POST SQL to Snowflake, return parsed JSON response
function sfRequest(path, method, body) {
  return new Promise((resolve, reject) => {
    const payload = body ? JSON.stringify(body) : null;
    const opts = {
      hostname: `${SF_ACCOUNT}.snowflakecomputing.com`,
      path,
      method,
      headers: {
        'Authorization': `Bearer ${SF_PAT}`,
        'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
        'User-Agent': 'plk-master-fz/1.0',
        'Accept': 'application/json',
        ...(payload ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) } : {})
      }
    };
    const req = https.request(opts, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(raw) }); }
        catch(e) { reject(new Error('JSON parse error: ' + raw.slice(0, 200))); }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

// ── Poll until statement is done, then collect all result pages
async function runQuery(sql) {
  // Submit
  const post = await sfRequest('/api/v2/statements', 'POST', {
    statement: sql,
    timeout: 120,
    database: SF_DATABASE,
    schema: SF_SCHEMA,
    warehouse: SF_WAREHOUSE,
    role: SF_ROLE
  });

  let result = post.body;
  let status = post.status;

  // Poll while 202 (running)
  let attempts = 0;
  while (status === 202 && attempts < 60) {
    await new Promise(r => setTimeout(r, 2000));
    const poll = await sfRequest(`/api/v2/statements/${result.statementHandle}`, 'GET', null);
    status = poll.status;
    result = poll.body;
    attempts++;
  }

  if (status !== 200) {
    throw new Error(`Snowflake error ${status}: ${JSON.stringify(result).slice(0, 300)}`);
  }

  // Collect page 0 rows
  const allRows = [...(result.data || [])];

  // Collect additional pages if any
  const partitions = (result.resultSetMetaData && result.resultSetMetaData.partitionInfo) || [];
  for (let p = 1; p < partitions.length; p++) {
    const page = await sfRequest(`/api/v2/statements/${result.statementHandle}?partition=${p}`, 'GET', null);
    (page.body.data || []).forEach(r => allRows.push(r));
  }

  return allRows;
}

// ── Pivot rows → [{plk, dt_pct, eatin_pct, takeout_pct, delivery_pct}]
// Columns: [0]=store_id [1]=service_mode_category [2]=cy_category_sales [3]=cy_store_total_sales
function pivotRows(rows) {
  const CAT_MAP = {
    'DRIVE THRU': 'dt_pct',
    'EAT IN':     'eatin_pct',
    'TAKE OUT':   'takeout_pct',
    'DELIVERY':   'delivery_pct'
  };
  const stores = {};
  rows.forEach(row => {
    const plk = parseInt(row[0]);
    if (isNaN(plk) || plk <= 0) return;
    const cat = String(row[1] || '').toUpperCase().trim();
    const key = CAT_MAP[cat];
    if (!key) return;
    if (!stores[plk]) stores[plk] = { plk };
    const catSales   = parseFloat(row[2]) || 0;
    const totalSales = parseFloat(row[3]) || 0;
    stores[plk][key] = totalSales > 0 ? catSales / totalSales : null;
  });
  return Object.values(stores);
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(204).end(); return; }

  if (!SF_PAT) {
    return res.status(503).json({ error: 'SF_PAT environment variable not set on server' });
  }

  try {
    const rows = await runQuery(SERVICE_MODE_SQL);
    const stores = pivotRows(rows);
    res.status(200).json({ stores, count: stores.length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
