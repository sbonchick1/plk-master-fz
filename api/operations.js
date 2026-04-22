const https = require('https');
const zlib  = require('zlib');

const SF_ACCOUNT   = process.env.SF_ACCOUNT   || 'RBI-RBI_USE1';
const SF_PAT       = process.env.SF_PAT;
const SF_WAREHOUSE = process.env.SF_WAREHOUSE  || 'ANALYSIS_PLK';
const SF_DATABASE  = process.env.SF_DATABASE   || 'BRAND_PLK';
const SF_SCHEMA    = process.env.SF_SCHEMA     || 'TLOG';
const SF_ROLE      = process.env.SF_ROLE       || 'ANALYST_PLK';

// ── Dynamic: auto-resolves last 3 FSS rounds based on current date
const OPERATIONS_SQL = `
WITH last_3_rounds AS (
    SELECT
        YEAR(CURRENT_DATE) || ' R' || CEIL(MONTH(CURRENT_DATE) / 4) AS current_round,
        CASE CEIL(MONTH(CURRENT_DATE) / 4)
            WHEN 1 THEN
                ARRAY_CONSTRUCT(
                    YEAR(CURRENT_DATE) || ' R1',
                    (YEAR(CURRENT_DATE) - 1) || ' R3',
                    (YEAR(CURRENT_DATE) - 1) || ' R2'
                )
            WHEN 2 THEN
                ARRAY_CONSTRUCT(
                    YEAR(CURRENT_DATE) || ' R2',
                    YEAR(CURRENT_DATE) || ' R1',
                    (YEAR(CURRENT_DATE) - 1) || ' R3'
                )
            WHEN 3 THEN
                ARRAY_CONSTRUCT(
                    YEAR(CURRENT_DATE) || ' R3',
                    YEAR(CURRENT_DATE) || ' R2',
                    YEAR(CURRENT_DATE) || ' R1'
                )
        END AS rounds
)
SELECT
    f.REST_NO,
    f.FZ_CODE,
    f.FZ_NAME,
    f.TIME_PERIOD,
    f.B3R_COUNT,
    f.TOTAL_TICKETS,
    f.ACR_VALUE,
    f.DRIVER_WAIT_TIME,
    f.EVENT_CAR_COUNT,
    f.OVER_ALL_STAR_RATING,
    f.REV_OVERALL_SCORE,
    f.REV_T1_OVERALL_SCORE
FROM BRAND_PLK.POPSIGHTS.FSS_ROUNDLY_REST_LEVEL f,
     last_3_rounds l
WHERE f.TIME_PERIOD IN (l.rounds[0], l.rounds[1], l.rounds[2])
ORDER BY f.FZ_NAME, f.REST_NO, f.TIME_PERIOD DESC
`.trim();

// ── Snowflake REST API helper (same pattern as service-mode.js)
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
        'Accept-Encoding': 'identity',
        ...(payload ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) } : {})
      }
    };
    const req = https.request(opts, (res) => {
      const enc = res.headers['content-encoding'] || '';
      const chunks = [];
      res.on('data', d => chunks.push(typeof d === 'string' ? Buffer.from(d) : d));
      res.on('end', () => {
        const buf = Buffer.concat(chunks);
        const decode = (b) => {
          try { return JSON.parse(b.toString('utf8')); }
          catch(e) { throw new Error('JSON parse error: ' + b.slice(0,200).toString('utf8')); }
        };
        if (enc.includes('gzip')) {
          zlib.gunzip(buf, (err, d) => {
            if (err) return reject(new Error('gunzip error: ' + err.message));
            try { resolve({ status: res.statusCode, body: decode(d) }); } catch(e) { reject(e); }
          });
        } else {
          try { resolve({ status: res.statusCode, body: decode(buf) }); } catch(e) { reject(e); }
        }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

async function runQuery(sql) {
  const post = await sfRequest('/api/v2/statements', 'POST', {
    statement: sql, timeout: 120,
    database: SF_DATABASE, schema: SF_SCHEMA,
    warehouse: SF_WAREHOUSE, role: SF_ROLE
  });
  let result = post.body, status = post.status;
  let attempts = 0;
  while (status === 202 && attempts < 60) {
    await new Promise(r => setTimeout(r, 2000));
    const poll = await sfRequest(`/api/v2/statements/${result.statementHandle}`, 'GET', null);
    status = poll.status; result = poll.body; attempts++;
  }
  if (status !== 200) throw new Error(`Snowflake error ${status}: ${JSON.stringify(result).slice(0,300)}`);
  const allRows = [...(result.data || [])];
  const partitions = (result.resultSetMetaData && result.resultSetMetaData.partitionInfo) || [];
  for (let p = 1; p < partitions.length; p++) {
    const page = await sfRequest(`/api/v2/statements/${result.statementHandle}?partition=${p}`, 'GET', null);
    (page.body.data || []).forEach(r => allRows.push(r));
  }
  return allRows;
}

// ── Sort TIME_PERIOD strings descending: "2026 R1" > "2025 R3" > "2025 R2"
function periodSort(a, b) {
  const parse = p => { const [y,r] = p.split(' '); return parseInt(y)*10 + parseInt(r.replace('R','')); };
  return parse(b) - parse(a);
}

// ── FSS grade from score
function fssGrade(v) {
  if (v == null) return null;
  return v >= 4 ? 'A' : v >= 3 ? 'B' : v >= 2.5 ? 'D' : 'F';
}

// ── Pivot rows → per-store objects
// Columns: [0]=REST_NO [1]=FZ_CODE [2]=FZ_NAME [3]=TIME_PERIOD
//          [4]=B3R_COUNT [5]=TOTAL_TICKETS [6]=ACR_VALUE [7]=DRIVER_WAIT_TIME
//          [8]=EVENT_CAR_COUNT [9]=OVER_ALL_STAR_RATING [10]=REV_OVERALL_SCORE [11]=REV_T1_OVERALL_SCORE
// Positional mapping (sorted desc): [0]=rtd [1]=r3_prev [2]=r2_prev
function pivotOps(rows) {
  // Group by REST_NO
  const byStore = {};
  rows.forEach(row => {
    const plk = parseInt(row[0]);
    if (isNaN(plk) || plk <= 0) return;
    if (!byStore[plk]) byStore[plk] = [];
    byStore[plk].push(row);
  });

  const stores = [];
  Object.entries(byStore).forEach(([plkStr, storeRows]) => {
    const plk = parseInt(plkStr);
    // Sort by TIME_PERIOD descending — most recent first
    storeRows.sort((a, b) => periodSort(a[3], b[3]));

    const store = { plk };
    const n = v => { const f = parseFloat(v); return isNaN(f) ? null : f; };

    // Most recent round → rtd fields + operational metrics
    if (storeRows[0]) {
      const r = storeRows[0];
      store.fss_rtd_period = r[3];                    // e.g. "2026 R1"
      store.fss_rtd        = n(r[9]);
      store.fss_grade      = fssGrade(store.fss_rtd);
      store.b3             = n(r[4]);
      store.total_tickets  = n(r[5]);
      store.acr            = n(r[6]);
      store.wait_time      = n(r[7]);
      store.car_count      = n(r[8]);
      store.rev            = n(r[10]);
      store.rev_t1         = n(r[11]);
    }

    // Second most recent → fss_r3
    if (storeRows[1]) {
      store.fss_r3_period = storeRows[1][3];
      store.fss_r3        = n(storeRows[1][9]);
    }

    // Third most recent → fss_r2
    if (storeRows[2]) {
      store.fss_r2_period = storeRows[2][3];
      store.fss_r2        = n(storeRows[2][9]);
    }

    stores.push(store);
  });

  return stores;
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(204).end(); return; }

  if (!SF_PAT) {
    return res.status(503).json({ error: 'SF_PAT environment variable not set on server' });
  }

  try {
    const rows = await runQuery(OPERATIONS_SQL);
    const stores = pivotOps(rows);

    // Extract the 3 unique round labels from raw rows, sorted desc: [rtd, r3, r2]
    const uniquePeriods = [...new Set(rows.map(r => r[3]).filter(Boolean))];
    uniquePeriods.sort((a, b) => periodSort(a, b));
    const rounds = uniquePeriods.slice(0, 3); // [most recent, second, third]

    res.status(200).json({ stores, count: stores.length, rounds });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
