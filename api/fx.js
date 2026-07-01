const https = require('https');
const zlib  = require('zlib');

const SF_ACCOUNT   = process.env.SF_ACCOUNT   || 'RBI-RBI_USE1';
const SF_PAT       = process.env.SF_PAT;
const SF_WAREHOUSE = process.env.SF_WAREHOUSE  || 'ANALYSIS_PLK';
const SF_DATABASE  = process.env.SF_DATABASE   || 'BRAND_PLK';
const SF_SCHEMA    = process.env.SF_SCHEMA     || 'SCORECARD';
const SF_ROLE      = process.env.SF_ROLE       || 'ANALYST_PLK';

// ── Monthly SAP USD↔CAD exchange rates from 2021-01 onward.
// The client averages the 12 monthly USD→CAD rates per calendar year for the FY columns,
// and maps monthly rates onto the sales / EBITDA windows — same logic as the old FX file.
const FX_SQL = `
SELECT
    FROM_CURRENCY,
    TO_CURRENCY,
    PROVENANCE$PARTITION_KEY AS MONTH,
    AVERAGE_RATE
FROM BRAND_PLK.SCORECARD.SAP_EXCHANGE_RATES
WHERE PROVENANCE$PARTITION_KEY >= '2021-01-01'
  AND ((FROM_CURRENCY = 'CAD' AND TO_CURRENCY = 'USD')
    OR (FROM_CURRENCY = 'USD' AND TO_CURRENCY = 'CAD'))
ORDER BY FROM_CURRENCY, MONTH
`.trim();

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
  let result = post.body, status = post.status, attempts = 0;
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

const MN = ['', 'Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const MON3 = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
// Robustly turn a SAP MONTH value into "Mon-YY" (e.g. "Jan-21"), handling the formats
// Snowflake / SAP partition keys tend to arrive in:
//   "2021-01-01" / "2021-01" / "2021/01/01" (ISO-ish)   → Jan-21
//   "20210101"   / "202101"  (compact)                  → Jan-21
//   integer days-since-1970 (Snowflake DATE via REST)   → Jan-21
//   "Jan-2021" / "JANUARY 2021" (month-name)            → Jan-21
function monthLabel(v) {
  if (v == null) return null;
  const s = String(v).trim();
  let m = s.match(/^(\d{4})[-/](\d{1,2})/);              // 2021-01, 2021-01-01, 2021/01
  if (m) return MN[parseInt(m[2],10)] + '-' + m[1].slice(2);
  m = s.match(/^(\d{4})(\d{2})\d{2}$/) || s.match(/^(\d{4})(\d{2})$/); // 20210101, 202101
  if (m) return MN[parseInt(m[2],10)] + '-' + m[1].slice(2);
  if (/^\d+$/.test(s)) {                                 // integer days since 1970-01-01
    const days = parseInt(s, 10);
    if (days > 3000 && days < 80000) {
      const dt = new Date(days * 86400000);
      return MN[dt.getUTCMonth()+1] + '-' + String(dt.getUTCFullYear()).slice(2);
    }
  }
  m = s.match(/([A-Za-z]{3,})[^\d]*(\d{2,4})/);          // Jan-2021, JANUARY 2021
  if (m) { const i = MON3.indexOf(m[1].slice(0,3).toLowerCase()); if (i >= 0) return MN[i+1] + '-' + String(m[2]).slice(-2); }
  return null;
}

// Columns: [0]=FROM_CURRENCY [1]=TO_CURRENCY [2]=MONTH [3]=AVERAGE_RATE
function pivot(rows) {
  const n = v => { const f = parseFloat(v); return isNaN(f) ? null : f; };
  const usdcad = {}, cadusd = {};
  rows.forEach(r => {
    const from = String(r[0] || '').toUpperCase().trim();
    const to   = String(r[1] || '').toUpperCase().trim();
    const label = monthLabel(r[2]);
    const rate = n(r[3]);
    if (!label || rate == null) return;
    if (from === 'USD' && to === 'CAD') usdcad[label] = rate;
    else if (from === 'CAD' && to === 'USD') cadusd[label] = rate;
  });
  return { usdcad, cadusd };
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(204).end(); return; }

  if (!SF_PAT) {
    return res.status(503).json({ error: 'SF_PAT environment variable not set on server' });
  }

  try {
    const rows = await runQuery(FX_SQL);
    const { usdcad, cadusd } = pivot(rows);
    res.status(200).json({ usdcad, cadusd, months: Object.keys(usdcad).length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
