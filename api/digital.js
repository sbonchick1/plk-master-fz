const https = require('https');
const zlib  = require('zlib');

const SF_ACCOUNT   = process.env.SF_ACCOUNT   || 'RBI-RBI_USE1';
const SF_PAT       = process.env.SF_PAT;
const SF_WAREHOUSE = process.env.SF_WAREHOUSE  || 'ANALYSIS_PLK';
const SF_DATABASE  = process.env.SF_DATABASE   || 'BRAND_PLK';
const SF_SCHEMA    = process.env.SF_SCHEMA     || 'TLOG';
const SF_ROLE      = process.env.SF_ROLE       || 'ANALYST_PLK';

const DIGITAL_SQL = `
SELECT
    STORE,
    SUM(
        IFNULL(IN_STORE_OFFERS_SALES, 0)
        + IFNULL(MOAP_TLOG_SALES, 0)
        + IFNULL(WHITE_LABEL_DELIVERY_SALES, 0)
        + IFNULL(UE_TLOG_SALES, 0) + IFNULL(DD_TLOG_SALES, 0) + IFNULL(GH_TLOG_SALES, 0)
        + IFNULL(CC_TOKEN_SALES, 0)
        + IFNULL(KIOSK_TLOG_SALES, 0)
        + IFNULL(CATERING_TLOG_SALES, 0)
    ) AS YTD_DIGITAL_SALES,
    SUM(SYSTEM_SALES) AS YTD_SYSTEMWIDE_SALES,
    CASE
        WHEN SUM(SYSTEM_SALES) > 0
        THEN SUM(
            IFNULL(IN_STORE_OFFERS_SALES, 0)
            + IFNULL(MOAP_TLOG_SALES, 0)
            + IFNULL(WHITE_LABEL_DELIVERY_SALES, 0)
            + IFNULL(UE_TLOG_SALES, 0) + IFNULL(DD_TLOG_SALES, 0) + IFNULL(GH_TLOG_SALES, 0)
            + IFNULL(CC_TOKEN_SALES, 0)
            + IFNULL(KIOSK_TLOG_SALES, 0)
            + IFNULL(CATERING_TLOG_SALES, 0)
        ) / SUM(SYSTEM_SALES)
        ELSE 0
    END AS YTD_PERCENT_DIGITAL_SALES
FROM DATA_PLK.DIGITAL_ANALYTICS.DIGITAL_PERFORMANCE_METRICS
WHERE DAY >= DATE_TRUNC('YEAR', CURRENT_DATE())
GROUP BY STORE
ORDER BY STORE
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
          zlib.gunzip(buf, (err, decompressed) => {
            if (err) return reject(new Error('gunzip error: ' + err.message));
            try { resolve({ status: res.statusCode, body: decode(decompressed) }); }
            catch(e) { reject(e); }
          });
        } else if (enc.includes('deflate')) {
          zlib.inflate(buf, (err, decompressed) => {
            if (err) return reject(new Error('inflate error: ' + err.message));
            try { resolve({ status: res.statusCode, body: decode(decompressed) }); }
            catch(e) { reject(e); }
          });
        } else {
          try { resolve({ status: res.statusCode, body: decode(buf) }); }
          catch(e) { reject(e); }
        }
      });
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

// ── Poll until statement is done, then collect all result pages
async function runQuery(sql) {
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

  const allRows = [...(result.data || [])];
  const partitions = (result.resultSetMetaData && result.resultSetMetaData.partitionInfo) || [];
  for (let p = 1; p < partitions.length; p++) {
    const page = await sfRequest(`/api/v2/statements/${result.statementHandle}?partition=${p}`, 'GET', null);
    (page.body.data || []).forEach(r => allRows.push(r));
  }

  return { rows: allRows, meta: result.resultSetMetaData };
}

// ── Map rows → [{plk, digital_pct}]
// SELECT columns: [0]=STORE [1]=YTD_DIGITAL_SALES [2]=YTD_SYSTEMWIDE_SALES [3]=YTD_PERCENT_DIGITAL_SALES
function mapRows(rows) {
  return rows
    .map(row => {
      const plk = parseInt(row[0]);
      if (isNaN(plk) || plk <= 0) return null;
      const pct = parseFloat(row[3]);
      return { plk, digital_pct: isNaN(pct) ? null : pct }; // already a ratio (0–1)
    })
    .filter(Boolean);
}

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(204).end(); return; }

  if (!SF_PAT) {
    return res.status(503).json({ error: 'SF_PAT environment variable not set on server' });
  }

  try {
    const { rows } = await runQuery(DIGITAL_SQL);
    const stores = mapRows(rows);
    res.status(200).json({ stores, count: stores.length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
