const https = require('https');

const SF_ACCOUNT   = process.env.SF_ACCOUNT   || 'RBI-RBI_USE1';
const SF_PAT       = process.env.SF_PAT;
const SF_WAREHOUSE = process.env.SF_WAREHOUSE  || 'ANALYSIS_PLK';
const SF_DATABASE  = process.env.SF_DATABASE   || 'BRAND_PLK';
const SF_SCHEMA    = process.env.SF_SCHEMA     || 'TLOG';
const SF_ROLE      = process.env.SF_ROLE       || 'ANALYST_PLK';

const SERVICE_MODE_SQL = `
with stores as
(SELECT
  store_id,
  case when division_name is null and ownership='COMPANY' then 'Company'
       else reporting_unit_name
  end as fz_name,
  designated_market_Area_name as DMA,
  case when division_name is null and ownership='COMPANY' then 'COMPANY'
       when division_name is null and reporting_unit_name='Carrols' then 'COMPANY'
       else division_name
  end as division
FROM brand_plk.STORES.STORES
where country_code='US'),
base as
(select
  DATE_TRUNC('week', current_year_business_day) AS week_start_date,
  sm.store_id, s.fz_name, s.division, s.dma,
  case service_mode_name
    when 'CATERING'                       then 'CATERING'
    when 'CURB_SIDE_PICK_UP'              then 'TAKE OUT'
    when 'KIOSK_TAKE_OUT'                 then 'TAKE OUT'
    when 'MOBILE_ORDER_TAKE_OUT'          then 'TAKE OUT'
    when 'PICK_UP'                        then 'TAKE OUT'
    when 'TAKE_OUT'                       then 'TAKE OUT'
    when 'DRIVE_THROUGH'                  then 'DRIVE THRU'
    when 'MOBILE_ORDER_DRIVE_THRU'        then 'DRIVE THRU'
    when 'EAT_IN'                         then 'EAT IN'
    when 'KIOSK_EAT_IN'                   then 'EAT IN'
    when 'KIOSK'                          then 'EAT IN'
    when 'MOBILE_ORDER_EAT_IN'            then 'EAT IN'
    when 'THIRD_PARTY_DELIVERY_DOORDASH'  then 'DELIVERY'
    when 'THIRD_PARTY_DELIVERY_GRUBHUB'   then 'DELIVERY'
    when 'THIRD_PARTY_DELIVERY_OTHERS'    then 'DELIVERY'
    when 'THIRD_PARTY_DELIVERY_UBEREATS'  then 'DELIVERY'
    when 'WHITE LABEL DELIVERY'           then 'DELIVERY'
    when 'WHITE_LABEL_DELIVERY'           then 'DELIVERY'
    else 'OTHER'
  end as service_mode_category,
  sum(current_year_sales_amount) as cy_sales
from brand_plk.pmix.DAILY_SERVICE_MODE_COMPARABLE_SALES_AND_TRAFFIC sm
left join stores s on sm.store_id = s.store_id
where sm.country_code = 'US'
and current_year_business_day between date '2025-12-29' and dateadd('day',-1,current_date)
group by all),
category_totals as
(select store_id, fz_name, division, dma, service_mode_category,
  sum(cy_sales) as cy_category_sales from base group by all),
store_totals as
(select store_id, sum(cy_category_sales) as cy_store_total_sales
  from category_totals group by store_id)
select
  ct.store_id, ct.fz_name, ct.division, ct.dma, ct.service_mode_category,
  ct.cy_category_sales, st.cy_store_total_sales,
  round(ct.cy_category_sales / nullif(st.cy_store_total_sales, 0) * 100, 2) as cy_sales_mix_pct
from category_totals ct
left join store_totals st on ct.store_id = st.store_id
order by ct.store_id, ct.service_mode_category
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

  return { rows: allRows, meta: result.resultSetMetaData };
}

// ── Pivot rows → [{plk, dt_pct, eatin_pct, takeout_pct, delivery_pct}]
// Final SELECT columns: [0]=store_id [1]=fz_name [2]=division [3]=dma
//   [4]=service_mode_category [5]=cy_category_sales [6]=cy_store_total_sales [7]=cy_sales_mix_pct (0-100)
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
    const cat = String(row[4] || '').toUpperCase().trim();
    const key = CAT_MAP[cat];
    if (!key) return;
    if (!stores[plk]) stores[plk] = { plk };
    const pct = parseFloat(row[7]);
    stores[plk][key] = isNaN(pct) ? null : pct / 100;
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
    const { rows } = await runQuery(SERVICE_MODE_SQL);
    const stores = pivotRows(rows);
    res.status(200).json({ stores, count: stores.length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
};
