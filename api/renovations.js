const https = require('https');

const SS_TOKEN    = process.env.SS_TOKEN;
const SS_SHEET_ID = process.env.SS_SHEET_ID || '3928541238349700';

module.exports = (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(204).end(); return; }

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
      res.setHeader('Content-Type', 'application/json');
      res.status(ssRes.statusCode).send(data);
    });
  });
  proxy.on('error', e => {
    res.status(500).json({ error: e.message });
  });
  proxy.end();
};
