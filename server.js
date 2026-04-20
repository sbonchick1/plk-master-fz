const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const url   = require('url');

const PORT = 3000;
const SS_TOKEN   = 'Qcv9OFzbweKqbamkePSG2WpggSbTkEmASUgh1';
const SS_SHEET_ID = '3928541238349700';

const server = http.createServer((req, res) => {
  const parsed = url.parse(req.url, true);

  // ── CORS headers for all responses
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

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
  console.log(`  Smartsheet proxy: http://localhost:${PORT}/api/renovations\n`);
});
