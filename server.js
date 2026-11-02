#!/usr/bin/env node
/**
 * Zoho Catalyst AppSail · VLESS + XHTTP packet-up launcher
 *
 * Reads:
 *   X_ZOHO_CATALYST_LISTEN_PORT  (default 9000) — platform-injected listen port
 *   UUID                         — VLESS client id (placeholder CHANGE_ME)
 *   WS_PATH or XPATH             — XHTTP path, e.g. /CHANGE_ME
 *
 * Writes a runtime Xray config, ensures a linux-amd64 `xray` binary is present
 * (prefer bundled ./xray; else download within the AppSail 10s listen budget),
 * then execs xray so it listens on the platform port.
 *
 * Client MUST use ALPN http/1.1 (not h2). Zoho ZGS gateway is not a transparent
 * proxy: WS upgrades fail with 400; XHTTP with negotiated h2 fails frame-too-large.
 */
'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');
const { spawn } = require('child_process');
const { pipeline } = require('stream');
const { promisify } = require('util');
const { createWriteStream } = require('fs');
const { execFileSync } = require('child_process');
const net = require('net');
const zlib = require('zlib');

const pipe = promisify(pipeline);

const PORT = Number(process.env.X_ZOHO_CATALYST_LISTEN_PORT || process.env.PORT || 9000);
const UUID = process.env.UUID || 'CHANGE_ME';
const RAW_PATH = process.env.WS_PATH || process.env.XPATH || '/CHANGE_ME';
const XPATH = RAW_PATH.startsWith('/') ? RAW_PATH : `/${RAW_PATH}`;
const XPORT = 19001; // xray internal listen port (front server takes the platform PORT)
const SUB_PATHS = new Set(['/clash.yaml', '/sub', '/all.yaml']);
const PUBLIC_HOST = 'yuki-xhttp-node-10137721694.development.catalystappsail.com';
const NODES = [
  { name: 'Zoho-1', host: 'yuki-xhttp-node-10137721694.development.catalystappsail.com', uuid: '1b7db5a7-84b5-46d3-ac69-5ef4a63202c6', path: '/5e39292d' },
  { name: 'Zoho-2', host: 'yuki-xhttp-node-2-10137721694.development.catalystappsail.com', uuid: '77185a99-a37f-4bef-bc24-ba0778e397a1', path: '/a9cb5c71' },
  { name: 'Zoho-3', host: 'yuki-xhttp-node-3-10137721694.development.catalystappsail.com', uuid: '91f3c069-a335-42b5-8615-4be170664750', path: '/b1841c62' },
  { name: 'Zoho-4', host: 'yuki-xhttp-node-4-10137721694.development.catalystappsail.com', uuid: '2c2c955e-27f9-44b5-98f7-caa1fae321b9', path: '/22bc6f3d' },
  { name: 'Zoho-5', host: 'yuki-xhttp-node-5-10137721694.development.catalystappsail.com', uuid: '0eca2bac-c5dd-4bf8-b8bd-cb3625f5e9fe', path: '/132f7c05' },
  { name: 'Zoho-6', host: 'yuki-xhttp-node-6-10137724101.development.catalystappsail.com', uuid: '06ef5872-5fc7-4d0e-a882-7449148cf582', path: '/e9ffaf95' },
  { name: 'Zoho-7', host: 'yuki-xhttp-node-7-10137724101.development.catalystappsail.com', uuid: 'af73906c-e79a-413f-a89f-b55516c0d462', path: '/d20a943e' },
  { name: 'Zoho-8', host: 'yuki-xhttp-node-8-10137724101.development.catalystappsail.com', uuid: '864d68d7-f374-4435-a44d-0e5eaf9d84c3', path: '/eb54a200' },
  { name: 'Zoho-9', host: 'yuki-xhttp-node-9-10137724101.development.catalystappsail.com', uuid: '5d1e2baf-2115-4c6a-a4b8-bdab6aae4e74', path: '/c75f9709' },
  { name: 'Zoho-10', host: 'yuki-xhttp-node-10-10137724101.development.catalystappsail.com', uuid: '4f984a70-9185-4b6a-9f1e-fa5fbf2d3ce8', path: '/efb9958b' }
];

const HERE = __dirname;
const XRAY_BIN = path.join(HERE, 'xray');
const CONFIG_PATH = path.join(HERE, 'xray-config.json');

// Official Xray-core release (pin a known-good amd64 zip). Override with XRAY_ZIP_URL.
const XRAY_ZIP_URL =
  process.env.XRAY_ZIP_URL ||
  'https://github.com/XTLS/Xray-core/releases/download/v25.10.15/Xray-linux-64.zip';

function die(msg) {
  console.error(`[zoho-xhttp] FATAL: ${msg}`);
  process.exit(1);
}

function buildConfig() {
  return {
    log: { loglevel: process.env.XRAY_LOGLEVEL || 'warning' },
    inbounds: [
      {
        listen: '127.0.0.1',
        port: XPORT,
        protocol: 'vless',
        settings: {
          clients: [{ id: UUID, email: 'zoho-appsail' }],
          decryption: 'none',
        },
        streamSettings: {
          network: 'xhttp',
          xhttpSettings: {
            path: XPATH,
            mode: 'packet-up',
          },
        },
        sniffing: {
          enabled: true,
          destOverride: ['http', 'tls', 'quic'],
        },
      },
    ],
    outbounds: [
      { protocol: 'freedom', tag: 'direct' },
      { protocol: 'blackhole', tag: 'block' },
    ],
  };
}

function fetchToFile(url, dest) {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, { timeout: 8000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        return fetchToFile(res.headers.location, dest).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        reject(new Error(`download HTTP ${res.statusCode} for ${url}`));
        res.resume();
        return;
      }
      const out = createWriteStream(dest);
      pipe(res, out).then(resolve, reject);
    });
    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy(new Error('download timeout'));
    });
  });
}

async function ensureXray() {
  if (fs.existsSync(XRAY_BIN)) {
    try {
      fs.chmodSync(XRAY_BIN, 0o755);
    } catch (_) {}
    return;
  }

  console.log('[zoho-xhttp] no bundled ./xray — downloading linux-amd64 release…');
  const zipPath = path.join(HERE, 'xray-download.zip');
  const started = Date.now();
  await fetchToFile(XRAY_ZIP_URL, zipPath);
  // Prefer system unzip if present; else use Node zlib only for stored entries via `unzip` fallback.
  try {
    execFileSync('unzip', ['-o', '-j', zipPath, 'xray', '-d', HERE], { stdio: 'ignore' });
  } catch (e) {
    // Busybox / no unzip: try python3 zipfile
    try {
      execFileSync(
        'python3',
        [
          '-c',
          'import zipfile,sys; z=zipfile.ZipFile(sys.argv[1]); z.extract("xray", sys.argv[2])',
          zipPath,
          HERE,
        ],
        { stdio: 'ignore' }
      );
    } catch (e2) {
      die(`failed to extract xray from zip (${e2.message}). Bundle ./xray in the deploy package.`);
    }
  }
  try {
    fs.unlinkSync(zipPath);
  } catch (_) {}
  if (!fs.existsSync(XRAY_BIN)) {
    die('xray binary missing after extract');
  }
  fs.chmodSync(XRAY_BIN, 0o755);
  console.log(`[zoho-xhttp] xray ready in ${Date.now() - started}ms`);
}


function buildClashYaml(host) {
  const h = host || process.env.PUBLIC_HOST || PUBLIC_HOST;
  return `# Clash Meta config - Zoho US XHTTP node
port: 7890
socks-port: 7891
allow-lan: false
mode: rule
log-level: info

proxies:
  - name: "Zoho-US"
    type: vless
    server: ${h}
    port: 443
    uuid: ${UUID}
    network: xhttp
    tls: true
    udp: true
    sni: ${h}
    alpn:
      - http/1.1
    client-fingerprint: chrome
    xhttp-opts:
      path: "${XPATH}"
      mode: "packet-up"
      host: ${h}

proxy-groups:
  - name: "PROXY"
    type: select
    proxies:
      - "Zoho-US"

rules:
  - MATCH,PROXY
`;
}

function proxyBlock(n) {
  return `  - name: "${n.name}"
    type: vless
    server: ${n.host}
    port: 443
    uuid: ${n.uuid}
    network: xhttp
    tls: true
    udp: true
    sni: ${n.host}
    alpn:
      - http/1.1
    client-fingerprint: chrome
    xhttp-opts:
      path: "${n.path}"
      mode: "packet-up"
      host: ${n.host}
`;
}

function buildCombinedYaml() {
  const proxies = NODES.map(proxyBlock).join('');
  const names = NODES.map((n) => `      - "${n.name}"`).join('\n');
  return `# Clash Meta config - all Zoho XHTTP nodes
port: 7890
socks-port: 7891
allow-lan: false
mode: rule
log-level: info

proxies:
${proxies}proxy-groups:
  - name: "PROXY"
    type: select
    proxies:
${names}
  - name: "Auto"
    type: url-test
    url: http://www.gstatic.com/generate_204
    interval: 300
    proxies:
${names}
rules:
  - MATCH,PROXY
`;
}

function hostFromHead(head) {
  const lines = head.split('\r\n');
  for (let k = 1; k < lines.length; k++) {
    const m = /^host:\s*([^:\s]+)/i.exec(lines[k]);
    if (m) return m[1];
  }
  return null;
}

function pipeToXray(sock, firstChunk) {
  const up = net.connect(XPORT, '127.0.0.1', () => {
    if (firstChunk && firstChunk.length) up.write(firstChunk);
    sock.pipe(up);
    up.pipe(sock);
  });
  const kill = () => { try { sock.destroy(); } catch (_) {} try { up.destroy(); } catch (_) {} };
  sock.on('error', kill);
  up.on('error', kill);
}

// Front TCP server on the platform port: serves /clash.yaml (and /sub) directly,
// everything else is piped raw to xray on 127.0.0.1:XPORT.
function startFront() {
  const srv = net.createServer((sock) => {
    let buf = Buffer.alloc(0);
    let decided = false;
    const onData = (chunk) => {
      if (decided) return;
      if (buf.length === 0) {
        // Fast path: not an HTTP GET/HEAD -> raw proxy to xray.
        const start = chunk.toString('latin1', 0, 16);
        if (!/^(GET|HEAD)\s/.test(start)) {
          decided = true;
          sock.removeListener('data', onData);
          pipeToXray(sock, chunk);
          return;
        }
      }
      buf = Buffer.concat([buf, chunk]);
      if (buf.length > 8192) {
        decided = true;
        sock.removeListener('data', onData);
        pipeToXray(sock, buf);
        return;
      }
      const head = buf.toString('latin1');
      if (head.indexOf('\r\n\r\n') === -1) return; // wait for complete headers
      decided = true;
      sock.removeListener('data', onData);
      const rl = /^(\S+)\s+(\S+)\s+HTTP\//.exec(head);
      const method = rl ? rl[1] : '';
      const path = rl ? rl[2].split('?')[0] : '';
      if ((method === 'GET' || method === 'HEAD') && SUB_PATHS.has(path)) {
        const yamlText = path === '/all.yaml'
          ? buildCombinedYaml()
          : buildClashYaml(hostFromHead(head) || process.env.PUBLIC_HOST || PUBLIC_HOST);
        const yaml = Buffer.from(yamlText, 'utf8');
        const header = Buffer.from(
          'HTTP/1.1 200 OK\r\nContent-Type: text/yaml; charset=utf-8\r\n' +
            `Content-Length: ${yaml.length}\r\nConnection: close\r\n\r\n`,
          'latin1'
        );
        sock.write(Buffer.concat([header, yaml]));
        sock.end();
        return;
      }
      pipeToXray(sock, buf);
    };
    sock.on('data', onData);
    sock.on('error', () => {});
  });
  srv.on('error', (e) => console.error('[zoho-xhttp] front server error', e.message));
  srv.listen(PORT, '0.0.0.0', () =>
    console.log(`[zoho-xhttp] front on 0.0.0.0:${PORT}, xray on 127.0.0.1:${XPORT}`)
  );
}

async function main() {
  if (!UUID || UUID === 'CHANGE_ME') {
    console.warn('[zoho-xhttp] WARNING: UUID is still CHANGE_ME — set env UUID before real use');
  }
  if (XPATH === '/CHANGE_ME') {
    console.warn('[zoho-xhttp] WARNING: path is still /CHANGE_ME — set WS_PATH or XPATH');
  }

  const cfg = buildConfig();
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2));
  console.log(`[zoho-xhttp] config written → ${CONFIG_PATH} (port=${PORT} path=${XPATH})`);

  await ensureXray();

  const child = spawn(XRAY_BIN, ['run', '-c', CONFIG_PATH], {
    stdio: 'inherit',
    env: process.env,
  });

  startFront();

  child.on('exit', (code, signal) => {
    console.error(`[zoho-xhttp] xray exited code=${code} signal=${signal}`);
    process.exit(code || 1);
  });

  process.on('SIGTERM', () => child.kill('SIGTERM'));
  process.on('SIGINT', () => child.kill('SIGINT'));
}

main().catch((err) => die(err.stack || err.message));
