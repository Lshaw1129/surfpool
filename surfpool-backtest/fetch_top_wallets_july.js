/* fetch_top_wallets_july.js
   - Fetches Jupiter router swaps for 2025-07-01..31 using Helius JSON-RPC
   - Rotates across provided RPC URLs, rate-limit aware with retries/backoff
   - Checkpoints signatures + per-tx decode so you never lose progress
   - Aggregates top wallets by swap count and approx USDC PnL per wallet
   Output:
     data/july_sigs_h1.jsonl, data/july_sigs_h2.jsonl      (raw signatures)
     data/july_txns_h1.jsonl, data/july_txns_h2.jsonl      (raw transactions)
     data/wallet_swaps_progress.json                       (running map)
     data/top_wallets_july.csv                             (final table)
     data/top_wallets_july.json                            (final JSON)
*/

const fs = require("fs");
const https = require("https");

const JUP_ROUTER = "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB";
const USDC_MINT  = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

const cfg = JSON.parse(fs.readFileSync("config.july.json","utf8"));
const RPCS = cfg.rpc_urls;
if (!Array.isArray(RPCS) || RPCS.length === 0) {
  console.error("No RPC URLs configured in config.july.json.rpc_urls");
  process.exit(1);
}

const START_TS = Math.floor(new Date(cfg.start_iso).getTime()/1000);
const END_TS   = Math.floor(new Date(cfg.end_iso).getTime()/1000);
const MAX_MIN  = cfg.max_minutes || 170;
const PAGE_LIM = cfg.page_limit || 1000;
const BASE_DELAY = cfg.per_tx_delay_ms || 250;
const MAX_BACKOFF = cfg.max_backoff_ms || 8000;

const startWall = Date.now();

function ms() { return Date.now() - startWall; }
function timeoutReached() {
  return (ms() / 60000) >= MAX_MIN;
}

let rpcIdx = 0;
function nextRpc() {
  rpcIdx = (rpcIdx + 1) % RPCS.length;
}
function currentRpc() {
  return RPCS[rpcIdx];
}

function jrpc(url, method, params, attempt=0) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({jsonrpc:"2.0", id:1, method, params});
    const u = new URL(url);
    const req = https.request({
      protocol: u.protocol,
      hostname: u.hostname,
      port: u.port || 443,
      path: u.pathname + u.search,
      method: "POST",
      headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) }
    }, res => {
      let chunks = [];
      res.on("data", d => chunks.push(d));
      res.on("end", async () => {
        const s = Buffer.concat(chunks).toString("utf8");
        if (res.statusCode >= 500 || res.statusCode === 429) {
          const backoff = Math.min(MAX_BACKOFF, BASE_DELAY * Math.pow(2, attempt));
          // rotate RPC and backoff
          nextRpc();
          await new Promise(r => setTimeout(r, backoff));
          return resolve(jrpc(currentRpc(), method, params, attempt+1));
        }
        try {
          const parsed = JSON.parse(s);
          if (parsed.error) {
            // Retry on server-ish errors, otherwise bubble up
            if (parsed.error.code === -32005 /* Too many requests */ || parsed.error.code <= -32000) {
              const backoff = Math.min(MAX_BACKOFF, BASE_DELAY * Math.pow(2, attempt));
              nextRpc();
              await new Promise(r => setTimeout(r, backoff));
              return resolve(jrpc(currentRpc(), method, params, attempt+1));
            }
            return reject(new Error("RPC error: " + JSON.stringify(parsed.error)));
          }
          return resolve(parsed.result);
        } catch (e) {
          return reject(new Error("Bad JSON from RPC: " + e.message + " body=" + s.slice(0,200)));
        }
      });
    });
    req.on("error", async (e) => {
      const backoff = Math.min(MAX_BACKOFF, BASE_DELAY * Math.pow(2, attempt));
      nextRpc();
      await new Promise(r => setTimeout(r, backoff));
      resolve(jrpc(currentRpc(), method, params, attempt+1));
    });
    req.write(body); req.end();
  });
}

// Utility: read JSONL into array of parsed objects
function readJSONL(path) {
  if (!fs.existsSync(path)) return [];
  const lines = fs.readFileSync(path,"utf8").split(/\r?\n/).filter(Boolean);
  return lines.map(l => JSON.parse(l));
}
function appendJSONL(path, obj) {
  fs.appendFileSync(path, JSON.stringify(obj) + "\n");
}

// Find signatures for a half-range by walking backwards with `before`
// Stop when blockTime < halfStart; Keep sigs with halfStart<=t<=halfEnd
async function collectSignatures(half, outPath) {
  const {start, end} = half;
  console.log(`[Sigs ${half.name}] target ${new Date(start*1000).toISOString()}..${new Date(end*1000).toISOString()}`);
  const existing = readJSONL(outPath);
  let seen = new Set(existing.map(o => o.signature));
  let outCount = existing.length;
  let before = null;
  if (existing.length > 0) {
    // Resume from last page by using the last signature as `before`
    before = existing[existing.length-1].signature;
    console.log(`[Sigs ${half.name}] resuming after ${before}, already have ${existing.length}`);
  }
  while (true) {
    if (timeoutReached()) { console.warn(`[Sigs ${half.name}] TIME LIMIT reached; checkpointed ${outCount}`); break; }
    const params = before ? [JUP_ROUTER, {limit: PAGE_LIM, before}] : [JUP_ROUTER, {limit: PAGE_LIM}];
    const page = await jrpc(currentRpc(), "getSignaturesForAddress", params);
    if (!Array.isArray(page) || page.length === 0) break;
    let keepPaging = true;
    for (const x of page) {
      const t = x.blockTime|0;
      if (!t) continue;
      if (t < start) { keepPaging = false; break; }
      if (t <= end) {
        if (!seen.has(x.signature)) {
          appendJSONL(outPath, {signature: x.signature, blockTime: t});
          seen.add(x.signature);
          outCount++;
        }
      }
    }
    if (!keepPaging) break;
    before = page[page.length-1]?.signature;
    if (!before) break;
    await new Promise(r=>setTimeout(r, 150)); // gentle throttle
  }
  console.log(`[Sigs ${half.name}] total kept = ${outCount}`);
  return outCount;
}

// Fetch full transactions (JSON) for signatures; checkpoint to JSONL
async function collectTransactions(sigPath, outPath) {
  const sigs = readJSONL(sigPath);
  const done = new Set(readJSONL(outPath).map(o => o.signature));
  let count = 0;
  for (const s of sigs) {
    if (timeoutReached()) { console.warn(`[Tx ${sigPath}] TIME LIMIT; saved ${count}`); break; }
    if (done.has(s.signature)) continue;
    const tx = await jrpc(currentRpc(), "getTransaction", [s.signature, {encoding:"json","maxSupportedTransactionVersion":0}])
      .catch(e => { console.warn("getTransaction fail", s.signature, e.message); return null; });
    if (tx) {
      appendJSONL(outPath, {signature: s.signature, blockTime: s.blockTime, tx});
      count++;
    }
    await new Promise(r=>setTimeout(r, BASE_DELAY));
  }
  console.log(`[Tx ${sigPath}] new fetched = ${count}`);
}

// Extract fee payer
function feePayer(tx) {
  try {
    const keys = tx?.transaction?.message?.accountKeys || [];
    const header = tx?.transaction?.message?.header;
    // First required signer is usually fee payer
    if (header?.numRequiredSignatures > 0 && keys.length > 0) return keys[0];
  } catch (_) {}
  return null;
}

// Approx USDC PnL for a wallet on one tx: sum of (post - pre) USDC in token balances owned by that wallet
function usdcDeltaLamportsForWallet(tx, wallet) {
  try {
    const pre = tx?.meta?.preTokenBalances || [];
    const post = tx?.meta?.postTokenBalances || [];
    // Build index by (accountIndex -> {uiTokenAmount, owner, mint})
    const byIndex = {};
    function ingest(arr, tag) {
      for (const b of arr) {
        const idx = b.accountIndex;
        if (!byIndex[idx]) byIndex[idx] = {pre:null, post:null, owner:b.owner, mint:b.mint};
        byIndex[idx][tag] = b.uiTokenAmount;
      }
    }
    ingest(pre, "pre");
    ingest(post,"post");
    let delta = 0;
    for (const [idx, rec] of Object.entries(byIndex)) {
      if (rec.mint !== USDC_MINT) continue;
      if (!rec.owner || rec.owner !== wallet) continue; // only count USDC balances owned by the wallet
      const preAmt = parseFloat(rec.pre?.amount || "0");
      const postAmt= parseFloat(rec.post?.amount || "0");
      // uiTokenAmount.amount is integer string in base units
      delta += (postAmt - preAmt);
    }
    return delta; // in base units (6 decimals)
  } catch (_) { return 0; }
}

function fmtUSDCLamports(lamports) {
  return (lamports / 1e6);
}

function ensureCSVHeader(path) {
  if (!fs.existsSync(path)) {
    fs.writeFileSync(path, "wallet,swaps,est_pnl_usd,first_ts,last_ts\n");
  }
}

// MAIN
(async () => {
  const half1 = { name:"H1", start: Math.floor(new Date("2025-07-01T00:00:00Z").getTime()/1000),
                  end:   Math.floor(new Date("2025-07-15T23:59:59Z").getTime()/1000) };
  const half2 = { name:"H2", start: Math.floor(new Date("2025-07-16T00:00:00Z").getTime()/1000),
                  end:   Math.floor(new Date("2025-07-31T23:59:59Z").getTime()/1000) };

  // 1) signatures (checkpoint JSONL)
  await collectSignatures(half1, "data/july_sigs_h1.jsonl");
  if (!timeoutReached()) await collectSignatures(half2, "data/july_sigs_h2.jsonl");

  // 2) transactions (checkpoint JSONL)
  if (!timeoutReached()) await collectTransactions("data/july_sigs_h1.jsonl","data/july_txns_h1.jsonl");
  if (!timeoutReached()) await collectTransactions("data/july_sigs_h2.jsonl","data/july_txns_h2.jsonl");

  // 3) aggregate per wallet
  const txns = [...readJSONL("data/july_txns_h1.jsonl"), ...readJSONL("data/july_txns_h2.jsonl")];

  const walletMap = JSON.parse(fs.existsSync("data/wallet_swaps_progress.json") ? fs.readFileSync("data/wallet_swaps_progress.json","utf8") : "{}");
  for (const rec of txns) {
    const w = feePayer(rec.tx);
    if (!w) continue;
    if (!walletMap[w]) walletMap[w] = {swaps:0, pnl_usdc_lamports:0, first_ts:rec.blockTime, last_ts:rec.blockTime};
    walletMap[w].swaps += 1;
    // Only add PnL from USDC balances owned by the fee payer (approx)
    walletMap[w].pnl_usdc_lamports += usdcDeltaLamportsForWallet(rec.tx, w);
    walletMap[w].first_ts = Math.min(walletMap[w].first_ts, rec.blockTime);
    walletMap[w].last_ts  = Math.max(walletMap[w].last_ts,  rec.blockTime);
  }
  fs.writeFileSync("data/wallet_swaps_progress.json", JSON.stringify(walletMap,null,2));

  // 4) build top-15 list by swap count, include est USD PnL
  let rows = Object.entries(walletMap).map(([wallet,stat]) => ({
    wallet,
    swaps: stat.swaps,
    est_pnl_usd: +fmtUSDCLamports(stat.pnl_usdc_lamports).toFixed(2),
    first_ts: stat.first_ts || null,
    last_ts:  stat.last_ts  || null
  }));
  rows.sort((a,b) => b.swaps - a.swaps || (b.est_pnl_usd - a.est_pnl_usd));
  const top15 = rows.slice(0,15);

  // Write CSV + JSON
  ensureCSVHeader("data/top_wallets_july.csv");
  for (const r of top15) {
    fs.appendFileSync("data/top_wallets_july.csv", `${r.wallet},${r.swaps},${r.est_pnl_usd},${r.first_ts},${r.last_ts}\n`);
  }
  fs.writeFileSync("data/top_wallets_july.json", JSON.stringify({generated_at: Date.now(), top: top15}, null, 2));

  console.log(`\n✅ Done. Files written:
  - data/july_sigs_h1.jsonl / data/july_sigs_h2.jsonl
  - data/july_txns_h1.jsonl / data/july_txns_h2.jsonl
  - data/wallet_swaps_progress.json
  - data/top_wallets_july.csv
  - data/top_wallets_july.json
  `);
})().catch(e => { console.error("FATAL", e); process.exit(1); });
