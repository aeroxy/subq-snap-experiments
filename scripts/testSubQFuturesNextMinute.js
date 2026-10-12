/**
 * Can SubQ call the next minute? 100 random minutes from the last 10 complete UTC days, each on a coin drawn from that
 * day's 20 most-traded USDT perpetuals (the testSubQFuturesBackfill.js ranking): will the price be higher 60 seconds
 * after as_of? SubQ sees ~90k tokens of microstructure: 6 hours of 1m and 24 hours of 5m candles, the last 1,000
 * trades, order-book depth snapshots of the last 15 minutes, open interest and long/short ratios, funding, and BTC.
 * Following it trades $100 at 1x with market orders, priced against simple rules and perfect foresight.
 *
 *   node scripts/testSubQFuturesNextMinute.js --build                      data/futuresNextMinute/dataset-<Date.now()>.json.gz
 *   node --env-file=.env scripts/testSubQFuturesNextMinute.js [--limit N]  ask SubQ about the newest dataset
 *
 * Trades and depth come from the data.binance.vision daily dumps (the REST API serves trades for 2 days only).
 * Each run saves data/futuresNextMinute/results-<Date.now()>.json.
 */
const fs = require("fs/promises");
const os = require("os");
const path = require("path");
const zlib = require("zlib");
const { SUBQ_MODEL, scoreItems, summarize, fixed, pct } = require("./testSubQPrediction");

const BACKFILL_DIR = path.join(__dirname, "..", "data", "futuresBackfill");
const OUT_DIR = path.join(__dirname, "..", "data", "futuresNextMinute");
const DUMP_CACHE = path.join(os.tmpdir(), "binance-vision-cache");
const DATASET_FILE = /^dataset-\d+\.json\.gz$/;

const SEED = 20261012;
const POINTS = 100;
const DAYS = 10;
const TOP_COINS = 20;

const FAPI_URL = "https://fapi.binance.com";
const DUMP_URL = "https://data.binance.vision/data/futures/um/daily";
const CONCURRENCY = 4;
const MAX_RETRIES = 3;
const MINUTE = 60000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;
const HORIZON = MINUTE;

const CANDLES = [["5m", 24 * 12], ["1m", 6 * 60]];
const BTC_CANDLES = [["1m", 60]];
const POSITIONING_BARS = 24 * 12; // 5m snapshots
const TRADES = 1000;
const DEPTH_WINDOW = 15 * MINUTE;
const FUNDING_DAYS = 3;
const INTERVAL_MS = { "5m": 5 * MINUTE, "1m": MINUTE };
// The metrics dump stamps each snapshot 5 minutes before the REST endpoints do (its 15:55 row is their 16:00)
const METRICS_LAG = 5 * MINUTE;

const NOTIONAL = 100; // USDT per position, at 1x
const TAKER_FEE = 0.0005; // VIP 0 USDⓈ-M taker rate, paid on entry and on exit

const CANDLE_COLUMNS = ["open", "high", "low", "close", "quote_volume_usdt", "taker_buy_share", "trades"];
const POSITIONING_COLUMNS = ["open_interest_usdt", "top_trader_long_short_accounts", "top_trader_long_short_positions", "long_short_accounts"];
const DEPTH_LEVELS = ["-5.00", "-4.00", "-3.00", "-2.00", "-1.00", "1.00", "2.00", "3.00", "4.00", "5.00"];
const DEPTH_COLUMNS = ["seconds_before_as_of", ...DEPTH_LEVELS.map((level) => `${Number(level) > 0 ? "+" : ""}${Number(level)}%`)];

const QUESTION = {
  type: "noul",
  instructions:
    "state holds Binance USDⓈ-M perpetual futures data for state.symbol, all observed up to state.as_of. Will its price at state.target_time, one minute later, be higher than state.last_price?",
  criteria: {
    true: "The price of state.symbol at state.target_time is higher than state.last_price, its price at state.as_of",
    false: "The price of state.symbol at state.target_time is at or below state.last_price, its price at state.as_of",
  },
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const sum = (values) => values.reduce((total, value) => total + value, 0);
const minute = (ms) => `${new Date(ms).toISOString().slice(0, 16)}Z`;
const dayOf = (ms) => new Date(ms).toISOString().slice(0, 10);
const exists = (file) => fs.access(file).then(() => true, () => false);
const utcTime = (text) => Date.parse(`${text.replace(" ", "T")}Z`);
const daysBetween = (from, to) => Array.from({ length: Math.floor(to / DAY) - Math.floor(from / DAY) + 1 }, (_, i) => dayOf(from + i * DAY));

// mulberry32: the same seed draws the same points
function random(seed) {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ---- Binance ----------------------------------------------------------------

// Request weight is capped at 2400 a minute (reported in x-mbx-used-weight-1m); staying under it avoids 429s, which
// Binance escalates to IP bans (418)
const WEIGHT_PAUSE_AT = 2000;
let pausedUntil = 0;

async function fapi(endpoint, params) {
  const url = `${FAPI_URL}${endpoint}?${new URLSearchParams(params)}`;
  for (let attempt = 0; ; attempt++) {
    while (Date.now() < pausedUntil) await sleep(pausedUntil - Date.now());
    const res = await fetch(url, { signal: AbortSignal.timeout(30000) }).catch((error) => ({ error }));
    if (Number(res.headers?.get("x-mbx-used-weight-1m")) >= WEIGHT_PAUSE_AT) {
      pausedUntil = Math.max(pausedUntil, Math.ceil(Date.now() / 60000) * 60000 + 1000);
    }
    if (res.ok) return res.json();
    const retryable = res.error || res.status === 429 || res.status === 418 || res.status >= 500;
    if (!retryable || attempt >= MAX_RETRIES) {
      throw new Error(`Binance ${endpoint} ${params.symbol ?? ""}: ${res.error?.message ?? `HTTP ${res.status} ${await res.text()}`}`);
    }
    pausedUntil = Math.max(pausedUntil, Date.now() + 1000 * (Number(res.headers?.get("retry-after")) || 2 ** attempt));
  }
}

// Every row from `from` to `to`, `step` apart, in slices of `limit` steps so each fits one page whichever end Binance
// fills it from. Slices start on a whole step: Binance rounds startTime down to one.
async function fetchAll(endpoint, params, from, to, limit, step) {
  const rows = [];
  for (let start = Math.floor(from / step) * step; start <= to; start += limit * step) {
    const end = Math.min(start + limit * step - 1, to);
    rows.push(...(await fapi(endpoint, { ...params, startTime: start, endTime: end, limit: Math.floor((end - start) / step) + 1 })));
  }
  return rows;
}

async function mapLimit(list, limit, fn) {
  const results = [];
  let next = 0;
  await Promise.all(
    Array.from({ length: limit }, async () => {
      while (next < list.length) {
        const i = next++;
        results[i] = await fn(list[i]);
      }
    }),
  );
  return results;
}

// The one CSV inside a data.binance.vision zip, as rows of strings without the header
function readZipCsv(buffer) {
  const directory = buffer.readUInt32LE(buffer.lastIndexOf(Buffer.from("PK\x05\x06", "latin1")) + 16);
  const compressedSize = buffer.readUInt32LE(directory + 20);
  const header = buffer.readUInt32LE(directory + 42);
  const start = header + 30 + buffer.readUInt16LE(header + 26) + buffer.readUInt16LE(header + 28);
  const body = buffer.subarray(start, start + compressedSize);
  const text = (buffer.readUInt16LE(directory + 10) === 8 ? zlib.inflateRawSync(body) : body).toString("utf8");
  return text.trim().split("\n").slice(1).map((line) => line.split(","));
}

// One day of a daily dump (aggTrades, bookDepth, metrics), cached on disk; [] when Binance has none for that day
async function dump(kind, symbol, day) {
  const file = path.join(DUMP_CACHE, kind, symbol, `${day}.zip`);
  if (await exists(file)) return readZipCsv(await fs.readFile(file));
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(`${DUMP_URL}/${kind}/${symbol}/${symbol}-${kind}-${day}.zip`, { signal: AbortSignal.timeout(120000) }).catch((error) => ({ error }));
    if (res.status === 404) return [];
    if (res.ok) {
      const buffer = Buffer.from(await res.arrayBuffer());
      await fs.mkdir(path.dirname(file), { recursive: true });
      await fs.writeFile(file, buffer);
      return readZipCsv(buffer);
    }
    if (attempt >= MAX_RETRIES) throw new Error(`${kind} ${symbol} ${day}: ${res.error?.message ?? `HTTP ${res.status}`}`);
    await sleep(1000 * 2 ** attempt);
  }
}

// A row at a time: a day of BTC trades (~300k rows) is too many arguments to spread into push
async function dumpRows(kind, symbol, from, to) {
  const rows = [];
  for (const day of daysBetween(from, to)) for (const row of await dump(kind, symbol, day)) rows.push(row);
  return rows;
}

// ---- series -----------------------------------------------------------------

// `bars` consecutive values on an interval grid ending at `end`, null where the source has none, leading nulls dropped
function gridSeries(interval, bars, end, valueAt) {
  const step = INTERVAL_MS[interval];
  const rows = Array.from({ length: bars }, (_, i) => valueAt(end - (bars - 1 - i) * step) ?? null);
  const first = rows.findIndex((row) => row !== null);
  return { interval, last_bar_closes_utc: minute(end), rows: first === -1 ? [] : rows.slice(first) };
}

const candleRow = (k) => {
  const quoteVolume = Number(k[7]);
  return [Number(k[1]), Number(k[2]), Number(k[3]), Number(k[4]), Math.round(quoteVolume), quoteVolume ? +(Number(k[10]) / quoteVolume).toFixed(3) : null, k[8]];
};

// Closed candles only: the last one closes at or before asOf
async function candleSet(symbol, spec, asOf) {
  const set = {};
  for (const [interval, bars] of spec) {
    const step = INTERVAL_MS[interval];
    const end = Math.floor(asOf / step) * step;
    const klines = await fetchAll("/fapi/v1/klines", { symbol, interval }, end - bars * step, end - 1, 1500, step);
    const byOpen = new Map(klines.map((k) => [k[0], k]));
    set[interval] = { ...gridSeries(interval, bars, end, (close) => byOpen.has(close - step) && candleRow(byOpen.get(close - step))), columns: CANDLE_COLUMNS };
  }
  return set;
}

// ---- datasets ---------------------------------------------------------------

// The backfill window holding `time` starts at the latest 23:30 SGT (15:30 UTC) before it
async function topCoinsAt(time) {
  const asOf = Math.floor((time - 15.5 * HOUR) / DAY) * DAY + 15.5 * HOUR;
  const file = path.join(BACKFILL_DIR, `dataset-${dayOf(asOf + DAY)}.json`);
  return JSON.parse(await fs.readFile(file, "utf8")).items.slice(0, TOP_COINS).map((item) => item.symbol);
}

// POINTS distinct (minute, coin) draws from the DAYS complete UTC days before today
async function samplePoints(now) {
  const end = Math.floor(now / DAY) * DAY;
  const start = end - DAYS * DAY;
  const next = random(SEED);
  const points = new Map();
  while (points.size < POINTS) {
    const asOf = start + Math.floor(next() * DAYS * 1440) * MINUTE;
    const coins = await topCoinsAt(asOf);
    const symbol = coins[Math.floor(next() * coins.length)];
    points.set(`${asOf} ${symbol}`, { asOf, symbol });
  }
  return { start, end, points: [...points.values()].sort((a, b) => a.asOf - b.asOf) };
}

async function buildItem({ asOf, symbol }) {
  const target = asOf + HORIZON;
  const [candles, outcome, funding, trades, depth, metrics] = await Promise.all([
    candleSet(symbol, CANDLES, asOf),
    // The candle opening at asOf closes at target: its close is the exit price
    fapi("/fapi/v1/klines", { symbol, interval: "1m", startTime: asOf, endTime: asOf, limit: 1 }),
    fetchAll("/fapi/v1/fundingRate", { symbol }, asOf - FUNDING_DAYS * DAY, target, 1000, HOUR),
    dumpRows("aggTrades", symbol, asOf - DEPTH_WINDOW - MINUTE, asOf - 1),
    dumpRows("bookDepth", symbol, asOf - DEPTH_WINDOW, asOf - 1),
    dumpRows("metrics", symbol, asOf - POSITIONING_BARS * 5 * MINUTE - METRICS_LAG, asOf),
  ]);
  const last = candles["1m"].rows.at(-1);
  const exit = outcome[0];
  if (!last || !exit || exit[0] !== asOf) return null;
  const entryPrice = last[3];
  const exitPrice = Number(exit[4]);

  // agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker
  const recentTrades = trades
    .filter((t) => Number(t[5]) < asOf)
    .sort((a, b) => Number(a[5]) - Number(b[5]))
    .slice(-TRADES)
    .map((t) => [+((Number(t[5]) - asOf) / 1000).toFixed(3), Number(t[1]), Number(t[2]), t[6] === "true" ? "sell" : "buy"]);

  const snapshots = new Map();
  for (const [time, level, , notional] of depth) {
    const t = utcTime(time);
    if (t >= asOf || t < asOf - DEPTH_WINDOW) continue;
    if (!snapshots.has(t)) snapshots.set(t, {});
    snapshots.get(t)[level] = notional;
  }
  const depthRows = [...snapshots]
    .sort((a, b) => a[0] - b[0])
    .map(([t, levels]) => [(t - asOf) / 1000, ...DEPTH_LEVELS.map((level) => (levels[level] === undefined ? null : +(Number(levels[level]) / 1e6).toFixed(3)))]);

  // Snapshots are known from their REST time; the series stops one step before asOf
  const at = new Map(metrics.map((row) => [utcTime(row[0]) + METRICS_LAG, row]));
  const positioningEnd = Math.floor((asOf - 5 * MINUTE) / (5 * MINUTE)) * 5 * MINUTE;
  const positioning = gridSeries("5m", POSITIONING_BARS, positioningEnd, (time) => {
    const row = at.get(time);
    return row && [Math.round(Number(row[3])), +Number(row[4]).toFixed(4), +Number(row[5]).toFixed(4), +Number(row[6]).toFixed(4)];
  });

  return {
    symbol,
    asOf: new Date(asOf).toISOString(),
    targetTime: new Date(target).toISOString(),
    entryPrice,
    candles,
    trades: recentTrades,
    depth: depthRows,
    positioning: { ...positioning, columns: POSITIONING_COLUMNS },
    funding: funding.filter((f) => f.fundingTime <= asOf).map((f) => [minute(f.fundingTime), Number(f.fundingRate)]),
    ...(symbol !== "BTCUSDT" && { btc: await candleSet("BTCUSDT", BTC_CANDLES, asOf) }),
    // Outcome: never sent to SubQ
    exitPrice,
    up: exitPrice > entryPrice,
    fundingInWindow: funding
      .filter((f) => f.fundingTime > asOf && f.fundingTime <= target)
      .map((f) => [minute(f.fundingTime), Number(f.fundingRate), Number(f.markPrice)]),
  };
}

async function build() {
  const { start, end, points } = await samplePoints(Date.now());
  console.log(`sampled ${points.length} minutes from ${dayOf(start)} to ${dayOf(end - 1)}`);
  let done = 0;
  const built = await mapLimit(points, CONCURRENCY, async (point) => {
    const item = await buildItem(point);
    process.stdout.write(`\r${++done}/${points.length}`);
    return item;
  });
  process.stdout.write("\n");
  const skipped = points.filter((_, i) => !built[i]);
  if (skipped.length) console.log(`skipped for missing candles: ${skipped.map((p) => `${p.symbol} ${minute(p.asOf)}`).join(", ")}`);
  const items = built.filter(Boolean);
  const builtAt = Date.now();
  const sizes = items.map((item) => JSON.stringify(buildState(item)).length).sort((a, b) => a - b);
  const meta = {
    source: "binance-usdm-futures",
    builtAt: new Date(builtAt).toISOString(),
    seed: SEED,
    from: new Date(start).toISOString(),
    to: new Date(end).toISOString(),
    up: items.filter((item) => item.up).length,
    down: items.filter((item) => !item.up).length,
    unchanged: items.filter((item) => item.exitPrice === item.entryPrice).length,
    stateChars: { min: sizes[0], median: sizes[Math.floor(sizes.length / 2)], max: sizes.at(-1) },
  };
  const file = `dataset-${builtAt}.json.gz`;
  await fs.mkdir(OUT_DIR, { recursive: true });
  await fs.writeFile(path.join(OUT_DIR, file), zlib.gzipSync(JSON.stringify({ meta, items })));
  console.log(`wrote ${items.length} minutes (${meta.up} up / ${meta.down} down, ${meta.unchanged} unchanged) to data/futuresNextMinute/${file}`);
}

// ---- SubQ -------------------------------------------------------------------

// Only data up to as_of: the exit price and the funding settled after it stay out
const buildState = (item) => ({
  symbol: item.symbol,
  market: "Binance USDⓈ-M perpetual futures",
  as_of: item.asOf,
  target_time: item.targetTime,
  last_price: item.entryPrice,
  candles: item.candles,
  recent_trades: { columns: ["seconds_before_as_of", "price", "quantity", "taker_side"], rows: item.trades },
  order_book_depth_musdt: { columns: DEPTH_COLUMNS, rows: item.depth },
  positioning_5m: item.positioning,
  funding_rates: { columns: ["time_utc", "rate"], rows: item.funding },
  ...(item.btc && { btc_candles: item.btc }),
});

// ---- trades -----------------------------------------------------------------

// $NOTIONAL opened at as_of and closed a minute later with market orders, paying any funding settled in between
function trade(item, side) {
  const qty = NOTIONAL / item.entryPrice;
  const pricePnl = side * qty * (item.exitPrice - item.entryPrice);
  const fees = TAKER_FEE * qty * (item.entryPrice + item.exitPrice);
  const funding = -side * sum(item.fundingInWindow.map(([, rate, markPrice]) => qty * markPrice * rate));
  return { pricePnl, fees, funding, net: pricePnl - fees + funding };
}

// 1 = long, -1 = short
const followSubQ = (row) => (row.score >= 0.5 ? 1 : -1);

const STRATEGIES = [
  ["follow SubQ", followSubQ],
  ["always long", () => 1],
  ["always short", () => -1],
  // The last minute's direction continues
  ["1m momentum", (row) => (row.candles["1m"].rows.at(-1)[3] > row.candles["1m"].rows.at(-1)[0] ? 1 : -1)],
  // More bids than asks within 1% of the mid in the latest depth snapshot; short when there is none (Binance's depth
  // dump has a gap from 2026-10-07 02:49 to 2026-10-09 04:05 UTC)
  ["book imbalance", (row) => (row.depth.length && row.depth.at(-1)[5] > row.depth.at(-1)[6] ? 1 : -1)],
  ["perfect foresight", (row) => (row.up ? 1 : -1)],
];

function totals(name, sideOf, rows) {
  const trades = rows.map((row) => trade(row, sideOf(row)));
  const total = (key) => sum(trades.map((t) => t[key]));
  return {
    strategy: name,
    longs: rows.filter((row) => sideOf(row) === 1).length,
    shorts: rows.filter((row) => sideOf(row) === -1).length,
    accuracy: pct(rows.filter((row) => (sideOf(row) === 1) === row.up).length / rows.length),
    pricePnl: +total("pricePnl").toFixed(2),
    fees: +(-total("fees")).toFixed(2),
    funding: +total("funding").toFixed(2),
    net: +total("net").toFixed(2),
  };
}

// ---- run --------------------------------------------------------------------

async function run(limit) {
  // Date.now() stamps have the same length, so the newest sorts last
  const datasetFile = (await fs.readdir(OUT_DIR).catch(() => [])).filter((file) => DATASET_FILE.test(file)).sort().at(-1);
  if (!datasetFile) throw new Error("no data/futuresNextMinute/dataset-*.json.gz, run with --build first");
  const { meta, items: allItems } = JSON.parse(zlib.gunzipSync(await fs.readFile(path.join(OUT_DIR, datasetFile))));
  const items = allItems.slice(0, limit);
  console.log(`asking SubQ about ${items.length} minutes from data/futuresNextMinute/${datasetFile}`);
  const rows = await scoreItems(items, buildState, "price_up", QUESTION);

  const scored = rows.filter((row) => row.score !== undefined);
  const failed = rows.filter((row) => row.error);
  if (scored.length === 0) throw new Error("Every SubQ call failed");
  const s = summarize(scored.map((row) => ({ ...row, outcome: row.up })));
  const unchanged = scored.filter((row) => row.exitPrice === row.entryPrice).length;
  const moved = scored.filter((row) => row.exitPrice !== row.entryPrice);

  console.log(`\nSubQ ${SUBQ_MODEL} on ${s.n} random minutes, ${dayOf(Date.parse(meta.from))} → ${dayOf(Date.parse(meta.to) - 1)}, next-minute direction`);
  console.log(`  ${s.yes} up / ${s.no} down (${unchanged} of the downs unchanged), ${failed.length} failed`);
  console.log(`  direction accuracy  ${pct(s.accuracy)}   (always-majority baseline ${pct(s.majorityBaseline)})`);
  console.log(`  excluding unchanged ${pct(moved.filter((row) => (row.score >= 0.5) === row.up).length / moved.length)} of ${moved.length}`);
  console.log(`  AUC                 ${fixed(s.auc, 3)}   (0.5 = no skill)`);
  console.log(`  Brier               ${fixed(s.brier, 4)}   (base-rate baseline ${fixed(s.brierBaseline, 4)})`);
  console.log(`  mean P(up)          up ${fixed(s.meanScoreYes, 3)} | down ${fixed(s.meanScoreNo, 3)}\n`);
  console.log(`$${NOTIONAL} per minute at 1x, market in and out (taker ${TAKER_FEE * 100}% each way), in USDT:`);
  const strategyTotals = STRATEGIES.map(([name, sideOf]) => totals(name, sideOf, scored));
  console.table(strategyTotals);
  if (failed.length) console.log(`failed (first 3): ${failed.slice(0, 3).map((row) => `${row.symbol} ${row.asOf}: ${row.error}`).join(" | ")}`);

  const results = rows.map((row) => {
    const { symbol, asOf, score, up, entryPrice, exitPrice, error } = row;
    if (error) return { symbol, asOf, up, entryPrice, exitPrice, error };
    const side = followSubQ(row);
    return { symbol, asOf, score, side: side === 1 ? "long" : "short", up, entryPrice, exitPrice, ...trade(row, side) };
  });
  // One file per run, stamped with Date.now(), so a rerun never overwrites an earlier one
  const ranAt = Date.now();
  const resultsFile = `results-${ranAt}.json`;
  const output = { ranAt: new Date(ranAt).toISOString(), model: SUBQ_MODEL, dataset: datasetFile, notional: NOTIONAL, takerFee: TAKER_FEE, summary: s, unchanged, totals: strategyTotals, results };
  await fs.writeFile(path.join(OUT_DIR, resultsFile), `${JSON.stringify(output, null, 2)}\n`);
  console.log(`per-minute results saved to data/futuresNextMinute/${resultsFile}`);
}

async function main() {
  const args = process.argv.slice(2);
  if (args.includes("--build")) return build();
  const limitAt = args.indexOf("--limit");
  return run(limitAt === -1 ? Infinity : Number(args[limitAt + 1]));
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err.message);
    process.exit(1);
  });
}

module.exports = { buildState, QUESTION, trade };
