/**
 * Asks SubQ systemone whether Binance USDⓈ-M perpetuals trade higher a day later, then prices following it:
 * $100 per symbol at 1x, long on "up" and short on "down", market in and out, paying funding as it settled.
 *
 *   node scripts/testSubQFuturesPrediction.js --build                    add data/futuresDataGoldenSet-<Date.now()>.json
 *   API_KEY=... node scripts/testSubQFuturesPrediction.js [--limit N]    ask SubQ about the newest, score it and price the trades
 *
 * --build takes the latest complete window, 23:30 SGT the day before → 23:30 SGT, and the 100 USDT perpetuals that
 * traded the most in the 24h before it. SubQ only sees data up to the window's start; the exit price and the funding
 * settled inside the window are kept to score it.
 */
const fs = require("fs/promises");
const path = require("path");
const { SUBQ_MODEL, scoreItems, summarize, fixed, pct } = require("./testSubQPrediction");

const DATA_DIR = path.join(__dirname, "..", "data");
// Each --build adds a golden set stamped with Date.now(), so none is overwritten, and a run uses the newest
const GOLDEN_SET_FILE = /^futuresDataGoldenSet-\d+\.json$/;

const FAPI_URL = "https://fapi.binance.com";
const BINANCE_CONCURRENCY = 8;
const MAX_RETRIES = 3;
const DAY = 86400000;
const WINDOW_END_UTC = [15, 30]; // 23:30 SGT. 30m klines end on it, 1h and 1d ones don't
const KLINES_PER_DAY = 48;
const TARGET_ITEMS = 100;
const HISTORY_DAYS = 30; // daily bars SubQ sees, and how long a symbol must have been listed
const HOURLY_BARS = 24;
const FUNDING_DAYS = 3;

const NOTIONAL = 100; // USDT per position, at 1x. Binance's minimum order is up to $50 (BTCUSDT)
const TAKER_FEE = 0.0005; // VIP 0 USDⓈ-M taker rate, paid on entry and on exit

const BAR_COLUMNS = ["end_utc", "open", "high", "low", "close", "quote_volume_usdt", "taker_buy_share"];
const POSITIONING_COLUMNS = ["time_utc", "open_interest_usdt", "long_short_ratio_accounts", "top_trader_long_short_ratio_positions"];

const QUESTION = {
  type: "noul",
  instructions:
    "state holds Binance USDⓈ-M perpetual futures data for state.symbol, all observed up to state.as_of. Will its price at state.target_time be higher than state.last_price?",
  criteria: {
    true: "The price of state.symbol at state.target_time is higher than state.last_price, its price at state.as_of",
    false: "The price of state.symbol at state.target_time is at or below state.last_price, its price at state.as_of",
  },
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const sum = (values) => values.reduce((total, value) => total + value, 0);
const minute = (ms) => `${new Date(ms).toISOString().slice(0, 16)}Z`;

// ---- golden set -------------------------------------------------------------

// Public market data. Rate limits (429, then 418 once banned) say how long to back off in Retry-After.
async function fapi(endpoint, params) {
  const url = `${FAPI_URL}${endpoint}?${new URLSearchParams(params)}`;
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(url, { signal: AbortSignal.timeout(30000) }).catch((error) => ({ error }));
    if (res.ok) return res.json();
    const retryable = res.error || res.status === 429 || res.status === 418 || res.status >= 500;
    if (!retryable || attempt >= MAX_RETRIES) {
      throw new Error(`Binance ${endpoint} ${params.symbol ?? ""}: ${res.error?.message ?? `HTTP ${res.status} ${await res.text()}`}`);
    }
    await sleep(1000 * (Number(res.headers?.get("retry-after")) || 2 ** attempt));
  }
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

// The `limit` 30m klines that close by `end`
const klinesUntil = (symbol, end, limit) => fapi("/fapi/v1/klines", { symbol, interval: "30m", endTime: end - 1, limit });

// Merges 30m klines into bars of `size` klines, the newest ending with the last kline
function toBars(klines, size) {
  const bars = [];
  for (let end = klines.length; end >= size; end -= size) {
    const group = klines.slice(end - size, end);
    const quoteVolume = sum(group.map((k) => Number(k[7])));
    bars.unshift([
      minute(group[size - 1][6] + 1),
      Number(group[0][1]),
      Math.max(...group.map((k) => Number(k[2]))),
      Math.min(...group.map((k) => Number(k[3]))),
      Number(group[size - 1][4]),
      Math.round(quoteVolume),
      quoteVolume ? +(sum(group.map((k) => Number(k[10]))) / quoteVolume).toFixed(3) : null,
    ]);
  }
  return bars;
}

// The latest 23:30 SGT that has passed, and the 23:30 SGT a day before it
function latestWindow(now) {
  const end = new Date(now);
  end.setUTCHours(...WINDOW_END_UTC, 0, 0);
  if (end > now) end.setUTCDate(end.getUTCDate() - 1);
  return { asOf: end.getTime() - DAY, target: end.getTime() };
}

// Crypto USDT perpetuals (TradFi ones are TRADIFI_PERPETUAL), ranked by quote volume in the 24h before asOf so the
// pick uses nothing after it
async function pickSymbols(asOf) {
  const { symbols } = await fapi("/fapi/v1/exchangeInfo", {});
  const candidates = symbols.filter(
    (s) => s.contractType === "PERPETUAL" && s.quoteAsset === "USDT" && s.status === "TRADING" && s.onboardDate <= asOf - HISTORY_DAYS * DAY,
  );
  const volumes = await mapLimit(candidates, BINANCE_CONCURRENCY, async (s) =>
    sum((await klinesUntil(s.symbol, asOf, KLINES_PER_DAY)).map((k) => Number(k[7]))),
  );
  return candidates
    .map((s, i) => ({
      symbol: s.symbol,
      quoteVolume24h: Math.round(volumes[i]),
      minNotional: Number(s.filters.find((f) => f.filterType === "MIN_NOTIONAL").notional),
    }))
    .sort((a, b) => b.quoteVolume24h - a.quoteVolume24h)
    .slice(0, TARGET_ITEMS);
}

async function buildItem({ symbol, quoteVolume24h, minNotional }, asOf, target) {
  const [history, [last], funding, openInterest, accounts, topPositions] = await Promise.all([
    klinesUntil(symbol, asOf, HISTORY_DAYS * KLINES_PER_DAY),
    klinesUntil(symbol, target, 1),
    fapi("/fapi/v1/fundingRate", { symbol, startTime: asOf - FUNDING_DAYS * DAY, endTime: target, limit: 1000 }),
    // Snapshots taken at their timestamp, so endTime = asOf leaks nothing
    ...["openInterestHist", "globalLongShortAccountRatio", "topLongShortPositionRatio"].map((endpoint) =>
      fapi(`/futures/data/${endpoint}`, { symbol, period: "1h", endTime: asOf, limit: HOURLY_BARS }),
    ),
  ]);
  if (history.length !== HISTORY_DAYS * KLINES_PER_DAY || history.at(-1)[6] + 1 !== asOf || last[6] + 1 !== target) {
    throw new Error(`${symbol}: klines don't cover ${minute(asOf - HISTORY_DAYS * DAY)} → ${minute(target)}`);
  }
  const ratioAt = (list) => new Map(list.map((x) => [x.timestamp, Number(x.longShortRatio)]));
  const accountRatio = ratioAt(accounts);
  const topRatio = ratioAt(topPositions);
  const entryPrice = Number(history.at(-1)[4]);
  const exitPrice = Number(last[4]);
  return {
    symbol,
    asOf: new Date(asOf).toISOString(),
    targetTime: new Date(target).toISOString(),
    quoteVolume24h,
    minNotional,
    entryPrice,
    daily: toBars(history, KLINES_PER_DAY),
    hourly: toBars(history.slice(-HOURLY_BARS * 2), 2),
    funding: funding.filter((f) => f.fundingTime <= asOf).map((f) => [minute(f.fundingTime), Number(f.fundingRate)]),
    positioning: openInterest.map((x) => [
      minute(x.timestamp),
      Math.round(Number(x.sumOpenInterestValue)),
      accountRatio.get(x.timestamp) ?? null,
      topRatio.get(x.timestamp) ?? null,
    ]),
    // Outcome: never sent to SubQ
    exitPrice,
    up: exitPrice > entryPrice,
    fundingInWindow: funding
      .filter((f) => f.fundingTime > asOf && f.fundingTime <= target)
      .map((f) => [minute(f.fundingTime), Number(f.fundingRate), Number(f.markPrice)]),
  };
}

// Pretty-printed, with each array of plain values (a bar, a funding row) on one line
const compactJson = (value) =>
  JSON.stringify(value, null, 2).replace(/\[\s+([^[\]{}]*?)\s+\]/g, (_, inner) => `[${inner.split(/,\s+/).join(", ")}]`);

async function build() {
  const { asOf, target } = latestWindow(new Date());
  const picked = await pickSymbols(asOf);
  console.log(`picked ${picked.length} symbols by quote volume in the 24h to ${minute(asOf)}`);
  const items = await mapLimit(picked, BINANCE_CONCURRENCY / 2, (pick) => buildItem(pick, asOf, target));
  const builtAt = Date.now();
  const meta = {
    source: "binance-usdm-futures",
    builtAt: new Date(builtAt).toISOString(),
    asOf: new Date(asOf).toISOString(),
    targetTime: new Date(target).toISOString(),
    up: items.filter((item) => item.up).length,
    down: items.filter((item) => !item.up).length,
  };
  const goldenSetFile = `futuresDataGoldenSet-${builtAt}.json`;
  await fs.mkdir(DATA_DIR, { recursive: true });
  await fs.writeFile(path.join(DATA_DIR, goldenSetFile), `${compactJson({ meta, items })}\n`);
  console.log(`wrote ${items.length} items (${meta.up} up / ${meta.down} down) to data/${goldenSetFile}`);
}

// ---- SubQ -------------------------------------------------------------------

// Only data up to as_of: the exit price and the funding settled after it stay out
const buildState = (item) => ({
  symbol: item.symbol,
  market: "Binance USDⓈ-M perpetual futures",
  as_of: item.asOf,
  target_time: item.targetTime,
  last_price: item.entryPrice,
  daily_bars: { columns: BAR_COLUMNS, rows: item.daily },
  hourly_bars: { columns: BAR_COLUMNS, rows: item.hourly },
  funding_rates: { columns: ["time_utc", "rate"], rows: item.funding },
  hourly_positioning: { columns: POSITIONING_COLUMNS, rows: item.positioning },
});

// ---- trades -----------------------------------------------------------------

// 1 = long, -1 = short
const followSubQ = (row) => (row.score >= 0.5 ? 1 : -1);

// $NOTIONAL opened at as_of and closed at target_time with market orders. A positive funding rate makes longs pay
// shorts, on the position's value at that settlement's mark price.
function trade(item, side) {
  const qty = NOTIONAL / item.entryPrice;
  const pricePnl = side * qty * (item.exitPrice - item.entryPrice);
  const fees = TAKER_FEE * qty * (item.entryPrice + item.exitPrice);
  const funding = -side * sum(item.fundingInWindow.map(([, rate, markPrice]) => qty * markPrice * rate));
  return { pricePnl, fees, funding, net: pricePnl - fees + funding };
}

function strategy(name, rows, sideOf) {
  const trades = rows.map((row) => trade(row, sideOf(row)));
  const total = (key) => sum(trades.map((t) => t[key]));
  return {
    strategy: name,
    longs: rows.filter((row) => sideOf(row) === 1).length,
    shorts: rows.filter((row) => sideOf(row) === -1).length,
    winners: trades.filter((t) => t.net > 0).length,
    pricePnl: +total("pricePnl").toFixed(2),
    fees: +(-total("fees")).toFixed(2),
    funding: +total("funding").toFixed(2),
    net: +total("net").toFixed(2),
    returnOnStake: pct(total("net") / (NOTIONAL * rows.length)),
  };
}

// ---- run --------------------------------------------------------------------

async function run(limit) {
  // Date.now() stamps have the same length, so the newest sorts last
  const goldenSetFile = (await fs.readdir(DATA_DIR)).filter((file) => GOLDEN_SET_FILE.test(file)).sort().at(-1);
  if (!goldenSetFile) throw new Error("no data/futuresDataGoldenSet-*.json, run with --build first");
  console.log(`asking SubQ about data/${goldenSetFile}`);
  const { meta, items: allItems } = JSON.parse(await fs.readFile(path.join(DATA_DIR, goldenSetFile), "utf8"));
  const items = allItems.slice(0, limit);
  const rows = await scoreItems(items, buildState, "price_up", QUESTION);

  const scored = rows.filter((row) => row.score !== undefined);
  const failed = rows.filter((row) => row.error);
  if (scored.length === 0) throw new Error("Every SubQ call failed");
  const s = summarize(scored.map((row) => ({ ...row, outcome: row.up })));

  console.log(`\nSubQ ${SUBQ_MODEL} on ${s.n} USDT perpetuals, ${minute(Date.parse(meta.asOf))} → ${minute(Date.parse(meta.targetTime))}`);
  console.log(`  ${s.yes} up / ${s.no} down, ${failed.length} failed`);
  console.log(`  direction accuracy  ${pct(s.accuracy)}   (always-majority baseline ${pct(s.majorityBaseline)})`);
  console.log(`  AUC                 ${fixed(s.auc, 3)}   (0.5 = no skill)`);
  console.log(`  Brier               ${fixed(s.brier, 4)}   (base-rate baseline ${fixed(s.brierBaseline, 4)})`);
  console.log(`  mean P(up)          up ${fixed(s.meanScoreYes, 3)} | down ${fixed(s.meanScoreNo, 3)}\n`);

  console.log(`$${NOTIONAL} per symbol at 1x, market in and out (taker ${TAKER_FEE * 100}% each way), funding as settled, in USDT:`);
  const strategies = [
    strategy("follow SubQ", scored, followSubQ),
    strategy("always long", scored, () => 1),
    strategy("always short", scored, () => -1),
  ];
  console.table(strategies);
  const belowMinimum = scored.filter((row) => row.minNotional > NOTIONAL);
  if (belowMinimum.length) {
    console.log(`Binance's minimum order is above $${NOTIONAL} on ${belowMinimum.map((row) => `${row.symbol} ($${row.minNotional})`).join(", ")}`);
  }
  if (failed.length) console.log(`failed (first 3): ${failed.slice(0, 3).map((row) => `${row.symbol}: ${row.error}`).join(" | ")}`);

  const results = rows.map((row) => {
    const { symbol, score, up, entryPrice, exitPrice, error } = row;
    if (error) return { symbol, up, entryPrice, exitPrice, error };
    const side = followSubQ(row);
    return { symbol, score, side: side === 1 ? "long" : "short", up, entryPrice, exitPrice, ...trade(row, side) };
  });
  // One file per run, stamped with Date.now(), so a rerun never overwrites an earlier one
  const ranAt = Date.now();
  const resultsFile = `futuresSubQResults-${ranAt}.json`;
  const output = { ranAt: new Date(ranAt).toISOString(), model: SUBQ_MODEL, dataset: goldenSetFile, asOf: meta.asOf, targetTime: meta.targetTime, summary: s, strategies, results };
  await fs.writeFile(path.join(DATA_DIR, resultsFile), `${JSON.stringify(output, null, 2)}\n`);
  console.log(`per-symbol results saved to data/${resultsFile}`);
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
