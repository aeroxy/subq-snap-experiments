/**
 * The SubQ futures test of testSubQFuturesPrediction.js over many days, to see whether one day's result holds up.
 * Each day is its own dataset: 23:30 SGT the day before → 23:30 SGT, with the 100 USDT perpetuals that traded the most
 * in the 24h before it; $100 per symbol at 1x, long on "up" and short on "down", market in and out, paying funding.
 *
 *   node scripts/testSubQFuturesBackfill.js --build [--days N]       build the latest N days (default 28) not built yet
 *   API_KEY=... node scripts/testSubQFuturesBackfill.js [--days N]   ask SubQ about every built day, or the latest N
 *
 * Days are saved as data/futuresBackfill/dataset-<date>.json and never rebuilt, so --build can be rerun to add days.
 * Binance only serves 30 days of open interest and long/short ratios, so older days can't be built. Each run saves
 * data/futuresBackfill/results-<Date.now()>.json.
 *
 * The question, state and trade math are copies of testSubQFuturesPrediction.js's rather than imports, so changing
 * one script can't alter the other's results.
 */
const fs = require("fs/promises");
const path = require("path");
const { SUBQ_MODEL, scoreItems, summarize, fixed, pct } = require("./testSubQPrediction");

const BACKFILL_DIR = path.join(__dirname, "..", "data", "futuresBackfill");
const DAYS = 28;

const FAPI_URL = "https://fapi.binance.com";
const BINANCE_CONCURRENCY = 8;
const SERIES_CONCURRENCY = 4;
const MAX_RETRIES = 3;
const HOUR = 3600000;
const DAY = 24 * HOUR;
const WINDOW_END_UTC = [15, 30]; // 23:30 SGT. 30m klines end on it, 1h and 1d ones don't
const KLINES_PER_DAY = 48;
const KLINE_MS = DAY / KLINES_PER_DAY;
const TARGET_ITEMS = 100;
const HISTORY_DAYS = 30; // daily bars SubQ sees, and how long a symbol must have been listed
const HOURLY_BARS = 24;
const FUNDING_DAYS = 3;
const POSITIONING_DAYS_KEPT = 30; // Binance rejects open interest and long/short ratio requests going back further

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
const dayOf = (ms) => new Date(ms).toISOString().slice(0, 10); // a window's day is the date of its closing 23:30 SGT
const datasetPath = (target) => path.join(BACKFILL_DIR, `dataset-${dayOf(target)}.json`);
const exists = (file) => fs.access(file).then(() => true, () => false);

// ---- Binance ----------------------------------------------------------------

// Request weight is capped at 2400 a minute (reported in x-mbx-used-weight-1m) and /futures/data at 1000 calls per
// 5 minutes (not reported). Staying under both avoids 429s, which Binance escalates to IP bans (418).
const WEIGHT_PAUSE_AT = 2000;
const DATA_CALLS_PER_5_MIN = 900;
let pausedUntil = 0;
const dataCalls = [];

async function throttle(endpoint) {
  for (;;) {
    const now = Date.now();
    if (now < pausedUntil) {
      await sleep(pausedUntil - now);
      continue;
    }
    if (!endpoint.startsWith("/futures/data/")) return;
    while (dataCalls.length && dataCalls[0] <= now - 300000) dataCalls.shift();
    if (dataCalls.length < DATA_CALLS_PER_5_MIN) return void dataCalls.push(now);
    await sleep(dataCalls[0] + 300000 - now);
  }
}

// Public market data. Rate limits (429, then 418 once banned) say how long to back off in Retry-After, for every call.
async function fapi(endpoint, params) {
  const url = `${FAPI_URL}${endpoint}?${new URLSearchParams(params)}`;
  for (let attempt = 0; ; attempt++) {
    await throttle(endpoint);
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
// fills it from (/futures/data returns the newest rows, klines and fundingRate the oldest). Slices start on a whole
// step: Binance rounds startTime down to one, so an unaligned start returns the previous slice's last row again.
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

// 30m klines opening in [from, to). Their weight grows with the limit asked for (1 under 100, 10 over 1000), which
// fetchAll keeps to the rows each slice holds.
const klinesBetween = (symbol, from, to) => fetchAll("/fapi/v1/klines", { symbol, interval: "30m" }, from, to - 1, 1500, KLINE_MS);

// ---- datasets ---------------------------------------------------------------

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

// For each asOf, the crypto USDT perpetuals (TradFi ones are TRADIFI_PERPETUAL) that traded the most in the 24h before
// it, so the pick uses nothing after it. Delisted perps stay listed as SETTLING until their deliveryDate and count
// while they traded, so past days aren't limited to the coins that survived.
async function rankSymbols(asOfs) {
  const first = Math.min(...asOfs);
  const last = Math.max(...asOfs);
  const { symbols } = await fapi("/fapi/v1/exchangeInfo", {});
  const candidates = symbols.filter(
    (s) =>
      s.contractType === "PERPETUAL" &&
      s.quoteAsset === "USDT" &&
      ["TRADING", "SETTLING"].includes(s.status) &&
      s.deliveryDate > first + DAY &&
      s.onboardDate <= last - HISTORY_DAYS * DAY,
  );
  const volumes = await mapLimit(candidates, BINANCE_CONCURRENCY, async (s) => {
    const quoteVolumeAt = new Map((await klinesBetween(s.symbol, first - DAY, last)).map((k) => [k[0], Number(k[7])]));
    return asOfs.map((asOf) => sum(Array.from({ length: KLINES_PER_DAY }, (_, i) => quoteVolumeAt.get(asOf - (i + 1) * KLINE_MS) ?? 0)));
  });
  return new Map(
    asOfs.map((asOf, w) => [
      asOf,
      candidates
        .map((s, i) => ({ s, quoteVolume24h: volumes[i][w] }))
        // Listed for HISTORY_DAYS by asOf, and still trading at the window's end
        .filter(({ s }) => s.onboardDate <= asOf - HISTORY_DAYS * DAY && s.deliveryDate > asOf + DAY)
        .sort((a, b) => b.quoteVolume24h - a.quoteVolume24h)
        .slice(0, TARGET_ITEMS)
        .map(({ s, quoteVolume24h }) => ({
          symbol: s.symbol,
          quoteVolume24h: Math.round(quoteVolume24h),
          minNotional: Number(s.filters.find((f) => f.filterType === "MIN_NOTIONAL").notional),
          ...(s.status !== "TRADING" && { delistedAt: new Date(s.deliveryDate).toISOString() }),
        })),
    ]),
  );
}

// Everything assembleItem needs for the windows starting at asOfs
async function fetchSeries(symbol, asOfs) {
  const first = Math.min(...asOfs);
  const last = Math.max(...asOfs);
  const [klines, funding, openInterest, accounts, topPositions] = await Promise.all([
    klinesBetween(symbol, first - HISTORY_DAYS * DAY, last + DAY),
    // Settlements are at least an hour apart
    fetchAll("/fapi/v1/fundingRate", { symbol }, first - FUNDING_DAYS * DAY, last + DAY, 1000, HOUR),
    // Snapshots taken at their timestamp, so the ones up to asOf leak nothing
    ...["openInterestHist", "globalLongShortAccountRatio", "topLongShortPositionRatio"].map((endpoint) =>
      fetchAll(`/futures/data/${endpoint}`, { symbol, period: "1h" }, first - DAY + 1, last, 500, HOUR),
    ),
  ]);
  return { klines, funding, openInterest, accounts, topPositions };
}

// One window's item, or null when its klines have a gap (a trading halt)
function assembleItem({ symbol, quoteVolume24h, minNotional, delistedAt }, series, asOf, target) {
  const index = new Map(series.klines.map((k, i) => [k[0], i]));
  const start = index.get(asOf - HISTORY_DAYS * DAY);
  const history = start === undefined ? [] : series.klines.slice(start, start + HISTORY_DAYS * KLINES_PER_DAY);
  const last = series.klines[index.get(target - KLINE_MS)];
  if (!last || history.at(-1)?.[6] + 1 !== asOf) return null;
  const ratioAt = (list) => new Map(list.map((x) => [x.timestamp, Number(x.longShortRatio)]));
  const accountRatio = ratioAt(series.accounts);
  const topRatio = ratioAt(series.topPositions);
  const entryPrice = Number(history.at(-1)[4]);
  const exitPrice = Number(last[4]);
  return {
    symbol,
    asOf: new Date(asOf).toISOString(),
    targetTime: new Date(target).toISOString(),
    quoteVolume24h,
    minNotional,
    ...(delistedAt && { delistedAt }),
    entryPrice,
    daily: toBars(history, KLINES_PER_DAY),
    hourly: toBars(history.slice(-HOURLY_BARS * 2), 2),
    funding: series.funding
      .filter((f) => f.fundingTime >= asOf - FUNDING_DAYS * DAY && f.fundingTime <= asOf)
      .map((f) => [minute(f.fundingTime), Number(f.fundingRate)]),
    positioning: series.openInterest
      .filter((x) => x.timestamp > asOf - DAY && x.timestamp <= asOf)
      .map((x) => [minute(x.timestamp), Math.round(Number(x.sumOpenInterestValue)), accountRatio.get(x.timestamp) ?? null, topRatio.get(x.timestamp) ?? null]),
    // Outcome: never sent to SubQ
    exitPrice,
    up: exitPrice > entryPrice,
    fundingInWindow: series.funding
      .filter((f) => f.fundingTime > asOf && f.fundingTime <= target)
      .map((f) => [minute(f.fundingTime), Number(f.fundingRate), Number(f.markPrice)]),
  };
}

// Pretty-printed, with each array of plain values (a bar, a funding row) on one line
const compactJson = (value) =>
  JSON.stringify(value, null, 2).replace(/\[\s+([^[\]{}]*?)\s+\]/g, (_, inner) => `[${inner.split(/,\s+/).join(", ")}]`);

async function writeDataset(asOf, target, items) {
  const meta = {
    source: "binance-usdm-futures",
    builtAt: new Date().toISOString(),
    asOf: new Date(asOf).toISOString(),
    targetTime: new Date(target).toISOString(),
    up: items.filter((item) => item.up).length,
    down: items.filter((item) => !item.up).length,
  };
  await fs.mkdir(BACKFILL_DIR, { recursive: true });
  await fs.writeFile(datasetPath(target), `${compactJson({ meta, items })}\n`);
  return meta;
}

async function build(days) {
  const { asOf: latest } = latestWindow(new Date());
  const windows = [];
  for (let i = 0; i < days; i++) {
    const asOf = latest - i * DAY;
    if (await exists(datasetPath(asOf + DAY))) continue;
    // A day needs the 24h of open interest and long/short ratios before it, fetched from the whole hour before that
    if (asOf - DAY - HOUR < Date.now() - POSITIONING_DAYS_KEPT * DAY) {
      console.log(`can't build ${dayOf(asOf + DAY)}: Binance keeps only ${POSITIONING_DAYS_KEPT} days of open interest and long/short ratios`);
      continue;
    }
    windows.push({ asOf, target: asOf + DAY });
  }
  if (windows.length === 0) return console.log("nothing to build");

  const picks = await rankSymbols(windows.map((w) => w.asOf));
  // Each symbol's history is fetched once, for every day that picked it
  const picksBySymbol = new Map();
  for (const w of windows) {
    for (const pick of picks.get(w.asOf)) {
      if (!picksBySymbol.has(pick.symbol)) picksBySymbol.set(pick.symbol, []);
      picksBySymbol.get(pick.symbol).push({ ...w, pick });
    }
  }
  console.log(`ranked ${windows.length} days, fetching the history of ${picksBySymbol.size} symbols`);
  const items = new Map();
  let done = 0;
  await mapLimit([...picksBySymbol], SERIES_CONCURRENCY, async ([symbol, picked]) => {
    const series = await fetchSeries(symbol, picked.map(({ asOf }) => asOf));
    for (const { asOf, target, pick } of picked) items.set(`${asOf} ${symbol}`, assembleItem(pick, series, asOf, target));
    process.stdout.write(`\r${++done}/${picksBySymbol.size}`);
  });
  process.stdout.write("\n");

  for (const { asOf, target } of windows.reverse()) {
    const picked = picks.get(asOf);
    const dayItems = picked.map((pick) => items.get(`${asOf} ${pick.symbol}`));
    const skipped = picked.filter((_, i) => !dayItems[i]).map((pick) => pick.symbol);
    const meta = await writeDataset(asOf, target, dayItems.filter(Boolean));
    console.log(
      `${dayOf(target)}: ${meta.up} up / ${meta.down} down${skipped.length ? `, skipped for gaps in their klines: ${skipped.join(", ")}` : ""}`,
    );
  }
  console.log(`wrote ${windows.length} days to data/futuresBackfill/`);
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

const STRATEGIES = [
  ["follow SubQ", followSubQ],
  ["always long", () => 1],
  ["always short", () => -1],
  // Long what rose in the 24h before as_of, short what fell: the simplest rule worth beating
  ["24h momentum", (row) => (row.entryPrice > row.daily.at(-1)[1] ? 1 : -1)],
];

// $NOTIONAL opened at as_of and closed at target_time with market orders. A positive funding rate makes longs pay
// shorts, on the position's value at that settlement's mark price.
function trade(item, side) {
  const qty = NOTIONAL / item.entryPrice;
  const pricePnl = side * qty * (item.exitPrice - item.entryPrice);
  const fees = TAKER_FEE * qty * (item.entryPrice + item.exitPrice);
  const funding = -side * sum(item.fundingInWindow.map(([, rate, markPrice]) => qty * markPrice * rate));
  return { pricePnl, fees, funding, net: pricePnl - fees + funding };
}

// One strategy over every day. tStat is the mean daily net over its standard error: beyond about ±2 is unlikely luck.
function totals(name, sideOf, rows, daily, subqDaily) {
  const trades = rows.map((row) => trade(row, sideOf(row)));
  const total = (key) => sum(trades.map((t) => t[key]));
  const mean = sum(daily) / daily.length;
  const sd = Math.sqrt(sum(daily.map((x) => (x - mean) ** 2)) / (daily.length - 1));
  return {
    strategy: name,
    longs: rows.filter((row) => sideOf(row) === 1).length,
    shorts: rows.filter((row) => sideOf(row) === -1).length,
    pricePnl: +total("pricePnl").toFixed(2),
    fees: +(-total("fees")).toFixed(2),
    funding: +total("funding").toFixed(2),
    net: +total("net").toFixed(2),
    avgDailyReturn: pct(total("net") / (NOTIONAL * rows.length)),
    profitableDays: `${daily.filter((x) => x > 0).length}/${daily.length}`,
    tStat: fixed(mean / (sd / Math.sqrt(daily.length)), 2),
    subqAhead: name === STRATEGIES[0][0] ? "" : `${subqDaily.filter((x, i) => x > daily[i]).length}/${daily.length} days`,
  };
}

// How often coins went up for each band of SubQ's P(up): a calibrated model's up rate tracks its P(up)
function calibration(rows, bins = 5) {
  const table = [];
  for (let i = 0; i < bins; i++) {
    const inBin = rows.filter((row) => Math.min(Math.floor(row.score * bins), bins - 1) === i);
    if (inBin.length === 0) continue;
    table.push({
      pUp: `${(i / bins).toFixed(1)}-${((i + 1) / bins).toFixed(1)}`,
      n: inBin.length,
      meanPUp: +(sum(inBin.map((row) => row.score)) / inBin.length).toFixed(3),
      actualUpRate: +(inBin.filter((row) => row.up).length / inBin.length).toFixed(3),
    });
  }
  return table;
}

// ---- run --------------------------------------------------------------------

async function run(days) {
  const files = (await fs.readdir(BACKFILL_DIR).catch(() => []))
    .filter((file) => /^dataset-\d{4}-\d{2}-\d{2}\.json$/.test(file))
    .sort()
    .slice(-days);
  if (files.length === 0) throw new Error("no datasets in data/futuresBackfill, run with --build first");
  const items = [];
  for (const file of files) {
    const { meta, items: dayItems } = JSON.parse(await fs.readFile(path.join(BACKFILL_DIR, file), "utf8"));
    items.push(...dayItems.map((item) => ({ ...item, date: dayOf(Date.parse(meta.targetTime)) })));
  }
  console.log(`asking SubQ about ${items.length} symbol-days over ${files.length} days`);
  const rows = await scoreItems(items, buildState, "price_up", QUESTION);

  const scored = rows.filter((row) => row.score !== undefined);
  const failed = rows.filter((row) => row.error);
  if (scored.length === 0) throw new Error("Every SubQ call failed");
  const withOutcome = (list) => list.map((row) => ({ ...row, outcome: row.up }));
  const s = summarize(withOutcome(scored));
  const dates = [...new Set(scored.map((row) => row.date))].sort();
  const byDay = dates.map((date) => scored.filter((row) => row.date === date));

  console.log(`\nSubQ ${SUBQ_MODEL} on ${dates.length} days, ${dates[0]} → ${dates.at(-1)}, each 23:30 SGT the day before → 23:30 SGT`);
  console.log(`  ${s.n} symbol-days: ${s.yes} up / ${s.no} down, ${failed.length} failed`);
  console.log(`  direction accuracy  ${pct(s.accuracy)}   (always-majority baseline ${pct(s.majorityBaseline)})`);
  console.log(`  AUC                 ${fixed(s.auc, 3)}   (0.5 = no skill)`);
  console.log(`  Brier               ${fixed(s.brier, 4)}   (base-rate baseline ${fixed(s.brierBaseline, 4)})`);
  console.log(`  mean P(up)          up ${fixed(s.meanScoreYes, 3)} | down ${fixed(s.meanScoreNo, 3)}\n`);
  const calibrationTable = calibration(scored);
  console.table(calibrationTable);

  // Net USDT per strategy per day
  const daily = STRATEGIES.map(([, sideOf]) => byDay.map((dayRows) => sum(dayRows.map((row) => trade(row, sideOf(row)).net))));
  const perDay = dates.map((date, i) => {
    const day = summarize(withOutcome(byDay[i]));
    return {
      date,
      n: day.n,
      up: day.yes,
      accuracy: pct(day.accuracy),
      auc: fixed(day.auc, 3),
      ...Object.fromEntries(STRATEGIES.map(([name], k) => [name, +daily[k][i].toFixed(2)])),
    };
  });
  console.log(`\nnet USDT per day, $${NOTIONAL} per symbol at 1x, after fees (taker ${TAKER_FEE * 100}% each way) and funding:`);
  console.table(perDay);
  const strategyTotals = STRATEGIES.map(([name, sideOf], k) => totals(name, sideOf, scored, daily[k], daily[0]));
  console.log(`\nall ${dates.length} days (avgDailyReturn on the $${NOTIONAL * TARGET_ITEMS} staked a day):`);
  console.table(strategyTotals);
  if (failed.length) console.log(`failed (first 3): ${failed.slice(0, 3).map((row) => `${row.date} ${row.symbol}: ${row.error}`).join(" | ")}`);

  const results = rows.map((row) => {
    const { date, symbol, delistedAt, score, up, entryPrice, exitPrice, error } = row;
    if (error) return { date, symbol, delistedAt, up, entryPrice, exitPrice, error };
    const side = followSubQ(row);
    return { date, symbol, delistedAt, score, side: side === 1 ? "long" : "short", up, entryPrice, exitPrice, ...trade(row, side) };
  });
  // One file per run, stamped with Date.now(), so a rerun never overwrites an earlier one
  const ranAt = Date.now();
  const resultsFile = `results-${ranAt}.json`;
  const output = {
    ranAt: new Date(ranAt).toISOString(),
    model: SUBQ_MODEL,
    notional: NOTIONAL,
    takerFee: TAKER_FEE,
    datasets: files,
    summary: s,
    calibration: calibrationTable,
    perDay,
    totals: strategyTotals,
    results,
  };
  await fs.writeFile(path.join(BACKFILL_DIR, resultsFile), `${JSON.stringify(output, null, 2)}\n`);
  console.log(`per-symbol results saved to data/futuresBackfill/${resultsFile}`);
}

async function main() {
  const args = process.argv.slice(2);
  const daysAt = args.indexOf("--days");
  const days = daysAt === -1 ? undefined : Number(args[daysAt + 1]);
  if (args.includes("--build")) return build(days ?? DAYS);
  return run(days ?? Infinity);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err.message);
    process.exit(1);
  });
}

module.exports = { buildState, QUESTION, trade };
