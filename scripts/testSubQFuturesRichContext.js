/**
 * Does SubQ call next-day direction better with far more context? testSubQFuturesBackfill.js gave it ~6k tokens per
 * coin-day; this gives the top 25 coins of each of its 28 days ~250k (SubQ takes up to ~400k): the same state, plus a
 * year of candles at finer intervals toward as_of, a month of open interest and long/short ratios, order-book depth,
 * and BTC as market context. The question, outcome and trades are the backfill's, so each call pairs with a compact one.
 *
 *   node scripts/testSubQFuturesRichContext.js --build [--days N]          data/futuresRichContext/dataset-<date>.json.gz
 *   node --env-file=.env scripts/testSubQFuturesRichContext.js [--days N]  ask SubQ and compare with the compact run
 *
 * Built only from Binance data that doesn't expire (klines, funding, and the data.binance.vision daily dumps of
 * metrics and bookDepth), so --build can rebuild a day later. Each run saves data/futuresRichContext/results-<Date.now()>.json.
 * The question, compact state and trade math are copies of testSubQFuturesBackfill.js's, so neither script can change
 * the other's results.
 */
const fs = require("fs/promises");
const os = require("os");
const path = require("path");
const zlib = require("zlib");
const { SUBQ_MODEL, scoreItems, summarize, fixed, pct } = require("./testSubQPrediction");

const BACKFILL_DIR = path.join(__dirname, "..", "data", "futuresBackfill");
// The compact-state SubQ run every rich call is paired with
const COMPACT_RESULTS = path.join(BACKFILL_DIR, "results-1791739573800.json");
const OUT_DIR = path.join(__dirname, "..", "data", "futuresRichContext");
const DUMP_CACHE = path.join(os.tmpdir(), "binance-vision-cache");
const COINS_PER_DAY = 25;

const FAPI_URL = "https://fapi.binance.com";
const DUMP_URL = "https://data.binance.vision/data/futures/um/daily";
const CONCURRENCY = 4;
const MAX_RETRIES = 3;
const MINUTE = 60000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

// [interval, bars before as_of], coarse to fine. A series lists consecutive bars, oldest first, the last one closing at
// last_bar_closes_utc: times are implicit because SubQ counts about a token per character.
const CANDLES = [["1d", 365], ["1h", 30 * 24], ["15m", 7 * 96], ["5m", 24 * 12], ["1m", 6 * 60]];
const BTC_CANDLES = [["1d", 90], ["1h", 7 * 24], ["5m", 24 * 12]];
const POSITIONING = [["1h", 30 * 24], ["5m", 24 * 12]];
const DEPTH = [["15m", 96], ["1m", 60]];
const FUNDING_DAYS = 14;
const INTERVAL_MS = { "1d": DAY, "1h": HOUR, "15m": 15 * MINUTE, "5m": 5 * MINUTE, "1m": MINUTE };

const CANDLE_COLUMNS = ["open", "high", "low", "close", "quote_volume_usdt", "taker_buy_share", "trades"];
// From the metrics dump: sum_open_interest_value, count_toptrader_long_short_ratio, sum_toptrader_long_short_ratio,
// count_long_short_ratio. Its taker volume ratio is left out: it is a bucket that may end after its timestamp.
const POSITIONING_COLUMNS = ["open_interest_usdt", "top_trader_long_short_accounts", "top_trader_long_short_positions", "long_short_accounts"];
const DEPTH_LEVELS = ["-5.00", "-4.00", "-3.00", "-2.00", "-1.00", "1.00", "2.00", "3.00", "4.00", "5.00"];

// ---- copied from testSubQFuturesBackfill.js ---------------------------------

const NOTIONAL = 100; // USDT per position, at 1x. Binance's minimum order is up to $50 (BTCUSDT)
const TAKER_FEE = 0.0005; // VIP 0 USDⓈ-M taker rate, paid on entry and on exit

const BAR_COLUMNS = ["end_utc", "open", "high", "low", "close", "quote_volume_usdt", "taker_buy_share"];
const COMPACT_POSITIONING_COLUMNS = ["time_utc", "open_interest_usdt", "long_short_ratio_accounts", "top_trader_long_short_ratio_positions"];

const QUESTION = {
  type: "noul",
  instructions:
    "state holds Binance USDⓈ-M perpetual futures data for state.symbol, all observed up to state.as_of. Will its price at state.target_time be higher than state.last_price?",
  criteria: {
    true: "The price of state.symbol at state.target_time is higher than state.last_price, its price at state.as_of",
    false: "The price of state.symbol at state.target_time is at or below state.last_price, its price at state.as_of",
  },
};

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
  hourly_positioning: { columns: COMPACT_POSITIONING_COLUMNS, rows: item.positioning },
});

// $NOTIONAL opened at as_of and closed at target_time with market orders. A positive funding rate makes longs pay
// shorts, on the position's value at that settlement's mark price.
function trade(item, side) {
  const qty = NOTIONAL / item.entryPrice;
  const pricePnl = side * qty * (item.exitPrice - item.entryPrice);
  const fees = TAKER_FEE * qty * (item.entryPrice + item.exitPrice);
  const funding = -side * sum(item.fundingInWindow.map(([, rate, markPrice]) => qty * markPrice * rate));
  return { pricePnl, fees, funding, net: pricePnl - fees + funding };
}

// -----------------------------------------------------------------------------

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const sum = (values) => values.reduce((total, value) => total + value, 0);
const minute = (ms) => `${new Date(ms).toISOString().slice(0, 16)}Z`;
const dayOf = (ms) => new Date(ms).toISOString().slice(0, 10);
const exists = (file) => fs.access(file).then(() => true, () => false);

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

// One day of a daily dump (e.g. metrics, bookDepth), cached on disk; [] when Binance has none for that day
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

// A row at a time: a big dump is too many arguments to spread into push
async function dumpRows(kind, symbol, from, to) {
  const rows = [];
  for (const day of daysBetween(from, to)) for (const row of await dump(kind, symbol, day)) rows.push(row);
  return rows;
}

const utcTime = (text) => Date.parse(`${text.replace(" ", "T")}Z`);
const daysBetween = (from, to) => Array.from({ length: Math.floor(to / DAY) - Math.floor(from / DAY) + 1 }, (_, i) => dayOf(from + i * DAY));

// ---- series -----------------------------------------------------------------

// `bars` consecutive values on an interval grid ending at `end`, null where the source has none, leading nulls (before
// a listing) dropped
function gridSeries(interval, bars, end, valueAt) {
  const step = INTERVAL_MS[interval];
  const rows = Array.from({ length: bars }, (_, i) => valueAt(end - (bars - 1 - i) * step) ?? null);
  const first = rows.findIndex((row) => row !== null);
  return { interval, last_bar_closes_utc: minute(end), rows: first === -1 ? [] : rows.slice(first) };
}

// Closed candles only: the last one closes at or before asOf
async function candles(symbol, interval, bars, asOf) {
  const step = INTERVAL_MS[interval];
  const end = Math.floor(asOf / step) * step;
  const klines = await fetchAll("/fapi/v1/klines", { symbol, interval }, end - bars * step, end - 1, 1500, step);
  const byOpen = new Map(klines.map((k) => [k[0], k]));
  const series = gridSeries(interval, bars, end, (close) => {
    const k = byOpen.get(close - step);
    if (!k) return null;
    const quoteVolume = Number(k[7]);
    return [Number(k[1]), Number(k[2]), Number(k[3]), Number(k[4]), Math.round(quoteVolume), quoteVolume ? +(Number(k[10]) / quoteVolume).toFixed(3) : null, k[8]];
  });
  return { ...series, columns: CANDLE_COLUMNS };
}

async function candleSet(symbol, spec, asOf) {
  const set = {};
  for (const [interval, bars] of spec) set[interval] = await candles(symbol, interval, bars, asOf);
  return set;
}

// Open interest and long/short ratios are snapshots at their timestamp. The metrics dump stamps each one 5 minutes
// before the REST endpoints do (its 15:55 row is their 16:00), so rows move to the REST time, from which they are
// known. The 5m series stops one step before asOf, in case a snapshot at asOf was taken just after it.
const METRICS_LAG = 5 * MINUTE;

function positioningSeries(metrics, asOf) {
  const at = new Map(metrics.map((row) => [utcTime(row[0]) + METRICS_LAG, row]));
  const set = {};
  for (const [interval, bars] of POSITIONING) {
    const step = INTERVAL_MS[interval];
    const end = Math.floor((asOf - (interval === "5m" ? step : 0)) / step) * step;
    const series = gridSeries(interval, bars, end, (time) => {
      const row = at.get(time);
      return row && [Math.round(Number(row[3])), +Number(row[4]).toFixed(4), +Number(row[5]).toFixed(4), +Number(row[6]).toFixed(4)];
    });
    set[interval] = { ...series, columns: POSITIONING_COLUMNS };
  }
  return set;
}

// Cumulative notional (millions of USDT) within ±1-5% of the mid price: the latest snapshot at or before each grid time,
// if it is less than one step old
function depthSeries(snapshots, asOf) {
  const set = {};
  for (const [interval, bars] of DEPTH) {
    const step = INTERVAL_MS[interval];
    let i = 0;
    const series = gridSeries(interval, bars, asOf, (time) => {
      while (i + 1 < snapshots.length && snapshots[i + 1].time <= time) i++;
      const snapshot = snapshots[i];
      return snapshot && snapshot.time <= time && snapshot.time > time - step ? snapshot.levels : null;
    });
    set[interval] = { ...series, columns: DEPTH_LEVELS.map((level) => `${Number(level) > 0 ? "+" : ""}${Number(level)}%`) };
  }
  return set;
}

function depthSnapshots(rows) {
  const byTime = new Map();
  for (const [time, level, , notional] of rows) {
    const t = utcTime(time);
    if (!byTime.has(t)) byTime.set(t, {});
    byTime.get(t)[level] = notional;
  }
  return [...byTime]
    .sort((a, b) => a[0] - b[0])
    .map(([time, levels]) => ({ time, levels: DEPTH_LEVELS.map((level) => (levels[level] === undefined ? null : +(Number(levels[level]) / 1e6).toFixed(3))) }));
}

// ---- datasets ---------------------------------------------------------------

const datasetPath = (date) => path.join(OUT_DIR, `dataset-${date}.json.gz`);

async function backfillDays(days) {
  const files = (await fs.readdir(BACKFILL_DIR)).filter((file) => /^dataset-\d{4}-\d{2}-\d{2}\.json$/.test(file)).sort().slice(-days);
  return Promise.all(
    files.map(async (file) => {
      const { meta, items } = JSON.parse(await fs.readFile(path.join(BACKFILL_DIR, file), "utf8"));
      return { date: dayOf(Date.parse(meta.targetTime)), asOf: Date.parse(meta.asOf), items: items.slice(0, COINS_PER_DAY) };
    }),
  );
}

async function build(days) {
  const todo = [];
  for (const day of await backfillDays(days)) if (!(await exists(datasetPath(day.date)))) todo.push(day);
  if (todo.length === 0) return console.log("nothing to build");

  // BTC context, once per day
  const btc = new Map();
  for (const day of todo) btc.set(day.date, await candleSet("BTCUSDT", BTC_CANDLES, day.asOf));

  // Each symbol's dumps are read once for all the days that picked it
  const bySymbol = new Map();
  for (const day of todo) {
    for (const item of day.items) {
      if (!bySymbol.has(item.symbol)) bySymbol.set(item.symbol, []);
      bySymbol.get(item.symbol).push({ day, item });
    }
  }
  console.log(`building ${todo.length} days, ${sum(todo.map((day) => day.items.length))} coin-days, ${bySymbol.size} symbols`);
  const rich = new Map();
  let done = 0;
  await mapLimit([...bySymbol], CONCURRENCY, async ([symbol, picked]) => {
    const first = Math.min(...picked.map(({ day }) => day.asOf));
    const last = Math.max(...picked.map(({ day }) => day.asOf));
    const metrics = await dumpRows("metrics", symbol, first - POSITIONING[0][1] * HOUR - DAY, last);
    const funding = await fetchAll("/fapi/v1/fundingRate", { symbol }, first - FUNDING_DAYS * DAY, last, 1000, HOUR);
    for (const { day, item } of picked) {
      const depthRows = await dumpRows("bookDepth", symbol, day.asOf - DAY, day.asOf);
      rich.set(`${day.date} ${symbol}`, {
        ...item,
        rich: {
          candles: await candleSet(symbol, CANDLES, day.asOf),
          positioning: positioningSeries(metrics, day.asOf),
          funding: funding
            .filter((f) => f.fundingTime > day.asOf - FUNDING_DAYS * DAY && f.fundingTime <= day.asOf)
            .map((f) => [minute(f.fundingTime), Number(f.fundingRate)]),
          depth: depthSeries(depthSnapshots(depthRows), day.asOf),
          ...(symbol !== "BTCUSDT" && { btc: btc.get(day.date) }),
        },
      });
    }
    process.stdout.write(`\r${++done}/${bySymbol.size} symbols`);
  });
  process.stdout.write("\n");

  await fs.mkdir(OUT_DIR, { recursive: true });
  for (const day of todo) {
    const items = day.items.map((item) => rich.get(`${day.date} ${item.symbol}`));
    const sizes = items.map((item) => JSON.stringify(buildRichState(item)).length);
    const meta = { source: "binance-usdm-futures", builtAt: new Date().toISOString(), asOf: new Date(day.asOf).toISOString(), coins: items.length, stateChars: { min: Math.min(...sizes), max: Math.max(...sizes) } };
    await fs.writeFile(datasetPath(day.date), zlib.gzipSync(JSON.stringify({ meta, items })));
    console.log(`${day.date}: ${items.length} coins, state ${Math.round(meta.stateChars.min / 1000)}-${Math.round(meta.stateChars.max / 1000)}k chars`);
  }
}

// ---- SubQ -------------------------------------------------------------------

// The backfill's compact state plus the rich series, all ending at or before as_of
const buildRichState = (item) => ({
  ...buildState(item),
  candles: item.rich.candles,
  positioning_history: item.rich.positioning,
  funding_history: { columns: ["time_utc", "rate"], rows: item.rich.funding },
  order_book_depth_musdt: item.rich.depth,
  ...(item.rich.btc && { btc_candles: item.rich.btc }),
});

// ---- comparison -------------------------------------------------------------

// 1 = long, -1 = short
const sideFor = (score) => (score >= 0.5 ? 1 : -1);

const STRATEGIES = [
  ["SubQ rich", (row) => sideFor(row.score)],
  ["SubQ compact", (row) => sideFor(row.compactScore)],
  ["always long", () => 1],
  ["always short", () => -1],
  // Long what rose in the 24h before as_of, short what fell
  ["24h momentum", (row) => (row.entryPrice > row.daily.at(-1)[1] ? 1 : -1)],
];

// Two-sided exact McNemar test: of the calls exactly one of the two got right, does either win more than chance?
// Binomial(n, 1/2) terms in log space, since 2^n overflows at a few hundred calls.
function mcnemar(onlyA, onlyB) {
  const n = onlyA + onlyB;
  let logTerm = -n * Math.LN2;
  let tail = 0;
  for (let k = 0; k <= Math.min(onlyA, onlyB); k++) {
    tail += Math.exp(logTerm);
    logTerm += Math.log((n - k) / (k + 1));
  }
  return Math.min(1, 2 * tail);
}

// One strategy over every day. tStat is the mean daily net over its standard error: beyond about ±2 is unlikely luck.
function totals(name, sideOf, rows, daily, richDaily) {
  const trades = rows.map((row) => trade(row, sideOf(row)));
  const total = (key) => sum(trades.map((t) => t[key]));
  const mean = sum(daily) / daily.length;
  const sd = Math.sqrt(sum(daily.map((x) => (x - mean) ** 2)) / (daily.length - 1));
  return {
    strategy: name,
    longs: rows.filter((row) => sideOf(row) === 1).length,
    shorts: rows.filter((row) => sideOf(row) === -1).length,
    accuracy: pct(rows.filter((row) => (sideOf(row) === 1) === row.up).length / rows.length),
    pricePnl: +total("pricePnl").toFixed(2),
    fees: +(-total("fees")).toFixed(2),
    funding: +total("funding").toFixed(2),
    net: +total("net").toFixed(2),
    profitableDays: `${daily.filter((x) => x > 0).length}/${daily.length}`,
    tStat: fixed(mean / (sd / Math.sqrt(daily.length)), 2),
    richAhead: name === STRATEGIES[0][0] ? "" : `${richDaily.filter((x, i) => x > daily[i]).length}/${daily.length} days`,
  };
}

// How often coins went up for each band of P(up): a calibrated model's up rate tracks its P(up)
function calibration(rows, scoreOf, bins = 5) {
  const table = [];
  for (let i = 0; i < bins; i++) {
    const inBin = rows.filter((row) => Math.min(Math.floor(scoreOf(row) * bins), bins - 1) === i);
    if (inBin.length === 0) continue;
    table.push({
      pUp: `${(i / bins).toFixed(1)}-${((i + 1) / bins).toFixed(1)}`,
      n: inBin.length,
      meanPUp: +(sum(inBin.map(scoreOf)) / inBin.length).toFixed(3),
      actualUpRate: +(inBin.filter((row) => row.up).length / inBin.length).toFixed(3),
    });
  }
  return table;
}

// ---- run --------------------------------------------------------------------

async function run(days) {
  const files = (await fs.readdir(OUT_DIR).catch(() => []))
    .filter((file) => /^dataset-\d{4}-\d{2}-\d{2}\.json\.gz$/.test(file))
    .sort()
    .slice(-days);
  if (files.length === 0) throw new Error("no datasets in data/futuresRichContext, run with --build first");
  const compact = new Map(
    JSON.parse(await fs.readFile(COMPACT_RESULTS, "utf8")).results.map((row) => [`${row.date} ${row.symbol}`, row.score]),
  );
  const items = [];
  for (const file of files) {
    const { items: dayItems } = JSON.parse(zlib.gunzipSync(await fs.readFile(path.join(OUT_DIR, file))));
    const date = file.slice(8, 18);
    items.push(...dayItems.map((item) => ({ ...item, date, compactScore: compact.get(`${date} ${item.symbol}`) })));
  }
  console.log(`asking SubQ about ${items.length} coin-days over ${files.length} days, ~250k tokens each`);
  const rows = await scoreItems(items, buildRichState, "price_up", QUESTION);

  const failed = rows.filter((row) => row.error);
  const paired = rows.filter((row) => row.score !== undefined && row.compactScore !== undefined);
  if (paired.length === 0) throw new Error("No rich call paired with a compact one");
  const asOutcome = (scoreOf) => paired.map((row) => ({ ...row, score: scoreOf(row), outcome: row.up }));
  const rich = summarize(asOutcome((row) => row.score));
  const compactSummary = summarize(asOutcome((row) => row.compactScore));
  const right = (score, row) => (score >= 0.5) === row.up;
  const onlyRich = paired.filter((row) => right(row.score, row) && !right(row.compactScore, row)).length;
  const onlyCompact = paired.filter((row) => !right(row.score, row) && right(row.compactScore, row)).length;
  const dates = [...new Set(paired.map((row) => row.date))].sort();

  console.log(`\nSubQ ${SUBQ_MODEL}, ${paired.length} coin-days over ${dates.length} days (${dates[0]} → ${dates.at(-1)}), ${failed.length} failed`);
  console.log(`  ${rich.yes} up / ${rich.no} down                         rich          compact`);
  console.log(`  direction accuracy                       ${pct(rich.accuracy).padEnd(14)}${pct(compactSummary.accuracy)}`);
  console.log(`  AUC (0.5 = no skill)                     ${fixed(rich.auc, 3).padEnd(14)}${fixed(compactSummary.auc, 3)}`);
  console.log(`  Brier (base rate ${fixed(rich.brierBaseline, 4)})                ${fixed(rich.brier, 4).padEnd(14)}${fixed(compactSummary.brier, 4)}`);
  console.log(`  mean P(up) when up | down                ${fixed(rich.meanScoreYes, 3)} | ${fixed(rich.meanScoreNo, 3)}   ${fixed(compactSummary.meanScoreYes, 3)} | ${fixed(compactSummary.meanScoreNo, 3)}`);
  const mcnemarP = mcnemar(onlyRich, onlyCompact);
  console.log(`  right only with rich context: ${onlyRich}, only with compact: ${onlyCompact} (McNemar p = ${fixed(mcnemarP, 3)})\n`);
  const richCalibration = calibration(paired, (row) => row.score);
  console.table(richCalibration);

  const byDay = dates.map((date) => paired.filter((row) => row.date === date));
  const daily = STRATEGIES.map(([, sideOf]) => byDay.map((dayRows) => sum(dayRows.map((row) => trade(row, sideOf(row)).net))));
  const perDay = dates.map((date, i) => ({
    date,
    n: byDay[i].length,
    up: byDay[i].filter((row) => row.up).length,
    ...Object.fromEntries(STRATEGIES.map(([name], k) => [name, +daily[k][i].toFixed(2)])),
  }));
  console.log(`\nnet USDT per day, $${NOTIONAL} per coin at 1x, after fees (taker ${TAKER_FEE * 100}% each way) and funding:`);
  console.table(perDay);
  const strategyTotals = STRATEGIES.map(([name, sideOf], k) => totals(name, sideOf, paired, daily[k], daily[0]));
  console.log(`\nall ${dates.length} days:`);
  console.table(strategyTotals);
  if (failed.length) console.log(`failed (first 3): ${failed.slice(0, 3).map((row) => `${row.date} ${row.symbol}: ${row.error}`).join(" | ")}`);

  const results = rows.map((row) => {
    const { date, symbol, delistedAt, score, compactScore, up, entryPrice, exitPrice, error } = row;
    const stateChars = JSON.stringify(buildRichState(row)).length;
    if (error) return { date, symbol, delistedAt, compactScore, up, entryPrice, exitPrice, stateChars, error };
    const side = sideFor(score);
    return { date, symbol, delistedAt, score, compactScore, side: side === 1 ? "long" : "short", up, entryPrice, exitPrice, stateChars, ...trade(row, side) };
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
    pairedWith: path.basename(COMPACT_RESULTS),
    summary: { rich, compact: compactSummary, rightOnlyRich: onlyRich, rightOnlyCompact: onlyCompact, mcnemarP },
    calibration: richCalibration,
    perDay,
    totals: strategyTotals,
    results,
  };
  await fs.writeFile(path.join(OUT_DIR, resultsFile), `${JSON.stringify(output, null, 2)}\n`);
  console.log(`per-coin results saved to data/futuresRichContext/${resultsFile}`);
}

async function main() {
  const args = process.argv.slice(2);
  const daysAt = args.indexOf("--days");
  const days = daysAt === -1 ? Infinity : Number(args[daysAt + 1]);
  if (args.includes("--build")) return build(days);
  return run(days);
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err.message);
    process.exit(1);
  });
}

module.exports = { buildState, buildRichState, QUESTION, trade };
