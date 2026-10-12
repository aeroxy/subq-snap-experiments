/**
 * Scores SubQ systemone on prediction-market events whose outcome is already known.
 *
 *   node scripts/testSubQPrediction.js --build                    refresh data/predictionDataGoldenSet.json
 *   API_KEY=... node scripts/testSubQPrediction.js [--limit N]    score SubQ on the golden set
 *
 * Binance's public prediction API only serves active events (resolved ones disappear and
 * their markets are stripped), so the golden set comes from Polymarket's public Gamma API.
 */
const fs = require("fs/promises");
const path = require("path");

const DATA_DIR = path.join(__dirname, "..", "data");
const GOLDEN_SET_PATH = path.join(DATA_DIR, "predictionDataGoldenSet.json");

const SUBQ_URL = process.env.SUBQ_URL || "https://api.subq.ai/v1/systemone";
const SUBQ_MODEL = "subq-snap-preview";
const CONCURRENCY = 4;
const MAX_RETRIES = 3;
const FATAL_STATUSES = [401, 402, 403]; // bad key, no credits, model not allowed: retrying can't help

const GAMMA_URL = "https://gamma-api.polymarket.com/events";
const WINDOW_DAYS = 14; // outcomes from the last two weeks only, so they can't be in SubQ's training data
const MIN_EVENT_VOLUME = 20000;
const MIN_MARKET_VOLUME = 10000;
const PAGE_SIZE = 20;
const MAX_PAGES = 3;
const TARGET_ITEMS = 100;
const RULES_MAX_CHARS = 2000;
// One pool per Polymarket tag. An event belongs to the first pool that qualifies it.
const POOLS = [
  { tag: "economy", category: "Economy" },
  { tag: "finance", category: "Finance" },
  { tag: "business", category: "Business" },
  { tag: "tech", category: "Tech" },
  { tag: "ai", category: "AI" },
  { tag: "awards", category: "Awards" },
  { tag: "world", category: "World" },
  { tag: "geopolitics", category: "Geopolitics" },
  { tag: "politics", category: "Politics" },
  // Props like "Both Teams to Score in Q1" are noise, so keep only match winners and draws
  { tag: "sports", category: "Sports", question: /^Will .+ (win|end in a draw)\b/i },
];
// Recurring and price-level markets can't be answered from the text alone
const SKIP_TAGS = new Set(["Recurring", "Hide From New", "Tweet Markets", "Up or Down", "Hit Price", "Crypto Prices"]);
const SKIP_TITLE = /\b(price|hit|above|below|FDV|market cap)\b/i;

const QUESTION = {
  type: "noul",
  instructions:
    'Given state.question, state.rules and state.as_of, will this market resolve to "Yes" by state.resolution_deadline?',
  criteria: {
    true: 'The event in state.question happens as defined by state.rules, so the market resolves to "Yes"',
    false: 'The event in state.question does not happen as defined by state.rules, so the market resolves to "No"',
  },
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const day = (iso) => String(iso).slice(0, 10);
const parseList = (value) => (typeof value === "string" ? JSON.parse(value) : value);
const byVolumeDesc = (a, b) => Number(b.volumeNum) - Number(a.volumeNum);

// ---- golden set -------------------------------------------------------------

const deadlineOf = (market, event) => market.endDate || event.endDate;

// true = resolved Yes, false = resolved No, null = not a clean, liquid Yes/No resolution in the window.
// The deadline must be in the window too: "by September 15" may only be finalized on September 30,
// but it was decided on the 15th, which SubQ could already have seen.
function resolvedOutcome(market, event, since) {
  if (market.umaResolutionStatus !== "resolved" || !market.closed) return null;
  const deadline = deadlineOf(market, event);
  if (!deadline || day(deadline) < since || day(market.closedTime) < since) return null;
  if (Number(market.volumeNum) < MIN_MARKET_VOLUME) return null;
  if (JSON.stringify(parseList(market.outcomes)) !== '["Yes","No"]') return null;
  const prices = JSON.stringify(parseList(market.outcomePrices));
  if (prices === '["1","0"]') return true;
  if (prices === '["0","1"]') return false;
  return null;
}

// The winning market plus the highest-volume losing one: a plausible contender, not a long shot
function pickPair(event, pool, since) {
  const best = { true: null, false: null };
  for (const market of event.markets || []) {
    if (pool.question && !pool.question.test(market.question)) continue;
    const outcome = resolvedOutcome(market, event, since);
    if (outcome !== null && (!best[outcome] || byVolumeDesc(market, best[outcome]) < 0)) best[outcome] = market;
  }
  return [true, false]
    .filter((outcome) => best[outcome])
    .map((outcome) => ({
      id: best[outcome].id,
      category: pool.category,
      eventTitle: event.title,
      question: best[outcome].question,
      rules: String(best[outcome].description || "").slice(0, RULES_MAX_CHARS),
      asOf: day(best[outcome].startDate || best[outcome].createdAt),
      endDate: day(deadlineOf(best[outcome], event)),
      outcome,
      resolvedAt: best[outcome].closedTime,
      eventSlug: event.slug,
      marketSlug: best[outcome].slug,
    }));
}

// Same template ("Largest Company end of July?" / "2nd Largest Company end of August?"), same
// series ("Brazil Election: Lula Vote Share" / "Brazil Election: 4th Place") or same match
// ("X vs. Y" / "X vs. Y - Exact Score"): one event per family keeps items independent
const titleFamily = (title) =>
  title
    .split(/\s-\s|:/)[0]
    .toLowerCase()
    .replace(/\b(january|february|march|april|may|june|july|august|september|october|november|december|sept?|jan|feb|mar|apr|jun|jul|aug|oct|nov|dec)\b|\b\d+(st|nd|rd|th)?\b/g, "")
    .replace(/[^a-z]+/g, " ")
    .trim();

// Round-robin over the pools, one event (winner + strongest loser) per turn, so categories stay balanced
function selectItems(eventsByPool, since) {
  const seenFamilies = new Set();
  const queues = POOLS.map((pool) => {
    const queue = [];
    for (const event of eventsByPool[pool.tag] || []) {
      const family = titleFamily(event.title);
      if (seenFamilies.has(family) || SKIP_TITLE.test(event.title)) continue;
      if ((event.tags || []).some((tag) => SKIP_TAGS.has(tag.label))) continue;
      const pair = pickPair(event, pool, since);
      if (pair.length === 0) continue;
      seenFamilies.add(family);
      queue.push(pair);
    }
    return queue;
  });

  const items = [];
  for (let turn = 0; items.length < TARGET_ITEMS && queues.some((queue) => queue.length > turn); turn++) {
    for (const queue of queues) if (queue[turn]) items.push(...queue[turn]);
  }
  return items.slice(0, TARGET_ITEMS);
}

// Newest resolutions first, stopping once a page reaches events resolved before the window
async function fetchPool(tag, since) {
  const events = [];
  for (let page = 0; page < MAX_PAGES; page++) {
    const query = new URLSearchParams({
      closed: "true",
      order: "closedTime",
      ascending: "false",
      volume_min: String(MIN_EVENT_VOLUME),
      limit: String(PAGE_SIZE),
      offset: String(page * PAGE_SIZE),
      tag_slug: tag,
    });
    const res = await fetch(`${GAMMA_URL}?${query}`, { signal: AbortSignal.timeout(120000) });
    if (!res.ok) throw new Error(`Gamma ${tag}: HTTP ${res.status}`);
    const batch = await res.json();
    events.push(...batch);
    if (batch.length < PAGE_SIZE || day(batch[batch.length - 1].closedTime) < since) break;
  }
  return events;
}

async function build() {
  const now = new Date();
  const since = day(new Date(now.getTime() - WINDOW_DAYS * 86400000).toISOString());
  const eventsByPool = {};
  for (const pool of POOLS) {
    eventsByPool[pool.tag] = await fetchPool(pool.tag, since);
    console.log(`fetched ${eventsByPool[pool.tag].length} ${pool.tag} events`);
  }

  const items = selectItems(eventsByPool, since);
  const meta = {
    source: "polymarket-gamma",
    builtAt: now.toISOString(),
    resolvedSince: since,
    yes: items.filter((item) => item.outcome).length,
    no: items.filter((item) => !item.outcome).length,
  };
  await fs.mkdir(path.dirname(GOLDEN_SET_PATH), { recursive: true });
  await fs.writeFile(GOLDEN_SET_PATH, `${JSON.stringify({ meta, items }, null, 2)}\n`);
  console.log(`wrote ${items.length} items (${meta.yes} Yes / ${meta.no} No) to data/predictionDataGoldenSet.json`);
}

// ---- SubQ -------------------------------------------------------------------

// Only pre-resolution facts: no outcome, prices or volume
const buildState = (item) => ({
  question: item.question,
  ...(item.eventTitle !== item.question && { event: item.eventTitle }),
  category: item.category,
  rules: item.rules,
  as_of: item.asOf,
  resolution_deadline: item.endDate,
});

class HttpError extends Error {
  constructor(status, detail) {
    super(`HTTP ${status}: ${detail}`);
    this.status = status;
  }
}

// Asks one noul question and returns its P(yes)
async function askSubQ(state, name, question) {
  const body = JSON.stringify({ model: SUBQ_MODEL, state, questions: { [name]: question } });
  for (let attempt = 0; ; attempt++) {
    try {
      const res = await fetch(SUBQ_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${process.env.API_KEY}` },
        body,
        signal: AbortSignal.timeout(60000),
      });
      if (!res.ok) throw new HttpError(res.status, (await res.text()).slice(0, 300));
      const json = await res.json();
      const score = json.answers?.[name]?.noul;
      if (typeof score !== "number") throw new HttpError(res.status, `unexpected response ${JSON.stringify(json).slice(0, 200)}`);
      return score;
    } catch (err) {
      const retryable = !(err instanceof HttpError) || err.status === 429 || err.status >= 500;
      if (!retryable || attempt >= MAX_RETRIES) throw err;
      await sleep(1000 * 2 ** attempt);
    }
  }
}

// Asks the question about every item, CONCURRENCY at a time. Each row gets `score` (P(yes)) or `error`.
async function scoreItems(items, toState, name, question) {
  if (!process.env.API_KEY) throw new Error("Set the API_KEY environment variable");
  const rows = items.map((item) => ({ ...item, error: "not run" }));
  let done = 0;
  let fatal = null;
  const score = async (i) => {
    try {
      rows[i] = { ...items[i], score: await askSubQ(toState(items[i]), name, question) };
    } catch (err) {
      rows[i] = { ...items[i], error: err.message };
      if (FATAL_STATUSES.includes(err.status)) fatal = err;
    }
    process.stdout.write(`\r${++done}/${items.length}`);
  };

  // The first call goes alone: a bad key or payload would otherwise fail every call
  await score(0);
  if (rows[0].error) throw new Error(`First SubQ call failed, aborting: ${rows[0].error}`);
  let next = 1;
  await Promise.all(
    Array.from({ length: CONCURRENCY }, async () => {
      while (next < items.length && !fatal) await score(next++);
    }),
  );
  process.stdout.write("\n");
  if (fatal) console.log(`stopped early: ${fatal.message}`);
  return rows;
}

// ---- metrics ----------------------------------------------------------------

const mean = (values) => values.reduce((sum, value) => sum + value, 0) / values.length;

// NaN means "undefined for this group", e.g. AUC needs at least one Yes and one No item
const fixed = (x, digits) => (Number.isNaN(x) ? "n/a" : x.toFixed(digits));
const cell = (x, digits) => (Number.isNaN(x) ? "n/a" : +x.toFixed(digits));
const pct = (x) => (Number.isNaN(x) ? "n/a" : `${(x * 100).toFixed(1)}%`);

// Probability that a random Yes item scores above a random No item (0.5 = no skill)
function auc(rows) {
  const yes = rows.filter((row) => row.outcome);
  const no = rows.filter((row) => !row.outcome);
  if (yes.length === 0 || no.length === 0) return NaN;
  let wins = 0;
  for (const y of yes) for (const n of no) wins += y.score > n.score ? 1 : y.score === n.score ? 0.5 : 0;
  return wins / (yes.length * no.length);
}

function summarize(rows) {
  const yes = rows.filter((row) => row.outcome);
  const no = rows.filter((row) => !row.outcome);
  const correct = (row) => Number((row.score >= 0.5) === row.outcome);
  const baseRate = yes.length / rows.length;
  const clip = (p) => Math.min(Math.max(p, 1e-6), 1 - 1e-6);
  return {
    n: rows.length,
    yes: yes.length,
    no: no.length,
    accuracy: mean(rows.map(correct)),
    majorityBaseline: Math.max(baseRate, 1 - baseRate),
    balancedAccuracy: yes.length && no.length ? (mean(yes.map(correct)) + mean(no.map(correct))) / 2 : NaN,
    auc: auc(rows),
    brier: mean(rows.map((row) => (row.score - Number(row.outcome)) ** 2)),
    brierBaseline: baseRate * (1 - baseRate),
    logLoss: mean(rows.map((row) => -Math.log(row.outcome ? clip(row.score) : 1 - clip(row.score)))),
    meanScoreYes: yes.length ? mean(yes.map((row) => row.score)) : NaN,
    meanScoreNo: no.length ? mean(no.map((row) => row.score)) : NaN,
  };
}

function calibration(rows, bins = 5) {
  const table = [];
  for (let i = 0; i < bins; i++) {
    const inBin = rows.filter((row) => Math.min(Math.floor(row.score * bins), bins - 1) === i);
    if (inBin.length === 0) continue;
    table.push({
      scoreRange: `${(i / bins).toFixed(1)}-${((i + 1) / bins).toFixed(1)}`,
      n: inBin.length,
      meanScore: +mean(inBin.map((row) => row.score)).toFixed(3),
      actualYesRate: +mean(inBin.map((row) => Number(row.outcome))).toFixed(3),
    });
  }
  return table;
}

function byCategory(rows) {
  return [...new Set(rows.map((row) => row.category))].map((category) => {
    const subset = rows.filter((row) => row.category === category);
    const s = summarize(subset);
    return { category, n: s.n, yes: s.yes, accuracy: cell(s.accuracy, 3), auc: cell(s.auc, 3) };
  });
}

// ---- run --------------------------------------------------------------------

async function run(limit) {
  const raw = await fs.readFile(GOLDEN_SET_PATH, "utf8").catch(() => {
    throw new Error("data/predictionDataGoldenSet.json not found, run with --build first");
  });
  const items = JSON.parse(raw).items.slice(0, limit);
  const rows = await scoreItems(items, buildState, "resolves_yes", QUESTION);

  const scored = rows.filter((row) => row.score !== undefined);
  const failed = rows.filter((row) => row.error);
  if (scored.length === 0) throw new Error("Every SubQ call failed");
  const s = summarize(scored);

  console.log(`\nSubQ ${SUBQ_MODEL} on ${s.n} resolved events: ${s.yes} Yes / ${s.no} No, ${failed.length} failed`);
  console.log(`  accuracy @0.5      ${pct(s.accuracy)}   (always-majority baseline ${pct(s.majorityBaseline)})`);
  console.log(`  balanced accuracy  ${pct(s.balancedAccuracy)}`);
  console.log(`  AUC                ${fixed(s.auc, 3)}   (0.5 = no skill)`);
  console.log(`  Brier              ${fixed(s.brier, 4)}   (base-rate baseline ${fixed(s.brierBaseline, 4)})`);
  console.log(`  log loss           ${fixed(s.logLoss, 4)}`);
  console.log(`  mean score         Yes ${fixed(s.meanScoreYes, 3)} | No ${fixed(s.meanScoreNo, 3)}\n`);
  console.table(calibration(scored));
  console.table(byCategory(scored));
  if (failed.length) console.log(`failed (first 3): ${failed.slice(0, 3).map((row) => `${row.id}: ${row.error}`).join(" | ")}`);

  const results = rows.map(({ id, category, question, outcome, resolvedAt, score, error }) => ({
    id,
    category,
    question,
    outcome,
    resolvedAt,
    score,
    error,
  }));
  // One file per run, stamped with Date.now(), so a rerun never overwrites an earlier one
  const ranAt = Date.now();
  const resultsFile = `predictionSubQResults-${ranAt}.json`;
  const output = { ranAt: new Date(ranAt).toISOString(), model: SUBQ_MODEL, summary: s, results };
  await fs.writeFile(path.join(DATA_DIR, resultsFile), `${JSON.stringify(output, null, 2)}\n`);
  console.log(`per-item results saved to data/${resultsFile}`);
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

module.exports = { POOLS, selectItems, buildState, QUESTION, SUBQ_MODEL, scoreItems, summarize, fixed, pct };
