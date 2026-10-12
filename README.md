# subq-snap-experiments

Can SubQ Snap (`subq-snap-preview`, via the [Decisions API](https://docs.subq.ai/endpoints/decisions-api/)) call things that haven't happened yet? These are the scripts, datasets and per-call results of five experiments run in October 2026. Every trade is simulated.

## Experiments

| Script | Question asked | Data | Result |
|---|---|---|---|
| `testSubQPrediction.js` | Will this Polymarket market resolve Yes? | 100 events resolved 2026-09-27 to 2026-10-11 (54 Yes, 46 No) | 47-48% accuracy over three runs (always answering "Yes": 54%), AUC 0.50-0.51. SubQ answered "No" to 94-95 of the 100 |
| `testSubQFuturesPrediction.js` | Will a Binance USDT perpetual be higher at 23:30 SGT tomorrow? One day | 100 most-traded perps, 2026-10-10 to 10-11 | 60% accuracy, +$55.70 on $10,000 (always long: +$103.43) |
| `testSubQFuturesBackfill.js` | The same, over 28 days | 2,800 coin-days, 2026-09-14 to 10-11 | 50.0% accuracy, AUC 0.501, net -$1,029 (always long: +$1,678, perfect foresight: +$15,427) |
| `testSubQFuturesRichContext.js` | Does ~220k tokens of context (a year of candles, order-book depth, a month of open interest) beat ~6k? | 25 coins a day, the same 28 days | **Partial: 379 of 700 calls**, credits ran out. 48.8% accuracy vs 50.7% with the compact state on the same coin-days (McNemar p = 0.48) |
| `testSubQFuturesNextMinute.js` | Will the price be higher one minute later? | 100 random minutes, 2026-10-01 to 10-10, top-20 coins | 49% accuracy, AUC 0.517, net -$10.12 (perfect foresight: +$2.11, because the median 1-minute move is smaller than the round-trip fee) |

Futures trades: $100 per coin at 1x, long if SubQ's P(up) is at least 0.5 and short otherwise, market orders in and out, 0.05% taker fee each way, funding as settled. SubQ only ever sees data up to the window's start; the exit price and funding after it are kept to score the trade. Baselines are always long, always short and 24h momentum (next-minute: 1-minute momentum and order-book imbalance).

## Reproduce

Node.js 20.6 or newer (built-in `fetch` and `--env-file`), no dependencies. Put your SubQ key in a `.env` file (gitignored):

```
API_KEY=...
```

```
node --env-file=.env scripts/testSubQPrediction.js
node --env-file=.env scripts/testSubQFuturesPrediction.js          # newest futuresDataGoldenSet-*.json
node --env-file=.env scripts/testSubQFuturesBackfill.js [--days N]
node --env-file=.env scripts/testSubQFuturesRichContext.js [--days N]
node --env-file=.env scripts/testSubQFuturesNextMinute.js [--limit N]
```

Every run saves a new `<name>-<Date.now()>.json` in `data/`, so nothing is overwritten. `--build` makes a dataset from public data (Binance, Polymarket) and prints what it wrote; the Polymarket script's `--build` replaces `predictionDataGoldenSet.json`. Binance's API keeps only 30 days of open interest and long/short history, so the older backfill days can't be rebuilt: the files in `data/` are the record.

The question, state and trade logic of each script were fixed before the results in `data/` were produced. The only later edits to older scripts moved the request loop into a shared function (`testSubQPrediction.js`) and changed output file naming (`testSubQFuturesPrediction.js`); new experiments are new scripts.

## Layout

```
scripts/                          one file per experiment
data/predictionDataGoldenSet.json   Polymarket events and outcomes
data/predictionSubQResults{1,2,3}.json
data/futuresDataGoldenSet-*.json    the one-day dataset
data/futuresSubQResults-*.json      its six runs
data/futuresBackfill/               28 daily datasets and the 28-day results
data/futuresRichContext/            28 gzipped datasets and the partial results
data/futuresNextMinute/             the 100-minute dataset and results
```

In the futures datasets, `exitPrice`, `up` and `fundingInWindow` are the outcome and are never sent to SubQ. In the results files, `score` is SubQ's P(yes) or P(up).

## Caveats

- One 28-day window. Coins move together, so 2,800 coin-days are far fewer independent observations.
- SubQ is close to deterministic: 89 of 100 single-day answers were identical across five runs, and 97 of 100 Polymarket scores between two runs. Reruns of the same dataset are not independent evidence.
- The rich-context run stopped with `402 insufficient_credits` after 379 of 700 calls (2026-09-14 to 09-29). The other 321 rows carry an `error`.
- Binance's depth dump has a gap from 2026-10-07 02:49 to 2026-10-09 04:05 UTC. Depth is missing for 17 of the 100 next-minute points and for the rich-context days 2026-10-08 and 2026-10-09 (not yet scored).
- The backfill ranking includes coins delisted later (3 coin-days, marked `delistedAt`) to avoid survivorship bias.
- Simulated trades: no slippage, no rounding to Binance's order step sizes, VIP 0 taker fees without a BNB discount.
- Not financial advice.
