# War stories: what real public data actually did to this pipeline

Every incident below happened during development, on real data, and is reproducible
from the repo. This page exists because "handles messy data" is an empty claim until
you can show the mess.

## The day CBK published everything backwards (2017-03-28)

The silver layer enforces a rule any FX desk would consider obvious: `buy_rate <=
sell_rate` (a bank buys currency for less than it sells it). On 2017-03-28 that rule
quarantined **22 rows — every currency CBK published that day**. Buy and sell were
swapped across the board, and the KES cross-rates were inverted too (`KES/USHS` at
0.0285 instead of ~33).

A pipeline without the rule would have loaded a full day of plausible-looking,
systematically wrong rates. The quarantine keeps the day visible with its original
payloads instead of guessing which column is which.

## Proving DD/MM instead of assuming it

The CBK historical CSV mixes two date formats in one column: 37,382 ISO rows
(`2016-10-11`) and 9 slash rows (`11/10/2016`). Is `11/10/2016` October 11th or
November 10th? Nothing on the site says.

Instead of assuming, we joined the file against itself: the slash rows carry rate
values **identical** to ISO rows for `2016-10-11` (all five columns, all nine rows).
That's only consistent with day-first. The proof lives as a comment in
[`silver/cbk_fx.py`](../src/mizani/silver/cbk_fx.py) and as a regression test.

## The row from 2038

One row in the historical file is dated `2038-01-22` — perfectly parseable, completely
wrong. It sails through any type-based validation. The range check
(`rate_date <= today`) is what catches it. Type safety is not data quality.

## A CSV with a header fragment in its belly

The file has no header row — except one mangled fragment (`Date,US DOLLAR,STG POUND`)
buried mid-file, presumably from a bad export concatenation on CBK's side. It parses
as a row where the "mean rate" is the string `STG POUND`. Quarantined with reason
`rate_date: not_nullable; mean_rate: not_nullable`.

## When the source republishes history with different numbers

210 rows share a (date, currency) key with another row but disagree on the values —
CBK re-issued revised rates without marking them. Most pipelines silently keep
"first" or "last" and call it deduplication. Mizani quarantines **both** versions:
picking one would be inventing a fact. The conflict is a property of the source, and
the pipeline's job is to surface it, not to hide it.

## The internet is not reliable, part 1: local

During a verification re-run, the CBK website dropped a TLS handshake mid-connection
(`SSLEOFError`). The run logged `status='error'` for that source, landed the other
three, and exited non-zero. This is the degrade-loudly contract working: one flaky
government website must not block three healthy sources, but the failure must be
impossible to miss.

## The internet is not reliable, part 2: in production

The very first scheduled publish run from GitHub Actions failed — the World Bank API
took longer than 60 seconds to answer from the runner. The red badge and blocked
publish were correct behavior; the fix was HTTP-layer retries (4 attempts, exponential
backoff) so *every* entry point gets resilience, not just the Dagster path. The
failing run and the fix are both in the history: commit
[`b31dbb4`](https://github.com/Lucas-Maingi/mizani/commit/b31dbb4).

## Two sources, one truth (the reconciliation)

After all the parsing — three date formats, two quote conventions, per-100-unit JPY
quotes — how do you know the numbers are *right*? By checking them against someone
else's: CBK's daily USD averages vs the World Bank's independently published annual
figure agree within **0.09% for every full-coverage year (2017–2023)**. That check now
runs as a dbt test with a 1% tolerance; if either source drifts or a parser regresses,
the build fails.

## The freshness gap we ship anyway

CBK's downloadable historical file currently ends at **2024-01-03**; the current daily
rates exist only inside a JS-driven table whose AJAX endpoint requires an
undocumented session nonce. Rather than ship a brittle scrape of it, the published
data feed labels every FX value with its `as_of` date and consumers are required to
read it. An honest stale date beats a fragile fresh one.
