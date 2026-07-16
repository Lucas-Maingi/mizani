"""Generate the public static site (dashboard + data-quality report) from gold.

Every number on the site is queried from the warehouse at build time —
nothing is hardcoded. Output: site/ (index.html, quality.html), ready for
GitHub Pages. The dbt docs static page is copied in separately by the
workflow.

    python scripts/build_site.py [db_path] [out_dir]
"""

import base64
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

GREEN, ORANGE, GREY = "#0a7d5c", "#d06010", "#555"

CSS = """
:root { color-scheme: light dark; }
* { box-sizing: border-box; }
body { font-family: -apple-system, "Segoe UI", Roboto, sans-serif; margin: 0;
       background: #fafaf8; color: #1c1c1a; }
@media (prefers-color-scheme: dark) {
  body { background: #16181a; color: #e8e6e3; }
  .card, .stat { background: #1f2225 !important; border-color: #33373b !important; }
  a { color: #6fc7a6; }
}
header { padding: 2.2rem 1.5rem 1.4rem; max-width: 960px; margin: 0 auto; }
h1 { margin: 0 0 .3rem; font-size: 1.6rem; }
h2 { font-size: 1.15rem; margin: 2rem 0 .6rem; }
.sub { color: #777; font-size: .92rem; }
main { max-width: 960px; margin: 0 auto; padding: 0 1.5rem 3rem; }
nav a { margin-right: 1.2rem; font-size: .95rem; }
.stats { display: flex; gap: .8rem; flex-wrap: wrap; margin: 1rem 0; }
.stat { background: #fff; border: 1px solid #e4e2dd; border-radius: 10px;
        padding: .9rem 1.1rem; min-width: 180px; flex: 1; }
.stat b { display: block; font-size: 1.45rem; margin-bottom: .15rem; }
.stat span { font-size: .82rem; color: #888; }
.card { background: #fff; border: 1px solid #e4e2dd; border-radius: 10px;
        padding: 1rem; margin: 1rem 0; overflow-x: auto; }
.card img { max-width: 100%; height: auto; }
table { border-collapse: collapse; width: 100%; font-size: .88rem; }
th, td { text-align: left; padding: .45rem .6rem; border-bottom: 1px solid #8884; }
code { font-size: .85em; background: #8882; padding: .1em .35em; border-radius: 4px; }
footer { max-width: 960px; margin: 0 auto; padding: 1rem 1.5rem 2.5rem;
         color: #888; font-size: .85rem; }
.ok { color: #0a7d5c; font-weight: 600; }
.bad { color: #b3261e; font-weight: 600; }
"""


def fig_to_img(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    b64 = base64.b64encode(buf.getvalue()).decode()
    return f'<img src="data:image/png;base64,{b64}" alt="chart">'


def page(title: str, body: str, built_at: str) -> str:
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title><style>{CSS}</style></head>
<body>
<header>
<h1>Mizani <span class="sub">— East African economic &amp; mobile-money data platform</span></h1>
<p class="sub">Medallion ETL · Dagster · dbt · DuckDB — rebuilt from live sources by a
scheduled pipeline. Every figure on this page was queried from the warehouse at build
time.</p>
<nav><a href="index.html">Dashboard</a> <a href="quality.html">Data quality</a>
<a href="dbt/index.html">dbt docs &amp; lineage</a>
<a href="https://github.com/Lucas-Maingi/mizani">GitHub</a></nav>
</header>
<main>{body}</main>
<footer>Built {built_at} · Sources: Central Bank of Kenya, World Bank, GSMA ·
<a href="https://github.com/Lucas-Maingi/mizani">Lucas-Maingi/mizani</a></footer>
</body></html>"""


def build_feed(con: duckdb.DuckDBPyConnection) -> dict:
    """Machine-readable snapshot of gold, published at data/latest.json.

    Every value carries its own as_of date because the sources have very
    different cadences (the CBK historical FX file currently ends
    2024-01-03; mobile-money statistics run to the previous month).
    Consumers must read as_of, not assume freshness.
    """
    fx_date, usd_mean, usd_buy, usd_sell = con.execute("""
        select d.calendar_date, f.kes_per_unit_mean, f.kes_per_unit_buy, f.kes_per_unit_sell
        from gold.fact_exchange_rate f join gold.dim_date d using (date_key)
        where f.currency_code = 'USD' order by d.calendar_date desc limit 1
    """).fetchone()
    regional = {
        code: {"units_per_kes": round(1.0 / rate, 6), "as_of": str(day)}
        for code, rate, day in con.execute("""
            select f.currency_code, f.kes_per_unit_mean, d.calendar_date
            from gold.fact_exchange_rate f join gold.dim_date d using (date_key)
            where f.currency_code in ('TZS','UGX','RWF','BIF')
            qualify row_number() over (partition by f.currency_code
                                       order by d.calendar_date desc) = 1
        """).fetchall()
    }
    mm_date, accounts_m, agents, vol_m, val_b = con.execute("""
        select d.calendar_date, m.registered_accounts_millions, m.active_agents,
               m.agent_cico_volume_million, m.agent_cico_value_ksh_billions
        from gold.fact_mobile_money m join gold.dim_date d using (date_key)
        order by d.calendar_date desc limit 1
    """).fetchone()
    trend = con.execute("""
        select d.calendar_date, m.agent_cico_value_ksh_billions
        from gold.fact_mobile_money m join gold.dim_date d using (date_key)
        order by d.calendar_date desc limit 12
    """).fetchall()
    ownership = {
        iso3: {"pct_adults_15plus": round(v, 1), "as_of_year": int(y)}
        for iso3, v, y in con.execute("""
            select country_iso3, value, year from gold.fact_worldbank_indicator
            where indicator_code = 'FX.OWN.TOTL.ZS'
            qualify row_number() over (partition by country_iso3 order by year desc) = 1
        """).fetchall()
    }
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": "https://github.com/Lucas-Maingi/mizani",
        "license_note": "Aggregated from CBK, World Bank, GSMA public data.",
        "fx": {
            "kes_per_usd": {
                "mean": round(usd_mean, 4),
                "buy": round(usd_buy, 4),
                "sell": round(usd_sell, 4),
                "as_of": str(fx_date),
            },
            "regional_units_per_kes": regional,
        },
        "kenya_mobile_money": {
            "as_of_month": str(mm_date),
            "registered_accounts_millions": round(accounts_m, 2),
            "active_agents": int(agents),
            "agent_cico_volume_million": round(vol_m, 2),
            "agent_cico_value_ksh_billions": round(val_b, 2),
            "cico_value_ksh_billions_last_12m": [
                {"month": str(m), "value": round(v, 2)} for m, v in reversed(trend)
            ],
        },
        "account_ownership": ownership,
    }


def build(db_path: str, out_dir: str) -> None:
    con = duckdb.connect(db_path, read_only=True)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ---------- headline stats ----------
    mm = con.execute("""
        select d.calendar_date, m.registered_accounts_millions, m.active_agents,
               m.agent_cico_value_ksh_billions
        from gold.fact_mobile_money m join gold.dim_date d using (date_key)
        order by 1 desc limit 1
    """).fetchone()
    fx_span = con.execute("""
        select min(d.calendar_date), max(d.calendar_date), count(*)
        from gold.fact_exchange_rate f join gold.dim_date d using (date_key)
    """).fetchone()
    q_total = con.execute("select count(*) from silver.quarantine").fetchone()[0]
    rows_landed = con.execute(
        "select sum(rows_inserted) from meta.ingestion_log where status='ok'"
    ).fetchone()[0]

    stats_html = f"""
    <div class="stats">
      <div class="stat"><b>{mm[1]:.1f}M</b><span>registered mobile-money accounts,
        Kenya ({mm[0]:%b %Y})</span></div>
      <div class="stat"><b>{mm[2]:,}</b><span>active agents ({mm[0]:%b %Y})</span></div>
      <div class="stat"><b>{fx_span[2]:,}</b><span>daily FX facts
        ({fx_span[0]:%Y}–{fx_span[1]:%Y})</span></div>
      <div class="stat"><b>{q_total:,}</b><span>rows quarantined with reasons —
        <a href="quality.html">see why</a></span></div>
    </div>"""

    # ---------- chart 1: CICO in USD ----------
    cico = con.execute("""
        with monthly_usd as (
            select d.month_start, avg(f.kes_per_unit_mean) as kes_per_usd
            from gold.fact_exchange_rate f join gold.dim_date d using (date_key)
            where f.currency_code = 'USD' group by 1
        )
        select d.calendar_date as month,
               m.agent_cico_value_ksh_billions / u.kes_per_usd as usd_b
        from gold.fact_mobile_money m
        join gold.dim_date d using (date_key)
        join monthly_usd u on u.month_start = d.calendar_date
        order by 1
    """).fetchdf()
    fig, ax = plt.subplots(figsize=(9, 3.6))
    ax.plot(cico["month"], cico["usd_b"], color=GREEN)
    ax.set_title("Kenya: monthly agent cash-in/cash-out value (USD billions)")
    ax.set_ylabel("USD billions")
    chart_cico = fig_to_img(fig)

    # ---------- chart 2: ownership ----------
    own = con.execute("""
        select w.year, c.country_name, w.value
        from gold.fact_worldbank_indicator w
        join gold.dim_country c using (country_iso3)
        where w.indicator_code = 'FX.OWN.TOTL.ZS' order by 1
    """).fetchdf()
    fig, ax = plt.subplots(figsize=(9, 3.6))
    for name, grp in own.groupby("country_name"):
        ax.plot(grp["year"], grp["value"], marker="o", label=name)
    ax.set_title("Account ownership, % of adults 15+ (World Bank Findex)")
    ax.set_ylabel("%")
    ax.legend(frameon=False, fontsize=8)
    chart_own = fig_to_img(fig)

    # ---------- chart 3: accounts vs agents ----------
    sat = con.execute("""
        select d.calendar_date as month, m.registered_accounts_millions,
               m.active_agents / 1000.0 as agents_k
        from gold.fact_mobile_money m join gold.dim_date d using (date_key) order by 1
    """).fetchdf()
    fig, ax1 = plt.subplots(figsize=(9, 3.6))
    ax1.plot(sat["month"], sat["registered_accounts_millions"], color=GREEN)
    ax1.set_ylabel("Accounts (M)", color=GREEN)
    ax2 = ax1.twinx()
    ax2.plot(sat["month"], sat["agents_k"], color=ORANGE)
    ax2.set_ylabel("Agents (k)", color=ORANGE)
    ax1.set_title("Kenya mobile money: accounts vs agent network, 2007–present")
    chart_sat = fig_to_img(fig)

    index_body = f"""
    {stats_html}
    <div class="card">{chart_cico}</div>
    <div class="card">{chart_own}</div>
    <div class="card">{chart_sat}</div>
    <p class="sub">Pipeline totals: {rows_landed:,} rows landed across four sources
    (World Bank API · CBK forex CSV · CBK mobile-payments scrape · GSMA workbook).</p>
    """
    (out / "index.html").write_text(page("Mizani", index_body, built_at), encoding="utf-8")

    # ---------- quality page ----------
    silver_log = con.execute("""
        select target_table, max(built_at) as built_at, max(rows_in) as rows_in,
               max(rows_clean) as clean, max(rows_quarantined) as quarantined
        from meta.silver_log where status = 'ok'
        group by 1 order by 1
    """).fetchdf()
    log_rows = "".join(
        f"<tr><td><code>{r.target_table}</code></td><td>{r.rows_in:,}</td>"
        f"<td>{r.clean:,}</td><td>{r.quarantined:,}</td></tr>"
        for r in silver_log.itertuples()
    )

    reasons = con.execute("""
        select reasons, count(*) as n from silver.quarantine group by 1 order by n desc
    """).fetchdf()
    reason_rows = "".join(
        f"<tr><td>{r.reasons}</td><td>{r.n:,}</td></tr>" for r in reasons.itertuples()
    )

    samples = con.execute("""
        select source, reasons, raw_payload from silver.quarantine
        order by reasons, source limit 6
    """).fetchdf()
    sample_rows = "".join(
        f"<tr><td><code>{r.source}</code></td><td>{r.reasons}</td>"
        f"<td><code>{json.dumps(json.loads(r.raw_payload))[:120]}</code></td></tr>"
        for r in samples.itertuples()
    )

    recon = con.execute(
        "select * from gold.recon_usd_rate_yearly order by year"
    ).fetchdf()
    if len(recon):
        recon_rows = "".join(
            f"<tr><td>{int(r.year)}</td><td>{r.cbk_avg_rate:.2f}</td>"
            f"<td>{r.worldbank_avg_rate:.2f}</td><td>{int(r.cbk_trading_days)}</td>"
            f"<td class='{'ok' if r.relative_difference <= 0.01 else 'bad'}'>"
            f"{r.relative_difference * 100:.2f}%</td></tr>"
            for r in recon.itertuples()
        )
        recon_table = f"""<table><tr><th>Year</th><th>CBK daily avg (KES/USD)</th>
        <th>World Bank annual avg</th><th>CBK trading days</th><th>divergence</th></tr>
        {recon_rows}</table>"""
    else:
        recon_table = "<p class='sub'>No full-coverage years in this build.</p>"

    quality_body = f"""
    <h2>Nothing is silently dropped</h2>
    <p>Rows that fail validation are quarantined with their original raw payload and a
    human-readable reason — including cases where the source <em>republished</em> a fact
    with different values (the pipeline refuses to guess which revision is true).
    Highlights caught by declarative rules: a day in 2017 where CBK published every
    currency with buy/sell swapped, a future-dated row (2038), and a mangled header
    fragment inside the CSV body.</p>

    <h2>Silver build results</h2>
    <div class="card"><table>
    <tr><th>table</th><th>rows in</th><th>clean</th><th>quarantined</th></tr>
    {log_rows}</table></div>

    <h2>Quarantine, by reason</h2>
    <div class="card"><table><tr><th>reason</th><th>rows</th></tr>{reason_rows}</table></div>

    <h2>Sample quarantined payloads (as received)</h2>
    <div class="card"><table><tr><th>source</th><th>reason</th><th>raw payload</th></tr>
    {sample_rows}</table></div>

    <h2>Cross-source reconciliation</h2>
    <p>CBK's daily USD averages are checked against the World Bank's independently
    published annual figure (dbt-tested tolerance: 1% on years with ≥200 trading
    days). Two sources agreeing within a fraction of a percent is strong evidence the
    parsing — three date formats, two quote conventions — is right.</p>
    <div class="card">{recon_table}</div>
    """
    (out / "quality.html").write_text(
        page("Mizani — data quality", quality_body, built_at), encoding="utf-8"
    )

    # ---------- machine-readable feed (consumed by PesaGuard's console) ----------
    feed = build_feed(con)
    (out / "data").mkdir(exist_ok=True)
    (out / "data" / "latest.json").write_text(
        json.dumps(feed, indent=2, default=str), encoding="utf-8"
    )
    con.close()
    print(f"site written to {out.resolve()}")


if __name__ == "__main__":
    build(
        sys.argv[1] if len(sys.argv) > 1 else "data/mizani.duckdb",
        sys.argv[2] if len(sys.argv) > 2 else "site",
    )
