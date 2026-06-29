#!/usr/bin/env python3
"""Mission control dashboard generator for the Ciena AS_Response pipeline.

Parses every logs/pipeline_*.log file (each may contain several appended runs),
extracts status/rows/dbt results/duration/errors, and writes a single
self-contained dashboard.html. No third-party packages, no internet needed
(the chart is inline SVG), so it runs anywhere the pipeline runs.

Usage:
    python dashboard.py            # writes ./dashboard.html
    python dashboard.py --open     # also opens it in the default browser

Wire it into run_pipeline.sh so it refreshes after every run (see DAGSTER_SETUP
/ README for the trap line).
"""
from __future__ import annotations

import html
import re
import sys
import webbrowser
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
LOG_DIR = REPO_ROOT / "logs"
OUT_FILE = REPO_ROOT / "dashboard.html"
MAX_RUNS_SHOWN = 30          # rows in the history table
CHART_RUNS = 14              # bars in the rows-extracted chart

_MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}

_TS = re.compile(r"\w{3} (\w{3})\s+(\d+) (\d{2}):(\d{2}):(\d{2}) \w+ (\d{4})")
_ROWS = re.compile(r"Extract complete:\s*([\d,]+)\s*rows")
_DBT = re.compile(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) TOTAL=(\d+)")
_WINDOW = re.compile(r"revision_date in \[([\d-]+),\s*([\d-]+)\)")


def _parse_ts(line: str) -> datetime | None:
    m = _TS.search(line)
    if not m:
        return None
    mon, day, hh, mm, ss, yr = m.groups()
    try:
        return datetime(int(yr), _MONTHS[mon], int(day), int(hh), int(mm), int(ss))
    except (KeyError, ValueError):
        return None


def _split_runs(text: str) -> list[list[str]]:
    """Split one log file into per-run blocks on each 'Pipeline started:' marker."""
    runs: list[list[str]] = []
    current: list[str] = []
    for line in text.splitlines():
        if line.startswith("Pipeline started:"):
            if current:
                runs.append(current)
            current = [line]
        elif current:
            current.append(line)
    if current:
        runs.append(current)
    return runs


def _parse_run(block: list[str], source: str) -> dict:
    text = "\n".join(block)
    started = _parse_ts(block[0]) if block else None

    finished = None
    for line in block:
        if line.startswith("Pipeline finished OK"):
            finished = _parse_ts(line)

    no_rows = "No rows extracted" in text
    ok = "Pipeline finished OK" in text

    rows = 0
    rm = _ROWS.search(text)
    if rm:
        rows = int(rm.group(1).replace(",", ""))

    dbt = None
    dm = _DBT.search(text)
    if dm:
        p, w, e, s, t = (int(x) for x in dm.groups())
        dbt = {"pass": p, "warn": w, "error": e, "skip": s, "total": t}

    window = None
    wm = _WINDOW.search(text)
    if wm:
        window = (wm.group(1), wm.group(2))

    if ok and no_rows:
        status = "no_rows"
    elif ok:
        status = "success"
    else:
        status = "failed"

    duration = None
    if started and finished and finished >= started:
        duration = int((finished - started).total_seconds())

    # For failed runs, pull a short, useful error excerpt (ERROR lines + last exception).
    error_excerpt = ""
    if status == "failed":
        err_lines = [ln for ln in block if "ERROR:" in ln]
        exc_lines = [ln for ln in block
                     if re.match(r"\s*\w[\w.]*(Error|Exception|Warning):", ln)
                     or "Traceback" in ln]
        picked = (err_lines + exc_lines)[:6] or block[-8:]
        error_excerpt = "\n".join(picked)

    return {
        "started": started,
        "status": status,
        "rows": rows,
        "dbt": dbt,
        "window": window,
        "duration": duration,
        "error": error_excerpt,
        "source": source,
    }


def collect_runs() -> list[dict]:
    runs: list[dict] = []
    if not LOG_DIR.exists():
        return runs
    for log in sorted(LOG_DIR.glob("pipeline_*.log")):
        try:
            text = log.read_text(errors="replace")
        except OSError:
            continue
        for block in _split_runs(text):
            runs.append(_parse_run(block, log.name))
    runs.sort(key=lambda r: r["started"] or datetime.min, reverse=True)
    return runs


# ---------- rendering helpers ----------

_STATUS = {
    "success": ("Success", "ok"),
    "no_rows": ("No rows", "warn"),
    "failed":  ("Failed", "err"),
}


def _fmt_dt(dt: datetime | None) -> str:
    return dt.strftime("%b %d, %H:%M") if dt else "—"


def _fmt_dur(sec: int | None) -> str:
    if sec is None:
        return "—"
    return f"{sec // 60}m {sec % 60:02d}s"


def _dbt_cell(dbt: dict | None) -> str:
    if not dbt:
        return '<span class="muted">—</span>'
    cls = "ok" if dbt["error"] == 0 and dbt["warn"] == 0 else (
        "err" if dbt["error"] else "warn")
    return f'<span class="dot {cls}"></span>{dbt["pass"]}/{dbt["total"]}'


def _svg_chart(runs: list[dict]) -> str:
    data = list(reversed(runs[:CHART_RUNS]))
    if not data:
        return '<p class="muted">No runs yet.</p>'
    w, h, pad_b, pad_t = 660, 170, 26, 10
    n = len(data)
    gap = 6
    bw = max(6, (w - (n - 1) * gap) / n)
    peak = max((r["rows"] for r in data), default=0) or 1
    color = {"ok": "#1D9E75", "warn": "#EF9F27", "err": "#E24B4A"}
    bars = []
    for i, r in enumerate(data):
        _, key = _STATUS[r["status"]]
        rows = r["rows"]
        bh = (rows / peak) * (h - pad_b - pad_t) if rows else 3
        x = i * (bw + gap)
        y = h - pad_b - bh
        label = r["started"].strftime("%m/%d") if r["started"] else ""
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw:.1f}" height="{bh:.1f}" '
            f'rx="3" fill="{color[key]}"><title>{label}: {rows:,} rows '
            f'({_STATUS[r["status"]][0]})</title></rect>'
            f'<text x="{x + bw/2:.1f}" y="{h - 8}" text-anchor="middle" '
            f'class="cl">{label}</text>')
    return (f'<svg viewBox="0 0 {w} {h}" width="100%" height="{h}" '
            f'role="img" aria-label="Rows extracted per run">{"".join(bars)}</svg>')


def render(runs: list[dict]) -> str:
    latest = runs[0] if runs else None
    total = len(runs)
    fails = sum(1 for r in runs if r["status"] == "failed")
    success_rate = round(100 * (total - fails) / total) if total else 0

    if latest:
        lname, lkey = _STATUS[latest["status"]]
        last_status = f'<span class="{lkey}">{lname}</span>'
        last_when = _fmt_dt(latest["started"])
        last_dur = _fmt_dur(latest["duration"])
        last_rows = f'{latest["rows"]:,}'
        last_dbt = (f'{latest["dbt"]["pass"]} / {latest["dbt"]["total"]}'
                    if latest["dbt"] else "—")
        last_dbt_sub = (
            f'{latest["dbt"]["warn"]} warn · {latest["dbt"]["error"]} error'
            if latest["dbt"] else "did not run")
        banner = lkey
    else:
        last_status = last_when = last_dur = last_rows = last_dbt = "—"
        last_dbt_sub = "no runs found"
        banner = "warn"

    rows_html = []
    for r in runs[:MAX_RUNS_SHOWN]:
        name, key = _STATUS[r["status"]]
        err = ""
        if r["error"]:
            err = (f'<tr class="errrow"><td colspan="5"><pre>'
                   f'{html.escape(r["error"])}</pre></td></tr>')
        rows_html.append(
            f'<tr><td>{_fmt_dt(r["started"])}</td>'
            f'<td><span class="dot {key}"></span>{name}</td>'
            f'<td class="r">{r["rows"]:,}</td>'
            f'<td>{_dbt_cell(r["dbt"])}</td>'
            f'<td class="r">{_fmt_dur(r["duration"])}</td></tr>{err}')

    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return _TEMPLATE.format(
        banner=banner,
        last_status=last_status, last_when=last_when, last_dur=last_dur,
        last_rows=last_rows, last_dbt=last_dbt, last_dbt_sub=last_dbt_sub,
        total=total, success_rate=success_rate, fails=fails,
        chart=_svg_chart(runs), rows="".join(rows_html) or
        '<tr><td colspan="5" class="muted">No runs found in ./logs.</td></tr>',
        generated=generated)


_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ciena ETL — Mission Control</title>
<style>
:root{{--bg:#faf9f5;--card:#fff;--bd:#e7e5df;--tx:#1f1e1c;--mut:#76746e;
--ok:#0f6e56;--warn:#854f0b;--err:#a32d2d;--okbg:#e1f5ee;--warnbg:#faeeda;--errbg:#fcebeb;}}
@media(prefers-color-scheme:dark){{:root{{--bg:#1c1b19;--card:#26252300;--card:#2a2926;
--bd:#3a3935;--tx:#ece9e2;--mut:#a3a09a;--ok:#5dcaa5;--warn:#ef9f27;--err:#f09595;
--okbg:#0f3b30;--warnbg:#3a2a0c;--errbg:#3a1414;}}}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--tx);
font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
line-height:1.5;padding:28px 22px;}}
.wrap{{max-width:720px;margin:0 auto;}}
.head{{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:18px;}}
.title{{font-size:18px;font-weight:500;margin:0;}}
.sub{{font-size:13px;color:var(--mut);margin:2px 0 0;}}
.pill{{font-size:12px;padding:4px 12px;border-radius:8px;}}
.pill.ok{{color:var(--ok);background:var(--okbg);}}
.pill.warn{{color:var(--warn);background:var(--warnbg);}}
.pill.err{{color:var(--err);background:var(--errbg);}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:22px;}}
.c{{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:14px 16px;}}
.c .lbl{{font-size:13px;color:var(--mut);margin:0 0 4px;}}
.c .big{{font-size:22px;font-weight:500;margin:0;}}
.c .tiny{{font-size:12px;color:var(--mut);margin:4px 0 0;}}
.section{{font-size:13px;color:var(--mut);margin:0 0 8px;}}
.panel{{background:var(--card);border:1px solid var(--bd);border-radius:12px;padding:16px;margin-bottom:22px;}}
table{{width:100%;border-collapse:collapse;font-size:13px;}}
th{{text-align:left;font-weight:500;color:var(--mut);padding:8px 6px;border-bottom:1px solid var(--bd);}}
td{{padding:8px 6px;border-bottom:1px solid var(--bd);}}
td.r,th.r{{text-align:right;}}
.dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:7px;vertical-align:1px;}}
.dot.ok,.ok{{color:var(--ok);}}.dot.ok{{background:var(--ok);}}
.dot.warn,.warn{{color:var(--warn);}}.dot.warn{{background:var(--warn);}}
.dot.err,.err{{color:var(--err);}}.dot.err{{background:var(--err);}}
.muted{{color:var(--mut);}}
.errrow td{{padding-top:0;border-bottom:1px solid var(--bd);}}
.errrow pre{{margin:0 0 6px;font-size:11px;color:var(--err);background:var(--errbg);
padding:8px 10px;border-radius:8px;overflow-x:auto;white-space:pre-wrap;}}
text.cl{{fill:var(--mut);font-size:10px;font-family:inherit;}}
.foot{{font-size:12px;color:var(--mut);text-align:center;margin-top:8px;}}
</style></head>
<body><div class="wrap">
<div class="head">
  <div><p class="title">Ciena AS_Response pipeline</p>
  <p class="sub">SQL Server &rarr; Snowflake &rarr; dbt</p></div>
  <span class="pill {banner}">{last_status}</span>
</div>
<div class="cards">
  <div class="c"><p class="lbl">Last run</p><p class="big">{last_status}</p><p class="tiny">{last_when} &middot; {last_dur}</p></div>
  <div class="c"><p class="lbl">Rows extracted</p><p class="big">{last_rows}</p><p class="tiny">most recent run</p></div>
  <div class="c"><p class="lbl">dbt build</p><p class="big">{last_dbt}</p><p class="tiny">{last_dbt_sub}</p></div>
  <div class="c"><p class="lbl">Success rate</p><p class="big">{success_rate}%</p><p class="tiny">{total} runs &middot; {fails} failed</p></div>
</div>
<p class="section">Rows extracted per run</p>
<div class="panel">{chart}</div>
<p class="section">Recent runs</p>
<div class="panel"><table>
<thead><tr><th>Started</th><th>Status</th><th class="r">Rows</th><th>dbt</th><th class="r">Duration</th></tr></thead>
<tbody>{rows}</tbody></table></div>
<p class="foot">Generated {generated} &middot; refresh by re-running dashboard.py</p>
</div></body></html>
"""


def main() -> None:
    runs = collect_runs()
    OUT_FILE.write_text(render(runs), encoding="utf-8")
    print(f"Wrote {OUT_FILE} ({len(runs)} runs from {LOG_DIR})")
    if "--open" in sys.argv:
        webbrowser.open(OUT_FILE.as_uri())


if __name__ == "__main__":
    main()
