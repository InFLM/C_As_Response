# Dagster Setup Guide — Ciena ETL on an Azure VM

A step-by-step runbook to put your existing Python + dbt pipeline behind a real
orchestrator with a web UI for **logs, past runs, schedules, and retries**.
Everything runs on your single Azure VM. No paid services, no external accounts.

- **What you have today:** `run_pipeline.sh` → `python -m etl.run_ciena_as_response`
  (SQL Server → Parquet → Snowflake → `dbt build`), with dated log files in `./logs`,
  a `check_status.sh` health check, and Teams notifications.
- **What this adds:** [Dagster OSS](https://docs.dagster.io/) (latest 1.13.x) running as
  two `systemd` services — a webserver (the UI) and a daemon (schedules/retries). Your
  pipeline becomes Dagster *assets*, so every run, every log line, and every dbt model
  is browsable in a browser with full history retained.

> **Why Dagster fits here:** it has first-class dbt support — each dbt model shows up as
> its own node with its own logs and lineage — and the OSS version is free and self-hostable.

---

## 0. Decisions baked into this guide

| Choice | What this guide uses | Why |
|---|---|---|
| Orchestrator | Dagster OSS | Best Python+dbt fit, free, self-hosted |
| Storage backend | SQLite in `$DAGSTER_HOME` (default) | Zero setup; fine for one pipeline. Postgres upgrade noted in §10 |
| Process mgmt | `systemd` (auto-start, auto-restart, journald logs) | Standard on Azure Ubuntu VMs |
| UI access | SSH tunnel to `localhost:3000` | **OSS Dagster has no built-in login** — never expose it publicly (see §8) |
| Scheduling | Dagster schedule in code | Replaces cron / Windows Task Scheduler |

This guide assumes an **Ubuntu** Azure VM (the repo already uses bash/WSL-style paths).
If your VM is Windows, see §11 for the Windows-service variant.

---

## 1. Prerequisites on the VM

SSH into the VM, then confirm you have the project and a working Python.

```bash
# Adjust the path to wherever the repo lives on the VM
cd ~/matillion_replacement_starter

python3 --version          # need 3.9–3.12
which unixodbc-dev || sudo apt-get update && sudo apt-get install -y unixodbc-dev   # for pyodbc
ls etl/run_ciena_as_response.py dbt_ciena/dbt_project.yml   # sanity check
```

Make sure the existing pipeline already runs by hand before adding Dagster:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # then edit .env with real values (see security note §9)
bash run_pipeline.sh        # should finish with "Pipeline finished OK"
```

If that works, Dagster will too — it just calls the same code.

---

## 2. Install Dagster into the same virtualenv

```bash
source .venv/bin/activate

pip install \
  dagster \
  dagster-webserver \
  dagster-dbt
```

Let pip resolve versions against your pinned `dbt-core==1.8.8` / `dbt-snowflake==1.8.4`.
If pip complains about a dbt version conflict, the simplest fix is to bump dbt:
`pip install --upgrade dbt-core dbt-snowflake` (test a `dbt build` afterward).

Append these to `requirements.txt` so the environment is reproducible:

```text
dagster
dagster-webserver
dagster-dbt
```

---

## 3. Pick an integration level

You have two options. **Start with Level 1** (10 minutes, immediate UI + history).
Move to Level 2 when you want per-dbt-model visibility.

### Level 1 — Wrap the whole pipeline as one asset (fastest)

Create `dagster_ciena/__init__.py`:

```python
# dagster_ciena/__init__.py
import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    asset,
    define_asset_job,
    ScheduleDefinition,
    RetryPolicy,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


@asset(
    # Retry the whole pull once if the fragile SQL Server link drops mid-run.
    retry_policy=RetryPolicy(max_retries=1, delay=60),
)
def ciena_as_response_pipeline(context: AssetExecutionContext) -> None:
    """Runs etl.run_ciena_as_response (SQL Server -> Snowflake -> dbt build)."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "etl.run_ciena_as_response"],
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    # Stream the script's stdout into Dagster's logs so it shows in the UI live.
    for line in proc.stdout:
        context.log.info(line.rstrip())
    code = proc.wait()
    if code != 0:
        raise Exception(f"Pipeline exited with code {code}")


ciena_job = define_asset_job("ciena_as_response_job", selection="*")

# Nightly at 02:00 VM-local time. Adjust cron as needed.
ciena_schedule = ScheduleDefinition(
    job=ciena_job,
    cron_schedule="0 2 * * *",
    execution_timezone="America/New_York",  # set to your VM/business timezone
)

defs = Definitions(
    assets=[ciena_as_response_pipeline],
    jobs=[ciena_job],
    schedules=[ciena_schedule],
)
```

Your existing Teams notifications in `etl/notify.py` keep working unchanged, because
Dagster just runs the same module. Skip to §5.

### Level 2 — Split extract+load from dbt (recommended for visibility)

This makes the **Snowflake load** one asset and each **dbt model** its own asset, so the
UI shows lineage (`raw → stg_as_response → fct_as_response`) and per-model logs/timings.

First set `RUN_DBT=false` in `.env` so the Python step stops calling dbt itself —
Dagster will own the dbt step now.

Create `dagster_ciena/project.py`:

```python
# dagster_ciena/project.py
from pathlib import Path
from dagster_dbt import DbtProject

REPO_ROOT = Path(__file__).resolve().parent.parent

ciena_dbt_project = DbtProject(
    project_dir=REPO_ROOT / "dbt_ciena",
    profiles_dir=REPO_ROOT / "dbt_ciena",
)
ciena_dbt_project.prepare_if_dev()   # builds manifest.json during local dev
```

Create `dagster_ciena/__init__.py`:

```python
# dagster_ciena/__init__.py
import subprocess
import sys
from pathlib import Path

from dagster import (
    AssetExecutionContext, Definitions, asset, define_asset_job,
    ScheduleDefinition, RetryPolicy,
)
from dagster_dbt import DbtCliResource, dbt_assets

from .project import ciena_dbt_project

REPO_ROOT = Path(__file__).resolve().parent.parent


@asset(retry_policy=RetryPolicy(max_retries=1, delay=60))
def raw_as_response(context: AssetExecutionContext) -> None:
    """Extract from SQL Server and load Parquet -> Snowflake RAW table.
    Runs the existing etl module with RUN_DBT disabled (dbt is handled below)."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "etl.run_ciena_as_response"],
        cwd=str(REPO_ROOT), env={**__import__("os").environ, "RUN_DBT": "false"},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
    )
    for line in proc.stdout:
        context.log.info(line.rstrip())
    if proc.wait() != 0:
        raise Exception("extract/load step failed")


@dbt_assets(manifest=ciena_dbt_project.manifest_path)
def ciena_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


ciena_job = define_asset_job("ciena_as_response_job", selection="*")
ciena_schedule = ScheduleDefinition(
    job=ciena_job, cron_schedule="0 2 * * *",
    execution_timezone="America/New_York",
)

defs = Definitions(
    assets=[raw_as_response, ciena_dbt_models],
    jobs=[ciena_job],
    schedules=[ciena_schedule],
    resources={"dbt": DbtCliResource(project_dir=ciena_dbt_project)},
)
```

> To get true lineage (the dbt `stg_as_response` model depending on `raw_as_response`),
> add `meta: {dagster: {asset_key: ["raw_as_response"]}}` to the matching source in
> `dbt_ciena/models/staging/sources.yml`. Optional — the pipeline runs correctly either way.

---

## 4. Point Dagster at your code (`workspace.yaml`)

Create `workspace.yaml` in the repo root:

```yaml
load_from:
  - python_module: dagster_ciena
```

---

## 5. Create `DAGSTER_HOME` (where runs & logs are stored)

This directory is what gives you **persistent run history**. It survives restarts and
is where the SQLite run/event databases and compute logs live.

```bash
mkdir -p ~/dagster_home
```

Create `~/dagster_home/dagster.yaml`:

```yaml
# Keep run history forever (default SQLite stores in $DAGSTER_HOME).
telemetry:
  enabled: false

# Auto-retry runs that crash (e.g. VM hiccup), independent of asset RetryPolicy.
run_retries:
  enabled: true
  max_retries: 1

# Optional: cap concurrent runs so a slow night can't stack up.
concurrency:
  runs:
    max_concurrent_runs: 1
```

Test it interactively before making it a service:

```bash
export DAGSTER_HOME=~/dagster_home
source ~/matillion_replacement_starter/.venv/bin/activate
cd ~/matillion_replacement_starter
dagster dev -h 127.0.0.1 -p 3000
```

Open an SSH tunnel from your laptop (see §8) and visit `http://localhost:3000`.
You should see the `ciena_as_response_job`, your assets, and a **Launchpad** to run it
manually. `Ctrl-C` to stop once it looks right.

---

## 6. Run it as `systemd` services (auto-start + auto-restart)

Replace `landon` / paths below with your actual VM username and repo path.

Create `/etc/systemd/system/dagster-webserver.service`:

```ini
[Unit]
Description=Dagster webserver (UI)
After=network.target

[Service]
User=landon
WorkingDirectory=/home/landon/matillion_replacement_starter
Environment=DAGSTER_HOME=/home/landon/dagster_home
EnvironmentFile=/home/landon/matillion_replacement_starter/.env
ExecStart=/home/landon/matillion_replacement_starter/.venv/bin/dagster-webserver -h 127.0.0.1 -p 3000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Create `/etc/systemd/system/dagster-daemon.service`:

```ini
[Unit]
Description=Dagster daemon (schedules, retries, run queue)
After=network.target

[Service]
User=landon
WorkingDirectory=/home/landon/matillion_replacement_starter
Environment=DAGSTER_HOME=/home/landon/dagster_home
EnvironmentFile=/home/landon/matillion_replacement_starter/.env
ExecStart=/home/landon/matillion_replacement_starter/.venv/bin/dagster-daemon run
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

> **The daemon is required** for schedules and retries to fire. The webserver alone only
> serves the UI. A deployment should have exactly **one** daemon process.

Enable and start both:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dagster-webserver dagster-daemon
sudo systemctl status dagster-webserver dagster-daemon --no-pager
```

They now start on boot and restart on crash.

---

## 7. Turn on the schedule

The schedule is defined in code but starts **stopped**. Turn it on once:

1. In the UI, go to **Automation** (or **Overview → Schedules**).
2. Toggle `ciena_as_response_job` schedule **on**.

(Or from the CLI: `dagster schedule start ciena_as_response_job` with `DAGSTER_HOME` set.)

Now retire the old trigger — disable the cron entry / Windows Task Scheduler job that
called `run_pipeline.sh`, so the pipeline doesn't run twice.

---

## 8. Access the UI safely (free, no public exposure)

**OSS Dagster has no authentication.** Do **not** open port 3000 on the Azure NSG to the
internet. Two safe, free options:

**Option A — SSH tunnel (recommended).** From your laptop:

```bash
ssh -L 3000:localhost:3000 landon@<vm-public-ip>
```

Then browse `http://localhost:3000`. The webserver stays bound to `127.0.0.1` (as in §6),
so nothing is exposed.

**Option B — Azure Bastion / VPN.** If your org uses Azure Bastion or a VPN to the VNet,
you can reach the VM's private IP directly; still keep it off the public internet.

---

## 9. Security cleanup (do this — your repo has plaintext creds)

`.env.example` in this repo contains what look like **real** SQL Server and Snowflake
values. Treat them as compromised:

- **Rotate** the SQL Server password and the Snowflake user/key that appear in
  `.env.example` / `.env.test`.
- Keep real secrets only in `.env` on the VM, `chmod 600 .env`, and ensure `.env` is
  git-ignored (`.env.example` should hold placeholders only).
- The `systemd` `EnvironmentFile=.env` loads these for both services. Note: systemd reads
  simple `KEY=value` lines — if any secret contains `#`, spaces, or quotes, wrap carefully
  or move to a key-pair file. If you use Snowflake key-pair auth, set
  `SNOWFLAKE_PRIVATE_KEY_PEM` in `.env` (your `run_ciena_as_response.py` already bridges the
  base64 form, but the dbt step reads the PEM directly).

---

## 10. Using it day to day

- **See past runs:** UI → **Runs**. Every execution is listed with status, duration,
  and timestamps — retained indefinitely in `$DAGSTER_HOME`.
- **Read logs:** click any run → **stdout/stderr** and structured event logs per step.
  In Level 2 you get logs *per dbt model*.
- **Re-run / backfill:** open a run → **Re-execute**, or launch from the **Launchpad**.
- **Failures:** failed runs are red in the UI; your Teams notification still fires from
  `etl/notify.py`. You can also add a Dagster failure sensor later if you want UI-native alerts.
- **Health check:** the UI replaces `check_status.sh`, but that script still works against
  the old `./logs` files if you keep Level 1.

### Optional upgrade: Postgres backend (only if you outgrow SQLite)

For one nightly pipeline, SQLite is fine. If you later add many pipelines or want more
robust concurrent writes, install Postgres on the VM and point `dagster.yaml` at it
(`run_storage`, `event_log_storage`, `schedule_storage` → `DagsterPostgresStorage`).
Still free, just more moving parts.

---

## 11. If your VM is Windows instead of Ubuntu

Everything above applies except process management:

- Use the same `pip install` and `dagster_ciena` code.
- Set `DAGSTER_HOME` as a system environment variable.
- Run the webserver and daemon as Windows services using
  [NSSM](https://nssm.cc/) (free): one service per command
  (`dagster-webserver -h 127.0.0.1 -p 3000` and `dagster-daemon run`), both pointed at the
  venv's executables, with `DAGSTER_HOME` in the service environment.
- Access the UI via RDP on the VM or an SSH tunnel as in §8.

---

## Quick reference

```bash
# Restart after a code change
sudo systemctl restart dagster-webserver dagster-daemon

# Tail service logs
journalctl -u dagster-daemon -f
journalctl -u dagster-webserver -f

# Run the job once from the CLI (no UI needed)
DAGSTER_HOME=~/dagster_home dagster job execute -m dagster_ciena -j ciena_as_response_job

# Rebuild the dbt manifest after changing models (Level 2)
cd dbt_ciena && dbt parse
```

---

### Sources
- [Deploying Dagster as a service (OSS, single machine)](https://docs.dagster.io/deployment/oss/deployment-options/deploying-dagster-as-a-service)
- [Dagster daemon](https://docs.dagster.io/deployment/execution/dagster-daemon)
- [OSS deployment architecture](https://docs.dagster.io/deployment/oss/oss-deployment-architecture)
- [dagster-dbt integration reference](https://docs.dagster.io/integrations/libraries/dbt/reference)
- [dagster.yaml reference](https://docs.dagster.io/deployment/oss/dagster-yaml)
