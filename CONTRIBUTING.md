# Contributing

Short, practical rules so this project stays clean as the team grows.

## First-time setup

```bash
git clone <repo-url>
cd matillion_replacement_starter

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env        # then fill in real values (never commit .env)
```

Verify your environment before doing anything else:

```bash
cd dbt_ciena && dbt debug && cd ..
bash run_pipeline.sh        # full extract -> load -> dbt build
```

## Golden rule: never commit secrets

- Real credentials live only in your local `.env` (and `.env.test`), which are git-ignored.
- The only tracked env file is `.env.example`, and it contains placeholders only.
- `dbt_ciena/profiles.yml` is safe to track because it reads everything via `env_var()`.
- If you ever commit a secret by accident, treat it as compromised: **rotate it**, then
  remove it from history (`git filter-repo` or BFG). Removing the file in a later commit is
  not enough — it stays in history.

## Branching & pull requests

- `main` is always deployable. Don't push directly to it.
- Branch per change: `feature/...`, `fix/...`, `chore/...`.
- Open a pull request, get one review, then squash-merge.
- Keep PRs small and focused; write a one-line summary of what and why.

## What does NOT get committed

Generated or machine-specific files are ignored on purpose (see `.gitignore`):
`.venv/`, `__pycache__/`, `landing/`, `logs/`, `dashboard.html`,
`dbt_ciena/target/`, `dbt_ciena/logs/`.

Don't force-add them.

## Commit messages

Use a short imperative subject, e.g. `Add reconciliation model for as_response`.
Group related changes into one commit; avoid "wip" / "fixes" noise on `main`.

## Code layout

```
etl/            Python extract/load (SQL Server -> Parquet -> Snowflake)
dbt_ciena/      dbt project (staging + marts)
sql_agent/      example scheduler job step
dashboard.py    mission-control dashboard generator (reads logs/)
run_pipeline.sh nightly entrypoint
```
