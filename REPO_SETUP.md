# Repository & team-readiness guide

How to centralize this project and keep it healthy as the team grows. The mechanical setup
(git, `.gitignore`, secret scrubbing) is already done — this explains the choices and the
steps left to you (creating the remote, branch protection, CI).

## What was set up for you

- A clean, commit-ready tree (the actual `git init` is one command you run locally — see
  below — so it uses your own git identity and Windows line-ending settings).
- `.gitignore` excluding secrets, virtualenvs, dbt artifacts, runtime data/logs, and the
  generated dashboard. Verified: real env files and generated data are ignored, while
  `.env.example` and the env-var-only `dbt_ciena/profiles.yml` stay tracked.
- Secrets scrubbed: `.env.example` now holds placeholders only.
- Removed `~BROMIUM` browser-isolation junk folders.
- `CONTRIBUTING.md` for new team members.

## Initialize the repo (run these on your machine)

Open a terminal in the project folder and run:

```bash
git init -b main
git add -A
git status        # sanity check: NO .env, no landing/, no logs/, no target/ should appear
git commit -m "Initial commit: Ciena ETL (Python + dbt) + mission-control dashboard"
```

If `git status` lists anything secret or generated, stop and tell me — but the `.gitignore`
has been verified to cover them.

> **Do this regardless of host:** the credentials currently in your local `.env.test` /
> `.env.example` history on your machine are real. Even though git never tracked them,
> **rotate the SQL Server password and Snowflake key** — they've been sitting in plaintext.

## Choosing where to host (all free, all have a web UI)

| | GitHub Free | GitLab Free | Azure DevOps Repos |
|---|---|---|---|
| Private repos | Unlimited | Unlimited | Unlimited |
| Web UI for history/diffs/PRs | Yes | Yes | Yes |
| Team-member limit | **Unlimited** collaborators | **5 users** per top-level group | 5 free users |
| Free CI/CD | 2,000 Actions min/mo | 400 CI min/mo (unlimited on self-hosted runners) | 1,800 pipeline min/mo |
| Best fit | General default, easiest onboarding | All-in-one, self-hostable | Already in your Azure bill/identity |

**Recommendation:** GitHub Free for a team that will grow — no user cap and the largest
ecosystem. GitLab is a fine free choice too (nice UI), but the 5-user group limit on Free is
the thing that bites a scaling team. You can switch hosts later with no rework; a git remote
is just a URL.

## Push it to the remote (GitHub example)

Create an **empty** private repo in the web UI (no README/.gitignore — this repo already has
them), then:

```bash
cd matillion_replacement_starter
git remote add origin https://github.com/<org>/matillion_replacement_starter.git
git push -u origin main
```

GitLab and Azure Repos are identical except the URL. For GitLab:
`git remote add origin https://gitlab.com/<group>/matillion_replacement_starter.git`.

## Protect `main` (do this right after the first push)

In the repo settings, add a branch protection rule on `main`:

- Require a pull request before merging (at least 1 approval).
- Disallow direct pushes / force-pushes.
- (Once CI exists) require status checks to pass before merge.

This is the single highest-value step for keeping `main` deployable as more people commit.

## Secrets management as the team scales

Plaintext `.env` files are fine for one person on one VM, but don't scale. The progression:

1. **Now:** local `.env` per machine, git-ignored. Rotate the leaked creds.
2. **Soon:** put the runtime secrets in **Azure Key Vault** (you already have Azure). The VM
   reads them at startup via a managed identity instead of a file on disk. No secrets in the
   repo, no secrets in env files checked into anyone's machine.
3. **CI secrets:** store them as encrypted repository/organization secrets in your host
   (GitHub Actions secrets, GitLab CI variables, Azure DevOps variable groups) — never in
   the YAML.
4. **Add a guard:** enable secret scanning (GitHub/GitLab do this for free) and optionally a
   pre-commit hook (`gitleaks`) so a secret can't be committed in the first place.

## Suggested CI (free tier is plenty)

Start small — a check that runs on every PR:

- `dbt deps && dbt parse` (catches broken models/refs).
- `dbt build --select state:modified+` against a CI Snowflake schema, or at minimum compile.
- A Python lint (`ruff`) and import check.

One workflow file (`.github/workflows/ci.yml` or `.gitlab-ci.yml`) is enough to begin. This
fits comfortably inside any of the free CI tiers above.

## Healthy-as-it-grows checklist

- [ ] Remote created; `main` pushed.
- [ ] Branch protection on `main` (PR + 1 review, no direct push).
- [ ] Leaked SQL Server / Snowflake credentials rotated.
- [ ] Secrets moved to Azure Key Vault when more than one person/VM is involved.
- [ ] Secret scanning enabled on the host.
- [ ] CI workflow added (dbt parse/build + lint on PRs).
- [ ] New joiners follow `CONTRIBUTING.md` and never touch `main` directly.
