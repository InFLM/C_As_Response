# Running the Ciena ETL on the Azure VM (CentOS 7.9) with auto start/stop

A complete walkthrough to move the nightly pipeline off your laptop and onto your Azure VM,
which starts itself before the run and shuts itself down (deallocated, so it stops billing)
after. This fixes the whole class of laptop/WSL/VPN connection failures and the missed
scheduled runs.

## The nightly lifecycle you're building

```
22:15  Azure Automation starts the VM (it was deallocated/off)
22:20  cron on the VM runs the pipeline (after the SQL Server :17 log restore)
         -> chunked extract -> Snowflake load -> dbt build -> dashboard refresh
~22:50 pipeline finishes; Teams notification sent
23:30  Azure auto-shutdown deallocates the VM -> compute billing stops
```

The VM bills for only ~1 hour/night instead of 24/7.

---

## ⚠️ Read first: CentOS 7.9 is end-of-life

CentOS 7 reached end-of-life on **30 June 2024**. Two practical consequences this guide works around:

- Its `yum` mirrors were taken offline (you'll get 404s) — fixed by pointing `yum` at the
  CentOS **vault** (Step 1).
- Its system Python is 3.6, which is **too old** for your `dbt-core 1.8.8` / `pandas 2.2` /
  `snowflake-connector` stack. We install **Miniconda** and run the pipeline from a Python
  3.11 environment, sidestepping the dead system Python entirely (Step 3).

This works, but the OS receives no security patches. Plan a migration to Rocky/Alma Linux 9,
RHEL 9, or Ubuntu LTS when you can. Using Miniconda now means that migration won't disturb
your Python environment.

---

## Step 0 — Confirm the VM and reach it

In the Azure portal: **Virtual machines → your VM → Overview** shows the OS, resource group,
and status. Note the **VM name** and **resource group** — you need them in Steps 5–6.

SSH in (from your laptop):

```bash
ssh <user>@<vm-public-ip-or-dns>
cat /etc/centos-release      # confirms CentOS Linux release 7.9.2009
```

---

## Step 1 — Make `yum` work again (point it at the vault)

Because CentOS 7 is EOL, the default mirrors 404. Repoint to the archive:

```bash
sudo sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
sudo sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
sudo yum clean all && sudo yum makecache
sudo yum install -y curl gcc gcc-c++ make   # build tools (needed to compile pyodbc)
```

---

## Step 2 — Connectivity check (do this before anything else)

Moving off your laptop changes the **source IP**, so the SQL Server firewall / network must
allow the VM. Confirm the VM can actually reach SQL Server before investing in the rest:

```bash
getent hosts snfi00.cloudg.com                       # DNS resolves?
timeout 5 bash -c '</dev/tcp/snfi00.cloudg.com/1433' && echo "port 1433 OPEN" || echo "BLOCKED"
```

If DNS fails or the port is blocked, fix that first: the VM's NSG must allow outbound to
`snfi00.cloudg.com:1433`, and that SQL host's firewall/VPN must permit the VM's outbound IP
(or you need VPN/peering between the VM's VNet and the SQL Server network). There's no point
scheduling a job that can't connect.

---

## Step 3 — Install the ODBC driver + Python (Miniconda) + the project

### 3a. Microsoft ODBC Driver 18 (RHEL 7 repo)

```bash
curl https://packages.microsoft.com/config/rhel/7/prod.repo | sudo tee /etc/yum.repos.d/mssql-release.repo
sudo yum remove -y unixODBC-utf16 unixODBC-utf16-devel 2>/dev/null || true   # remove conflicts if present
sudo ACCEPT_EULA=Y yum install -y msodbcsql18
sudo yum install -y unixODBC-devel                                           # headers for pyodbc
```

### 3b. Miniconda + a Python 3.11 environment

```bash
curl -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p "$HOME/miniconda3"
source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda create -y -n etl python=3.11
conda activate etl
```

### 3c. Clone the repo and install dependencies

```bash
git clone https://github.com/InFLM/ci_As_Response.git "$HOME/ci_As_Response"
cd "$HOME/ci_As_Response"
pip install -r requirements.txt
```

If `pyodbc` fails to build, install it from conda-forge instead, then retry the rest:
`conda install -y -c conda-forge pyodbc && pip install -r requirements.txt`.

### 3d. Credentials and a smoke test

Copy your `.env` to the VM (never commit it). From your laptop:

```bash
scp .env <user>@<vm>:~/ci_As_Response/.env
```

Then on the VM:

```bash
cd "$HOME/ci_As_Response"
chmod 600 .env
cd dbt_ciena && dbt debug && cd ..          # confirms Snowflake auth
bash run_pipeline_vm.sh                      # full run; tail logs/pipeline_<date>.log
```

`run_pipeline_vm.sh` (already in the repo) is the VM twin of `run_pipeline.sh` — it activates
the conda `etl` env instead of a `.venv`, logs to `logs/pipeline_<date>.log`, and refreshes
`dashboard.html` on exit. If you put Miniconda somewhere other than `$HOME/miniconda3`, edit
the `source ... conda.sh` line near the top.

---

## Step 4 — Schedule the pipeline on the VM (cron)

First set the VM's timezone so cron lines up with the SQL Server's hourly `:17` restore and
your business hours:

```bash
sudo timedatectl set-timezone America/Denver     # use your actual timezone
timedatectl                                       # verify
```

Add the cron job (runs at 22:20, just after the VM starts at 22:15 and after the :17 restore):

```bash
crontab -e
```
```cron
20 22 * * * /home/<user>/ci_As_Response/run_pipeline_vm.sh
```

cron starts automatically on boot (`systemctl status crond`), so as long as the VM is up by
22:20 the job fires. Use the **full path** to the script — cron has a minimal environment.

---

## Step 5 — Auto-start the VM (Azure Automation)

A deallocated VM can't wake itself; an Azure-side trigger must start it. The standard near-free
way is an Automation Account runbook on a schedule.

1. **Create an Automation Account**: portal → *Create resource* → "Automation" → same region/
   resource group as the VM. (Free tier includes 500 job-minutes/month — far more than enough.)
2. **Give it permission**: Automation Account → **Identity** → turn **System assigned** = On.
   Then VM's resource group → **Access control (IAM)** → **Add role assignment** →
   **Virtual Machine Contributor** → assign to that Automation Account's managed identity.
3. **Create the runbook**: Automation Account → **Runbooks** → *Create a runbook* → type
   **PowerShell** (5.1 or 7.2). Paste:

   ```powershell
   Connect-AzAccount -Identity | Out-Null
   Start-AzVM -ResourceGroupName "<your-rg>" -Name "<your-vm>"
   ```

   **Publish** it.
4. **Schedule it**: open the runbook → **Schedules** → *Add a schedule* → new schedule,
   **Recurring, daily, 22:15**, in your timezone. Link it.

(Stopping is handled natively in Step 6 — you don't need a stop runbook.)

---

## Step 6 — Auto-shutdown so billing stops

> Important: an OS-level `shutdown -h` only puts the VM in **Stopped** state — it stays
> *allocated* and **keeps billing compute**. To stop paying you must **deallocate**. Azure's
> native Auto-shutdown deallocates for you.

**Native Auto-shutdown (simple, recommended baseline):**
VM → **Operations → Auto-shutdown** → **On** → time **23:30**, your timezone → optionally add
a notification webhook/email → **Save**. At 23:30 the VM deallocates and compute charges stop.

Set 23:30 comfortably after the run's expected finish. If a run ever needs longer (e.g. the
log-shipping standby is catching up), widen this window.

**Optional — self-deallocate the moment the run finishes (cheapest, no risk of cutoff):**
Instead of a fixed time, have the pipeline deallocate the VM when it's done:

1. VM → **Identity** → **System assigned** = On; then grant that identity
   **Virtual Machine Contributor** on the VM (or its resource group), same as Step 5.2.
2. Install the CLI in the env: `conda activate etl && pip install azure-cli`.
3. Uncomment the self-deallocate block at the bottom of `run_pipeline_vm.sh`.

If you use self-deallocate, **still keep a native Auto-shutdown as a backstop** (say 00:30) so
a hung run can't leave the VM billing all night.

---

## Step 7 — Verify end to end

1. **Dry run now:** start the VM manually (portal or `az vm start`), SSH in, run
   `bash run_pipeline_vm.sh`, and confirm: it connects, chunks through the extract, loads
   Snowflake, dbt builds, `dashboard.html` updates, and the Teams notification arrives.
2. **Auto-start:** in the Automation runbook, **Start** it once manually and confirm the VM
   powers on; then confirm the 22:15 schedule shows a next-run time.
3. **Auto-shutdown:** confirm Auto-shutdown is **On** with the right time/timezone.
4. **Let it run a night**, then check `logs/pipeline_<date>.log` and the dashboard the next
   morning, and confirm the VM is back in **Stopped (deallocated)**.

---

## Cost & security notes

- **Cost:** deallocated = no compute charge; you still pay for the OS disk and any static
  public IP while it's off (small). ~1 hour/night of compute is the bulk of the cost.
- **Automation Account** free grant (500 min/mo) covers the start runbook easily.
- **Secrets:** keep `.env` at `chmod 600`, never in git. Since you're moving hosts, this is a
  good moment to **rotate** the SQL Server and Snowflake credentials. Longer term, store them
  in **Azure Key Vault** and have the VM read them via its managed identity — no secrets on disk.
- **Timezone:** the VM timezone (Step 4), the Automation schedule (Step 5), and Auto-shutdown
  (Step 6) must all agree, and should line up with the SQL Server's `:17` restore so the run
  lands in a clean window.

---

### Sources
- [Azure auto-shutdown deallocates / stops billing](https://learn.microsoft.com/en-us/answers/questions/1662540/stopped-vm-charged-me-or-not)
- [Start VMs on a schedule with an Automation runbook](https://blog.matrixpost.net/azure-virtual-machine-start-stop-schedule/)
- [Install the Microsoft ODBC driver for SQL Server (Linux)](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
- [Fixing CentOS 7 yum after EOL (vault repos)](https://medium.com/@sobiatahmedkhan/fixing-centos-7-yum-repository-issues-after-centos-mirrors-were-moved-to-vault-d6f516a28145)
