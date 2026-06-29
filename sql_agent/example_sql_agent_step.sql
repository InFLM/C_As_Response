-- Example SQL Server Agent CmdExec step command.
-- Adjust paths for your actual WSL/project location.

wsl bash -lc "cd ~/Desktop/matillion_replacement_starter && source .venv/bin/activate && python etl/run_ciena_as_response.py && cd dbt_ciena && dbt run --select stg_as_response fct_as_response"
