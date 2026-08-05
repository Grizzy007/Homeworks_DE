"""Мінімальний custom Operator для dbt — той самий патерн, що й у `GHArchiveSensor` із ДЗ.

Навіщо власний оператор, а не BashOperator:
- одна точка правди для шляхів (`--project-dir`, `--profiles-dir`) і бінарника dbt;
- `dbt_vars` у `template_fields`, тож Airflow сам підставляє в них logical date;
- stdout dbt построково їде в task log, а ненульовий exit code стає падінням задачі.

У проді замість саморобного оператора зазвичай беруть `astronomer-cosmos` (розбирає
`manifest.json` і робить із кожної dbt-моделі окрему Airflow-задачу) або, для dbt Cloud,
`apache-airflow-providers-dbt-cloud`. Логіка запуску — та сама, що тут.
"""

import json
import os
import subprocess

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

# dbt живе в окремому venv (див. Dockerfile): у нього свої версії jinja2/click,
# які конфліктують із залежностями Airflow, тож ділити один site-packages не можна.
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt_taxi"


class DbtOperator(BaseOperator):
    """Запускає одну dbt-команду (`run`, `test`, `build`, `seed`, …)."""

    template_fields = ("dbt_vars", "select")
    ui_color = "#ff694a"          # фірмовий помаранчевий dbt — видно в Graph view

    def __init__(
        self,
        *,
        command: str = "run",
        select: str | None = None,
        dbt_vars: dict | None = None,
        full_refresh: bool = False,
        project_dir: str = DBT_PROJECT_DIR,
        dbt_bin: str = DBT_BIN,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.command = command
        self.select = select
        self.dbt_vars = dbt_vars or {}
        self.full_refresh = full_refresh
        self.project_dir = project_dir
        self.dbt_bin = dbt_bin

    def execute(self, context):
        cmd = [
            self.dbt_bin,
            self.command,
            "--project-dir", self.project_dir,
            "--profiles-dir", self.project_dir,
        ]
        if self.select:
            cmd += ["--select", self.select]
        if self.full_refresh:
            cmd += ["--full-refresh"]
        if self.dbt_vars:
            cmd += ["--vars", json.dumps(self.dbt_vars)]

        # target/ і logs/ пишемо у /tmp контейнера, щоб dbt не смітив у змонтованому проєкті
        env = {
            **os.environ,
            "DBT_TARGET_PATH": "/tmp/dbt-target",
            "DBT_LOG_PATH": "/tmp/dbt-logs",
        }

        self.log.info("dbt-команда: %s", " ".join(cmd))
        proc = subprocess.Popen(
            cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        for line in proc.stdout:                       # логи dbt -> логи задачі в UI
            self.log.info(line.rstrip())
        returncode = proc.wait()

        if returncode != 0:
            raise AirflowException(f"dbt {self.command} впав із кодом {returncode}")
        return returncode
