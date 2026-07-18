"""Gold stage — three analytics tables built from silver.

TODO (Завдання 4, 5, 6): реалізуйте три функції нижче.
Контракт: див. CONTRACTS.md → "gold repo_activity", "gold activity_per_minute",
"gold push_commits_by_repo". Усі лічильники приводьте до Int64 (.cast(pl.Int64)),
щоб схема результату була стабільною.

  * build_repo_activity:        кількість подій + кількість унікальних типів на repo
  * build_activity_per_minute:  кількість подій по хвилинах (.dt.truncate("1m"))
  * build_push_commits_by_repo: тільки PushEvent — кількість пушів і сума commit_count на repo
"""

from __future__ import annotations

import polars as pl

from . import config

from pathlib import Path


def build_repo_activity(silver: pl.DataFrame) -> pl.DataFrame:
    gold1 = (
        silver
        .group_by("repo_name")
        .agg(
            pl.len().cast(pl.Int64).alias("event_count"),
            pl.col("event_type").n_unique().cast(pl.Int64).alias("distinct_event_types"),
        )
        .sort("event_count", descending=True)
    )
    Path(config.GOLD_REPO_ACTIVITY).parent.mkdir(parents=True, exist_ok=True)
    gold1.write_parquet(config.GOLD_REPO_ACTIVITY)
    return gold1
silver = pl.read_parquet(config.SILVER_FILE)
build_repo_activity(silver)

def build_activity_per_minute(silver: pl.DataFrame) -> pl.DataFrame:
    gold2 = (
        silver
        .with_columns(
            pl.col("created_at").dt.truncate("1m").alias("minute")
        )
        .group_by("minute")
        .agg(pl.len().cast(pl.Int64).alias("event_count"))
        .sort("minute")
    )

    gold2.write_parquet(config.GOLD_ACTIVITY_PER_MINUTE)
    return gold2

build_activity_per_minute(silver)

def build_push_commits_by_repo(silver: pl.DataFrame) -> pl.DataFrame:
    gold3 = (
        silver
        .filter(pl.col("event_type") == 'PushEvent')
        .group_by('repo_name')
        .agg(pl.len().cast(pl.Int64).alias("push_events"), pl.col('commit_count').sum().cast(pl.Int64).alias('total_commits'))
    )
    gold3.write_parquet(config.GOLD_PUSH_COMMITS)
    return gold3

build_push_commits_by_repo(silver)
