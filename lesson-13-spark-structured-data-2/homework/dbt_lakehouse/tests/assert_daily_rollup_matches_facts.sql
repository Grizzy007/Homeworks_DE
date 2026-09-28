-- Тест: sum(fact_repo_activity_daily.commits) = count(*) з fact_commit.
-- Специфікація: ../../SPEC.md → «Тести». Тест падає, якщо запит поверне рядки.
select rollup_commits, fact_commits
from (
    select
        (select sum(commits) from {{ ref('fact_repo_activity_daily') }}) as rollup_commits,
        (select count(*)     from {{ ref('fact_commit') }})              as fact_commits
)
where not (rollup_commits <=> fact_commits)
