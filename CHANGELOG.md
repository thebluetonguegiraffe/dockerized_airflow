# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and versions are tagged as git tags (`vX.Y.Z`) following [Semantic Versioning](https://semver.org/).

### [0.2.0] - 26 -10-26

### Changed

- Migrated the metadata database and executor from **SQLite + SequentialExecutor** to **Postgres + LocalExecutor**, so tasks run as parallel OS processes instead of one at a time, and the scheduler no longer loses its heartbeat under load.
- `Dockerfile.airflow` now also installs `apache-airflow-providers-postgres` and [`uv`](https://docs.astral.sh/uv/), the latter used by native DAGs to sync their own Python dependencies on the fly without building a separate image.
- Reworked the README to clearly document the two DAG execution patterns side by side: **containerized DAGs** (`DockerOperator` + sibling image) and **native DAGs** (run directly in the Airflow container via `BashOperator`, optionally through `uv`).


## [0.1.0] - 2025-12-12 to 2026-04-07

Initial infrastructure and incremental onboarding of DAG projects, before this file existed.

### Added

- Initial Dockerized Airflow setup: Standalone Airflow on SQLite, `DockerOperator` support via `apache-airflow-providers-docker`, `makefile`, and base README (2025-12-12).
- Mounted [the_news_hub](https://github.com/thebluetonguegiraffe/the_news_hub) DAGs (2025-12-12).
- Mounted [artguide](https://github.com/thebluetonguegiraffe/artguide) DAGs (2025-12-21).
- Mounted netweave DAGs (2026-04-07).

### Security

- Restricted the Airflow webserver port binding to `127.0.0.1` instead of all interfaces (2026-02-22).
