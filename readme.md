# Dockerized Airflow Deployment Guide

This repository provides the core infrastructure files required to deploy an Apache Airflow-based automation system on a Virtual Private Server (VPS). The entire orchestration environment is containerized using Docker to ensure portability, stability, and isolation.

## Architectural Principles

The Airflow core components (Webserver, Scheduler, Triggerer) run in a single **Standalone** container, backed by a dedicated **Postgres** container. DAGs themselves, however, are **not defined in this repository**; they live in their own project repos and are mounted into the Airflow container as volumes. Depending on how heavy or isolated a project's dependencies are, a DAG executes in one of two ways:

* **Containerized DAGs (Docker-on-Docker):** The task runs inside its own **sibling container**, built from a dedicated Docker image that bundles the project's code and dependencies (e.g. [the_news_hub](https://github.com/thebluetonguegiraffe/the_news_hub/pkgs/container/the_news_hub)). Airflow only triggers the `DockerOperator`; the actual work happens in a fully isolated, disposable container. Use this when a project has its own heavy/conflicting dependency set, or when you want the exact same image used elsewhere (e.g. a GHCR image also deployed as a standalone service).
* **Native DAGs (in-container, no image):** The task runs directly on the Airflow container itself, via `BashOperator`/`PythonOperator`, against a project repo that is mounted straight into the container's filesystem (e.g. `keep_qdrant_alive` in [artguide](https://github.com/thebluetonguegiraffe/artguide), which just shells out to `curl`, or [llm_lens](https://github.com/llm-lens-agent/llm-lens-agent), whose repo is mounted and invoked with `uv run`). There is no image to build or push; you only need the project folder mounted and, if it has Python dependencies, `uv` (already installed in `Dockerfile.airflow`) to sync and run it on the fly. Use this for lightweight scripts or projects with no conflicting dependencies.

Both patterns coexist side by side in the same Airflow instance; the DAG's own code decides which one it uses, nothing in the Airflow configuration has to change between them.

## Repository Components

* **`Dockerfile.airflow`** : Defines the custom Docker image for the Airflow environment. It extends `apache/airflow:2.9.0` and installs:
  * **`apache-airflow-providers-docker`** and **`apache-airflow-providers-postgres`**, so the `DockerOperator`/`Docker Registry` connection type and the Postgres metadata backend are available at runtime.
  * **[`uv`](https://docs.astral.sh/uv/)**, used by **native DAGs** to install a mounted project's dependencies and run its scripts without building a separate image.
* **`airflow.yaml`**: The main Docker Compose file. It defines and orchestrates the `airflow` and `postgres` containers, specifying volumes, ports, and environment variables.

### Airflow Environment Configuration

The `airflow.yaml` configuration ensures an operational setup ready for task execution:

- Airflow is configured to run in **Standalone** mode (webserver, scheduler and triggerer in a single container), backed by a **Postgres** container and the **LocalExecutor**, so tasks run as parallel OS processes instead of the single-threaded SQLite setup this used to have. Scaling further can be achieved by migrating to a **Celery Executor** or implementing the **KubernetesPodOperator** for high-concurrency needs.
- A fallback SMTP email configuration is set up for the Airflow core to report immediate failures.
- Mapping is performed between the repo and the container directories:
  * **Logs:** The log directory is mounted to persist logs on the host.
  * **Plugins:** The `plugins` directory is mapped to allow the use of custom Airflow scripts, hooks, and extensions.
  * **DAGs:** DAGs are not defined in this repository but are mounted from their respective source repositories via dedicated volumes. Projects running **containerized DAGs** only need their `dags`/`airflow_dags` folder mounted (the actual code lives in the image). Projects running **native DAGs** additionally mount the *whole* project repo into the container (see the `llm_lens` entries in `airflow.yaml`), since the code is executed in place with `uv`.
  * **Docker Socket:** The `/var/run/docker.sock` socket of the host is mounted into the container to enable Airflow to launch and manage sibling Docker containers (required for containerized DAGs).

Finally, the local web server port has been redirected to a [public URL](https://airflow.thebluetonguegiraffe.online/home) to facilitate easy access to the Airflow User Interface (UI).

## How to create a new DAG

Pick the pattern that fits the project: **containerized** if it needs its own image/dependencies, **native** if it's a lightweight script or has no conflicting dependencies.

### Option A: Containerized DAG (`DockerOperator`)

---

```
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

DOCKER_IMAGE_NAME = "ghcr.io/thebluetonguegiraffe/the_news_hub:latest"

default_args = {
    "owner": "the_blue_tongue_giraffe_dev",
    "depends_on_past": False,
    "email": ["thebluetonguegiraffe@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

dag = DAG(
    "ingest_api_news_dag",
    default_args=default_args,
    description="DAG for API news ingestion running on Docker",
    schedule="55 23 * * *",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["ingestion", "news", "docker"],
)

ingest_news = DockerOperator(
    task_id="ingest_api_news",
    image=DOCKER_IMAGE_NAME,
    api_version='auto',
    auto_remove=True,
    command="python scripts/db_population/ingest_api_news.py --date {{ ds }}T23:55",
    environment={
        "PYTHONPATH": "/the_news_hub",
        "CHROMA_DB_TOKEN": "{{ var.value.CHROMA_DB_TOKEN }}",
        "FINLIGHT_API_TOKEN": "{{ var.value.FINLIGHT_API_TOKEN }}",
        "GITHUB_TOKEN": "{{ var.value.GITHUB_TOKEN }}",
    },
    network_mode="bridge",
    docker_url="unix://var/run/docker.sock",
    dag=dag,
    docker_conn_id="ghcr_test",
    force_pull=True
)
```

The file contains the definition of the DAG object with its `default_args` and `DockerOperator` where:

* The Docker image to be used is specified.
* The command to run inside the container is defined.
* Any required variables are provided via the `environment` parameter. Secrets should be passed using Airflow Variables.
* **`force_pull`** is enabled to ensure the image is refreshed before each run (recommended when using dynamic tags like `:latest`).
* **`auto_remove`** is set to **`True`** so the container is deleted when the task finishes.
* **`network_mode`** is typically set to  **`'bridge'`** , allowing the container to reach external services or host-level resources (e.g., PostgreSQL or MongoDB).
* **`docker_conn_id`** must correspond to a connection configured in the Airflow UI ( **Admin → Connections** , type  **`Docker`** ), providing the necessary credentials, such as a PAT token, for pulling private images from registries like GHCR.
* **`docker_url`**, which is a critical setting that defines how the Docker client (running inside the Airflow container) communicates with the Docker Daemon on the host system. This is required so Airflow can launch and manage Docker containers directly on the VPS/server

### Creating a repo image and pushing it into GHCR

To create a Docker image from an existing repository, you need to create a `Dockerfile` that specifies:

* The Python version to use
* The dependencies required by the application
* Which project files should be included in the image
* How to set up and activate the virtual environment for those dependencies

```
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /the_news_hub

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project --no-dev

COPY config.py .
COPY src ./src
COPY scripts ./scripts
COPY templates ./templates
RUN uv sync --frozen --no-dev

ENV PATH="/the_news_hub/.venv/bin:$PATH"

```

Finally the image must be update in a Docker container Registry as **GHCR**.

```
echo $(CR_PAT) | docker login ghcr.io -u $(GITHUB_USER) --password-stdin
docker build --no-cache -t image_name:tag .
docker push image_name:tag
```

### Option B: Native DAG (no image, runs inside the Airflow container)

For lightweight tasks that don't need isolation, skip the image entirely and run the task directly in the Airflow container. All you need is the project folder mounted (see `airflow.yaml`) and, for pure shell/API calls, nothing else:

```
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "the_blue_tongue_giraffe_dev",
    "depends_on_past": False,
    "email": ["thebluetonguegiraffe@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

dag = DAG(
    "keep_qdrant_alive_daily",
    default_args=default_args,
    description="Daily task to ping Qdrant API to keep the cluster alive",
    schedule="0 12 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "qdrant", "api"],
)

ping_qdrant = BashOperator(
    task_id="ping_qdrant_api",
    bash_command="curl -s -X GET https://my-qdrant-cluster/collections -H 'api-key: {{ var.value.QDRANT_API_KEY }}'",
    dag=dag,
)
```

If the project has its own Python dependencies, mount the full repository (not just its `dags` folder) and use [`uv`](https://docs.astral.sh/uv/), already available in the Airflow image, to sync and run it in place, without polluting Airflow's own environment:

```
sync_and_run = BashOperator(
    task_id="sync_notion_knowledge",
    bash_command=(
        "cd {{ var.value.MY_PROJECT_REPO_PATH }} && "
        "export UV_CACHE_DIR=/tmp/uv-cache && "
        "uv sync --only-group my_group && "
        "uv run --no-sync python scripts/my_script.py"
    ),
    env={"MY_API_KEY": "{{ var.value.MY_API_KEY }}"},
    append_env=True,  # keep PATH etc. so `uv` resolves on the worker
    dag=dag,
)
```

Key points:

* **No image build/push step**; the project's own repo is mounted straight into the Airflow container's filesystem, so changes are picked up on the next run without rebuilding anything.
* **Secrets** are still injected via Airflow Variables (`{{ var.value.MY_VAR }}`), exactly as with containerized DAGs; never hardcode credentials in the DAG file.
* **`append_env=True`** is important when using `uv`/other binaries installed on `PATH`, otherwise `env` fully replaces the task's environment instead of extending it.
* Because this runs on the Airflow container itself (not a disposable sibling container), avoid anything long-running, resource-heavy, or with dependencies that could conflict with Airflow's own Python environment; that's what Option A is for.
