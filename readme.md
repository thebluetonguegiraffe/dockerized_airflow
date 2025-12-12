# Dockerized Airflow Deployment Guide

This repository provides the core infrastructure files required to deploy an Apache Airflow-based automation system on a Virtual Private Server (VPS). The entire orchestration environment is containerized using Docker to ensure portability, stability, and isolation.

## Architectural Principles

The system is built upon the **Docker-on-Docker** pattern, utilizing Ephemeral Containers for task execution.

* **Isolated Orchestration:** The Airflow core components (Webserver, Scheduler) operate within a dedicated container environment.
* **Decoupled Execution:** Each Airflow task runs inside its own  **sibling container** , utilizing a specific Docker image containing the project's code (i.e. [the_news_hub](https://github.com/thebluetonguegiraffe/the_news_hub/pkgs/container/the_news_hub)).
* **Flexibility:** This design decouples Airflow's environment from project dependencies, enabling DAGs to consume code from **multiple external repositories** without risking library conflicts.

## Repository Components

* **`Dockerfile.airflow`** : Defines the custom Docker image for the Airflow environment. It extends `apache/airflow:2.9.0` and installs the necessary **`apache-airflow-providers-docker`** globally to ensure the `DockerOperator` and `Docker Registry` connection type are available at runtime.
* **`docker-compose.yaml`**: The main configuration file that defines and orchestrates the Airflow container, specifying volumes, ports, and environment variables.

### Airflow Environment Configuration

The `docker-compose.yaml` configuration ensures an operational setup ready for task execution:

- Airflow is configured to run in **Standalone** mode, consolidating the architecture into a single container instance. This mode is optimized for local development and low-concurrency environments. Scaling can be achieved by migrating to a **Celery Executor** or implementing the **KubernetesPodOperator** for high-concurrency needs.
- A fallback SMTP email configuration is set up for the Airflow core to report immediate failures.
- Mapping is performed between the repo and the container directories:
  * **Logs:** The log directory is mounted to persist logs on the host.
  * **Plugins:** The `plugins` directory is mapped to allow the use of custom Airflow scripts, hooks, and extensions.
  * **DAGs:** DAGs are not defined in this repository but are mounted from their respective source repositories via dedicated volumes.
  * **Docker Socket:** The `/var/run/docker.sock` socket of the host is mounted into the container to enable Airflow to launch and manage sibling Docker containers.

Finally, the local web server port has been redirected to a [public URL](https://airflow.thebluetonguegiraffe.online/home) to facilitate easy access to the Airflow User Interface (UI).

## How to create a new DAG

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
* **`docker_conn_id`** must correspond to a connection configured in the Airflow UI ( **Admin → Connections** , type  **`Docker`** ), providing the necessary credentials—such as a PAT token—for pulling private images from registries like GHCR.
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
