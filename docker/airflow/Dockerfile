FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl git openjdk-17-jre-headless \
    && JAVA_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && ln -sfn "${JAVA_PATH}" /usr/lib/jvm/default-java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
WORKDIR /opt/project
COPY --chown=airflow:root pyproject.toml README.md /opt/project/
COPY --chown=airflow:root src /opt/project/src
COPY --chown=airflow:root dbt /opt/project/dbt
COPY --chown=airflow:root dashboard /opt/project/dashboard
COPY --chown=airflow:root orchestration /opt/project/orchestration
RUN pip install --no-cache-dir -e /opt/project
