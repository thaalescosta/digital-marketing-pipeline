# Astro Runtime base image for the digital-marketing pipeline.
#
# M5 VERIFY (PRD 0.3 / DoD AT-012 — pin a mutually compatible set and record it):
#   1. Runtime version. As of 2026-09-24 the Airflow 3.x-based Runtime line is 3.3.x
#      (Airflow 3.3.1, Python 3.12-3.14, default 3.14) and 3.2.x (Airflow 3.2.2,
#      default Python 3.13). This image targets 3.2-8 because GX Core 1.x officially
#      supports Python 3.10-3.13 (3.14 is experimental). If you move to Runtime 3.3,
#      pin Python to 3.13 and re-verify GX.
#   2. Registry. quay.io/astronomer/astro-runtime stopped at the Airflow 2.x-era line
#      (12.x = Airflow 2.10). Airflow 3.x Runtime images are published at
#      astrocrpublic.azurecr.io/runtime. Confirm the latest 3.2 patch before build.
#   3. The Google provider (apache-airflow-providers-google) ships in Astro Runtime by
#      default; confirm it is present so the google_cloud_default connection and the
#      Cosmos GoogleCloudServiceAccountDictionaryProfileMapping resolve.
FROM astrocrpublic.azurecr.io/runtime:3.2-8

# Astro convention: requirements.txt (pip) and packages.txt (apt).
COPY requirements.txt packages.txt /tmp/
RUN pip install --user --no-cache-dir -r /tmp/requirements.txt

# System packages must be installed as root; switch back to astro after.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends $(cat /tmp/packages.txt) \
    && rm -rf /var/lib/apt/lists/*
USER astro

# datagen as an editable local package. The astro dev runtime mount re-exposes
# /usr/local/airflow/include/datagen, so the editable .pth keeps resolving at run time.
# Use --chown so the astro user can write egg-info during editable install.
COPY --chown=astro:0 include/datagen /usr/local/airflow/include/datagen
RUN pip install --user --no-cache-dir -e /usr/local/airflow/include/datagen

# Dedicated dbt virtualenv (airflow stays free of dbt dependency conflicts).
# dbt-core 1.12.x is the current stable line (2.0 is still alpha as of 2026-09-24).
# dbt-utils is a dbt package installed via `dbt deps` (packages.yml), not pip.
RUN python -m venv /usr/local/airflow/dbt_venv \
    && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.3 \
        dbt-bigquery==1.12.0

# Dedicated Great Expectations virtualenv (isolates pandas/pyarrow from datagen+Airflow).
# GX Core 1.23.x is the current 1.x line; sqlalchemy-bigquery is the BigQuery dialect.
RUN python -m venv /usr/local/airflow/gx_venv \
    && /usr/local/airflow/gx_venv/bin/pip install --no-cache-dir \
        great-expectations==1.23.1 \
        sqlalchemy-bigquery==1.17.2

# Astro images default to PYTHONPATH=/usr/local/airflow; extend it for the datagen
# src-layout package so `import datagen` works for in-process @task calls.
ENV PYTHONPATH="/usr/local/airflow:/usr/local/airflow/include/datagen/src"

# Enable connection testing for Airflow CLI
RUN echo -e "\n[core]\ntest_connection = True" >> /usr/local/airflow/airflow.cfg

# Service-account key is MOUNTED with the astro project (include/gcp_creds/), never
# baked into the image. google-cloud libs and dbt-bigquery read ADC from this env var.
ENV GOOGLE_APPLICATION_CREDENTIALS="/usr/local/airflow/include/gcp_creds/service_account.json"

# Entrypoint script to initialize connection at runtime
USER root
COPY scripts/init_connection.sh /usr/local/airflow/init_connection.sh
RUN chmod +x /usr/local/airflow/init_connection.sh
USER astro
ENTRYPOINT ["/usr/local/airflow/init_connection.sh"]