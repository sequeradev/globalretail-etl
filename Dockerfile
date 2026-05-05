# ==============================================================================
# Custom Airflow image for GlobalRetail ETL project
# Baking requirements into the image is faster and more reliable than
# using _PIP_ADDITIONAL_REQUIREMENTS at runtime.
# ==============================================================================
FROM apache/airflow:2.9.3-python3.11

USER root

# System deps (minimal — mostly for potential wheel builds)
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
