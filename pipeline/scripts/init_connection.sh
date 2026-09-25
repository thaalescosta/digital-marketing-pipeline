#!/bin/bash
set -e

# Initialize Airflow connection at container startup
echo "Initializing Airflow connection..."

# Enable connection testing in Airflow config (avoid duplicate section)
if ! grep -q "^\[core\]" /usr/local/airflow/airflow.cfg; then
    echo -e "\n[core]\ntest_connection = True" >> /usr/local/airflow/airflow.cfg
else
    # Check if test_connection is already set
    if ! grep -q "test_connection" /usr/local/airflow/airflow.cfg; then
        sed -i '/^\[core\]/a test_connection = True' /usr/local/airflow/airflow.cfg
    fi
fi

# Ensure database is migrated
airflow db migrate

# Create or update GCP connection
airflow connections delete google_cloud_default 2>/dev/null || true
airflow connections add google_cloud_default \
    --conn-type google_cloud_platform \
    --conn-host '' \
    --conn-login '' \
    --conn-password '' \
    --conn-port 0 \
    --conn-schema '' \
    --conn-extra '{"project": "digital-marketing-509604", "keyfile_dict": "/usr/local/airflow/include/gcp_creds/service_account.json"}'

# Start the original entrypoint
exec /entrypoint "$@"