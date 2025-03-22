#!/bin/bash
# fix race with postgresql

set -e

# Set the PostgreSQL host (adjust if necessary)
host="postgres"

until nc -z "$host" 5432; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

echo "PostgreSQL is up - executing command"
exec airflow webserver
