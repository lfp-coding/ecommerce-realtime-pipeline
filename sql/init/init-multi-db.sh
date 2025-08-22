#!/bin/bash
# This script initializes the test database in the postgres container.
# It drops the test database if it exists, recreates it, and runs all SQL scripts on it.

set -e

# Drop the test database if it exists
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "DROP DATABASE IF EXISTS $POSTGRES_TEST_DB;"
# Create the test database
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE $POSTGRES_TEST_DB;"

# Run all SQL scripts on the test database
for sql_file in /docker-entrypoint-initdb.d/*.sql; do
    echo "Running $sql_file on $POSTGRES_TEST_DB..."
    psql -U "$POSTGRES_USER" -d "$POSTGRES_TEST_DB" -f "$sql_file"
done
