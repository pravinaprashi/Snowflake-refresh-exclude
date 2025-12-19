# Snowflake Refresh Exclude

This repository contains Snowflake stored procedures and a scheduled task
to refresh tables from DNA schemas into WORKATO_DB.PUBLIC while excluding
specific columns.

## Folder structure
- procedures/  : Stored procedures
- tasks/       : Snowflake tasks
- monitoring/  : Queries to monitor task execution

## Deployment order
1. procedures/refresh_tables_exclude_column.sql
2. procedures/run_refresh_exclude_batch.sql
3. tasks/daily_refresh_exclude.sql

## Monitoring
Use monitoring/task_history.sql.
A run is successful when STATE = 'SUCCEEDED'.