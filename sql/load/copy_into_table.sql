-- Used by function_app.py. {table} and {pattern} are filled in by Python.
-- Snowflake remembers loaded files for 64 days, so reruns skip files already loaded.
COPY INTO {table}
FROM @sftp_stage
PATTERN = '{pattern}'
FILE_FORMAT = (FORMAT_NAME = 'mystry_shopper_csv')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
