-- Used by function_app.py. {table} and {sample_file} are filled in by Python.
-- Creates the table from the CSV header of one staged file, if it does not exist yet.
CREATE TABLE IF NOT EXISTS {table}
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION => '@sftp_stage',
        FILE_FORMAT => 'mystry_shopper_csv',
        FILES => ('{sample_file}')
      )
    )
  )
