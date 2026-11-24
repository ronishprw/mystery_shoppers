-- 02_file_format_stage.sql
-- Safe to run repeatedly. The function also runs this file on every execution.

USE ROLE DATA_ENGINEER;
USE WAREHOUSE WH_DATA_ENGINEER;
USE SCHEMA METRO_BRONZE_DB.MYSTERY_SHOPPER;

-- file formate
CREATE FILE FORMAT IF NOT EXISTS mystry_shopper_csv
  TYPE = CSV 
  PARSE_HEADER = TRUE 
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Bronze tables have two extra audit columns (_SOURCE_FILE, _LOADED_AT) that are not in
-- the CSV files, so the column count of file and table differs. Allow that.
ALTER FILE FORMAT mystry_shopper_csv SET ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

--  ff stage
CREATE STAGE IF NOT EXISTS sftp_stage 
  FILE_FORMAT = (FORMAT_NAME = 'mystry_shopper_csv');
