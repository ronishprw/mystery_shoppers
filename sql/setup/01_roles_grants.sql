-- 01_roles_grants.sql
-- One-time setup. Run as ACCOUNTADMIN or SECURITYADMIN in a Snowflake worksheet.

-- CREATE ROLE IF NOT EXISTS DATA_ENGINEER;
-- GRANT USAGE ON WAREHOUSE WH_DATA_ENGINEER TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON DATABASE METRO_BRONZE_DB TO ROLE DATA_ENGINEER;
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
  ON SCHEMA METRO_BRONZE_DB.MYSTERY_SHOPPER TO ROLE DATA_ENGINEER;

-- Give the role to the user the job runs as (the dot in the name needs quotes).
GRANT ROLE DATA_ENGINEER TO USER "RONISH.SHAH";

-- Attach the public key for key-pair login.
-- Paste the key text only, without the BEGIN/END lines.
-- ALTER USER "RONISH.SHAH" SET RSA_PUBLIC_KEY = '<public key text>';

-- Recommended for a scheduled job: a dedicated service user instead of a personal login.
-- CREATE USER SVC_SFTP_LOADER
--   TYPE = SERVICE
--   DEFAULT_ROLE = DATA_ENGINEER
--   DEFAULT_WAREHOUSE = WH_DATA_ENGINEER
--   RSA_PUBLIC_KEY = '<public key text>';
-- GRANT ROLE DATA_ENGINEER TO USER SVC_SFTP_LOADER;
