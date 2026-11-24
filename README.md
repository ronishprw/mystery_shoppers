# mystery_shoppers

Weekly load of the provider's `mystery_shopping_*.csv` files from SFTP into Snowflake.

```
SFTP /uploads  ->  Azure Function (timer)  ->  Snowflake stage  ->  COPY INTO tables
```

## Layout

| Path | Purpose |
|---|---|
| `function_app.py` | Timer-triggered function: SFTP download, PUT, COPY INTO |
| `connection.py` | SFTP and Snowflake connection helpers; run it to test both |
| `sql/setup/` | One-time Snowflake setup (roles, grants, file format, stage) |
| `sql/load/` | SQL templates used by the function at runtime |
| `sql/checks/verify.sql` | Queries to verify row counts and load history |
| `.github/workflows/deploy.yml` | Deploys to Azure on every push to `main` |
| `local.settings.json.example` | Template for local settings (copy to `local.settings.json`) |

## Setup order

1. Run `sql/setup/01_roles_grants.sql` in Snowflake as an admin.
2. Create a key pair, attach the public key to the Snowflake user, and base64-encode the private key.
3. Create the Azure Function App (Python 3.11, Linux) and add the App Settings below.
4. Send the Function App's outbound IPs to the SFTP provider for port 1891.
5. Set `AZURE_FUNCTIONAPP_NAME` in `.github/workflows/deploy.yml` and add the repo secret `AZURE_FUNCTIONAPP_PUBLISH_PROFILE`
   (Function App > Overview > Get publish profile).
6. Push to `main`. Run the function once from the portal (Functions > sftp_to_snowflake > Code + Test > Test/Run).
7. Run `sql/checks/verify.sql` to confirm the data loaded.

## App Settings

| Name | Value |
|---|---|
| `SFTP_PWD` | SFTP password (store as a Key Vault reference) |
| `SF_PRIVATE_KEY_B64` | base64 of `rsa_key.p8` (store as a Key Vault reference) |
| `SF_ACCOUNT` | Snowflake account identifier (default in `connection.py`) |

Everything else has a default in `connection.py` and can be overridden with
`SFTP_HOST`, `SFTP_PORT`, `SFTP_USER`, `SFTP_DIR`, `SF_USER`, `SF_ROLE`,
`SF_WAREHOUSE`, `SF_DATABASE` or `SF_SCHEMA`.

## Secrets

Never commit passwords, `.p8` keys or `local.settings.json`. They are listed in `.gitignore`.

## Local test

```
pip install -r requirements.txt
set SFTP_PWD=...                      (PowerShell: $env:SFTP_PWD="...")
set SF_PRIVATE_KEY_PATH=C:\keys\rsa_key.p8
python connection.py
```

The SFTP server only accepts allowlisted IPs, so this works only from an allowlisted network.
