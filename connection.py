"""
connection.py - SFTP and Snowflake connection helpers.

Non-secret details have defaults below. Secrets (SFTP password, Snowflake
private key, Snowflake account) are read from environment variables:
  - in Azure: Function App > Settings > Environment variables
  - locally: set them in your terminal before running

Required environment variables:
  SFTP_PWD              SFTP password (from 1Password)
  SF_PRIVATE_KEY_B64    base64 of rsa_key.p8        (use this in Azure)
    or
  SF_PRIVATE_KEY_PATH   path to rsa_key.p8          (use this locally)
  SF_PRIVATE_KEY_PASSPHRASE  only if the private key is encrypted

Optional overrides: SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_DIR,
SF_ACCOUNT, SF_USER, SF_ROLE, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA.
"""

import base64
import os
import re

import paramiko
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# ---------------------------------------------------------------- SFTP ----
SFTP_HOST = os.environ.get("SFTP_HOST", "205.174.24.185")
SFTP_PORT = int(os.environ.get("SFTP_PORT", "1891"))
SFTP_USER = os.environ.get("SFTP_USER", "metroexpress")
SFTP_DIR = os.environ.get("SFTP_DIR", "/uploads")

# ----------------------------------------------------------- Snowflake ----
SF_ACCOUNT = os.environ.get("SF_ACCOUNT", "ZFWHITR-MC64001")
SF_USER = os.environ.get("SF_USER", "RONISH.SHAH")  # or SVC_SFTP_LOADER
SF_ROLE = os.environ.get("SF_ROLE", "DATA_ENGINEER")
SF_WAREHOUSE = os.environ.get("SF_WAREHOUSE", "WH_DATA_ENGINEER")
SF_DATABASE = os.environ.get("SF_DATABASE", "METRO_BRONZE_DB")
SF_SCHEMA = os.environ.get("SF_SCHEMA", "MYSTERY_SHOPPER")


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_sftp():
    """Return (transport, sftp_client). Caller must close both."""
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=_require("SFTP_PWD"))
    return transport, paramiko.SFTPClient.from_transport(transport)


def _load_key(raw: bytes, password, depth: int = 0):
    """Load a private key from PEM (even if its line breaks were lost) or DER bytes."""
    text = raw.decode("utf-8", errors="ignore").strip()

    # Legacy encrypted PEM has header lines inside the body: load it as-is.
    if "Proc-Type:" in text:
        return serialization.load_pem_private_key(raw, password=password)

    m = re.search(r"-{3,}BEGIN ([A-Z ]+?)-{3,}(.*?)-{3,}END \1-{3,}", text, re.S)
    if m:
        label = m.group(1)
        body = "".join(m.group(2).split())
        lines = "\n".join(body[i:i + 64] for i in range(0, len(body), 64))
        pem = f"-----BEGIN {label}-----\n{lines}\n-----END {label}-----\n".encode()
        return serialization.load_pem_private_key(pem, password=password)

    try:  # raw DER bytes
        return serialization.load_der_private_key(raw, password=password)
    except ValueError:
        pass

    # base64 text (a PEM body without BEGIN/END lines, or a key encoded twice)
    decoded = base64.b64decode("".join(text.split()))
    if depth >= 2 or not decoded or decoded == raw:
        raise ValueError("unrecognised private key format")
    return _load_key(decoded, password, depth + 1)


def _private_key_der() -> bytes:
    """Load the Snowflake private key and return it as DER bytes."""
    b64 = (os.environ.get("SF_PRIVATE_KEY_B64") or "").strip()
    path = os.environ.get("SF_PRIVATE_KEY_PATH")
    if b64:
        if re.search(r"-{3,}BEGIN", b64):
            # The secret holds the PEM text itself (not base64). Accept it even if
            # something sits before the BEGIN line, or newlines are written as "\\n".
            raw = b64.replace("\\n", "\n").encode()
        else:
            try:
                raw = base64.b64decode("".join(b64.split()))
            except ValueError as exc:
                raise RuntimeError(
                    "SF_PRIVATE_KEY_B64 is not valid base64. It must be the base64 of "
                    "the .p8 private key file."
                ) from exc
    elif path:
        with open(path, "rb") as f:
            raw = f.read()
    else:
        raise RuntimeError("Set SF_PRIVATE_KEY_B64 or SF_PRIVATE_KEY_PATH")

    # Optional: only needed if the private key file is encrypted
    # (its first line is "-----BEGIN ENCRYPTED PRIVATE KEY-----").
    passphrase = os.environ.get("SF_PRIVATE_KEY_PASSPHRASE")
    password = passphrase.encode() if passphrase else None

    try:
        key = _load_key(raw, password)
    except TypeError as exc:
        raise RuntimeError(
            "The private key is encrypted. Set SF_PRIVATE_KEY_PASSPHRASE "
            "(GitHub secret / App Setting) to its passphrase."
        ) from exc
    except ValueError as exc:
        text = raw.decode("utf-8", errors="ignore")
        raise RuntimeError(
            "The Snowflake private key could not be loaded "
            f"(decoded {len(raw)} bytes, {len(text.splitlines())} lines, "
            f"BEGIN line present: {'-----BEGIN' in text}, "
            f"END line present: {'-----END' in text}; "
            f"secret length {len(b64)}, contains BEGIN: {bool(re.search(r'-{3,}BEGIN', b64))}). "
            "SF_PRIVATE_KEY_B64 must be the base64 of the .p8 PRIVATE key file "
            "(not the .pub file)."
        ) from exc

    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake():
    """Return a Snowflake connection using key-pair authentication."""
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=_private_key_der(),
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )


if __name__ == "__main__":
    # Quick connectivity test: python connection.py
    print(f"Testing SFTP {SFTP_USER}@{SFTP_HOST}:{SFTP_PORT} ...")
    transport, sftp = get_sftp()
    try:
        files = sftp.listdir(SFTP_DIR)
        print(f"  OK - {len(files)} files in {SFTP_DIR}")
        for name in sorted(files):
            print("   ", name)
    finally:
        sftp.close()
        transport.close()

    print(f"Testing Snowflake as {SF_USER} / role {SF_ROLE} ...")
    conn = get_snowflake()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), "
            "CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        print("  OK -", cur.fetchone())
    finally:
        conn.close()
