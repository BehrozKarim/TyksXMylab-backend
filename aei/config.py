import os


# Local development credentials only; the Compose service is bound to loopback.
DEFAULT_DATABASE_URL = "postgresql://aei:aei_local_only@127.0.0.1:55432/aei"


def database_url() -> str:
    return os.environ.get("AEI_DATABASE_URL", DEFAULT_DATABASE_URL)


def database_schema() -> str:
    return os.environ.get("AEI_DATABASE_SCHEMA", "aei")
