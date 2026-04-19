import os

# Secret key — override via env var in production
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-in-env")

# Metadata database (where Superset stores dashboards, charts, users)
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ.get('POSTGRES_USER', 'postgres')}"
    f":{os.environ.get('POSTGRES_PASSWORD', 'postgres')}"
    f"@postgres:5432/superset"
)

# Allow all features useful for a demo
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "GLOBAL_ASYNC_QUERIES": False,
}

# Disable CSRF for local dev (re-enable for anything internet-facing)
WTF_CSRF_ENABLED = False

# Row limit for SQL Lab queries
SQL_MAX_ROW = 100000
DISPLAY_MAX_ROW = 10000

# Allow longer query timeouts (useful for large mart queries)
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300
