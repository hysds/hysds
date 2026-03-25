
from celery import Celery

app = Celery("hysds")

# Try to load celeryconfig if it exists (for runtime environments)
# but allow imports to succeed without it (for pip-installed packages)
try:
    app.config_from_object("celeryconfig")
except (ImportError, ModuleNotFoundError):
    # celeryconfig not found - using default Celery configuration
    pass


if __name__ == "__main__":
    app.start()
