import os
import sys
from celery import Celery

# Add etc/ directories to Python path for celeryconfig in PyPI installs
# Check common HySDS component directories
home = os.environ.get('HOME', os.path.expanduser('~'))
for component in ['mozart', 'sciflo', 'metrics', 'verdi']:
    etc_dir = os.path.join(home, component, 'etc')
    if os.path.exists(etc_dir) and etc_dir not in sys.path:
        sys.path.insert(0, etc_dir)

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
