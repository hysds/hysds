#!/bin/bash
# Wrapper to find watchdog_job_timeouts.py in either editable or PyPI install
 
# Detect node type based on which virtualenv exists
if [ -d "$HOME/mozart" ]; then
    NODE_DIR="mozart"
elif [ -d "$HOME/sciflo" ]; then
    NODE_DIR="sciflo"
elif [ -d "$HOME/metrics" ]; then
    NODE_DIR="metrics"
elif [ -d "$HOME/verdi" ]; then
    NODE_DIR="verdi"
else
    echo "ERROR: Cannot detect node type (no mozart/metrics/sciflo/verdi directory found)" >&2
    exit 1
fi
 
# Try editable install location first
EDITABLE_PATH="$HOME/$NODE_DIR/ops/hysds/scripts/watchdog_job_timeouts.py"
if [ -f "$EDITABLE_PATH" ]; then
    exec python "$EDITABLE_PATH" "$@"
fi
 
# Fall back to PyPI install location
PYPI_PATH=$(python -c "import sysconfig, os; print(os.path.join(sysconfig.get_path('data'), 'share', 'hysds', 'scripts', 'watchdog_job_timeouts.py'))" 2>/dev/null)
if [ -f "$PYPI_PATH" ]; then
    exec python "$PYPI_PATH" "$@"
fi
 
echo "ERROR: Cannot find watchdog_job_timeouts.py" >&2
exit 1
