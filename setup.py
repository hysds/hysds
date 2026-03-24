# Minimal setup.py shim for backward compatibility
# This delegates to pyproject.toml for all configuration
# This file will be removed in a future release (v7.1.0+)
#
# Modern installation (recommended):
#   pip install .
#   pip install -e .
#
# This shim ensures existing scripts that expect setup.py continue to work
from setuptools import setup

setup()
