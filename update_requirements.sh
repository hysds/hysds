#!/bin/bash

# Ensure we're in the right directory
cd "$(dirname "$0")"

# Install pip-tools if not already installed
pip install --upgrade pip
pip install pip-tools

# Update requirements.txt
pip-compile --upgrade --output-file=requirements.txt requirements.in

# Update dev-requirements.txt
pip-compile --upgrade --output-file=dev-requirements.txt dev-requirements.in

echo "Requirements files have been updated successfully!"
echo "To install the requirements, run:"
echo "  pip install -r requirements.txt"
echo "  pip install -r dev-requirements.txt"
