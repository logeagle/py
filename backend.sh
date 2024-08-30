#!/bin/bash

# Create logeagle directory if it doesn't exist
mkdir -p ~/logeagle

# Create virtual environment if it doesn't exist
if [ ! -d "$HOME/logeagle/backend_venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$HOME/logeagle/backend_venv"
fi

# Activate virtual environment
source "$HOME/logeagle/backend_venv/bin/activate"

# Upgrade pip
pip install --upgrade pip

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

# Run the log processor
echo "Running log processor..."
python logeagle.py

# Deactivate virtual environment
deactivate