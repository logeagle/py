#!/bin/bash

# Backend (Log Processor) setup and run script

# Set variables
VENV_NAME="backend_venv"
SCRIPT_NAME="log_processor.py"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_NAME" ]; then
    echo "Creating virtual environment..."
    python -m venv $VENV_NAME
fi

# Activate virtual environment
source $VENV_NAME/bin/activate

# Install or upgrade pip
pip install --upgrade pip

# Install requirements
if [ -f "requirements.txt" ]; then
    echo "Installing requirements..."
    pip install -r requirements.txt
else
    echo "requirements.txt not found. Please make sure it exists in the current directory."
    exit 1
fi

# Run the log processor script
if [ -f "$SCRIPT_NAME" ]; then
    echo "Running log processor..."
    python $SCRIPT_NAME "$@"
else
    echo "$SCRIPT_NAME not found. Please make sure it exists in the current directory."
    exit 1
fi

# Deactivate virtual environment
deactivate