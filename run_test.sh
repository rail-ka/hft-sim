#!/usr/bin/env bash
set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <config_file>"
    echo "Example: $0 configs/baseline.json"
    exit 1
fi

CONFIG_FILE=$1

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Configuration file $CONFIG_FILE not found."
    exit 1
fi

# Run the HFT application with the specified config
hft "$CONFIG_FILE"
