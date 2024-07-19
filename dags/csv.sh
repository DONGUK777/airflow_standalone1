#!/bin/bash

YYYYMMDD=$1

echo "csv"
CSV_PATH=~/data/done/${YYYYMMDD}
CSV_PATH_FILE="${CSV_PATH}/_CSV"

if [ -e "$CSV_PATH_FILE" ]; then
        figlet "move on"
        exit 0
else
        echo " back => $CSV_PATH_FILE"
        exit 1
fi
