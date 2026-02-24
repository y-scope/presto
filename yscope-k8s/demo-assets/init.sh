#!/usr/bin/env bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <ip-address>"
    exit 1
fi

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

IP="$1"
FILE="${SCRIPT_PATH}/clp-config.yml"

cp "${FILE}.bak" "$FILE"

sed -i "s|\${REPLACE_IP}|$IP|g" "$FILE"

echo "Replaced \${REPLACE_IP} with $IP in $FILE"
