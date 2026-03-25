#!/bin/bash
set -e

cd "$(dirname "$0")"

source .venv/bin/activate

if [ ! -f "database/ecommerce.db" ]; then
    python scripts/init_db.py
fi

python scripts/demo.py
