#!/bin/bash

SCRIPT_DIR=$(cd "$(dirname "$0")/.." && pwd)
database="${SCRIPT_DIR}/data/orders.db"

if [ ! -f "${database}" ]; then
  echo "ERROR: Database not found - ${database}"
  exit 1
fi

echo "========================================================================"

sqlite3 "${database}" <<EOF
SELECT row_id,
       timestamp,
       customer_id,
       order_amount,
       status
FROM orders
ORDER BY rowid ASC
EOF

echo "------------------------------------------------------------------------"
