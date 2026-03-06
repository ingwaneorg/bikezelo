#!/bin/bash

directory="data"
database="orders.db"

if [ ! -f "${directory}/${database}" ]; then
  echo "ERROR: Database not found - ${directory}/${database}"
  exit 1
fi

echo "========================================================================"

sqlite3 ${directory}/${database} <<EOF
SELECT row_id, 
       timestamp,
       customer_id,
       order_amount,
       status
FROM orders
ORDER BY rowid ASC
EOF

echo "------------------------------------------------------------------------"
