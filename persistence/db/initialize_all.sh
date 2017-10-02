#!/bin/bash

echo "Running initialize_all..."

mysql -uroot -pvmturbo < create.mysql.sql
mysql -uroot -pvmturbo < grants.sql

echo "Database initialized!"
