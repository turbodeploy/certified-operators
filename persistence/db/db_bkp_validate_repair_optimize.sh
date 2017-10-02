#!/bin/bash
#echo 'Creating database backup..'
#sh /srv/rails/webapps/persistence/db/dump_data.sh bkp_vmt_update
echo 'Running validate and repair...'
mysqlcheck -u root -pvmturbo vmtdb --medium-check --auto-repair --check-only-changed
#mysqlcheck -u root -pvmturbo vmtdb --optimize
echo 'Validation script finished'
