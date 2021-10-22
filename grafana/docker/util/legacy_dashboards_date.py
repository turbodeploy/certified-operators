#!/bin/env python3

import db
import logging
from logger import logger
import time

# Script to print date string for the legacy dashboards folder, if that folder is required in
# this installation. The conditions are:
#
# * We are provisioning for Embedded Reporting (N.B. this is NOT checked by this script!)
# * The `extractor.entity_old` table is not empty
#
# As mentioned above, the first condition is not tested by this script; the script should only
# be called when the condition has already been verified.
#
# If the conditions are satisfied, the timestamp on which the V1.14 flyway migration for
# `extractor` schema was applied is retrieved, and it is printed to STDOUT. Otherwise, nothing
# is printed.

def main():
    with db.get_endpoint_connection('postgres', 'dbs.query', ['extractor'], user='query') as conn:
        cur = conn.cursor()
        cur.execute("SELECT oid FROM entity LIMIT 1")
        if not cur.fetchone():
            # no legacy data, so print nothing
            return
        cur.execute("SELECT installed_on FROM schema_version WHERE version='1.14'")
        timestamp = cur.fetchone()
        if timestamp:
            print(timestamp[0].strftime('%b %e, %G %H:%M:%S %p'))


if __name__ == '__main__':
    done = False
    while not done:
        try:
            main()
            done = True
        except:
            logger.exception("Failed to access extractor database; retrying in 10 seconds")
        time.sleep(10)
