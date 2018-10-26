#!/usr/bin/env python
from subprocess import call
import datetime
import time
import os
import logging

DUMP_DIR = '/home/influxdb/influxdb-dump/'
DATABASE_NAME = 'metron'
DEFAULT_DUMP_INTERVAL_SECONDS = 86400  # Default dump interval is one day.
INFLUX_DUMP_INTERVAL_SECONDS = 'INFLUX_DUMP_INTERVAL_SECONDS'
LOG_FORMAT = '%(asctime)s %(levelname)s: %(message)s'

"""
This script will periodically dump the contents of influxdb to the DUMP_DIR.

The dump contents will contain the data between the time of the last dump and the current time.
The schedule for dumping can be configured via the environment variable specified by
INFLUX_DUMP_INTERVAL_SECONDS or it will default to 1 day.
"""


def current_time():
    """
    Get the current time in UTC.

    :return: the current time in UTC.
    """
    return datetime.datetime.utcnow()


def calculate_backup_time_end(backup_time_start, seconds_to_sleep):
    """
    Calculate the time at which to end waiting for the next backup.

    :param backup_time_start: The time at which the last backup started.
    :param seconds_to_sleep: The number of seconds we should wait until starting the next backup.
    :return: The time in the future at which we should start the next backup.
    """
    return backup_time_start + datetime.timedelta(seconds=seconds_to_sleep)


def rfc3339(time):
    """
    Get an rfc3339 format string for a given time.

    :param time: The time to format.
    :return: an rfc3339 format string for the input time
    """
    return str(time.isoformat()).partition(".")[0] + "Z"


def schedule_interval_seconds():
    """
    Get the number of seconds to wait between dumping the database.

    This value is pulled from the INFLUX_DUMP_INTERVAL_SECONDS environment variable
    or if that does not exist, we return the default.

    :return: the number of seconds to wait between dumping the database.
    """
    return int(os.environ[INFLUX_DUMP_INTERVAL_SECONDS]) if INFLUX_DUMP_INTERVAL_SECONDS \
                                                            in os.environ else DEFAULT_DUMP_INTERVAL_SECONDS


logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
backup_time_start = current_time()
seconds_to_sleep = schedule_interval_seconds()
backup_time_end = calculate_backup_time_end(backup_time_start, seconds_to_sleep)

while True:
    backup_time_start_rfc3339 = rfc3339(backup_time_start)
    backup_time_end_rfc3339 = rfc3339(backup_time_end)

    # Sleep for the scheduled amount of time.
    logging.info("Sleeping for {} seconds between {} and {} "
                 "before triggering next influx dump.".format(seconds_to_sleep,
                                                              backup_time_start_rfc3339,
                                                              backup_time_end_rfc3339))
    time.sleep(seconds_to_sleep)

    # Trigger the influx backup. The call method blocks until the subprocess exits and returns
    # its return code.
    logging.info("Triggering influx dump between {} and {}.".format(backup_time_start_rfc3339,
                                                                    backup_time_end_rfc3339))
    dump_status = call(["influxd", "backup", "-portable", "-database", DATABASE_NAME, "-start",
                        backup_time_start_rfc3339, "-end", backup_time_end_rfc3339, DUMP_DIR])
    logging.info("Influx dump completed with status {}".format(dump_status))

    # Set the next start time to be the last end time. Note that we do the calculation here
    # so that time does not slip forward due to the time lost to perform the actual backup.
    next_backup_time = calculate_backup_time_end(backup_time_end, schedule_interval_seconds())
    backup_time_start = backup_time_end
    backup_time_end = next_backup_time
    seconds_to_sleep = (backup_time_end - backup_time_start).total_seconds()
