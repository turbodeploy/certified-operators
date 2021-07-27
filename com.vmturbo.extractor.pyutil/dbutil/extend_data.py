import argparse
import collections
import importlib
import logging
import os
from datetime import datetime, timedelta
from enum import Enum

from dblib import Database, config
from extenders.action_group_extender import ActionGroupExtender
from extenders.cloud_service_cost_extender import CloudServiceCostExtender
from extenders.completed_action_extender import CompletedActionExtender
from extenders.entity_cost_extender import EntityCostExtender
from extenders.entity_extender import EntityExtender
from extenders.file_extender import FileExtender
from extenders.historical_entity_attrs_extender import HistoricalEntityAttrsExtender
from extenders.metric_extender import MetricExtender
from extenders.pending_action_extender import PendingActionExtender
from extenders.pending_action_stats_extender import PendingActionStatsExtender
from extenders.scope_extender import ScopeExtender

log_sizes = getattr(importlib.import_module("schema_size_info"), 'log_sizes')


class ArgParser:
    """Command line argument parser."""

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Extractor Data Extender',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        # database connection
        parser.add_argument(
            '--db-host', '-H', help='database host', default='localhost')

        parser.add_argument(
            '--db-port', '-p', help='database port', default='5432')
        parser.add_argument(
            '--db-user', '-U', help='database user name', default='extractor')
        parser.add_argument(
            '--db-password', '-P', help='database password', default='vmturbo')
        parser.add_argument(
            '--db-database', '-D', help='database to connect to', default='extractor')
        parser.add_argument(
            '--db-schema', '-S', help='database schema for sample data', default='extractor')
        # extension arguments
        parser.add_argument(
            '--extend-tables', help='list of tables to extend, omit for all', nargs='*',
            default=[e.name for e in Extendable])
        parser.add_argument(
            '--sample-start', '--start',
            help='beginning of time interval to replicate; default earliest metric',
            type=is_valid_time)
        parser.add_argument(
            '--sample-end', '--end',
            help='end of time interval to replicate; default earliest metric',
            type=is_valid_time)
        parser.add_argument(
            '--replica-count', '-n', help='number of time-dimension replicas to create', type=int,
            default=1)
        parser.add_argument(
            '--scale-factor', '-s',
            help='number of topology-dimension copies to create before replicating, can be fractional',
            type=int, default=1
        )
        parser.add_argument(
            '--replica-gap',
            help='gap between end of one replica and start of next, e.g. 10m or 30s or 10m30s',
            default='10m')
        parser.add_argument(
            '--replica-schema', help='schema into which replicas are written',
            default='extractor_extend')
        parser.add_argument(
            '--init-replica-schema', help='drop and recreate replica schema before copying data',
            action='store_true')
        # miscellaneous
        parser.add_argument(
            '--log-level', '-l', help='log level', type=str.upper,
            choices=[logging.getLevelName(level)
                     for level in logging._levelToName.keys()], default='INFO')
        parser.add_argument(
            '--no-log-sizes', help="don't log final table sizes for sample and replica schemas",
            action="store_true")
        self.parser = parser

    def parse(self):
        return self.parser.parse_args()


class Extendable(Enum):
    """Tables that can be extended by this utility.

    The value of each item contains the class of the corresponding extender class.

    The enum members are themselves callable, and calling one creates a new instance.
    """
    action_group = [ActionGroupExtender]
    cloud_service_cost = [CloudServiceCostExtender]
    completed_action = [CompletedActionExtender]
    entity = [EntityExtender]
    entity_cost = [EntityCostExtender]
    file = [FileExtender]
    historical_entity_attrs = [HistoricalEntityAttrsExtender]
    metric = [MetricExtender]
    pending_action = [PendingActionExtender]
    pending_action_stats = [PendingActionStatsExtender]
    scope = [ScopeExtender]

    def __call__(self, *args, **kwargs):
        """Calling an enum member means creating a new instance."""
        return self.value[0](*args, **kwargs)


def is_valid_time(time):
    """Check that the given datetime string can be parsed.
    :param time: datetime string
    :return: True if it's syntactically valid
    """
    datetime.fromisoformat(time)
    return True


def dedupe(dupes):
    """Remove duplicates from a list."""
    return list(collections.OrderedDict.fromkeys(dupes))


def get_args():
    """Parse command line args and perform any required post-processing."""
    parser = ArgParser()
    args = parser.parse()

    # remove any repeats in the extend-tables list
    args.extend_tables = dedupe(args.extend_tables)
    # if all extend-tables entries are negative, use the full list minus those entities
    tables = [e.name for e in Extendable] \
        if all(t.startswith('!') for t in args.extend_tables) \
        else [t for t in args.extend_tables if not t.startswith('!')]
    # remove negated tables
    [tables.remove(t[1:]) for t in args.extend_tables if t.startswith('!')]
    args.extend_tables = tables
    # set up database access
    config.db = Database(args, config.logger)
    # query DB to get default start end end times if not given
    # we need to omit cluster-stats records when retrieving min/max times, since those records
    # are always pinned to midnight of the day in which they are observed.
    if args.sample_start:
        args.sample_start = datetime.fromisoformat(args.sample_start)
    else:
        args.sample_start = config.db.query("SELECT min(time) AS min FROM metric "
                                            "WHERE entity_type != 'COMPUTE_CLUSTER'") \
            .fetchone()['min']
    if args.sample_end:
        args.sample_end = datetime.fromisoformat(args.sample_end)
    else:
        # use max existing time plus 1 ms so max time is included, e.g. in intervals
        one_ms = timedelta(milliseconds=1)
        args.sample_end = config.db.query('SELECT max(time) AS max FROM metric').fetchone()[
                              'max'] + one_ms

    # create a timedelta spanning the sample data start/end timestamps plus the replica gap
    gap_sql = f"SELECT extract(EPOCH FROM INTERVAL '${args.replica_gap}')::bigint AS gap"
    gap_sec = config.db.query(gap_sql).fetchone()['gap']
    vars(args)['replica_gap_delta'] = \
        (args.sample_end - args.sample_start) + timedelta(seconds=gap_sec)
    return args


def main():
    """Main program - collect command line args, and extend tables as indicated."""
    os.environ['PGOPTIONS'] = '-c statement_timeout=3600000'
    config.logger = logging.getLogger("extend-data")
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s %(message)s")
    args = get_args()
    config.logger.setLevel(args.log_level)
    if config.logger.isEnabledFor(logging.DEBUG):
        for name in sorted(vars(args).keys()):
            config.logger.debug(f"Command line arg {name}: {vars(args)[name]}")
    if args.init_replica_schema:
        # drop and recreate the replica schema if called for
        config.logger.info(f"(Re-)Creating replica schema {args.replica_schema}")
        schema = args.replica_schema
        config.db.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        config.db.execute(f"CREATE SCHEMA {schema}")
    extenders = [Extendable[table](args) for table in args.extend_tables]
    t0 = datetime.now()
    # keep track of record count and time spent on each table
    table_times = {}
    table_counts = {}
    for i in range(1, args.replica_count + 1):
        config.logger.info(f"Copying sample data to all tables for replica #{i}")
        for extender in extenders:
            table_t0 = datetime.now()
            if i == 1:
                extender.drop_indexes()
                extender.drop_primary_key()
            config.logger.info(f"Copying data to table {extender.table} for replica #{i}")
            if i == 1:
                n = extender.scale()
            else:
                n = 0 if extender.is_scale_only() else extender.extend(i)
            # tie each table off after we complete its last replica
            if i == args.replica_count:
                extender.create_primary_key()
                extender.create_indexes()
                extender.finish()
            # and update overall stats after each replica
            table_counts[extender] = table_counts.get(extender, 0) + n
            table_times[extender] = \
                table_times.get(extender, timedelta(0)) \
                + (datetime.now() - table_t0)
    for extender in extenders:
        extender.log_stats(table_counts[extender], table_times[extender])
    config.logger.info(f"Replication completed in {datetime.now() - t0}")
    if not args.no_log_sizes:
        log_sizes(args.db_schema, config.db)
        log_sizes(args.replica_schema, config.db)


if __name__ == '__main__':
    main()
