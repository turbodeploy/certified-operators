import argparse
import collections
import importlib
import logging
import os
import sys
from datetime import datetime, timedelta
from enum import Enum

import humanize as humanize
from dateutil import tz

from dblib import Database, HypertableConfig

# won't work as a normal import for various reasons
log_sizes = getattr(importlib.import_module("schema_size_info"), 'log_sizes')

global db
global logger


class Adjustment:
    """Class that represents an adjustment to be applied to a copied column value."""

    def __init__(self, col_name, delta):
        """Create a new adjustment.

        The adjustment is not applied for the first replica, used as-is for the second,
        doubled for the third, etc.

        :param col_name: name of column to be adjusted
        :param delta: amount of adjustment
        """
        self.col_name = col_name
        self.delta = delta

    def adjust(self, col_list, multiplier):
        """Apply this adjustment to a list of column names, yielding the same list but with
        this adjustment's column name replaced with an SQL expression that will produce the
        adjusted value.

        :param col_list: list of column names (some may already be adjusted)
        :param multiplier: multiplier for adjustment value
        :return: the updated column list
        """
        return [self.__adjust(col, multiplier) for col in col_list]

    def __adjust(self, col, multiplier):
        if col == self.col_name:
            return f"{col} + {self.__adjustment(multiplier * self.delta)}"
        else:
            return col

    @staticmethod
    def __adjustment(a):
        if isinstance(a, timedelta):
            return f"INTERVAL '{a}'"
        else:
            return str(a)


class Extender:
    """Class to perform data-extension processing for a given table."""

    def __init__(self, table, args, adjustments=None, time_col=None):
        """Create an instance for a table.

        :param table: name of table to be extended
        :param args: command line args
        :param adjustments: any adjustments needed for columns in this table
        :param time_col: time column used to select sample data records, if any
        """
        self.table = table
        self.sample = f"{args.db_schema}.{table}"
        self.extended = f"{args.replica_schema}.{table}"
        self.args = args
        self.adjustments = adjustments or []
        self.time_col = time_col
        self.columns = [row[0] for row in list(db.query(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='{self.args.db_schema}' AND table_name='{table}'"))]
        self.prepare()

    def prepare(self):
        """Prepare this table in the replica schema.

        If the table is missing from the replica schema, it is created with same configuration
        as in the sample data schema. Otherwise it is left as-is, but truncated.

        One possible use-case for pre-creating the table is to test different compression settings
        for a hypertable.

        :return: true if the table is created anew in the replica schema
        """
        exists = db.query(f"SELECT EXISTS(SELECT FROM pg_tables "
                          f"WHERE schemaName='{self.args.replica_schema}' "
                          f"AND tableName='{self.table}') AS exists").fetchone()['exists']
        if exists:
            db.execute(f"TRUNCATE TABLE {self.extended}")
        else:
            db.execute(f"CREATE TABLE {self.extended} (LIKE {self.sample} INCLUDING ALL)")
        self.indexes = db.get_indexes(self.args.replica_schema, self.table)
        return not exists

    def drop_indexes(self):
        """Drop this table's indexes in the replica schema.

        This is done in order to improve speed of record insertion when copying data from base
        table. The indexes can be restored when copying is complete.
        """
        for index in self.indexes:
            index.drop()

    def create_indexes(self):
        """Recreate indexes that were previously dropped, once data copying is complete"""
        for index in self.indexes:
            index.create()

    def finish(self):
        """Peform any required work after all the data copying has completed.
        """
        db.execute(f"ANALYZE {self.extended}")

    def log_stats(self, count, time):
        """Log summary stats for this table's replication.

        :param count: number of records copied into the replica table
        :param time: amount of time spent copying data for this table
        """
        logger.info(f"Copied {count} records from {self.table} to {self.extended} in {time}")

    def extend(self, replica_number):
        """Copy the sample data into replica table with values adjusted so that this replica
        doesn't collide in any fashion with other replicas.

        :param replica_number: number of replica to copy; one means the first replica, which
        should be copied with no column value adjustments
        """
        return self.copy_data(replica_number)

    def copy_data(self, replica_number, start=None, end=None, extra_cond=None, source=None,
                  adjustments=None, source_desc=None):
        """Copy data from the table in the sample schema to the corresponding table in the
        replica schema.

        If the adjustments are configured, they are applied to sample data with a multiplier of
        replica_number - 1 (so no effect on first replica).

        :param replica_number: one-origin replica count
        :param start: inclusive beginning range of timestamps of sample data to copy
        :param end: exclusive end of range of timestamps of sample data to copy
        :param extra_cond: any conditions to add to the standard time-based condition
        :param source: source table to copy records from (defaults to table in sample schema)
        :param source_dest: name of source, purely for logging purposes
        :return: number of records copied
        """
        start = start or self.args.sample_start
        end = end or self.args.sample_end
        source = source or self.table
        adjustments = adjustments or self.adjustments
        cond = f"{self.time_col} >= '{start}' AND {self.time_col} < '{end}'" if self.time_col \
            else 'true'
        if extra_cond:
            cond = f"{cond} AND {extra_cond}"
        select_list = self.__adjusted_columns(replica_number, adjustments)
        t0 = datetime.now()
        n = db.execute(f"INSERT INTO {self.extended} ({', '.join(self.columns)}) "
                       f"SELECT {select_list} FROM {source} WHERE {cond}")
        from_desc = f" from {source_desc}" if source_desc else ""
        logger.debug(f"Copied {n} records{from_desc} in {datetime.now() - t0}")
        return n

    def __adjusted_columns(self, replica_number, adjustments):
        cols = self.columns
        for adjustment in adjustments:
            cols = adjustment.adjust(cols, replica_number - 1)
        return ', '.join(cols)


class HypertableExtender(Extender):
    """Extender implementation base for hypertables.

    This handles capturing and transferring hypertable configuration and chunk-by-chunk data
    copying with compression of chunks as they become eligible.

    Compression eligibility does not currently adhere to retention policy; we simply compress
    every chunk as soon as it becomes later than the earliest data we've written, since we write
    later replicas first. We don't ever compress the latest replica, which leaves the replica
    schema suitable as a target for ingestion, should that be desired.
    """
    def __init__(self, table, args, adjustments=None, time_col=None):
        self.sample_htconfig = HypertableConfig(args.db_schema, table, db)
        super().__init__(table, args, adjustments, time_col)

    def prepare(self):
        """Implementation of prepare method for hypertable extenders.

        In the case that the super method creates this table in the replica schema, we configure
        it as a hypertable and copy hypertable configuration options from table in the sample
        schema to the table in the replication schema. Limitations in the HypertableConfig
        class apply here (e.g. dimensions beyond the primary time dimension are not copied).

        :return:true if the table was created anew in the replica schema
        """
        if super().prepare():
            # created extended table - configure hypertable same as sample table
            if logger.isEnabledFor(logging.DEBUG):
                self.sample_htconfig.log_config(logger, logging.DEBUG)
            self.sample_htconfig.configure(self.args.replica_schema, self.table)
            HypertableConfig(self.args.replica_schema, self.table, db) \
                .log_config(logger, logging.DEBUG)
            return True
        else:
            return False

    def extend(self, replica_number):
        n = super().extend(replica_number)
        # after all replicas have been copied, compress any chunk partially filled when copying
        # final (earliest) replica (unless it's also the latest existing chunk)
        if replica_number == self.args.replica_count:
            self.compress_compressible_chunks(datetime.min)
        return n

    def copy_data(self, replica_number, start=None, end=None, extra_cond=None, source=None,
                  adjustments=None, source_desc=None):
        """Implementation of copy_data for a hypertable extender.

        The copies are done chunk-by-chunk, from latest to earliest chunk. After each chunk is
        copied, if any chunks in the replica table are now eligible for compression, they are
        compressed. A chunk is eligible for compression if it is not the latest chunk, and if its
        time range lies wholly beyond the timestamp of the last record inserted.

        :param replica_number: one-origin replica count
        :param start: inclusive beginning range of timestamps of sample data to copy
        :param end: exclusive end of range of timestamps of sample data to copy
        :param extra_cond: any conditions to add to the standard time-based condition
        :param source: source table to copy records from (defaults to table in sample schema)
        :param source_dest: name of source, purely for logging purposes
        :return: number of records copied
        """
        start = start or self.args.sample_start
        end = end or self.args.sample_end
        source = source or self.table
        adjustments = adjustments or self.adjustments
        chunks = self.sample_htconfig.chunk_ranges
        n = 0
        t0 = datetime.now()
        for c in chunks:
            _start, _end = max(start, c.start), min(end, c.end)
            if _start < _end:
                sd = source_desc or f"chunk #{c.number}"
                logger.debug(f"Copying records from {sd}[{c.start}..{c.end}] "
                             f"of {self.table}")
                n += super().copy_data(replica_number, start=_start, end=_end,
                                       extra_cond=extra_cond, adjustments=adjustments,
                                       source=source, source_desc=sd)
                self.compress_compressible_chunks(
                    _start - (replica_number - 1) * self.args.replica_gap_delta)
            else:
                logger.debug(f"Skipping chunk {c.number} of {self.table}: no range overlap")
        return n

    def compress_compressible_chunks(self, start):
        """Compress all eligible chunks for this table.

        :param start: timestamp of earliest record written to table so far
        """
        if not self.sample_htconfig.base['compression_enabled']:
            # do nothing if compression is not enabled on the table
            return
        chunks = list(db.query(f"SELECT * FROM timescaledb_information.chunks "
                               f"WHERE hypertable_schema='{self.args.replica_schema}' "
                               f"AND hypertable_Name='{self.table}'"
                               f"AND range_start >= '{start}' AND not is_compressed "
                               f"ORDER BY range_start DESC "
                               # we always leave the latest chunk uncompressed in case we'll be
                               # performing ingestion against this schema. (And even if we won't,
                               # this will yield more realistic sizing data.)
                               f"OFFSET 1"))
        if chunks:
            for chunk in chunks:
                t0 = datetime.now()
                chunk_schema = chunk['chunk_schema']
                chunk_name = chunk['chunk_name']
                db.execute(f"SELECT compress_chunk('{chunk_schema}.{chunk_name}')")
                before, after = next(db.query(
                    f"SELECT before_compression_total_bytes, after_compression_total_bytes "
                    f"FROM chunk_compression_stats('{self.extended}') "
                    f"WHERE chunk_schema='{chunk_schema}' AND chunk_name='{chunk_name}'"))
                factor = '%.1f' % (before / after)
                logger.debug(f"Compressed chunk {chunk_name} of {self.extended}: "
                             f"{humanize.naturalsize(before)} => {humanize.naturalsize(after)} "
                             f"({factor}x) in {datetime.now() - t0}")
        else:
            logger.debug("No chunks ready for compression")


class EntityExtender(Extender):
    """Extender for the entity table."""

    def __init__(self, args):
        super().__init__('entity', args,
                         adjustments=[Adjustment('first_seen', -args.replica_gap_delta)])

    def extend(self, replica_number):
        """Extend entity records in the sample entity table.

        Extending an entity record just means adjusting its first_seen timestamp into the earliest
        replica, with the same offset form that replica's start time as it has in the sample data
        from the sample start time.
        """
        if replica_number == 1:
            return self.copy_data(replica_number)
        else:
            return 0

class MetricExtender(HypertableExtender):
    """Extender for the metric hypertable."""

    def __init__(self, args):
        super().__init__('metric', args, time_col='time',
                         adjustments=[Adjustment('time', -args.replica_gap_delta)])


class HistoricalEntityAttrsExtender(HypertableExtender):
    """Extender for the historical_entity_attrs hypertable."""

    def __init__(self, args):
        super().__init__('historical_entity_attrs', args, time_col='time',
                         adjustments=[Adjustment('time', -args.replica_gap_delta)]),


class CompletedActionExtender(HypertableExtender):
    """Extender for completed_action hypertable."""

    def __init__(self, args, time_col='completion_time'):
        super().__init__('completed_action', args,
                         adjustments=[Adjustment('completion_time', -args.replica_gap_delta)],
                         time_col='completion_time')


class PendingActionExtender(Extender):
    """Extender for pending_action table."""

    def __init__(self, args):
        super().__init__('pending_action', args)
        oid_span = db.query(f"SELECT max(action_oid) - min(action_oid) AS span "
                            f"FROM {self.table}").fetchone()['span'] or 0
        self.adjustments = [Adjustment('action_oid', oid_span + 1)]


class WastedFileExtender(Extender):
    """Extender for wasted_file table. We only copy into first replica replica data, because this
    table is completely rewritten on every ingestion."""

    def __init__(self, args):
        super().__init__('wasted_file', args)

    def extend(self, replica_number):
        if replica_number == 1:
            return self.copy_data(1)
        else:
            return 0


class ScopeExtender(Extender):
    """Extender for scope table.

    This table requires very different handling that other tables because of the internal
    structure of the data.

    We consider the following scenarios in the sample data:

    * If a record has finish = INF, we'll adjust that record so that its start time lands
      in the final (earliest) replica, so that its scope will continue through all later
      replicas. The record itself will not be duplicated for other replicas.
    * If a record with finish != INF has the same oids as another record with finish = INF,
      that record is moved to the final (earliest) replica). In all following replicas it must
      not appear, since it would overlap with the extension of the matching finish=INF record
      that runs through all replicas (first case)
    * If a record with finish != INF does have the same oids as any other record with finish = INF,
      that record is repeated in every replica, with times adjusted.
    """

    def __init__(self, args):
        super().__init__('scope', args)
        self.inf_time = datetime(year=9999, month=12, day=31, hour=0, minute=0, second=0,
                                 tzinfo=tz.tzutc())

    def extend(self, replica_number):
        # we have have different implementations for first replica, last replica, and all others
        replica_count = self.args.replica_count
        t0 = datetime.now()
        return self.extend_first_replica() if replica_number == 1 \
            else self.extend_other_replicas(replica_number)

    def extend_first_replica(self):
        """
        For the first replica we'll copy all records into the replica table, and then (unless
        replica-count = 1):

        * Adjust start times in records with finish=INF so they fall in the final repcica
        * Move all records with finish != INF that matches another record with FIN=INF so that
          its start and finish times are in the final record.

        After making these adjustments, any record whose start time remains in the first replica
        will be eligible for copying into all other replicas, and that's how we'll process the
        other replicas.

        :return: number of records written
        """
        t0 = datetime.now()
        n = self.copy_data(1)
        logger.debug(
            f"Wrote {n} records into scope table for first replica in {datetime.now() - t0}")
        moved = extended = 0
        if self.args.replica_count != 1:
            shift = f"INTERVAL '{self.args.replica_gap_delta}'"
            # move all records with non-inf finish but for which there is a corresponding
            # inf-finish record so they fall in the earliest scope
            t0 = datetime.now()
            moved = db.execute(
                f"UPDATE {self.extended} "
                f"SET start = scope.start - {shift}, finish = scope.finish - {shift} "
                f"FROM {self.extended} s "
                f"WHERE scope.seed_oid = s.seed_oid AND scope.scoped_oid = s.scoped_oid "
                f"AND scope.finish != '{self.inf_time}' AND s.finish = '{self.inf_time}'")
            logger.debug(
                f"Moved {moved} records to earliest replica in {datetime.now() - t0}")
            t0 = datetime.now()
            extended = db.execute(
                f"UPDATE {self.extended} SET start = start - {shift}"
                f"WHERE finish = '{self.inf_time}'")
            logger.debug(
                f"Moved {extended} record starts to earliest replica in {datetime.now() - t0}")
        return n

    def extend_other_replicas(self, replica_number):
        """Fill in replicas other than first (latest) replica.

        We copy from the extended schema rather than the sample schema, because the adjustments
        already made in creating first replica mean tht we can avoid the relative expensive
        filtering of scope records that are followed by matching inf-finish records. The only
        records with remaining with a start time in the first replica time range are the ones
        we need.

        :param replica_number: replica number
        :return: # of records written into this replica
        """
        t0 = datetime.now()
        shift = (replica_number - 1) * self.args.replica_gap_delta
        adjustments = [Adjustment('start', -shift), Adjustment('finish', -shift)]
        n = self.copy_data(replica_number, source=self.extended, adjustments=adjustments,
                           extra_cond=f"start >= '{self.args.sample_start}'")
        logger.debug(f"Wrote {n} records to scope table for replica {replica_number} "
                     f"in {datetime.now() - t0}")
        return n

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
            '--replica-count', '-n', help='number of replicas to create', type=int, default=1)
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
    entity = [EntityExtender]
    metric = [MetricExtender]
    historical_entity_attrs = [HistoricalEntityAttrsExtender]
    completed_action = [CompletedActionExtender]
    pending_action = [PendingActionExtender]
    wasted_file = [WastedFileExtender]
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
    global db
    db = Database(args, logger)
    # query DB to get default start end end times if not given
    # we need to omit cluster-stats records when retrieving min/max times, since those records
    # are always pinned to midnight of the day in which they are observed.
    if args.sample_start:
        args.sample_start = datetime.fromisoformat(args.sample_start)
    else:
        args.sample_start = db.query("SELECT min(time) AS min FROM metric "
                                     "WHERE entity_type != 'COMPUTE_CLUSTER'") \
            .fetchone()['min']
    if args.sample_end:
        args.sample_end = datetime.fromisoformat(args.sample_end)
    else:
        # use max existing time plus 1 ms so max time is included, e.g. in intervals
        one_ms = timedelta(milliseconds=1)
        args.sample_end = db.query('SELECT max(time) AS max FROM metric').fetchone()['max'] + one_ms

    # create a timedelta spanning the sample data start/end timestamps plus the replica gap
    gap_sql = f"SELECT extract(EPOCH FROM INTERVAL '${args.replica_gap}')::bigint AS gap"
    gap_sec = db.query(gap_sql).fetchone()['gap']
    vars(args)['replica_gap_delta'] = \
        (args.sample_end - args.sample_start) + timedelta(seconds=gap_sec)
    return args


def main():
    """Main program - collect command line args, and extend tables as indicated."""
    os.environ['PGOPTIONS'] = '-c statement_timeout=3600000'
    global logger
    logger = logging.getLogger("extend-data")
    logging.basicConfig(format="%(asctime)s:%(levelname)s:%(name)s %(message)s")
    args = get_args()
    logger.setLevel(args.log_level)
    if logger.isEnabledFor(logging.DEBUG):
        for name in sorted(vars(args).keys()):
            logger.debug(f"Command line arg {name}: {vars(args)[name]}")
    if args.init_replica_schema:
        # drop and recreate the replica schema if called for
        logger.info(f"(Re-)Creating replica schema {args.replica_schema}")
        schema = args.replica_schema
        db.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        db.execute(f"CREATE SCHEMA {schema}")
    extenders = [Extendable[table](args) for table in args.extend_tables]
    t0 = datetime.now()
    # keep track of record count and time spent on each table
    tableTimes = {}
    tableCounts = {}
    for i in range(1, args.replica_count + 1):
        logger.info(f"Copying sample data to all tables for replica #{i}")
        for extender in extenders:
            table_t0 = datetime.now()
            if i == 1:
                extender.drop_indexes()
            logger.info(f"Copying data to table {extender.table} for replica #{i}")
            n = extender.extend(i)
            # tie each table off after we complete its last replica
            if i == args.replica_count:
                extender.create_indexes()
                extender.finish()
            # and update overall stats after each replica
            tableCounts[extender] = tableCounts.get(extender, 0) + n
            tableTimes[extender] = tableTimes.get(extender, timedelta(0)) \
                                   + (datetime.now() - table_t0)
    for extender in extenders:
        extender.log_stats(tableCounts[extender], tableTimes[extender])
    logger.info(f"Replication completed in {datetime.now() - t0}")
    if not args.no_log_sizes:
        log_sizes(args.db_schema, db)
        log_sizes(args.replica_schema, db)


if __name__ == '__main__':
    main()
