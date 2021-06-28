import argparse
import collections
import importlib
import logging
import os
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
        return not exists

    def extend(self, replica_number):
        """Copy the sample data into replica table with values adjusted so that this replica
        doesn't collide in any fashion with other replicas.

        :param replica_number: number of replica to copy; one means the first replica, which
        should be copied with no column value adjustments
        """
        self.copy_data(replica_number)

    def copy_data(self, replica_number, start=None, end=None, source=None):
        """Copy data from the table in the sample schema to the corresponding table in the
        replica schema.

        If the adjustments are configured, they are applied to sample data with a multiplier of
        replica_number - 1 (so no effect on first replica).

        :param replica_number: one-origin replica count
        :param start: inclusive beginning range of timestamps of sample data to copy
        :param end: exclusive end of range of timestamps of sample data to copy
        :param source: name of source or data, purely for logging
        :return: number of records copied
        """
        start = start or self.args.sample_start
        end = end or self.args.sample_end
        cond = f"{self.time_col} >= '{start}' AND {self.time_col} < '{end}'" if self.time_col \
            else 'true'
        select_list = self.__adjusted_columns(replica_number)
        t0 = datetime.now()
        n = db.execute(f"INSERT INTO {self.extended} ({', '.join(self.columns)}) "
                       f"SELECT {select_list} FROM {self.table} WHERE {cond}")
        from_desc = f" from {source}" if source else ""
        logger.info(f"Wrote {n} records{from_desc} to {self.extended} in {datetime.now() - t0}")
        return n

    def __adjusted_columns(self, replica_number):
        cols = self.columns
        for adjustment in self.adjustments:
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
                .log_config(logger, logging.INFO)
            return True
        else:
            return False

    def extend(self, replica_number):
        super().extend(replica_number)
        # after all replicas have been copied, compress any chunk partially filled when copying
        # final (earliest) replica (unless it's also the latest existing chunk)
        if replica_number == self.args.replica_count:
            self.compress_compressible_chunks(datetime.min)

    def copy_data(self, replica_number, start=None, end=None, source=None):
        """Implementation of copy_data for a hypertable extender.

        The copies are done chunk-by-chunk, from latest to earliest chunk. After each chunk is
        copied, if any chunks in the replica table are now eligible for compression, they are
        compressed. A chunk is eligible for compression if it is not the latest chunk, and if its
        time range lies wholly beyond the timestamp of the last record inserted.

        :param replica_number: one-origin replica number
        :param start: inclusive lower bound of timestamps to copy
        :param end: exclusive upper bound of timestamps to copy
        :param source: source of data, purely for loging
        :return: number of records copied
        """
        start = start or self.args.sample_start
        end = end or self.args.sample_end
        chunks = self.sample_htconfig.chunk_ranges
        n = 0
        t0 = datetime.now()
        for c in chunks:
            _start, _end = max(start, c.start), min(end, c.end)
            if _start < _end:
                source = f"from chunk #{c.number}"
                logger.debug(f"Copying records from {source}[{c.start}..{c.end}] "
                             f"of {self.table}")
                n += super().copy_data(replica_number, start=_start, end=_end, source=source)
                self.compress_compressible_chunks(
                    _start - (replica_number - 1) * self.args.replica_gap_delta)
            else:
                logger.debug(f"Skipping chunk {c.number} of {self.table}: no range overlap")
        logger.info(f"Copied {n} records total from {self.table} to {self.extended} in "
                    f"{datetime.now() - t0}")

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
                logger.info(f"Compressed chunk {chunk_name} of {self.extended}: "
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
            self.copy_data(replica_number)


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
                            f"FROM {self.table}").fetchone()['span']
        self.adjustments = [Adjustment('action_oid', oid_span + 1)]


class WastedFileExtender(Extender):
    """Extender for wasted_file table. We don't actually create any replica data, because this
    table is completely rewritten on every ingestion."""

    def __init__(self, args):
        super().__init__('wasted_file', args)

    def extend(self, replica_number):
        if replica_number == 1:
            self.copy_data(1)


class ScopeExtender(Extender):
    """Extender for scope table.

    This table requires very different handling that other tables because of the internal
    structure of the data.
    """

    def __init__(self, args):
        super().__init__('scope', args)
        self.inf_time = datetime(year=9999, month=12, day=31, hour=0, minute=0, second=0,
                                 tzinfo=tz.tzutc())

    def extend(self, replica_number):
        # we have have different implementations for first replica, last replica, and all others
        replica_count = self.args.replica_count
        t0 = datetime.now()
        n = self.extend_first_replica() if replica_number == 1 \
            else self.extend_last_replica(replica_number) if replica_number == replica_count \
            else self.extend_interior_replica(replica_number)
        logger.info(f"Wrote {n} records to {self.extended} in {datetime.now() - t0}")

    def extend_first_replica(self):
        """
        For first replica we copy any record that either starts after the start timestamp of the
        sample data, or has a non-infinite finish. Such records are wholly contained within the
        interior of the sample data and so will appear in each replica, with times adjusted.

        Except that if this is also the final replica (i.e. replica_count == 1), then we just
        copy all the records as-is.

        :return: number of records written
        """
        if self.args.replica_count != 1:
            n = db.execute(
                f"INSERT INTO {self.extended} SELECT * FROM {self.table} "
                f"WHERE (start > '{self.args.sample_start}' OR finish != '{self.inf_time}') "
                f"AND seed_oid != 0")
            logger.debug(f"First replica wrote {n} scope records")
            return n
        else:
            n = db.execute(
                f"INSERT INTO {self.extended} SELECT * FROM {self.table}")
            logger.debug(f"First (and only) replica wrote {n} scope records")

    def extend_last_replica(self, replica_number):
        """For last replica, we write all records (including the special marker with seed_oid =
        scoped_oid = 0, and finish = time of last topology processed, used during restart).

        In any record with start > sample_start and finish = INF, we change the finish to
        replica-end, since that scope will reappear in the next replica and should not be
        considered active at the start of that replica.

        When start > sample_start and finish != INF the record is wholly contained in the
        replica and is copied with times adjusted.

        When start = sample_start and finish = INF, we copy the record with the start time
        adjusted and the finish left at INF. This is a scope that was present throughout the sample
        data. We have suppressed it in other replicas because it will appear here with finish =
        INF and will therefore be active throughout the extended data

        :param replica_number: one-based replica number
        :return: number of records written
        """
        time_shift = self.__get_time_shift(replica_number)
        n = db.execute(
            f"INSERT INTO {self.extended} (seed_oid, scoped_oid, scoped_type, start, finish) "
            f"SELECT seed_oid, scoped_oid, scoped_type, start - {time_shift}, "
            f"'{self.args.sample_end}'::timestamptz - {time_shift} FROM {self.table} "
            f"WHERE start > '{self.args.sample_start}' AND finish = '{self.inf_time}' "
            f"AND seed_oid != 0")
        logger.debug(f"Last replica (start > sample_start, finish = INF) wrote {n} scope records")
        total = n
        n = db.execute(
            f"INSERT INTO {self.extended} (seed_oid, scoped_oid, scoped_type, start, finish) "
            f"SELECT seed_oid, scoped_oid, scoped_type, "
            f"  start - {time_shift}, finish FROM {self.table} "
            f"WHERE start = '{self.args.sample_start}' AND finish = '{self.inf_time}'"
            f"AND seed_oid != 0")
        logger.debug(f"Last replica (start = sample_start, finish = INF) wrote {n} scope records")
        total += n
        n = db.execute(
            f"INSERT INTO {self.extended} (seed_oid, scoped_oid, scoped_type, start, finish) "
            f"SELECT seed_oid, scoped_oid, scoped_type, "
            f"  start - {time_shift}, finish - {time_shift} FROM {self.table} "
            f"WHERE finish != '{self.inf_time}' AND seed_oid != 0")
        logger.debug(f"Last replica (start = sample_start, finish = INF) wrote {n} scope records")
        total += n
        n = db.execute(
            f"INSERT INTO {self.extended} (seed_oid, scoped_oid, scoped_type, start, finish) "
            f"SELECT seed_oid, scoped_oid, scoped_type, start, finish FROM scope "
            f"WHERE seed_oid = 0")
        logger.debug(f"Last replica (seed = 0 marker) wrote {n} scope records")
        return total + n

    def extend_interior_replica(self, replica_number):
        """Write an interior (neither earliest nor latest) replica.

        Any record with start > sample_start and finish = INF, we copy the record with start
        adjusted and finish replaced with replica end, since this record will reappear as a
        disjoint scope in the next replica.

        Any record with finish != INF is copied with both start and finish adjusted. It is wholly
        contained in this replica and will likewise appear in all replicas.

        All other records (start = sample_start and finish = INF) are suppressed since they'll
        be included in a record written for the earliest replica.

        :param replica_number: one-based replica number
        :return: number of records written
        """
        time_shift = self.__get_time_shift(replica_number)
        n = db.execute(
            f"INSERT INTO {self.extended} (seed_oid, scoped_oid, scoped_type, start, finish) "
            f"SELECT seed_oid, scoped_oid, scoped_type, "
            f"  start - {time_shift}, '{self.args.sample_end}'::timestamptz - {time_shift} "
            f" FROM {self.table} "
            f"WHERE start > '{self.args.sample_start}' AND finish = '{self.inf_time}' "
            f"AND seed_oid != 0")
        logger.debug(f"Interior replica (start > sample_start and finish = INF) "
                     f"wrote {n} scope records")
        total = n
        n = db.execute(
            f"INSERT INTO {self.extended} (seed_oid, scoped_oid, scoped_type, start, finish) "
            f"SELECT seed_oid, scoped_oid, scoped_type, "
            f"  start - {time_shift}, finish - {time_shift} FROM {self.table} "
            f"WHERE finish != '{self.inf_time}' AND seed_oid != 0")
        logger.debug(f"Interior replica (finish != INF) wrote {n} scope records")
        return total + n

    def __get_time_shift(self, replica_number):
        return f"INTERVAL '{(replica_number - 1) * self.args.replica_gap_delta}'"


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
            '--log-level', '-l', help='log level',
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


#log_sizes = getattr(importlib.import_module("schema-size-info"), 'log_sizes')


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
    if args.sample_start:
        args.sample_start = datetime.fromisoformat(args.sample_start)
    else:
        args.sample_start = db.query('SELECT min(time) AS min FROM metric').fetchone()['min']
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
    for i in range(1, args.replica_count + 1):
        logger.info(f"Copying sample data to all tables for replica #{i}")
        for extender in extenders:
            logger.info(f"Replicating to table {extender.table} for replica #{i}")
            extender.extend(i)
    logger.info(f"Replication completed in {datetime.now() - t0}")
    if not args.no_log_sizes:
        log_sizes(args.db_schema, db, logger=logger)
        log_sizes(args.replica_schema, db, logger)


if __name__ == '__main__':
    main()
