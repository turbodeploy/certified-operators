from datetime import datetime
from enum import auto, Enum

from dblib import config
from extenders.adjustment import Adjustment


class Extender:
    """Class to perform data-extension processing for a given table."""

    # scaling gap is computed once when the first table is scaled, so it's a static class var
    scaling_gap = None

    def __init__(self, table, args, adjustments=None, time_col=None, oid_cols=None, scale_only=False):
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
        self.oid_cols = oid_cols
        self.scale_only = scale_only
        self.columns = [row[0] for row in list(config.db.query(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema='{self.args.db_schema}' AND table_name='{table}'"))]
        self.prepare()
        self.pkey = config.db.get_primary_key(self.args.replica_schema, self.table)
        self.indexes = config.db.get_indexes(self.args.replica_schema, self.table)
        if not Extender.scaling_gap:
            Extender.scaling_gap = self.compute_scaling_gap()
        # for ease of access

    def is_scale_only(self):
        return self.scale_only

    def prepare(self):
        """Prepare this table in the replica schema.

        If the table is missing from the replica schema, it is created with same configuration
        as in the sample data schema. Otherwise it is left as-is, but truncated.

        One possible use-case for pre-creating the table is to test different compression settings
        for a hypertable.

        :return: true if the table is created anew in the replica schema
        """
        exists = config.db.query(f"SELECT EXISTS(SELECT FROM pg_tables "
                                 f"WHERE schemaName='{self.args.replica_schema}' "
                                 f"AND tableName='{self.table}') AS exists").fetchone()['exists']
        if exists:
            config.db.execute(f"TRUNCATE TABLE {self.extended}")
        else:
            config.db.execute(f"CREATE TABLE {self.extended} (LIKE {self.sample} INCLUDING ALL)")
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

    def drop_primary_key(self):
        """Drop this table's primary key in the replica schema if there is one.

        This is done in order to improve speed of record insertion when copying data from base
        table. The primary key can be restored when copying is complete.
        """
        if self.pkey:
            self.pkey.drop()

    def create_primary_key(self):
        """Recreate a primary that was previously dropped, once data copying is complete"""
        if self.pkey:
            self.pkey.create()

    def finish(self):
        """Perform any required work after all the data copying has completed.
        """
        config.db.execute(f"ANALYZE {self.extended}")

    def log_stats(self, count, time):
        """Log summary stats for this table's replication.

        :param count: number of records copied into the replica table
        :param time: amount of time spent copying data for this table
        """
        config.logger.info(f"Copied {count} records from {self.table} to {self.extended} in {time}")

    def compute_scaling_gap(self):
        sql = f"SELECT max(max_oid) - min(min_oid) + 1 AS gap FROM ( " \
              f"  SELECT max(oid) AS max_oid, min(oid) AS min_oid " \
              f"    FROM {self.args.db_schema}.entity " \
              f"  UNION SELECT max(action_oid) as max_oid, min(action_oid) AS min_oid" \
              f"    FROM {self.args.db_schema}.completed_action " \
              f"  UNION SELECT max(action_oid) as max_oid, min(action_oid) AS min_oid" \
              f"    FROM {self.args.db_schema}.pending_action " \
              f") AS foo"
        gap = config.db.query(sql).fetchone()['gap'] or 1
        config.logger.debug(f"Scaling gap: {gap}")
        return gap

    def extend(self, replica_number):
        """Copy the sample data into replica table with values adjusted so that this replica
        doesn't collide in any fashion with other replicas.

        :param replica_number: number of replica to copy; one means the first replica, which
        should be copied with no column value adjustments
        """
        if replica_number == 1 or not self.scale_only:
            return self.copy_data(replica_number, source=self.extended)
        else:
            return 0

    def scale(self):
        n = 0
        effective_scale_factor = 1 if not self.oid_cols else self.args.scale_factor
        for i in range(1, effective_scale_factor + 1):
            adjustments = [Adjustment(col, Extender.scaling_gap) for col in self.oid_cols or []]
            n += self.copy_data(i, adjustments=adjustments)
        return n

    def copy_data(self, replica_number, start=None, end=None, extra_cond=None, source=None,
                  adjustments=None, source_desc=None, disabled=None):
        """Copy data from the table in the sample schema to the corresponding table in the
        replica schema.

        If the adjustments are configured, they are applied to sample data with a multiplier of
        replica_number - 1 (so no effect on first replica).

        :param replica_number: one-origin replica count
        :param start: inclusive beginning range of timestamps of sample data to copy
        :param end: exclusive end of range of timestamps of sample data to copy
        :param extra_cond: any conditions to add to the standard time-based condition
        :param source: source table to copy records from (defaults to table in sample schema)
        :param adjustments: adjustments to be made to records being copied
        :param source_desc: name of source, purely for logging purposes
        :param disabled: features to be disabled during this copy operation
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
        n = config.db.execute(f"INSERT INTO {self.extended} ({', '.join(self.columns)}) "
                              f"SELECT {select_list} FROM {source} WHERE {cond}")
        from_desc = f" from {source_desc}" if source_desc else ""
        config.logger.debug(f"Copied {n} records{from_desc} in {datetime.now() - t0}")
        return n

    def __adjusted_columns(self, replica_number, adjustments):
        cols = self.columns
        for adjustment in adjustments:
            cols = adjustment.adjust(cols, replica_number - 1)
        return ', '.join(cols)


class Feature(Enum):
    """Features that can be disabled in certain operations, most notably copy operations."""
    compression = auto
    compression_policy = auto
