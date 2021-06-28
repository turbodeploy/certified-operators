import collections

import humanize


class HypertableConfig:
    """Class to manage hypertable configuration for a table."""

    def __init__(self, schema, table, db):
        """Gather configuration information for the given hypertable in the given schema."""
        self.schema = schema
        self.table = table
        self.db = db

        ti = "timescaledb_information"
        cond = f"hypertable_schema='{schema}' AND hypertable_name='{table}'"
        self.base = dict(db.query(f"SELECT * FROM {ti}.hypertables WHERE {cond}").fetchone())
        self.dims = [dict(row) for row in db.query(f"SELECT * from {ti}.dimensions WHERE {cond}")]
        self.segment_by = [row['attname'] for row in db.query(
            f"SELECT attname FROM {ti}.compression_settings "
            f"WHERE {cond} AND segmentby_column_index IS NOT NULL "
            f"ORDER BY segmentby_column_index")]
        self.order_by = [row['attname'] for row in db.query(
            f"SELECT attname FROM {ti}.compression_settings "
            f"WHERE {cond} AND orderby_column_index IS NOT NULL "
            f"ORDER BY orderby_column_index")]
        rows = list(db.query(
            f"SELECT config->>'compress_after' AS compress_after FROM {ti}.jobs WHERE {cond} "
            f"AND proc_schema = '_timescaledb_internal' AND proc_name = 'policy_compression'"))
        self.compress_after = rows[0]['compress_after'] if rows else None
        rows = list(db.query(
            f"SELECT config->>'drop_after' AS drop_after FROM {ti}.jobs WHERE {cond} "
            f"AND proc_schema = '_timescaledb_internal' AND proc_name = 'policy_retention'"))
        self.drop_after = rows[0]['drop_after'] if rows else None
        rows = db.query(f"SELECT {ti}.chunks.*, row_number() "
                        f"OVER (ORDER BY range_start DESC) AS number "
                        f"FROM {ti}.chunks WHERE {cond}")
        ChunkRange = collections.namedtuple('ChunkRange', 'number start end compressed')
        self.chunk_ranges = \
            [ChunkRange(row['number'], row['range_start'], row['range_end'], row['is_compressed'])
             for row in rows]

    def configure(self, schema, table):
        """Apply this hypertable configuration to the given table in the given schema."""
        time_dim = self.dims[0]
        self.db.execute(
            f"SELECT create_hypertable('{schema}.{table}', '{time_dim['column_name']}', "
            f"chunk_time_interval => INTERVAL '{time_dim['time_interval']}')")
        if self.base['compression_enabled']:
            s_by = f"'{','.join(self.segment_by)}'" if self.segment_by else 'NULL'
            o_by = f"'{','.join(self.order_by)}'" if self.order_by else 'NULL'
            self.db.execute(f"ALTER TABLE {schema}.{table} SET( "
                            f"  timescaledb.compress, "
                            f"  timescaledb.compress_segmentby = {s_by}, "
                            f"  timescaledb.compress_orderby = {o_by})")
            if self.compress_after:
                self.db.query(f"SELECT add_compression_policy('{schema}.{table}', "
                              f"INTERVAL '{self.compress_after}')")
        if self.drop_after:
            self.db.query(f"SELECT add_retention_policy('{schema}.{table}', "
                          f"INTERVAL '{self.drop_after}')")

    def log_config(self, logger, level):
        """Write collected hypertable information to the log at given logging level."""
        logger.log(level, f"Base hypertable config for {self.schema}.{self.table}: {self.base}")
        for dim in self.dims:
            logger.log(level, f"Hypertable dimension {dim['dimension_number']} "
                              f"for {self.schema}.{self.table}: {dim}")
        logger.log(level, f"Hypertable compression settings for {self.schema}.{self.table}: "
                          f"segmentby={self.segment_by}, orderby={self.order_by}")
        logger.log(level, f"Hypertable compression policy for {self.schema}.{self.table}: "
                          f"after {self.compress_after}")
        logger.log(level, f"Hypertable retention policy for {self.schema}.{self.table}: "
                          f"drop after {self.drop_after}")
        for c in self.chunk_ranges:
            logger.log(level, f"Chunk #{c.number}[{c.start} .. {c.end}] "
                              f"is {'' if c.compressed else 'not '}compressed")

    @staticmethod
    def is_hypertable(schema, table, db):
        rows = db.query(f"SELECT 1 FROM timescaledb_information.hypertables "
                        f"WHERE hypertable_schema='{schema}' "
                        f"AND hypertable_name='{table}' "
                        f"LIMIT 1")
        return any(rows)

    @staticmethod
    def hypertable_size_info(schema, table, db):
        sname = f"{schema}.{table}"
        ns = humanize.naturalsize
        bt_tot = at_tot = bi_tot = ai_tot = bo_tot = ao_tot = btotal = atotal = utotal = 0
        rows = list(db.query(f"select * from chunk_compression_stats('{sname}') "
                             f"WHERE compression_status = 'Compressed'"))
        for row in rows:
            bt_tot += row['before_compression_table_bytes']
            bi_tot += row['before_compression_index_bytes']
            bo_tot += row['before_compression_total_bytes'] \
                - row['before_compression_table_bytes'] \
                - row['before_compression_index_bytes']
            btotal += row['before_compression_total_bytes']
            at_tot += row['after_compression_table_bytes']
            ai_tot += row['after_compression_index_bytes']
            ao_tot += (row['after_compression_total_bytes']
                       - row['after_compression_table_bytes']
                       - row['after_compression_index_bytes'])
            atotal += row['after_compression_total_bytes']

        uncompressed = list(db.query(f"SELECT chunk_schema || '.' || chunk_name AS name "
                                     f"FROM chunk_compression_stats('{sname}') "
                                     f"WHERE compression_status = 'Uncompressed'"))
        ratio = 0 if atotal == 0 else btotal / atotal
        for chunk in uncompressed:
            utotal += next(db.query(f"SELECT pg_total_relation_size('{chunk['name']}')"))[0]
        total = atotal + utotal
        detail = f"Table: {ns(bt_tot)}=>{ns(at_tot)}; " \
                 f"Indexes: {ns(bi_tot)}=>{ns(ai_tot)}; " \
                 f"Other: {ns(bo_tot)}=>{ns(ao_tot)}; " \
                 f"Total: {ns(btotal)}=>{ns(atotal)}; " \
                 f"Compression: {'%.1f' % ratio}x; " \
                 f"Uncompressed: {ns(utotal)}"
        return total, detail
