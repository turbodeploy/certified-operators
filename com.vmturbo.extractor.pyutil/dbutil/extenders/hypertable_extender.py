import logging
from datetime import datetime

import humanize

from dblib import HypertableConfig, config
from extenders.adjustment import Adjustment
from extenders.extender import Extender, Feature


class HypertableExtender(Extender):
    """Extender implementation base for hypertables.

    This handles capturing and transferring hypertable configuration and chunk-by-chunk data
    copying with compression of chunks as they become eligible.

    Compression eligibility does not currently adhere to retention policy; we simply compress
    every chunk as soon as it becomes later than the earliest data we've written, since we write
    later replicas first. We don't ever compress the latest replica, which leaves the replica
    schema suitable as a target for ingestion, should that be desired.
    """

    def __init__(self, table, args, adjustments=None, time_col=None, oid_cols=None):
        self.sample_htconfig = HypertableConfig(args.db_schema, table, config.db)
        self.does_compression = self.sample_htconfig.base['compression_enabled']
        super().__init__(table, args, adjustments, time_col, oid_cols=oid_cols)

    def prepare(self):
        """Implementation of prepare method for hypertable extenders.

        In the case that the super method creates this table in the replica schema, we configure
        it as a hypertable and copy hypertable configuration options from table in the sample
        schema to the table in the replication schema. Limitations in the HypertableConfig
        class apply here (e.g. dimensions beyond the primary time dimension are not copied).

        :return:true if the table was created anew in the replica schema
        """
        if super().prepare():
            # create extended table - configure hypertable same as sample table but without
            # a compression policy (we may be dealing with old data, and we don't want timescale
            # deciding to compress a chunk we're still writing into, let alone after we've created
            # the extended data set and expect it to be stable while it's being analyzed)
            if config.logger.isEnabledFor(logging.DEBUG):
                self.sample_htconfig.log_config(config.logger, logging.DEBUG)
            self.sample_htconfig.configure(self.args.replica_schema, self.table, disabled=[Feature.compression_policy])
            HypertableConfig(self.args.replica_schema, self.table, config.db) \
                .log_config(config.logger, logging.DEBUG)
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

    def scale(self):
        n = 0
        effective_scale_factor = self.args.scale_factor if self.oid_cols else 1
        for i in range(1, effective_scale_factor + 1):
            config.logger.debug(f"Scaling copy #{i}")
            adjustments = [Adjustment(col, Extender.scaling_gap) for col in self.oid_cols]
            n += self.copy_data(i, adjustments=adjustments, disabled=[Feature.compression])
        return n

    def copy_data(self, replica_number, start=None, end=None, extra_cond=None, source=None,
                  adjustments=None, source_desc=None, disabled=None):
        """Implementation of copy_data for a hypertable extender.

        The copies are done chunk-by-chunk, from latest to earliest chunk. After each chunk is
        copied, if any chunks in the replica table are now eligible for compression, they are
        compressed. A chunk is eligible for compression if it is not the latest chunk, and if its
        time range lies wholly beyond the timestamp of the last record inserted.

        :param replica_number: one-origin replica count
        :param start: inclusive beginning range of timestamps of sample data to copy
        :param end: exclusive end of range of timestamps of sample data to copy
        :param extra_cond: any conditions to add to the standard time-based condition
        :param adjustments: adjustments to be made to records being copied
        :param source: source table to copy records from (defaults to table in sample schema)
        :param source_desc: name of source, purely for logging purposes
        :param disabled: features to be disabled during this copy operation
        :return: number of records copied
        """
        start = start or self.args.sample_start
        end = end or self.args.sample_end
        source = source or self.table
        adjustments = adjustments or self.adjustments
        chunks = self.sample_htconfig.chunk_ranges
        n = 0
        for c in chunks:
            _start, _end = max(start, c.start), min(end, c.end)
            if _start < _end:
                sd = source_desc or f"chunk #{c.number}"
                config.logger.debug(f"Copying records from {sd}[{c.start}..{c.end}] "
                                    f"of {self.table}")
                n += super().copy_data(replica_number, start=_start, end=_end,
                                       extra_cond=extra_cond, adjustments=adjustments,
                                       source=source, source_desc=sd)
                if self.does_compression and Feature.compression not in (disabled or []):
                    self.compress_compressible_chunks(
                        _start - (replica_number - 1) * self.args.replica_gap_delta)
            else:
                config.logger.debug(f"Skipping chunk {c.number} of {self.table}: no range overlap")
        return n

    def compress_compressible_chunks(self, start):
        """Compress all eligible chunks for this table.

        :param start: timestamp of earliest record written to table so far
        """
        if not self.does_compression:
            # do nothing if compression is not enabled on the table
            return
        chunks = list(config.db.query(f"SELECT * FROM timescaledb_information.chunks "
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
                config.db.execute(f"SELECT compress_chunk('{chunk_schema}.{chunk_name}')")
                before, after = next(config.db.query(
                    f"SELECT before_compression_total_bytes, after_compression_total_bytes "
                    f"FROM chunk_compression_stats('{self.extended}') "
                    f"WHERE chunk_schema='{chunk_schema}' AND chunk_name='{chunk_name}'"))
                factor = '%.1f' % (before / after)
                config.logger.debug(f"Compressed chunk {chunk_name} of {self.extended}: "
                                    f"{humanize.naturalsize(before)} => {humanize.naturalsize(after)} "
                                    f"({factor}x) in {datetime.now() - t0}")
        else:
            config.logger.debug("No chunks ready for compression")
