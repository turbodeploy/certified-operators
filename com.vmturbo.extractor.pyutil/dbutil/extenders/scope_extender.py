from datetime import datetime

from dateutil.tz import tz

from dblib import config
from extenders.adjustment import Adjustment
from extenders.extender import Extender


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
        super().__init__('scope', args, oid_cols=["seed_oid", "scoped_oid"])
        self.inf_time = datetime(year=9999, month=12, day=31, hour=0, minute=0, second=0,
                                 tzinfo=tz.tzutc())

    def extend(self, replica_number):
        return self.extend_first_replica() if replica_number == 1 \
            else self.extend_other_replicas(replica_number)

    def extend_first_replica(self):
        """
        For the first replica all records will already have been copied into the replica table by
        scaling. Unless replica_count == 1, we then make the following adjustments:

        * Adjust start times in records with finish=INF so they fall in the final (earliest) replica
        * Move any record with finish != INF that matches another record with finish=INF so that
          its start and finish times are in the final (earliest) replica.

        After making these adjustments, any record whose start time remains in the first replica
        will be eligible for copying into all other replicas, and that's how we'll process the
        other replicas.

        :return: 0, since we never create new records
        """
        if self.args.replica_count != 1:
            shift = f"INTERVAL '{self.args.replica_gap_delta}'"
            # move all records with non-inf finish but for which there is a corresponding
            # inf-finish record so they fall in the earliest scope
            t0 = datetime.now()
            moved = config.db.execute(
                f"UPDATE {self.extended} "
                f"SET start = scope.start - {shift}, finish = scope.finish - {shift} "
                f"FROM {self.extended} s "
                f"WHERE scope.seed_oid = s.seed_oid AND scope.scoped_oid = s.scoped_oid "
                f"AND scope.finish != '{self.inf_time}' AND s.finish = '{self.inf_time}'")
            config.logger.debug(
                f"Moved {moved} records to earliest replica in {datetime.now() - t0}")
            t0 = datetime.now()
            extended = config.db.execute(
                f"UPDATE {self.extended} SET start = start - {shift}"
                f"WHERE finish = '{self.inf_time}'")
            config.logger.debug(
                f"Moved {extended} record starts to earliest replica in {datetime.now() - t0}")
        return 0

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
        config.logger.debug(f"Wrote {n} records to scope table for replica {replica_number} "
                            f"in {datetime.now() - t0}")
        return n
