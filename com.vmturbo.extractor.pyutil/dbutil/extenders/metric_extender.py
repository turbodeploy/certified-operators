from extenders.adjustment import Adjustment
from extenders.hypertable_extender import HypertableExtender


class MetricExtender(HypertableExtender):
    """Extender for the metric hypertable."""

    def __init__(self, args):
        super().__init__('metric', args, time_col='time', oid_cols=["entity_oid", "provider_oid"],
                         adjustments=[Adjustment('time', -args.replica_gap_delta)])
