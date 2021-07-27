from extenders.adjustment import Adjustment
from extenders.hypertable_extender import HypertableExtender


class HistoricalEntityAttrsExtender(HypertableExtender):
    """Extender for the historical_entity_attrs hypertable."""

    def __init__(self, args):
        super().__init__('historical_entity_attrs', args, time_col='time', oid_cols=["entity_oid"],
                         adjustments=[Adjustment('time', -args.replica_gap_delta)]),
