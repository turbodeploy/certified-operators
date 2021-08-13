from extenders.adjustment import Adjustment
from extenders.hypertable_extender import HypertableExtender


class EntityCostExtender(HypertableExtender):
    """Extender for the entity_cost table."""

    def __init__(self, args):
        super().__init__('entity_cost', args, time_col='time', oid_cols=["entity_oid"],
                         adjustments=[Adjustment('time', -args.replica_gap_delta)])
