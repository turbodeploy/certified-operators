from extenders.adjustment import Adjustment
from extenders.hypertable_extender import HypertableExtender


class CompletedActionExtender(HypertableExtender):
    """Extender for completed_action hypertable."""

    def __init__(self, args):
        super().__init__('completed_action', args, oid_cols=["action_oid", "target_entity_oid"],
                         adjustments=[Adjustment('completion_time', -args.replica_gap_delta)],
                         time_col='completion_time')
