from extenders.adjustment import Adjustment
from extenders.extender import Extender


class EntityExtender(Extender):
    """Extender for the entity table.

    Extending an entity record just means adjusting its first_seen timestamp into the earliest
    replica, with the same offset form that replica's start time as it has in the sample data
    from the sample start time.
    """

    def __init__(self, args):
        total_adjustment = args.replica_gap_delta * (args.replica_count - 1)
        super().__init__('entity', args, oid_cols=["oid"],
                         adjustments=[Adjustment('first_seen', -total_adjustment)],
                         scale_only=True)
