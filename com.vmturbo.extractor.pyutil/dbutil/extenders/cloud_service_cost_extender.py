from extenders.hypertable_extender import HypertableExtender


class CloudServiceCostExtender(HypertableExtender):
    """Extender for the entity_cost table."""

    def __init__(self, args):
        super().__init__('cloud_service_cost', args, time_col='time',
                         oid_cols=["account_oid", "cloud_service_oid"])
