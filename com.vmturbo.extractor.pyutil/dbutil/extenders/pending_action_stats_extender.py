from extenders.hypertable_extender import HypertableExtender


class PendingActionStatsExtender(HypertableExtender):
    """Extender for the pending_action_stats table."""

    def __init__(self, args):
        super().__init__('pending_action_stats', args, time_col='time', oid_cols=["scope_oid"])
