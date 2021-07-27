from dblib import config
from extenders.adjustment import Adjustment
from extenders.extender import Extender


class PendingActionExtender(Extender):
    """Extender for pending_action table."""

    def __init__(self, args):
        super().__init__('pending_action', args, oid_cols=["action_oid", "target_entity_oid"])
        oid_span = config.db.query(f"SELECT max(action_oid) - min(action_oid) AS span "
                                   f"FROM {self.table}").fetchone()['span'] or 0
        self.adjustments = [Adjustment('action_oid', oid_span + 1)]
