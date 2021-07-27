from extenders.extender import Extender


class ActionGroupExtender(Extender):
    """Extender for action_group table."""

    def __init__(self, args):
        super().__init__('action_group', args, scale_only=True, oid_cols=[])
