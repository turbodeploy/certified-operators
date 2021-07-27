from extenders.extender import Extender


class FileExtender(Extender):
    """Extender for file table. We only copy into first replica replica data, because this
    table is completely rewritten on every ingestion."""

    def __init__(self, args):
        super().__init__('file', args, scale_only=True, oid_cols=["volume_oid", "storage_oid"])
