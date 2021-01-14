-- This new column is responsible for a boolean flag that
-- shows if the manual TDD is context-based or not.
ALTER TABLE manual_topo_data_defs
ADD COLUMN context_based BOOLEAN
DEFAULT FALSE;