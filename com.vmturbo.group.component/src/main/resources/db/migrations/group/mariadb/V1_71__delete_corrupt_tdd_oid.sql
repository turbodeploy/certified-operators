-- This script deletes dormant/corrupt tdd records from topology_data_definition_oid table
-- which were deleted by the customer on the UI, but due to bug (OM-93120), these records
-- were not deleted from the topology_data_definition_oid table but all other tables
-- including manual_topo_data_defs and auto_topo_data_defs

-- Look at the Oid's present in view all_topo_data_defs ()
DELETE FROM topology_data_definition_oid
 WHERE id NOT IN (SELECT a.id
                        FROM all_topo_data_defs a)