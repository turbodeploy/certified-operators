-- This migration is aimed to change GroupType mapped by ordinal to getNumer()
-- The previous migration overlooked the group_type in the group_expected_members_groups table.
--
-- This is the version of GroupType at the time of this migration:
--
-- enum GroupType {
--      REGULAR = 0;
--      reserved 1;
--      reserved "DISCOVERED";
--      RESOURCE = 2;
--      // A host compute cluster is comprised of physical machines.
--      COMPUTE_HOST_CLUSTER = 3;
--       // A virtual machine compute cluster is comprised of virtual machines.
--      COMPUTE_VIRTUAL_MACHINE_CLUSTER = 4;
--      // A storage cluster is comprised of storages.
--      STORAGE_CLUSTER = 5;
--       // A billing family is comprised of billing accounts.
--       BILLING_FAMILY = 6;
--   }
--
-- We are effectively incrementing all non-zero group types by 1 (because that's the difference
-- between the ordinal and the index). However, because "group_type" is part of the primary key
-- of this table, we need to change it in descending order to avoid primary key collisions.
UPDATE group_expected_members_groups set group_type = 6 where group_type = 5;
UPDATE group_expected_members_groups set group_type = 5 where group_type = 4;
UPDATE group_expected_members_groups set group_type = 4 where group_type = 3;
UPDATE group_expected_members_groups set group_type = 3 where group_type = 2;
UPDATE group_expected_members_groups set group_type = 2 where group_type = 1;
