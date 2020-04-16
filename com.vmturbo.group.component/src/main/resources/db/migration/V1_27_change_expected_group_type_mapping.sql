-- This migration is aimed to change GroupType mapped by ordinal to getNumer()
-- The previous migration overlooked the group_type in the group_expected_members_groups table.
--
-- At the time of this migration, the difference between GroupType.ordinal() and GroupType.getNumber()
-- is that 1 is deprecated and unused in GroupType, and all non-Regular groups start at 2.
UPDATE group_expected_members_groups set group_type = group_type + 1 where group_type > 0;
