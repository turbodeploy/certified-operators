-- This migration sets the "direct_member" attribute of expected member types for all dynamic
-- groups to "true" (1) instead of "false" (0). "true" is always correct for groups recorded
-- up to this point. We populate this data from the list of filters in the group definition.
-- These filters define the immediate/direct members of the group. We do not populate indirect
-- members of dynamic nested groups.
--
-- Using procedure for an idempotent migration.
DROP PROCEDURE IF EXISTS `FIXUP_DYNAMIC_EXPECTED_MEMBERS`;
DELIMITER $$
CREATE PROCEDURE FIXUP_DYNAMIC_EXPECTED_MEMBERS()
BEGIN
    -- "entity filters IS NOT NULL OR group_filters IS NOT NULL" is how we identify dynamic groups.
    UPDATE group_expected_members_groups SET direct_member = 1
        WHERE group_id IN (SELECT id FROM grouping WHERE entity_filters IS NOT NULL OR group_filters IS NOT NULL);
    UPDATE group_expected_members_entities SET direct_member = 1
        WHERE group_id IN (select id FROM grouping WHERE entity_filters IS NOT NULL OR group_filters IS NOT NULL);
END $$
DELIMITER ;

call FIXUP_DYNAMIC_EXPECTED_MEMBERS();
DROP PROCEDURE IF EXISTS `FIXUP_DYNAMIC_EXPECTED_MEMBERS`;
