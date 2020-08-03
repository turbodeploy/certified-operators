-- This patch makes FK not to remove a link in policy_group table when the group is removed
-- After this link is lost, we will have an inconsistent policy in the database, referring to
-- non-existing group.
ALTER TABLE policy_group DROP FOREIGN KEY fk_group_id;
ALTER TABLE policy_group ADD CONSTRAINT fk_group_id FOREIGN KEY (group_id) REFERENCES grouping (id) ON DELETE RESTRICT;