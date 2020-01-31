/*
 * All insertions into the entities table use entity ids appearing in an incoming topology, so
 * the auto_increment feature on this column makes no sense. It appears to be interacting badly
 * with an attempt to use upserts to update other data when it changes. So this migration takes
 * away the auto_increment character.
 *
 * The column is used as foreign key elsewehere, so we need to turn off foreign key checks before
 * any change to the column is allowed.
 */

SET FOREIGN_KEY_CHECKS=0;
ALTER TABLE entities MODIFY COLUMN id BIGINT(20) NOT NULL;
SET FOREIGN_KEY_CHECKS=1;

