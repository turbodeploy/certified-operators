-- Change column type to Boolean so that both MariaDB and Postgres will have the same JOOQ class.
ALTER TABLE plan_entity MODIFY COLUMN is_placed BOOLEAN;