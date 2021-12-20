-- The type of service entity the template applies to.
ALTER TABLE template CHANGE COLUMN template_type entity_type INTEGER NOT NULL;

-- The type of the template - system-provided, or user-created.
ALTER TABLE template ADD COLUMN type VARCHAR(255) NOT NULL;