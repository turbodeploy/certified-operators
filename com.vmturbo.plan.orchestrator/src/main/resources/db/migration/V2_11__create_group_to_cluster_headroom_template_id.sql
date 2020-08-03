-- This table keeps the id for cluster headroom template for a group.
CREATE TABLE cluster_to_headroom_template_id (
    -- Id of the group
    group_id                   BIGINT         NOT NULL,
    -- Id of the template
    template_id                BIGINT         NOT NULL,
    -- Create template id as a foreign key of Template table.
    FOREIGN  KEY (template_id) REFERENCES template(id) ON DELETE CASCADE,
    -- Group id and template id as primary key since one cluster has one head room template.
    PRIMARY KEY (group_id)
)