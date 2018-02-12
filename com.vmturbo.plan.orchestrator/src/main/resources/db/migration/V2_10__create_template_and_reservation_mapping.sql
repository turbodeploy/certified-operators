-- This table maintain many to many relationship between reservation with templates.
CREATE TABLE reservation_to_template (
    -- Id of reservation.
    reservation_id             BIGINT         NOT NULL,
    -- Id of template.
    template_id                BIGINT         NOT NULL,
    -- Create reservation id as a foreign key of Reservation table.
    FOREIGN  KEY (reservation_id) REFERENCES reservation(id) ON DELETE CASCADE,
    -- Create template id as a foreign key of Template table.
    FOREIGN  KEY (template_id) REFERENCES template(id) ON DELETE CASCADE,
    -- Combination of reservation id and template as primary key.
    PRIMARY KEY (reservation_id, template_id)
)