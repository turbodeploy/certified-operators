-- Add probe send unique Id for template. For user created template, it will be null.
ALTER TABLE template ADD COLUMN probe_template_id VARCHAR(255) DEFAULT NULL;