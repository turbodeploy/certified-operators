ALTER TABLE policy ADD column display_name varchar(255) NOT NULL DEFAULT '';
UPDATE policy SET display_name = name;
ALTER TABLE policy ALTER display_name DROP DEFAULT;
