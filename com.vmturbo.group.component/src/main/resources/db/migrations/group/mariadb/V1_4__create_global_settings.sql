-- Store for Global Settings
CREATE TABLE global_settings (

  -- The name of the setting. The name should be unique.
  name                 VARCHAR(255)     NOT NULL UNIQUE,

  -- A binary blob containing the raw protobuf which provides additional
  -- details about the setting.
  setting_data         BLOB            NOT NULL,

  PRIMARY KEY (name)
) ENGINE=INNODB DEFAULT CHARSET=utf8;
