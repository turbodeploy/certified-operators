-- This migration introduces schedules as standalone entities.
--
-- For more info on schedules see
-- https://vmturbo.atlassian.net/wiki/spaces/PMTES/pages/1019281789/Feature+Page+-+XL+Schedules

CREATE TABLE schedule (
    -- The ID assigned to a schedule.
    id              BIGINT          NOT NULL,
    -- The name of the schedule. Name is required to be unique.
    display_name    VARCHAR(255)    NOT NULL UNIQUE,
    -- Start date/time of the schedule window
    start_time      TIMESTAMP       NOT NULL,
    -- End date/time of the schedule window
    end_time        TIMESTAMP       NOT NULL,
    -- Last date when the schedule is active
    last_date       TIMESTAMP       NULL,
    -- Recurring pattern rule as described here
    -- https://tools.ietf.org/html/rfc2445#section-4.3.10
    recur_rule      VARCHAR(80),
    -- time zone ID as described here
    -- https://en.wikipedia.org/wiki/Tz_database
    time_zone_id    VARCHAR(40)     NOT NULL,

    PRIMARY KEY (id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- This table maintains many to many relationship between setting policies and schedules.
CREATE TABLE setting_policy_schedule (
    -- Settings policy ID.
    setting_policy_id   BIGINT      NOT NULL,
    -- Schedule ID.
    schedule_id         BIGINT      NOT NULL,
    -- Foreign key into setting_policy table.
    FOREIGN  KEY (setting_policy_id) REFERENCES setting_policy(id) ON DELETE CASCADE,
    -- Foreign key into schedule table.
    FOREIGN  KEY (schedule_id) REFERENCES schedule(id) ON DELETE RESTRICT,
    PRIMARY KEY (setting_policy_id, schedule_id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;