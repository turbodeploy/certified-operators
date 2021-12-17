-- This migration script will update the time_zone_id for schedules of the Zulu timezone.
-- There was a time when Zulu timezone was stored as 'Z' but now we expect and store it
-- appropriately as 'Zulu'.

update schedule set time_zone_id = 'Zulu', start_time = start_time where time_zone_id = 'Z';