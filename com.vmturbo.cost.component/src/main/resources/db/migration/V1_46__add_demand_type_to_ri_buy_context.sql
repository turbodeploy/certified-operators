-- Add a demand type column to action context RI buy table
ALTER TABLE action_context_ri_buy ADD column demand_type int NOT NULL DEFAULT 0;
-- Drop the default after filling in old data
ALTER TABLE action_context_ri_buy ALTER demand_type DROP DEFAULT;

-- Add a datapoint interval column to action context RI buy table
ALTER TABLE action_context_ri_buy ADD column datapoint_interval varchar(255) NOT NULL DEFAULT 'PT1H';
-- Drop the default after filling in old data
ALTER TABLE action_context_ri_buy ALTER datapoint_interval DROP DEFAULT;