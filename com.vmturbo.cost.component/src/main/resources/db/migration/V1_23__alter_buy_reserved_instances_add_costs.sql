ALTER TABLE buy_reserved_instance ADD COLUMN per_instance_fixed_cost DOUBLE;

ALTER TABLE buy_reserved_instance ADD COLUMN per_instance_recurring_cost_hourly DOUBLE;

ALTER TABLE buy_reserved_instance ADD COLUMN per_instance_amortized_cost_hourly DOUBLE;
