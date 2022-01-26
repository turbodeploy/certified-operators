-- The unique key for billed_cost tables are missing the provider_id and provider_type columns.
-- Recreate the unique constraint to add these two columns.

ALTER TABLE billed_cost_hourly DROP CONSTRAINT IF EXISTS billed_cost_hourly_sample_time_entity_id_entity_type_accoun_key;
ALTER TABLE billed_cost_hourly ADD CONSTRAINT unique_hourly_billing_item UNIQUE (sample_time, entity_id, entity_type, account_id,
            region_id, cloud_service_id, service_provider_id, tag_group_id,
            price_model, cost_category, commodity_type, provider_id, provider_type);

ALTER TABLE billed_cost_daily DROP CONSTRAINT IF EXISTS billed_cost_daily_sample_time_entity_id_entity_type_account_key;
ALTER TABLE billed_cost_daily ADD CONSTRAINT unique_daily_billing_item UNIQUE (sample_time, entity_id, entity_type, account_id,
            region_id, cloud_service_id, service_provider_id, tag_group_id,
            price_model, cost_category, commodity_type, provider_id, provider_type);

ALTER TABLE billed_cost_monthly DROP CONSTRAINT IF EXISTS billed_cost_monthly_sample_time_entity_id_entity_type_accou_key;
ALTER TABLE billed_cost_monthly ADD CONSTRAINT unique_monthly_billing_item UNIQUE (sample_time, entity_id, entity_type, account_id,
            region_id, cloud_service_id, service_provider_id, tag_group_id,
            price_model, cost_category, commodity_type, provider_id, provider_type);

