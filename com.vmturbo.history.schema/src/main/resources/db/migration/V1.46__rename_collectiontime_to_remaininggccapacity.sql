--    Update stats tables to rename CollectionTime to RemainingGcCapacity
--    Application Server
UPDATE app_server_stats_by_day SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_server_stats_by_hour SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_server_stats_by_month SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_server_stats_latest SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
--    Generic Application
UPDATE app_stats_by_day SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_stats_by_hour SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_stats_by_month SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_stats_latest SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_spend_by_day SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_spend_by_hour SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE app_spend_by_month SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
--    Market
UPDATE market_stats_by_day SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE market_stats_by_hour SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE market_stats_by_month SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';
UPDATE market_stats_latest SET property_type='RemainingGcCapacity' WHERE property_type = 'CollectionTime';