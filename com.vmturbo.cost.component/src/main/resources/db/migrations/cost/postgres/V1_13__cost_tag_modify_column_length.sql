-- Increase tag_key and tag_value column lengths to match max limits of all Cloud Providers:
--
-- | Cloud Provider | tag_key limit | tag_value limit |
-- |----------------|---------------|-----------------|
-- | Azure          | 512           | 256             |
-- | AWS            | 127           | 255             |
-- | GCP            | 63            | 63              |
--

ALTER TABLE cost_tag ALTER COLUMN tag_key TYPE varchar(512);
ALTER TABLE cost_tag ALTER COLUMN tag_value TYPE varchar(256);