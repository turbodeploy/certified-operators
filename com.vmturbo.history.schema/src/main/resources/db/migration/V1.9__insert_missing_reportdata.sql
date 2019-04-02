/*
Following entries are required by most OpsMgr reports.
For the utilThreshold_xx, we currently insert the same default values as in OpsMgr 6.3.
OM-44203 is opened to retrieve these values from policy settings UI.
*/

LOCK TABLES `entities` WRITE;
INSERT INTO `entities` (`name`, `display_name`, `uuid`, `creation_class`, `created_at`)
VALUES
    ('MarketSettingsManager', 'MarketSettingsManager', '_1wq9QTUoEempSo2vlSygbA', 'MarketSettingsManager', NULL);
SET @market_settings_manager_id = LAST_INSERT_ID();
UNLOCK TABLES;

LOCK TABLES `entity_attrs` WRITE;
INSERT INTO `entity_attrs` (`name`, `value`, `entity_entity_id`)
VALUES
    ('utilThreshold_SA', '90.0', @market_settings_manager_id),
    ('utilThreshold_IOPS', '100.0', @market_settings_manager_id),
    ('utilThreshold_CPU', '100.0', @market_settings_manager_id),
    ('utilThreshold_MEM', '100.0', @market_settings_manager_id),
    ('utilThreshold_IO', '50.0', @market_settings_manager_id),
    ('utilThreshold_NW', '50.0', @market_settings_manager_id),
    ('utilThreshold_NW_Switch', '70.0', @market_settings_manager_id),
    ('utilThreshold_NW_Network', '100.0', @market_settings_manager_id),
    ('utilThreshold_NW_internet', '100.0', @market_settings_manager_id),
    ('utilThreshold_SW', '20.0', @market_settings_manager_id),
    ('utilThreshold_CPU_SC', '100.0', @market_settings_manager_id),
    ('utilThreshold_LT', '100.0', @market_settings_manager_id),
    ('utilThreshold_RQ', '50.0', @market_settings_manager_id),
    ('utilUpperBound_VMEM', '85.0', @market_settings_manager_id),
    ('utilUpperBound_VCPU', '85.0', @market_settings_manager_id),
    ('utilLowerBound_VMEM', '10.0', @market_settings_manager_id),
    ('utilLowerBound_VCPU', '10.0', @market_settings_manager_id),
    ('utilUpperBound_VStorage', '85.0', @market_settings_manager_id),
    ('utilLowerBound_VStorage', '10.0', @market_settings_manager_id),
    ('usedIncrement_VMEM', '1024.0', @market_settings_manager_id),
    ('usedIncrement_VCPU', '1800.0', @market_settings_manager_id),
    ('usedIncrement_VStorage', '999999.0', @market_settings_manager_id),
    ('usedIncrement_VDCMem', '1024.0', @market_settings_manager_id),
    ('usedIncrement_VDCCPU', '1800.0', @market_settings_manager_id),
    ('usedIncrement_VDCStorage', '1.0', @market_settings_manager_id),
    ('historicalTimeRange', '8', @market_settings_manager_id),
    ('cost_PhysicalMachine', '9000.0', @market_settings_manager_id),
    ('cost_VCPU', '200.0', @market_settings_manager_id),
    ('cost_VMem', '50.0', @market_settings_manager_id),
    ('cost_VStorage', '50.0', @market_settings_manager_id),
    ('usedIncrement_StAmt', '100.0', @market_settings_manager_id),
    ('utilTarget', '70.0', @market_settings_manager_id),
    ('targetBand', '10.0', @market_settings_manager_id),
    ('peaK_WEIGHT', '99.0', @market_settings_manager_id),
    ('useD_WEIGHT', '50.0', @market_settings_manager_id),
    ('ratE_OF_RESIZE', '2.0', @market_settings_manager_id),
    ('pM_MAX_PRICE', '10000.0', @market_settings_manager_id),
    ('sT_MAX_PRICE', '10000.0', @market_settings_manager_id),
    ('capacity_CPUProvisioned', '1000.0', @market_settings_manager_id),
    ('capacity_MemProvisioned', '1000.0', @market_settings_manager_id),
    ('disableDas', 'false', @market_settings_manager_id),
    ('vmDeletionSensitivity', '5', @market_settings_manager_id);
UNLOCK TABLES;

LOCK TABLES `entities` WRITE;
INSERT INTO `entities` (`name`, `display_name`, `uuid`, `creation_class`, `created_at`)
VALUES
    ('PresentationManager', 'PresentationManager', '_zfrLATUoEempSo2vlSygbA', 'PresentationManager', NULL);
SET @presentation_manager_id = LAST_INSERT_ID();
UNLOCK TABLES;

LOCK TABLES `entity_attrs` WRITE;
INSERT INTO `entity_attrs` ( `name`, `value`, `entity_entity_id`)
VALUES
    ('currencySetting', '$', @presentation_manager_id);
UNLOCK TABLES;