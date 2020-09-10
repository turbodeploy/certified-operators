-- Summary
-- ---------------------------------------------------------------------------------------------------------------------------------
-- |                                       |                    | EntityType |                                                     |
-- | Old Setting Name                      | EntityType         |    Number  |  New Setting Name                                   |
-- ---------------------------------------------------------------------------------------------------------------------------------
-- | businessUserMove                      | BUSINESS_USER      | 17         | move                                                |
-- | suspendIsDisabled                     | IO_MODULE          | 21         | suspend                                             |
-- | provisionIsDisabled                   | STORAGE_CONTROLLER | 18         | provision                                           |
-- | moveActionWorkflowWithNativeAsDefault | BUSINESS_USER      | 17         | moveActionWorkflow                                  |
-- | moveActionWorkflow                    | VIRTUAL_MACHINE    | 10         | moveActionWorkflow                                  |
-- |                                       |                    |            | storageMoveActionWorkflow                           |
-- | preMoveActionWorkflow                 | VIRTUAL_MACHINE    | 10         | preMoveActionWorkflow                               |
-- |                                       |                    |            | preStorageMoveActionWorkflow                        |
-- | postMoveActionWorkflow                | VIRTUAL_MACHINE    | 10         | postMoveActionWorkflow                              |
-- |                                       |                    |            | postStorageMoveActionWorkflow                       |
-- | resizeActionWorkflow                  | VIRTUAL_MACHINE    | 10         | resizeVmemUpInBetweenThresholdsActionWorkflow       |
-- |                                       |                    |            | resizeVmemDownInBetweenThresholdsActionWorkflow     |
-- |                                       |                    |            | resizeVmemAboveMaxThresholdActionWorkflow           |
-- |                                       |                    |            | resizeVmemBelowMinThresholdActionWorkflow           |
-- |                                       |                    |            | resizeVcpuUpInBetweenThresholdsActionWorkflow       |
-- |                                       |                    |            | resizeVcpuDownInBetweenThresholdsActionWorkflow     |
-- |                                       |                    |            | resizeVcpuAboveMaxThresholdActionWorkflow           |
-- |                                       |                    |            | resizeVcpuBelowMinThresholdActionWorkflow           |
-- | preResizeActionWorkflow               | VIRTUAL_MACHINE    | 10         | preResizeVmemUpInBetweenThresholdsActionWorkflow    |
-- |                                       |                    |            | preResizeVmemDownInBetweenThresholdsActionWorkflow  |
-- |                                       |                    |            | preResizeVmemAboveMaxThresholdActionWorkflow        |
-- |                                       |                    |            | preResizeVmemBelowMinThresholdActionWorkflow        |
-- |                                       |                    |            | preResizeVcpuUpInBetweenThresholdsActionWorkflow    |
-- |                                       |                    |            | preResizeVcpuDownInBetweenThresholdsActionWorkflow  |
-- |                                       |                    |            | preResizeVcpuAboveMaxThresholdActionWorkflow        |
-- |                                       |                    |            | preResizeVcpuBelowMinThresholdActionWorkflow        |
-- | postResizeActionWorkflow              | VIRTUAL_MACHINE    | 10         | postResizeVmemUpInBetweenThresholdsActionWorkflow   |
-- |                                       |                    |            | postResizeVmemDownInBetweenThresholdsActionWorkflow |
-- |                                       |                    |            | postResizeVmemAboveMaxThresholdActionWorkflow       |
-- |                                       |                    |            | postResizeVmemBelowMinThresholdActionWorkflow       |
-- |                                       |                    |            | postResizeVcpuUpInBetweenThresholdsActionWorkflow   |
-- |                                       |                    |            | postResizeVcpuDownInBetweenThresholdsActionWorkflow |
-- |                                       |                    |            | postResizeVcpuAboveMaxThresholdActionWorkflow       |
-- |                                       |                    |            | postResizeVcpuBelowMinThresholdActionWorkflow       |
-- ---------------------------------------------------------------------------------------------------------------------------------

-- rename businessUserMove to move
-- customer might have old, unused settings from when BUSINESS_USER move was used. get rid of them.
DELETE FROM setting_policy_setting
WHERE policy_id IN (
        -- only include settings from policies of entity type BUSINESS_USER(17)
        SELECT id FROM setting_policy where
            entity_type = 17 and (policy_type = 'default' or policy_type = 'user'))
    AND setting_name = 'move';
UPDATE setting_policy_setting
    SET setting_name = 'move'
    WHERE policy_id IN (
            -- only include settings from policies of entity type BUSINESS_USER(17)
            SELECT id FROM setting_policy where
                entity_type = 17 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'businessUserMove';

-- rename suspendIsDisabled to suspend
-- customer might have old, unused settings from when IO_MODULE suspend was used. get rid of them.
DELETE FROM setting_policy_setting
WHERE policy_id IN (
        -- only include settings from policies of entity type IO_MODULE(21)
        SELECT id FROM setting_policy where
            entity_type = 21 and (policy_type = 'default' or policy_type = 'user'))
    AND setting_name = 'suspend';
UPDATE setting_policy_setting
    SET setting_name = 'suspend'
    WHERE policy_id IN (
            -- only include settings from policies of entity type IO_MODULE(21)
            SELECT id FROM setting_policy where
                entity_type = 21 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'suspendIsDisabled';

-- rename provisionIsDisabled to provision
-- delete bugged, existing, STORAGE_CONTROLLER provision setting value
-- STORAGE_CONTROLLER incorrectly used both Provision and DisabledProvision when only one should
-- DisabledProvision. In order to rename to Provision, we to get rid of the incorrect provision
-- setting values before we can rename.
DELETE FROM setting_policy_setting
WHERE policy_id IN (
        -- only include settings from policies of entity type STORAGE_CONTROLLER(18)
        SELECT id FROM setting_policy where
            entity_type = 18 and (policy_type = 'default' or policy_type = 'user'))
    AND setting_name = 'provision';
UPDATE setting_policy_setting
    SET setting_name = 'provision'
    WHERE policy_id IN (
            -- only include settings from policies of entity type STORAGE_CONTROLLER(18)
            SELECT id FROM setting_policy where
                entity_type = 18 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'provisionIsDisabled';

-- moveActionWorkflowWithNativeAsDefault did not follow the convention
-- should be moveActionWorkflow. this separate setting was unnecessary because action
-- workflow settings are already native as default.
-- BusinessUserMove already used preMoveActionWorkflow and postMoveActionWorkflow, so no
-- rename was necessary there.
-- rename moveActionWorkflowWithNativeAsDefault to moveActionWorkflow
-- customer might have old, unused settings from when BUSINESS_USER moveActionWorkflow was used.
-- get rid of them.
DELETE FROM setting_policy_setting
WHERE policy_id IN (
        -- only include settings from policies of entity type BUSINESS_USER(17)
        SELECT id FROM setting_policy where
            entity_type = 17 and (policy_type = 'default' or policy_type = 'user'))
    AND setting_name = 'moveActionWorkflow';
UPDATE setting_policy_setting
    SET setting_name = 'moveActionWorkflow'
    WHERE policy_id IN (
            -- only include settings from policies of entity type BUSINESS_USER(17)
            SELECT id FROM setting_policy where
                entity_type = 17 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'moveActionWorkflowWithNativeAsDefault';

-- need to copy move to storageMove. from 7.22.7 onwards, VM storageMove (the storage that
-- the VM is hosted on) is no longer grouped with VM move (the physical machine the VM is
-- hosted on).
-- copy moveActionWorkflow to storageMoveActionWorkflow
-- storageMoveActionWorkflow never existed until now, so no need to delete.
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'storageMoveActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'moveActionWorkflow';
-- copy preMoveActionWorkflow to preStorageMoveActionWorkflow
-- storageMoveActionWorkflow never existed until now, so no need to delete.
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preStorageMoveActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preMoveActionWorkflow';
-- copy postMoveActionWorkflow to postStorageMoveActionWorkflow
-- postStorageMoveActionWorkflow never existed until now, so no need to delete.
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postStorageMoveActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postMoveActionWorkflow';

-- VM Resize setting split up by vcpu|vmem x Up|Down|AboveMax|BelowMin.
-- copy resizeActionWorkflow to resizeVmemUpInBetweenThresholdsActionWorkflow
-- None of the resize[commodity][direction]ActionWorkflow settings existed until now, so no need
-- to delete.
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'resizeVmemUpInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'resizeActionWorkflow';
-- copy resizeActionWorkflow to resizeVmemDownInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'resizeVmemDownInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'resizeActionWorkflow';
-- copy resizeActionWorkflow to resizeVmemAboveMaxThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'resizeVmemAboveMaxThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'resizeActionWorkflow';
-- copy resizeActionWorkflow to resizeVmemBelowMinThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'resizeVmemBelowMinThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'resizeActionWorkflow';
-- copy resizeActionWorkflow to resizeVcpuUpInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'resizeVcpuUpInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'resizeActionWorkflow';
-- copy resizeActionWorkflow to resizeVcpuDownInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'resizeVcpuDownInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'resizeActionWorkflow';
-- copy resizeActionWorkflow to resizeVcpuAboveMaxThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'resizeVcpuAboveMaxThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'resizeActionWorkflow';
-- copy resizeActionWorkflow to resizeVcpuBelowMinThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'resizeVcpuBelowMinThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'resizeActionWorkflow';
-- delete resizeActionWorkflow
DELETE FROM setting_policy_setting
WHERE policy_id IN (
        -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
        SELECT id FROM setting_policy where
            entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
    AND setting_name = 'resizeActionWorkflow';

-- copy preResizeActionWorkflow to preResizeVmemUpInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preResizeVmemUpInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preResizeActionWorkflow';
-- copy preResizeActionWorkflow to preResizeVmemDownInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preResizeVmemDownInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preResizeActionWorkflow';
-- copy preResizeActionWorkflow to preResizeVmemAboveMaxThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preResizeVmemAboveMaxThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preResizeActionWorkflow';
-- copy preResizeActionWorkflow to preResizeVmemBelowMinThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preResizeVmemBelowMinThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preResizeActionWorkflow';
-- copy preResizeActionWorkflow to preResizeVcpuUpInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preResizeVcpuUpInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preResizeActionWorkflow';
-- copy preResizeActionWorkflow to preResizeVcpuDownInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preResizeVcpuDownInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preResizeActionWorkflow';
-- copy preResizeActionWorkflow to preResizeVcpuAboveMaxThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preResizeVcpuAboveMaxThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preResizeActionWorkflow';
-- copy preResizeActionWorkflow to preResizeVcpuBelowMinThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'preResizeVcpuBelowMinThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'preResizeActionWorkflow';
-- delete preResizeActionWorkflow
DELETE FROM setting_policy_setting
WHERE policy_id IN (
        -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
        SELECT id FROM setting_policy where
            entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
    AND setting_name = 'preResizeActionWorkflow';

-- copy postResizeActionWorkflow to postResizeVmemUpInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postResizeVmemUpInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postResizeActionWorkflow';
-- copy postResizeActionWorkflow to postResizeVmemDownInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postResizeVmemDownInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postResizeActionWorkflow';
-- copy postResizeActionWorkflow to postResizeVmemAboveMaxThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postResizeVmemAboveMaxThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postResizeActionWorkflow';
-- copy postResizeActionWorkflow to postResizeVmemBelowMinThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postResizeVmemBelowMinThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postResizeActionWorkflow';
-- copy postResizeActionWorkflow to postResizeVcpuUpInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postResizeVcpuUpInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postResizeActionWorkflow';
-- copy postResizeActionWorkflow to postResizeVcpuDownInBetweenThresholdsActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postResizeVcpuDownInBetweenThresholdsActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postResizeActionWorkflow';
-- copy postResizeActionWorkflow to postResizeVcpuAboveMaxThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postResizeVcpuAboveMaxThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postResizeActionWorkflow';
-- copy postResizeActionWorkflow to postResizeVcpuBelowMinThresholdActionWorkflow
INSERT INTO setting_policy_setting (policy_id, setting_name, setting_type, setting_value)
    SELECT policy_id, 'postResizeVcpuBelowMinThresholdActionWorkflow', setting_type, setting_value
    FROM setting_policy_setting
    WHERE policy_id IN (
            -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
            SELECT id FROM setting_policy where
                entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
        AND setting_name = 'postResizeActionWorkflow';
-- delete postResizeActionWorkflow
DELETE FROM setting_policy_setting
WHERE policy_id IN (
        -- only include settings from policies of entity type VIRTUAL_MACHINE(10)
        SELECT id FROM setting_policy where
            entity_type = 10 and (policy_type = 'default' or policy_type = 'user'))
    AND setting_name = 'postResizeActionWorkflow';