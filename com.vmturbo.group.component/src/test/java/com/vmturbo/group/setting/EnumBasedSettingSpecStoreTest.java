package com.vmturbo.group.setting;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * Verifies EnumBasedSettingSpecStore contains
 * {@link com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec} from
 * {@link com.vmturbo.components.common.setting.EntitySettingSpecs} and
 * {@link com.vmturbo.components.common.setting.ActionSettingSpecs}.
 */
public class EnumBasedSettingSpecStoreTest {

    private static final String RESIZE_EXECUTION_SETTING_NAME = "resizeExecutionSchedule";
    private static final String RESIZE_EXTERNAL_APPROVAL_TYPE_SETTING_NAME =
        "approvalResizeActionWorkflow";

    /**
     * EnumBasedSettingSpecStore should contain settings from
     * {@link com.vmturbo.components.common.setting.ActionSettingSpecs}.
     */
    @Test
    public void testActionModeSubSettingsGenerated() {
        EnumBasedSettingSpecStore enumBasedSettingSpecStore = new EnumBasedSettingSpecStore(false, false);
        Assert.assertTrue(
            enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXECUTION_SETTING_NAME).isPresent());
        Assert.assertTrue(
            enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXTERNAL_APPROVAL_TYPE_SETTING_NAME)
                .isPresent());
    }

    /**
     * EXTERNAL_APPROVAL should be hidden when the feature flag says to hide it.
     */
    @Test
    public void testHiddenValues() {
        EnumBasedSettingSpecStore enumBasedSettingSpecStore = new EnumBasedSettingSpecStore(
            false, true);
        Assert.assertFalse(enumBasedSettingSpecStore.getSettingSpec(
            EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds.getSettingName())
            .get()
            .getEnumSettingValueType()
            .getEnumValuesList()
            .contains(ActionMode.EXTERNAL_APPROVAL.name()));
        Assert.assertFalse(
            enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXTERNAL_APPROVAL_TYPE_SETTING_NAME)
                .isPresent());

        enumBasedSettingSpecStore = new EnumBasedSettingSpecStore(
            true, false);
        Assert.assertFalse(
            enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXECUTION_SETTING_NAME).isPresent());
    }

}
