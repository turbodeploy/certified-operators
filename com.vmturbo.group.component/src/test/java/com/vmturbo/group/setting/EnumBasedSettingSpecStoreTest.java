package com.vmturbo.group.setting;

import java.util.Collection;

import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.utils.ProbeFeature;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

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
    private static final String RESIZE_EXTERNAL_AUDIT_TYPE_SETTING_NAME =
            "onGenResizeActionWorkflow";
    private final ThinTargetCache thinTargetCache = Mockito.mock(ThinTargetCache.class);

    /**
     * EnumBasedSettingSpecStore should contain settings from
     * {@link com.vmturbo.components.common.setting.ActionSettingSpecs}.
     */
    @Test
    public void testActionModeSubSettingsGenerated() {
        // emulate that there is target supported external approval and audit features in
        // order not to filter out external approval and audit settings
        Mockito.when(thinTargetCache.getAvailableProbeFeatures())
                .thenReturn(Sets.newHashSet(ProbeFeature.ACTION_APPROVAL,
                        ProbeFeature.ACTION_AUDIT, ProbeFeature.DISCOVERY));
        final EnumBasedSettingSpecStore enumBasedSettingSpecStore =
                new EnumBasedSettingSpecStore(false, false, thinTargetCache);
        Assert.assertTrue(enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXECUTION_SETTING_NAME)
                .isPresent());
        Assert.assertTrue(
                enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXTERNAL_APPROVAL_TYPE_SETTING_NAME)
                        .isPresent());
        Assert.assertTrue(
                enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXTERNAL_AUDIT_TYPE_SETTING_NAME)
                        .isPresent());
    }

    /**
     * Test that external audit and approval settings are not available if there is no added
     * orchestration target.
     */
    @Test
    public void testWhenOrchestratorRelatedSettingAreNotAvailable() {
        // emulate that there is no target supported external approval and audit features
        Mockito.when(thinTargetCache.getAvailableProbeFeatures())
                .thenReturn(Sets.newHashSet(ProbeFeature.SUPPLY_CHAIN,
                        ProbeFeature.DISCOVERY));
        final EnumBasedSettingSpecStore enumBasedSettingSpecStore =
                new EnumBasedSettingSpecStore(false, false, thinTargetCache);
        Assert.assertFalse(
                enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXTERNAL_AUDIT_TYPE_SETTING_NAME)
                        .isPresent());
        Assert.assertFalse(
                enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXTERNAL_APPROVAL_TYPE_SETTING_NAME)
                        .isPresent());
        final Collection<String> allAvailableSettingSpecs =
                Collections2.transform(enumBasedSettingSpecStore.getAllSettingSpecs(),
                        SettingSpec::getName);
        Assert.assertFalse(
                allAvailableSettingSpecs.contains(RESIZE_EXTERNAL_AUDIT_TYPE_SETTING_NAME));
        Assert.assertFalse(
                allAvailableSettingSpecs.contains(RESIZE_EXTERNAL_APPROVAL_TYPE_SETTING_NAME));
    }

    /**
     * EXTERNAL_APPROVAL should be hidden when the feature flag says to hide it.
     */
    @Test
    public void testHiddenValues() {
        EnumBasedSettingSpecStore enumBasedSettingSpecStore = new EnumBasedSettingSpecStore(
                false, true, thinTargetCache);
        Assert.assertFalse(enumBasedSettingSpecStore.getSettingSpec(
            ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds.getSettingName())
                .get()
                .getEnumSettingValueType()
                .getEnumValuesList()
                .contains(ActionMode.EXTERNAL_APPROVAL.name()));
        Assert.assertFalse(
                enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXTERNAL_APPROVAL_TYPE_SETTING_NAME)
                        .isPresent());

        enumBasedSettingSpecStore = new EnumBasedSettingSpecStore(
                true, false, thinTargetCache);
        Assert.assertFalse(
                enumBasedSettingSpecStore.getSettingSpec(RESIZE_EXECUTION_SETTING_NAME).isPresent());
    }

}
