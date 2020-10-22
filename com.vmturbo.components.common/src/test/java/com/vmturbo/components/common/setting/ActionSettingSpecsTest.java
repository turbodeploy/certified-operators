package com.vmturbo.components.common.setting;

import static org.hamcrest.Matchers.is;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.setting.SettingProto.AvailableEnumValues;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec.SettingValueTypeCase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Verifies that {@link ActionSettingSpecs} generates sub settings for each action mode setting.
 */
public class ActionSettingSpecsTest {

    private static final String EXECUTION_SCHEDULE_SETTING_NAME_SUFFIX = "ExecutionSchedule";
    private static final String ACTION_WORKFLOW_SUFFIX = "ActionWorkflow";
    private static final String APPROVAL_SETTING_NAME_PREFIX = "approval";
    private static final String ON_GENERATION_SETTING_NAME_PREFIX = "onGen";
    private static final String AFTER_EXECUTION_SETTING_NAME_PREFIX = "afterExec";
    private static final String UNKNOWN_SETTING = "UnknownSetting";

    /**
     * Each action mode setting in {@link EntitySettingSpecs} should have a generated setting.
     * There should be 4 settings for each: external approval, external audit on generation,
     * external audit after execution, and execution schedule.
     */
    @Test
    public void testSettingsGenerated() {
        for (ConfigurableActionSettings actionModeSettingSpecs : ConfigurableActionSettings.values()) {
                String executionScheduleName = actionModeSettingSpecs.getSettingName()
                    + EXECUTION_SCHEDULE_SETTING_NAME_SUFFIX;
                String externalApprovalName = APPROVAL_SETTING_NAME_PREFIX
                    + StringUtils.capitalize(actionModeSettingSpecs.getSettingName())
                    + ACTION_WORKFLOW_SUFFIX;
                String onGenAuditName = ON_GENERATION_SETTING_NAME_PREFIX
                    + StringUtils.capitalize(actionModeSettingSpecs.getSettingName())
                    + ACTION_WORKFLOW_SUFFIX;
                String afterExecAuditName = AFTER_EXECUTION_SETTING_NAME_PREFIX
                    + StringUtils.capitalize(actionModeSettingSpecs.getSettingName())
                    + ACTION_WORKFLOW_SUFFIX;

                Assert.assertTrue(ActionSettingSpecs.isActionModeSetting(
                    actionModeSettingSpecs.getSettingName()));
                Assert.assertEquals(
                    executionScheduleName,
                    ActionSettingSpecs.getSubSettingFromActionModeSetting(
                        actionModeSettingSpecs.getSettingName(), ActionSettingType.SCHEDULE));
                Assert.assertEquals(
                    executionScheduleName,
                    ActionSettingSpecs.getSubSettingFromActionModeSetting(
                        actionModeSettingSpecs, ActionSettingType.SCHEDULE));
                Assert.assertEquals(
                    actionModeSettingSpecs.getSettingName(),
                    ActionSettingSpecs.getActionModeSettingFromExecutionScheduleSetting(
                        executionScheduleName));
                Assert.assertTrue(ActionSettingSpecs.isExecutionScheduleSetting(
                    executionScheduleName));
                Assert.assertTrue(ActionSettingSpecs.isExternalApprovalOrAuditSetting(
                    externalApprovalName));
                Assert.assertTrue(ActionSettingSpecs.isExternalApprovalOrAuditSetting(
                    onGenAuditName));
                Assert.assertTrue(ActionSettingSpecs.isExternalApprovalOrAuditSetting(
                    afterExecAuditName));
                Assert.assertTrue(ActionSettingSpecs.isActionModeSubSetting(executionScheduleName));
                Assert.assertTrue(ActionSettingSpecs.isActionModeSubSetting(externalApprovalName));
                Assert.assertTrue(ActionSettingSpecs.isActionModeSubSetting(onGenAuditName));
                Assert.assertTrue(ActionSettingSpecs.isActionModeSubSetting(afterExecAuditName));
                checkSettingSpec(executionScheduleName,
                    SettingValueTypeCase.SORTED_SET_OF_OID_SETTING_VALUE_TYPE);
                checkSettingSpec(externalApprovalName,
                    SettingValueTypeCase.STRING_SETTING_VALUE_TYPE);
                checkSettingSpec(onGenAuditName, SettingValueTypeCase.STRING_SETTING_VALUE_TYPE);
                checkSettingSpec(afterExecAuditName, SettingValueTypeCase.STRING_SETTING_VALUE_TYPE);
        }
    }

    /**
     * When a setting is not found, boolean method should return false and methods that return
     * objects should return null.
     */
    @Test
    public void testNotFound() {
        String expectedNotFoundSetting = EntitySettingSpecs.CpuUtilization.getSettingName();
        Assert.assertFalse(ActionSettingSpecs.isActionModeSetting(expectedNotFoundSetting));
        Assert.assertNull(
            ActionSettingSpecs.getSubSettingFromActionModeSetting(
                expectedNotFoundSetting, ActionSettingType.SCHEDULE));
        Assert.assertNull(
            ActionSettingSpecs.getActionModeSettingFromExecutionScheduleSetting(
                expectedNotFoundSetting));
        Assert.assertFalse(ActionSettingSpecs.isExecutionScheduleSetting(expectedNotFoundSetting));
        Assert.assertFalse(
            ActionSettingSpecs.isExternalApprovalOrAuditSetting(expectedNotFoundSetting));
        Assert.assertFalse(ActionSettingSpecs.isActionModeSubSetting(expectedNotFoundSetting));
    }

    /**
     * Unknown setting name should not cause an exception. Null should be returned instead.
     */
    @Test
    public void testUnknownSettingSpec() {
        Assert.assertEquals(null, ActionSettingSpecs.getSettingSpec(UNKNOWN_SETTING));
    }

    private void checkSettingSpec(String settingName,
                                  SettingValueTypeCase expectedSettingValueTypeCase) {
        SettingSpec settingSpec = ActionSettingSpecs.getSettingSpec(settingName);
        Assert.assertNotNull(settingSpec);
        Assert.assertEquals(settingName, settingSpec.getName());
        Assert.assertEquals(expectedSettingValueTypeCase, settingSpec.getSettingValueTypeCase());
    }

    /**
     * Test EntityEnumValuesMap.
     */
    @Test
    public void testEntityEnumValues() {
        final EnumSettingValueType resizeEnum = ActionSettingSpecs.getSettingSpec(
            ConfigurableActionSettings.Resize.getSettingName()).getEnumSettingValueType();
        Assert.assertEquals(Arrays.asList(ActionMode.DISABLED.name(), ActionMode.RECOMMEND.name(),
            ActionMode.EXTERNAL_APPROVAL.name(), ActionMode.MANUAL.name(), ActionMode.AUTOMATIC.name()),
            resizeEnum.getEnumValuesList());
        Assert.assertThat(resizeEnum.getEntityEnumValuesCount(), is(1));
        Assert.assertTrue(resizeEnum.getEntityEnumValuesMap().containsKey(EntityType.SWITCH_VALUE));
        Assert.assertEquals(AvailableEnumValues.newBuilder().addAllEnumValues(
            Arrays.asList(ActionMode.DISABLED.name(), ActionMode.RECOMMEND.name())).build(),
            resizeEnum.getEntityEnumValuesMap().get(EntityType.SWITCH_VALUE));
    }
}
