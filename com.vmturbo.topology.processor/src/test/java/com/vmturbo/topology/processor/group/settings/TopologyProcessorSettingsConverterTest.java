package com.vmturbo.topology.processor.group.settings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * Test class for {@link TopologyProcessorSettingsConverter}.
 */
public class TopologyProcessorSettingsConverterTest {

    private static final Setting BOOLEAN_SETTING = Setting.newBuilder()
            .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true).build())
            .setSettingSpecName("boolean_setting")
            .build();

    private static final Setting STRING_SETTING = Setting.newBuilder()
            .setStringSettingValue(StringSettingValue.newBuilder().setValue("Test").build())
            .setSettingSpecName("string_setting")
            .build();

    private static final Setting ENUM_SETTING = Setting.newBuilder()
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("Test").build())
            .setSettingSpecName("enum_value")
            .build();

    private static final Setting NUMERIC_SETTING = Setting.newBuilder()
            .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(20F).build())
            .setSettingSpecName("numeric_value")
            .build();

    private static final Setting SET_OF_OID_SETTING = Setting.newBuilder()
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(Arrays.asList(1L, 2L))
                    .build())
            .setSettingSpecName("set_of_oid_value")
            .build();

    private static final Setting ACTION_MODE_SETTING = Setting.newBuilder()
            .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("AUTOMATIC").build())
            .setSettingSpecName(EntitySettingSpecs.Move.getSettingName())
            .build();

    private static final Setting EXECUTION_SCHEDULE_SETTING = Setting.newBuilder()
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(Arrays.asList(1L, 2L))
                    .build())
            .setSettingSpecName(EntitySettingSpecs.MoveExecutionSchedule.getSettingName())
            .build();

    /**
     * Test case when policy contains single setting.
     */
    @Test
    public void testConvertingSingleSettingToTopologyProcessorSetting() {
        final List<Setting> singleProtoSettings =
                Arrays.asList(BOOLEAN_SETTING, STRING_SETTING, NUMERIC_SETTING, ENUM_SETTING,
                        SET_OF_OID_SETTING);

        for (Setting setting : singleProtoSettings) {
            final Collection<Setting> collectionWithSingleSetting =
                    Collections.singletonList(setting);
            final TopologyProcessorSetting<?> topologyProcessorSetting =
                    TopologyProcessorSettingsConverter.toTopologyProcessorSetting(
                            collectionWithSingleSetting);
            Assert.assertEquals(collectionWithSingleSetting,
                    TopologyProcessorSettingsConverter.toProtoSettings(topologyProcessorSetting));
        }
    }

    /**
     * Test case when policy contains ActionMode setting with corresponding ExecutionSchedule
     * setting.
     */
    @Test
    public void testActionModeAndExecutionScheduleSettings() {
        final List<Setting> groupedSettings =
                Arrays.asList(ACTION_MODE_SETTING, EXECUTION_SCHEDULE_SETTING);
        final TopologyProcessorSetting<?> topologyProcessorCombinedSetting =
                TopologyProcessorSettingsConverter.toTopologyProcessorSetting(groupedSettings);

        Assert.assertTrue(
                topologyProcessorCombinedSetting instanceof ActionModeExecutionScheduleTopologyProcessorSetting);
        final ActionModeExecutionScheduleSetting actionModeExecutionScheduleSetting =
                (ActionModeExecutionScheduleSetting)topologyProcessorCombinedSetting.getSettingValue();
        Assert.assertEquals(actionModeExecutionScheduleSetting.getActionMode(),
                ACTION_MODE_SETTING.getEnumSettingValue());
        Assert.assertTrue(CollectionUtils.isEqualCollection(
                actionModeExecutionScheduleSetting.getExecutionSchedules(),
                EXECUTION_SCHEDULE_SETTING.getSortedSetOfOidSettingValue().getOidsList()));
        Assert.assertEquals(groupedSettings, TopologyProcessorSettingsConverter.toProtoSettings(
                topologyProcessorCombinedSetting));
    }

    /**
     * Test case when policy contains ActionMode setting without corresponding ExecutionSchedule
     * setting.
     */
    @Test
    public void testOnlyActionModeSetting() {
        final List<Setting> actionModeSetting = Collections.singletonList(ACTION_MODE_SETTING);
        final TopologyProcessorSetting<?> topologyProcessorCombinedSetting =
                TopologyProcessorSettingsConverter.toTopologyProcessorSetting(actionModeSetting);

        Assert.assertTrue(
                topologyProcessorCombinedSetting instanceof ActionModeExecutionScheduleTopologyProcessorSetting);
        final ActionModeExecutionScheduleSetting actionModeExecutionScheduleSetting =
                (ActionModeExecutionScheduleSetting)topologyProcessorCombinedSetting.getSettingValue();
        Assert.assertEquals(actionModeExecutionScheduleSetting.getActionMode(),
                ACTION_MODE_SETTING.getEnumSettingValue());
        Assert.assertTrue(actionModeExecutionScheduleSetting.getExecutionSchedules().isEmpty());
        Assert.assertEquals(actionModeSetting, TopologyProcessorSettingsConverter.toProtoSettings(
                topologyProcessorCombinedSetting));
    }

}
