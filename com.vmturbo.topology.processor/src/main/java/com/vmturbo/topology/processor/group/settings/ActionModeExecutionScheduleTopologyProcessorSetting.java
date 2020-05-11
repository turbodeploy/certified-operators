package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Combined setting. Contains values from ActionMode and associated ExecutionSchedule settings.
 */
public class ActionModeExecutionScheduleTopologyProcessorSetting extends
        TopologyProcessorSetting<ActionModeExecutionScheduleSetting> {

    /**
     * Constructor of {@link ActionModeExecutionScheduleTopologyProcessorSetting}.
     *
     * @param setting composite setting
     */
    protected ActionModeExecutionScheduleTopologyProcessorSetting(
            @Nonnull ActionModeExecutionScheduleSetting setting) {
        super(setting);
    }

    @Nonnull
    @Override
    protected Pair<TopologyProcessorSetting<?>, Boolean> resolveConflict(
            @Nonnull TopologyProcessorSetting<ActionModeExecutionScheduleSetting> otherSetting,
            @Nonnull SettingTiebreaker tiebreaker,
            @Nonnull SettingSpec settingSpec) {
        final EnumSettingValue existingActionMode = getSettingValue().getActionMode();
        final Collection<Long> existingExecutionWindows = getSettingValue().getExecutionSchedules();

        final EnumSettingValue newActionMode = otherSetting.getSettingValue().getActionMode();
        final Collection<Long> newExecutionWindows = otherSetting.getSettingValue().getExecutionSchedules();

        int comparisonResults =
                SettingDTOUtil.compareEnumSettingValues(existingActionMode,
                        newActionMode, settingSpec.getEnumSettingValueType());

        // define winner setting
        if (comparisonResults == 0) {
            final Set<Long> mergedExecutionSchedules =
                    new HashSet<>(existingExecutionWindows.size());
            mergedExecutionSchedules.addAll(existingExecutionWindows);
            mergedExecutionSchedules.addAll(newExecutionWindows);
            final ActionModeExecutionScheduleSetting
                    actionModeExecutionScheduleSetting =
                    new ActionModeExecutionScheduleSetting(existingActionMode, mergedExecutionSchedules,
                            settingSpec.getName());
            return new Pair<>(new ActionModeExecutionScheduleTopologyProcessorSetting(
                    actionModeExecutionScheduleSetting), true);
        } else if (comparisonResults > 0) {
            return new Pair<>(otherSetting, false);
        }
        return new Pair<>(this, false);
    }

    @Nonnull
    @Override
    protected String getSettingSpecName() {
        return getSettingValue().getSettingName();
    }
}
