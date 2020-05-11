package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;

/**
 * Class combined values of several settings(ActionMode and ExecutionSchedule).
 */
public class ActionModeExecutionScheduleSetting {
    private final EnumSettingValue actionMode;
    private final Collection<Long> executionSchedules;
    private final String settingName;

    /**
     * Constructor of {@link ActionModeExecutionScheduleSetting}.
     *
     * @param actionMode value of ActionMode setting
     * @param executionSchedules collection of execution schedules related to this action mode
     * @param settingName setting name (used setting name of ActionMode setting)
     */
    public ActionModeExecutionScheduleSetting(@Nonnull EnumSettingValue actionMode,
            @Nonnull Collection<Long> executionSchedules, @Nonnull String settingName) {
        this.actionMode = Objects.requireNonNull(actionMode);
        this.executionSchedules =
                Collections.unmodifiableCollection(Objects.requireNonNull(executionSchedules));
        this.settingName = Objects.requireNonNull(settingName);
    }

    /**
     * Return action mode value represented by
     * {@link com.vmturbo.common.protobuf.setting.SettingProto.Setting} with EnumSetting value.
     *
     * @return action mode value
     */
    @Nonnull
    public EnumSettingValue getActionMode() {
        return actionMode;
    }

    /**
     * Return collection of execution schedule windows. Defines periods when action with
     * corresponding action mode could be executed.
     *
     * @return list of schedule ids
     */
    @Nonnull
    public Collection<Long> getExecutionSchedules() {
        return executionSchedules;
    }

    /**
     * Return combined setting name equals setting name of ActionMode setting.
     *
     * @return setting name.
     */
    @Nonnull
    public String getSettingName() {
        return settingName;
    }
}
