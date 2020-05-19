package com.vmturbo.topology.processor.group.settings;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Representation of protobuf settings inside topology processor component. Topology processor
 * settings used for conflict resolution.
 *
 * @param <T> define internal type of setting (e.g. simple protobuf setting or complex object
 * combined several settings)
 */
public abstract class TopologyProcessorSetting<T> {

    /**
     * Contains setting value. Can be represented by protofub setting or complex object combined
     * several settings
     */
    private final T settingValue;

    /**
     * Constructor of {@link TopologyProcessorSetting}.
     *
     * @param settingValue internal setting value
     */
    protected TopologyProcessorSetting(T settingValue) {
        this.settingValue = settingValue;
    }

    /**
     * Compares current setting with other one with the same type and define the winner
     * setting depends on tiebreaker value.
     * Returns pair of winner setting and boolean value (defining whether settings merged or not).
     *
     * @param otherSetting setting compared with current setting
     * @param tiebreaker defines the strategy used to resolve conflicts
     * @param settingSpec setting spec
     * @return {@link Pair} contains winner setting and boolean value (if it is true than winner
     * policy combines values of several settings otherwise false)
     */
    @Nonnull
    protected abstract Pair<TopologyProcessorSetting<?>, Boolean> resolveConflict(
            @Nonnull TopologyProcessorSetting<T> otherSetting,
            @Nonnull SettingTiebreaker tiebreaker, @Nonnull SettingSpec settingSpec);

    /**
     * Returns the name of the setting.
     *
     * @return setting name
     */
    @Nonnull
    protected abstract String getSettingSpecName();

    /**
     * Returns internal value of topology processor setting representation.
     *
     * @return internal setting value
     */
    @Nonnull
    protected T getSettingValue() {
        return settingValue;
    }
}
