package com.vmturbo.topology.processor.group.settings;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Simple topology processor setting representation for protobuf settings.
 */
public abstract class SimpleTopologyProcessorSetting extends TopologyProcessorSetting<Setting> {

    /**
     * Constructor of {@link SimpleTopologyProcessorSetting}.
     *
     * @param settingValue internal setting value
     */
    protected SimpleTopologyProcessorSetting(@Nonnull Setting settingValue) {
        super(settingValue);
    }

    @Nonnull
    @Override
    protected String getSettingSpecName() {
        return getSettingValue().getSettingSpecName();
    }

    /**
     * Defines winner between two settings depends on tiebreaker value.
     *
     * @param currentSetting current setting value
     * @param newSetting new setting value
     * @param comparisonResult comparison results after comparing two settings
     * @param tiebreaker tiebreaker value
     * @return Pair of winner setting and false second value because there were no merging
     * between settings values
     */
    @Nonnull
    protected static Pair<TopologyProcessorSetting<?>, Boolean> defineWinnerSetting(
            @Nonnull TopologyProcessorSetting<Setting> currentSetting,
            @Nonnull TopologyProcessorSetting<Setting> newSetting,
            int comparisonResult, @Nonnull SettingTiebreaker tiebreaker) {
        TopologyProcessorSetting<Setting> winnerSetting;
        switch (tiebreaker) {
            case BIGGER:
                if (comparisonResult >= 0) {
                    winnerSetting = currentSetting;
                } else {
                    winnerSetting = newSetting;
                }
                break;
            case SMALLER:
                if (comparisonResult <= 0) {
                    winnerSetting = currentSetting;
                } else {
                    winnerSetting = newSetting;
                }
                break;
            default:
                // shouldn't reach here.
                throw new IllegalArgumentException(
                        "Illegal tiebreaker value - " + tiebreaker + " for comparing `"
                                + currentSetting.getSettingSpecName()
                                + "` settings with value type `" + currentSetting.getSettingValue()
                                .getValueCase() + "`.");
        }
        return new Pair<>(winnerSetting, false);
    }
}
