package com.vmturbo.topology.processor.group.settings;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Numeric topology processor setting - wrapper of protobuf setting with numeric value.
 */
public class NumericTopologyProcessorSetting extends SimpleTopologyProcessorSetting {

    /**
     * Constructor of {@link NumericTopologyProcessorSetting}.
     *
     * @param settingValue internal setting value
     */
    protected NumericTopologyProcessorSetting(@Nonnull Setting settingValue) {
        super(settingValue);
    }

    @Nonnull
    @Override
    protected Pair<TopologyProcessorSetting<?>, Boolean> resolveConflict(
            @Nonnull TopologyProcessorSetting<Setting> otherSetting,
            @Nonnull SettingTiebreaker tiebreaker, @Nonnull SettingSpec settingSpec) {
        final int comparisionResult =
                Float.compare(getSettingValue().getNumericSettingValue().getValue(),
                        otherSetting.getSettingValue().getNumericSettingValue().getValue());
        return defineWinnerSetting(this, otherSetting, comparisionResult, tiebreaker);
    }
}
