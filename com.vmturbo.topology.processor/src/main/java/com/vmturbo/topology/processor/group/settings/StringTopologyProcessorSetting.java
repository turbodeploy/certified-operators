package com.vmturbo.topology.processor.group.settings;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * String topology processor setting - wrapper of protobuf setting with String value.
 */
public class StringTopologyProcessorSetting extends SimpleTopologyProcessorSetting {

    /**
     * Constructor of {@link StringTopologyProcessorSetting}.
     *
     * @param settingValue internal setting value
     */
    protected StringTopologyProcessorSetting(@Nonnull Setting settingValue) {
        super(settingValue);
    }

    @Nonnull
    @Override
    protected Pair<TopologyProcessorSetting<?>, Boolean> resolveConflict(
            @Nonnull TopologyProcessorSetting<Setting> otherSetting,
            @Nonnull SettingTiebreaker tiebreaker, @Nonnull SettingSpec settingSpec) {
        int comparisonResult = getSettingValue().getStringSettingValue()
                .getValue()
                .compareTo(otherSetting.getSettingValue().getStringSettingValue().getValue());
        return defineWinnerSetting(this, otherSetting, comparisonResult, tiebreaker);
    }
}
