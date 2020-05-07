package com.vmturbo.topology.processor.group.settings;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Boolean topology processor setting - wrapper of protobuf setting with boolean value.
 */
public class BooleanTopologyProcessorSetting extends SimpleTopologyProcessorSetting {

    /**
     * Constructor of {@link BooleanTopologyProcessorSetting}.
     *
     * @param settingValue internal setting value
     */
    protected BooleanTopologyProcessorSetting(@Nonnull Setting settingValue) {
        super(settingValue);
    }

    @Nonnull
    @Override
    protected Pair<TopologyProcessorSetting<?>, Boolean> resolveConflict(
            @Nonnull TopologyProcessorSetting<Setting> otherSetting,
            @Nonnull SettingTiebreaker tiebreaker, @Nonnull SettingSpec settingSpec) {
        final int comparisonResult =
                Boolean.compare(getSettingValue().getBooleanSettingValue().getValue(),
                        otherSetting.getSettingValue().getBooleanSettingValue().getValue());
        return defineWinnerSetting(this, otherSetting, comparisonResult, tiebreaker);
    }
}
