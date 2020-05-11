package com.vmturbo.topology.processor.group.settings;

import javax.annotation.Nonnull;

import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Enum topology processor setting - wrapper of protobuf setting with Enum value.
 */
class EnumTopologyProcessorSetting extends SimpleTopologyProcessorSetting {

    /**
     * Constructor of {@link EnumTopologyProcessorSetting}.
     *
     * @param settingValue internal setting value
     */
    protected EnumTopologyProcessorSetting(@Nonnull Setting settingValue) {
        super(settingValue);
    }

    @NotNull
    @Override
    protected Pair<TopologyProcessorSetting<?>, Boolean> resolveConflict(
            @Nonnull TopologyProcessorSetting<Setting> otherSetting,
            @Nonnull SettingTiebreaker tiebreaker, @Nonnull SettingSpec settingSpec) {
        final int comparisonResult =
                SettingDTOUtil.compareEnumSettingValues(getSettingValue().getEnumSettingValue(),
                        otherSetting.getSettingValue().getEnumSettingValue(),
                        settingSpec.getEnumSettingValueType());
        return defineWinnerSetting(this, otherSetting, comparisonResult, tiebreaker);
    }
}
