package com.vmturbo.topology.processor.group.settings;

import static com.vmturbo.common.protobuf.ListUtil.mergeTwoSortedListsAndRemoveDuplicates;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Set of oid topology processor setting - wrapper of protobuf setting with set of oid value.
 */
public class SortedSetOfOidTopologyProcessorSetting extends SimpleTopologyProcessorSetting {

    /**
     * Constructor of {@link SortedSetOfOidTopologyProcessorSetting}.
     *
     * @param settingValue internal setting value
     */
    protected SortedSetOfOidTopologyProcessorSetting(@Nonnull Setting settingValue) {
        super(settingValue);
    }

    @Nonnull
    @Override
    protected Pair<TopologyProcessorSetting<?>, Boolean> resolveConflict(
            @Nonnull TopologyProcessorSetting<Setting> otherSetting,
            @Nonnull SettingTiebreaker tiebreaker, @Nonnull SettingSpec settingSpec) {

        final List<Long> currentOids =
                this.getSettingValue().getSortedSetOfOidSettingValue().getOidsList();
        final List<Long> otherOids = otherSetting.getSettingValue()
                        .getSortedSetOfOidSettingValue()
                        .getOidsList();
        final List<Long> sortedOids = mergeTwoSortedListsAndRemoveDuplicates(currentOids, otherOids);
        final SortedSetOfOidTopologyProcessorSetting winnerSetting =
                new SortedSetOfOidTopologyProcessorSetting(Setting.newBuilder()
                        .setSettingSpecName(getSettingValue().getSettingSpecName())
                        .setSortedSetOfOidSettingValue(
                                SortedSetOfOidSettingValue.newBuilder().addAllOids(sortedOids))
                        .build());

        return new Pair<>(winnerSetting, true);
    }
}
