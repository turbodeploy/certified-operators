package com.vmturbo.market.topology.conversions;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;

/**
 * This class is used to retrieve "Savings vs Reversibility" setting for analysed entities.
 */
public class ReversibilitySettingFetcher {

    private final SettingPolicyServiceBlockingStub settingPolicyService;

    /**
     * Constructs new {@code ReversibilitySettingRetriever} instance.
     *
     * @param settingPolicyService Setting policy service client (used to fetch policy settings
     * from Group component).
     */
    public ReversibilitySettingFetcher(
            @Nonnull final SettingPolicyServiceBlockingStub settingPolicyService) {
        this.settingPolicyService = settingPolicyService;
    }

    /**
     * Get entities with reversibility mode.
     *
     * @return Set of entity OIDs.
     */
    public Set<Long> getEntityOidsWithReversibilityPreferred() {
        final String settingName = EntitySettingSpecs.PreferSavingsOverReversibility
                .getSettingName();
        final GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
                .setSettingFilter(EntitySettingFilter.newBuilder()
                        .addSettingName(settingName)
                        .build())
                .setIncludeSettingPolicies(false)
                .build();
        // Get OIDs of all entities with PreferSavingsOverReversibility = false
        return SettingDTOUtil.flattenEntitySettings(settingPolicyService.getEntitySettings(request))
                .filter(group -> !group.getSetting().getBooleanSettingValue().getValue())
                .map(EntitySettingGroup::getEntityOidsList)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }
}
