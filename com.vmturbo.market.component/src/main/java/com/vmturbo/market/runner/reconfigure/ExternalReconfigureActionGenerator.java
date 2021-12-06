package com.vmturbo.market.runner.reconfigure;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Abstract class to generate reconfiguration actions.
 */
public abstract class ExternalReconfigureActionGenerator {
    /**
     * Generate reconfigure actions.
     *
     * @param settingPolicyService Setting policy service.
     * @param topologyEntities Id to entityDTOs.
     * @param existingActions Actions already generated before doing this reconfiguration
     *         analysis.
     * @return A list of generated Reconfigure actions.
     */
    protected abstract List<Action> execute(
            @Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities,
            @Nonnull Collection<Action> existingActions);
}
