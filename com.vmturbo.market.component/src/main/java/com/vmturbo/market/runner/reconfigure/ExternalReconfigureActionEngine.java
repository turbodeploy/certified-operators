package com.vmturbo.market.runner.reconfigure;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.market.runner.reconfigure.vcpu.CoresPerSocketReconfigureActionGenerator;
import com.vmturbo.market.runner.reconfigure.vcpu.SocketReconfigureActionGenerator;

/**
 * Perform reconfiguration actions analysis.
 */
public class ExternalReconfigureActionEngine {
    private final SettingPolicyServiceBlockingStub settingPolicyService;
    private final List<ExternalReconfigureActionGenerator> externalReconfigureActionGenerators =
            ImmutableList.of(new CoresPerSocketReconfigureActionGenerator(),
                    new SocketReconfigureActionGenerator());

    /**
     * Consturctor.
     *
     * @param settingPolicyService settingService
     */
    public ExternalReconfigureActionEngine(SettingPolicyServiceBlockingStub settingPolicyService) {
        this.settingPolicyService = settingPolicyService;
    }

    /**
     * Execute analysis.
     *
     * @param topologyDTOs topologyDTOs
     * @param existingActions Actions already generated.
     * @return Generated actions.
     */
    public List<Action> execute(Map<Long, TopologyEntityDTO> topologyDTOs,
            Collection<Action> existingActions) {
        List<Action> results = new ArrayList<>();
        for (ExternalReconfigureActionGenerator generator : externalReconfigureActionGenerators) {
            results.addAll(generator.execute(settingPolicyService, topologyDTOs, existingActions));
        }
        return results;
    }
}
