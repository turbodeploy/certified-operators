package com.vmturbo.market.runner.reconfigure;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Abstract class to generate reconfiguration actions.
 */
abstract class ExternalReconfigureActionGenerator {
        abstract List<Action> execute(@Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
                @Nonnull Map<Long, TopologyEntityDTO> topologyEntities);
}
