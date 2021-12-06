package com.vmturbo.market.runner.reconfigure.vcpu;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.VCPUScalingUnitsEnum;
import com.vmturbo.components.common.setting.VcpuScalingSocketsCoresPerSocketModeEnum;

/**
 * Generate cores per socket change actions.
 */
public class CoresPerSocketReconfigureActionGenerator
        extends VcpuScalingReconfigureActionGenerator {

    /**
     * Three steps to generate actions:
     * 1. Retrieve VMs who have VcpuScalingUnits and filter out those whose policy values are SOCKET.
     * 2. Retrieve VcpuScaling_Socket_CoresPerSocketMode policy among the filtered VMs, and filter out those whose values are USER_SPECIFIED.
     * 3. Retrieve VcpuScaling_Sockets_CoresPerSocketValue policy among the filtered VMs,
     *    and generate actions for those whose policy values are different from their current cores per socket.
     * @param settingPolicyService the setting service
     * @param topologyEntities entityId to entityDTO
     * @param sourceVMs the VMs to be processed
     * @return Generated reconfigure actions for VMs have user-specified cores per socket number.
     */
    @Override
    List<Action> generateActions(@Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities, Collection<Long> sourceVMs) {

        Map<Long, Set<Long>> vmsWithScaleInSocketsToPolicyIds =
                findEntities2ReasonSettingsWithGivenSetting(settingPolicyService, sourceVMs,
                        new HashMap<>(), EntitySettingSpecs.VcpuScalingUnits,
                        setting -> setting.hasEnumSettingValue()
                                && VCPUScalingUnitsEnum.SOCKETS == VCPUScalingUnitsEnum.valueOf(
                                setting.getEnumSettingValue().getValue()));

        Map<Long, Set<Long>> vmsWithUserSpecifiedCoresPerSocketToPolicyIds =
                findEntities2ReasonSettingsWithGivenSetting(settingPolicyService,
                        vmsWithScaleInSocketsToPolicyIds.keySet(), vmsWithScaleInSocketsToPolicyIds,
                        EntitySettingSpecs.VcpuScaling_Sockets_CoresPerSocketMode,
                        setting -> setting.hasEnumSettingValue()
                                && VcpuScalingSocketsCoresPerSocketModeEnum.USER_SPECIFIED
                                == VcpuScalingSocketsCoresPerSocketModeEnum.valueOf(
                                setting.getEnumSettingValue().getValue()));
        return generateActionsForEntitiesWithUndesiredNumericValue(settingPolicyService,
                EntitySettingSpecs.VcpuScaling_Sockets_CoresPerSocketValue,
                vmsWithUserSpecifiedCoresPerSocketToPolicyIds, topologyEntities,
                entity -> (float)entity.getTypeSpecificInfo()
                        .getVirtualMachine()
                        .getCoresPerSocketRatio(), EntityAttribute.CORES_PER_SOCKET);
    }
}
