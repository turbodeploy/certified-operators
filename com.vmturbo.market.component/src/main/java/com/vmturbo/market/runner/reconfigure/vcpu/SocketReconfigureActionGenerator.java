package com.vmturbo.market.runner.reconfigure.vcpu;

import static com.vmturbo.market.topology.conversions.MarketAnalysisUtils.EPSILON;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.VCPUScalingUnitsEnum;
import com.vmturbo.components.common.setting.VcpuScalingCoresPerSocketSocketModeEnum;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Generate cores per socket change actions.
 */
public class SocketReconfigureActionGenerator extends VcpuScalingReconfigureActionGenerator {

    private final Logger logger = LogManager.getLogger();

    @Override
    List<Action> generateActions(@Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities, Collection<Long> sourceVMs) {

        //Retrieve VMs who have VcpuScalingUnits and filter out those whose policy values are CORES.
        Map<Long, Set<Long>> vmsWithScaleInCoresPerSocketToPolicyIds =
                findEntities2ReasonSettingsWithGivenSetting(settingPolicyService, sourceVMs,
                        new HashMap<>(), EntitySettingSpecs.VcpuScalingUnits,
                        setting -> setting.hasEnumSettingValue() && VCPUScalingUnitsEnum.CORES
                            == VCPUScalingUnitsEnum.valueOf(setting.getEnumSettingValue().getValue()));

        //If vcpu scale in cores per socket, but user specifies a socket number, we need to generate reconfigure actions to match the given number.
        List<Action> result = generateUserSpecifiedReconfigureSocketAction(settingPolicyService, topologyEntities, vmsWithScaleInCoresPerSocketToPolicyIds);
        //If vcpu scale in cores per socket, but user choose socket match host, we need to generate reconfigure actions to match host's socket.
        result.addAll(generateMatchHostReconfigureSocketAction(settingPolicyService, topologyEntities, vmsWithScaleInCoresPerSocketToPolicyIds));
        return result;
    }

    /**
     * Three steps to generate user-specified reconfigure actions:
     * 1. Retrieve VMs who have VcpuScalingUnits and filter out those whose policy values are CORES.
     * 2. Retrieve VcpuScaling_CoresPerSocket_SocketMode policy among the filtered VMs, and filter out those whose values are USER_SPECIFIED.
     * 3. Retrieve VcpuScaling_Sockets_CoresPerSocketValue policy among the filtered VMs,
     *    and generate actions for those whose policy values are different from their current cores per socket.
     * @param settingPolicyService the setting service
     * @param topologyEntities entityId to entityDTO
     * @param vmsWithScaleInCoresPerSocketToPolicyIds the VMs have scale in cores per socket policies
     * @return Generated reconfigure actions for VMs have user-specified socket number.
     */
    private List<Action> generateUserSpecifiedReconfigureSocketAction(@Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities,  Map<Long, Set<Long>> vmsWithScaleInCoresPerSocketToPolicyIds ) {
        Map<Long, Set<Long>> vmsWithUserSpecifiedCoresPerSocketToPolicyIds =
                findEntities2ReasonSettingsWithGivenSetting(settingPolicyService,
                        vmsWithScaleInCoresPerSocketToPolicyIds.keySet(),
                        vmsWithScaleInCoresPerSocketToPolicyIds,
                        EntitySettingSpecs.VcpuScaling_CoresPerSocket_SocketMode,
                        setting -> setting.hasEnumSettingValue() && VcpuScalingCoresPerSocketSocketModeEnum.USER_SPECIFIED
                                == VcpuScalingCoresPerSocketSocketModeEnum.valueOf(setting.getEnumSettingValue().getValue()));

        return generateActionsForEntitiesWithUndesiredNumericValue(settingPolicyService,
                EntitySettingSpecs.VcpuScaling_CoresPerSocket_SocketValue,
                vmsWithUserSpecifiedCoresPerSocketToPolicyIds, topologyEntities, getVMCurrentSockets, EntityAttribute.SOCKET);
    }

    /**
     * Three steps to generate match host socket reconfigure actions:
     *  1. Retrieve VMs who have VcpuScalingUnits and filter out those whose policy values are CORES.
     *  2. Retrieve VcpuScaling_CoresPerSocket_SocketMode policy among the filtered VMs, and filter out those whose values are MATCH_HOST.
     *  3. Compare socket number of the VM and the host, and generate actions for the VMs whose sockets are different from their hosts.
     * @param settingPolicyService the setting service
     * @param topologyEntities entityId to entityDTO
     * @param vmsWithScaleInCoresPerSocketToPolicyIds the VMs have scale in cores per socket policies
     * @return Generated reconfigure actions for VMs have match host socket number.
     */
    private List<Action> generateMatchHostReconfigureSocketAction(@Nonnull SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull Map<Long, TopologyEntityDTO> topologyEntities,  Map<Long, Set<Long>> vmsWithScaleInCoresPerSocketToPolicyIds ) {

        Map<Long, Set<Long>> vmsWithMatchHostPolicyToPolicyIds =
                findEntities2ReasonSettingsWithGivenSetting(settingPolicyService,
                        vmsWithScaleInCoresPerSocketToPolicyIds.keySet(),
                        vmsWithScaleInCoresPerSocketToPolicyIds,
                        EntitySettingSpecs.VcpuScaling_CoresPerSocket_SocketMode,
                        setting -> setting.hasEnumSettingValue() && VcpuScalingCoresPerSocketSocketModeEnum.MATCH_HOST
                                == VcpuScalingCoresPerSocketSocketModeEnum.valueOf(setting.getEnumSettingValue().getValue()));

        List<Action> result = new ArrayList<>();
        Map<Long, Integer> hostIdToHostSockets = new HashMap<>();

        for (long vmOid : vmsWithMatchHostPolicyToPolicyIds.keySet()) {
            TopologyEntityDTO vm = topologyEntities.get(vmOid);
            Optional<Long> hostId = vm.getCommoditiesBoughtFromProvidersList().stream()
                    .filter(commBought -> commBought.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                    .findFirst()
                    .map(CommoditiesBoughtFromProvider::getProviderId);
            if (!hostId.isPresent()) {
                //Could be a template VM in plan.
                continue;
            }
            int hostSockets = hostIdToHostSockets.computeIfAbsent(hostId.get(), hostOid ->
                topologyEntities.get(hostOid).getTypeSpecificInfo().getPhysicalMachine().getNumCpuSockets()
            );

            if (hostSockets == 0) {
                continue;
            }

            //If VM's current socket is different from the host's socket, generate actions.
            float currentSocket = getVMCurrentSockets.apply(vm);
            if (Math.abs(currentSocket - hostSockets) > EPSILON) {
                Set<Long> reasonPolicies = vmsWithScaleInCoresPerSocketToPolicyIds.get(vmOid);
                Action reconfigureAction =
                        generateReconfigureActionWithEntityAttribute(vm,
                                currentSocket, hostSockets, reasonPolicies, EntityAttribute.SOCKET);
                result.add(reconfigureAction);
            }
        }
        return result;
    }

    private Function<TopologyEntityDTO, Float> getVMCurrentSockets = vm -> {
        //Calculate the current socket number of this VM.
        VirtualMachineInfo vmInfo = vm.getTypeSpecificInfo().getVirtualMachine();
        float numCpus = vmInfo.getNumCpus();
        float coresPerSocket = vmInfo.getCoresPerSocketRatio();
        if (coresPerSocket == 0) {
            logger.warn("VM {} got a 0 cores per socket so current sockets can't be calculated, using numCPUs", vm.getDisplayName());
            return numCpus;
        }
        return numCpus / coresPerSocket;
    };
}
