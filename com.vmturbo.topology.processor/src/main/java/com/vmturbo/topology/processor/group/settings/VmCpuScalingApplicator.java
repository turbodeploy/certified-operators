package com.vmturbo.topology.processor.group.settings;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.VCPUScalingUnitsEnum;
import com.vmturbo.components.common.setting.VcpuScalingCoresPerSocketSocketModeEnum;
import com.vmturbo.components.common.setting.VcpuScalingSocketsCoresPerSocketModeEnum;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Applicator to apply VM CPU scaling settings. The following modes will be handled: VCPU scaling
 * mode:
 * <ul>
 *     <li>MHz - legacy mode transforms implicitly amount of MHz in capacity into amount of cores
 *     that required by a target entity;</li>
 *     <li>Cores - mode which changes total number of cores on the VM. It can support three modes
 *     to change Sockets:
 *     <ul>
 *     <li>Preserve - keeps current ratio between total number of cores and cores per socket ratio
 *     configured for the target entity;</li>
 *     <li>User specified - keeps explicitly specified number of sockets, i.e. CPS value will be
 *     changed;</li>
 *     <li>Match host - keeps current ratio between total number of virtual cores on the host and
 *     sockets available on the host.</li>
 *     </ul>
 *     </li>
 *     <li>Sockets - mode which changes sockets configured for the VM, i.e. so in case one socket
 *     has 3 cores then CPU scaling action will recommend to add 1 more socket, i.e. 3 cores. This
 *     mode also handles two options:
 *     <ul>
 *         <li>Preserve - so cores per socket value will remain the same;</li>
 *         <li>User specified - cores per socket value will be changed to explicitly specified
 *         by the user.</li>
 *     </ul></li>
 * </ul>
 * All those settings affecting on the target capacity increment value, that will be used by
 * market to generate correct recommendations.
 * Additionally, for correct action description and action execution the applicator
 * sets {@link VirtualMachineInfo.CpuScalingPolicy} instance.
 */
@ThreadSafe
class VmCpuScalingApplicator extends BaseSettingApplicator {
    private static final Logger LOGGER = LogManager.getLogger(VmCpuScalingApplicator.class);
    private static final MhzHandler MHZ_HANDLER = new MhzHandler();
    private static final Map<VCPUScalingUnitsEnum, VCPUScalingModeHandler>
                    VCPU_SCALING_MODE_TO_HANDLER =
                    ImmutableMap.of(VCPUScalingUnitsEnum.MHZ, MHZ_HANDLER,
                                    VCPUScalingUnitsEnum.CORES, new CoresHandler(),
                                    VCPUScalingUnitsEnum.SOCKETS, new SocketsHandler(),
                                    VCPUScalingUnitsEnum.VCPUS, new VcpusHandler());

    private final TopologyGraph<TopologyEntity> topologyGraph;

    VmCpuScalingApplicator(@Nonnull TopologyGraph<TopologyEntity> topologyGraph) {
        this.topologyGraph = topologyGraph;
    }

    @Override
    public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                    @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                    @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
        if (entity.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            return;
        }
        final Optional<CommoditySoldDTO.Builder> vcpuCommodityBuilderOptional =
                        entity.getCommoditySoldListBuilderList().stream().filter(commodity ->
                                        commodity.getCommodityType().getType()
                                                        == CommodityType.VCPU_VALUE).findFirst();
        vcpuCommodityBuilderOptional.ifPresent(vcpuCommodityBuilder -> {
            final VCPUScalingUnitsEnum vcpuScalingUnitsType =
                            getSettingValue(entitySettings, EntitySettingSpecs.VcpuScalingUnits,
                                            VCPUScalingUnitsEnum.MHZ);
            final float vcpuIncrement =
                            VCPU_SCALING_MODE_TO_HANDLER.getOrDefault(vcpuScalingUnitsType,
                                                            MHZ_HANDLER)
                                            .getVcpuIncrement(entity, topologyGraph, entitySettings,
                                                            vcpuCommodityBuilder);
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("CSR mode for {} is {}, resulting capacity increment={}",
                                asString(entity), vcpuScalingUnitsType, vcpuIncrement);
            }
            vcpuCommodityBuilder.setCapacityIncrement(vcpuIncrement);
        });
    }

    private static String asString(@Nonnull TopologyEntityDTO.Builder entity) {
        return String.format("%s[%s]", entity.getDisplayName(), entity.getOid());
    }

    @Nullable
    private static Integer getVmInfoParameter(@Nonnull TopologyEntityDTO.Builder entity,
                    @Nonnull Predicate<VirtualMachineInfo> valueChecker,
                    @Nonnull Function<VirtualMachineInfo, Integer> valueGetter) {
        if (!entity.hasTypeSpecificInfo()) {
            return null;
        }
        final TopologyDTO.TypeSpecificInfo typeSpecificInfo = entity.getTypeSpecificInfo();
        if (!typeSpecificInfo.hasVirtualMachine()) {
            return null;
        }
        final VirtualMachineInfo vmInfo = typeSpecificInfo.getVirtualMachine();
        if (valueChecker.test(vmInfo)) {
            return valueGetter.apply(vmInfo);
        }
        return null;
    }

    /**
     * Interface for handling VcpuScalingUnit modes. Each mode has a different way to process vcpu
     * increments.
     */
    private interface VCPUScalingModeHandler {
        float getVcpuIncrement(@Nonnull TopologyEntityDTO.Builder entity,
                        @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                        @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                        @Nonnull CommoditySoldDTO.Builder vcpuCommodityBuilder);
    }


    /**
     * Implementation of handling MHZ case. In this setting, it will only get the increment mhz and
     * scale using that.
     */
    private static class MhzHandler implements VCPUScalingModeHandler {
        @Override
        public float getVcpuIncrement(@Nonnull TopologyEntityDTO.Builder entity,
                        @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                        @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                        @Nonnull CommoditySoldDTO.Builder vcpuCommodityBuilder) {
            return (int)getNumericSetting(entitySettings, EntitySettingSpecs.VmVcpuIncrement,
                            LOGGER);
        }
    }


    /**
     * Implementation of CORES case. This setting has multiple subsettings which will behave
     * differently. Based on PRESERVE, MATCH HOST, or USER SPECIFIED subsetting, vcpu increment will
     * differ
     */
    private static class CoresHandler extends MhzHandler {
        private static final Map<VcpuScalingCoresPerSocketSocketModeEnum, CoresModeHandler>
                        MODE_TO_HANDLER =
                        ImmutableMap.of(VcpuScalingCoresPerSocketSocketModeEnum.PRESERVE,
                                        new HandlePreserve(),
                                        VcpuScalingCoresPerSocketSocketModeEnum.MATCH_HOST,
                                        new HandleMatchHost(),
                                        VcpuScalingCoresPerSocketSocketModeEnum.USER_SPECIFIED,
                                        (entity, topologyGraph, entitySettings, numCpus, cpsr) -> (int)getNumericSetting(
                                                        entitySettings,
                                                        EntitySettingSpecs.VcpuScaling_CoresPerSocket_SocketValue,
                                                        LOGGER));

        @Override
        public float getVcpuIncrement(@Nonnull TopologyEntityDTO.Builder entity,
                        @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                        @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                        @Nonnull CommoditySoldDTO.Builder vcpuCommodityBuilder) {
            final Integer numCpus = getVmInfoParameter(entity, VirtualMachineInfo::hasNumCpus,
                            VirtualMachineInfo::getNumCpus);
            final Integer cpsr =
                            getVmInfoParameter(entity, VirtualMachineInfo::hasCoresPerSocketRatio,
                                            VirtualMachineInfo::getCoresPerSocketRatio);

            final VcpuScalingCoresPerSocketSocketModeEnum coresPerSocketModeType =
                            getSettingValue(entitySettings,
                                            EntitySettingSpecs.VcpuScaling_CoresPerSocket_SocketMode,
                                            VcpuScalingCoresPerSocketSocketModeEnum.PRESERVE);

            final CoresModeHandler modeHandler = MODE_TO_HANDLER.get(coresPerSocketModeType);
            final int sockets;
            if (modeHandler == null) {
                sockets = 1;
            } else {
                sockets = modeHandler.getIncrementSockets(entity, topologyGraph, entitySettings,
                                numCpus, cpsr);
                entity.getTypeSpecificInfoBuilder().getVirtualMachineBuilder()
                                .getCpuScalingPolicyBuilder().setSockets(sockets);
            }
            if (numCpus != null && numCpus > 0) {
                return Math.round(vcpuCommodityBuilder.getCapacity() / numCpus) * sockets;
            } else {
                return super.getVcpuIncrement(entity, topologyGraph, entitySettings,
                                vcpuCommodityBuilder);
            }
        }

    }

    /**
     * Implementation of VCPUS case. This case will take a user specified value
     * and scale in the specified increments in sockets.
     * VC probe should execute the action, change the vcpu capacity by changing the sockets based on
     * the socket increment and set the cores per socket to 1.
     */
    private static class VcpusHandler extends MhzHandler {
        @Override
        public float getVcpuIncrement(@Nonnull TopologyEntityDTO.Builder entity,
                @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                @Nonnull CommoditySoldDTO.Builder vcpuCommodityBuilder) {
            final Integer numCpus = getVmInfoParameter(entity, VirtualMachineInfo::hasNumCpus,
                    VirtualMachineInfo::getNumCpus);

            final int vcpuIncrementSize = (int)getNumericSetting(entitySettings,
                    EntitySettingSpecs.VcpuScaling_Vcpus_VcpusIncrementValue, LOGGER);

            entity.getTypeSpecificInfoBuilder().getVirtualMachineBuilder()
                    .getCpuScalingPolicyBuilder().setCoresPerSocket(1);

            if (numCpus != null && numCpus > 0) {
                return Math.round(vcpuCommodityBuilder.getCapacity() / numCpus) * vcpuIncrementSize;
            } else {
                return super.getVcpuIncrement(entity, topologyGraph, entitySettings,
                        vcpuCommodityBuilder);
            }
        }
    }


    /**
     * Interface for handling sub-settings for CORES mode Each sub-setting has a different way of
     * calculating increment sockets.
     */
    @FunctionalInterface
    private interface CoresModeHandler {
        int getIncrementSockets(@Nonnull TopologyEntityDTO.Builder entity,
                        @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                        @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                        @Nullable Integer numCpus, @Nullable Integer cpsr);
    }


    /**
     * Implementation of PRESERVE option This option will keep the current VM socket value and
     * increment using that.
     */
    private static class HandlePreserve implements CoresModeHandler {
        @Override
        public int getIncrementSockets(@Nonnull TopologyEntityDTO.Builder entity,
                        @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                        @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                        @Nullable Integer numCpus, @Nullable Integer cpsr) {
            if (numCpus != null && cpsr != null && cpsr > 0) {
                return numCpus / cpsr;
            } else {
                LOGGER.warn("Increment sockets value cannot be computed for '{}' with '{}' CPU number and '{}' cores per socket",
                                asString(entity), numCpus, cpsr);
                return 1;
            }
        }
    }


    /**
     * Implementation of MATCH HOST option. This option will retrieve the number of sockets of the
     * physical machine. Incrementing will be done using the new socket count.
     */
    private static class HandleMatchHost implements CoresModeHandler {
        @Override
        public int getIncrementSockets(@Nonnull TopologyEntityDTO.Builder entity,
                        @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                        @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                        @Nullable Integer numCpus, @Nullable Integer cpsr) {
            final CommoditiesBoughtFromProvider commdityBoughtFromPM =
                            entity.getCommoditiesBoughtFromProvidersList().stream()
                                            .filter(e -> e.getProviderEntityType()
                                                            == EntityType.PHYSICAL_MACHINE_VALUE)
                                            .findFirst().orElse(null);
            if (commdityBoughtFromPM == null) {
                LOGGER.warn("There is no commodity bought from provider for physical machine entity type in the virtual machine {}",
                                entity.getDisplayName());
                return 1;
            }
            final TopologyEntity pm = topologyGraph.getEntity(commdityBoughtFromPM.getProviderId())
                            .orElse(null);
            if (pm == null) {
                LOGGER.warn("There is no physical machine in the Topology that matches the given oid {}",
                                commdityBoughtFromPM.getProviderId());
                return 1;
            }
            final TopologyEntityDTO.Builder pmEntity = pm.getTopologyEntityDtoBuilder();
            if (!pmEntity.hasTypeSpecificInfo()) {
                LOGGER.warn("There is no TypeSpecificInfo for the given for PM {}",
                                pm.getDisplayName());
                return 1;
            }
            final TopologyDTO.TypeSpecificInfo typeSpecificInfo = pmEntity.getTypeSpecificInfo();
            if (!typeSpecificInfo.hasPhysicalMachine()) {
                LOGGER.warn("There is no PhysicalMachineInfo for the given for PM {}",
                                pm.getDisplayName());
                return 1;
            }
            final PhysicalMachineInfo pmInfo = typeSpecificInfo.getPhysicalMachine();
            if (pmInfo.hasNumCpuSockets()) {
                return pmInfo.getNumCpuSockets();
            }
            LOGGER.warn("There is no NumCpuSockets for the given PM {}", pm.getDisplayName());
            return 1;
        }
    }


    /**
     * Implementation of SOCKETS case. Handles both modes:
     * <ul>
     *     <li>Preserve - takes cores per socket value from the field of the VM and tries to keep
     *     it unchanged;</li>
     *     <li>User specified - takes cores per socket value from the user and change it if
     *     necessary.</li>
     * </ul>
     * Calculates total VCPU increment that will be used by the market to generate the actions.
     * Total VCPU increment calculated as coreSpeed * cores per socket * sockets increment.
     * Additionally, populates {@link VirtualMachineInfo.CpuScalingPolicy} in case cores per socket
     * value has to be changed or sockets value has to be changed.
     */
    private static class SocketsHandler extends MhzHandler {
        @Override
        public float getVcpuIncrement(@Nonnull TopologyEntityDTO.Builder entity,
                        @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                        @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                        @Nonnull CommoditySoldDTO.Builder vcpuCommodityBuilder) {
            final Integer coresPerSocket = getCoresPerSocket(entity, entitySettings);
            final float socketsIncrement = getSettingValue(entitySettings,
                            EntitySettingSpecs.VcpuScaling_Sockets_SocketIncrementValue, 1F);
            final Integer numCpus = getVmInfoParameter(entity, VirtualMachineInfo::hasNumCpus,
                            VirtualMachineInfo::getNumCpus);
            if (coresPerSocket != null && numCpus != null && numCpus > 0) {
                final long coreSpeed = Math.round(vcpuCommodityBuilder.getCapacity() / numCpus);
                return coreSpeed * coresPerSocket * socketsIncrement;
            } else if (vcpuCommodityBuilder.hasCapacityIncrement()) {
                /*
                  All probes are sending capacity increment as a socket speed, i.e.
                  core speed * cores per socket. We have to additionally multiply it by sockets
                  increment in case it specified.
                 */
                return vcpuCommodityBuilder.getCapacityIncrement() * socketsIncrement;
            }
            return super.getVcpuIncrement(entity, topologyGraph, entitySettings,
                            vcpuCommodityBuilder);
        }

        @Nullable
        private Integer getCoresPerSocket(TopologyEntityDTO.Builder entity,
                        Map<EntitySettingSpecs, Setting> entitySettings) {
            final VcpuScalingSocketsCoresPerSocketModeEnum mode = getSettingValue(entitySettings,
                            EntitySettingSpecs.VcpuScaling_Sockets_CoresPerSocketMode,
                            VcpuScalingSocketsCoresPerSocketModeEnum.USER_SPECIFIED);
            if (mode == VcpuScalingSocketsCoresPerSocketModeEnum.USER_SPECIFIED) {
                final int result = (int)getNumericSetting(entitySettings,
                                EntitySettingSpecs.VcpuScaling_Sockets_CoresPerSocketValue, LOGGER);
                entity.getTypeSpecificInfoBuilder().getVirtualMachineBuilder()
                                .getCpuScalingPolicyBuilder().setCoresPerSocket(result);
                return result;
            }
            return getVmInfoParameter(entity, VirtualMachineInfo::hasCoresPerSocketRatio,
                            VirtualMachineInfo::getCoresPerSocketRatio);
        }

    }

}
