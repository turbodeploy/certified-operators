package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.TopologyDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTOREST.ActionMode;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProviderOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline;

/**
 * The {@link EntitySettingsApplicator} is responsible for applying resolved settings
 * to a {@link TopologyGraph}.
 *
 * It's separated from {@link EntitySettingsResolver} (which resolves settings) for clarity, and ease
 * of tracking, debugging, and measuring. It's separated from {@link GraphWithSettings} (even
 * though it works on the graph and settings) so that the {@link GraphWithSettings} can be
 * a simple data object passed between stages in the {@link TopologyPipeline}.
 *
 * As the number of settings in the system goes up, we may need to rework this class to be
 * more efficient.
 */
public class EntitySettingsApplicator {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Applies the settings contained in a {@link GraphWithSettings} to the topology graph
     * contained in it.
     *
     * @param graphWithSettings A {@link TopologyGraph} and the settings that apply to it.
     */
    public void applySettings(@Nonnull final TopologyInfo topologyInfo,
                              @Nonnull final GraphWithSettings graphWithSettings) {
        graphWithSettings.getTopologyGraph().entities()
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .forEach(entity -> {
                final Map<EntitySettingSpecs, Setting> settingsForEntity =
                        new EnumMap<>(EntitySettingSpecs.class);
                for (final Setting setting : graphWithSettings.getSettingsForEntity(
                        entity.getOid())) {
                    final Optional<EntitySettingSpecs> policySetting =
                            EntitySettingSpecs.getSettingByName(setting.getSettingSpecName());
                    if (policySetting.isPresent()) {
                        settingsForEntity.put(policySetting.get(), setting);
                    } else {
                        logger.warn("Unknown setting {} for entity {}",
                                setting.getSettingSpecName(), entity.getOid());
                        return;
                    }
                }

                for (SettingApplicator applicator: buildApplicators(topologyInfo)) {
                    applicator.apply(entity, settingsForEntity);
                }
            });
    }

    /**
     * Get the list of applicators for a particular {@link TopologyInfo}.
     *
     * @param topologyInfo The {@link TopologyInfo} of an in-progress topology broadcast.
     *
     * @return A list of {@link SettingApplicator}s for settings that apply to this topology.
     */
    private static List<SettingApplicator> buildApplicators(@Nonnull final TopologyInfo topologyInfo) {
        return ImmutableList.of(new MoveApplicator(),
                new VMShopTogetherApplicator(topologyInfo),
                new SuspendApplicator(),
                new ProvisionApplicator(),
                new ResizeApplicator(),
                new StorageMoveApplicator(),
                new UtilizationThresholdApplicator(EntitySettingSpecs.IoThroughput,
                        CommodityType.IO_THROUGHPUT),
                new UtilizationThresholdApplicator(EntitySettingSpecs.NetThroughput,
                        CommodityType.NET_THROUGHPUT),
                new UtilizationThresholdApplicator(EntitySettingSpecs.SwappingUtilization,
                        CommodityType.SWAPPING),
                new UtilizationThresholdApplicator(EntitySettingSpecs.ReadyQueueUtilization,
                        CommodityType.QN_VCPU),
                new UtilizationThresholdApplicator(EntitySettingSpecs.StorageAmountUtilization,
                        CommodityType.STORAGE_AMOUNT),
                new UtilizationThresholdApplicator(EntitySettingSpecs.IopsUtilization,
                        CommodityType.STORAGE_ACCESS),
                new UtilizationThresholdApplicator(EntitySettingSpecs.LatencyUtilization,
                        CommodityType.STORAGE_LATENCY),
                new UtilizationThresholdApplicator(EntitySettingSpecs.HeapUtilization,
                        CommodityType.HEAP),
                new UtilizationThresholdApplicator(EntitySettingSpecs.CollectionTimeUtilization,
                        CommodityType.COLLECTION_TIME),
                new UtilTargetApplicator(),
                new TargetBandApplicator(),
                new HaDependentUtilizationApplicator(topologyInfo),
                new ResizeIncrementApplicator(EntitySettingSpecs.VcpuIncrement,
                        CommodityType.VCPU),
                new ResizeIncrementApplicator(EntitySettingSpecs.VmemIncrement,
                        CommodityType.VMEM),
                new ResizeIncrementApplicator(EntitySettingSpecs.VstorageIncrement,
                        CommodityType.VSTORAGE),
                new ResizeTargetUtilizationApplicator(EntitySettingSpecs.ResizeTargetUtilizationVcpu,
                        CommodityType.VCPU),
                new ResizeTargetUtilizationApplicator(EntitySettingSpecs.ResizeTargetUtilizationVmem,
                        CommodityType.VMEM));
    }

    private static Collection<CommoditySoldDTO.Builder> getCommoditySoldBuilders(
            TopologyEntityDTO.Builder entity, CommodityType commodityType) {
        return entity.getCommoditySoldListBuilderList()
                .stream()
                .filter(commodity -> commodity.getCommodityType().getType() ==
                        commodityType.getNumber())
                .collect(Collectors.toList());
    }

    /**
     * The applicator of a single {@link Setting} to a single {@link TopologyEntityDTO.Builder}.
     */
    private static abstract class SingleSettingApplicator implements SettingApplicator {

        private final EntitySettingSpecs setting;

        private SingleSettingApplicator(@Nonnull EntitySettingSpecs setting) {
            this.setting = Objects.requireNonNull(setting);
        }

        protected abstract void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final Setting setting);

        @Override
        public void apply(@Nonnull Builder entity,
                @Nonnull Map<EntitySettingSpecs, Setting> settings) {
            final Setting settingObject = settings.get(setting);
            if (settingObject != null) {
                apply(entity, settingObject);
            }
        }
    }

    /**
     * Settings applicator, that requires multiple settings to be processed.
     */
    private interface SettingApplicator {
        /**
         * Applies settings to the specified entity.
         *
         * @param entity entity to apply settings to
         * @param settings settings to apply
         */
        void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final Map<EntitySettingSpecs, Setting> settings);
    }

    /**
     * Applies the "move" setting to a {@link TopologyEntityDTO.Builder}. In particular,
     * if it is VM and the "move" is disabled, set the commodities purchased from a host to non-movable.
     * If it is storage and the "move" is disabled, set the all commodities bought to non-movable.
     */
    private static class MoveApplicator extends SingleSettingApplicator {

        private MoveApplicator() {
            super(EntitySettingSpecs.Move);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            final boolean movable =
                    !setting.getEnumSettingValue().getValue().equals(ActionMode.DISABLED.name());

            entity.getCommoditiesBoughtFromProvidersBuilderList().stream()
                // Only disable moves for placed entities (i.e. those that have providers).
                // Doesn't make sense to disable them for unplaced ones.
                .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderId)
                .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderEntityType)
                // The "move" setting controls vm moves between hosts and storage moves between its
                // providers(disk array, logical pool). We want to set the VM group of commodities
                // bought from hosts (physical machines) to non-movable and Storage group of
                // commodities bought from its providers to non-movable.
                .filter(commBought -> shouldOverrideMovable(commBought, entity.getEntityType()))
                .forEach(commBought -> commBought.setMovable(movable));
        }
    }

    /**
     * Set shop together on a virtual machine{@link TopologyEntityDTO.Builder} based on the "move"
     * and "storage move" settings. In particular, if the "move" and "storage move"
     * action settings are both in Manual, or both in Automatic state, shop together can be enabled.
     * Otherwise, set the shop together to false because user has to explicitly change the settings
     * to turn on bundled moves on compute and storage resources.
     */
    private static class VMShopTogetherApplicator implements SettingApplicator {
        // a flag to indicate if the shop together should be set to false based on action settings
        private final boolean disableShopTogether;

        public VMShopTogetherApplicator(TopologyInfo topologyInfo) {
            super();
            // In case of initial placement, the template VM shop together should always be true
             // regardless of action settings.
            disableShopTogether = topologyInfo.hasPlanInfo() && topologyInfo.getPlanInfo()
                    .getPlanProjectType().equals(PlanProjectType.INITAL_PLACEMENT);
        }

        @Override
        public void apply(Builder entity, Map<EntitySettingSpecs, Setting> settings) {
            if (!disableShopTogether && entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                    && settings.containsKey(EntitySettingSpecs.Move)
                    && settings.containsKey(EntitySettingSpecs.StorageMove)) {
                // TODO: For migration plans from on prem to cloud or from cloud to cloud,
                // we should shop together.
                final String computeMoveSetting = settings.get(EntitySettingSpecs.Move)
                        .getEnumSettingValue().getValue();
                final String storageMoveSetting = settings.get(EntitySettingSpecs.StorageMove)
                        .getEnumSettingValue().getValue();
                boolean isAutomateOrManual = computeMoveSetting.equals(ActionMode.AUTOMATIC.name())
                                || computeMoveSetting.equals(ActionMode.MANUAL.name());
                // Note: if a VM does not support shop together execution, even when move and storage
                // move sets to Manual or Automatic, dont enable shop together.
                if (!computeMoveSetting.equals(storageMoveSetting) || !isAutomateOrManual
                        || entity.getEnvironmentType() == EnvironmentType.CLOUD) {
                    // user has to change default VM action settings to explicitly indicate they
                    // want to have compound move actions generated considering best placements in
                    // terms of both storage and compute resources. Usually user ask for shop together
                    // moves so as to take the actions to move VM across networks, thus Manual and
                    // Automatic has to be chosen to execute the actions.
                    // We make Cloud VMs shop together false. This is done because shop
                    // together generates compound moves but we need to show separate actions in the
                    // user interface. So, we just disable shop together for them because it has
                    // no real advantage in the cloud.
                    entity.getAnalysisSettingsBuilder().setShopTogether(false);
                    logger.debug("Shoptogether is disabled for {} with move mode {} and storage move mode {}.",
                            entity.getDisplayName(), computeMoveSetting, storageMoveSetting);
                }
            } else if (entity.getEntityType() == EntityType.DATABASE_SERVER_VALUE ||
                    entity.getEntityType() == EntityType.DATABASE_VALUE) {
                // database entities should not perform shop-together
                entity.getAnalysisSettingsBuilder().setShopTogether(false);
            }
        }
    }

    /**
     * For move setting, it supports both VM and Storage entities. For VM entity, it only controls
     * moves between hosts, because moves between storage is controlled by storage setting.
     *
     * @param commoditiesBought {@link CommoditiesBoughtFromProvider} of the entity.
     * @param entityType entity type.
     * @return a boolean, true means should override movable for this commodity bought, false
     *         should not override movable for this commodity bought.
     */
    private static boolean shouldOverrideMovable(
            @Nonnull final CommoditiesBoughtFromProvider.Builder commoditiesBought,
            final int entityType) {
        if (EntitySettingSpecs.Move.getEntityTypeScope().contains(EntityType.forNumber(entityType))) {
            if (entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
                // if it is a VM entity, only override movable for hosts providers. Because Storage move
                // is controlled by StorageMoveApplicator.
                return commoditiesBought.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE;
            }
            return true;
        } else {
            logger.error("Unknown entity type scope {} for Move setting.", entityType);
            return false;
        }
    }

    /**
     * Applies the "storage move" setting to a {@link TopologyEntityDTO.Builder}. In particular,
     * if the "move" is disabled, set the commodities purchased from a storage to non-movable.
     */
    static class StorageMoveApplicator extends SingleSettingApplicator {

        private StorageMoveApplicator() {
            super(EntitySettingSpecs.StorageMove);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            final boolean movable = !setting.getEnumSettingValue().getValue().equals("DISABLED");
            entity.getCommoditiesBoughtFromProvidersBuilderList().stream()
                    .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderId)
                    .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderEntityType)
                    .filter(commBought -> TopologyDTOUtil.isStorageEntityType(
                            commBought.getProviderEntityType()))
                    .forEach(commBought -> commBought.setMovable(movable));
        }
    }

    /**
     * Applies the "suspend" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class SuspendApplicator extends SingleSettingApplicator {

        private SuspendApplicator() {
            super(EntitySettingSpecs.Suspend);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            // when setting value is DISABLED, set suspendable to false,
            // otherwise keep the original value which could be set during converting SDK entityDTO.
            if (setting.getEnumSettingValue().getValue().equals("DISABLED")) {
                entity.getAnalysisSettingsBuilder().setSuspendable(false);
            }
        }
    }

    /**
     * Applies the "provision" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ProvisionApplicator extends SingleSettingApplicator {

        private ProvisionApplicator() {
            super(EntitySettingSpecs.Provision);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            entity.getAnalysisSettingsBuilder().setCloneable(
                    !setting.getEnumSettingValue().getValue().equals("DISABLED"));
        }
    }

    /**
     * Applies the "resize" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ResizeApplicator extends SingleSettingApplicator {

        private ResizeApplicator() {
            super(EntitySettingSpecs.Resize);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            final boolean resizeable =
                    !setting.getEnumSettingValue().getValue().equals(ActionMode.DISABLED.name());
            entity.getCommoditySoldListBuilderList()
                    .forEach(commSoldBuilder -> {
                        /* We shouldn't change isResizable if it comes as false from a probe side.
                           For example static memory VMs comes from Hyper-V targets with resizeable=false
                           for VMEM(53) commodities.
                         */
                        if (commSoldBuilder.getIsResizeable()) {
                            commSoldBuilder.setIsResizeable(resizeable);
                        }
                    });
        }
    }

    /**
     * Applicator for utilization threshold settings. Utilization thresholds work by setting
     * effective capacity percentage, so Market reads this value to create effective capacity as
     * {@code effectiveCapacity = capacity * effectiveCapacityPercentage / 100}
     * The effective capacity is later used to calculate resources price.
     */
    @ThreadSafe
    private static class UtilizationThresholdApplicator extends SingleSettingApplicator {

        private final CommodityType commodityType;

        private UtilizationThresholdApplicator(@Nonnull EntitySettingSpecs setting,
                @Nonnull final CommodityType commodityType) {
            super(setting);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull Builder entity, @Nonnull Setting setting) {
            final float settingValue = setting.getNumericSettingValue().getValue();
            for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity, commodityType)) {
                commodity.setEffectiveCapacityPercentage(settingValue);
            }
        }
    }

    /**
     * HA related commodities applicator. This applicator will process ignoreHA setting and cpu/mem
     * utilization threshold. Both of the commodities are calculated on top of appropriate settings
     * and HA ignorance configuration.
     */
    @ThreadSafe
    private static class HaDependentUtilizationApplicator implements SettingApplicator {

        private final TopologyInfo topologyInfo;

        private HaDependentUtilizationApplicator(@Nonnull final TopologyInfo topologyInfo) {
            this.topologyInfo = Objects.requireNonNull(topologyInfo);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final Map<EntitySettingSpecs, Setting> settings) {
            if (topologyInfo.getPlanInfo().getPlanProjectType() == PlanProjectType.CLUSTER_HEADROOM) {
                // For cluster headroom calculation, we ignore the normal settings that affect
                // effective capacity.
                final Setting targetBand = settings.get(EntitySettingSpecs.TargetBand);
                final Setting utilTarget = settings.get(EntitySettingSpecs.UtilTarget);
                if (targetBand != null && utilTarget != null) {
                    // Calculate maximum desired utilization for the desired state of this entity.
                    // Limit the range to [0.0-100.0]
                    final float maxDesiredUtilization =
                        Math.min(100f,
                            Math.max(utilTarget.getNumericSettingValue().getValue() +
                                targetBand.getNumericSettingValue().getValue() / 2.0f,
                                0f));
                    applyMaxUtilizationToCapacity(entity, CommodityType.CPU, maxDesiredUtilization);
                    applyMaxUtilizationToCapacity(entity, CommodityType.MEM, maxDesiredUtilization);
                }
            } else {
                final Setting ignoreHaSetting = settings.get(EntitySettingSpecs.IgnoreHA);
                final boolean ignoreHa =
                        ignoreHaSetting != null && ignoreHaSetting.getBooleanSettingValue().getValue();
                final Setting cpuUtilSetting = settings.get(EntitySettingSpecs.CpuUtilization);
                final Setting memUtilSetting = settings.get(EntitySettingSpecs.MemoryUtilization);
                applyUtilizationChanges(entity, CommodityType.CPU, cpuUtilSetting, ignoreHa);
                applyUtilizationChanges(entity, CommodityType.MEM, memUtilSetting, ignoreHa);
            }
        }

        private void applyMaxUtilizationToCapacity(@Nonnull final TopologyEntityDTO.Builder entity,
                                                   @Nonnull final CommodityType commodityType,
                                                   final float maxDesiredUtilization) {
            // We only want to do this for cluster headroom calculations.
            Preconditions.checkArgument(topologyInfo.getPlanInfo().getPlanProjectType() ==
                    PlanProjectType.CLUSTER_HEADROOM);
            for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity, commodityType)) {
                // We want to factor the max desired utilization into the effective capacity
                // of the sold commodity. For cluster headroom calculations, the desired state has
                // no effect because provisions/moves are disabled in the market analysis. However,
                // we want the headroom numbers to reflect the desired state, so we take the
                // desired state into account when calculating the effective capacity of commodities.
                //
                // For example, suppose we have a host with 10GB of memory, HA is 80%, and max desired
                // utilization is 75%. Suppose each VM requires 1GB of memory, and there are currently
                // no VMs on the host. If we set effective capacity to the level of HA, we'd say
                // headroom is 8 VMs (80% of 10GB). However, if the customer actually adds 8 VMs the
                // market would recommend moving some of them off the host because of the desired
                // state setting. The "real" headroom is 80% * 75% = 60% -> 6 VMs.
                final double newCapacity = (commodity.getEffectiveCapacityPercentage() * maxDesiredUtilization) / 100.0f;
                commodity.setEffectiveCapacityPercentage(newCapacity);
            }
        }

        private void applyUtilizationChanges(@Nonnull TopologyEntityDTO.Builder entity,
                                             @Nonnull CommodityType commodityType,
                                             @Nullable Setting setting,
                                             boolean ignoreHa) {
            for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity, commodityType)) {
                if (setting != null) {
                    commodity.setEffectiveCapacityPercentage(
                            setting.getNumericSettingValue().getValue());
                } else if (ignoreHa) {
                    // The ignore HA setting does NOT override explicitly set capacity percentage,
                    // but it does override the values sent over from the probe.
                    // Unfortunately, at the time of this writing the probe rolls capacity and HA
                    // into one property, so there's no way to only ignore HA without ignoring
                    // other things that may have affected capacity percentage.
                    commodity.clearEffectiveCapacityPercentage();
                }
            }
        }
    }

    /**
     *  Applies the "utilTarget" setting to a PM entity.
     */
    private static class UtilTargetApplicator extends SingleSettingApplicator {

        private UtilTargetApplicator() {
            super(EntitySettingSpecs.UtilTarget);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {

            if (entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                entity.getAnalysisSettingsBuilder()
                    .setDesiredUtilizationTarget(setting.getNumericSettingValue().getValue());
            }
        }
    }

    /**
     *  Applies the "targetBand" setting to a PM entity.
     */
    private static class TargetBandApplicator extends SingleSettingApplicator {

        private TargetBandApplicator() {
            super(EntitySettingSpecs.TargetBand);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {

            if (entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                entity.getAnalysisSettingsBuilder()
                    .setDesiredUtilizationRange(setting.getNumericSettingValue().getValue());
            }
        }
    }

    /**
     * Applicator for capacity resize increment settings.
     * This sets the capacity_increment in the {@link CommoditySoldDTO}
     * for Virtual Machine entities.
     *
     */
    @ThreadSafe
    private static class ResizeIncrementApplicator extends SingleSettingApplicator {

        private final CommodityType commodityType;

        // convert into units that market uses.
        private final ImmutableMap<Integer, Float> conversionFactor =
            ImmutableMap.of(
                //VMEM setting value is in MBs. Market expects it in KBs.
                CommodityType.VMEM_VALUE, 1024.0f,
                //VSTORAGE setting value is in GBs. Market expects it in MBs.
                CommodityType.VSTORAGE_VALUE, 1024.0f);


        private ResizeIncrementApplicator(@Nonnull EntitySettingSpecs setting,
                @Nonnull final CommodityType commodityType) {
            super(setting);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull Builder entity, @Nonnull Setting setting) {
            if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                final float settingValue = setting.getNumericSettingValue().getValue();
                entity.getCommoditySoldListBuilderList().stream()
                    .filter(commodity -> commodity.getCommodityType().getType() ==
                            commodityType.getNumber())
                    .forEach(commodityBuilder -> {
                        final float userRequestedIncrement =
                            settingValue * conversionFactor.getOrDefault(
                                commodityBuilder.getCommodityType().getType(), 1.0f);
                        final float probeProvidedIncrement = commodityBuilder.hasCapacityIncrement() ?
                            commodityBuilder.getCapacityIncrement() : 1.0f;
                        // We round the user-requested increment to the probe-provided increment
                        // here, so that the increment is set properly in the outgoing topology.
                        //
                        // There is the small possibility that the resize increment that comes
                        // out of stitching is retrieved from a different probe than the one
                        // that will be chosen to execute the action. Realistically, that
                        // shouldn't happen for resize actions.
                        commodityBuilder.setCapacityIncrement(
                            ResizeIncrementAdjustor.roundToProbeIncrement(
                                    userRequestedIncrement, probeProvidedIncrement));

                        // Value out of the box is set to large number for vStorage commodity and it is done so that
                        // we don't resize. It still leaves some scope of resize in case provider is large enough.
                        // To prevent that set resizeable false if vStorage increment is set to default value.
                        if (commodityType.getNumber() == CommodityType.VSTORAGE_VALUE
                                && setting.getNumericSettingValue().getValue() == getDefualtVStorageIncrement()) {
                            commodityBuilder.setIsResizeable(false);
                        }

                        logger.debug("Apply Resize Increment for commodity: {} , value={}",
                            commodityType.getNumber(), commodityBuilder.getCapacityIncrement());
                    });
            }
        }

        private float getDefualtVStorageIncrement() {
            return EntitySettingSpecs.VstorageIncrement
                    .getSettingSpec()
                    .getNumericSettingValueType()
                    .getDefault();
        }
    }

    /**
     * Applicator for the resize target utilization settings.
     * This sets the resize_target_utilization in the {@link CommoditySoldDTO}
     * for Virtual Machine entities.
     */
    @ThreadSafe
    private static class ResizeTargetUtilizationApplicator extends SingleSettingApplicator {

        private final CommodityType commodityType;

        private ResizeTargetUtilizationApplicator(@Nonnull EntitySettingSpecs setting,
                                          @Nonnull final CommodityType commodityType) {
            super(setting);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull Builder entity, @Nonnull Setting setting) {
            if (ImmutableSet.of(EntityType.VIRTUAL_MACHINE_VALUE, EntityType.DATABASE_VALUE,
                    EntityType.DATABASE_SERVER_VALUE, EntityType.CONTAINER_VALUE).contains(entity.getEntityType())) {
                final float settingValue = setting.getNumericSettingValue().getValue();
                entity.getCommoditySoldListBuilderList().stream()
                        .filter(commodity -> commodity.getCommodityType().getType() ==
                                commodityType.getNumber())
                        .forEach(commodityBuilder -> {
                            // Divide by 100 since the RTU value set by the user in the UI is a
                            // percentage value
                            commodityBuilder.setResizeTargetUtilization(settingValue / 100);
                            logger.debug("Apply Resize Target utilization for entity = {}, " +
                                            "commodity = {} , value = {}", entity.getDisplayName(),
                                    commodityType.getNumber(), commodityBuilder.getCapacityIncrement());
                        });
            }
        }
    }
}