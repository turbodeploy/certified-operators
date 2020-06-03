package com.vmturbo.topology.processor.group.settings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProviderOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.applicators.ComputeTierInstanceStoreCommoditiesCreator;
import com.vmturbo.topology.processor.group.settings.applicators.InstanceStoreSettingApplicator;
import com.vmturbo.topology.processor.group.settings.applicators.VmInstanceStoreCommoditiesCreator;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline;

/**
 * The {@link EntitySettingsApplicator} is responsible for applying resolved settings
 * to a {@link TopologyGraph<TopologyEntity>}.
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
     * @param topologyInfo the {@link TopologyInfo}
     * @param graphWithSettings A {@link TopologyGraph<TopologyEntity>} and the settings that apply to it.
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
                    }
                }

                for (SettingApplicator applicator: buildApplicators(topologyInfo, graphWithSettings)) {
                    applicator.apply(entity, settingsForEntity);
                }

            });
    }

    /**
     * Get the list of applicators for a particular {@link TopologyInfo}.
     *
     * @param topologyInfo The {@link TopologyInfo} of an in-progress topology broadcast.
     * @param graphWithSettings {@link GraphWithSettings} of the in-progress topology
     * @return A list of {@link SettingApplicator}s for settings that apply to this topology.
     */
    private static List<SettingApplicator> buildApplicators(@Nonnull final TopologyInfo topologyInfo,
                                                            @Nonnull final GraphWithSettings graphWithSettings) {
        return ImmutableList.of(new MoveApplicator(graphWithSettings),
                new VMShopTogetherApplicator(topologyInfo),
                new SuspendApplicator(true), new SuspendApplicator(false),
                new ProvisionApplicator(true), new ProvisionApplicator(false),
                new ResizeApplicator(),
                new ScalingApplicator(),
                new MoveCommoditiesFromProviderTypesApplicator(EntitySettingSpecs.StorageMove,
                        TopologyDTOUtil.STORAGE_TYPES),
                new MoveCommoditiesFromProviderTypesApplicator(EntitySettingSpecs.BusinessUserMove,
                        Collections.singleton(EntityType.DESKTOP_POOL)),
                new VirtualMachineResizeVcpuApplicator(),
                new VirtualMachineResizeVmemApplicator(),
                new DeleteApplicator(),
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
                new UtilizationThresholdApplicator(EntitySettingSpecs.VCPURequestUtilization,
                        CommodityType.VCPU_REQUEST),
                new UtilizationThresholdApplicator(EntitySettingSpecs.PoolCpuUtilizationThreshold,
                        CommodityType.POOL_CPU),
                new UtilizationThresholdApplicator(EntitySettingSpecs.PoolMemoryUtilizationThreshold,
                        CommodityType.POOL_MEM),
                new UtilizationThresholdApplicator(EntitySettingSpecs.PoolStorageUtilizationThreshold,
                        CommodityType.POOL_STORAGE),
                new UtilTargetApplicator(),
                new TargetBandApplicator(),
                new HaDependentUtilizationApplicator(topologyInfo),
                new ResizeIncrementApplicator(EntitySettingSpecs.VmVcpuIncrement,
                        CommodityType.VCPU),
                new ResizeIncrementApplicator(EntitySettingSpecs.VmVmemIncrement,
                        CommodityType.VMEM),
                new ResizeIncrementApplicator(EntitySettingSpecs.ContainerVcpuIncrement,
                        CommodityType.VCPU),
                new ResizeIncrementApplicator(EntitySettingSpecs.ContainerVmemIncrement,
                        CommodityType.VMEM),
                new ResizeIncrementApplicator(EntitySettingSpecs.ContainerVcpuIncrement,
                        CommodityType.VCPU_REQUEST),
                new ResizeIncrementApplicator(EntitySettingSpecs.ContainerVmemIncrement,
                        CommodityType.VMEM_REQUEST),
                new ResizeIncrementApplicator(EntitySettingSpecs.VstorageIncrement,
                        CommodityType.VSTORAGE),
                new ResizeIncrementApplicator(EntitySettingSpecs.StorageIncrement,
                        CommodityType.STORAGE_AMOUNT),
                new VMThresholdApplicator(ImmutableMap.of(
                        CommodityType.VMEM.getNumber(), Sets.newHashSet(
                                EntitySettingSpecs.ResizeVmemMinThreshold,
                                EntitySettingSpecs.ResizeVmemMaxThreshold,
                                EntitySettingSpecs.ResizeVmemUpInBetweenThresholds,
                                EntitySettingSpecs.ResizeVmemDownInBetweenThresholds,
                                EntitySettingSpecs.ResizeVmemAboveMaxThreshold,
                                EntitySettingSpecs.ResizeVmemBelowMinThreshold),
                        CommodityType.VCPU.getNumber(), Sets.newHashSet(
                                EntitySettingSpecs.ResizeVcpuMinThreshold,
                                EntitySettingSpecs.ResizeVcpuMaxThreshold,
                                EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds,
                                EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds,
                                EntitySettingSpecs.ResizeVcpuAboveMaxThreshold,
                                EntitySettingSpecs.ResizeVcpuBelowMinThreshold)),
                        graphWithSettings),
                new ResizeTargetUtilizationCommodityBoughtApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationImageCPU,
                        CommodityType.IMAGE_CPU),
                new ResizeTargetUtilizationCommodityBoughtApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationImageMem,
                        CommodityType.IMAGE_MEM),
                new ResizeTargetUtilizationCommodityBoughtApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationImageStorage,
                        CommodityType.IMAGE_STORAGE),
                new ResizeTargetUtilizationCommodityBoughtApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationIoThroughput,
                        CommodityType.IO_THROUGHPUT),
                new ResizeTargetUtilizationCommodityBoughtApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationNetThroughput,
                        CommodityType.NET_THROUGHPUT),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationVcpu, CommodityType.VCPU),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationVmem, CommodityType.VMEM),
                new InstanceStoreSettingApplicator(graphWithSettings.getTopologyGraph(),
                                        new VmInstanceStoreCommoditiesCreator(),
                                        new ComputeTierInstanceStoreCommoditiesCreator()),
                new OverrideCapacityApplicator(EntitySettingSpecs.ViewPodActiveSessionsCapacity,
                        CommodityType.ACTIVE_SESSIONS),
                new VsanStorageApplicator(graphWithSettings),
                new ResizeVStorageApplicator());
    }

    /**
     * The applicator of a single {@link Setting} to a single {@link TopologyEntityDTO.Builder}.
     */
    public abstract static class SingleSettingApplicator implements SettingApplicator {

        private final EntitySettingSpecs setting;

        protected SingleSettingApplicator(@Nonnull EntitySettingSpecs setting) {
            this.setting = Objects.requireNonNull(setting);
        }

        protected EntitySettingSpecs getEntitySettingSpecs() {
            return setting;
        }

        protected abstract void apply(@Nonnull TopologyEntityDTO.Builder entity,
                @Nonnull Setting setting);

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                @Nonnull Map<EntitySettingSpecs, Setting> settings) {
            final Setting settingObject = settings.get(setting);
            if (settingObject != null) {
                apply(entity, settingObject);
            }
        }
    }

    /**
     * The applicator of multiple {@link Setting}s to a single {@link TopologyEntityDTO.Builder}.
     */
    private abstract static class MultipleSettingsApplicator implements SettingApplicator {

        private final List<EntitySettingSpecs> settings;

        private MultipleSettingsApplicator(@Nonnull List<EntitySettingSpecs> settings) {
            this.settings = Objects.requireNonNull(settings);
        }

        protected List<EntitySettingSpecs> getEntitySettingSpecs() {
            return settings;
        }

        protected abstract void apply(@Nonnull TopologyEntityDTO.Builder entity,
                                      @Nonnull Collection<Setting> settings);

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                          @Nonnull Map<EntitySettingSpecs, Setting> settings) {
            List<Setting> settingObjects = new ArrayList();
            for (EntitySettingSpecs setting : this.settings) {
                final Setting settingObject = settings.get(setting);
                //The settings passed to the method should contain ALL the settings of the
                // applicator
                if (settingObject == null) {
                    return;
                } else {
                    settingObjects.add(settingObject);
                }
            }
            apply(entity, settingObjects);
        }

        protected void updateCommodities(TopologyEntityDTO.Builder entity,
                                         CommodityType commodityType) {
            entity.getCommoditySoldListBuilderList()
                .forEach(commSoldBuilder -> {
                    if (commSoldBuilder.getCommodityType().getType() == commodityType.getNumber() &&
                        commSoldBuilder.getIsResizeable()) {
                        commSoldBuilder.setIsResizeable(false);
                    }
                });
        }
    }

    /**
     * Abstract applicator for {@link CommoditiesBoughtFromProvider#hasMovable()}.
     */
    private abstract static class AbstractMoveApplicator extends SingleSettingApplicator {

        protected AbstractMoveApplicator(@Nonnull EntitySettingSpecs setting) {
            super(setting);
        }

        @Override
        protected void apply(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull Setting setting) {
            final boolean isMoveEnabled =
                    getEntitySettingSpecs().getValue(setting, ActionMode.class) !=
                            ActionMode.DISABLED;
            apply(entity, isMoveEnabled);
        }

        /**
         * Applies setting to the specified entity.
         *
         * @param entity the {@link TopologyEntityDTO.Builder}
         * @param isMoveEnabled {@code true} if the {@link ActionMode} setting
         * is not {@link ActionMode#DISABLED}, otherwise {@code false}
         */
        protected abstract void apply(@Nonnull TopologyEntityDTO.Builder entity,
                boolean isMoveEnabled);

        /**
         * Apply the movable flag to the commodities of the entity which satisfies the provided
         * override condition.
         *
         * @param entity to apply the setting
         * @param movable is movable or not
         * @param predicate condition function which the commodity should apply the movable or not.
         */
        protected static void applyMovableToCommodities(@Nonnull TopologyEntityDTO.Builder entity,
                boolean movable,
                @Nonnull Predicate<CommoditiesBoughtFromProvider.Builder> predicate) {
            entity.getCommoditiesBoughtFromProvidersBuilderList()
                    .stream()
                    .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderId)
                    .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderEntityType)
                    .filter(predicate)
                    .forEach(c -> {
                        // Apply setting value only if move is not disabled by the entity
                        if (!c.hasMovable() || (c.hasMovable() && c.getMovable())) {
                            c.setMovable(movable);
                        } else {
                            // Do not override with the setting if move has been disabled at entity level
                            logger.trace("{}:{} Not overriding move setting, move is disabled at entity level {}",
                                    entity::getEntityType, entity::getDisplayName,
                                    c::getMovable);
                        }
                    });
        }
    }

    /**
     * Applicator for {@link CommoditiesBoughtFromProvider#hasMovable()}
     * for {@link CommoditiesBoughtFromProvider#getProviderEntityType()}
     * with a specific {@link EntityType}.
     */
    private static class MoveCommoditiesFromProviderTypesApplicator extends AbstractMoveApplicator {

        private Set<EntityType> providerTypes;

        private MoveCommoditiesFromProviderTypesApplicator(@Nonnull EntitySettingSpecs setting,
                @Nonnull Set<EntityType> providerTypes) {
            super(setting);
            this.providerTypes = providerTypes;
        }

        @Override
        protected void apply(@Nonnull TopologyEntityDTO.Builder entity, boolean isMoveEnabled) {
            applyMovableToCommodities(entity, isMoveEnabled,
                    c -> providerTypes.contains(EntityType.forNumber(c.getProviderEntityType())));
        }
    }

    /**
     * Applies the "move" setting to a {@link TopologyEntityDTO.Builder}. In particular, if it is
     * virtual machine and the "move" is disabled, set the commodities purchased from a host to
     * non-movable. If it is storage and the "move" is disabled, set the all commodities bought to
     * non-movable.
     */
    private static class MoveApplicator extends AbstractMoveApplicator {

        private final GraphWithSettings graphWithSettings;
        private final Map<EntityType, BiConsumer<TopologyEntityDTO.Builder, Boolean>> specialCases;

        private MoveApplicator(@Nonnull final GraphWithSettings graphWithSettings) {
            super(EntitySettingSpecs.Move);
            this.graphWithSettings = Objects.requireNonNull(graphWithSettings);
            this.specialCases = new HashMap<>();
            this.specialCases.put(EntityType.VIRTUAL_VOLUME, this::applyVirtualVolumeMove);
            this.specialCases.put(EntityType.VIRTUAL_MACHINE, (virtualMachine, isMoveEnabled) -> {
                applyMovableToCommodities(virtualMachine, isMoveEnabled,
                        c -> c.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE);
            });
        }

        @Override
        protected void apply(@Nonnull TopologyEntityDTO.Builder entity, boolean isMoveEnabled) {
            this.specialCases.getOrDefault(EntityType.forNumber(entity.getEntityType()),
                    (e, s) -> applyMovableToCommodities(e, s, c -> true))
                    .accept(entity, isMoveEnabled);
        }

        private void applyVirtualVolumeMove(TopologyEntityDTO.Builder virtualVolume,
                Boolean isMoveEnabled) {
            logger.debug(
                    "Setting will be applied to virtual machines connected to virtual volume {}",
                    virtualVolume::getOid);
            final Collection<TopologyEntityDTO.Builder> connectedVirtualMachines =
                    this.graphWithSettings.getTopologyGraph()
                            .getEntity(virtualVolume.getOid())
                            .map(vv -> vv.getInboundAssociatedEntities()
                                    .stream()
                                    .filter(connectedEntity -> connectedEntity.getEntityType() ==
                                            EntityType.VIRTUAL_MACHINE_VALUE)
                                    .map(TopologyEntity::getTopologyEntityDtoBuilder)
                                    .collect(Collectors.toList()))
                            .orElseGet(() -> {
                                logger.error(
                                        "Topology graph does not contain virtual volume with id {}",
                                        virtualVolume.getOid());
                                return Collections.emptyList();
                            });
            if (connectedVirtualMachines.isEmpty()) {
                logger.debug("Unattached virtual volume {}. No move settings applied",
                        virtualVolume::getOid);
            } else {
                connectedVirtualMachines.forEach(vm ->
                        // Virtual machine may have more than one virtual volume with the same storage tier.
                        // Only apply to the one with the associated virtual volume.
                        applyMovableToCommodities(vm, isMoveEnabled,
                                c -> c.getProviderEntityType() == EntityType.STORAGE_TIER_VALUE &&
                                        c.getVolumeId() == virtualVolume.getOid()));
            }
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

        TopologyInfo topologyInfo_;

        private VMShopTogetherApplicator(TopologyInfo topologyInfo) {
            super();
            topologyInfo_ = topologyInfo;
        }

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                @Nonnull Map<EntitySettingSpecs, Setting> settings) {
            if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                    && settings.containsKey(EntitySettingSpecs.ShopTogether)) {
                // TODO: For migration plans from on prem to cloud or from cloud to cloud,
                // we should shop together.
                final boolean isShopTogetherSettingEnabled = settings.get(EntitySettingSpecs.ShopTogether)
                        .getBooleanSettingValue().getValue();
                final boolean isRealTime = !TopologyDTOUtil.isPlan(topologyInfo_);

                // Note: if a VM does not support shop together execution, even when move and storage
                // move sets to Manual or Automatic, dont enable shop together.
                // If the entity is a reserved VM then it should always shop together.
                if (entity.getOrigin().hasReservationOrigin()) {
                    entity.getAnalysisSettingsBuilder().setShopTogether(true);
                // Apply shop together to false only if shopTogether setting is disabled by user
                // and topology is realtime. We don't want to do it for plans because other stages
                // like "IgnoreConstraints" set shop together to 'true' and we don't want to override it here.
                } else if ((!isShopTogetherSettingEnabled && isRealTime)
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
                    logger.debug("Shop together is disabled for {}.",
                            entity.getDisplayName());
                }
            } else if (entity.getEntityType() == EntityType.DATABASE_SERVER_VALUE ||
                    entity.getEntityType() == EntityType.DATABASE_VALUE) {
                // database entities should not perform shop-together
                entity.getAnalysisSettingsBuilder().setShopTogether(false);
            }
        }
    }

    /**
     * Applies the "suspend" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class SuspendApplicator extends SingleSettingApplicator {

        private SuspendApplicator(boolean isEnabledByDefault) {
            super(isEnabledByDefault ? EntitySettingSpecs.Suspend :
                EntitySettingSpecs.DisabledSuspend);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            // when setting value is DISABLED, set suspendable to false,
            // otherwise keep the original value which could have been set
            // when converting from SDK entityDTO.
            if (ActionMode.DISABLED.name().equals(setting.getEnumSettingValue().getValue())) {
                entity.getAnalysisSettingsBuilder().setSuspendable(false);
                logger.trace("Disabled suspendable for {}::{}",
                                entity::getEntityType, entity::getDisplayName);
            }
        }
    }

    /**
     * Applies the "delete" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class DeleteApplicator extends SingleSettingApplicator {
        private DeleteApplicator() {
            super(EntitySettingSpecs.Delete);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final Setting setting) {
            if (getEntitySettingSpecs().getValue(setting, ActionMode.class) ==
                    ActionMode.DISABLED) {
                entity.getAnalysisSettingsBuilder().setDeletable(false);
            }
        }
    }

    /**
     * Applies the "provision" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ProvisionApplicator extends SingleSettingApplicator {

        private ProvisionApplicator(boolean isEnabledByDefault) {
            super(isEnabledByDefault ? EntitySettingSpecs.Provision
                    : EntitySettingSpecs.DisabledProvision);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            // when setting value is DISABLED, set cloneable to false,
            // otherwise keep the original value which could have been set
            // when converting from SDK entityDTO.
            if (ActionMode.DISABLED.name().equals(setting.getEnumSettingValue().getValue())) {
                entity.getAnalysisSettingsBuilder().setCloneable(false);
                logger.trace("Disabled provision for {}::{}",
                            entity::getEntityType, entity::getDisplayName);
            }
        }
    }

    /**
     * Adds the "scaling" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ScalingApplicator extends SingleSettingApplicator {

        private ScalingApplicator() {
            super(EntitySettingSpecs.Move);
        }
        @Override
        protected void apply(@Nonnull final Builder entity, @Nonnull final Setting setting) {
            List<CommoditiesBoughtFromProvider.Builder> commBoughtGroupingList = entity
                    .getCommoditiesBoughtFromProvidersBuilderList().stream()
                    .filter(s -> s.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE ||
                            s.getProviderEntityType() == EntityType.DATABASE_SERVER_TIER_VALUE
                    || s.getProviderEntityType() == EntityType.DATABASE_TIER_VALUE).collect(Collectors.toList());
            for (CommoditiesBoughtFromProvider.Builder commBought : commBoughtGroupingList) {
                if (setting.getEnumSettingValue().getValue().equals(ActionMode.DISABLED.name())) {
                    commBought.setScalable(false);
                }
            }
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
                    ActionMode.DISABLED !=
                            getEntitySettingSpecs().getValue(setting, ActionMode.class);
            entity.getCommoditySoldListBuilderList()
                    .forEach(commSoldBuilder -> {
                        /* We shouldn't change isResizable if it comes as false from a probe side.
                           For example static memory VMs comes from Hyper-V targets with resizeable=false
                           for VMEM(53) commodities.
                         */
                        // Apply setting value only if resize is not disabled by the entity
                        if (!commSoldBuilder.hasIsResizeable() ||
                                (commSoldBuilder.hasIsResizeable() && commSoldBuilder.getIsResizeable())) {
                            commSoldBuilder.setIsResizeable(resizeable);

                            if (!resizeable) {
                                logger.trace("Disabled resize for {}:{}:{}",
                                        entity::getEntityType, entity::getDisplayName,
                                        commSoldBuilder::getCommodityType);
                            }
                        } else {
                            // Do not override with the setting if resize has been disabled at entity level
                            logger.trace("{}:{}:{} : Not overriding resizeable setting, resizeable is disabled at entity level",
                                    entity::getEntityType, entity::getDisplayName, commSoldBuilder::getCommodityType);
                        }
                    });
        }
    }

    /**
     * Applies the "ResizeVStorage" setting to {@link TopologyEntityDTO.Builder} which is an on-prem VM.
     */
    private static class ResizeVStorageApplicator extends SingleSettingApplicator {

        private ResizeVStorageApplicator() {
            super(EntitySettingSpecs.ResizeVStorage);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE &&
                entity.getEnvironmentType() == EnvironmentType.ON_PREM) {
                final boolean resizeable = setting.hasBooleanSettingValue() && setting.getBooleanSettingValue().getValue();
                entity.getCommoditySoldListBuilderList().stream()
                    .filter(c -> c.getCommodityType().getType() == CommodityType.VSTORAGE_VALUE)
                    .forEach(commSoldBuilder -> {
                        commSoldBuilder.setIsResizeable(resizeable);
                        logger.trace("Setting resizable for {}:{}:{} to {}",
                            entity.getEntityType(), entity.getDisplayName(),
                            commSoldBuilder.getCommodityType(), resizeable);
                    });
            }
        }
    }

    /**
     * Applies a Vcpu resize to the virtual machine represented in
     * {@link TopologyEntityDTO.Builder}.
     */
    private static class VirtualMachineResizeVcpuApplicator extends MultipleSettingsApplicator {

        private VirtualMachineResizeVcpuApplicator() {
            super(Arrays.asList(EntitySettingSpecs.ResizeVcpuBelowMinThreshold,
                    EntitySettingSpecs.ResizeVcpuDownInBetweenThresholds,
                    EntitySettingSpecs.ResizeVcpuUpInBetweenThresholds,
                    EntitySettingSpecs.ResizeVcpuAboveMaxThreshold));
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                             @Nonnull final Collection<Setting> settings) {
            boolean allSettingsDisabled = settings.stream()
                    .filter(setting -> setting.getEnumSettingValue().getValue().equals(ActionMode.DISABLED.name()))
                    .collect(Collectors.toList()).size() == this.getEntitySettingSpecs().size();
            if (allSettingsDisabled) {
                updateCommodities(entity, CommodityType.VCPU);
            }
        }
    }

    /**
     * Applies a Vmem resize to the virtual machine represented in
     * {@link TopologyEntityDTO.Builder}.
     */
    private static class VirtualMachineResizeVmemApplicator extends MultipleSettingsApplicator {

        private VirtualMachineResizeVmemApplicator() {
            super(Arrays.asList(EntitySettingSpecs.ResizeVmemBelowMinThreshold,
                    EntitySettingSpecs.ResizeVmemDownInBetweenThresholds,
                    EntitySettingSpecs.ResizeVmemUpInBetweenThresholds,
                    EntitySettingSpecs.ResizeVmemAboveMaxThreshold));
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                             @Nonnull final Collection<Setting> settings) {
            boolean allSettingsDisabled = settings.stream()
                    .filter(setting -> setting.getEnumSettingValue().getValue().equals(ActionMode.DISABLED.name())).count() == this.getEntitySettingSpecs().size();
            if (allSettingsDisabled) {
                updateCommodities(entity, CommodityType.VMEM);
            }
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
        public void apply(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull Setting setting) {
            final float settingValue = setting.getNumericSettingValue().getValue();
            for (CommoditySoldDTO.Builder commodity : SettingApplicator
                    .getCommoditySoldBuilders(entity, commodityType)) {
                commodity.setEffectiveCapacityPercentage(settingValue);
            }
        }
    }

    /**
     * HA related commodities applicator. This applicator will process cpu/mem utilization
     * threshold. Both of the commodities are calculated on top of appropriate settings.
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
                final Setting cpuUtilSetting = settings.get(EntitySettingSpecs.CpuUtilization);
                final Setting memUtilSetting = settings.get(EntitySettingSpecs.MemoryUtilization);
                applyUtilizationChanges(entity, CommodityType.CPU, cpuUtilSetting);
                applyUtilizationChanges(entity, CommodityType.MEM, memUtilSetting);
            }
        }

        private void applyMaxUtilizationToCapacity(@Nonnull final TopologyEntityDTO.Builder entity,
                                                   @Nonnull final CommodityType commodityType,
                                                   final float maxDesiredUtilization) {
            // We only want to do this for cluster headroom calculations.
            Preconditions.checkArgument(topologyInfo.getPlanInfo().getPlanProjectType() ==
                    PlanProjectType.CLUSTER_HEADROOM);
            for (CommoditySoldDTO.Builder commodity : SettingApplicator
                    .getCommoditySoldBuilders(entity, commodityType)) {
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
                                             @Nullable Setting setting) {
            for (CommoditySoldDTO.Builder commodity : SettingApplicator
                    .getCommoditySoldBuilders(entity, commodityType)) {
                if (setting != null) {
                    commodity.setEffectiveCapacityPercentage(
                            setting.getNumericSettingValue().getValue());
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
     * Applies min/Max to the virtual machine represented in {@link TopologyEntityDTO.Builder}.
     */
    private static class VMThresholdApplicator extends MultipleSettingsApplicator {

        //VMem setting value is in MBs. Market expects it in KBs.
        private final float conversionFactor = 1024.0f;
        private final Map<Integer, Set<EntitySettingSpecs>> entitySettingMapping;
        // We need the graph to fin the core CPU speed of the hosting PM for the unit conversion.
        private final TopologyGraph<TopologyEntity> graph;

        private VMThresholdApplicator(@Nonnull final Map<Integer, Set<EntitySettingSpecs>> settingMapping,
                                          @Nonnull final GraphWithSettings settingsGraph) {
            super(settingMapping.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()));
            this.graph = settingsGraph.getTopologyGraph();
            this.entitySettingMapping = Objects.requireNonNull(settingMapping);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                             @Nonnull final Collection<Setting> settings) {
            if (EntityType.VIRTUAL_MACHINE_VALUE == entity.getEntityType() &&
                    EnvironmentType.ON_PREM == entity.getEnvironmentType()) {
                entity.getCommoditySoldListBuilderList().stream()
                    .filter(commodity -> entitySettingMapping.get(commodity.getCommodityType().getType()) != null)
                    .forEach(commodityBuilder -> {
                        Set<EntitySettingSpecs> validSpecs = entitySettingMapping.get(commodityBuilder
                                .getCommodityType().getType());
                        if (commodityBuilder.getCommodityType().getType() == CommodityType.VCPU.getNumber()) {
                            // Gets the CPU speed of the PM.
                            final Optional<Integer> cpuSpeed = graph.getProviders(entity.getOid())
                                    .filter(provider -> provider.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                                    .filter(provider -> provider.getTypeSpecificInfo().getPhysicalMachine().hasCpuCoreMhz())
                                    .map(pm -> pm.getTypeSpecificInfo().getPhysicalMachine().getCpuCoreMhz())
                                    .findFirst();
                            if (cpuSpeed.isPresent()) {
                                setThresholds(entity, validSpecs, settings, commodityBuilder, cpuSpeed.get());
                            } else if (entity.getTypeSpecificInfo().getVirtualMachine().hasNumCpus()) {
                                double multiplier = commodityBuilder.getCapacity()
                                        / entity.getTypeSpecificInfo().getVirtualMachine().getNumCpus();
                                setThresholds(entity, validSpecs, settings, commodityBuilder, multiplier);
                            } else {
                                logger.error("VCPU threshold applicator couldn't find the CPU speed " +
                                        "of the host (VM: {})", entity.getDisplayName());
                            }
                        } else if (commodityBuilder.getCommodityType().getType() == CommodityType.VMEM.getNumber()) {
                            setThresholds(entity, validSpecs, settings, commodityBuilder, conversionFactor);
                        }
                });
            }
        }

        /**
         * Iterates over settings for a VM. Retrives its resize modes in different intervals.
         * Based on the modes and where the current commodity capacity stands wrt Min and Max, we populate the thresholds.
         *
         * @param entity being processed.
         * @param validSpecs to consider.
         * @param settings associated with the entity.
         * @param commodityBuilder is the commodity being processed.
         * @param multiplier multiplicand used along with the thresholds.
         */
        protected void setThresholds(@Nonnull final TopologyEntityDTO.Builder entity,
                                     Set<EntitySettingSpecs> validSpecs,
                                     Collection<Setting> settings,
                                     CommoditySoldDTO.Builder commodityBuilder,
                                     double multiplier) {
            final Thresholds.Builder thresholdsBuilder = Thresholds.newBuilder();
            double minThreshold = 0;
            double maxThreshold = Double.MAX_VALUE;
            ActionMode modeForMin = null;
            ActionMode modeForMax = null;
            ActionMode modeDownInBetweenThresholds = null;
            ActionMode modeUpInBetweenThresholds = null;
            for (Setting setting : settings) {
                final EntitySettingSpecs policySetting =
                        EntitySettingSpecs.getSettingByName(setting.getSettingSpecName()).get();
                if (validSpecs.contains(policySetting)) {
                    final float settingValue = setting.getNumericSettingValue().getValue();
                    switch (policySetting) {
                        case ResizeVcpuMinThreshold:
                        case ResizeVmemMinThreshold:
                            minThreshold = settingValue * multiplier;
                            break;
                        case ResizeVcpuBelowMinThreshold:
                        case ResizeVmemBelowMinThreshold:
                            modeForMin = ActionMode.valueOf(setting.getEnumSettingValue().getValue());
                            break;
                        case ResizeVcpuMaxThreshold:
                        case ResizeVmemMaxThreshold:
                            maxThreshold = settingValue * multiplier;
                            break;
                        case ResizeVcpuAboveMaxThreshold:
                        case ResizeVmemAboveMaxThreshold:
                            modeForMax = ActionMode.valueOf(setting.getEnumSettingValue().getValue());
                            break;
                        case ResizeVcpuUpInBetweenThresholds:
                        case ResizeVmemUpInBetweenThresholds:
                            modeUpInBetweenThresholds = ActionMode.valueOf(setting.getEnumSettingValue().getValue());
                            break;
                        case ResizeVcpuDownInBetweenThresholds:
                        case ResizeVmemDownInBetweenThresholds:
                            modeDownInBetweenThresholds = ActionMode.valueOf(setting.getEnumSettingValue().getValue());
                            break;
                    }
                }
            }
            if (modeForMax == null || modeForMin == null || modeDownInBetweenThresholds == null || modeUpInBetweenThresholds == null) {
                logger.error("Unable to set capacity bounds based on policy for {} on {}", entity.getDisplayName(),
                        commodityBuilder.getDisplayName());
                return;
            }

            double capacityUpperBound = Double.MAX_VALUE;
            double capacityLowerBound = 0;
            double capacity = commodityBuilder.getCapacity();
            if (capacity >= maxThreshold) {
                if (modeForMax == ActionMode.DISABLED) {
                    capacityUpperBound = capacity;
                }
            } else {
                if (modeUpInBetweenThresholds == ActionMode.DISABLED) {
                    capacityUpperBound = capacity;
                } else if (modeUpInBetweenThresholds != modeForMax) {
                    capacityUpperBound = maxThreshold;
                }
            }

            if (capacity <= minThreshold) {
                if (modeForMin == ActionMode.DISABLED) {
                    capacityLowerBound = capacity;
                }
            } else {
                if (modeDownInBetweenThresholds == ActionMode.DISABLED) {
                    capacityLowerBound = capacity;
                } else if (modeDownInBetweenThresholds != modeForMin) {
                    capacityLowerBound = minThreshold;
                }
            }
            commodityBuilder.setThresholds(thresholdsBuilder.setMax(capacityUpperBound).setMin(capacityLowerBound));
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
        // Entity types for which this setting is valid
        private static final ImmutableSet<Integer> applicableEntityTypes =
            ImmutableSet.of(
                EntityType.VIRTUAL_MACHINE_VALUE,
                EntityType.CONTAINER_VALUE,
                EntityType.STORAGE_VALUE
            );

        private final CommodityType commodityType;

        // convert into units that market uses.
        private final ImmutableMap<Integer, Float> conversionFactor =
            ImmutableMap.of(
                //VMEM setting value is in MBs. Market expects it in KBs.
                CommodityType.VMEM_VALUE, 1024.0f,
                //VMEM_REQUEST setting value is in MBs. Market expects it in KBs.
                CommodityType.VMEM_REQUEST_VALUE, 1024.0f,
                //VSTORAGE setting value is in GBs. Market expects it in MBs.
                CommodityType.VSTORAGE_VALUE, 1024.0f,
                //STORAGE_AMOUNT setting value is in GBs. Market expects it in MBs.
                CommodityType.STORAGE_AMOUNT_VALUE, 1024.0f);


        private ResizeIncrementApplicator(@Nonnull EntitySettingSpecs setting,
                @Nonnull final CommodityType commodityType) {
            super(setting);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull Setting setting) {
            if (applicableEntityTypes.contains(entity.getEntityType())) {
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
                                && setting.getNumericSettingValue().getValue() == getDefaultVStorageIncrement()) {
                            commodityBuilder.setIsResizeable(false);
                        }

                        logger.debug("Apply Resize Increment for commodity: {} , value={}",
                            commodityType.getNumber(), commodityBuilder.getCapacityIncrement());
                    });
            }
        }

        private float getDefaultVStorageIncrement() {
            return EntitySettingSpecs.VstorageIncrement
                    .getSettingSpec()
                    .getNumericSettingValueType()
                    .getDefault();
        }
    }

    /**
     * Applicator for the resize target utilization settings.
     */
    @ThreadSafe
    private abstract static class ResizeTargetUtilizationApplicator extends
            SingleSettingApplicator {

        static final String APPLY_RESIZE_TARGET_UTILIZATION_MESSAGE =
                "Apply Resize Target Utilization for entity = {}, commodity = {} , value = {}";

        private final CommodityType commodityType;

        private ResizeTargetUtilizationApplicator(@Nonnull EntitySettingSpecs setting,
                @Nonnull final CommodityType commodityType) {
            super(setting);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull Setting setting) {
            final EntityType entityType = EntityType.forNumber(entity.getEntityType());
            if (entityType != null &&
                    getEntitySettingSpecs().getEntityTypeScope().contains(entityType)) {
                final float settingValue = setting.getNumericSettingValue().getValue();
                // Divide by 100 since the RTU value set by the user in the UI is a percentage value
                apply(entity, settingValue / 100);
            }
        }

        protected abstract void apply(@Nonnull TopologyEntityDTO.Builder entity,
                double resizeTargetUtilization);

        @Nonnull
        CommodityType getCommodityType() {
            return commodityType;
        }
    }

    /**
     * Applicator for the resize target utilization settings.
     * This sets the resize_target_utilization in the {@link CommoditySoldDTO}.
     */
    @ThreadSafe
    private static class ResizeTargetUtilizationCommoditySoldApplicator extends
            ResizeTargetUtilizationApplicator {

        private ResizeTargetUtilizationCommoditySoldApplicator(@Nonnull EntitySettingSpecs setting,
                @Nonnull final CommodityType commodityType) {
            super(setting, commodityType);
        }

        @Override
        protected void apply(@Nonnull TopologyEntityDTO.Builder entity,
                double resizeTargetUtilization) {
            entity.getCommoditySoldListBuilderList()
                    .stream()
                    .filter(commodity -> commodity.getCommodityType().getType() ==
                            getCommodityType().getNumber())
                    .forEach(commodityBuilder -> {
                        commodityBuilder.setResizeTargetUtilization(resizeTargetUtilization);
                        logger.debug(APPLY_RESIZE_TARGET_UTILIZATION_MESSAGE,
                                entity.getDisplayName(), getCommodityType().getNumber(),
                                commodityBuilder.getResizeTargetUtilization());
                    });
        }
    }

    /**
     * Applicator for the resize target utilization settings.
     * This sets the resize_target_utilization in the {@link CommodityBoughtDTO}.
     */
    @ThreadSafe
    private static class ResizeTargetUtilizationCommodityBoughtApplicator extends
            ResizeTargetUtilizationApplicator {

        private ResizeTargetUtilizationCommodityBoughtApplicator(
                @Nonnull EntitySettingSpecs setting, @Nonnull final CommodityType commodityType) {
            super(setting, commodityType);
        }

        @Override
        protected void apply(@Nonnull TopologyEntityDTO.Builder entity,
                double resizeTargetUtilization) {
            entity.getCommoditiesBoughtFromProvidersBuilderList()
                    .stream()
                    .map(CommoditiesBoughtFromProvider.Builder::getCommodityBoughtBuilderList)
                    .flatMap(Collection::stream)
                    .filter(commodity -> commodity.getCommodityType().getType() ==
                            getCommodityType().getNumber())
                    .forEach(commodityBuilder -> {
                        commodityBuilder.setResizeTargetUtilization(resizeTargetUtilization);
                        logger.debug(APPLY_RESIZE_TARGET_UTILIZATION_MESSAGE,
                                entity.getDisplayName(), getCommodityType().getNumber(),
                                commodityBuilder.getResizeTargetUtilization());
                    });
        }
    }

    /**
     * Applicator for the setting of capacity.
     * This sets capacity in {@link CommoditySoldDTO}.
     */
    @ThreadSafe
    private static class OverrideCapacityApplicator extends SingleSettingApplicator {

        private final CommodityType commodityType;

        private OverrideCapacityApplicator(@Nonnull EntitySettingSpecs setting,
                                               @Nonnull final CommodityType commodityType) {
            super(setting);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull Setting setting) {
            final Float settingValue = getEntitySettingSpecs().getValue(setting, Float.class);
            if (settingValue == null) {
                return;
            }
            for (CommoditySoldDTO.Builder commodity : SettingApplicator
                    .getCommoditySoldBuilders(entity, commodityType)) {
                commodity.setCapacity(settingValue);
            }
        }
    }
}
