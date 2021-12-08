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
import java.util.function.Function;
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
import org.jetbrains.annotations.NotNull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.setting.ActionSettingSpecs;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.components.common.setting.VCPUScalingUnitsEnum;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.ScalingPolicyEnum;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.components.common.setting.VcpuScalingCoresPerSocketSocketModeEnum;
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

    private static final Set<Integer> CPU_COMMODITY_TYPES = ImmutableSet.of(
        CommodityType.VCPU_VALUE,
        CommodityType.VCPU_REQUEST_VALUE
    );

    private static final Set<Integer> MEM_COMMODITY_TYPES = ImmutableSet.of(
        CommodityType.VMEM_VALUE,
        CommodityType.VMEM_REQUEST_VALUE
    );

    private final boolean considerUtilizationConstraintInClusterHeadroomPlan;
    private final boolean addAccessCommoditiesForVsan;

    /**
     * Constructor.
     *
     * @param considerUtilizationConstraintInClusterHeadroomPlan consider utilization constraint in cluster headroom plan or not
     * @param addAccessCommoditiesForVsan add access commodities between vsan hosts and storages when true.
     */
    public EntitySettingsApplicator(final boolean considerUtilizationConstraintInClusterHeadroomPlan,
                                    final boolean addAccessCommoditiesForVsan) {
        this.considerUtilizationConstraintInClusterHeadroomPlan = considerUtilizationConstraintInClusterHeadroomPlan;
        this.addAccessCommoditiesForVsan = addAccessCommoditiesForVsan;
    }

    /**
     * Applies the settings contained in a {@link GraphWithSettings} to the topology graph
     * contained in it.
     *
     * @param topologyInfo the {@link TopologyInfo}
     * @param graphWithSettings A {@link TopologyGraph<TopologyEntity>} and the settings that apply to it.
     */
    public void applySettings(@Nonnull final TopologyInfo topologyInfo,
                              @Nonnull final GraphWithSettings graphWithSettings) {
        final List<SettingApplicator> applicators =
            buildApplicators(topologyInfo, graphWithSettings,
                    considerUtilizationConstraintInClusterHeadroomPlan, addAccessCommoditiesForVsan);
        graphWithSettings.getTopologyGraph().entities()
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .forEach(entity -> {
                final Map<EntitySettingSpecs, Setting> settingsForEntity =
                        new EnumMap<>(EntitySettingSpecs.class);
                final Map<ConfigurableActionSettings, Setting> actionModeSettings =
                    new EnumMap<>(ConfigurableActionSettings.class);
                for (final Setting setting : graphWithSettings.getSettingsForEntity(
                        entity.getOid())) {
                    final Optional<EntitySettingSpecs> policySetting =
                            EntitySettingSpecs.getSettingByName(setting.getSettingSpecName());
                    if (policySetting.isPresent()) {
                        settingsForEntity.put(policySetting.get(), setting);
                    } else if (ActionSettingSpecs.isActionModeSetting(setting.getSettingSpecName())) {
                        actionModeSettings.put(ConfigurableActionSettings.fromSettingName(
                            setting.getSettingSpecName()), setting);
                    } else if (!ActionSettingSpecs.isActionModeSubSetting(setting.getSettingSpecName())) {
                        // Action workflow and execution schedule settings do not affect topology
                        // setting applicators.
                        logger.warn("Unknown setting {} for entity {}",
                                setting.getSettingSpecName(), entity.getOid());
                    }
                }

                for (SettingApplicator applicator: applicators) {
                    applicator.apply(entity, settingsForEntity, actionModeSettings);
                }

            });
    }

    /**
     * Get the list of applicators for a particular {@link TopologyInfo}.
     *
     * @param topologyInfo The {@link TopologyInfo} of an in-progress topology broadcast.
     * @param graphWithSettings {@link GraphWithSettings} of the in-progress topology
     * @param considerUtilizationConstraintInClusterHeadroomPlan consider utilization constraint in cluster headroom plan or not
     * @param addAccessCommoditiesForVsan add access commodities between vsan hosts and storages when true.
     * @return A list of {@link SettingApplicator}s for settings that apply to this topology.
     */
    private static List<SettingApplicator> buildApplicators(@Nonnull final TopologyInfo topologyInfo,
                                                            @Nonnull final GraphWithSettings graphWithSettings,
                                                            final boolean considerUtilizationConstraintInClusterHeadroomPlan,
                                                            final boolean addAccessCommoditiesForVsan) {
        return ImmutableList.of(new MoveApplicator(),
                new VMShopTogetherApplicator(topologyInfo),
                new ReconfigureApplicator(),
                new SuspendApplicator(),
                new ProvisionApplicator(),
                new ResizeApplicator(),
                new RateOfResizeApplicator(),
                new ScalingApplicator(ConfigurableActionSettings.CloudComputeScale),
                new ScalingApplicator(ConfigurableActionSettings.CloudDBScale),
                new ScalingApplicator(ConfigurableActionSettings.CloudDBServerScale),
                new MoveCommoditiesFromProviderTypesApplicator(ConfigurableActionSettings.StorageMove,
                        TopologyDTOUtil.STORAGE_TYPES),
                new VirtualMachineResizeVcpuApplicator(),
                new VirtualMachineResizeVmemApplicator(),
                new DeleteApplicator(),
                new UtilizationThresholdApplicator(EntitySettingSpecs.IoThroughput,
                        CommodityType.IO_THROUGHPUT),
                new UtilizationThresholdApplicator(EntitySettingSpecs.NetThroughput,
                        CommodityType.NET_THROUGHPUT),
                new UtilizationThresholdApplicator(EntitySettingSpecs.SwappingUtilization,
                        CommodityType.SWAPPING),
                new ReadyQueueUtilizationThresholdApplicator(),
                new UtilizationThresholdApplicator(EntitySettingSpecs.StorageAmountUtilization,
                        CommodityType.STORAGE_AMOUNT),
                new UtilizationThresholdApplicator(EntitySettingSpecs.StorageProvisionedUtilization,
                        CommodityType.STORAGE_PROVISIONED),
                new UtilizationThresholdApplicator(EntitySettingSpecs.IopsUtilization,
                        CommodityType.STORAGE_ACCESS),
                new UtilizationThresholdApplicator(EntitySettingSpecs.LatencyUtilization,
                        CommodityType.STORAGE_LATENCY),
                new UtilizationThresholdApplicator(EntitySettingSpecs.HeapUtilization,
                        CommodityType.HEAP),
                new UtilizationThresholdApplicator(EntitySettingSpecs.RemainingGcCapacityUtilization,
                        CommodityType.REMAINING_GC_CAPACITY),
                new UtilizationThresholdApplicator(EntitySettingSpecs.DbCacheHitRateUtilization,
                        CommodityType.DB_CACHE_HIT_RATE),
                new UtilizationThresholdApplicator(EntitySettingSpecs.VCPURequestUtilization,
                        CommodityType.VCPU_REQUEST),
                new UtilizationThresholdApplicator(EntitySettingSpecs.DTUUtilization,
                        CommodityType.DTU),
                new UtilizationThresholdApplicator(EntitySettingSpecs.PoolCpuUtilizationThreshold,
                        CommodityType.POOL_CPU),
                new UtilizationThresholdApplicator(EntitySettingSpecs.PoolMemoryUtilizationThreshold,
                        CommodityType.POOL_MEM),
                new UtilizationThresholdApplicator(EntitySettingSpecs.PoolStorageUtilizationThreshold,
                        CommodityType.POOL_STORAGE),
                new UtilizationThresholdApplicator(EntitySettingSpecs.DBMemUtilization,
                        CommodityType.DB_MEM),
                new UtilizationThresholdApplicator(EntitySettingSpecs.ResizeTargetUtilizationStorageAmount,
                        CommodityType.STORAGE_AMOUNT),
                new UtilTargetApplicator(),
                new TargetBandApplicator(),
                new HaDependentUtilizationApplicator(topologyInfo, considerUtilizationConstraintInClusterHeadroomPlan),
                new VMCPUIncrementApplicator(graphWithSettings.getTopologyGraph()),
                new ResizeIncrementApplicator(EntitySettingSpecs.VmVmemIncrement,
                        CommodityType.VMEM),
                new ResizeIncrementApplicator(EntitySettingSpecs.ContainerSpecVcpuIncrement,
                        CommodityType.VCPU),
                new ResizeIncrementApplicator(EntitySettingSpecs.ContainerSpecVmemIncrement,
                        CommodityType.VMEM),
                new ResizeIncrementApplicator(EntitySettingSpecs.ContainerSpecVcpuIncrement,
                        CommodityType.VCPU_REQUEST),
                new ResizeIncrementApplicator(EntitySettingSpecs.ContainerSpecVmemIncrement,
                        CommodityType.VMEM_REQUEST),
                new ResizeIncrementApplicator(EntitySettingSpecs.VstorageIncrement,
                        CommodityType.VSTORAGE),
                new ResizeIncrementApplicator(EntitySettingSpecs.StorageIncrement,
                        CommodityType.STORAGE_AMOUNT),
                new EntityThresholdApplicator(
                    ImmutableMap.of(
                        CommodityType.VMEM.getNumber(), Sets.newHashSet(
                                EntitySettingSpecs.ResizeVmemMinThreshold,
                                EntitySettingSpecs.ResizeVmemMaxThreshold),
                        CommodityType.VCPU.getNumber(), Sets.newHashSet(
                                EntitySettingSpecs.ResizeVcpuMinThreshold,
                                EntitySettingSpecs.ResizeVcpuMaxThreshold)),
                    ImmutableMap.of(
                        CommodityType.VMEM.getNumber(), Sets.newHashSet(
                            ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
                            ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds,
                            ConfigurableActionSettings.ResizeVmemAboveMaxThreshold,
                            ConfigurableActionSettings.ResizeVmemBelowMinThreshold),
                        CommodityType.VCPU.getNumber(), Sets.newHashSet(
                            ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds,
                            ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds,
                            ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold,
                            ConfigurableActionSettings.ResizeVcpuBelowMinThreshold)),
                    graphWithSettings),
                new EntityThresholdApplicator(
                    ImmutableMap.of(
                        CommodityType.VMEM.getNumber(), Sets.newHashSet(
                            EntitySettingSpecs.ResizeVmemLimitMinThreshold,
                            EntitySettingSpecs.ResizeVmemLimitMaxThreshold),
                        CommodityType.VCPU.getNumber(), Sets.newHashSet(
                            EntitySettingSpecs.ResizeVcpuLimitMinThreshold,
                            EntitySettingSpecs.ResizeVcpuLimitMaxThreshold),
                        CommodityType.VMEM_REQUEST.getNumber(), Sets.newHashSet(
                            EntitySettingSpecs.ResizeVmemRequestMinThreshold),
                        CommodityType.VCPU_REQUEST.getNumber(), Sets.newHashSet(
                            EntitySettingSpecs.ResizeVcpuRequestMinThreshold)),
                    ImmutableMap.of(
                        CommodityType.VMEM.getNumber(), Sets.newHashSet(
                            ConfigurableActionSettings.Resize,
                            ConfigurableActionSettings.ResizeVmemLimitAboveMaxThreshold,
                            ConfigurableActionSettings.ResizeVmemLimitBelowMinThreshold),
                        CommodityType.VCPU.getNumber(), Sets.newHashSet(
                            ConfigurableActionSettings.Resize,
                            ConfigurableActionSettings.ResizeVcpuLimitAboveMaxThreshold,
                            ConfigurableActionSettings.ResizeVcpuLimitBelowMinThreshold),
                        CommodityType.VMEM_REQUEST.getNumber(), Sets.newHashSet(
                            ConfigurableActionSettings.Resize,
                            ConfigurableActionSettings.ResizeVmemLimitAboveMaxThreshold,
                            ConfigurableActionSettings.ResizeVmemRequestBelowMinThreshold),
                        CommodityType.VCPU_REQUEST.getNumber(), Sets.newHashSet(
                            ConfigurableActionSettings.Resize,
                            ConfigurableActionSettings.ResizeVcpuLimitAboveMaxThreshold,
                            ConfigurableActionSettings.ResizeVcpuRequestBelowMinThreshold)),
                    graphWithSettings
                ),
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
                new ResizeTargetUtilizationCommodityBoughtApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationIops, CommodityType.STORAGE_ACCESS),
                new ResizeTargetUtilizationCommodityBoughtApplicator(
                        EntitySettingSpecs.DTUUtilization,
                        CommodityType.DTU),
                new ResizeTargetUtilizationCommodityBoughtApplicator(
                        EntitySettingSpecs.StorageAmountUtilization,
                        CommodityType.STORAGE_AMOUNT),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.DTUUtilization,
                        CommodityType.DTU),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.StorageAmountUtilization,
                        CommodityType.STORAGE_AMOUNT),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationVcpu, CommodityType.VCPU),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationVmem, CommodityType.VMEM),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationIopsAndThroughput,
                        CommodityType.STORAGE_ACCESS),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationIopsAndThroughput,
                        CommodityType.IO_THROUGHPUT),
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationIops, CommodityType.STORAGE_ACCESS),
                // Applies to cloud entities only.
                new ResizeTargetUtilizationCommoditySoldApplicator(
                        EntitySettingSpecs.ResizeTargetUtilizationStorageAmount,
                        CommodityType.STORAGE_AMOUNT),
                new InstanceStoreSettingApplicator(graphWithSettings.getTopologyGraph(),
                                        new VmInstanceStoreCommoditiesCreator(),
                                        new ComputeTierInstanceStoreCommoditiesCreator(),
                                        topologyInfo),
                new OverrideCapacityApplicator(EntitySettingSpecs.ViewPodActiveSessionsCapacity,
                        CommodityType.ACTIVE_SESSIONS),
                new OverrideCapacityApplicator(EntitySettingSpecs.ViewPodActiveSessionsCapacity,
                        CommodityType.TOTAL_SESSIONS),
                new OverrideCapacityByUserApplicator(EntitySettingSpecs.LatencyCapacity,
                                               CommodityType.STORAGE_LATENCY, graphWithSettings),
                new OverrideCapacityByUserApplicator(EntitySettingSpecs.IOPSCapacity,
                        CommodityType.STORAGE_ACCESS, graphWithSettings),
                new VsanStorageApplicator(graphWithSettings, addAccessCommoditiesForVsan),
                new ResizeVStorageApplicator(),
                new ResizeIncrementApplicator(EntitySettingSpecs.ApplicationHeapScalingIncrement,
                        CommodityType.HEAP),
                FeatureFlags.SERVICE_HORIZONTAL_SCALE.isEnabled()
                                        ? new HorizontalScalePolicyApplicator()
                                        : new ScalingPolicyApplicator(),
                new ResizeIncrementApplicator(EntitySettingSpecs.DBMemScalingIncrement,
                        CommodityType.DB_MEM),
                new EnableScaleApplicator(),
                new EnableDeleteApplicator(),
                new MinMaxReplicasApplicator());
    }

    /**
     * The applicator of a single {@link Setting} to a single {@link TopologyEntityDTO.Builder}.
     */
    public abstract static class ActionModeSettingApplicator extends BaseSettingApplicator {

        private final ConfigurableActionSettings setting;

        protected ActionModeSettingApplicator(@Nonnull ConfigurableActionSettings setting) {
            this.setting = Objects.requireNonNull(setting);
        }

        protected abstract void apply(@Nonnull TopologyEntityDTO.Builder entity,
                                      @Nonnull ActionMode actionMode);

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                          @Nonnull Map<EntitySettingSpecs, Setting> settings,
                          @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
            final Setting settingObject = actionModeSettings.get(setting);
            if (settingObject != null) {
                ActionMode actionMode = ActionMode.valueOf(settingObject.getEnumSettingValue().getValue());
                apply(entity, actionMode);
            }
        }
    }

    /**
     * The applicator of a single {@link EntitySettingSpecs} to a single {@link TopologyEntityDTO.Builder}.
     */
    public abstract static class SingleSettingApplicator extends BaseSettingApplicator {

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
                          @Nonnull Map<EntitySettingSpecs, Setting> settings,
                          @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
            final Setting settingObject = settings.get(setting);
            if (settingObject != null) {
                apply(entity, settingObject);
            }
        }
    }

    /**
     * The applicator of multiple {@link Setting}s to a single {@link TopologyEntityDTO.Builder}.
     */
    private abstract static class MultipleSettingsApplicator extends BaseSettingApplicator {

        private final List<EntitySettingSpecs> settings;
        private final List<ConfigurableActionSettings> actionModeSettings;

        private MultipleSettingsApplicator(
                @Nonnull List<EntitySettingSpecs> settings,
                @Nonnull List<ConfigurableActionSettings> actionModeSettings) {
            this.settings = Objects.requireNonNull(settings);
            this.actionModeSettings = Objects.requireNonNull(actionModeSettings);
        }

        private MultipleSettingsApplicator(@Nonnull List<EntitySettingSpecs> settings) {
            this(settings, Collections.emptyList());
        }

        protected List<EntitySettingSpecs> getEntitySettingSpecs() {
            return settings;
        }

        protected List<ConfigurableActionSettings> getConfigurableActionSettings() {
            return actionModeSettings;
        }

        protected abstract void apply(@Nonnull TopologyEntityDTO.Builder entity,
                                      @Nonnull Collection<Setting> settings);

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                          @Nonnull Map<EntitySettingSpecs, Setting> settings,
                          @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
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
            for (ConfigurableActionSettings setting : this.actionModeSettings) {
                final Setting settingObject = actionModeSettings.get(setting);
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
    private abstract static class AbstractMoveApplicator extends ActionModeSettingApplicator {

        protected AbstractMoveApplicator(@Nonnull ConfigurableActionSettings setting) {
            super(setting);
        }

        @Override
        protected void apply(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull ActionMode actionMode) {
            final boolean isMoveEnabled = actionMode != ActionMode.DISABLED;
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
    }

    /**
     * Applicator for {@link CommoditiesBoughtFromProvider#hasMovable()}
     * for {@link CommoditiesBoughtFromProvider#getProviderEntityType()}
     * with a specific {@link EntityType}.
     */
    private static class MoveCommoditiesFromProviderTypesApplicator extends AbstractMoveApplicator {

        private Set<EntityType> providerTypes;

        private MoveCommoditiesFromProviderTypesApplicator(@Nonnull ConfigurableActionSettings setting,
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

        private final Map<EntityType, BiConsumer<TopologyEntityDTO.Builder, Boolean>> specialCases;

        private MoveApplicator() {
            super(ConfigurableActionSettings.Move);
            this.specialCases = new HashMap<>();
            this.specialCases.put(EntityType.VIRTUAL_MACHINE, (virtualMachine, isMoveEnabled) -> {
                applyMovableToCommodities(virtualMachine, isMoveEnabled,
                        c -> c.getProviderEntityType() == EntityType.PHYSICAL_MACHINE_VALUE);
            });
            this.specialCases.put(EntityType.BUSINESS_USER, (businessUser, isMoveEnabled) -> {
                applyMovableToCommodities(businessUser, isMoveEnabled,
                    c -> c.getProviderEntityType() == EntityType.DESKTOP_POOL_VALUE);
            });
        }

        @Override
        protected void apply(@Nonnull TopologyEntityDTO.Builder entity, boolean isMoveEnabled) {
            this.specialCases.getOrDefault(EntityType.forNumber(entity.getEntityType()),
                    (e, s) -> applyMovableToCommodities(e, s, c -> true))
                    .accept(entity, isMoveEnabled);
        }
    }

    /**
     * Set shop together on a virtual machine{@link TopologyEntityDTO.Builder} based on the "move"
     * and "storage move" settings. In particular, if the "move" and "storage move"
     * action settings are both in Manual, or both in Automatic state, shop together can be enabled.
     * Otherwise, set the shop together to false because user has to explicitly change the settings
     * to turn on bundled moves on compute and storage resources.
     */
    private static class VMShopTogetherApplicator extends BaseSettingApplicator {

        TopologyInfo topologyInfo_;

        private VMShopTogetherApplicator(TopologyInfo topologyInfo) {
            super();
            topologyInfo_ = topologyInfo;
        }

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                @Nonnull Map<EntitySettingSpecs, Setting> settings,
                @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
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
                        || (entity.getEnvironmentType() == EnvironmentType.CLOUD
                        && !TopologyDTOUtil.isCloudMigrationPlan(topologyInfo_))) {
                    // user has to change default VM action settings to explicitly indicate they
                    // want to have compound move actions generated considering best placements in
                    // terms of both storage and compute resources. Usually user ask for shop together
                    // moves so as to take the actions to move VM across networks, thus Manual and
                    // Automatic has to be chosen to execute the actions.
                    // We make Cloud VMs shop together false. This is done because shop
                    // together generates compound moves but we need to show separate actions in the
                    // user interface. So, we just disable shop together for them because it has
                    // no real advantage in the cloud.
                    // NOTE: For migration plans from on prem to cloud or from cloud to cloud,
                    // we should shop together.
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
    private static class SuspendApplicator extends ActionModeSettingApplicator {

        private SuspendApplicator() {
            super(ConfigurableActionSettings.Suspend);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final ActionMode actionMode) {
            // when setting value is DISABLED, set suspendable to false,
            // otherwise keep the original value which could have been set
            // when converting from SDK entityDTO.
            if (ActionMode.DISABLED == actionMode) {
                entity.getAnalysisSettingsBuilder().setSuspendable(false);
                logger.trace("Disabled suspendable for {}::{}",
                                entity::getEntityType, entity::getDisplayName);
            }
        }
    }

    /**
     * Applies the "delete" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class DeleteApplicator extends ActionModeSettingApplicator {
        private DeleteApplicator() {
            super(ConfigurableActionSettings.Delete);
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final ActionMode actionMode) {
            if (ActionMode.DISABLED == actionMode) {
                entity.getAnalysisSettingsBuilder().setDeletable(false);
            }
        }
    }

    /**
     * Applies the "provision" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ProvisionApplicator extends ActionModeSettingApplicator {

        private ProvisionApplicator() {
            super(ConfigurableActionSettings.Provision);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final ActionMode actionMode) {
            // when setting value is DISABLED, set cloneable to false,
            // otherwise keep the original value which could have been set
            // when converting from SDK entityDTO.
            if (ActionMode.DISABLED == actionMode) {
                entity.getAnalysisSettingsBuilder().setCloneable(false);
                logger.trace("Disabled provision for {}::{}",
                            entity::getEntityType, entity::getDisplayName);
            }
        }
    }

    /**
     * Applies the "reconfigurable" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ReconfigureApplicator extends ActionModeSettingApplicator {

        private ReconfigureApplicator() {
            super(ConfigurableActionSettings.Reconfigure);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final ActionMode actionMode) {
            // when setting value is DISABLED, set reconfigurable to false.
            // otherwise make sure reconfigurable is set on analysis settings.
            if (ActionMode.DISABLED == actionMode) {
                entity.getAnalysisSettingsBuilder().setReconfigurable(false);
                logger.trace("Disabled reconfigure for {}::{}",
                            entity::getEntityType, entity::getDisplayName);
            } else if (!entity.getAnalysisSettingsBuilder().hasReconfigurable()) {
                entity.getAnalysisSettingsBuilder().setReconfigurable(true);
            }
        }
    }

    /**
     * Adds the "scaling" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ScalingApplicator extends ActionModeSettingApplicator {

        private ScalingApplicator(ConfigurableActionSettings setting) {
            super(setting);
        }
        @Override
        protected void apply(@Nonnull final Builder entity, @Nonnull final ActionMode actionMode) {
            List<CommoditiesBoughtFromProvider.Builder> commBoughtGroupingList = entity
                    .getCommoditiesBoughtFromProvidersBuilderList().stream()
                    .filter(s -> s.getProviderEntityType() == EntityType.COMPUTE_TIER_VALUE ||
                            s.getProviderEntityType() == EntityType.DATABASE_SERVER_TIER_VALUE
                    || s.getProviderEntityType() == EntityType.DATABASE_TIER_VALUE).collect(Collectors.toList());
            for (CommoditiesBoughtFromProvider.Builder commBought : commBoughtGroupingList) {
                if (ActionMode.DISABLED == actionMode) {
                    commBought.setScalable(false);
                }
            }
        }
    }

    /**
     * Applies the "resize" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ResizeApplicator extends ActionModeSettingApplicator {

        private ResizeApplicator() {
            super(ConfigurableActionSettings.Resize);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final ActionMode actionMode) {
            final boolean resizeable = ActionMode.DISABLED != actionMode;
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
     * Applies the "rateOfResize" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class RateOfResizeApplicator extends SingleSettingApplicator {

        private RateOfResizeApplicator() {
            super(EntitySettingSpecs.RateOfResize);
        }

        @Override
        protected void apply(@Nonnull final Builder entity, @Nonnull final Setting setting) {
            entity.getAnalysisSettingsBuilder().setRateOfResize(setting.getNumericSettingValue().getValue());
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
     * Applies minReplicas and maxReplicas settings to entity.
     */
    private static class MinMaxReplicasApplicator extends BaseSettingApplicator {

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder application,
                          @Nonnull Map<EntitySettingSpecs, Setting> settings,
                          @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
            if (application.getEntityType() != EntityType.APPLICATION_COMPONENT_VALUE) {
                return;
            }
            final double minReplicasMin = EntitySettingSpecs.MinReplicas.getNumericMin();
            final double minReplicasMax = EntitySettingSpecs.MinReplicas.getNumericMax();
            final double minReplicasDefault = EntitySettingSpecs.MinReplicas.getNumericDefault();
            final double maxReplicasMin = EntitySettingSpecs.MaxReplicas.getNumericMin();
            final double maxReplicasMax = EntitySettingSpecs.MaxReplicas.getNumericMax();
            final double maxReplicasDefault = EntitySettingSpecs.MaxReplicas.getNumericDefault();

            double minReplicas = getNumericSetting(settings, EntitySettingSpecs.MinReplicas);
            double maxReplicas = getNumericSetting(settings, EntitySettingSpecs.MaxReplicas);
            if (minReplicas < minReplicasMin
                    || maxReplicas < maxReplicasMin
                    || minReplicas > minReplicasMax
                    || maxReplicas > maxReplicasMax) {
                logger.error("Invalid value set for minReplicas: {} or maxReplicas: {} for"
                                + " entity {}. Valid values for minReplicas: {} to {}. Valid values"
                                + " for maxReplicas: {} to {}. Using default values {} and {}.",
                        minReplicas, maxReplicas, application.getDisplayName(),
                        minReplicasMin, minReplicasMax, maxReplicasMin, maxReplicasMax,
                        minReplicasDefault, maxReplicasDefault);
                minReplicas = minReplicasDefault;
                maxReplicas = maxReplicasDefault;
            } else if (minReplicas > maxReplicas) {
                logger.error("Invalid value set for minReplicas: {} or maxReplicas: {} for"
                                + " entity {}. minReplicas must be less than or equal to maxReplicas."
                                + " Using default values {} and {}.",
                        minReplicas, maxReplicas, application.getDisplayName(),
                        minReplicasDefault, maxReplicasDefault);
                minReplicas = minReplicasDefault;
                maxReplicas = maxReplicasDefault;
            }
            application.getAnalysisSettingsBuilder()
                    .setMinReplicas((int)minReplicas)
                    .setMaxReplicas((int)maxReplicas);
        }
    }

    /**
     * Applies a Vcpu resize to the virtual machine represented in
     * {@link TopologyEntityDTO.Builder}.
     */
    private static class VirtualMachineResizeVcpuApplicator extends MultipleSettingsApplicator {

        private VirtualMachineResizeVcpuApplicator() {
            super(Collections.emptyList(),
                Arrays.asList(ConfigurableActionSettings.ResizeVcpuBelowMinThreshold,
                    ConfigurableActionSettings.ResizeVcpuDownInBetweenThresholds,
                    ConfigurableActionSettings.ResizeVcpuUpInBetweenThresholds,
                    ConfigurableActionSettings.ResizeVcpuAboveMaxThreshold));
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                             @Nonnull final Collection<Setting> settings) {
            boolean allSettingsDisabled = settings.stream()
                    .filter(setting -> setting.getEnumSettingValue().getValue().equals(ActionMode.DISABLED.name()))
                    .collect(Collectors.toList()).size() == this.getConfigurableActionSettings().size();
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
            super(Collections.emptyList(),
                Arrays.asList(ConfigurableActionSettings.ResizeVmemBelowMinThreshold,
                ConfigurableActionSettings.ResizeVmemDownInBetweenThresholds,
                ConfigurableActionSettings.ResizeVmemUpInBetweenThresholds,
                ConfigurableActionSettings.ResizeVmemAboveMaxThreshold));
        }

        @Override
        protected void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                             @Nonnull final Collection<Setting> settings) {
            boolean allSettingsDisabled = settings.stream()
                    .filter(setting -> setting.getEnumSettingValue().getValue()
                        .equals(ActionMode.DISABLED.name())).count() == this.getConfigurableActionSettings().size();
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
            for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity,
                    commodityType)) {
                commodity.setEffectiveCapacityPercentage(settingValue);
            }
        }
    }

    /**
     * Applies the CPU Ready Queue Utilization threshold setting to all the relevant commodities.
     */
    @ThreadSafe
    private static class ReadyQueueUtilizationThresholdApplicator extends SingleSettingApplicator {

        private static final Set<Integer> allQueueCommodities = ImmutableSet.of(
                CommodityType.Q1_VCPU_VALUE, CommodityType.Q2_VCPU_VALUE,
                CommodityType.Q3_VCPU_VALUE, CommodityType.Q4_VCPU_VALUE,
                CommodityType.Q5_VCPU_VALUE, CommodityType.Q6_VCPU_VALUE,
                CommodityType.Q7_VCPU_VALUE, CommodityType.Q8_VCPU_VALUE,
                CommodityType.Q16_VCPU_VALUE, CommodityType.Q32_VCPU_VALUE,
                CommodityType.Q64_VCPU_VALUE, CommodityType.QN_VCPU_VALUE,
                CommodityType.CPU_READY_VALUE);

        private ReadyQueueUtilizationThresholdApplicator() {
            super(EntitySettingSpecs.ReadyQueueUtilization);
        }

        @Override
        protected void apply(@NotNull Builder entity, @NotNull Setting setting) {
            final float settingValue = setting.getNumericSettingValue().getValue();
            entity.getCommoditySoldListBuilderList().stream()
                    .filter(commodity -> allQueueCommodities.contains(commodity.getCommodityType().getType()))
                    .forEach(commodity -> commodity.setEffectiveCapacityPercentage(settingValue));
        }
    }

    /**
     * HA related commodities applicator. This applicator will process cpu/mem utilization
     * threshold. Both of the commodities are calculated on top of appropriate settings.
     */
    @ThreadSafe
    private static class HaDependentUtilizationApplicator extends BaseSettingApplicator {

        private final TopologyInfo topologyInfo;

        private final boolean considerUtilizationConstraintInClusterHeadroomPlan;

        private HaDependentUtilizationApplicator(@Nonnull final TopologyInfo topologyInfo,
                                                 final boolean considerUtilizationConstraintInClusterHeadroomPlan) {
            this.topologyInfo = Objects.requireNonNull(topologyInfo);
            this.considerUtilizationConstraintInClusterHeadroomPlan = considerUtilizationConstraintInClusterHeadroomPlan;
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                @Nonnull final Map<EntitySettingSpecs, Setting> settings,
                @Nonnull final Map<ConfigurableActionSettings, Setting> actionModeSettings) {
            if (topologyInfo.getPlanInfo().getPlanProjectType() != PlanProjectType.CLUSTER_HEADROOM
                    || considerUtilizationConstraintInClusterHeadroomPlan) {
                final Setting cpuUtilSetting = settings.get(EntitySettingSpecs.CpuUtilization);
                final Setting memUtilSetting = settings.get(EntitySettingSpecs.MemoryUtilization);
                applyUtilizationChanges(entity, CommodityType.CPU, cpuUtilSetting);
                applyUtilizationChanges(entity, CommodityType.MEM, memUtilSetting);
            }

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
            }
        }

        private void applyMaxUtilizationToCapacity(@Nonnull final TopologyEntityDTO.Builder entity,
                                                   @Nonnull final CommodityType commodityType,
                                                   final float maxDesiredUtilization) {
            // We only want to do this for cluster headroom calculations.
            Preconditions.checkArgument(topologyInfo.getPlanInfo().getPlanProjectType() ==
                    PlanProjectType.CLUSTER_HEADROOM);
            for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity,
                    commodityType)) {
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
            for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity,
                    commodityType)) {
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
     * Applies min/Max to the entity represented in {@link TopologyEntityDTO.Builder}, like VM or
     * container.
     */
    private static class EntityThresholdApplicator extends MultipleSettingsApplicator {

        //VMem setting value is in MBs. Market expects it in KBs.
        private final float conversionFactor = 1024.0f;
        private final Map<Integer, Set<EntitySettingSpecs>> entitySettingMapping;
        private final Map<Integer, Set<ConfigurableActionSettings>> actionModeSettingsMapping;
        // Map from VM OID to cpu speed per millicore. This is to cache the data per VM when setting
        // threshold for containers because multiple containers can be hosted on the same VM.
        private final Map<Long, Double> vmToCPUSpeedPerMillicoreMap;
        // We need the graph to fin the core CPU speed of the hosting PM for the unit conversion.
        private final TopologyGraph<TopologyEntity> graph;

        private EntityThresholdApplicator(@Nonnull final Map<Integer, Set<EntitySettingSpecs>> settingMapping,
                                          @Nonnull final Map<Integer, Set<ConfigurableActionSettings>> actionModeSettingsMapping,
                                          @Nonnull final GraphWithSettings settingsGraph) {
            super(settingMapping.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()),
                actionModeSettingsMapping.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList()));
            this.graph = settingsGraph.getTopologyGraph();
            this.entitySettingMapping = Objects.requireNonNull(settingMapping);
            this.actionModeSettingsMapping = Objects.requireNonNull(actionModeSettingsMapping);
            this.vmToCPUSpeedPerMillicoreMap = new HashMap<>();
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
                        Set<ConfigurableActionSettings> validActionModeSettings = actionModeSettingsMapping.get(commodityBuilder
                            .getCommodityType().getType());
                        if (commodityBuilder.getCommodityType().getType() == CommodityType.VCPU.getNumber()) {
                            // Gets the CPU speed of the PM.
                            final Optional<Integer> cpuSpeed = graph.getProviders(entity.getOid())
                                    .filter(provider -> provider.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
                                    .filter(provider -> provider.getTypeSpecificInfo().getPhysicalMachine().hasCpuCoreMhz())
                                    .map(pm -> pm.getTypeSpecificInfo().getPhysicalMachine().getCpuCoreMhz())
                                    .findFirst();
                            if (cpuSpeed.isPresent()) {
                                setThresholds(entity, validSpecs, validActionModeSettings, settings, commodityBuilder, cpuSpeed.get());
                            } else if (entity.getTypeSpecificInfo().getVirtualMachine().hasNumCpus()) {
                                double multiplier = commodityBuilder.getCapacity()
                                        / entity.getTypeSpecificInfo().getVirtualMachine().getNumCpus();
                                setThresholds(entity, validSpecs, validActionModeSettings, settings, commodityBuilder, multiplier);
                            } else {
                                logger.error("VCPU threshold applicator couldn't find the CPU speed " +
                                        "of the host (VM: {})", entity.getDisplayName());
                            }
                        } else if (commodityBuilder.getCommodityType().getType() == CommodityType.VMEM.getNumber()) {
                            setThresholds(entity, validSpecs, validActionModeSettings, settings, commodityBuilder, conversionFactor);
                        }
                });
            } else if (EntityType.CONTAINER_VALUE == entity.getEntityType()) {
                setThresholdsForContainer(entity, settings);
            }
        }

        private void setThresholdsForContainer(@Nonnull final TopologyEntityDTO.Builder entity,
                                               @Nonnull final Collection<Setting> settings) {
            entity.getCommoditySoldListBuilderList().stream()
                .filter(commodity -> entitySettingMapping.get(commodity.getCommodityType().getType()) != null)
                .forEach(commodity -> {
                    Set<EntitySettingSpecs> validSpecs = entitySettingMapping.get(commodity.getCommodityType().getType());
                    Set<ConfigurableActionSettings> validActionModeSettings = actionModeSettingsMapping.get(commodity.getCommodityType().getType());
                    if (CPU_COMMODITY_TYPES.contains(commodity.getCommodityType().getType())) {
                        // As container VCPU/VCPURequest commodities capacity values are in millicores
                        // sent from kubeturbo and given CPU related setting values are in millicores
                        // as well, we don't need to translate setting values and just use multiplier
                        // as 1.
                        setThresholds(entity, validSpecs, validActionModeSettings, settings, commodity, 1);
                    } else if (MEM_COMMODITY_TYPES.contains(commodity.getCommodityType().getType())) {
                        setThresholds(entity, validSpecs, validActionModeSettings, settings, commodity, conversionFactor);
                    }
                });
        }

        /**
         * Iterates over settings for a VM. Retrives its resize modes in different intervals.
         * Based on the modes and where the current commodity capacity stands wrt Min and Max, we populate the thresholds.
         *
         * @param entity being processed.
         * @param validSpecs to consider.
         * @param validActionModeSettings to consider.
         * @param settings associated with the entity.
         * @param commodityBuilder is the commodity being processed.
         * @param multiplier multiplicand used along with the thresholds.
         */
        protected void setThresholds(@Nonnull final TopologyEntityDTO.Builder entity,
                                     Set<EntitySettingSpecs> validSpecs,
                                     Set<ConfigurableActionSettings> validActionModeSettings,
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
                final Optional<EntitySettingSpecs> policySetting =
                        EntitySettingSpecs.getSettingByName(setting.getSettingSpecName());
                if (policySetting.isPresent() && validSpecs.contains(policySetting.get())) {
                    final float settingValue = setting.getNumericSettingValue().getValue();
                    switch (policySetting.get()) {
                        case ResizeVcpuMinThreshold:
                        case ResizeVmemMinThreshold:
                        case ResizeVcpuLimitMinThreshold:
                        case ResizeVcpuRequestMinThreshold:
                        case ResizeVmemLimitMinThreshold:
                        case ResizeVmemRequestMinThreshold:
                            minThreshold = settingValue * multiplier;
                            break;
                        case ResizeVcpuMaxThreshold:
                        case ResizeVmemMaxThreshold:
                        case ResizeVcpuLimitMaxThreshold:
                        case ResizeVmemLimitMaxThreshold:
                            maxThreshold = settingValue * multiplier;
                            break;
                    }
                }
                if (ActionSettingSpecs.isActionModeSetting(setting.getSettingSpecName())) {
                    final ConfigurableActionSettings configurableActionSettings =
                        ConfigurableActionSettings.fromSettingName(setting.getSettingSpecName());
                    if (configurableActionSettings != null && validActionModeSettings.contains(configurableActionSettings)) {
                        switch (configurableActionSettings) {
                            case ResizeVcpuBelowMinThreshold:
                            case ResizeVmemBelowMinThreshold:
                            case ResizeVcpuLimitBelowMinThreshold:
                            case ResizeVcpuRequestBelowMinThreshold:
                            case ResizeVmemLimitBelowMinThreshold:
                            case ResizeVmemRequestBelowMinThreshold:
                                modeForMin = ActionMode.valueOf(setting.getEnumSettingValue().getValue());
                                break;
                            case ResizeVcpuAboveMaxThreshold:
                            case ResizeVmemAboveMaxThreshold:
                            case ResizeVcpuLimitAboveMaxThreshold:
                            case ResizeVmemLimitAboveMaxThreshold:
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
                            case Resize:
                                modeUpInBetweenThresholds = ActionMode.valueOf(setting.getEnumSettingValue().getValue());
                                modeDownInBetweenThresholds = ActionMode.valueOf(setting.getEnumSettingValue().getValue());
                                break;
                        }
                    }
                }
            }
            if (modeForMax == null || modeForMin == null || modeDownInBetweenThresholds == null || modeUpInBetweenThresholds == null) {
                logger.error("Unable to set capacity bounds based on policy for {} on {}", entity.getDisplayName(),
                        CommodityType.forNumber(commodityBuilder.getCommodityType().getType()));
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
                EntityType.STORAGE_VALUE,
                EntityType.APPLICATION_COMPONENT_VALUE,
                EntityType.DATABASE_SERVER_VALUE
            );

        private final CommodityType commodityType;

        // convert into units that market uses.
        private final ImmutableMap<Integer, Float> conversionFactor =
            ImmutableMap.<Integer, Float>builder()
                //VMEM setting value is in MBs. Market expects it in KBs.
                .put(CommodityType.VMEM_VALUE, 1024.0f)
                //VMEM_REQUEST setting value is in MBs. Market expects it in KBs.
                .put(CommodityType.VMEM_REQUEST_VALUE, 1024.0f)
                //VSTORAGE setting value is in GBs. Market expects it in MBs.
                .put(CommodityType.VSTORAGE_VALUE, 1024.0f)
                //STORAGE_AMOUNT setting value is in GBs. Market expects it in MBs.
                .put(CommodityType.STORAGE_AMOUNT_VALUE, 1024.0f)
                //HEAP setting value is in MBs. Market expects it in KBs.
                .put(CommodityType.HEAP_VALUE, 1024.0f)
                //DB_MEM setting value is in MBs. Market expects it in KBs.
                .put(CommodityType.DB_MEM_VALUE, 1024.0f)
                .build();

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
     * Applicator for capacity resize increment settings.
     * If the CoreSocketRatioMode for this VM is IGNORE, the VM's VCPU sold->capacityIncrement will be from VmVcpuIncrement setting.
     * If the CoreSocketRatioMode for this VM is RESPECT, the VM's VCPU sold->capacityIncrement will be the nearest multiple of current capacityIncrement.
     * Eg, current capacityIncrement = 5200, VmVcpuIncrement = 1800, the final capacityIncrement will be 5200.
     * for Virtual Machine entities.
     *
     */
    @ThreadSafe
    private static class VMCPUIncrementApplicator extends BaseSettingApplicator {
        TopologyGraph<TopologyEntity> topologyGraph;

        public VMCPUIncrementApplicator(TopologyGraph<TopologyEntity> topologyGraph) {
            this.topologyGraph = topologyGraph;
        }

        private static MhzHandler mhzHandler = new MhzHandler();
        private static final Map<VCPUScalingUnitsEnum, VCPUScalingModeHandler>
                vcpuScalingModeToHandler = ImmutableMap.of(
                VCPUScalingUnitsEnum.MHZ, mhzHandler,
                VCPUScalingUnitsEnum.CORES, new CoresHandler(),
                VCPUScalingUnitsEnum.SOCKETS, new SocketsHandler());

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
            if (entity.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
                return;
            }
            Optional<CommoditySoldDTO.Builder> vcpuCommodityBuilderOptional =
                    entity.getCommoditySoldListBuilderList().stream().filter(
                            commodity -> commodity.getCommodityType().getType()
                                    == CommodityType.VCPU_VALUE).findFirst();
            if (vcpuCommodityBuilderOptional.isPresent()) {
                final CommoditySoldDTO.Builder vcpuCommodityBuilder =
                        vcpuCommodityBuilderOptional.get();
                final VCPUScalingUnitsEnum vcpuScalingUnitsType = getSettingValue(entitySettings,
                        EntitySettingSpecs.VcpuScalingUnits, VCPUScalingUnitsEnum.MHZ);
                float vcpuIncrement = vcpuScalingModeToHandler.getOrDefault(vcpuScalingUnitsType, mhzHandler).getVcpuIncrement(
                        entity, topologyGraph, entitySettings, vcpuCommodityBuilder);
                if (logger.isTraceEnabled()) {
                    logger.trace("CSR mode for {} is {}, resulting capacity increment={}",
                            toString(entity), vcpuScalingUnitsType, vcpuIncrement);
                }
                vcpuCommodityBuilder.setCapacityIncrement(vcpuIncrement);
            }
        }

        private static String toString(@Nonnull Builder entity) {
            return String.format("%s[%s]", entity.getDisplayName(), entity.getOid());
        }

        /**
         * Interface for handling VcpuScalingUnit modes.
         * Each mode has a different way to process vcpu increments.
         *
         */
        private interface VCPUScalingModeHandler {
            float getVcpuIncrement(TopologyEntityDTO.Builder entity,
                    TopologyGraph<TopologyEntity> topologyGraph,
                    Map<EntitySettingSpecs, Setting> entitySettings,
                    CommoditySoldDTO.Builder vcpuCommodityBuilder);
        }

        /**
         * Implementation of handling MHZ case.
         * In this setting, it will only get the increment mhz and scale using that.
         *
         */
        private static class MhzHandler implements VCPUScalingModeHandler {
            @Override
            public float getVcpuIncrement(TopologyEntityDTO.Builder entity,
                    TopologyGraph<TopologyEntity> topologyGraph,
                    Map<EntitySettingSpecs, Setting> entitySettings,
                    CommoditySoldDTO.Builder vcpuCommodityBuilder) {
                return (int)getNumericSetting(entitySettings, EntitySettingSpecs.VmVcpuIncrement,
                        logger);
            }
        }

        /**
         * Implementation of CORES case.
         * This setting has multiple subsettings which will behave differently.
         * Based on PRESERVE, MATCH HOST, or USER SPECIFIED subsetting, vcpu increment will differ
         *
         */
        private static class CoresHandler extends MhzHandler {
            private static final Map<VcpuScalingCoresPerSocketSocketModeEnum, ModeHandler>
                    MODE_TO_HANDLER =
                    ImmutableMap.of(VcpuScalingCoresPerSocketSocketModeEnum.PRESERVE,
                            new HandlePreserve(),
                            VcpuScalingCoresPerSocketSocketModeEnum.MATCH_HOST,
                            new HandleMatchHost(),
                            VcpuScalingCoresPerSocketSocketModeEnum.USER_SPECIFIED,
                            (entity, topologyGraph, entitySettings, numCpus, cpsr) -> (int)getNumericSetting(
                                    entitySettings,
                                    EntitySettingSpecs.VcpuScaling_CoresPerSocket_SocketValue,
                                    logger));

            /**
             * Interface for handling sub-settings for CORES mode
             * Each sub-setting has a different way of calculating increment sockets.
             *
             */
            @FunctionalInterface
            private interface ModeHandler {
                int getIncrementSockets(TopologyEntityDTO.Builder entity,
                        TopologyGraph<TopologyEntity> topologyGraph,
                        Map<EntitySettingSpecs, Setting> entitySettings, Integer numCpus,
                        Integer cpsr);
            }

            /**
             * Implementation of MATCH HOST option.
             * This option will retrieve the number of sockets of the physical machine.
             * Incrementing will be done using the new socket count.
             *
             */
            private static class HandleMatchHost implements ModeHandler {
                @Override
                public int getIncrementSockets(TopologyEntityDTO.Builder entity,
                        TopologyGraph<TopologyEntity> topologyGraph,
                        Map<EntitySettingSpecs, Setting> entitySettings, Integer numCpus,
                        Integer cpsr) {
                    CommoditiesBoughtFromProvider commdityBoughtFromPM =
                            entity.getCommoditiesBoughtFromProvidersList()
                                    .stream()
                                    .filter(e -> e.getProviderEntityType()
                                            == EntityType.PHYSICAL_MACHINE_VALUE)
                                    .findFirst()
                                    .orElse(null);
                    if (commdityBoughtFromPM == null) {
                        logger.warn(
                                "There is no commodity bought from provider for physical machine entity type in the virtual machine {}",
                                entity.getDisplayName());
                        return 1;
                    }
                    TopologyEntity pm = topologyGraph.getEntity(commdityBoughtFromPM.getProviderId())
                            .orElse(null);
                    if (pm == null) {
                        logger.warn(
                                "There is no physical machine in the Topology that matches the given oid {}",
                                commdityBoughtFromPM.getProviderId());
                        return 1;
                    }
                    TopologyEntityDTO.Builder pmEntity = pm.getTopologyEntityDtoBuilder();
                    if (!pmEntity.hasTypeSpecificInfo()) {
                        logger.warn("There is no TypeSpecificInfo for the given for PM {}",
                                pm.getDisplayName());
                        return 1;
                    }
                    final TopologyDTO.TypeSpecificInfo typeSpecificInfo = pmEntity.getTypeSpecificInfo();
                    if (!typeSpecificInfo.hasPhysicalMachine()) {
                        logger.warn("There is no PhysicalMachineInfo for the given for PM {}",
                                pm.getDisplayName());
                        return 1;
                    }
                    final PhysicalMachineInfo pmInfo = typeSpecificInfo.getPhysicalMachine();
                    if (pmInfo.hasNumCpuSockets()) {
                        return pmInfo.getNumCpuSockets();
                    }
                    logger.warn("There is no NumCpuSockets for the given PM {}", pm.getDisplayName());
                    return 1;
                }
            }

            /**
             * Implementation of PRESERVE option
             * This option will keep the current VM socket value and increment using that.
             *
             */
            private static class HandlePreserve implements ModeHandler {
                @Override
                public int getIncrementSockets(TopologyEntityDTO.Builder entity,
                        TopologyGraph<TopologyEntity> topologyGraph,
                        Map<EntitySettingSpecs, Setting> entitySettings, Integer numCpus,
                        Integer cpsr) {
                    if (numCpus != null && cpsr != null && cpsr > 0) {
                        return numCpus / cpsr;
                    } else {
                        return 1;
                    }
                }
            }

            @Override
            public float getVcpuIncrement(TopologyEntityDTO.Builder entity,
                    TopologyGraph<TopologyEntity> topologyGraph,
                    Map<EntitySettingSpecs, Setting> entitySettings,
                    CommoditySoldDTO.Builder vcpuCommodityBuilder) {
                final Integer numCpus = getVmInfoParameter(entity, VirtualMachineInfo::hasNumCpus,
                        VirtualMachineInfo::getNumCpus);
                final Integer cpsr = getVmInfoParameter(entity,
                        VirtualMachineInfo::hasCoresPerSocketRatio,
                        VirtualMachineInfo::getCoresPerSocketRatio);

                final VcpuScalingCoresPerSocketSocketModeEnum coresPerSocketModeType =
                        getSettingValue(entitySettings,
                                EntitySettingSpecs.VcpuScaling_CoresPerSocket_SocketMode,
                                VcpuScalingCoresPerSocketSocketModeEnum.PRESERVE);

                final ModeHandler modeHandler = MODE_TO_HANDLER.get(coresPerSocketModeType);
                final int sockets;
                if (modeHandler == null) {
                    sockets = 1;
                } else {
                    sockets = modeHandler.getIncrementSockets(entity,
                            topologyGraph, entitySettings, numCpus, cpsr);
                    entity.getTypeSpecificInfoBuilder().getVirtualMachineBuilder()
                            .getCpuScalingPolicyBuilder().setSockets(sockets);
                }
                if (numCpus != null && numCpus > 0) {
                    return Math.round(vcpuCommodityBuilder.getCapacity() / numCpus)
                            * sockets;
                } else {
                    return super.getVcpuIncrement(entity, topologyGraph, entitySettings,
                            vcpuCommodityBuilder);
                }
            }
        }

        /**
         * Implementation of SOCKETS case.
         * Yet to be implemented. Base functionality for now.
         * If capacity increment is sent from probe, that will be used for increment.
         * Otherwise the speed per core of the VM will be used for increment.
         * Else this case will be handled the same way as MHZ mode - Will change in future sprint
         *
         */
        private static class SocketsHandler extends MhzHandler {
            @Override
            public float getVcpuIncrement(TopologyEntityDTO.Builder entity,
                    TopologyGraph<TopologyEntity> topologyGraph,
                    Map<EntitySettingSpecs, Setting> entitySettings,
                    CommoditySoldDTO.Builder vcpuCommodityBuilder) {
                final Integer numCpus = getVmInfoParameter(entity, VirtualMachineInfo::hasNumCpus,
                        VirtualMachineInfo::getNumCpus);
                if (vcpuCommodityBuilder.hasCapacityIncrement()) {
                    return vcpuCommodityBuilder.getCapacityIncrement();
                } else if (numCpus != null && numCpus > 0) {
                    return Math.round(vcpuCommodityBuilder.getCapacity() / numCpus);
                } else {
                    return super.getVcpuIncrement(entity, topologyGraph, entitySettings,
                            vcpuCommodityBuilder);
                }
            }
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
        /**
         * The Gcp IO Throughput commodities.
         */
        private static Set<Integer> IOThroughPutCommodities = ImmutableSet.of(CommodityType.IO_THROUGHPUT_READ_VALUE,
                CommodityType.IO_THROUGHPUT_WRITE_VALUE);
        /**
         * The Gcp IOPS commodities.
         */
        private static Set<Integer> IOPSCommodities = ImmutableSet.of(CommodityType.STORAGE_ACCESS_SSD_READ_VALUE,
                CommodityType.STORAGE_ACCESS_SSD_WRITE_VALUE, CommodityType.STORAGE_ACCESS_STANDARD_READ_VALUE,
                CommodityType.STORAGE_ACCESS_STANDARD_WRITE_VALUE);


        private ResizeTargetUtilizationCommodityBoughtApplicator(
                @Nonnull EntitySettingSpecs setting, @Nonnull final CommodityType commodityType) {
            super(setting, commodityType);
        }

        /**
         * When a Gcp IOPS or IO throughput commodity bought is given, convert the type to
         * IO_THROUGHPUT_VALUE or STORAGE_ACCESS_VALUE so that the resize target utilization
         * can be propagated down.
         *
         * @param origType the original commodity type.
         * @return converted commodity type.
         */
        private int convertRelevantResizableCommodityBought(int origType) {
            if (IOThroughPutCommodities.contains(origType)) {
                return CommodityType.IO_THROUGHPUT_VALUE;
            }
            if (IOPSCommodities.contains(origType)) {
                return CommodityType.STORAGE_ACCESS_VALUE;
            }
            return origType;
        }

        @Override
        protected void apply(@Nonnull TopologyEntityDTO.Builder entity,
                double resizeTargetUtilization) {
            entity.getCommoditiesBoughtFromProvidersBuilderList()
                    .stream()
                    .map(CommoditiesBoughtFromProvider.Builder::getCommodityBoughtBuilderList)
                    .flatMap(Collection::stream)
                    .filter(commodity -> convertRelevantResizableCommodityBought(commodity.getCommodityType().getType())
                            == getCommodityType().getNumber())
                    .forEach(commodityBuilder -> {
                        commodityBuilder.setResizeTargetUtilization(resizeTargetUtilization);
                        logger.debug(APPLY_RESIZE_TARGET_UTILIZATION_MESSAGE,
                                entity.getDisplayName(), getCommodityType().getNumber(),
                                commodityBuilder.getResizeTargetUtilization());
                    });
        }
    }

    /**
     * Base Applicator for the capacity setting.
     * Sets capacity for {@link CommoditySoldDTO}.
     */
    private abstract static class BaseOverrideCapacityApplicator extends SingleSettingApplicator {

        private final CommodityType commodityType;

        private BaseOverrideCapacityApplicator(@Nonnull EntitySettingSpecs setting,
                                               @Nonnull final CommodityType commodityType) {
            super(setting);
            this.commodityType = Objects.requireNonNull(commodityType);
        }

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull Setting setting) {
            final Optional<Float> settingValueOpt = getSettingValue(entity, setting);
            if (!settingValueOpt.isPresent()) {
                return;
            }
            float settingValue = settingValueOpt.get().floatValue();
            for (CommoditySoldDTO.Builder commodity : getCommoditySoldBuilders(entity,
                    commodityType)) {
                commodity.setCapacity(settingValue);
            }
        }

        protected abstract Optional<Float> getSettingValue(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull Setting setting);
    }

    /**
     * Applicator for the capacity setting.
     * Sets capacity for {@link CommoditySoldDTO}.
     */
    @ThreadSafe
    private static class OverrideCapacityApplicator extends BaseOverrideCapacityApplicator {

        private OverrideCapacityApplicator(@Nonnull EntitySettingSpecs setting,
                        @Nonnull final CommodityType commodityType) {
            super(setting, commodityType);
        }

        @Override
        protected Optional<Float> getSettingValue(@Nonnull TopologyEntityDTO.Builder entity, @Nonnull Setting setting) {
            return Optional.ofNullable(getEntitySettingSpecs().getValue(setting, Float.class));
        }
    }

    /**
     * Applicator for the user capacity setting.
     * Sets capacity for {@link CommoditySoldDTO} if there is user setting with new value.
     */
    @ThreadSafe
    private static class OverrideCapacityByUserApplicator extends BaseOverrideCapacityApplicator {
        private final Map<Long, EntitySettings> settingsByEntity;

        private OverrideCapacityByUserApplicator(@Nonnull EntitySettingSpecs setting,
                        @Nonnull final CommodityType commodityType,
                        @Nonnull final GraphWithSettings graphWithSettings) {
            super(setting, commodityType);
            this.settingsByEntity = Collections.unmodifiableMap(new HashMap<>(graphWithSettings
                            .getSettingsByEntity()));
        }

        @Override
        protected Optional<Float> getSettingValue(@Nonnull TopologyEntityDTO.Builder entity,
                                                 @Nonnull Setting setting) {
            final EntitySettings settingsForEntity = settingsByEntity.get(entity.getOid());
            if (settingsForEntity == null) {
                return Optional.empty();
            }
            final String settingName = setting.getSettingSpecName();
            final Optional<Setting> userSettingOpt = settingsForEntity.getUserSettingsList()
                            .stream()
                            .map(SettingToPolicyId::getSetting)
                            .filter(s -> s.getSettingSpecName().equals(settingName))
                            .findFirst();
            return userSettingOpt.map(seting -> setting.getNumericSettingValue().getValue());
        }

    }

    /**
     * Applicator to apply resize or horizontal scale policy on Application Components when
     * SERVICE_HORIZONTAL_SCALE feature is enabled.
     * Resize or horizontal scale policies are applied to Application Components only, and are
     * mutually exclusive.
     * We check the Horizontal Scale Up and Horizontal Scale Down setting. When both settings are
     * disabled, then horizontal scale is disabled, and resize is enabled. Otherwise, horizontal
     * scale is enabled, and resize is disabled.
     */
    private static class HorizontalScalePolicyApplicator extends BaseSettingApplicator {

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                          @Nullable final Map<EntitySettingSpecs, Setting> entitySettings,
                          @Nullable final Map<ConfigurableActionSettings, Setting> actionModeSettings) {
            if (entity.getEntityType() != EntityType.APPLICATION_COMPONENT_VALUE) {
                return;
            }
            final boolean cloneable = isActionEnabled(
                    actionModeSettings, ConfigurableActionSettings.HorizontalScaleUp);
            final boolean suspendable = isActionEnabled(
                    actionModeSettings, ConfigurableActionSettings.HorizontalScaleDown);
            // Explicitly set cloneable or suspendable to false only when they are disabled in
            // the policy setting. Otherwise, don't update the cloneable or suspendable setting,
            // and leave the way they are set by the probe
            if (!cloneable) {
                entity.getAnalysisSettingsBuilder().setCloneable(false);
            }
            if (!suspendable) {
                entity.getAnalysisSettingsBuilder().setSuspendable(false);
            }
            // Disable resize explicitly when horizontal scale is enabled
            if (cloneable || suspendable) {
                entity.getCommoditySoldListBuilderList()
                        .forEach(c -> c.setIsResizeable(false));
            }
        }

        /**
         * Check if an action setting exists and is enabled.
         *
         * @param actionModeSettings the action settings on the entity
         * @param actionModeSpec the action to check
         * @return true if an action setting exists and is enabled
         */
        private boolean isActionEnabled(
                @Nullable final Map<ConfigurableActionSettings, Setting> actionModeSettings,
                @Nonnull final ConfigurableActionSettings actionModeSpec) {
            return Optional.ofNullable(actionModeSettings)
                    .map(map -> map.get(actionModeSpec))
                    .map(SettingDTOUtil::isActionEnabled)
                    .orElse(false);
        }
    }

    /**
     * Applies the "ScalingPolicy" setting to a {@link TopologyEntityDTO.Builder}.
     */
    private static class ScalingPolicyApplicator extends SingleSettingApplicator {

        private ScalingPolicyApplicator() {
            super(EntitySettingSpecs.ScalingPolicy);
        }

        @Override
        public void apply(@Nonnull final TopologyEntityDTO.Builder entity,
                          @Nonnull final Setting setting) {
            if (entity.getEntityType() == EntityType.APPLICATION_COMPONENT_VALUE) {
                final String settingValue = setting.getEnumSettingValue().getValue();
                boolean resizeScaling = ScalingPolicyEnum.RESIZE.name().equals(settingValue);
                boolean horizontalScaling = ScalingPolicyEnum.HORIZONTAL_SCALE.name().equals(settingValue);

                if (!resizeScaling && !horizontalScaling) {
                    logger.error("Entity {} has an invalid scaling policy: {}",
                            entity.getDisplayName(), settingValue);
                    return;
                }
                // Explicitly disable provision and suspension only when the scaling policy is Resize
                // Otherwise (i.e., the scaling policy is Horizontal Scale), leave the provision and
                // suspension setting the way they were set by the probe.
                if (!horizontalScaling) {
                    entity.getAnalysisSettingsBuilder().setCloneable(false);
                    entity.getAnalysisSettingsBuilder().setSuspendable(false);
                }
                // If resize scaling then leave isResizeable the way it was set by the probe,
                // otherwise set to false (no resize).
                if (!resizeScaling) {
                    entity.getCommoditySoldListBuilderList().forEach(c ->
                        c.setIsResizeable(false)
                    );
                }
                logger.trace("Set scaling policy {} for entity {}",
                        settingValue, entity.getDisplayName());
            }
        }
    }

    /**
     * Applicator for "Enable Scale Actions" setting.
     */
    private static class EnableScaleApplicator extends BaseSettingApplicator {

        private static final EntitySettingSpecs settingSpec = EntitySettingSpecs.EnableScaleActions;

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                          @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                          @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
            final Setting setting = entitySettings.get(settingSpec);
            if (setting != null) {
                final EntityType entityType = EntityType.forNumber(entity.getEntityType());
                if (settingSpec.getEntityTypeScope().contains(entityType)) {
                    final boolean isMoveEnabled = setting.getBooleanSettingValue().getValue();
                    applyMovableToCommodities(entity, isMoveEnabled, builder -> true);
                }
            }
        }
    }

    /**
     * Applicator for "Enable Delete Actions" setting.
     */
    private static class EnableDeleteApplicator extends BaseSettingApplicator {

        private static final EntitySettingSpecs settingSpec = EntitySettingSpecs.EnableDeleteActions;

        @Override
        public void apply(@Nonnull TopologyEntityDTO.Builder entity,
                          @Nonnull Map<EntitySettingSpecs, Setting> entitySettings,
                          @Nonnull Map<ConfigurableActionSettings, Setting> actionModeSettings) {
            final Setting setting = entitySettings.get(settingSpec);
            if (setting != null) {
                final EntityType entityType = EntityType.forNumber(entity.getEntityType());
                if (settingSpec.getEntityTypeScope().contains(entityType)) {
                    final boolean isDeleteEnabled = setting.getBooleanSettingValue().getValue();
                    if (!isDeleteEnabled) {
                        entity.getAnalysisSettingsBuilder().setDeletable(false);
                    }
                }
            }
        }
    }
}
