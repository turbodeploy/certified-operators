package com.vmturbo.stitching.poststitching;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU_PROVISIONED_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_REQUEST_QUOTA_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_REQUEST_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;

/**
 * Post stitching operation to set CPU scaling factor to corresponding entities to reflect the
 * CPU performance of underlying infrastructure for more reasonable action results from market analysis.
 * <p/>
 * Scaling factor will be used to convert CPU commodity values from MHz/millicore to normalized MHz
 * to be analyzed by market engine. For non cloud native entities, scaling factor will convert CPU
 * values from raw MHz to normalized MHz; and for cloud native entities, scaling factor will convert
 * CPU values from millicore to normalized MHz.
 **/
public abstract class CpuScalingFactorPostStitchingOperation implements PostStitchingOperation {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The commodity types for commodities where we should apply the scalingFactor.
     * This set should be used with support for heterogeneous providers.
     */
    private static final Set<Integer> COMMODITY_TYPES_TO_SCALE_CONSISTENT_SCALING_FACTOR = ImmutableSet.of(
        CPU_VALUE,
        CPU_PROVISIONED_VALUE,
        VCPU_VALUE,
        VCPU_LIMIT_QUOTA_VALUE,
        VCPU_REQUEST_VALUE,
        VCPU_REQUEST_QUOTA_VALUE
    );

    /**
     * The commodity types for commodities where we should apply the scalingFactor.
     * This set should be used without support for heterogeneous providers.
     */
    private static final Set<Integer> COMMODITY_TYPES_TO_SCALE_BASIC = ImmutableSet.of(
        CPU_VALUE,
        CPU_PROVISIONED_VALUE,
        VCPU_VALUE,
        VCPU_LIMIT_QUOTA_VALUE
    );

    /**
     * Cloud native entities to use millicore scaling factor.
     */
    private static final Set<Integer> CLOUD_NATIVE_ENTITY_TYPES = ImmutableSet.of(
        EntityType.CONTAINER_VALUE,
        EntityType.CONTAINER_POD_VALUE,
        EntityType.WORKLOAD_CONTROLLER_VALUE,
        EntityType.NAMESPACE_VALUE,
        EntityType.VIRTUAL_MACHINE_VALUE
    );

    /**
     * Entity types in this category may span multiple hosts and therefore may have their
     * scalingFactors set multiple times during the updating process. Rather than always overwriting
     * the scalingFactor for these entities with whatever host gets updated last, these entities
     * should get the LARGEST scaling factor of any host they are connected to.
     * <p/>
     * Entity types in this category should also skip updates to their consumers. This prevents
     * triggering updates that do nothing too frequently for the same consumers of these entity types.
     */
    private static final Set<Integer> ENTITY_TYPES_REQUIRING_MAX = ImmutableSet.of(
        EntityType.WORKLOAD_CONTROLLER_VALUE,
        EntityType.NAMESPACE_VALUE
    );

    /**
     * Map of EntityTypes to Set of supported provider EntityTypes where we need to propagate the
     * update for CPU scalingFactor to.
     * <p/>
     * Specifically, for ContainerPods, although VMs are also providers, we explicitly exclude
     * them from pod providers to update here because the scalingFactor of VM should be set via hosts.
     */
    private static final Map<Integer, Set<Integer>> ENTITIES_TO_UPDATE_PROVIDERS =
        ImmutableMap.of(EntityType.CONTAINER_POD_VALUE,
            ImmutableSet.of(EntityType.WORKLOAD_CONTROLLER_VALUE, EntityType.NAMESPACE_VALUE),
            EntityType.WORKLOAD_CONTROLLER_VALUE, ImmutableSet.of(EntityType.NAMESPACE_VALUE));

    private final boolean enableConsistentScalingOnHeterogeneousProviders;

    /**
     * Create a new CpuScalingFactorPostStitchingOperation.
     *
     * @param enableConsistentScalingOnHeterogeneousProviders Whether to enable consistent scaling for containers
     *                                                        on heterogeneous providers.
     */
    protected CpuScalingFactorPostStitchingOperation(final boolean enableConsistentScalingOnHeterogeneousProviders) {
        this.enableConsistentScalingOnHeterogeneousProviders = enableConsistentScalingOnHeterogeneousProviders;
    }

    /**
     * Set the scalingFactor for the entity and all the consumers that buy from it.
     * The commodities to scale are sold by the entity given, and we update the
     * commodities for each entity that buys from it.
     *
     * @param entityToUpdate                 The TopologyEntity to update.
     * @param rawMHzOrMillicoreScalingFactor The scalingFactor to set on the entity. Raw MHz scaling
     *                                       factor if given entity is non cloud native; millicore
     *                                       scaling factor if given entity is cloud native one.
     * @param updatedSet A set of oids of entities that have already been updated.
     */
    @VisibleForTesting
    protected void updateScalingFactorForEntity(@Nonnull final TopologyEntity entityToUpdate,
                                                final double rawMHzOrMillicoreScalingFactor,
                                                @Nonnull LongSet updatedSet) {
        // Avoid potential for infinite recursion.
        if (updatedSet.contains(entityToUpdate.getOid())) {
            if (enableConsistentScalingOnHeterogeneousProviders) {
                if (!ENTITY_TYPES_REQUIRING_MAX.contains(entityToUpdate.getEntityType())) {
                    return;
                }
            } else {
                return;
            }
        }

        // Get millicore scalingFactor if cloud native entity.
        final double millicoreScalingFactor = CLOUD_NATIVE_ENTITY_TYPES.contains(entityToUpdate.getEntityType())
            ? getMillicoreScalingFactor(entityToUpdate, rawMHzOrMillicoreScalingFactor) : 0d;
        final long soldModified = updateScalingFactorForSoldCommodities(entityToUpdate, rawMHzOrMillicoreScalingFactor, millicoreScalingFactor);
        final long boughtModified = updateScalingFactorForBoughtCommodities(entityToUpdate, rawMHzOrMillicoreScalingFactor);
        boolean firstAdded = updatedSet.add(entityToUpdate.getOid());
        boolean shouldUpdateConsumers = firstAdded && !ENTITY_TYPES_REQUIRING_MAX.contains(entityToUpdate.getEntityType());
        // If we updated any sold commodities, go up to the consumers and make sure their respective
        // bought commodities are updated. To prevent infinite recursion along ENTITY_TYPES_REQUIRING_MAX,
        // we only permit recursive updates on consumer connections if this is the first time we saw
        // an entity.
        if (soldModified > 0 && shouldUpdateConsumers) {
            entityToUpdate.getConsumers().forEach(consumer -> {
                final double scalingFactor =
                    getRelatedScalingFactor(consumer.getEntityType(), rawMHzOrMillicoreScalingFactor, millicoreScalingFactor);
                updateScalingFactorForEntity(consumer, scalingFactor, updatedSet);
            });
        }

        // We need to propagate the scalingFactor update to the providers of certain entities to make
        // sure the changes of sold and bought commodities are consistent.
        // For example, for ContainerPod and WorkloadController, propagating scalingFactor is to
        // respect reseller logic so that VCPULimitQuota commodity sold by WorkloadController and
        // Namespace is consistent with VCPU commodity bought by ContainerPod.
        // We only need to do this if we updated any bought commodities.
        Set<Integer> providerTypesToUpdate = ENTITIES_TO_UPDATE_PROVIDERS.get(entityToUpdate.getEntityType());
        if (boughtModified > 0 && providerTypesToUpdate != null) {
            entityToUpdate.getProviders().stream()
                .filter(provider -> providerTypesToUpdate.contains(provider.getEntityType()))
                .forEach(
                    provider -> {
                        final double scalingFactor =
                            getRelatedScalingFactor(provider.getEntityType(), rawMHzOrMillicoreScalingFactor, millicoreScalingFactor);
                        updateScalingFactorForEntity(provider, scalingFactor, updatedSet);
                    }
                );
        }
    }

    private boolean isContainerVM(@Nonnull final TopologyEntity entity) {
        return entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
            && entity.getAggregators().stream()
            .anyMatch(consumer -> consumer.getEntityType() == EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE);
    }

    /**
     * Get millicore scaling factor for cloud native entity. If VM, calculate millicore scaling factor
     * based on CPU speed; if other cloud native entity, return the given scaling factor because it's
     * already the millicore scaling factor.
     * <p/>
     * Cloud native entities like Container, ContainerPod, WorkloadController and Namespace have CPU
     * commodity values in millicores sent from kubeturbo probe and market runs analysis on CPU related
     * commodities in normalized MHz. Millicore scalingFactor is used to convert CPU values from millicores
     * to normalized MHz by CPU value * millicoreScalingFactor.
     *
     * @param entity        Given entity to calculate cpu speed so as to calculate normalized scalingFactor.
     * @param scalingFactor Given original scalingFactor.
     * @return Millicore scalingFactor to be used to convert CPU values from millicores to normalized MHz.
     */
    private double getMillicoreScalingFactor(@Nonnull final TopologyEntity entity, final double scalingFactor) {
        if (isContainerVM(entity)) {
            return TopologyDTOUtil.getCPUCoreMhz(entity.getTopologyEntityDtoBuilder())
                .map(cpuCoreMhz -> scalingFactor * cpuCoreMhz / 1000)
                .orElseGet(() -> {
                    logger.error("CPU core MHz is not found from VM {}", entity.getOid());
                    return 0d;
                });
        }
        return scalingFactor;
    }

    /**
     * Update the 'scalingFactor' for any commoditiesSold of this entity that should be
     * scaled (see COMMODITIES_TO_SCALE).
     *
     * @param entityToUpdate         A TopologyEntity for which we should check for commodities to scale.
     * @param rawMHzScalingFactor    Raw MHz scaling factor to apply to any commodities that should scale.
     *                               It'll be used to convert commodity values from raw MHz to normalized MHz
     * @param millicoreScalingFactor Millicore scaling factor to apply to any commodities that should scale.
     *                               It'll be used to convert commodity values from millicores to normalized MHz
     * @return The number of modified commodities.
     */
    private long updateScalingFactorForSoldCommodities(@Nonnull final TopologyEntity entityToUpdate,
                                                       final double rawMHzScalingFactor,
                                                       final double millicoreScalingFactor) {
        return Objects.requireNonNull(entityToUpdate)
            .getTopologyEntityDtoBuilder()
            .getCommoditySoldListBuilderList().stream()
            .filter(commoditySold -> commodityTypeShouldScale(commoditySold.getCommodityType()))
            .peek(commSold -> {
                final double scalingFactor =
                    getRelatedScalingFactor(entityToUpdate.getEntityType(), commSold.getCommodityType().getType(),
                        rawMHzScalingFactor, millicoreScalingFactor);
                updateCommSoldScalingFactor(entityToUpdate, commSold, scalingFactor);
            })
            .count();
    }

    /**
     * Get related raw MHz or millicore scaling factor based on given entity type.
     *
     * @param entityType             Given entity type to determine which scaling factor to use.
     * @param rawMHzScalingFactor    Raw MHz scaling factor to be used to convert commodities of non
     *                               cloud native entities from raw MHz to normalized MHz for market analysis.
     * @param millicoreScalingFactor Millicore scaling factor to be used to convert commodities of
     *                               cloud native entities from millicores to normalized MHz for
     *                               market analysis.
     * @return Related scaling factor based on given entity type.
     */
    private double getRelatedScalingFactor(final int entityType, final double rawMHzScalingFactor,
                                           final double millicoreScalingFactor) {
        return getRelatedScalingFactor(entityType, -1, rawMHzScalingFactor, millicoreScalingFactor);
    }

    /**
     * Get related raw MHz or millicore scaling factor based on given entity type and commodity type.
     * For example, return millicoreScalingFactor for VM VCPURequest (in millicores) and return
     * rawMHzScalingFactor for VM VCPU (in MHz).
     *
     * @param entityType             Given entity type to determine which scaling factor to use.
     * @param commodityType          Given commoidty type to determin which scaling factor to use.
     * @param rawMHzScalingFactor    Raw MHz scaling factor to be used to convert commodities of non
     *                               cloud native entities from raw MHz to normalized MHz for market analysis.
     * @param millicoreScalingFactor Millicore scaling factor to be used to convert commodities of
     *                               cloud native entities from millicores to normalized MHz for
     *                               market analysis.
     * @return Related scaling factor based on given entity type.
     */
    private double getRelatedScalingFactor(final int entityType, final int commodityType,
                                           final double rawMHzScalingFactor, final double millicoreScalingFactor) {
        if (entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
            return commodityType == VCPU_REQUEST_VALUE ? millicoreScalingFactor : rawMHzScalingFactor;
        } else {
            return CLOUD_NATIVE_ENTITY_TYPES.contains(entityType) ? millicoreScalingFactor : rawMHzScalingFactor;
        }
    }

    /**
     * Update the 'scalingFactor' for any commoditiesBought of this entity that should be
     * scaled (see COMMODITIES_TO_SCALE).
     *
     * @param entityToUpdate a TopologyEntity for which we should check for commodities to scale
     * @param scalingFactor the scaling factor to apply to any commodities that should scale
     * @return The number of modified commodities.
     */
    private long updateScalingFactorForBoughtCommodities(@Nonnull final TopologyEntity entityToUpdate,
                                                       final double scalingFactor) {
        return Objects.requireNonNull(entityToUpdate)
            .getTopologyEntityDtoBuilder()
            .getCommoditiesBoughtFromProvidersBuilderList().stream()
            .flatMap(commoditiesBoughtFromProvider ->
                commoditiesBoughtFromProvider.getCommodityBoughtBuilderList().stream())
            .filter(commodityBoughtFromProvider ->
                commodityTypeShouldScale(commodityBoughtFromProvider.getCommodityType()))
            .peek(commBought -> updateCommBoughtScalingFactor(entityToUpdate, commBought, scalingFactor))
            .count();
    }

    /**
     * Update the scaling factor for an individual commodity sold.
     *
     * @param entityToUpdate The entity whose commodity is being updated.
     * @param commSold The commodity being updated.
     * @param scalingFactor The scaling factor to set.
     */
    private void updateCommSoldScalingFactor(@Nonnull final TopologyEntity entityToUpdate,
                                             @Nonnull final CommoditySoldDTO.Builder commSold,
                                             final double scalingFactor) {
        // If the commSold already has a scalingFactor and requires a maximum of the scalingFactors
        // from all related hosts, pick the larger between the new and existing scaling factors.
        if (commSold.hasScalingFactor()
            && ENTITY_TYPES_REQUIRING_MAX.contains(entityToUpdate.getEntityType())) {
            // Pick the larger between the new scalingFactor and the existing scalingFactor.
            if (scalingFactor > commSold.getScalingFactor()) {
                commSold.setScalingFactor(scalingFactor);
            }
        } else {
            commSold.setScalingFactor(scalingFactor);
        }
    }

    /**
     * Update the scaling factor for an individual commodity bought.
     *
     * @param entityToUpdate The entity whose commodity is being updated.
     * @param commBought The commodity being updated.
     * @param scalingFactor The scaling factor to set.
     */
    private void updateCommBoughtScalingFactor(@Nonnull final TopologyEntity entityToUpdate,
                                               @Nonnull final CommodityBoughtDTO.Builder commBought,
                                               final double scalingFactor) {
        // If the commBought already has a scalingFactor and requires a maximum of the scalingFactors
        // from all related hosts, pick the larger between the new and existing scaling factors.
        if (commBought.hasScalingFactor()
            && ENTITY_TYPES_REQUIRING_MAX.contains(entityToUpdate.getEntityType())) {
            // Pick the larger between the new scalingFactor and the existing scalingFactor.
            if (scalingFactor > commBought.getScalingFactor()) {
                commBought.setScalingFactor(scalingFactor);
            }
        } else {
            commBought.setScalingFactor(scalingFactor);
        }
    }

    /**
     * Check to see if the given Commodity is of the type to which 'scaleFactor' applies.
     *
     * @param commodityType the commodityType to test
     * @return true iff this commodity should have a 'scaleFactor' applied, if one is found
     */
    private boolean commodityTypeShouldScale(@Nonnull TopologyDTO.CommodityType commodityType) {
        if (enableConsistentScalingOnHeterogeneousProviders) {
            return COMMODITY_TYPES_TO_SCALE_CONSISTENT_SCALING_FACTOR.contains(commodityType.getType());
        } else {
            return COMMODITY_TYPES_TO_SCALE_BASIC.contains(commodityType.getType());
        }
    }

    /**
     * Set CPU scaling factor to entities based on hosts. The scaling factor will be used to convert
     * CPU commodity values from raw MHz to normalized MHz for market analysis.
     * <p/>
     * For any PhysicalMachine with the 'cpuModel' value set, look up the
     * 'scalingFactor' by calling the CpuCapacity RPC service for that 'cpuModel'.
     * Store the 'scalingFactor' returned 'CPU' sold commodity; the 'CPUProvisioned' commodity;
     * and for each VM that buys from this PM, store the 'scalingFactor' on the 'VCPU'
     * commodity for that VM.
     * <p/>
     * In order avoid a round trip to the CpuCapacity RPC service for each PM, we build a
     * multimap of 'cpuModel' -> PMs with that CPU model, and then make a single request
     * to the CpuCapacity RPC service to fetch all of the 'scalingFactor's for all the 'cpuModel's
     * in one call. Then we insert each returned 'scalingFactor' in the appropriate PM (and VM)
     * commodities in the result.
     */
    public static class HostCpuScalingFactorPostStitchingOperation extends CpuScalingFactorPostStitchingOperation {

        private final CpuCapacityStore cpuCapacityStore;

        /**
         * Create a new HostCpuScalingFactorPostStitchingOperation.
         *
         * @param cpuCapacityStore The CPU capacity store.
         * @param enableConsistentScalingOnHeterogeneousProviders Whether to enable consistent scaling for containers
         *                                                        on heterogeneous providers.
         */
        public HostCpuScalingFactorPostStitchingOperation(final CpuCapacityStore cpuCapacityStore,
                                                          final boolean enableConsistentScalingOnHeterogeneousProviders) {
            super(enableConsistentScalingOnHeterogeneousProviders);
            this.cpuCapacityStore = cpuCapacityStore;
        }

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.multiEntityTypesScope(ImmutableList.of(EntityType.PHYSICAL_MACHINE));
        }

        @Nonnull
        @Override
        public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> pmEntities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            // the entities to operate on will all be PM's
            pmEntities.forEach(pmEntity -> {
                Optional<Double> scalingFactor = getCpuModelOpt(pmEntity)
                    .flatMap(cpuCapacityStore::getScalingFactor);
                scalingFactor.ifPresent(sf -> {
                    // queue update to just this entity
                    resultBuilder.queueUpdateEntityAlone(pmEntity, entityToUpdate ->
                        updateScalingFactorForEntity(entityToUpdate, sf, new LongOpenHashSet()));
                });
            });
            return resultBuilder.build();
        }

        /**
         * Fetch the 'cpuModel' of a TopologyEntity. 'cpuModel' is a PM-only attribute, and so is
         * part of the type-specific info, PhysicalMachine oneof. If the TopologyEntity does not have a
         * PhysicalMachine TypeSpecificInfo, or the PhysicalMachine doesn't have a 'cpuModel', then
         * return Optional.empty().
         *
         * @param entityToFetchFrom the TopologyEntity from which we are going to check for the 'cpuModel'
         *                          field.
         * @return an Optional containing the 'cpuModel', or Optional.empty() if this isn't a PM, or
         * the PM doesn't have a 'cpuModel' field set.
         */
        private Optional<String> getCpuModelOpt(@Nonnull final TopologyEntity entityToFetchFrom) {
            final TypeSpecificInfo typeSpecificInfo = Objects.requireNonNull(entityToFetchFrom)
                .getTopologyEntityDtoBuilder().getTypeSpecificInfo();
            return typeSpecificInfo.hasPhysicalMachine() &&
                typeSpecificInfo.getPhysicalMachine().hasCpuModel()
                ? Optional.of(typeSpecificInfo.getPhysicalMachine().getCpuModel())
                : Optional.empty();
        }
    }

    /**
     * Operation to set CPU scaling factor to cloud native entities based on VMs discovered by cloud
     * native targets.
     * <p/>
     * Cloud native entities have CPU commodities in millicores discovered from kubeturbo probe and
     * market runs analysis on CPU related commodities in MHz. The scaling factor will be used to
     * convert CPU values from millicores to normalized MHz for market analysis by
     * scalingFactor * cpuSpeed (MHz/millicore).
     * <p/>
     * Note that this operation is specifically to update scaling factor to cloud native entities
     * in cloud or standalone container platform cluster. On-prem cloud native entities have scaling
     * factor updated in {@link HostCpuScalingFactorPostStitchingOperation} based on underlying hosts.
     */
    public static class CloudNativeVMCpuScalingFactorPostStitchingOperation extends CpuScalingFactorPostStitchingOperation {

        /**
         * Create a new CloudNativeVMCpuScalingFactorPostStitchingOperation.
         *
         * @param enableConsistentScalingOnHeterogeneousProviders Whether to enable consistent scaling for containers
         *                                                        on heterogeneous providers.
         */
        public CloudNativeVMCpuScalingFactorPostStitchingOperation(
            final boolean enableConsistentScalingOnHeterogeneousProviders) {
            super(enableConsistentScalingOnHeterogeneousProviders);
        }

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.probeCategoryEntityTypeScope(ProbeCategory.CLOUD_NATIVE,
                EntityType.VIRTUAL_MACHINE);
        }

        @Nonnull
        @Override
        public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> vmEntities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
            // The entities to operate on will be VMs discovered by cloud native probe except
            // on-prem ones because those VMs already have scalingFactor updated in previous
            // HostCpuScalingFactorPostStitchingOperation based on underlying hosts.
            // For entities discovered from both on-prem and cloud targets, if not discovered from
            // only app or container targets, the environment type will be on-prem set up in
            // EnvironmentTypeInjector.
            vmEntities.filter(vmEntity -> vmEntity.getEnvironmentType() != EnvironmentType.ON_PREM)
                .forEach(vmEntity -> getScalingFactorFromVM().ifPresent(sf -> {
                    // queue update to just this entity
                    resultBuilder.queueUpdateEntityAlone(vmEntity,
                        entityToUpdate -> updateScalingFactorForEntity(entityToUpdate, sf, new LongOpenHashSet()));
                }));
            return resultBuilder.build();
        }

        /**
         * Get scalingFactor from VM to make sure cloud native entities in cloud or standalone container
         * platform cluster will have proper normalized scalingFactor set up to CPU commodities.
         * TODO: Always return default value as 1 for now. Will update scalingFactor calculation for
         *  cloud VMs to improve container pod move recommendation in a heterogeneous cloud cluster
         *  in OM-68739.
         *
         * @return ScalingFactor from VM.
         */
        private Optional<Double> getScalingFactorFromVM() {
            return Optional.of(1d);
        }

        @Override
        protected void updateScalingFactorForEntity(@Nonnull final TopologyEntity entityToUpdate,
                                                    final double rawMHzOrMillicoreScalingFactor,
                                                    @Nonnull LongSet updatedSet) {
            // Avoid updating non cloud native entities
            if (!CLOUD_NATIVE_ENTITY_TYPES.contains(entityToUpdate.getEntityType())) {
                return;
            }
            super.updateScalingFactorForEntity(entityToUpdate, rawMHzOrMillicoreScalingFactor, updatedSet);
        }
    }
}
