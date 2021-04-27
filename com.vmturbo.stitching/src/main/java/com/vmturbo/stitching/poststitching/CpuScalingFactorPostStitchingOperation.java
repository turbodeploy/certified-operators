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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;

/**
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
 **/
public class CpuScalingFactorPostStitchingOperation implements PostStitchingOperation {

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
     * Entity types in this category may span multiple hosts and therefore may have their
     * scalingFactors set multiple times during the updating process. Rather than always overwriting
     * the scalingFator for these entities with whatever host gets updated last, these entities
     * should get the LARGEST scaling factor of any host they are connected to.
     */
    private static final Set<Integer> ENTITY_TYPES_REQUIRING_MAX = ImmutableSet.of(
        EntityType.WORKLOAD_CONTROLLER_VALUE,
        EntityType.NAMESPACE_VALUE
    );

    /**
     * Map of EntityTypes to Set of supported provider EntityTypes where we need to propagate the
     * update for CPU scalingFactor to.
     *
     * <p>Specifically, for ContainerPods, although VMs are also providers, we explicitly exclude
     * them from pod providers to update here because the scalingFactor of VM should be set via hosts.
     */
    private static final Map<Integer, Set<Integer>> ENTITIES_TO_UPDATE_PROVIDERS =
        ImmutableMap.of(EntityType.CONTAINER_POD_VALUE,
            ImmutableSet.of(EntityType.WORKLOAD_CONTROLLER_VALUE, EntityType.NAMESPACE_VALUE),
            EntityType.WORKLOAD_CONTROLLER_VALUE, ImmutableSet.of(EntityType.NAMESPACE_VALUE));

    private final CpuCapacityStore cpuCapacityStore;
    private final boolean enableConsistentScalingOnHeterogeneousProviders;

    /**
     * Create a new CpuScalingFactorPostStitchingOperation.
     *
     * @param cpuCapacityStore The CPU capacity store.
     * @param enableConsistentScalingOnHeterogeneousProviders Whether to enable consistent scaling for containers
     *                                                        on heterogeneous providers.
     */
    public CpuScalingFactorPostStitchingOperation(final CpuCapacityStore cpuCapacityStore,
                                                  final boolean enableConsistentScalingOnHeterogeneousProviders) {
        this.cpuCapacityStore = cpuCapacityStore;
        this.enableConsistentScalingOnHeterogeneousProviders = enableConsistentScalingOnHeterogeneousProviders;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.entityTypeScope(EntityType.PHYSICAL_MACHINE);
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
     * Set the scalingFactor for the entity and all the consumers that buy from it.
     * The commodities to scale are sold by the entity given, and we update the
     * commodities for each entity that buys from it.
     *
     * @param entityToUpdate the TopologyEntity to update.
     * @param scalingFactor the scalingFactor to set on the entity.
     * @param updatedSet A set of oids of entities that have already been updated.
     */
    @VisibleForTesting
    void updateScalingFactorForEntity(@Nonnull final TopologyEntity entityToUpdate,
                                      final double scalingFactor,
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

        final long soldModified = updateScalingFactorForSoldCommodities(entityToUpdate, scalingFactor);
        final long boughtModified = updateScalingFactorForBoughtCommodities(entityToUpdate, scalingFactor);
        boolean firstAdded = updatedSet.add(entityToUpdate.getOid());
        // If we updated any sold commodities, go up to the consumers and make sure their respective
        // bought commodities are updated. To prevent infinite recursion along ENTITY_TYPES_REQUIRING_MAX,
        // we only permit recursive updates on consumer connections if this is the first time we saw
        // an entity.
        if (soldModified > 0 && firstAdded) {
            entityToUpdate.getConsumers().forEach(consumer -> updateScalingFactorForEntity(consumer, scalingFactor, updatedSet));
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
                    provider -> updateScalingFactorForEntity(provider, scalingFactor, updatedSet)
                );
        }
    }

    /**
     * Update the 'scalingFactor' for any commoditiesSold of this entity that should be
     * scaled (see COMMODITIES_TO_SCALE).
     *
     * @param entityToUpdate a TopologyEntity for which we should check for commodities to scale
     * @param scalingFactor the scaling factor to apply to any commodities that should scale
     * @return The number of modified commodities.
     */
    private long updateScalingFactorForSoldCommodities(@Nonnull final TopologyEntity entityToUpdate,
                                                       final double scalingFactor) {
        return Objects.requireNonNull(entityToUpdate)
                .getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList().stream()
            .filter(commoditySold -> commodityTypeShouldScale(commoditySold.getCommodityType()))
            .peek(commSold -> updateCommSoldScalingFactor(entityToUpdate, commSold, scalingFactor))
                .count();
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
}
