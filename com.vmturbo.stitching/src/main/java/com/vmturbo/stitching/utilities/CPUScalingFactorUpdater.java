package com.vmturbo.stitching.utilities;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CPU_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_LIMIT_QUOTA_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_REQUEST_QUOTA_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_REQUEST_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU_VALUE;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;

/**
 * A class to set CPU scaling factor to entities based on hosts or vms. The scaling factor will be
 * used to convert CPU commodity values from raw MHz to normalized MHz for market analysis.
 */
public class CPUScalingFactorUpdater {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The commodity types for commodities where we should apply the scalingFactor.
     * This set should be used with support for heterogeneous providers.
     */
    private static final Set<Integer> COMMODITY_TYPES_TO_SCALE_CONSISTENT_SCALING_FACTOR = ImmutableSet.of(
            CPU_VALUE,
            VCPU_VALUE,
            VCPU_LIMIT_QUOTA_VALUE,
            VCPU_REQUEST_VALUE,
            VCPU_REQUEST_QUOTA_VALUE
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
    public void update(@Nonnull final TopologyEntity entityToUpdate,
                       final double rawMHzOrMillicoreScalingFactor, @Nonnull LongSet updatedSet) {
        // Avoid potential for infinite recursion.
        if (updatedSet.contains(entityToUpdate.getOid())) {
            if (!ENTITY_TYPES_REQUIRING_MAX.contains(entityToUpdate.getEntityType())) {
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
                update(consumer, scalingFactor, updatedSet);
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
                                update(provider, scalingFactor, updatedSet);
                            }
                    );
        }
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
     * @param entity        Given entity to calculate cpu speed to calculate normalized scalingFactor.
     * @param scalingFactor Given original scalingFactor.
     * @return Millicore scalingFactor to be used to convert CPU values from millicores to normalized MHz.
     */
    private static double getMillicoreScalingFactor(@Nonnull final TopologyEntity entity,
                                                    final double scalingFactor) {
        if (isContainerVM(entity)) {
            return TopologyDTOUtil.getCPUCoreMhz(entity.getTopologyEntityImpl())
                    .map(cpuCoreMhz -> scalingFactor * cpuCoreMhz / 1000)
                    .orElseGet(() -> {
                        //Only Print the log if the vm state is not unknown to avoid log pollution
                        //See https://vmturbo.atlassian.net/browse/OM-73884
                        if (entity.getEntityState() != TopologyDTO.EntityState.UNKNOWN) {
                            logger.error("CPU core MHz is not found from VM {}", entity.getOid());
                        }
                        return 0d;
                    });
        }
        return scalingFactor;
    }

    private static boolean isContainerVM(@Nonnull final TopologyEntity entity) {
        return entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                && entity.getAggregators().stream()
                .anyMatch(consumer -> consumer.getEntityType() == EntityType.CONTAINER_PLATFORM_CLUSTER_VALUE);
    }

    /**
     * Update the 'scalingFactor' for any commoditiesSold of this entity that should be
     * scaled (see COMMODITIES_TO_SCALE).
     *
     * @param entityToUpdate         A TopologyEntity for which we should check for commodities to
     * scale.
     * @param rawMHzScalingFactor    Raw MHz scaling factor to apply to any commodities that should scale.
     *                               It'll be used to convert commodity values from raw MHz to normalized MHz
     * @param millicoreScalingFactor Millicore scaling factor to apply to any commodities that should scale.
     *                               It'll be used to convert commodity values from millicores to normalized MHz
     * @return The number of modified commodities.
     */
    private static long updateScalingFactorForSoldCommodities(@Nonnull final TopologyEntity entityToUpdate,
                                                              final double rawMHzScalingFactor,
                                                              final double millicoreScalingFactor) {
        return Objects.requireNonNull(entityToUpdate)
                .getTopologyEntityImpl()
                .getCommoditySoldListImplList().stream()
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
    private static double getRelatedScalingFactor(final int entityType, final double rawMHzScalingFactor,
                                                  final double millicoreScalingFactor) {
        return getRelatedScalingFactor(entityType, -1, rawMHzScalingFactor, millicoreScalingFactor);
    }

    /**
     * Get related raw MHz or millicore scaling factor based on given entity type and commodity type.
     * For example, return millicoreScalingFactor for VM VCPURequest (in millicores) and return
     * rawMHzScalingFactor for VM VCPU (in MHz).
     *
     * @param entityType             Given entity type to determine which scaling factor to use.
     * @param commodityType          Given commodity type to determine which scaling factor to use.
     * @param rawMHzScalingFactor    Raw MHz scaling factor to be used to convert commodities of non
     *                               cloud native entities from raw MHz to normalized MHz for market analysis.
     * @param millicoreScalingFactor Millicore scaling factor to be used to convert commodities of
     *                               cloud native entities from millicores to normalized MHz for
     *                               market analysis.
     * @return Related scaling factor based on given entity type.
     */
    private static double getRelatedScalingFactor(final int entityType, final int commodityType,
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
    private static long updateScalingFactorForBoughtCommodities(@Nonnull final TopologyEntity entityToUpdate,
                                                                final double scalingFactor) {
        return Objects.requireNonNull(entityToUpdate)
                .getTopologyEntityImpl()
                .getCommoditiesBoughtFromProvidersImplList().stream()
                .flatMap(commoditiesBoughtFromProvider ->
                                 commoditiesBoughtFromProvider.getCommodityBoughtImplList().stream())
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
    private static void updateCommSoldScalingFactor(@Nonnull final TopologyEntity entityToUpdate,
                                                    @Nonnull final CommoditySoldImpl commSold,
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
    private static void updateCommBoughtScalingFactor(@Nonnull final TopologyEntity entityToUpdate,
                                                      @Nonnull final CommodityBoughtImpl commBought,
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
    private static boolean commodityTypeShouldScale(@Nonnull CommodityTypeView commodityType) {
        return COMMODITY_TYPES_TO_SCALE_CONSISTENT_SCALING_FACTOR.contains(commodityType.getType());
    }

    /**
     * A class to set CPU scaling factor to cloud native entities based on VMs.
     */
    public static class CloudNativeCPUScalingFactorUpdater extends CPUScalingFactorUpdater {
        @Override
        public void update(@Nonnull final TopologyEntity entityToUpdate,
                           final double rawMHzOrMillicoreScalingFactor,
                           @Nonnull LongSet updatedSet) {
            // Avoid updating non cloud native entities
            if (!CLOUD_NATIVE_ENTITY_TYPES.contains(entityToUpdate.getEntityType())) {
                return;
            }
            super.update(entityToUpdate, rawMHzOrMillicoreScalingFactor, updatedSet);
        }
    }
}
