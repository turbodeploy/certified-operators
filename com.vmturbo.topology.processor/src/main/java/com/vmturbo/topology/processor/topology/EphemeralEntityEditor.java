package com.vmturbo.topology.processor.topology;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Cloud native containers and pods are ephemeral (short lifespan) and isomorphic (the only
 * feature distinguishing Foo-1 from Foo-2 is its unique ID). When an individual container
 * is restarted, behind the scenes the original container (say, "Foo-1234") and a new one
 * is started in its place (say, "Foo-5678"). Furthermore, when sizing containers, all
 * containers of the same type must be assigned the new size (similar to a consistent
 * scaling group for VMs in the cloud). This leads to the problem that when a vertical
 * scaling action is generated in Turbo, all containers of the same type (say, "Foo") are
 * killed and replaced with new containers. This results in the loss of all history for all
 * "Foo" instances. Since Market analysis is performed on historical utilization, this
 * loss of history results in very poor recommendations after all containers are restarted.
 * For example, since we forget historical peaks in the utilization history we may resize
 * down far too aggressively if present utilization drops once all the containers are restarted.
 * <p/>
 * To resolve these problems (and others) that result from forgetting history for ephemeral
 * entities, we store history for the ephemeral entities on durable entities and then
 * transferring that shared history onto the individual entities at topology processor
 * broadcast time.
 * <p/>
 * For additional details on the design for retaining history for ephemeral entities see:
 * https://vmturbo.atlassian.net/browse/OM-56130
 * <p/>
 * Note that this class does no maintenance, settings lookups, etc. Instead, all the work
 * for these functions is done on the entities where we aggregate shared ephemeral entity
 * history (ie CONTAINER_SPEC). This stage performs a very simple and naive copy
 * of that shared history onto the individual ephemeral entities that contribute to that
 * shared history. Entities inserted or modified by plan scenario changes should
 * set up their relationships so that they get the correct behavior from this stage.
 * <p/>
 * Right now only sold commodities are edited because only sold commodities are resized.
 */
public class EphemeralEntityEditor {
    private static final Logger logger = LogManager.getLogger();

    /**
     * CONTAINER_SPEC entity store shared, persistent history for ephemeral CONTAINER replicas.
     * <p/>
     * CONTAINER_SPEC entities are connected to CONTAINER entities via "aggregates"
     * relationships. This editor copies the shared history from the CONTAINER_SPEC
     * BACK onto the individual CONTAINER instances by traversing these connections
     * in the topology graph.
     * <p/>
     * As of today the only persistent->ephemeral relation we have is
     * CONTAINER_SPEC->CONTAINER but we may wish to add
     * WORKLOAD_CONTROLLER->CONTAINER_POD in the future if we ever want
     * history on container pods.
     */
    private static final Set<Integer> ROOT_PERSISTENT_ENTITY_TYPES =
        ImmutableSet.of(EntityType.CONTAINER_SPEC.getNumber());

    /**
     * Apply edits to commodities sold by ephemeral entities by copying their
     * shared history from the related persistent entities.
     *
     * @param graph The {@link TopologyGraph} containing all the entities in
     *              the topology and their relationships.
     */
    public void applyEdits(@Nonnull final TopologyGraph<TopologyEntity> graph) {
        ROOT_PERSISTENT_ENTITY_TYPES.stream().forEach(entityType ->
            graph.entitiesOfType(entityType).forEach(this::applyEdits));
    }

    /**
     * Copy the commodity history from the persistent entity onto the related
     * ephemeral entities.
     *
     * @param persistentEntity The persistent entity responsible for aggregating
     *                         the shared history for ephemeral entity replicas.
     */
    private void applyEdits(@Nonnull final TopologyEntity persistentEntity) {
        final Map<Integer, List<CommoditySoldDTO>> persistentSoldCommodities =
            persistentEntity.soldCommoditiesByType();

        // As of now all persistent entities aggregate their ephemeral counterparts.
        persistentEntity.getAggregatedEntities().forEach(ephemeralEntity -> {
            logger.debug("Copying commodity history from persistent entity {} to ephemeral entity {}",
                () -> persistentEntity, () -> ephemeralEntity);
            copyCommodityHistory(persistentSoldCommodities,
                ephemeralEntity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList());
        });
    }

    /**
     * Copy commodity historical values from the persistent entity's sold commodities onto
     * the matching sold commodities from the ephemeral entity.
     * <p/>
     * Ephemeral entity commodities not sold by the persistent entity are not modified.
     * <p/>
     * Also updates the ephemeral commodity resizeable flag to match that of the persistent
     * commodity.
     *
     * @param persistentSoldCommodities Commodities sold by the persistent entity.
     * @param ephemeralSoldCommodities Commodities sold by the ephemeral entity.
     */
    private void copyCommodityHistory(
        @Nonnull final Map<Integer, List<CommoditySoldDTO>> persistentSoldCommodities,
        @Nonnull final List<CommoditySoldDTO.Builder> ephemeralSoldCommodities) {
        ephemeralSoldCommodities.stream().forEach(ephemeralCommSold -> {
            final List<CommoditySoldDTO> soldOfType =
                persistentSoldCommodities.get(ephemeralCommSold.getCommodityType().getType());
            getMatchingPersistentCommodity(ephemeralCommSold, soldOfType).ifPresent(persistentCommSold -> {
                logger.trace("Copying historicalUsed {}, historicalPeak {} and isResizeable {} for commodity of type {}",
                    persistentCommSold::getHistoricalUsed,
                    persistentCommSold::getHistoricalPeak,
                    persistentCommSold::getIsResizeable,
                    persistentCommSold::getCommodityType);

                // Set the resizable flag. If there's insufficient data on the persistent entity's
                // commodity, it will be set to resizable=false and the associated ephemeral
                // entities should match this behavior.
                if (persistentCommSold.hasIsResizeable()) {
                    ephemeralCommSold.setIsResizeable(persistentCommSold.getIsResizeable());
                }

                // Copy over historical used and peak
                if (persistentCommSold.hasHistoricalPeak()) {
                    ephemeralCommSold.setHistoricalPeak(persistentCommSold.getHistoricalPeak());
                }
                if (persistentCommSold.hasHistoricalUsed()) {
                    ephemeralCommSold.setHistoricalUsed(persistentCommSold.getHistoricalUsed());
                }

                // Note that we purposely do NOT copy over the UtilizationData field on
                // the commodities. The UtilizationData is a temporary field only used internally
                // in TopologyProcessor for computing the percentile values set on the
                // historical used and peak fields.
            });
        });
    }

    /**
     * For a given commodity sold by an ephemeral entity, find the matching commodity from
     * the list of commodities sold by the persistent entity.
     * <p/>
     * We consider a commodity a match when they have the same type and key.
     *
     * @param ephemeralCommodity The ephemeral commodity whose match we wish to search for.
     * @param persistentSoldCommoditiesOfType The list of commodities sold by the persistent
     *                                        entity with the same commodity type as the
     *                                        ephemeralCommodity type. This list may be null
     *                                        if the persistent entity does not sell any commodities
     *                                        of the given type.
     * @return The first matching persistent commodity found, or {@link Optional#empty()}
     *         if no match can be found.
     */
    @Nonnull
    private Optional<CommoditySoldDTO> getMatchingPersistentCommodity(
        @Nonnull final CommoditySoldDTO.Builder ephemeralCommodity,
        @Nullable final List<CommoditySoldDTO> persistentSoldCommoditiesOfType) {
        return Optional.ofNullable(persistentSoldCommoditiesOfType)
            .flatMap(comms -> comms.stream()
                .filter(persistent -> Objects.equals(
                    persistent.getCommodityType().getKey(), ephemeralCommodity.getCommodityType().getKey()))
                .findFirst());
    }
}
