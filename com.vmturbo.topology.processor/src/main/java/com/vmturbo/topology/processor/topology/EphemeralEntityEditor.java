package com.vmturbo.topology.processor.topology;

import java.util.HashMap;
import java.util.HashSet;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
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
 * Also disables resizing for consistent scaling groups that violate the invariant of having
 * the same capacity on a commodity. If we don't, the market ends up generating nonsense
 * recommendations for these groups.
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
     * See https://vmturbo.atlassian.net/browse/OM-62824 for further details.
     * Sometimes when containers are running on nodes with different CPU speeds, even though
     * they are configured with the same millicore limits and requests, they may end up with
     * different speeds in MHz. The market assumes an invariant that all members of a consistent
     * scaling group have the same capacity for a commodity and will generate nonsense recommendations
     * when they don't. Until we fix this, disable consistent scaling when we detect this
     * situation.
     */
    private static final Set<Integer> REQUIRED_CONSISTENT_COMMODITIES =
        ImmutableSet.of(CommodityType.VCPU_VALUE, CommodityType.VCPU_REQUEST_VALUE);

    private static final double DBL_EPSILON = Math.ulp(1.0);

    /**
     * Apply edits to commodities sold by ephemeral entities by copying their
     * shared history from the related persistent entities.
     *
     * @param graph The {@link TopologyGraph} containing all the entities in
     *              the topology and their relationships.
     * @return a summary of the edits made to the topology by applying edits.
     */
    public EditSummary applyEdits(@Nonnull final TopologyGraph<TopologyEntity> graph) {
        final EditSummary editSummary = new EditSummary();
        ROOT_PERSISTENT_ENTITY_TYPES.stream().forEach(entityType ->
            graph.entitiesOfType(entityType)
                .forEach(persistentEntity -> applyEdits(persistentEntity, editSummary)));

        if (editSummary.getInconsistentScalingGroups() > 0) {
            logger.warn("Disabled resize on {} scaling groups due to capacity inconsistencies.",
                editSummary.getInconsistentScalingGroups());
        }
        return editSummary;
    }

    /**
     * Copy the commodity history from the persistent entity onto the related
     * ephemeral entities.
     *
     * @param persistentEntity The persistent entity responsible for aggregating
     *                         the shared history for ephemeral entity replicas.
     * @param editSummary A summary of the edits made.
     */
    private void applyEdits(@Nonnull final TopologyEntity persistentEntity,
                            @Nonnull final EditSummary editSummary) {
        final Map<Integer, List<CommoditySoldDTO>> persistentSoldCommodities =
            persistentEntity.soldCommoditiesByType();
        final Map<Integer, Double> requiredConsistentCommodityValues = new HashMap<>();
        final Set<Integer> inconsistentCommodities = new HashSet<>();

        // As of now all persistent entities aggregate their ephemeral counterparts.
        persistentEntity.getAggregatedEntities().forEach(ephemeralEntity -> {
            logger.debug("Copying commodity history from persistent entity {} to ephemeral entity {}",
                () -> persistentEntity, () -> ephemeralEntity);
            copyCommodityHistory(persistentSoldCommodities,
                ephemeralEntity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList(),
                editSummary);
            identifyInconsistentCommodities(requiredConsistentCommodityValues,
                inconsistentCommodities, ephemeralEntity);
        });

        editSummary.incrementTotalScalingGroups();
        if (!inconsistentCommodities.isEmpty()) {
            logger.debug("Disabling resize on commodities {} for scaling group identified "
                + "by persistent entity {}", inconsistentCommodities, persistentEntity.getDisplayName());
            editSummary.incrementInconsistentScalingGroups();
            persistentEntity.getAggregatedEntities().forEach(ephemeralEntity ->
                ephemeralEntity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
                    .filter(builder -> inconsistentCommodities.contains(builder.getCommodityType().getType()))
                    .forEach(builder -> builder.setIsResizeable(false)));
        }
    }

    /**
     * Identify commodities whose capacities are inconsistent across the scaling group.
     *
     * @param commodityTypeToCapacity Map of commodities to the detected capacity of that commodity for the
     *                                scaling group.
     * @param inconsistentCommodities The inconsistent commodities detected so far.
     * @param ephemeralEntity The entity whose commodities should be scanned for inconsistencies with
     *                        the other scaling group members.
     */
    private void identifyInconsistentCommodities(@Nonnull final Map<Integer, Double> commodityTypeToCapacity,
                                                 @Nonnull final Set<Integer> inconsistentCommodities,
                                                 @Nonnull final TopologyEntity ephemeralEntity) {
        ephemeralEntity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(builder -> REQUIRED_CONSISTENT_COMMODITIES.contains(builder.getCommodityType().getType()))
            .forEach(builder -> {
                final Integer type = builder.getCommodityType().getType();
                final Double otherMemberCapacity = commodityTypeToCapacity.get(type);
                // Be sure to scale by scaling factor because the market will do so.
                final double scaledCapacity = builder.getCapacity() * builder.getScalingFactor();
                if (otherMemberCapacity != null) {
                    // Compare for relative equality.
                    if (Math.abs(otherMemberCapacity - scaledCapacity) > DBL_EPSILON) {
                        inconsistentCommodities.add(type);
                    }
                }

                commodityTypeToCapacity.put(type, scaledCapacity);
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
     *  @param persistentSoldCommodities Commodities sold by the persistent entity.
     * @param ephemeralSoldCommodities Commodities sold by the ephemeral entity.
     * @param editSummary A summary to capture edits made.
     */
    private void copyCommodityHistory(
        @Nonnull final Map<Integer, List<CommoditySoldDTO>> persistentSoldCommodities,
        @Nonnull final List<Builder> ephemeralSoldCommodities,
        @Nonnull final EditSummary editSummary) {
        for (CommoditySoldDTO.Builder ephemeralCommSold : ephemeralSoldCommodities) {
            final List<CommoditySoldDTO> soldOfType =
                persistentSoldCommodities.get(ephemeralCommSold.getCommodityType().getType());
            getMatchingPersistentCommodity(ephemeralCommSold, soldOfType).ifPresent(persistentCommSold -> {
                boolean commoditiesAdjusted = false;
                logger.trace("Copying historicalUsed {}, historicalPeak {} and isResizeable {} for commodity of type {}",
                    persistentCommSold::getHistoricalUsed,
                    persistentCommSold::getHistoricalPeak,
                    persistentCommSold::getIsResizeable,
                    persistentCommSold::getCommodityType);

                // Set the resizable flag. If there's insufficient data on the persistent entity's
                // commodity, it will be set to resizable=false and the associated ephemeral
                // entities should match this behavior.
                if (persistentCommSold.hasIsResizeable()) {
                    if (!ephemeralCommSold.hasIsResizeable() || ephemeralCommSold.getIsResizeable()) {
                        ephemeralCommSold.setIsResizeable(persistentCommSold.getIsResizeable());
                        editSummary.incrementInsufficientData();
                    }
                }

                // Copy over historical used and peak
                if (persistentCommSold.hasHistoricalPeak()) {
                    ephemeralCommSold.setHistoricalPeak(persistentCommSold.getHistoricalPeak());
                    commoditiesAdjusted = true;
                }
                if (persistentCommSold.hasHistoricalUsed()) {
                    ephemeralCommSold.setHistoricalUsed(persistentCommSold.getHistoricalUsed());
                    commoditiesAdjusted = true;
                }

                if (commoditiesAdjusted) {
                    editSummary.incrementCommodities();
                }

                // Note that we purposely do NOT copy over the UtilizationData field on
                // the commodities. The UtilizationData is a temporary field only used internally
                // in TopologyProcessor for computing the percentile values set on the
                // historical used and peak fields.
            });
        }
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

    /**
     * Helper class summarizing the edits made by the {@link EphemeralEntityEditor}.
     */
    public static class EditSummary {
        private long commoditiesAdjusted;
        private long commoditiesWithInsufficientData;
        private long inconsistentScalingGroups;
        private long totalScalingGroups;

        /**
         * Increment the number of commodities adjusted.
         */
        public void incrementCommodities() {
            commoditiesAdjusted++;
        }

        /**
         * Increment the number of commodities whose resize was disabled because of insufficient data.
         */
        public void incrementInsufficientData() {
            commoditiesWithInsufficientData++;
        }

        /**
         * Increment the number of inconsistent scaling groups detected.
         */
        private void incrementInconsistentScalingGroups() {
            inconsistentScalingGroups++;
        }

        /**
         * Increment the number total number of scaling groups.
         */
        private void incrementTotalScalingGroups() {
            totalScalingGroups++;
        }

        /**
         * Get the number of commodities adjusted.
         *
         * @return the number of commodities adjusted.
         */
        public long getCommoditiesAdjusted() {
            return commoditiesAdjusted;
        }

        /**
         * Get the number of inconsistent scaling groups detected.
         *
         * @return the number of inconsistent scaling groups detected.
         */
        public long getInconsistentScalingGroups() {
            return inconsistentScalingGroups;
        }

        /**
         * Get the number of commodities whose resize was disabled because of insufficient data.
         *
         * @return the number of commodities whose resize was disabled because of insufficient data.
         */
        public long getCommoditiesWithInsufficientData() {
            return commoditiesWithInsufficientData;
        }

        /**
         * Get the total number of scaling groups examined by the editor.
         *
         * @return the total number of scaling groups examined by the editor.
         */
        public long getTotalScalingGroups() {
            return totalScalingGroups;
        }
    }
}
