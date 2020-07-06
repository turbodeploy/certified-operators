package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Inject thresholds on request commodities to prevent resize up actions on request commodities.
 * We wish to receive resize down actions on request commodities but NOT resize up actions.
 * <p/>
 * Inject thresholds onto limit commodities where request is present to prevent resizing
 * down limits below requests.
 */
public class RequestAndLimitCommodityThresholdsInjector {

    private static final Set<Integer> REQUEST_COMMODITY_TYPES = ImmutableSet.of(
        CommodityType.VCPU_REQUEST_VALUE,
        CommodityType.VMEM_REQUEST_VALUE
    );

    private static final Set<Integer> LIMIT_COMMODITY_TYPES = ImmutableSet.of(
        CommodityType.VCPU_VALUE,
        CommodityType.VMEM_VALUE
    );

    private static final Map<Integer, Integer> REQUEST_TO_LIMIT_MAP = ImmutableMap.of(
        CommodityType.VCPU_REQUEST_VALUE, CommodityType.VCPU_VALUE,
        CommodityType.VMEM_REQUEST_VALUE, CommodityType.VMEM_VALUE
    );

    private static final Logger logger = LogManager.getLogger();

    /**
     * Inject thresholds onto Container entity Request (VCPURequest, VMemRequest)
     * commodities to prevent the generation of resize up actions for these commodities.
     * <p/>
     * Inject thresholds onto limit commodities where request is present to prevent resizing
     * down limits below requests.
     *
     * @param graph The {@link TopologyGraph} containing all the entities in
     *              the topology and their relationships.
     * @return {@link InjectionStats} summarizing the changes made.
     */
    public InjectionStats injectThresholds(@Nonnull final TopologyGraph<TopologyEntity> graph) {
        final InjectionStats injectionStats = new InjectionStats();
        graph.entitiesOfType(EntityType.CONTAINER.getNumber()).forEach(entity ->
            injectThresholds(entity, injectionStats));

        if (injectionStats.entitiesModified > 0) {
            logger.info("Injected {} request commodity thresholds and {} limit "
                    + "commodity thresholds on {} entities.",
                injectionStats.getRequestCommoditiesModified(),
                injectionStats.getLimitCommoditiesModified(),
                injectionStats.getEntitiesModified());
        }
        return injectionStats;
    }

    /**
     * Inject thresholds onto the request commodities for a particular entity in order to prevent
     * the generation of resize up actions in the market for these commodities.
     * <p/>
     * Inject thresholds onto limit commodities where request is present to prevent resizing
     * down limits below requests.
     *
     * @param entity The entity whose request commodities on which we wish to inject thresholds.
     * @param stats for summarizing the changes made. Stats will be incremented depending
     *              to indicate the threhsolds injected.
     */
    private void injectThresholds(@Nonnull final TopologyEntity entity,
                                  @Nonnull final InjectionStats stats) {
        final int initialCommoditiesModified = stats.getRequestCommoditiesModified();
        final List<CommoditySoldDTO.Builder> requests = new ArrayList<>(REQUEST_COMMODITY_TYPES.size());
        final List<CommoditySoldDTO.Builder> limits = new ArrayList<>(LIMIT_COMMODITY_TYPES.size());

        // Find the request and limit commodities to be modified
        for (CommoditySoldDTO.Builder comm : entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()) {
            if (REQUEST_COMMODITY_TYPES.contains(comm.getCommodityType().getType())) {
                requests.add(comm);
            } else if (LIMIT_COMMODITY_TYPES.contains(comm.getCommodityType().getType())
                && comm.getIsResizeable()) {
                // We only care about limit commodities that are resizable=true. If resizable=false, that
                // indicates the user has not set a limit on the commodity.
                limits.add(comm);
            }
        }

        // Update request and limit commodities
        for (CommoditySoldDTO.Builder requestComm : requests) {
            // Set max threshold to capacity. In the market this is translated to a capacityUpperBound
            // equal to the capacity which prevents resize up actions.
            requestComm.setThresholds(Thresholds.newBuilder().setMax(requestComm.getCapacity()));
            stats.incrementRequestCommoditiesModified();

            // If there's a corresponding limit commodity for the request commodity, prevent the
            // market from resizing the limit below the request which is not permitted.
            matchingLimit(requestComm.getCommodityType().getType(), limits).ifPresent(limitComm -> {
                limitComm.setThresholds(Thresholds.newBuilder().setMin(requestComm.getCapacity()));
                stats.incrementLimitCommoditiesModified();
            });
        }

        if (stats.getRequestCommoditiesModified() != initialCommoditiesModified) {
            stats.incrementEntitiesModified();
        }
    }

    private Optional<CommoditySoldDTO.Builder> matchingLimit(final int requestCommodityType,
                                                             @Nonnull final List<CommoditySoldDTO.Builder> limits) {
        final int limitCommodityType = REQUEST_TO_LIMIT_MAP.getOrDefault(requestCommodityType, -1);
        if (limitCommodityType >= 0) {
            return limits.stream()
                .filter(limitComm -> limitComm.getCommodityType().getType() == limitCommodityType)
                .findAny();
        } else {
            logger.error("No matching limit commodity type for request commodity type {}",
                CommodityType.forNumber(requestCommodityType));
            return Optional.empty();
        }
    }

    /**
     * Statistics about the number of entities and commodities on which we inject thresholds.
     */
    public static class InjectionStats {
        private int entitiesModified;
        private int requestCommoditiesModified;
        private int limitCommoditiesModified;

        /**
         * Create a new {@link InjectionStats}.
         */
        public InjectionStats() {

        }

        private void incrementEntitiesModified() {
            entitiesModified++;
        }

        private void incrementRequestCommoditiesModified() {
            requestCommoditiesModified++;
        }

        private void incrementLimitCommoditiesModified() {
            limitCommoditiesModified++;
        }

        /**
         * Get the number of entities modified.
         *
         * @return The number of entities modified.
         */
        public int getEntitiesModified() {
            return entitiesModified;
        }

        /**
         * Get the number of request commodities modified.
         *
         * @return The number of request commodities modified.
         */
        public int getRequestCommoditiesModified() {
            return requestCommoditiesModified;
        }

        /**
         * Get the number of limit commodities modified.
         *
         * @return The number of limit commodities modified.
         */
        public int getLimitCommoditiesModified() {
            return limitCommoditiesModified;
        }
    }
}
