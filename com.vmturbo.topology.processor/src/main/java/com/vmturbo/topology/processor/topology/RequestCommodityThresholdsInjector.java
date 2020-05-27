package com.vmturbo.topology.processor.topology;

import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Thresholds;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Inject thresholds on request commodities to prevent resize up actions on request commodities.
 * We wish to receive resize down actions on request commodities but NOT resize up actions.
 */
public class RequestCommodityThresholdsInjector {

    private static final Set<Integer> REQUEST_COMMODITY_TYPES = ImmutableSet.of(
        CommodityType.VCPU_REQUEST_VALUE,
        CommodityType.VMEM_REQUEST_VALUE
    );

    private static final Logger logger = LogManager.getLogger();

    /**
     * Inject thresholds onto Container entity Request (VCPURequest, VMemRequest)
     * commodities to prevent the generation of resize up actions for these commodities.
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
            logger.info("Injected {} request commodity thresholds on {} entities.",
                injectionStats.getCommoditiesModified(), injectionStats.getEntitiesModified());
        }
        return injectionStats;
    }

    /**
     * Inject thresholds onto the request commodities for a particular entity in order to prevent
     * the generation of resize up actions in the market for these commodities.
     *
     * @param entity The entity whose request commodities on which we wish to inject thresholds.
     * @param stats for summarizing the changes made. Stats will be incremented depending
     *              to indicate the threhsolds injected.
     */
    private void injectThresholds(@Nonnull final TopologyEntity entity,
                                  @Nonnull final InjectionStats stats) {
        final int initialCommoditiesModified = stats.getCommoditiesModified();

        entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList().stream()
            .filter(commSold -> REQUEST_COMMODITY_TYPES.contains(commSold.getCommodityType().getType()))
            .forEach(commSold -> {
                // Set max threshold to capacity. In the market this is translated to a capacityUpperBound
                // equal to the capacity which prevents resize up actions.
                commSold.setThresholds(Thresholds.newBuilder().setMax(commSold.getCapacity()));
                stats.incrementCommoditiesModified();
            });

        if (stats.getCommoditiesModified() != initialCommoditiesModified) {
            stats.incrementEntitiesModified();
        }
    }

    /**
     * Statistics about the number of entities and commodities on which we inject thresholds.
     */
    public static class InjectionStats {
        private int entitiesModified;
        private int commoditiesModified;

        /**
         * Create a new {@link InjectionStats}.
         */
        public InjectionStats() {

        }

        private void incrementEntitiesModified() {
            entitiesModified++;
        }

        private void incrementCommoditiesModified() {
            commoditiesModified++;
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
         * Get the number of commodities modified.
         *
         * @return The number of commodities modified.
         */
        public int getCommoditiesModified() {
            return commoditiesModified;
        }
    }
}
