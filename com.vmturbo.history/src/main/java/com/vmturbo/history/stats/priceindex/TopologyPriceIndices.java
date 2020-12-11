package com.vmturbo.history.stats.priceindex;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * A utility class to group the price indices for a topology by multiple parameters, and
 * iterate over them easily.
 *
 * <p>The price index for the original topology arrives together with the PROJECTED topology, because
 * it is the market that calculates it as part of the market analysis. We build up the
 * {@link TopologyPriceIndices}, and can then use a {@link TopologyPriceIndexVisitor} to write
 * the entries into the database.</p>
 */
public class TopologyPriceIndices {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyInfo topologyInfo;

    private final Map<Integer, Map<EnvironmentType, Map<Long, Double>>> priceIndexMap;

    /**
     * The total number of entities in the topology that appeared in the projected topology and
     * had a price index specified.
     */
    private final int numEntities;

    /**
     * The total number of entities in the topology that had no price index specified.
     */
    private final int numSkipped;

    private TopologyPriceIndices(@Nonnull final TopologyInfo topologyInfo,
            @Nonnull final Map<Integer, Map<EnvironmentType, Map<Long, Double>>> priceIndexMap,
            final int numEntities,
            final int numSkipped) {
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.priceIndexMap = Collections.unmodifiableMap(Objects.requireNonNull(priceIndexMap));
        this.numEntities = numEntities;
        this.numSkipped = numSkipped;
    }

    /**
     * Visit the {@link TopologyPriceIndices} with a particular {@link TopologyPriceIndexVisitor}
     * implementation.
     *
     * <p>If the visitor throws any error, the entire visiting process will be abandoned.</p>
     *
     * @param visitor The {@link TopologyPriceIndexVisitor}.
     * @throws InterruptedException if interrupted
     */
    public void visit(@Nonnull final TopologyPriceIndexVisitor visitor) throws InterruptedException {
        final DataMetricTimer timer = SharedMetrics.UPDATE_PRICE_INDEX_DURATION_SUMMARY
            .labels(SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
            .startTimer();
        logger.debug("Persisting priceIndex info for context: {}, topology: {}," +
                " count: {}, skipped entities: {}",
            topologyInfo.getTopologyContextId(),
            topologyInfo.getTopologyId(),
            numEntities, numSkipped);

        try {
            for (final Entry<Integer, Map<EnvironmentType, Map<Long, Double>>> entityTypeEntry :
                    priceIndexMap.entrySet()) {
                final Integer entityType = entityTypeEntry.getKey();
                for (final Entry<EnvironmentType, Map<Long, Double>> envTypeEntry :
                        entityTypeEntry.getValue().entrySet()) {
                    visitor.visit(entityType, envTypeEntry.getKey(),
                        Collections.unmodifiableMap(envTypeEntry.getValue()));
                }
            }
            visitor.onComplete();
        } catch (VmtDbException e) {
            logger.error("Error creating connection to persist PriceIndex information to DB", e);
        } catch (RuntimeException e) {
            logger.error("Failed to save PriceIndex information due to exception.", e);
        }

        final double durationSec = timer.observe();

        logger.debug("Done persisting priceIndex info for context: {}, topology: {}," +
                        "time {} sec", topologyInfo.getTopologyContextId(), topologyInfo.getTopologyId(),
                durationSec);
    }

    /**
     * Create a new price indices builder for a topology.
     *
     * @param topologyInfo topology info
     * @return new builder
     */
    @Nonnull
    public static Builder builder(@Nonnull final TopologyInfo topologyInfo) {
        return new Builder(topologyInfo);
    }

    /**
     * Class to construct a new price indices structure for a topology.
     */
    public static class Builder {

        private final TopologyInfo topologyInfo;

        private final Map<Integer, Map<EnvironmentType, Map<Long, Double>>> priceIndexMap = new HashMap<>();

        private int numEntities = 0;

        private int numSkipped = 0;

        /**
         * Create a new builder instance.
         *
         * <p>Code should use {@link TopologyPriceIndices#builder(TopologyInfo)}.</p>
         *
         * @param topologyInfo topology
         */
        private Builder(final TopologyInfo topologyInfo) {
            this.topologyInfo = Objects.requireNonNull(topologyInfo);
        }

        /**
         * Incorporate an entity from the topology into the price index structure, if applicable.
         *
         * @param projectedTopologyEntity entity to be added
         * @return this builder
         */
        public Builder addEntity(@Nonnull final ProjectedTopologyEntity projectedTopologyEntity) {
            final double priceIdx;
            // We do not want to write stats for entities originated by the market
            if (projectedTopologyEntity.getEntity().getOrigin().hasAnalysisOrigin()) {
                return this;
            }
            if (projectedTopologyEntity.hasOriginalPriceIndex()) {
                priceIdx = projectedTopologyEntity.getOriginalPriceIndex();
                numEntities++;
            } else {
                priceIdx = HistoryStatsUtils.DEFAULT_PRICE_IDX;
                numSkipped++;
            }
            setPriceIdx(projectedTopologyEntity.getEntity().getEntityType(),
                    projectedTopologyEntity.getEntity().getEnvironmentType(),
                    projectedTopologyEntity.getEntity().getOid(),
                    priceIdx);
            return this;
        }

        private void setPriceIdx(final int entityType,
                @Nonnull final EnvironmentType envType,
                final long id,
                final double priceIdx) {
            final Map<EnvironmentType, Map<Long, Double>> idxByIdAndEnvType =
                    priceIndexMap.computeIfAbsent(entityType, k -> new HashMap<>());
            final Map<Long, Double> idxById = idxByIdAndEnvType.computeIfAbsent(
                    envType, k -> new HashMap<>());
            idxById.put(id, priceIdx);
        }


        /**
         * Build the price index structure.
         *
         * @return the new price index structure
         */
        public TopologyPriceIndices build() {
            return new TopologyPriceIndices(topologyInfo, priceIndexMap, numEntities, numSkipped);
        }
    }
}
