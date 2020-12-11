package com.vmturbo.history.stats;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Multimap;

import org.apache.logging.log4j.LogManager;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkInserter;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.DelayedCommodityBoughtWriter;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.MarketStatsData;
import com.vmturbo.history.stats.live.LiveStatsAggregator.CapacityCache;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Interface implemented by {@link MarketStatsAccumulatorImpl} class.
 *
 * <p>The interface is broken out here specifically to make it easy to create a dummy instance
 * when the real thing cannot be created (e.g. because the entity type for which it is requested
 * is not associated with any stats tables). Default method implementations fully implement the
 * dummy objects.</p>
 */
public interface MarketStatsAccumulator {

    /**
     * Obtain per-entity-type stats data accumulated by this accumulator.
     *
     * @return stats data objects
     */
    default Collection<MarketStatsData> values() {
        return Collections.emptyList();
    }

    /**
     * Process an entity from the topology, writing per-entity stats records for commodities and
     * attributes, and aggregating values for per-entity-type market-wide stats.
     *
     * @param entityDTO                the entity to be processed
     * @param capacityCache            cache of capacities recorded for entities processed so far
     * @param delayedCommoditiesBought information for bought commodities for which capacities were
     *                                 not available at the time of processing; they will be
     *                                 handled when the needed capacities show up
     * @param entityByOid              map of entity id to entity
     * @throws InterruptedException if interrupted
     */
    default void recordEntity(@Nonnull TopologyEntityDTO entityDTO,
            @Nonnull CapacityCache capacityCache,
            @Nonnull Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought,
            @Nonnull Map<Long, TopologyEntityDTO> entityByOid) throws InterruptedException {
    }

    /**
     * Write market-wide per-entity-type stats to the database.
     *
     * @throws InterruptedException if interrupted
     */
    default void writeFinalStats() throws InterruptedException {
    }

    /**
     * Create an object to accumulate min / max / total / capacity over the commodities for a given
     * EntityType.
     *
     * @param topologyInfo           topology info
     * @param entityType             the type of entity for which these stats are being accumulated.
     *                               A given stat may be bought and sold be different entities. We
     *                               must record those usages separately.
     * @param environmentType        environment type
     * @param historydbIO            DBIO handler for the History tables
     * @param excludedCommodityTypes a list of commodity names used by the market but not necessary
     *                               to be persisted as stats in the db
     * @param loaders                {@link SimpleBulkLoaderFactory} from which needed {@link
     *                               BulkInserter} objects
     * @param longCommodityKeys      where to store commodity keys that had to be shortened, for
     *                               consolidated logging
     * @return the new market stats accumulator, or a dummy if a real one could not be created
     */
    static MarketStatsAccumulator create(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final String entityType,
            @Nonnull final EnvironmentType environmentType,
            @Nonnull final HistorydbIO historydbIO,
            @Nonnull final Set<CommodityType> excludedCommodityTypes,
            @Nonnull final SimpleBulkLoaderFactory loaders,
            @Nonnull final Set<String> longCommodityKeys) {
        try {
            return new MarketStatsAccumulatorImpl(topologyInfo, entityType, environmentType,
                    historydbIO, excludedCommodityTypes, loaders, longCommodityKeys);
        } catch (IllegalArgumentException e) {
            LogManager.getLogger().error(
                    "Failed to create market stats accumulator; "
                            + "stats will not be aggregated for entity type {};", entityType);
            return new MarketStatsAccumulator() {
            };
        }
    }
}
