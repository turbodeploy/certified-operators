package com.vmturbo.history.stats.live;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.MarketStatsAccumulator;
import com.vmturbo.history.stats.MarketStatsAccumulator.DelayedCommodityBoughtWriter;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Handles receiving realtime topology by chunks, and while doing so
 * aggregates various realtime topology stats.
 *
 */
public class LiveStatsAggregator {

    private final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    private final TopologyOrganizer topologyOrganizer;

    /**
     * Each string should be a value of {@link CommodityTypes}.
     */
    private final ImmutableList<String> commoditiesToExclude;

    private final int writeTopologyChunkSize;

    /**
     * Cache for commodities sold capacities.
     * First key is provider oid, second key is commodity (numerical type) and value is the capacity.
     */
    private Map<Long, Map<Integer, Double>> capacities = Maps.newHashMap();
    /**
     * Reusable capacity maps. The assumption is that there may be multiple providers selling
     * commodities with exactly the same set of capacities. For example hosts in the same cluster,
     * VMs deployed from the same template etc. Since we cache the capacities of all providers,
     * in case we hit a consumer later, it could be efficient to reuse the maps when possible.
     */
    private Map<Map<Integer, Double>, Map<Integer, Double>> reusableCapacityMaps = Maps.newHashMap();
    /**
     * Market statistics accumulators keyed by base entity type.
     */
    private Map<String, MarketStatsAccumulator> accumulators = Maps.newHashMap();
    /**
     * Map from numerical entity type to {@link EntityType}.
     */
    private Map<Integer, Optional<EntityType>> entityTypes = Maps.newHashMap();
    /**
     * Map from numerical entity type to (String) base entity type.
     */
    private Map<Integer, Optional<String>> baseEntityTypes = Maps.newHashMap();
    /**
     * Number of entities by base entity type.
     */
    private Map<String, Counter> perTypeCounters = Maps.newHashMap();

    /**
     * This map is used when a commodity bought list is received but the provider
     * is not yet known. The key is the provider oid and the value is a list of
     * functions that would be executed when the provider capacities become available.
     */
    private Multimap<Long, MarketStatsAccumulator.DelayedCommodityBoughtWriter> delayedCommoditiesBought
            = HashMultimap.create();

    /**
     * Metrics used for counting DB operations so as to better understand DB performance.
     */


    public LiveStatsAggregator(HistorydbIO historydbIO,
                               TopologyOrganizer topologyOrganizer,
                               ImmutableList<String> commoditiesToExclude,
                               int writeTopologyChunkSize) {
        this.topologyOrganizer = topologyOrganizer;
        this.historydbIO = historydbIO;
        this.commoditiesToExclude = commoditiesToExclude;
        this.writeTopologyChunkSize = writeTopologyChunkSize;
    }

    /**
     * Cache commodity sold capacities of an entity.
     * @param entityDTO the entity which sold commodities capacities are added to the cache.
     */
    private void cacheCapacities(TopologyEntityDTO entityDTO) {
        Map<Integer, Double> map = entityDTO.getCommoditySoldListList().stream()
            .collect(Collectors.toMap(commSold -> (commSold.getCommodityType().getType()),
                    CommoditySoldDTO::getCapacity, (k1, k2) -> k1));

        if (!map.isEmpty()) {
            // If such a map already exists then reuse it, otherwise put it in the map.
            Map<Integer, Double> existingMap = reusableCapacityMaps.computeIfAbsent(map, key -> map);
            capacities.put(entityDTO.getOid(), existingMap);
        }
    }

    /**
     * Write commodities bought rows to the DB for commodities bought that were received before
     * their provider DTO (and therefore their sold capacities) was available. Now that we
     * processed the provider sold commodities, we can write these rows.
     * @param providerId the provider OID
     * @throws VmtDbException when there is a problem writing to the DD.
     */
    private void handleDelayedCommoditiesBought(Long providerId) throws VmtDbException {
        Collection<DelayedCommodityBoughtWriter> list = delayedCommoditiesBought.get(providerId);
        if (!list.isEmpty()) { // Multimap.get is never null
            for (DelayedCommodityBoughtWriter action : list) {
                // call the delayed action
                action.queCommoditiesNow();
            }
            delayedCommoditiesBought.removeAll(providerId);
        }
    }

    /**
     * Gather entity stats to be persisted into chunks by entity type. When chunks are full,
     * write them to the database.
     *
     * @param entityDTO a topology entity DTO
     * @throws VmtDbException if writing to the DB fails.
     */
    private void aggregateEntityStats(TopologyEntityDTO entityDTO)
                    throws VmtDbException {
        int sdkEntityType = entityDTO.getEntityType();
        // determine the DB Entity Type for this SDK Entity Type
        Optional<EntityType> entityDBInfo =
            entityTypes.computeIfAbsent(sdkEntityType, historydbIO::getEntityType);
        if (!entityDBInfo.isPresent()) {
            logger.debug("DB info for entity type {}[{}] not present.",
                    EntityDTO.EntityType.forNumber(sdkEntityType), sdkEntityType);
            return;
        }

        // Capture the base entity type without aliasing, used for the market_stats_xxx table
        // The market_stats_last entries MUST NOT alias DataCenter with PhysicalMachine.
        // This is why we require "baseEntityType".
        Optional<String> baseEntityTypeOptional = baseEntityTypes.computeIfAbsent(sdkEntityType,
                historydbIO::getBaseEntityType);
        if (!baseEntityTypeOptional.isPresent()) {
            return;
        }
        final String baseEntityType = baseEntityTypeOptional.get();
        MarketStatsAccumulator marketStatsAccumulator = accumulators.computeIfAbsent(baseEntityType,
                type -> new MarketStatsAccumulator(type, historydbIO, writeTopologyChunkSize,
                        commoditiesToExclude));

        long snapshotTime = topologyOrganizer.getSnapshotTime();
        marketStatsAccumulator.persistCommoditiesBought(snapshotTime, entityDTO, capacities,
                delayedCommoditiesBought);
        marketStatsAccumulator.persistCommoditiesSold(snapshotTime, entityDTO.getOid(),
            entityDTO.getCommoditySoldListList());
        marketStatsAccumulator.persistEntityAttributes(snapshotTime, entityDTO);
        perTypeCounters.computeIfAbsent(baseEntityType, key -> new Counter()).increment();
    }

    /**
     * The number of pending commodities bought maps.
     * @return then number of pending commodities bought maps
     */
    @VisibleForTesting
    protected int numPendingBought() {
        return delayedCommoditiesBought.size();
    }

    /**
     * The sold commodities cache.
     * @return the sold commodities cache
     */
    @VisibleForTesting
    protected Map<Long, Map<Integer, Double>> capacities() {
        return Collections.unmodifiableMap(capacities);
    }



    /**
     * Persist the various per-entity-type aggregate stats and then write all remaining queued
     * stats rows in case there are partial chunks. Called when done handling the
     * incoming message chunks.
     *
     * @throws VmtDbException when there is a problem writing to the DB.
     */
    public void writeFinalStats() throws VmtDbException {
        for (Entry<String, MarketStatsAccumulator> statsEntry : accumulators.entrySet()) {
            String baseType = statsEntry.getKey();
            MarketStatsAccumulator statsAccumulator = statsEntry.getValue();
            statsAccumulator.writeQueuedRows();
            statsAccumulator.persistMarketStats(perTypeCounters.get(baseType).value(), topologyOrganizer);
        }
    }

    /**
     * Record stats information from the given {@link EntityDTO}. Stats data will be
     * batched to optimize the number of commands sent to the Relational Database.
     *
     * @param entityDTO the {@link EntityDTO} to records the stats from
     * @throws VmtDbException if there is an error persisting to the database
     */
    public void aggregateEntity(TopologyEntityDTO entityDTO) throws VmtDbException {
        // save commodity sold capacities for filling other commodity bought capacities
        cacheCapacities(entityDTO);
        // provide commodity sold capacitites for previously unsatisfied commodity bought
        handleDelayedCommoditiesBought(entityDTO.getOid());
        // schedule the stats for the entity for persisting to db
        aggregateEntityStats(entityDTO);
    }

    /**
     * A counter.
     *
     */
    private static class Counter {

        private int value = 0;

        /**
         * Get the value of the counter.
         *
         * @return the value of the counter
         */
        private int value() {
            return value;
        }

        /**
         * Increment the counter by 1.
         *
         */
        private void increment() {
            value++;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }
}
