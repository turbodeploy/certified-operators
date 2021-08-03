package com.vmturbo.history.stats.live;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMaps;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.components.common.utils.DataPacks;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * The {@link LiveStatsStore} keeps track of stats from the most recent live topology.
 *
 * <p>We currently keep these stats in memory because they are transient, and we don't need to keep
 * them over the long term. In the future it may be worth putting it in a slightly more "stable"
 * place - e.g. a KV store, or even a database table.</p>
 */
@ThreadSafe
public class LiveStatsStore implements MemReporter {

    private final Set<CommodityDTO.CommodityType> excludedCommodities;

    private final IDataPack<Long> oidPack;

    private final Object liveStatsLock = new Object();

    private CommodityCache commodityCache;

    /**
     * Create a new {@link LiveStatsStore}.
     *
     * @param excludedCommodityTypes The list of commodities to exclude from the store. This should
     *                               be the same list we use for "source" topology stat ingestion.
     * @param oidPack                data pack for entity oids
     */
    public LiveStatsStore(@Nonnull final Set<CommodityDTO.CommodityType> excludedCommodityTypes,
                          IDataPack<Long> oidPack) {
        this.excludedCommodities = excludedCommodityTypes;
        this.oidPack = oidPack;
        this.commodityCache = new CommodityCache(excludedCommodityTypes, oidPack);
    }

    /**
     * return CommodityCache.
     *
     * @return {@link CommodityCache}
     */
    public CommodityCache getCommodityCache() {
        CommodityCache commodityCache;
        synchronized (liveStatsLock) {
            commodityCache = this.commodityCache;
        }
        return commodityCache;
    }

    /**
     * Retrieves all the cached smoothed usages for the given entity, in the form of a map of {@link
     * Integer} to capacity value.
     *
     * @param entityId id of entity whose cached smoothed usages capacities are required
     * @return map from commodityType in integer to cached smoothed usages
     */
    public Map<Integer, Double> getEntityUsageStats(long entityId) {
        CommodityCache commodityCache = getCommodityCache();
        return commodityCache.getEntityUsageStats(entityId);
    }

    /**
     * Retrieves all the cached capacities for the given entity, in the form of a map of {@link
     * Integer} to capacity value.
     *
     * @param entityId id of entity whose cached capacities are required
     * @return map from commodityType in integer to cached capacities
     */
    public Map<Integer, Double> getEntityCapacityStats(long entityId) {
        CommodityCache commodityCache = getCommodityCache();
        return commodityCache.getEntityCapacityStats(entityId);
    }

    /**
     * update commodity cache.
     *
     * @param commodityCache is the {@link CommodityCache} to update LiveStatStore with.
     */
    public void updateCommodityCache(final CommodityCache commodityCache) {
        synchronized (liveStatsLock) {
            this.commodityCache = commodityCache;
        }
    }

    /**
     * Class to store capacities for all sold commodities encountered during the processing of a
     * topology.
     *
     * <p>These may be needed to fill in capacities in the stats record created for corresponding
     * bought commodities in other entities appearing in the topology.</p>
     */
    public static class CommodityCache implements MemReporter {
        private final Set<CommodityDTO.CommodityType> excludedCommodityTypes;
        private final IDataPack<Long> oidPack;

        // entity oid (via oidPack) -> commodity type/key -> capacity
        Int2ObjectMap<Int2FloatMap> capacities = new Int2ObjectOpenHashMap<>();
        // entity oid (via oidPack) -> commodity type/key -> smoothedUsage
        Int2ObjectMap<Int2FloatMap> smoothedUsage = new Int2ObjectOpenHashMap<>();
        // data pack that allows us to use ints to represent commodity type/key
        DataPacks.CommodityTypeDataPack commodityTypePack = new DataPacks.CommodityTypeDataPack();

        /**
         * Create a new instance.
         *
         * @param excludedCommodityTypes commodity types for which we don't cache capacities
         * @param oidPack                data pack so we can represent oids as ints in data
         *                               structures
         */
        public CommodityCache(Set<CommodityDTO.CommodityType> excludedCommodityTypes, IDataPack<Long> oidPack) {
            this.excludedCommodityTypes = excludedCommodityTypes;
            this.oidPack = oidPack;
        }

        /**
         * Cache capacities for all commodities sold by the given entity.
         *
         * @param entity the entity
         */
        public void cacheUsagesAndCapacity(TopologyDTO.TopologyEntityDTO entity) {
            final Iterator<TopologyDTO.CommoditySoldDTO> soldCommodities = entity.getCommoditySoldListList().stream()
                    .filter(sold -> !excludedCommodityTypes.contains(
                            CommodityDTO.CommodityType.forNumber(sold.getCommodityType().getType())))
                    .iterator();
            if (soldCommodities.hasNext()) {
                int iid = oidPack.toIndex(entity.getOid());
                final Int2FloatMap entityCapacities = capacities.computeIfAbsent(iid,
                        _iid -> new Int2FloatOpenHashMap());
                final Int2FloatMap entitySmoothedUsageMap = smoothedUsage.computeIfAbsent(iid,
                        _iid -> new Int2FloatOpenHashMap());
                soldCommodities.forEachRemaining(sold -> {
                    final int index = commodityTypePack.toIndex(sold.getCommodityType());
                    entityCapacities.put(index, (float)sold.getCapacity());
                    if (sold.hasHistoricalUsed()) {
                        TopologyDTO.HistoricalValues historicalValues = sold.getHistoricalUsed();
                        if (historicalValues.hasMovingMeanPlusStandardDeviations()) {
                            entitySmoothedUsageMap.put(index,
                                (float)(historicalValues.getMovingMeanPlusStandardDeviations() / sold.getCapacity()));
                        } else if (historicalValues.hasHistUtilization()) {
                            // The HistUtilization attribute contains the smoothed historical used. We convert the usage into
                            // utilization and save it into the entitySmoothedUsageMap
                            entitySmoothedUsageMap.put(index,
                                (float)(historicalValues.getHistUtilization() / sold.getCapacity()));
                        }
                    }
                });
            }
        }

        /**
         * Check whether we have any cached capacities for a given entity.
         *
         * @param providerId entity OID to check
         * @return true if we have cached capacities for the entity
         */
        public boolean hasEntityCapacities(long providerId) {
            return capacities.containsKey(oidPack.toIndex(providerId));
        }

        /**
         * Get the cached capacity for the given commodity type sold by the given provider.
         *
         * @param providerId    provider entity id
         * @param commodityType commodity type whose capacity is needed
         * @return the cached capacity, or null if not found
         */
        public Double getCapacity(final long providerId, final TopologyDTO.CommodityType commodityType) {
            final Int2FloatMap entityCapacities = capacities.get(oidPack.toIndex(providerId));
            return entityCapacities != null
                    ? Double.valueOf(entityCapacities.get(commodityTypePack.toIndex(commodityType)))
                    : null;
        }

        @VisibleForTesting
        Map<Long, Map<TopologyDTO.CommodityType, Double>> getAllEntityCapacities() {
            final Map<Long, Map<TopologyDTO.CommodityType, Double>> result
                    = new Long2ObjectOpenHashMap<>();
            capacities.keySet().forEach((int iid) ->
                    result.put(oidPack.fromIndex(iid), getEntityCapacities(iid)));
            return result;
        }

        /**
         * Retrieves all the cached capacities for the given entity, in the form of a map of {@link
         * CommodityDTO.CommodityType} to capacity value.
         *
         * <p>This is used in tests, but in general, requests for individual capacity values using
         * {@link #getCapacity(long, TopologyDTO.CommodityType)} is recommended, since it does not
         * carry the overhead of constructing {@link CommodityDTO.CommodityType} instaces.</p>
         *
         * @param providerIid iid of entity whose cached capacities are required
         * @return cached capacities
         */
        @VisibleForTesting
        public Map<TopologyDTO.CommodityType, Double> getEntityCapacities(final int providerIid) {
            Map<TopologyDTO.CommodityType, Double> result = new HashMap<>();
            capacities.getOrDefault(providerIid, Int2FloatMaps.EMPTY_MAP).forEach((index, capacity) ->
                    result.put(commodityTypePack.fromIndex(index), Double.valueOf(capacity)));
            return result;
        }

        /**
         * Retrieves all the cached smoothed usages for the given entity, in the form of a map of {@link
         * CommodityDTO.CommodityType} to smoothed usage value.
         *
         *
         * @param entityId id of entity whose cached smoothed usages are required
         * @return cached smoothed usages
         */
        @VisibleForTesting
        public Map<Integer, Double> getEntityUsageStats(final long entityId) {
            Map<Integer, Double> result = new HashMap<>();
            smoothedUsage.getOrDefault(oidPack.toIndex(entityId), Int2FloatMaps.EMPTY_MAP).forEach((index, usage) ->
                    result.put(commodityTypePack.fromIndex(index).getType(), Double.valueOf(usage)));
            return result;
        }

        /**
         * Retrieves all the cached capacities for the given entity, in the form of a map of {@link
         * CommodityDTO.CommodityType} to capacity value.
         *
         *
         * @param entityId id of entity whose cached capacities are required
         * @return cached capacities
         */
        @VisibleForTesting
        public Map<Integer, Double> getEntityCapacityStats(final long entityId) {
            Map<Integer, Double> result = new HashMap<>();
            capacities.getOrDefault(oidPack.toIndex(entityId), Int2FloatMaps.EMPTY_MAP).forEach((index, capacity) ->
                    result.put(commodityTypePack.fromIndex(index).getType(), Double.valueOf(capacity)));
            return result;
        }

        @Override
        public Long getMemSize() {
            return null;
        }

        @Override
        public List<MemReporter> getNestedMemReporters() {
            return ImmutableList.of(
                    new SimpleMemReporter("capacities", capacities),
                    new SimpleMemReporter("smoothedUsages", smoothedUsage),
                    new SimpleMemReporter("commodityTypePack", commodityTypePack)
            );
        }
    }

    @Override
    public List<MemReporter> getNestedMemReporters() {
        return Arrays.asList(
                commodityCache
        );
    }
}
