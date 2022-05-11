package com.vmturbo.history.stats.live;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

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

    @Override
    public List<MemReporter> getNestedMemReporters() {
        return Arrays.asList(
                commodityCache
        );
    }
}
