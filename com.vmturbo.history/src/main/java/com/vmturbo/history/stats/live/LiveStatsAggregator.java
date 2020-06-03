package com.vmturbo.history.stats.live;

import static gnu.trove.impl.Constants.DEFAULT_CAPACITY;
import static gnu.trove.impl.Constants.DEFAULT_LOAD_FACTOR;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.stats.MarketStatsAccumulator;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.DelayedCommodityBoughtWriter;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Handles receiving realtime topology by chunks, and while doing so
 * aggregates various realtime topology stats.
 */
public class LiveStatsAggregator {

    private final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    private final TopologyInfo topologyInfo;

    /**
     * Each string should be a value of {@link CommodityType}.
     */
    private final Set<String> commoditiesToExclude;

    // supplier of loaders for the records we produce
    private final SimpleBulkLoaderFactory loaders;

    /**
     * Cache for sold commodity capacities, so bought commodity records can include seller capacities.
     */
    private CapacityCache capacityCache = new CapacityCache();

    /**
     * Commodity keys that exceeded their max allowed length, encountered during the lifetime of
     * this {@link LiveStatsAggregator instance}.
     */
    Set<String> longCommodityKeys = new HashSet<>();

    /**
     * {@link MarketStatsAccumulator}s by base entity type and environment type.
     */
    private Table<String, EnvironmentType, MarketStatsAccumulator> accumulatorsByEntityAndEnvType = HashBasedTable.create();
    /**
     * Map from numerical entity type to {@link EntityType}.
     */
    private Map<Integer, Optional<EntityType>> entityTypes = Maps.newHashMap();
    /**
     * Map from numerical entity type to (String) base entity type.
     */
    private Map<Integer, Optional<String>> baseEntityTypes = Maps.newHashMap();

    /**
     * This map is used when a commodity bought list is received but the provider
     * is not yet known. The key is the provider oid and the value is a list of
     * functions that would be executed when the provider capacities become available.
     */
    private Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought
            = HashMultimap.create();

    /**
     * Metrics used for counting DB operations so as to better understand DB performance.
     *
     * @param historydbIO          db methods
     * @param topologyInfo         topology info
     * @param commoditiesToExclude commodities for which we will not record metrics
     * @param loaders              bulk loader factory
     */
    public LiveStatsAggregator(@Nonnull final HistorydbIO historydbIO,
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull Set<String> commoditiesToExclude,
            @Nonnull SimpleBulkLoaderFactory loaders) {
        this.historydbIO = historydbIO;
        this.topologyInfo = topologyInfo;
        this.commoditiesToExclude = commoditiesToExclude;
        this.loaders = loaders;
    }

    /**
     * Cache commodity sold capacities of an entity.
     *
     * @param entityDTO the entity which sold commodities capacities are added to the cache.
     */
    private void cacheCapacities(TopologyEntityDTO entityDTO) {
        // for cloud volumes, the capacity is available in the volume info
        if (HistoryStatsUtils.isCloudEntity(entityDTO) &&
                entityDTO.getEntityType() == EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE) {
            // todo: currently volume doesn't sell any commodities, the capacity is stored as
            // properties, we should remove this logic once volume starts selling commodities
            if (entityDTO.hasTypeSpecificInfo() && entityDTO.getTypeSpecificInfo().hasVirtualVolume()) {
                VirtualVolumeInfo volume = entityDTO.getTypeSpecificInfo().getVirtualVolume();
                capacityCache.cacheCapacity(
                        entityDTO.getOid(), CommodityType.STORAGE_AMOUNT.getNumber(), volume.getStorageAmountCapacity());
                capacityCache.cacheCapacity(
                        entityDTO.getOid(), CommodityType.STORAGE_ACCESS.getNumber(), volume.getStorageAccessCapacity());
                capacityCache.cacheCapacity(
                    entityDTO.getOid(), CommodityType.IO_THROUGHPUT.getNumber(), volume.getIoThroughputCapacity());
            } else {
                logger.warn("Capacity info is missing for volume {}", entityDTO.getOid());
            }
        } else {
            capacityCache.cacheCapacities(entityDTO);
        }
    }

    /**
     * Write commodities bought rows to the DB for commodities bought that were received before
     * their provider DTO (and therefore their sold capacities) was available. Now that we
     * processed the provider sold commodities, we can write these rows.
     *
     * @param providerId the provider OID
     * @throws InterruptedException if interrupted
     */
    private void handleDelayedCommoditiesBought(Long providerId) throws InterruptedException {
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
     * @param entityDTO   a topology entity DTO
     * @param entityByOid map of entities by OID
     * @throws InterruptedException if interrupted
     */
    private void aggregateEntityStats(TopologyEntityDTO entityDTO,
            Map<Long, TopologyEntityDTO> entityByOid) throws InterruptedException {
        final int sdkEntityType = entityDTO.getEntityType();
        // determine the DB Entity Type for this SDK Entity Type
        final Optional<EntityType> entityDBInfo =
                entityTypes.computeIfAbsent(sdkEntityType, historydbIO::getEntityType);
        if (!entityDBInfo.isPresent()) {
            logger.debug("DB info for entity type {}[{}] not present.",
                    EntityDTO.EntityType.forNumber(sdkEntityType), sdkEntityType);
            return;
        }

        // Capture the base entity type without aliasing, used for the market_stats_xxx table
        // The market_stats_last entries MUST NOT alias DataCenter with PhysicalMachine.
        // This is why we require "baseEntityType".
        final Optional<String> baseEntityTypeOptional = baseEntityTypes.computeIfAbsent(sdkEntityType,
                historydbIO::getBaseEntityType);
        if (!baseEntityTypeOptional.isPresent()) {
            return;
        }
        final String baseEntityType = baseEntityTypeOptional.get();

        MarketStatsAccumulator marketStatsAccumulator =
                accumulatorsByEntityAndEnvType.get(baseEntityType, entityDTO.getEnvironmentType());
        if (marketStatsAccumulator == null) {
            marketStatsAccumulator = MarketStatsAccumulator.create(topologyInfo, baseEntityType,
                    entityDTO.getEnvironmentType(), historydbIO, commoditiesToExclude, loaders,
                    longCommodityKeys);
            accumulatorsByEntityAndEnvType.put(baseEntityType, entityDTO.getEnvironmentType(), marketStatsAccumulator);
        }

        marketStatsAccumulator.recordEntity(entityDTO, capacityCache, delayedCommoditiesBought, entityByOid);
    }

    /**
     * The number of pending commodities bought maps.
     *
     * @return then number of pending commodities bought maps
     */
    @VisibleForTesting
    int numPendingBought() {
        return delayedCommoditiesBought.size();
    }

    /**
     * The sold commodities cache.
     *
     * @return the sold commodities cache
     */
    @VisibleForTesting
    CapacityCache capacities() {
        return capacityCache;
    }

    /**
     * Persist the various per-entity-type aggregate stats and then write all remaining queued
     * stats rows in case there are partial chunks. Called when done handling the
     * incoming message chunks.
     *
     * @throws VmtDbException when there is a problem writing to the DB.
     * @throws InterruptedException if interrupted
     */
    public void writeFinalStats() throws VmtDbException, InterruptedException {
        for (final MarketStatsAccumulator statsAccumulator : accumulatorsByEntityAndEnvType.values()) {
            statsAccumulator.writeFinalStats();
        }
    }

    /**
     * Log a list of commodities that were shortened to fit in the database, if any.
     */
    public void logShortenedCommodityKeys() {
        if (!longCommodityKeys.isEmpty()) {
            logger.error("Following commodity keys needed to be shortened when persisted; " +
                    "data access anomalies may result: {}", longCommodityKeys);
        }
    }

    /**
     * Record stats information from the given {@link EntityDTO}. Stats data will be
     * batched to optimize the number of commands sent to the Relational Database.
     *
     * @param entityDTO   the {@link EntityDTO} to records the stats from
     * @param entityByOid map of entities by oid
     * @throws InterruptedException if interrupted
     */
    public void aggregateEntity(TopologyEntityDTO entityDTO,
            Map<Long, TopologyEntityDTO> entityByOid) throws InterruptedException {
        // save commodity sold capacities for filling other commodity bought capacities
        cacheCapacities(entityDTO);
        // provide commodity sold capacitites for previously unsatisfied commodity bought
        handleDelayedCommoditiesBought(entityDTO.getOid());
        if (EntityType.fromSdkEntityType(entityDTO.getEntityType())
                .map(EntityType::persistsStats)
                .orElse(false)) {
            // schedule the stats for the entity for persisting to db
            aggregateEntityStats(entityDTO, entityByOid);
        }
    }

    /**
     * Class to store capacities for all sold commodities encountered during the processing of a topology.
     *
     * <p>These may be needed to fill in capacities in the stats record created for corresponding bought
     * commodities in other entities appearing in the topology.</p>
     */
    public static class CapacityCache {
        // we're using trove collections to reduce memory footprint. They avoid boxing primitives.
        // overall map can use default size and load factor, and since OIDs cannot be negative, we
        // use a negative value for no-entry
        TLongObjectMap<TIntObjectMap<TObjectDoubleMap<String>>> capacities
                = new TLongObjectHashMap<>(DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L);

        // Reusable capacity maps. The assumption is that there may be multiple providers selling
        // commodities with exactly the same set of capacities. For example hosts in the same cluster,
        // VMs deployed from the same template etc. To reduce overall memory footprint we use shared
        // capacity structures among entities whose structures end up identical.
        private Map<TIntObjectMap<TObjectDoubleMap<String>>, TIntObjectMap<TObjectDoubleMap<String>>>
                reusableCapacityMaps = new HashMap<>();

        /**
         * Cache capacities for all commodities sold by the given entity.
         *
         * @param entity the entity
         */
        public void cacheCapacities(TopologyEntityDTO entity) {
            if (entity.getCommoditySoldListCount() > 0) {
                long oid = entity.getOid();
                ensureCommTypeMap(oid, entity.getCommoditySoldListCount());
                entity.getCommoditySoldListList().forEach(comm -> {
                    int type = comm.getCommodityType().getType();
                    String key = comm.getCommodityType().getKey();
                    double capacity = comm.getCapacity();
                    cacheCapacity(oid, type, key, capacity);
                });
                // check whether we have an identical capacities map for any prior entities
                TIntObjectMap<TObjectDoubleMap<String>> entityCapacities = capacities.get(oid);
                TIntObjectMap<TObjectDoubleMap<String>> reusableEntityCapacities
                        = reusableCapacityMaps.get(entityCapacities);
                if (reusableEntityCapacities != null) {
                    // yes, replace ours with the shared instance
                    capacities.put(oid, reusableEntityCapacities);
                } else {
                    // nope, remember this one for possible reuse
                    reusableCapacityMaps.put(entityCapacities, entityCapacities);
                }
            }
        }

        /**
         * Cache a sold commodity capacity.
         *
         * @param oid      sellilng entity OID
         * @param type     commodity type
         * @param key      commodity key, may be null
         * @param capacity seller's capacity
         */
        public void cacheCapacity(long oid, int type, @Nullable String key, double capacity) {
            TIntObjectMap<TObjectDoubleMap<String>> commTypeMap = ensureCommTypeMap(oid, 1);
            TObjectDoubleMap<String> commKeyMap = commTypeMap.get(type);
            if (commKeyMap == null) {
                // we mostly only ever need one entry and no growth, and we'll use -1.0 to mean
                // no entry
                commKeyMap = new TObjectDoubleHashMap<>(1, 1f, -1.0);
                commTypeMap.put(type, commKeyMap);
            }
            // protobuf default for a string field is "", so we'll use that here too
            commKeyMap.put(key != null ? key : "", capacity);
        }

        /**
         * Cache a sold capacity without a commodity key.
         *
         * @param oid      selling entity OID
         * @param type     commodity type
         * @param capacity seller's capacity
         */
        public void cacheCapacity(long oid, int type, double capacity) {
            cacheCapacity(oid, type, null, capacity);
        }

        /**
         * Check whehter we have any cached capacities for a given entity.
         *
         * @param providerId entity OID to check
         * @return true if we have cached capacities for the entity
         */
        public boolean hasEntityCapacities(Long providerId) {
            return capacities.containsKey(providerId);
        }

        /**
         * Get the cached capacities map for a given entity.
         *
         * @param providerId entity OID
         * @return that entity's cached sold capacities, or null if none
         */
        public TIntObjectMap<TObjectDoubleMap<String>> getEntityCapacities(long providerId) {
            return capacities.get(providerId);
        }

        /**
         * Allocate a new entry for the given oid if it's not already present in the cache.
         *
         * <p>The given size will be used as the initial map capacity, and since we hardly
         * ever grow these maps after initial allocation, we'll set load factor to 1.</p>
         *
         * @param oid  oid of entity that needs a cache entry
         * @param size # of commodity types sold by the entity
         * @return the new or previously existing entry
         */
        private TIntObjectMap<TObjectDoubleMap<String>> ensureCommTypeMap(long oid, int size) {
            TIntObjectMap<TObjectDoubleMap<String>> entry = capacities.get(oid);
            if (entry == null) {
                // all commodity types are positive, so use a negative value for no entry
                entry = new TIntObjectHashMap<>(size, 1f, -1);
                capacities.put(oid, entry);
            }
            return entry;
        }

        @VisibleForTesting
        Collection<TIntObjectMap<TObjectDoubleMap<String>>> getAllEntityCapacities() {
            return capacities.valueCollection();
        }
    }
}
