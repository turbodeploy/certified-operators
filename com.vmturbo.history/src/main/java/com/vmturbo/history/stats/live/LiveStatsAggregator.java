package com.vmturbo.history.stats.live;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMaps;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.DataPacks.CommodityTypeDataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;
import com.vmturbo.history.stats.MarketStatsAccumulator;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.DelayedCommodityBoughtWriter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Handles receiving realtime topology by chunks, and while doing so aggregates various realtime
 * topology stats.
 */
public class LiveStatsAggregator implements MemReporter {

    private final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    private final TopologyInfo topologyInfo;

    /**
     * Each string should be a value of {@link CommodityType}.
     */
    private final Set<CommodityType> excludedCommodityTypes;

    // supplier of loaders for the records we produce
    private final SimpleBulkLoaderFactory loaders;

    /**
     * Cache for sold commodity capacities, so bought commodity records can include seller
     * capacities.
     */
    private final CapacityCache capacityCache;

    /**
     * Commodity keys that exceeded their max allowed length, encountered during the lifetime of
     * this {@link LiveStatsAggregator instance}.
     */
    final Set<String> longCommodityKeys = new HashSet<>();

    /**
     * {@link MarketStatsAccumulator}s by base entity type and environment type.
     */
    private final Table<String, EnvironmentType, MarketStatsAccumulator> accumulatorsByEntityAndEnvType
            = HashBasedTable.create();
    /**
     * Map from numerical entity type to {@link EntityType}.
     */
    private final Map<Integer, Optional<EntityType>> entityTypes = Maps.newHashMap();
    /**
     * Map from numerical entity type to (String) base entity type.
     */
    private final Map<Integer, Optional<String>> baseEntityTypes = Maps.newHashMap();

    /**
     * This map is used when a commodity bought list is received but the provider is not yet known.
     * The key is the provider oid and the value is a list of functions that would be executed when
     * the provider capacities become available.
     */
    private final Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought
            = HashMultimap.create();

    /**
     * Metrics used for counting DB operations so as to better understand DB performance.
     *
     * @param historydbIO            db methods
     * @param topologyInfo           topology info
     * @param excludedCommodityTypes commodities for which we will not record metrics
     * @param state                  ingester state
     */
    public LiveStatsAggregator(@Nonnull final HistorydbIO historydbIO,
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull Set<CommodityType> excludedCommodityTypes,
            @Nonnull IngesterState state) {
        this.historydbIO = Objects.requireNonNull(historydbIO);
        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.excludedCommodityTypes = Objects.requireNonNull(excludedCommodityTypes);
        this.capacityCache = new CapacityCache(excludedCommodityTypes, state.getOidPack());
        this.loaders = Objects.requireNonNull(state.getLoaders());
        createAccumulators();
    }

    /**
     * Preallocate accumulators for all entity type/environment pairs.
     *
     * <p>This will result in a small number of additional records in the database for entities
     * that never appear in a topology, but it fixes a bug (OM-62025) wherein if an entity type
     * disappeared from a topology, its market-wide stats would effectively be frozen at their most
     * recent values in rollup tables.</p>
     */
    private void createAccumulators() {
        EntityType.allEntityTypes().stream()
                .filter(EntityType::persistsStats)
                .forEach(this::createAccumulatorsForType);
    }

    private void createAccumulatorsForType(EntityType entityType) {
        Arrays.stream(EnvironmentType.values())
                .forEach(env -> createAccumulatorForTypeAndEnv(entityType, env));
    }

    private void createAccumulatorForTypeAndEnv(final EntityType entityType, final EnvironmentType env) {
        final String entityTypeName = entityType.getName();
        final MarketStatsAccumulator accumulator =
                MarketStatsAccumulator.create(topologyInfo, entityTypeName, env,
                        historydbIO, excludedCommodityTypes, loaders, longCommodityKeys);
        accumulatorsByEntityAndEnvType.put(entityTypeName, env, accumulator);
    }

    /**
     * Cache commodity sold capacities of an entity.
     *
     * @param entityDTO the entity which sold commodities capacities are added to the cache.
     */
    private void cacheCapacities(TopologyEntityDTO entityDTO) {
        capacityCache.cacheCapacities(entityDTO);
    }

    /**
     * Write commodities bought rows to the DB for commodities bought that were received before
     * their provider DTO (and therefore their sold capacities) was available. Now that we processed
     * the provider sold commodities, we can write these rows.
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
     * Gather entity stats to be persisted into chunks by entity type. When chunks are full, write
     * them to the database.
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

        accumulatorsByEntityAndEnvType.get(baseEntityType, entityDTO.getEnvironmentType())
                .recordEntity(entityDTO, capacityCache, delayedCommoditiesBought, entityByOid);
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
     * Persist the various per-entity-type aggregate stats and then write all remaining queued stats
     * rows in case there are partial chunks. Called when done handling the incoming message
     * chunks.
     *
     * @throws VmtDbException       when there is a problem writing to the DB.
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
            logger.error("Following commodity keys needed to be shortened when persisted; "
                    + "data access anomalies may result: {}", longCommodityKeys);
        }
    }

    /**
     * Record stats information from the given {@link EntityDTO}. Stats data will be batched to
     * optimize the number of commands sent to the Relational Database.
     *
     * @param entityDTO   the {@link EntityDTO} to records the stats from
     * @param entityByOid map of entities by oid
     * @throws InterruptedException if interrupted
     */
    public void aggregateEntity(TopologyEntityDTO entityDTO,
            Map<Long, TopologyEntityDTO> entityByOid) throws InterruptedException {
        // save commodity sold capacities for filling other commodity bought capacities
        cacheCapacities(entityDTO);
        // provide commodity sold capacities for previously unsatisfied commodity bought
        handleDelayedCommoditiesBought(entityDTO.getOid());
        if (EntityType.fromSdkEntityType(entityDTO.getEntityType())
                .map(EntityType::persistsStats)
                .orElse(false)) {
            // schedule the stats for the entity for persisting to db
            aggregateEntityStats(entityDTO, entityByOid);
        }
    }

    /**
     * Class to store capacities for all sold commodities encountered during the processing of a
     * topology.
     *
     * <p>These may be needed to fill in capacities in the stats record created for corresponding
     * bought commodities in other entities appearing in the topology.</p>
     */
    public static class CapacityCache implements MemReporter {
        private final Set<CommodityType> excludedCommodityTypes;
        private final IDataPack<Long> oidPack;

        // entity oid (via oidPack) -> commodity type/key -> capacity
        Int2ObjectMap<Int2FloatMap> capacities = new Int2ObjectOpenHashMap<>();
        // data pack that allows us to use ints to represent commodity type/key
        CommodityTypeDataPack commodityTypePack = new CommodityTypeDataPack();

        /**
         * Create a new instance.
         *
         * @param excludedCommodityTypes commodity types for which we don't cache capacities
         * @param oidPack                data pack so we can represent oids as ints in data
         *                               structures
         */
        public CapacityCache(Set<CommodityType> excludedCommodityTypes, IDataPack<Long> oidPack) {
            this.excludedCommodityTypes = excludedCommodityTypes;
            this.oidPack = oidPack;
        }

        /**
         * Cache capacities for all commodities sold by the given entity.
         *
         * @param entity the entity
         */
        public void cacheCapacities(TopologyEntityDTO entity) {
            final Iterator<CommoditySoldDTO> soldCommodities = entity.getCommoditySoldListList().stream()
                    .filter(sold -> !excludedCommodityTypes.contains(
                            CommodityType.forNumber(sold.getCommodityType().getType())))
                    .iterator();
            if (soldCommodities.hasNext()) {
                int iid = oidPack.toIndex(entity.getOid());
                final Int2FloatMap entityCapacities = capacities.computeIfAbsent(iid,
                        _iid -> new Int2FloatOpenHashMap());
                soldCommodities.forEachRemaining(sold -> {
                    final int index = commodityTypePack.toIndex(sold.getCommodityType());
                    entityCapacities.put(index, (float)sold.getCapacity());
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
         * CommodityType} to capacity value.
         *
         * <p>This is used in tests, but in general, requests for individual capacity values using
         * {@link #getCapacity(long, TopologyDTO.CommodityType)} is recommended, since it does not
         * carry the overhead of constructing {@link CommodityType} instaces.</p>
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


        @Override
        public Long getMemSize() {
            return null;
        }

        @Override
        public List<MemReporter> getNestedMemReporters() {
            return ImmutableList.of(
                    new SimpleMemReporter("capacities", capacities),
                    new SimpleMemReporter("commodityTypePack", commodityTypePack)
            );
        }
    }

    @Override
    public Long getMemSize() {
        return null;
    }

    @Override
    public List<MemReporter> getNestedMemReporters() {
        return Arrays.asList(capacityCache);
    }
}
