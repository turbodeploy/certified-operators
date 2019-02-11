package com.vmturbo.history.stats.live;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.stats.MarketStatsAccumulator;
import com.vmturbo.history.stats.MarketStatsAccumulator.DelayedCommodityBoughtWriter;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Handles receiving realtime topology by chunks, and while doing so
 * aggregates various realtime topology stats.
 *
 */
public class LiveStatsAggregator {

    private final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    private final TopologyInfo topologyInfo;

    /**
     * Each string should be a value of {@link CommodityType}.
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
    private Multimap<Long, MarketStatsAccumulator.DelayedCommodityBoughtWriter> delayedCommoditiesBought
            = HashMultimap.create();

    /**
     * Metrics used for counting DB operations so as to better understand DB performance.
     */
    public LiveStatsAggregator(@Nonnull final HistorydbIO historydbIO,
                               @Nonnull final TopologyInfo topologyInfo,
                               ImmutableList<String> commoditiesToExclude,
                               int writeTopologyChunkSize) {
        this.historydbIO = historydbIO;
        this.topologyInfo = topologyInfo;
        this.commoditiesToExclude = commoditiesToExclude;
        this.writeTopologyChunkSize = writeTopologyChunkSize;
    }

    /**
     * Cache commodity sold capacities of an entity.
     *
     * @param entityDTO the entity which sold commodities capacities are added to the cache.
     */
    private void cacheCapacities(TopologyEntityDTO entityDTO) {
        final Map<Integer, Double> map;
        if (entityDTO.getEntityType() == EntityDTO.EntityType.VIRTUAL_VOLUME.getNumber()) {
            // todo: currently volume doesn't sell any commodities, the capacity is stored as
            // properties, we should remove this logic once volume starts selling commodities
            map = new HashMap<>();
            if (entityDTO.hasTypeSpecificInfo() && entityDTO.getTypeSpecificInfo().hasVirtualVolume()) {
                VirtualVolumeInfo volume = entityDTO.getTypeSpecificInfo().getVirtualVolume();
                map.put(CommodityType.STORAGE_AMOUNT.getNumber(),
                        Double.valueOf(volume.getStorageAmountCapacity()));
                map.put(CommodityType.STORAGE_ACCESS.getNumber(),
                        Double.valueOf(volume.getStorageAccessCapacity()));
            } else {
                logger.warn("Capacity info is missing for volume {}", entityDTO.getOid());
            }
        } else {
             map = entityDTO.getCommoditySoldListList().stream()
                    .collect(Collectors.toMap(commSold -> (commSold.getCommodityType().getType()),
                            CommoditySoldDTO::getCapacity, (k1, k2) -> k1));
        }

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
    private void aggregateEntityStats(TopologyEntityDTO entityDTO,
            Map<Long, TopologyEntityDTO> entityByOid) throws VmtDbException {
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
            marketStatsAccumulator = new MarketStatsAccumulator(topologyInfo, baseEntityType,
                entityDTO.getEnvironmentType(), historydbIO, writeTopologyChunkSize,
                commoditiesToExclude);
            accumulatorsByEntityAndEnvType.put(baseEntityType, entityDTO.getEnvironmentType(), marketStatsAccumulator);
        }

        marketStatsAccumulator.recordEntity(entityDTO, capacities, delayedCommoditiesBought, entityByOid);
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
        for (final MarketStatsAccumulator statsAccumulator : accumulatorsByEntityAndEnvType.values()) {
            statsAccumulator.writeFinalStats();
        }
    }

    /**
     * Record stats information from the given {@link EntityDTO}. Stats data will be
     * batched to optimize the number of commands sent to the Relational Database.
     *
     * @param entityDTO the {@link EntityDTO} to records the stats from
     * @throws VmtDbException if there is an error persisting to the database
     */
    public void aggregateEntity(TopologyEntityDTO entityDTO,
            Map<Long, TopologyEntityDTO> entityByOid) throws VmtDbException {
        // save commodity sold capacities for filling other commodity bought capacities
        cacheCapacities(entityDTO);
        // provide commodity sold capacitites for previously unsatisfied commodity bought
        handleDelayedCommoditiesBought(entityDTO.getOid());
        // schedule the stats for the entity for persisting to db
        aggregateEntityStats(entityDTO, entityByOid);
    }
}
