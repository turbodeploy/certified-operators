package com.vmturbo.stitching.poststitching;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.base.Stopwatch;

import io.grpc.stub.StreamObserver;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMaps;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.memory.MemoryMeasurer;
import com.vmturbo.common.protobuf.memory.MemoryMeasurer.MemoryMeasurement;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.Stats.EntityAndCommodityType;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesCapacityValuesRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesCapacityValuesResponse;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Set commodity capacity (auto scaled range).
 * The main algorithm for setting the capacity is as following:
 * if (user policy exits) {
 *     if ("Enable SLO" setting from user policy is enabled) {
 *         Take the SLO value from the user settings and use it as capacity.
 *         If not found, take the SLO value from the default settings.
 *     } else {
 *         Auto-set the capacity.
 *     }
 * } else {
 *     if ("Enable SLO" setting from default policy is enabled) {
 *         Take the SLO value from the default settings and use it as capacity.
 *     } else {
 *         Auto-set the capacity.
 *     }
 * }
 *
 *<p>** Auto setting is done as the following:
 * Take highest capacity value from db last 7 days (or hours on initialization) from the stats daily/hourly table,
 * Then use the weighted avg of:
 * (HISTORICAL_CAPACITY_WEIGHT * db value) + (CURRENT_CAPACITY_WEIGHT * capacity returned from probe)
 *
 * <p/>If no data returned from the db we will set the 'commodity used' value as the capacity
 */
public class SetAutoSetCommodityCapacityPostStitchingOperation implements PostStitchingOperation {

    private final EntityType entityType;
    private final ProbeCategory probeCategory;
    private final String sloValueSettingName;
    private final float defaultSLO;
    private final String enableSLOSettingName;
    private final CommodityType commodityType;
    private final MaxCapacityCache maxCapacityCache;
    private static final Logger logger = LogManager.getLogger();
    private static final double HISTORICAL_CAPACITY_WEIGHT = 0.8;
    private static final double CURRENT_CAPACITY_WEIGHT = 0.2;

    /**
     * Creates an instance of this class.
     * @param entityType entity type
     * @param probeCategory probe category
     * @param commodityType commodity type
     * @param sloValueSettingName the SLO value setting name
     * @param defaultSLO default SLO value for this commodity type
     * @param enableSLOSettingName the Enable SLO setting name
     * @param maxCapacityCache Cache for historical max capacities.
     */
    public SetAutoSetCommodityCapacityPostStitchingOperation(
            @Nonnull final EntityType entityType,
            @Nonnull final ProbeCategory probeCategory,
            @Nonnull final CommodityType commodityType,
            @Nonnull final String sloValueSettingName,
            @Nonnull final Float defaultSLO,
            @Nonnull final String enableSLOSettingName,
            @Nonnull final MaxCapacityCache maxCapacityCache) {
        this.entityType = Objects.requireNonNull(entityType);
        this.probeCategory = Objects.requireNonNull(probeCategory);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.sloValueSettingName = Objects.requireNonNull(sloValueSettingName);
        this.defaultSLO = defaultSLO;
        this.enableSLOSettingName = Objects.requireNonNull(enableSLOSettingName);
        this.maxCapacityCache = maxCapacityCache;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeCategoryEntityTypeScope(probeCategory, entityType);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        // iterate over entities and if the named setting exists for that entity, find all
        // sold commodities of the correct type and set their capacities according to the
        // value in the setting.
        entities.forEach(entity -> {
            final Optional<Setting> sloEnabledSetting = settingsCollection
                .getEntitySetting(entity.getOid(), enableSLOSettingName);
            if (sloEnabledSetting.isPresent()) {
                final Optional<Setting> sloValueSetting = settingsCollection
                        .getEntitySetting(entity.getOid(), sloValueSettingName);
                if (sloEnabledSetting.get().getBooleanSettingValue().getValue()
                    && !sloValueSetting.isPresent()) {
                    logger.error("SLO value Setting {} does not exist for entity {} while its enabled."
                                    + " Not setting capacity for it.", sloValueSettingName,
                            entity.getDisplayName());
                } else {
                    // Checking if the capacity value should be updated from the db
                    if (shouldUpdateCapacityFromDb(entity, settingsCollection)) {
                        queueCapacityUpdate(entity, entity.getTopologyEntityDtoBuilder()
                             .getCommoditySoldListBuilderList().stream()
                             .filter(this::commodityTypeMatches)
                             .iterator(), settingsCollection, resultBuilder);
                    } else {
                        // Queueing and updating entities which we can determine their capacity value
                        // by using the SLO value from the settings as capacity.
                        resultBuilder.queueUpdateEntityAlone(entity,
                            entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                                .getCommoditySoldListBuilderList().stream()
                                .filter(this::commodityTypeMatches)
                                .forEach(commSold -> commSold.setCapacity(
                                    getSLOValueFromSetting(settingsCollection, entity))));
                    }
                }
            }
        });

        return resultBuilder.build();
    }

    private void queueCapacityUpdate(TopologyEntity entity,
            Iterator<Builder> commsSold,
            EntitySettingsCollection settingsCollection,
            EntityChangesBuilder<TopologyEntity> resultBuilder) {
        if (!commsSold.hasNext()) {
            return;
        }

        resultBuilder.queueUpdateEntityAlone(entity, entityToUpdate -> {
            while (commsSold.hasNext()) {
                Builder commSold = commsSold.next();
                // Deactivate so they will not be sent to the market
                deactivateCommodities(entity, commSold);
                double maxCapacity = maxCapacityCache.getMaxCapacity(entityToUpdate.getOid(), commSold.getCommodityType().getType());
                if (maxCapacity > 0) {
                    // Value returned from the history component.
                    setValidCapacity(commSold, maxCapacity, entityToUpdate);
                } else {
                    // set the used value if its not 0, otherwise set the policy value
                    commSold.setCapacity(commSold.getUsed() != 0 ? commSold.getUsed()
                            : settingsCollection.getEntitySetting(entityToUpdate.getOid(), sloValueSettingName)
                                    // we know its not empty because that is the first validation in performOperation
                                    .get().getNumericSettingValue().getValue());
                }
            }
        });
    }

    /**
     * Deactivate the sold commodity, and the corresponding bought commodities from consumers of this
     * entity. Deactivated commodities are not sent to market for analysis.
     *
     * @param entity the entity that sells the commodity
     * @param commSold the commodity to deactivate
     */
    private void deactivateCommodities(
            @Nonnull final TopologyEntity entity,
            @Nonnull final CommoditySoldDTO.Builder commSold) {
        if (logger.isDebugEnabled()) {
            logger.debug("Deactivate {} for {}", commSold, entity);
        }
        // Deactivate sold commodity
        commSold.setActive(false);
        // Deactivate corresponding bought commodities from consumers
        entity.getCommoditiesBoughtBuilderByConsumers(commodityType.getNumber())
                .forEach(commBought -> commBought.setActive(false));
    }

    /**
     * Setting the commodity capacity after null check and debug logging.
     *
     * @param commSold to which we want to set the value
     * @param capacity Capacity returned from the db.
     * @param entityToUpdate contains the current 'used' value returned from the probe
     */
    private void setValidCapacity(final Builder commSold, final double capacity,
                                  final TopologyEntity entityToUpdate) {
        logger.debug("Calculating sold {} commodity capacity for {} with current used capacity: {} "
                + "and historical capacity: {}", commodityType.name(), entityToUpdate.getDisplayName(),
            commSold.getUsed(), capacity);
        commSold.setCapacity(getWeightedAvgCapacity(
            capacity,
            commSold.getUsed()));
    }

    private double getWeightedAvgCapacity(double historicalCapacity, double currentCapacity) {
        return (historicalCapacity * HISTORICAL_CAPACITY_WEIGHT + currentCapacity * CURRENT_CAPACITY_WEIGHT);
    }

    /**
     * Get a specific setting for a specific entity.
     * If a user setting exists, it is returned. If not, the default setting is returned.
     *
     * @param entity the entity for which to get the setting.
     * @param settingsCollection the settings collection.
     * @param settingName the name of the setting.
     * @return the user / default setting for the entity.
     */
    private Optional<Setting> getSetting(@Nonnull final TopologyEntity entity,
                                         @Nonnull final EntitySettingsCollection settingsCollection,
                                         @Nonnull final String settingName) {
        if (settingsCollection.hasUserPolicySettings(entity.getOid())) {
            return settingsCollection.getEntityUserSetting(entity,
                    EntitySettingSpecs.getSettingByName(settingName).get());

        }
        return settingsCollection.getEntitySetting(entity.getOid(), settingName);
    }

    /**
     * Indicate if we need to go the the db for updating the capacity (auto-set).
     * If a user policy exists and its "Enable SLO" setting is disabled, or a user policy does not
     * exist and the default policy's "Enable SLO" setting is disabled, then return true (and
     * auto-set algorithm will be used).
     *
     * @param entity for which we need to set capacity
     * @param settingsCollection contains the settings we check in
     * @return true if auto-set is needed, false otherwise.
     */
    private boolean shouldUpdateCapacityFromDb(final TopologyEntity entity,
                                               final EntitySettingsCollection settingsCollection) {
        return getSetting(entity, settingsCollection, enableSLOSettingName)
                .map(setting -> !setting.getBooleanSettingValue().getValue())
                .orElse(true);
    }

    @Nonnull
    @Override
    public String getOperationName() {
        return String.join("_", getClass().getSimpleName(),
                probeCategory.getCategory(), entityType.toString(), commodityType.name(),
                sloValueSettingName, enableSLOSettingName);
    }

    private boolean commodityTypeMatches(TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return commodity.getCommodityType().getType() == commodityType.getNumber();
    }

    /**
     * Get the SLO from the relevant user / default setting.
     * If the "Enable SLO" setting is true and SLO numeric setting exists, then return
     * the SLO value from the setting. Otherwise return default value.
     *
     * @param settingsCollection the settings collection.
     * @param entity the entity for which to get the SLO value.
     * @return the SLO value if exists, default value otherwise.
     */
    private double getSLOValueFromSetting(EntitySettingsCollection settingsCollection,
                                         TopologyEntity entity) {
        return getSetting(entity, settingsCollection, enableSLOSettingName)
                // make sure that the "Enable SLO" setting is true
                .filter(setting -> setting.getBooleanSettingValue().getValue())
                .map(setting -> getSetting(entity, settingsCollection, sloValueSettingName)
                        .map(valueSetting -> valueSetting.getNumericSettingValue().getValue())
                        .orElse(defaultSLO))
                .orElse(defaultSLO);
    }

    /**
     * Utility class to cache the maximum capacities over the past 7 days. The cache is
     * optimized for memory efficiency, and refreshes every configurable interval.
     */
    public static class MaxCapacityCache {

        private final StatsHistoryServiceStub statsStub;

        private final Map<EntityType, Set<Integer>> commoditiesByType = new HashMap<>();

        private final Object cacheLock = new Object();

        /**
         * For 2 commodities, this is more space-efficient than id -> list of comms.
         *
         * <p/>500k entities + 2 commodities per entity measured 32MB. 1m -> 64MB.
         */
        @GuardedBy("cacheLock")
        private Int2ObjectMap<Long2DoubleMap> cachedMaxValuesByCommodityAndEntityId = Int2ObjectMaps.emptyMap();

        private final ScheduledExecutorService scheduledExecutorService;

        /**
         * Saved reference to the cache refresh future, mainly for debugging.
         */
        private final ScheduledFuture<?> refreshFuture;

        /**
         * Create a new instance of the cache.
         *
         * @param statsStub Stub to access the history service.
         * @param scheduledExecutorService For the refresh task.
         * @param refreshInterval The refresh interval.
         * @param refreshIntervalTimeUnit The time units for the refresh interval.
         */
        public MaxCapacityCache(@Nonnull final StatsHistoryServiceStub statsStub,
                                @Nonnull final ScheduledExecutorService scheduledExecutorService,
                                final long refreshInterval,
                                @Nonnull final TimeUnit refreshIntervalTimeUnit) {
            this.statsStub = statsStub;
            // The initialization will happen separately via "initializeFromStitchingOperations".
            this.scheduledExecutorService = scheduledExecutorService;

            this.refreshFuture = scheduledExecutorService.scheduleAtFixedRate(() -> {
                try {
                    refresh();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Refresh thread interrupted.", e);
                }
            }, refreshInterval, refreshInterval, refreshIntervalTimeUnit);
        }

        /**
         * Return the max capacity for the specified commodity + entity combination.
         * If the cache is not initialized, returns -1 (an illegal capacity).
         * If the entity has no value in the cache, returns -1.
         *
         * @param entityId The entity id.
         * @param commodityType The commodity type. Expected to be either ResponseTime or Transaction.
         * @return The capacity.
         */
        double getMaxCapacity(long entityId, int commodityType) {
            synchronized (cacheLock) {
                final Long2DoubleMap valsByEntity = cachedMaxValuesByCommodityAndEntityId.getOrDefault(
                    commodityType, Long2DoubleMaps.EMPTY_MAP);
                return valsByEntity.getOrDefault(entityId, -1);
            }
        }

        /**
         * Initialize the cache based on the stitching operations in the {@link PostStitchingOperationLibrary}.
         *
         * @param library The {@link PostStitchingOperationLibrary}.
         */
        public void initializeFromStitchingOperations(@Nonnull final PostStitchingOperationLibrary library) {
            library.getPostStitchingOperations().forEach(op -> {
                // Look for the SetAutoSetCommodityCapacityPostStitchingOperation or its subclasses.
                if (op instanceof SetAutoSetCommodityCapacityPostStitchingOperation) {
                    final SetAutoSetCommodityCapacityPostStitchingOperation autoSetOp =
                            (SetAutoSetCommodityCapacityPostStitchingOperation)op;
                    commoditiesByType.computeIfAbsent(autoSetOp.entityType, k -> new HashSet<>())
                        .add(autoSetOp.commodityType.getNumber());
                }
            });

            // Schedule the cache initialization.
            scheduledExecutorService.execute(() -> {
                try {
                    RetriableOperation.newOperation(this::refresh)
                      // Retry until successful.
                      .retryOnOutput(success -> !success)
                      // Continue trying until the next scheduled refresh.
                      .run(refreshFuture.getDelay(TimeUnit.SECONDS), TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted cache initialization.", e);
                } catch (RetriableOperationFailedException e) {
                    logger.error("Failed to initialize max capacity cache.", e);
                } catch (TimeoutException e) {
                    logger.error("Max capacity cache initialization timed out", e);
                }
            });
        }

        boolean refresh() throws InterruptedException {
            // Do RPC call to get new max capacities over the last 7 days.
            try {
                final Stopwatch stopwatch = Stopwatch.createStarted();
                final Int2ObjectMap<Long2DoubleMap> response = retrieveNewCapacities();

                if (response.size() > 2) {
                    // This is unexpected.
                    logger.warn("Capacity cache should only contain Response Time"
                        + "and Transaction commodities. Found: {}", response.keySet().stream()
                            .map(UICommodityType::fromType)
                            .collect(Collectors.toSet()));
                }

                // This is fairly fast, and we are only doing it once every several hours.
                final MemoryMeasurement memoryMeasurement = MemoryMeasurer.measure(response);

                // Populate
                synchronized (cacheLock) {
                    cachedMaxValuesByCommodityAndEntityId = response;
                    logger.info("Max Capacity Cache refresh took {}s. Cache size: {}. Num values by commodity: {}",
                            stopwatch.elapsed(TimeUnit.SECONDS), memoryMeasurement, response.entrySet().stream()
                                 .map(e -> UICommodityType.fromType(e.getKey()) + " : " + e.getValue().size())
                                 .collect(Collectors.joining(", ")));
                }
                CACHE_REFRESH_SUMMARY.observe((double)stopwatch.elapsed(TimeUnit.SECONDS));
                return true;
            } catch (ExecutionException e) {
                logger.error("Failed to fetch max capacities from history component.", e);
                return false;
            }
        }

        @Nonnull
        Int2ObjectMap<Long2DoubleMap> retrieveNewCapacities() throws InterruptedException, ExecutionException {
            final GetEntityCommoditiesCapacityValuesRequest.Builder requestBuilder = GetEntityCommoditiesCapacityValuesRequest.newBuilder();
            commoditiesByType.forEach((entityType, commodities) -> {
                requestBuilder.addTargetComms(EntityAndCommodityType.newBuilder()
                        .setEntityType(entityType.getNumber())
                        .addAllCommodityType(commodities)
                        .build());
            });

            final Int2ObjectMap<Long2DoubleMap> ret;
            synchronized (cacheLock) {
                ret = new Int2ObjectOpenHashMap<>(2);
                cachedMaxValuesByCommodityAndEntityId.forEach((commType, curVals) -> {
                    ret.put(commType, new Long2DoubleOpenHashMap(curVals.size()));
                });
            }

            HistoryResponseObserver responseObserver = new HistoryResponseObserver(ret);
            statsStub.getEntityCommoditiesCapacityValues(requestBuilder.build(), responseObserver);
            responseObserver.future.get();
            return ret;
        }

        private static final DataMetricSummary CACHE_REFRESH_SUMMARY = DataMetricSummary.builder()
                .withName("tp_max_capacity_cache_refresh_duration_seconds")
                .withHelp("Duration in seconds it takes to refresh the max capacity cache in the topology processor.")
                .build()
                .register();

        /**
         * Observer for the entity commodity capacities coming from the history component.
         */
        private static class HistoryResponseObserver implements StreamObserver<GetEntityCommoditiesCapacityValuesResponse> {
            private final Int2IntMap numWithKeys = new Int2IntOpenHashMap();
            private final CompletableFuture<Void> future = new CompletableFuture<>();
            private final Int2ObjectMap<Long2DoubleMap> ret;

            private HistoryResponseObserver(final Int2ObjectMap<Long2DoubleMap> ret) {
                this.ret = ret;
            }

            @Override
            public void onNext(final GetEntityCommoditiesCapacityValuesResponse resp) {
                resp.getEntitiesToCommodityTypeCapacityList().forEach(entityRecord -> {
                    int commodityType = entityRecord.getCommodityType().getType();
                    if (!StringUtils.isEmpty(entityRecord.getCommodityType().getKey())) {
                        numWithKeys.put(commodityType,
                            numWithKeys.getOrDefault(commodityType, 0) + 1);
                    }
                    final Long2DoubleMap entityVals = ret.computeIfAbsent(
                        entityRecord.getCommodityType().getType(),
                        k -> new Long2DoubleOpenHashMap());
                    entityVals.put(entityRecord.getEntityUuid(), entityRecord.getCapacity());
                });
            }

            @Override
            public void onError(final Throwable throwable) {
                future.completeExceptionally(throwable);
            }

            @Override
            public void onCompleted() {
                if (!numWithKeys.isEmpty()) {
                    logger.error("Unexpected commodities with keys: {}", numWithKeys.entrySet().stream()
                        .map(e -> FormattedString.format("{} - {} times",
                            UICommodityType.fromType(e.getKey()), e.getValue()))
                        .collect(Collectors.joining(", ")));
                }
                ret.values().forEach(v -> ((Long2DoubleOpenHashMap)v).trim());
                future.complete(null);
            }
        }
    }
}
