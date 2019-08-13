package com.vmturbo.topology.processor.history;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;

/**
 * Historical editor with state that caches pre-calculated data from previous topology broadcast
 * invocations and/or from persistent store (history db).
 *
 * @param <DbValue> the pre-calculated element of per-field data as retrieved from the persistent store
 * @param <HistoryData> per-commodity field historical data to cache that wraps DbValue with runtime info
 * @param <HistoryLoadingTask> loader of DbValue's from the persistent store
 * @param <Config> per-editor type configuration values holder
 */
public abstract class AbstractCachingHistoricalEditor<HistoryData extends IHistoryCommodityData<Config, DbValue>,
            HistoryLoadingTask extends IHistoryLoadingTask<DbValue>,
            Config extends CachingHistoricalEditorConfig,
            DbValue>
        extends AbstractHistoricalEditor<Config> {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Per-commodity field cached historical data.
     */
    private final ConcurrentMap<EntityCommodityFieldReference, HistoryData> cache = new ConcurrentHashMap<>();

    private final Function<StatsHistoryServiceBlockingStub, HistoryLoadingTask> historyLoadingTaskCreator;
    private final Supplier<HistoryData> historyDataCreator;

    /**
     * Construct the instance of a caching history editor.
     *
     * @param config per-type configuration
     * @param statsHistoryClient history db client
     * @param historyLoadingTaskCreator create an instance of a db value loading task
     * @param historyDataCreator create an instance of cached history element
     */
    protected AbstractCachingHistoricalEditor(@Nullable Config config,
                                              @Nonnull StatsHistoryServiceBlockingStub statsHistoryClient,
                                              @Nonnull Function<StatsHistoryServiceBlockingStub, HistoryLoadingTask> historyLoadingTaskCreator,
                                              @Nonnull Supplier<HistoryData> historyDataCreator) {
        super(config, statsHistoryClient);
        this.historyLoadingTaskCreator = historyLoadingTaskCreator;
        this.historyDataCreator = historyDataCreator;
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<EntityCommodityFieldReference>>>
           createPreparationTasks(@Nonnull List<EntityCommodityReferenceWithBuilder> commodityRefs) {
        // load only commodities that have no fields in the cache yet
        List<EntityCommodityReferenceWithBuilder> uninitializedCommodities = commodityRefs.stream()
                        .filter(ref -> {
                            for (CommodityField field : CommodityField.values()) {
                                if (cache.containsKey(new EntityCommodityFieldReference(ref, field))) {
                                    return false;
                                }
                            }
                            return true;
                        })
                        .collect(Collectors.toList());
        // chunk by configured size
        List<List<EntityCommodityReferenceWithBuilder>> partitions = Lists
                        .partition(uninitializedCommodities, getConfig().getLoadingChunkSize());
        return partitions.stream()
                        .map(chunk -> new HistoryLoadingCallable(historyLoadingTaskCreator.apply(getStatsHistoryClient()),
                                                                 chunk))
                        .collect(Collectors.toList());
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<Void>>>
           createCalculationTasks(@Nonnull List<EntityCommodityReferenceWithBuilder> commodityRefs) {
        // calculate only fields for commodities present in the cache
        List<EntityCommodityFieldReference> cachedFields = commodityRefs.stream()
                        .flatMap(commRef -> Arrays.stream(CommodityField.values())
                                        .map(field -> new EntityCommodityFieldReference(commRef, field)))
                        .filter(cache::containsKey)
                        .collect(Collectors.toList());
        // chunk by configured size
        List<List<EntityCommodityFieldReference>> partitions = Lists
                        .partition(cachedFields, getConfig().getCalculationChunkSize());
        return partitions.stream()
                        .map(chunk -> new HistoryCalculationCallable(chunk))
                        .collect(Collectors.toList());
    }

    /**
     * Wrapper callable to load a chunk of historical data from the persistent store.
     * Upon success the values are stored into cache, with configured expiration on write.
     */
    private class HistoryLoadingCallable implements Callable<List<EntityCommodityFieldReference>> {
        private final HistoryLoadingTask task;
        private final List<EntityCommodityReferenceWithBuilder> commodities;

        /**
         * Construct the wrapper to load a chunk of historical commodity values from the history db.
         *
         * @param task task that will do the loading
         * @param commodities list of commodities
         */
        HistoryLoadingCallable(@Nonnull HistoryLoadingTask task,
                               @Nonnull List<EntityCommodityReferenceWithBuilder> commodities) {
            this.task = task;
            this.commodities = commodities;
        }

        @Override
        public List<EntityCommodityFieldReference> call() throws Exception {
            Map<EntityCommodityFieldReference, DbValue> dbValues = task.load(commodities);
            // update the cache with loaded db values
            dbValues.forEach((fieldRef, dbValue) -> cache.compute(fieldRef, (key, cacheValue) -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("Loaded persisted value {} for {}", dbValue, fieldRef);
                }
                if (cacheValue == null) {
                    cacheValue = historyDataCreator.get();
                }
                cacheValue.init(dbValue, getConfig());
                return cacheValue;
            }));
            // return the list of loaded fields
            return new ArrayList<>(dbValues.keySet());
        }

    }

    /**
     * Wrapper callable to update historical value of a commodity field from cached history data
     * and a running new value.
     */
    private class HistoryCalculationCallable implements Callable<List<Void>> {
        private final List<EntityCommodityFieldReference> commodityFields;

        /**
         * Construct the wrapper to aggregate the chunk of commodity fields.
         *
         * @param commodityFields references to commodity fields to aggregate
         */
        HistoryCalculationCallable(@Nonnull List<EntityCommodityFieldReference> commodityFields) {
            this.commodityFields = commodityFields;
        }

        @Override
        public List<Void> call() throws Exception {
            commodityFields.forEach(ref -> {
                HistoryData data = cache.get(ref);
                if (data == null) {
                    // shouldn't have happened, preparation is supposed to add entries
                    logger.error("Missing historical data cache entry for " + ref);
                } else {
                    data.aggregate(ref, getConfig());
                }
            });
            return Collections.emptyList();
        }
    }

}
