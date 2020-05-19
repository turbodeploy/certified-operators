package com.vmturbo.topology.processor.history;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.EntityCommodityReference;

/**
 * Historical editor with state that caches pre-calculated data from previous topology broadcast
 * invocations and/or from persistent store (history db).
 *
 * @param <DbValue> the pre-calculated element of per-field data as retrieved from the persistent store
 * @param <HistoryData> per-commodity field historical data to cache that wraps DbValue with runtime info
 * @param <HistoryLoadingTask> loader of DbValue's from the persistent store
 * @param <Config> per-editor type configuration values holder
 * @param <Stub> type of history component stub
 * @param <CheckpointResult> the result of checkpoint, if applicable
 */
public abstract class AbstractCachingHistoricalEditor<HistoryData extends IHistoryCommodityData<Config, DbValue, CheckpointResult>,
            HistoryLoadingTask extends IHistoryLoadingTask<Config, DbValue>,
            Config extends CachingHistoricalEditorConfig,
            DbValue,
            Stub extends io.grpc.stub.AbstractStub<Stub>,
            CheckpointResult>
        extends AbstractHistoricalEditor<Config, Stub> {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Per-commodity field cached historical data.
     */
    private final ConcurrentMap<EntityCommodityFieldReference, HistoryData> cache = new ConcurrentHashMap<>();

    /**
     * Creator for the task that fetches persisted data.
     */
    private final BiFunction<Stub, Pair<Long, Long>, HistoryLoadingTask> historyLoadingTaskCreator;
    /**
     * Creator for the cached per-commodity-field data entry.
     */
    protected final Supplier<HistoryData> historyDataCreator;

    /**
     * Construct the instance of a caching history editor.
     *
     * @param config per-type configuration
     * @param statsHistoryClient history db client
     * @param historyLoadingTaskCreator create an instance of a db value loading task
     * @param historyDataCreator create an instance of cached history element
     */
    protected AbstractCachingHistoricalEditor(@Nullable Config config,
                                              @Nonnull Stub statsHistoryClient,
                                              @Nonnull BiFunction<Stub, Pair<Long, Long>, HistoryLoadingTask> historyLoadingTaskCreator,
                                              @Nonnull Supplier<HistoryData> historyDataCreator) {
        super(config, statsHistoryClient);
        this.historyLoadingTaskCreator = historyLoadingTaskCreator;
        this.historyDataCreator = historyDataCreator;
    }

    @Override
    @Nonnull
    public List<? extends Callable<List<EntityCommodityFieldReference>>>
           createPreparationTasks(@Nonnull HistoryAggregationContext context,
                                  @Nonnull List<EntityCommodityReference> commodityRefs) {
        List<EntityCommodityReference> uninitializedCommodities =
                        gatherUninitializedCommodities(commodityRefs);
        // chunk by configured size
        List<List<EntityCommodityReference>> partitions = Lists
                        .partition(uninitializedCommodities, getConfig().getLoadingChunkSize());
        return partitions.stream()
                        .map(chunk -> new HistoryLoadingCallable(context, createLoadingTask(null),
                                        chunk)).collect(Collectors.toList());
    }

    /**
     * Collects references to entity commodities which settings have been changed.
     *
     * @param references all references related to historical processing.
     * @param context invocation context i.e current graph
     * @return collection of references which settings have been changed.
     */
    @Nonnull
    protected Set<EntityCommodityReference> gatherSettingsChangedCommodities(
                    @Nonnull Collection<EntityCommodityReference> references,
                    @Nonnull HistoryAggregationContext context) {
        return getCache().entrySet().stream().filter(entry -> {
            final EntityCommodityFieldReference ref = entry.getKey();
            final HistoryData data = entry.getValue();
            return data.needsReinitialization(ref, context, getConfig());
        }).map(Entry::getKey).collect(Collectors.toSet());
    }

    /**
     * Prints debug messages using specified logger and debug string provider used to extract
     * required information from history data instance.
     *
     * @param logger which should be used to print message
     * @param dataToDebugString history data to debug string transformer.
     */
    protected void debugLogDataValues(@Nonnull Logger logger,
                    @Nonnull Function<HistoryData, String> dataToDebugString) {
        if (logger.isDebugEnabled()) {
            final Optional<Long> oidToBeTraced = getConfig().getOidToBeTracedInLog().map(Long::valueOf);
            oidToBeTraced.ifPresent(oid -> getCache().entrySet().stream()
                            .filter(e -> e.getKey().getEntityOid() == oid)
                            .forEach(e -> logger.debug(dataToDebugString.apply(e.getValue()))));
        }
    }

    /**
     * Creates a new history loading task instance for the specified time range.
     *
     * @param rawRange pair of start and end timestamps between which we want to get
     *                 data.
     * @return instance of history loading task which will load data in requested time
     *                 range.
     */
    @Nonnull
    protected HistoryLoadingTask createLoadingTask(@Nullable Pair<Long, Long> rawRange) {
        final Pair<Long, Long> range = rawRange == null ? Pair.create(null, null) : rawRange;
        return historyLoadingTaskCreator.apply(getStatsHistoryClient(), range);
    }


    @Override
    @Nonnull
    public List<? extends Callable<List<Void>>>
           createCalculationTasks(@Nonnull HistoryAggregationContext context,
                                  @Nonnull List<EntityCommodityReference> commodityRefs) {
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
                        .map(chunk -> new HistoryCalculationCallable(context, chunk))
                        .collect(Collectors.toList());
    }

    @Override
    public void cleanupCache(@Nonnull List<EntityCommodityReference> commodities) {
        // remove cached history for entities that are not in current topology
        // (the history component will keep storing data for them for retention period, in case they reappear)
        final Set<EntityCommodityReference> refSet = new HashSet<>(commodities);
        cache.keySet().removeIf(field -> !refSet
            .contains(new EntityCommodityReference(field.getEntityOid(),
                field.getCommodityType(),
                field.getProviderOid())));
    }

    protected Map<EntityCommodityFieldReference, HistoryData> getCache() {
        return cache;
    }

    /**
     * Gather the cache entries that have not been loaded from persistence store yet.
     *
     * @param commodityRefs references from the current broadcast
     * @return commodities for which data are yet to be loaded
     */
    @Nonnull
    protected List<EntityCommodityReference>
              gatherUninitializedCommodities(@Nonnull List<EntityCommodityReference> commodityRefs) {

        // load only commodities that have no fields in the cache yet
        return commodityRefs.stream()
                        .filter(ref -> {
                            for (CommodityField field : CommodityField.values()) {
                                if (cache.containsKey(new EntityCommodityFieldReference(ref, field))) {
                                    return false;
                                }
                            }
                            return true;
                        })
                        .collect(Collectors.toList());
    }

    /**
     * Wrapper callable to load a chunk of historical data from the persistent store.
     * Upon success the values are stored into cache, with configured expiration on write.
     */
    protected class HistoryLoadingCallable implements Callable<List<EntityCommodityFieldReference>> {
        private final HistoryLoadingTask task;
        private final List<EntityCommodityReference> commodities;
        private final HistoryAggregationContext context;

        /**
         * Construct the wrapper to load a chunk of historical commodity values from the history db.
         *
         * @param context invocation context i.e current graph
         * @param task task that will do the loading
         * @param commodities list of commodities
         */
        public HistoryLoadingCallable(@Nonnull HistoryAggregationContext context,
                                      @Nonnull HistoryLoadingTask task,
                                      @Nonnull List<EntityCommodityReference> commodities) {
            this.task = task;
            this.commodities = commodities;
            this.context = context;
        }

        @Override
        public List<EntityCommodityFieldReference> call() throws Exception {
            Map<EntityCommodityFieldReference, DbValue> dbValues = task.load(commodities, getConfig());
            // update the cache with loaded db values
            dbValues.forEach((fieldRef, dbValue) -> cache.compute(fieldRef, (key, cacheValue) -> {
                if (logger.isTraceEnabled()) {
                    logger.trace("Loaded persisted value {} for {}", dbValue, fieldRef);
                }
                if (cacheValue == null) {
                    cacheValue = historyDataCreator.get();
                }
                cacheValue.init(fieldRef, dbValue, getConfig(), context);
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
        private final HistoryAggregationContext context;

        /**
         * Construct the wrapper to aggregate the chunk of commodity fields.
         *
         * @param context invocation context i.e current graph
         * @param commodityFields references to commodity fields to aggregate
         */
        HistoryCalculationCallable(@Nonnull HistoryAggregationContext context,
                                   @Nonnull List<EntityCommodityFieldReference> commodityFields) {
            this.commodityFields = commodityFields;
            this.context = context;
        }

        @Override
        public List<Void> call() throws Exception {
            commodityFields.forEach(ref -> {
                HistoryData data = cache.get(ref);
                if (data == null) {
                    // shouldn't have happened, preparation is supposed to add entries
                    logger.error("Missing historical data cache entry for " + ref);
                } else {
                    data.aggregate(ref, getConfig(), context);
                }
            });
            return Collections.emptyList();
        }
    }

}
