package com.vmturbo.history.ingesters.common;

import java.util.Collection;
import java.util.Comparator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.IDataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.history.db.bulk.BulkInserterFactory;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.db.bulk.BulkInserterStats;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;

/**
 * Base class for topology ingesters.
 *
 * @param <T> type of topology entity
 */
public class TopologyIngesterBase<T>
        extends AbstractTopologyProcessor<T, IngesterState, Pair<Integer, BulkInserterFactoryStats>> {

    // TODO maybe use data packs in other writers
    protected final Logger logger;
    protected final TopologyIngesterConfig topologyIngesterConfig;
    private final Supplier<SimpleBulkLoaderFactory> loaderFactorySupplier;
    private int entityCount;

    /**
     * Create a new instance.
     *
     * @param chunkProcessorFactories factories for writers that will participate in ingestion
     * @param topologyIngesterConfig  config values for the ingester
     * @param loaderFactorySupplier   supplier that provides new bulk loader factories
     * @param topologyType            tooplogy type string
     */
    public TopologyIngesterBase(
            @Nonnull final
            Collection<? extends IChunkProcessorFactory<T, TopologyInfo, IngesterState>>
                    chunkProcessorFactories,
            @Nonnull TopologyIngesterConfig topologyIngesterConfig,
            @Nonnull Supplier<SimpleBulkLoaderFactory> loaderFactorySupplier,
            @Nonnull TopologyType topologyType) {
        super(chunkProcessorFactories, topologyIngesterConfig, topologyType.isProjectedTopology());
        this.logger = LogManager.getLogger(getClass());
        this.topologyIngesterConfig = topologyIngesterConfig;
        this.loaderFactorySupplier = loaderFactorySupplier;
    }

    /**
     * Provides external access to create a new shared state object, which permits the caller to
     * make use of the state after topology processing has been completed.
     *
     * <p>This is optional; if a pre-created shared state is not provided, the ingester will create
     * one in any case.</p>
     *
     * @param topologyInfo topology info for topology to be processed
     * @return new shared state instance
     */
    public IngesterState createSharedState(TopologyInfo topologyInfo) {
        return getSharedState(topologyInfo);
    }

    @Override
    protected IngesterState getSharedState(TopologyInfo topologyInfo) {
        return new IngesterState(loaderFactorySupplier.get(), new LongDataPack(), new DataPack<>());
    }

    /**
     * If we're configured for per-chunk-commits, we flush all writers after every chunk.
     *
     * <p>This can be turned on to align database commits with per-chunk kafka commits, once
     * support is added for the latter, enabling new restart options.</p>
     *
     * @param chunk        the chunk that was just processed
     * @param chunkNo      the chunk number (starting with 1) of this chunk in the overall
     *                     broadcast
     * @param topologyInfo topology info
     * @param state        shared writer state
     * @param timer        timer for this broadcast
     */
    @Override
    protected void afterChunkHook(final Collection<T> chunk,
            final int chunkNo,
            final TopologyInfo topologyInfo,
            final IngesterState state,
            final MultiStageTimer timer) {
        try {
            if (topologyIngesterConfig.perChunkCommit()) {
                state.getLoaders().flushAll();
            }
        } catch (InterruptedException e) {
            logger.error("Failed to flush bulk writers", e);
        }
    }

    /**
     * Finish up after all chunks have been processed.
     *
     * <p>Here we close (and flush as a side-effect) all bulk writers, and report some per-table
     * bulk writer stats.</p>
     *
     * @param topologyInfo the topology info
     * @param state        shared writer state
     * @param entityCount  number of entities ingested
     * @param timer        timer object for this broadcast
     */
    @Override
    protected void afterFinishHook(TopologyInfo topologyInfo,
            IngesterState state,
            int entityCount,
            MultiStageTimer timer) {
        this.entityCount = entityCount;
        try {
            state.getLoaders().close();
        } catch (InterruptedException e) {
            logger.error("Failed to close bulk writers", e);
        }
        final BulkInserterFactoryStats stats = state.getLoaders().getStats();
        int width = stats.getKeys().stream()
                .filter(key -> !stats.getByKey(key).isEmpty())
                .map(BulkInserterFactory::getKeyLabel)
                .mapToInt(String::length)
                .max().orElse(5);
        String statsSummary =
                String.format("  %-" + width + "s %11s %7s %5s %11s\n",
                        "TABLE", "RECORDS", "BATCHES", "FAILS", "DISCARDED")
                        + stats.getKeys().stream()
                        .filter(key -> !stats.getByKey(key).isEmpty())
                        .sorted(Comparator.comparing(BulkInserterFactory::getKeyLabel))
                        .map(key -> {
                            BulkInserterStats stat = stats.getByKey(key);
                            return String.format("  %-" + width + "s %,11d %,7d %,5d %,11d%s",
                                    BulkInserterFactory.getKeyLabel(key),
                                    stat.getWritten(),
                                    stat.getBatches(),
                                    stat.getFailedBatches(),
                                    stat.getDiscardedRecords(),
                                    stat.mayBePartial() ? " (partial)" : "");
                        })
                        .collect(Collectors.joining("\n"));
        logger.info("Completed ingestion for {}:\n{}",
            summarizeInfo(topologyInfo), statsSummary);
    }

    @Override
    protected Pair<Integer, BulkInserterFactoryStats> getProcessingResult(
            final TopologyInfo topologyInfo, final IngesterState state) {
        return Pair.of(entityCount, state.getLoaders().getStats());
    }

    /**
     * Topology types.
     */
    protected enum TopologyType {
        LIVE,
        PROJECTED_LIVE,
        PLAN,
        PROJECTED_PLAN;

        public boolean isProjectedTopology() {
            return this == PROJECTED_LIVE || this == PROJECTED_PLAN;
        }

        public String getReadableName() {
            return WordUtils.capitalizeFully(name().replaceAll("_", " "));
        }
    }

    /**
     * Shared state for topology ingesters.
     *
     * <p>Sharing is among writers on a per-ingester-instance basis, not among ingesters.</p>
     */
    public static class IngesterState {

        private final SimpleBulkLoaderFactory loaders;
        // data pack that can be used with any oids (entity oids, group ids, etc.) appearing in
        // the topology
        private final IDataPack<Long> oidPack;
        // data pack that can be used with any commodity keys appearing in the topology
        private final IDataPack<String> keyPack;

        /**
         * Create a new instance.
         *
         * @param loaders bulk loader factory
         * @param oidPack oid data pack
         * @param keyPack commodity key data pack
         */
        public IngesterState(
                SimpleBulkLoaderFactory loaders, IDataPack<Long> oidPack, IDataPack<String> keyPack) {
            this.loaders = loaders;
            this.oidPack = oidPack;
            this.keyPack = keyPack;
        }

        /**
         * Create a new instance with new data packs.
         *
         * @param loaders bulk loader factory
         */
        public IngesterState(SimpleBulkLoaderFactory loaders) {
            this(loaders, new LongDataPack(), new DataPack<>());
        }

        public SimpleBulkLoaderFactory getLoaders() {
            return loaders;
        }

        public IDataPack<Long> getOidPack() {
            return oidPack;
        }

        public IDataPack<String> getKeyPack() {
            return keyPack;
        }
    }

}
