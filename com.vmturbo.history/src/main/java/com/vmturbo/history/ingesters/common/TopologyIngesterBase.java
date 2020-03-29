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
import com.vmturbo.history.db.bulk.BulkInserterFactory;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.db.bulk.BulkInserterStats;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.utils.MultiStageTimer;

/**
 * Base class for topology ingesters.
 *
 * @param <T> type of topology entity
 */
public class TopologyIngesterBase<T>
    extends AbstractTopologyProcessor
    <T, SimpleBulkLoaderFactory, Pair<Integer, BulkInserterFactoryStats>> {

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
        Collection<? extends IChunkProcessorFactory<T, TopologyInfo, SimpleBulkLoaderFactory>>
            chunkProcessorFactories,
        @Nonnull TopologyIngesterConfig topologyIngesterConfig,
        @Nonnull Supplier<SimpleBulkLoaderFactory> loaderFactorySupplier,
        @Nonnull TopologyType topologyType) {
        super(chunkProcessorFactories, topologyIngesterConfig, topologyType.getReadableName());
        this.logger = LogManager.getLogger(getClass());
        this.topologyIngesterConfig = topologyIngesterConfig;
        this.loaderFactorySupplier = loaderFactorySupplier;
    }

    public SimpleBulkLoaderFactory getLoadersInstance() {
        return loaderFactorySupplier.get();
    }

    @Override
    protected SimpleBulkLoaderFactory getSharedState(TopologyInfo topologyInfo) {
        return loaderFactorySupplier.get();
    }

    /**
     * If we're configured for per-chunk-commits, we flush all writers after every chunk.
     *
     * <p>This can be turned on to align database commits with per-chunk kafka commits, once
     * support is added for the latter, enabling new restart options.</p>
     *
     * @param chunk        the chunk that was just processed
     * @param chunkNo      the chunk number (starting with 1) of this chunk in the overall broadcast
     * @param topologyInfo topology info
     * @param inserters    bulk inserter factory
     * @param timer        timer for this broadcast
     */
    @Override
    protected void afterChunkHook(final Collection<T> chunk,
                                  final int chunkNo,
                                  final TopologyInfo topologyInfo,
                                  final SimpleBulkLoaderFactory inserters,
                                  final MultiStageTimer timer) {
        try {
            if (topologyIngesterConfig.perChunkCommit()) {
                inserters.flushAll();
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
     * @param loaders      the bulk loader factory
     * @param entityCount  number of entities ingested
     * @param timer        timer object for this broadcast
     */
    @Override
    protected void afterFinishHook(TopologyInfo topologyInfo,
                                   SimpleBulkLoaderFactory loaders,
                                   int entityCount,
                                   MultiStageTimer timer) {
        this.entityCount = entityCount;
        try {
            loaders.close();
        } catch (InterruptedException e) {
            logger.error("Failed to close bulk writers", e);
        }
        final BulkInserterFactoryStats stats = loaders.getStats();
        String statsSummary =
            String.format("  %-25s %11s %7s %5s\n", "TABLE", "RECORDS", "BATCHES", "FAILS")
                + stats.getKeys().stream()
                .sorted(Comparator.comparing(BulkInserterFactory::getKeyLabel))
                .map(key -> {
                    BulkInserterStats stat = stats.getByKey(key);
                    return String.format("  %-25s %,11d %,7d %,5d",
                        BulkInserterFactory.getKeyLabel(key),
                        stat.getWritten(),
                        stat.getBatches(),
                        stat.getFailedBatches());
                })
                .collect(Collectors.joining("\n"));
        logger.info("Completed ingestion for {}:\n{}",
            summarizeInfo(topologyInfo), statsSummary);
    }

    @Override
    protected Pair<Integer, BulkInserterFactoryStats> getProcessingResult(
        final TopologyInfo topologyInfo, final SimpleBulkLoaderFactory loaders) {
        return Pair.of(entityCount, loaders.getStats());
    }

    /**
     * Topology types.
     */
    protected enum TopologyType {
        LIVE,
        PROJECTED_LIVE,
        PLAN,
        PROJECTED_PLAN;

        public String getReadableName() {
            return WordUtils.capitalizeFully(name().replaceAll("_", " "));
        }
    }
}
