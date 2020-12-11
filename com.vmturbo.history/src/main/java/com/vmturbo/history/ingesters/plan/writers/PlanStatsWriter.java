package com.vmturbo.history.ingesters.plan.writers;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.stats.PlanStatsAggregator;

/**
 * {@link PlanStatsWriter} records market stats data from plan topology broadcast by market.
 */
public class PlanStatsWriter extends TopologyWriterBase {
    private static Logger logger = LogManager.getLogger();

    private final PlanStatsAggregator aggregator;
    private boolean initFailed = false;

    private PlanStatsWriter(@Nonnull TopologyInfo topologyInfo,
                            @Nonnull HistorydbIO historydbIO,
                            @Nonnull SimpleBulkLoaderFactory loaders) {
        try {
            historydbIO.addMktSnapshotRecord(topologyInfo);
        } catch (VmtDbException e) {
            logger.error("Failed to add market snapshot record; will skip stats collection", e);
            this.initFailed = true;
        }
        this.aggregator = new PlanStatsAggregator(loaders, historydbIO, topologyInfo, true);
    }

    @Override
    public ChunkDisposition processEntities(@Nonnull final Collection<TopologyEntityDTO> entities,
                                         @Nonnull final String infoSummary) {
        if (initFailed) {
            return ChunkDisposition.DISCONTINUE;
        } else {
            aggregator.handleChunk(entities);
            return ChunkDisposition.SUCCESS;
        }
    }

    @Override
    public void finish(final int entityCount, final boolean expedite, final String infoSummary)
        throws InterruptedException {
        aggregator.writeAggregates();
    }

    /**
     * Factory to create new writer instances.
     */
    public static class Factory extends TopologyWriterBase.Factory {

        private final HistorydbIO historydbIO;

        /**
         * Create a new factory instance.
         *
         * @param historydbIO database utils
         */
        public Factory(@Nonnull HistorydbIO historydbIO) {
            this.historydbIO = historydbIO;
        }

        @Override
        public Optional<IChunkProcessor<Topology.DataSegment>>
        getChunkProcessor(final TopologyInfo topologyInfo, final SimpleBulkLoaderFactory loaders) {
            return Optional.of(new PlanStatsWriter(topologyInfo, historydbIO, loaders));
        }
    }
}
