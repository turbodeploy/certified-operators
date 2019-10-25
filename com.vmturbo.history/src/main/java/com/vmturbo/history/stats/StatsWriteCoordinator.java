package com.vmturbo.history.stats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Persist information in a Live Topology to the History DB. This includes persisting the entities,
 * and for each entity write the stats, including commodities and entity attributes.
 **/
public class StatsWriteCoordinator {

    private final Logger logger = LogManager.getLogger();
    private final ExecutorService statsWritersPool;
    private final Collection<IStatsWriter> chunkedTopologyStatsWriters;
    private final Collection<ICompleteTopologyStatsWriter> completeTopologyStatsWriters;
    private final int writeTopologyChunkSize;

    /**
     * Creates {@link StatsWriteCoordinator} instance.
     *
     * @param statsWritersPool pool which will be used to execute stats aggregating
     *                 and database writing tasks that support independent chunks.
     * @param chunkedTopologyStatsWriters collection of statistics writers which
     *                 would work with independent topology chunks.
     * @param completeTopologyStatsWriters collection of statistics writers which
     *                 cannot work with independent topology chunks because theirs statistics
     * @param writeTopologyChunkSize the number of entities for which stats are
     */
    public StatsWriteCoordinator(@Nonnull ExecutorService statsWritersPool,
                    @Nonnull Collection<IStatsWriter> chunkedTopologyStatsWriters,
                    @Nonnull Collection<ICompleteTopologyStatsWriter> completeTopologyStatsWriters,
                    int writeTopologyChunkSize) {
        this.statsWritersPool = statsWritersPool;
        this.chunkedTopologyStatsWriters = chunkedTopologyStatsWriters;
        this.completeTopologyStatsWriters = completeTopologyStatsWriters;
        this.writeTopologyChunkSize = writeTopologyChunkSize;
    }

    /**
     * Processes multiple chunks provided by {@link RemoteIterator} implementation. Schedules tasks
     * for every chunk to aggregate and write statistics into database, after collection all
     * topology objects into one collection schedules tasks which are requiring whole topology for
     * processing. Waiting completion of all scheduled tasks.
     *
     * @param topologyInfo information about topology which is going to be
     *                 processed.
     * @param dtosIterator provides chunks of topology sent by another component.
     * @return amount of objects processed.
     * @throws CommunicationException in case retrieval of the next chunk has failed
     *                 due to connection issues.
     * @throws TimeoutException if await timed out
     * @throws InterruptedException in case chunk retrieval, writers tasks awaiting
     *                 have been interrupted.
     */
    public int processChunks(@Nonnull final TopologyInfo topologyInfo,
                    @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> dtosIterator)
                    throws CommunicationException, TimeoutException, InterruptedException {
        try (DataMetricTimer dataMetricTimer = SharedMetrics.STATISTICS_AGGREGATION_SUMMARY
                        .labels(SharedMetrics.ALL_AGGREGATORS_LABEL).startTimer()) {
            final Stopwatch chunkTimer = Stopwatch.createStarted();
            final ChunkedStatistics chunkedStatistics = collectChunks(topologyInfo, dtosIterator);
            final Map<DataWritingTask, Future<?>> allWritingTasks =
                            new HashMap<>(chunkedStatistics.getChunkWritingTasks());
            logger.debug("All {} chunks received for {} : {}", chunkedStatistics::getAmountOfChunks,
                            topologyInfo::toString, chunkTimer::toString);

            final Collection<TopologyEntityDTO> allTopologyDTOs =
                            chunkedStatistics.getAllTopologyDTOs();
            allWritingTasks.putAll(submitTasks(topologyInfo, allTopologyDTOs,
                            completeTopologyStatsWriters));
            for (Entry<DataWritingTask, Future<?>> entry : allWritingTasks.entrySet()) {
                final DataWritingTask task = entry.getKey();
                try {
                    entry.getValue().get();
                } catch (ExecutionException e) {
                    logger.error("Cannot write statistics for '{}'", task, e.getCause());
                } finally {
                    logger.trace("Task '{}' completed", task::toString);
                }
            }
            logger.info("Done handling topology notification for realtime topology {} and context {}. Number of entities: {} in {}",
                            topologyInfo.getTopologyId(), topologyInfo.getTopologyContextId(),
                            allTopologyDTOs.size(), dataMetricTimer.getTimeElapsedSecs());
            return allTopologyDTOs.size();
        }
    }

    private ChunkedStatistics collectChunks(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull RemoteIterator<TopologyDTO.Topology.DataSegment> dtosIterator)
                    throws InterruptedException, TimeoutException, CommunicationException {
        /*
         * Stores all DTOs from all independent chunks which will be used to schedule complete
         * topology stats writer tasks.
         */
        final Collection<TopologyEntityDTO> allTopologyDTOs = new ArrayList<>();
        final Map<DataWritingTask, Future<?>> chunkWritingTasks = new HashMap<>();
        int amountOfChunks = 0;
        int numberOfEntities = 0;
        while (dtosIterator.hasNext()) {
            final Collection<TopologyEntityDTO> chunk =
                dtosIterator.nextChunk().stream()
                .filter(TopologyDTO.Topology.DataSegment::hasEntity)
                .map(TopologyDTO.Topology.DataSegment::getEntity)
                .collect(Collectors.toList());
            final int chunkSize = chunk.size();
            numberOfEntities += chunkSize;
            createInternalChunks(new ArrayList<>(chunk))
                .forEach(ch -> chunkWritingTasks.putAll(submitTasks(topologyInfo, ch,
                                                                    chunkedTopologyStatsWriters)));
            logger.debug("Received chunk #{} of size {} for topology {} and context {} [soFar={}]",
                            ++amountOfChunks, chunkSize, topologyInfo.getTopologyId(),
                            topologyInfo.getTopologyContextId(), numberOfEntities);
            allTopologyDTOs.addAll(chunk);
        }
        return new ChunkedStatistics(amountOfChunks, allTopologyDTOs, chunkWritingTasks);
    }

    private Collection<List<TopologyEntityDTO>> createInternalChunks(
                    List<TopologyEntityDTO> chunk) {
        if (chunk.size() > writeTopologyChunkSize) {
            return Lists.partition(chunk, writeTopologyChunkSize);
        }
        return Collections.singleton(chunk);
    }

    private Map<DataWritingTask, Future<Void>> submitTasks(@Nonnull TopologyInfo topologyInfo,
                    @Nonnull Collection<TopologyEntityDTO> allTopologyDTOs,
                    @Nonnull Collection<? extends IStatsWriter> writers) {
        return writers.stream().map(sw -> new DataWritingTask(topologyInfo, allTopologyDTOs, sw))
                        .collect(Collectors.toMap(Function.identity(), statsWritersPool::submit));
    }

    /**
     * {@link DataWritingTask} task which will be scheduled in the thread pool to write data in
     * database using dedicated {@link IStatsWriter} implementation.
     */
    private static class DataWritingTask implements Callable<Void> {
        private final TopologyInfo topologyInfo;
        private final Collection<TopologyEntityDTO> objects;
        private final IStatsWriter writer;

        /**
         * Creates {@link DataWritingTask} instance .
         *
         * @param topologyInfo generic information about topology snapshot currently
         *                 processing.
         * @param objects collection of objects which needs to be processed by
         *                 writer instance in a separate thread.
         * @param writer instance of the {@link IStatsWriter} which should do
         *                 aggregation and storing aggregated information for collection of the
         *                 provided objects.
         */
        private DataWritingTask(@Nonnull TopologyInfo topologyInfo,
                        @Nonnull Collection<TopologyEntityDTO> objects,
                        @Nonnull IStatsWriter writer) {
            this.topologyInfo = topologyInfo;
            this.objects = ImmutableList.copyOf(objects);
            this.writer = writer;
        }

        @Override
        public Void call() throws VmtDbException {
            writer.processObjects(topologyInfo, objects);
            return null;
        }

        @Override
        public String toString() {
            return String.format("%s [writerType=%s, objectsSize=%s]", getClass().getSimpleName(),
                            writer.getClass().getSimpleName(), objects.size());
        }

    }


    /**
     * {@link ChunkedStatistics} contains information about processed collection of chunks.
     */
    private static class ChunkedStatistics {
        private final int amountOfChunks;
        private final Collection<TopologyEntityDTO> allTopologyDTOs;
        private final Map<DataWritingTask, Future<?>> chunkWritingTasks;

        private ChunkedStatistics(int amountOfChunks, Collection<TopologyEntityDTO> allTopologyDTOs,
                        Map<DataWritingTask, Future<?>> chunkWritingTasks) {
            this.amountOfChunks = amountOfChunks;
            this.allTopologyDTOs = allTopologyDTOs;
            this.chunkWritingTasks = chunkWritingTasks;
        }

        private int getAmountOfChunks() {
            return amountOfChunks;
        }

        private Collection<TopologyEntityDTO> getAllTopologyDTOs() {
            return allTopologyDTOs;
        }

        private Map<DataWritingTask, Future<?>> getChunkWritingTasks() {
            return chunkWritingTasks;
        }
    }
}
