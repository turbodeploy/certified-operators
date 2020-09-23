package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.topology.ITopologyWriter.getFinishPhaseLabel;
import static com.vmturbo.extractor.topology.ITopologyWriter.getIngestionPhaseLabel;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.extractor.topology.SupplyChainEntity.Builder;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Handler for entity information coming in from the Topology Processor. Listens only to the live
 * topology.
 */
public class TopologyEntitiesListener implements EntitiesListener {

    private static final DataMetricHistogram PROCESSING_HISTOGRAM = DataMetricHistogram.builder()
            .withName("xtr_topology_processing_duration_seconds")
            .withHelp("Duration of topology processing (reading, fetching related data, and writing),"
                    + " broken down by stage. Only for topologies that fully processed without errors.")
            .withLabelNames("stage")
            // 10s, 1min, 3min, 7min, 10min, 15min, 30min
            // The SLA is to process everything in under 10 minutes.
            .withBuckets(10, 60, 180, 420, 600, 900, 1800)
            .build()
            .register();

    private static final DataMetricCounter  PROCESSING_ERRORS_CNT = DataMetricCounter.builder()
            .withName("xtr_topology_processing_error_count")
            .withHelp("Errors encountered during topology processing, broken down by high level type.")
            .withLabelNames("type")
            .build()
            .register();

    private final Logger logger = LogManager.getLogger();

    private final List<Supplier<? extends ITopologyWriter>> writerFactories;
    private final WriterConfig config;
    private final AtomicBoolean busy = new AtomicBoolean(false);
    private final DataProvider dataProvider;

    /**
     * Create a new instance.
     *
     * @param writerFactories factories to create required writers
     * @param writerConfig    common config parameters for writers
     * @param dataProvider    providing data for ingestion
     */
    public TopologyEntitiesListener(
            @Nonnull final List<Supplier<? extends ITopologyWriter>> writerFactories,
            @Nonnull final WriterConfig writerConfig,
            @Nonnull final DataProvider dataProvider) {
        this.writerFactories = writerFactories;
        this.config = writerConfig;
        this.dataProvider = dataProvider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
            @Nonnull final RemoteIterator<DataSegment> entityIterator,
            @Nonnull final SpanContext tracingContext) {
        String label = TopologyDTOUtil.getSourceTopologyLabel(topologyInfo);
        if (busy.compareAndSet(false, true)) {
            logger.info("Received topology {}", TopologyDTOUtil.getSourceTopologyLabel(topologyInfo));
            try (TracingScope scope = Tracing.trace("extractor_run_topology", tracingContext)) {
                new TopologyRunner(topologyInfo, entityIterator).runTopology();
            } catch (SQLException | UnsupportedDialectException e) {
                logger.error("Failed to process topology", e);
            } finally {
                RemoteIteratorDrain.drainIterator(entityIterator, label, false);
                busy.set(false);
            }
        } else {
            RemoteIteratorDrain.drainIterator(
                    entityIterator, TopologyDTOUtil.getSourceTopologyLabel(topologyInfo), true);
        }
    }

    /**
     * Class instantiated for each topology, responsible for processing the topology.
     */
    private class TopologyRunner {

        private final MultiStageTimer timer = new MultiStageTimer(logger);

        private final TopologyGraphCreator<Builder, SupplyChainEntity> graphBuilder
                = new TopologyGraphCreator<>();

        private final TopologyInfo topologyInfo;
        private final RemoteIterator<DataSegment> entityIterator;

        TopologyRunner(final TopologyInfo topologyInfo, final RemoteIterator<DataSegment> entityIterator) {
            this.topologyInfo = topologyInfo;
            this.entityIterator = entityIterator;
        }

        private void runTopology() throws UnsupportedDialectException, SQLException {
            final AsyncTimer elapsedTimer = timer.async("Total Elapsed");
            timer.stop();
            long successCount = 0;
            try {
                successCount = writeTopology();
            } catch (IOException | InterruptedException e) {
                logger.error("Interrupted while writing topology", e);
                Thread.currentThread().interrupt();
            }
            elapsedTimer.close();
            // Record all the stages into the Prometheus metric.
            timer.visit((stageName, stopped, totalDurationMs) -> {
                if (stopped) {
                    PROCESSING_HISTOGRAM.labels(stageName)
                            .observe((double)TimeUnit.MILLISECONDS.toSeconds(totalDurationMs));
                }
            });
            timer.info(String.format("Processed %s entities", successCount), Detail.STAGE_SUMMARY);
        }

        private long writeTopology() throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
            List<ITopologyWriter> writers = new ArrayList<>();
            writerFactories.stream()
                    .map(Supplier::get)
                    .forEach(writers::add);

            final long topologyId = topologyInfo.getTopologyId();
            final long topologyContextId = topologyInfo.getTopologyContextId();

            final List<Pair<Consumer<TopologyEntityDTO>, ITopologyWriter>> entityConsumers = new ArrayList<>();
            for (ITopologyWriter writer : writers) {
                Consumer<TopologyEntityDTO> entityConsumer =
                        writer.startTopology(topologyInfo, config, timer);
                entityConsumers.add(Pair.of(entityConsumer, writer));
            }
            long entityCount = 0L;
            try {
                while (entityIterator.hasNext()) {
                    timer.start("Chunk Retrieval");
                    final Collection<DataSegment> chunk = entityIterator.nextChunk();
                    timer.stop();
                    entityCount += chunk.size();
                    scrapeChunk(chunk, dataProvider);
                    for (Pair<Consumer<TopologyEntityDTO>, ITopologyWriter> consumer : entityConsumers) {
                        timer.start(getIngestionPhaseLabel(consumer.getRight()));
                        for (final DataSegment dataSegment : chunk) {
                            if (dataSegment.hasEntity()) {
                                consumer.getLeft().accept(dataSegment.getEntity());
                            }
                        }
                        timer.stop();
                    }
                }

                // fetch all data from other components for use in finish stage
                if (!writers.isEmpty()) {
                    final boolean requireSupplyChainForAllEntities = writers.stream()
                            .anyMatch(ITopologyWriter::requireSupplyChainForAllEntities);
                    dataProvider.fetchData(timer, graphBuilder.build(),
                            requireSupplyChainForAllEntities);
                }

                for (final ITopologyWriter writer : writers) {
                    AsyncTimer asyncTimer = timer.async(getFinishPhaseLabel(writer));
                    writer.finish(dataProvider);
                    asyncTimer.close();
                }

                // clean unneeded data while keeping useful data for actions ingestion
                dataProvider.clean();
            } catch (CommunicationException | TimeoutException e) {
                logger.error("Error occurred while receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
                PROCESSING_ERRORS_CNT.labels("communication").increment();
            } catch (InterruptedException e) {
                logger.error("Thread interrupted receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
                PROCESSING_ERRORS_CNT.labels("interrupted").increment();
            } catch (RuntimeException e) {
                logger.error("Unexpected error occurred while receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
                PROCESSING_ERRORS_CNT.labels(e.getClass().getSimpleName()).increment();
                throw e; // Re-throw the exception to abort reading the topology.
            }
            return entityCount;
        }

        private void scrapeChunk(final Collection<DataSegment> chunk,
                                 final DataProvider dataProvider) {
            for (DataSegment dataSegment : chunk) {
                if (dataSegment.hasEntity()) {
                    TopologyEntityDTO topologyEntityDTO = dataSegment.getEntity();
                    // add entity to graph
                    graphBuilder.addEntity(SupplyChainEntity.newBuilder(topologyEntityDTO));
                    // cache pertaining entity data for later use
                    dataProvider.scrapeData(topologyEntityDTO);
                }
            }
        }
    }
}
