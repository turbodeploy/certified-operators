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

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.GuestLoadFilters;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.extractor.topology.ITopologyWriter.TopologyWriterFactory;
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

    private static final DataMetricCounter PROCESSING_ERRORS_CNT = DataMetricCounter.builder()
            .withName("xtr_topology_processing_error_count")
            .withHelp("Errors encountered during topology processing, broken down by high level type.")
            .withLabelNames("type")
            .build()
            .register();

    private final Logger logger = LogManager.getLogger();

    private final List<TopologyWriterFactory<?>> writerFactories;
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
            @Nonnull final List<TopologyWriterFactory<?>> writerFactories,
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
        final String label = TopologyDTOUtil.getSourceTopologyLabel(topologyInfo);
        logger.info("Received topology {}", label);
        if (busy.compareAndSet(false, true)) {
            try (TracingScope scope = Tracing.trace("extractor_run_topology", tracingContext)) {
                logger.info("Processing topology {}", label);
                new TopologyRunner(topologyInfo, entityIterator).runTopology();
            } catch (SQLException | UnsupportedDialectException e) {
                logger.error("Failed to process topology", e);
            } finally {
                RemoteIteratorDrain.drainIterator(entityIterator, label, false);
                busy.set(false);
            }
        } else {
            logger.info("Skipping topology {} because prior execution is not complete", label);
            RemoteIteratorDrain.drainIterator(entityIterator, label, true);
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
        private final String topologyLabel;

        TopologyRunner(final TopologyInfo topologyInfo, final RemoteIterator<DataSegment> entityIterator) {
            this.topologyInfo = topologyInfo;
            this.entityIterator = entityIterator;
            this.topologyLabel = TopologyDTOUtil.getSourceTopologyLabel(topologyInfo);
        }

        private void runTopology() throws UnsupportedDialectException, SQLException {
            final AsyncTimer elapsedTimer = timer.async("Total Elapsed");
            timer.stop();
            long successCount = 0;
            try {
                successCount = writeTopology();
            } catch (IOException | InterruptedException e) {
                logger.error("Interrupted while writing topology {}", topologyLabel, e);
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
                    .map(TopologyWriterFactory::newInstance)
                    .forEach(writers::add);

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
                            if (shouldIngest(dataSegment)) {
                                consumer.getLeft().accept(dataSegment.getEntity());
                            }
                        }
                        timer.stop();
                    }
                }
            } catch (CommunicationException | TimeoutException e) {
                logger.error("Error occurred while receiving topology {}", topologyLabel, e);
                PROCESSING_ERRORS_CNT.labels("communication").increment();
            } catch (InterruptedException e) {
                logger.error("Thread interrupted receiving topology {}", topologyLabel, e);
                PROCESSING_ERRORS_CNT.labels("interrupted").increment();
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                logger.error("Unexpected error occurred while receiving topology {}", topologyLabel, e);
                PROCESSING_ERRORS_CNT.labels(e.getClass().getSimpleName()).increment();
                throw e; // Re-throw the exception to abort reading the topology.
            }

            try {
                // fetch all data from other components for use in finish stage
                if (!writers.isEmpty()) {
                    final boolean requireFullSupplyChain = writers.stream()
                            .anyMatch(ITopologyWriter::requireFullSupplyChain);
                    dataProvider.fetchData(timer, graphBuilder.build(), requireFullSupplyChain);
                }

                for (final ITopologyWriter writer : writers) {
                    AsyncTimer asyncTimer = timer.async(getFinishPhaseLabel(writer));
                    try {
                        writer.finish(dataProvider);
                    } catch (RuntimeException e) {
                        logger.error("Writer {} failed to complete ingestion.",
                                writer.getClass().getSimpleName(), e);
                    }
                    asyncTimer.close();
                }

                // clean unneeded data while keeping useful data for actions ingestion
                dataProvider.clean();
            } catch (RuntimeException e) {
                logger.error("Unexpected error during finish processing for topology {}", topologyLabel, e);
            }
            return entityCount;
        }

        private void scrapeChunk(final Collection<DataSegment> chunk,
                final DataProvider dataProvider) {
            for (DataSegment dataSegment : chunk) {
                if (shouldIngest(dataSegment)) {
                    TopologyEntityDTO topologyEntityDTO = dataSegment.getEntity();
                    // add entity to graph
                    graphBuilder.addEntity(SupplyChainEntity.newBuilder(topologyEntityDTO));
                    // cache pertaining entity data for later use
                    dataProvider.scrapeData(topologyEntityDTO);
                }
            }
        }

        /**
         * Check if the entity should be ingested or not. Do not ingest Guestload application.
         *
         * @param dataSegment the segment which contains entity
         * @return true if entity should be ingested, otherwise false
         */
        private boolean shouldIngest(DataSegment dataSegment) {
            return dataSegment.hasEntity() && GuestLoadFilters.isNotGuestLoad(dataSegment.getEntity());
        }
    }
}
