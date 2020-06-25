package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.topology.ITopologyWriter.getFinishPhaseLabel;
import static com.vmturbo.extractor.topology.ITopologyWriter.getIngestionPhaseLabel;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.extractor.topology.SupplyChainEntity.Builder;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Handler for entity information coming in from the Topology Processor. Listens only to the live
 * topology.
 */
public class TopologyEntitiesListener implements EntitiesListener {
    private final Logger logger = LogManager.getLogger();

    private final List<Supplier<? extends ITopologyWriter>> writerFactories;
    private final GroupServiceBlockingStub groupService;
    private final ActionsServiceBlockingStub actionService;
    private final WriterConfig config;
    private final AtomicBoolean busy = new AtomicBoolean(false);

    /**
     * Create a new instance.
     *
     * @param writerFactories factories to create required writers
     * @param writerConfig    common config parameters for writers
     * @param groupService    group service endpoint
     * @param actionService   action service endpoint
     */
    public TopologyEntitiesListener(
            @Nonnull final List<Supplier<? extends ITopologyWriter>> writerFactories,
            @Nonnull final WriterConfig writerConfig,
            @Nonnull final GroupServiceBlockingStub groupService,
            @Nonnull final ActionsServiceBlockingStub actionService) {
        this.writerFactories = writerFactories;
        this.config = writerConfig;
        this.groupService = groupService;
        this.actionService = actionService;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
            @Nonnull final RemoteIterator<DataSegment> entityIterator) {
        String label = TopologyDTOUtil.getSourceTopologyLabel(topologyInfo);
        if (busy.compareAndSet(false, true)) {
            logger.info("Received topology {}", TopologyDTOUtil.getSourceTopologyLabel(topologyInfo));
            try {
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
            timer.info(String.format("Processed %s entities", successCount), Detail.STAGE_SUMMARY);
        }

        private long writeTopology() throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
            // fetch cluster membership
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
                final DataProvider dataProvider = new DataProvider();
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
                    }
                }

                // fetch all data from other components for use in finish stage
                dataProvider.fetchData(timer, graphBuilder.build(), groupService, actionService,
                        topologyContextId);

                for (final ITopologyWriter writer : writers) {
                    timer.start(getFinishPhaseLabel(writer));
                    writer.finish(dataProvider);
                    timer.stop();
                }
            } catch (CommunicationException | TimeoutException e) {
                logger.error("Error occurred while receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
            } catch (InterruptedException e) {
                logger.error("Thread interrupted receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
            } catch (RuntimeException e) {
                logger.error("Unexpected error occurred while receiving topology " + topologyId
                        + " for context " + topologyContextId, e);
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
                    // cache interested commodities for use later
                    dataProvider.scrapeCommodities(topologyEntityDTO);
                }
            }
        }
    }
}
