package com.vmturbo.repository.listener;

import java.util.Collection;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.SharedMetrics;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyEntitiesException;

/**
 * Utility class for topology creation in repository.
 */
public class TopologyEntitiesUtil {

    private static final Logger logger = LoggerFactory.getLogger(TopologyEntitiesUtil.class);

    private TopologyEntitiesUtil() {}

    /**
     * Create topology in repository.
     *
     * @param entityIterator Iterator over the entities
     * @param topologyId The topology ID
     * @param topologyContextId The topology context ID
     * @param timer Timer
     * @param tid The TopologyID
     * @param topologyCreator The topology creator
     * @param notificationSender The notification sender
     * @throws CommunicationException Throws CommunicationException
     * @throws InterruptedException Throws InterruptedException
     */
    public static void createTopology(@Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator,
                                      @Nonnull final long topologyId,
                                      @Nonnull final long topologyContextId,
                                      @Nonnull final DataMetricTimer timer,
                                      @Nonnull final TopologyID tid,
                                      @Nonnull final TopologyCreator<TopologyEntityDTO> topologyCreator,
                                      @Nonnull final RepositoryNotificationSender notificationSender)
            throws CommunicationException, InterruptedException {
        createTopology(entityIterator, topologyId, topologyContextId, timer, tid, topologyCreator, notificationSender, e -> true);
    }

    /**
     * Create topology in repository.
     *
     * @param entityIterator Iterator over the entities
     * @param topologyId The topology ID
     * @param topologyContextId The topology context ID
     * @param timer Timer
     * @param tid The TopologyID
     * @param topologyCreator The topology creator
     * @param notificationSender The notification sender
     * @param entitiesFilter entities filter
     * @throws CommunicationException Throws CommunicationException
     * @throws InterruptedException Throws InterruptedException
     */
    public static void createTopology(@Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator,
                                      @Nonnull final long topologyId,
                                      @Nonnull final long topologyContextId,
                                      @Nonnull final DataMetricTimer timer,
                                      @Nonnull final TopologyID tid,
                                      @Nonnull final TopologyCreator<TopologyEntityDTO> topologyCreator,
                                      @Nonnull final RepositoryNotificationSender notificationSender,
                                      @Nonnull final Predicate<TopologyEntityDTO> entitiesFilter)
            throws CommunicationException, InterruptedException {
        try {
            topologyCreator.initialize();
            logger.info("Start updating topology {}", tid);
            int numberOfEntities = 0;
            int chunkNumber = 0;
            while (entityIterator.hasNext()) {
                Collection<TopologyDTO.Topology.DataSegment> chunk = entityIterator.nextChunk();
                logger.debug("Received chunk #{} of size {} for topology {}", ++chunkNumber, chunk.size(), tid);
                Collection<TopologyEntityDTO> entities =
                    chunk.stream()
                         .filter(TopologyDTO.Topology.DataSegment::hasEntity)
                         .map(TopologyDTO.Topology.DataSegment::getEntity)
                         .filter(entitiesFilter)
                         .collect(Collectors.toList());
                if (entities.isEmpty()) {
                    continue;
                }
                topologyCreator.addEntities(entities, tid);
                numberOfEntities += entities.size();
            }
            topologyCreator.complete();

            SharedMetrics.TOPOLOGY_ENTITY_COUNT_GAUGE
                    .labels(SharedMetrics.SOURCE_LABEL)
                    .setData((double)numberOfEntities);
            notificationSender.onSourceTopologyAvailable(topologyId, topologyContextId);

            double timeTaken = timer.observe();
            logger.info("Finished updating topology {} with {} entities in {} s", tid, numberOfEntities, timeTaken);
        } catch (GraphDatabaseException | CommunicationException |
                TopologyEntitiesException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId, e);
            topologyCreator.rollback();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                    "Error receiving source topology " + topologyId +
                            " for context " + topologyContextId + ": " + e.getMessage());
        } catch (InterruptedException e) {
            logger.error("Thread interrupted receiving topology " + topologyId, e);
            topologyCreator.rollback();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                    "Error receiving source topology " + topologyId +
                            " for context " + topologyContextId + ": " + e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Exception while receiving topology " + topologyId, e);
            topologyCreator.rollback();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                    "Error receiving source topology " + topologyId +
                            " for context " + topologyContextId + ": " + e.getMessage());
            throw e;
        }
    }

    /**
     * Drain the entity iterator to free memory.
     *
     * @param entityIterator The entity iterator
     * @param topologyId Topology ID
     * @throws InterruptedException Throws InterruptedException
     * @throws CommunicationException Throws CommunicationException
     */
    public static void drainIterator(@Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator,
                               final long topologyId)
            throws InterruptedException, CommunicationException {
        // drain the iterator and exit.
        try {
            while (entityIterator.hasNext()) {
                entityIterator.nextChunk();
            }
        } catch (TimeoutException e) {
            logger.warn("TimeoutException while skipping topology {}", topologyId);
        } finally {
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.SKIPPED_LABEL).increment();
        }
    }
}
