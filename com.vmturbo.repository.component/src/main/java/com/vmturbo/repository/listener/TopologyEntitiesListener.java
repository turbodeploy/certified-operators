package com.vmturbo.repository.listener;

import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.arangodb.ArangoDBException;

import io.opentracing.SpanContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.DataSegment;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary.ResultCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.RepositoryNotificationSender;
import com.vmturbo.repository.SharedMetrics;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyCreator;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.EntitiesWithNewStateListener;
import com.vmturbo.topology.processor.api.TopologySummaryListener;

/**
 * Handler for entity information coming in from the Topology Processor. Only used to receive
 * live topology.
 */
public class TopologyEntitiesListener implements EntitiesListener, TopologySummaryListener,
    EntitiesWithNewStateListener {

    private final Logger logger = LoggerFactory.getLogger(TopologyEntitiesListener.class);

    private final RepositoryNotificationSender notificationSender;
    private final TopologyLifecycleManager topologyManager;
    private final LiveTopologyStore liveTopologyStore;
    private final Predicate<TopologyEntityDTO> entitiesFilter;

    private Object topologyInfoLock = new Object();

    @GuardedBy("topologyInfoLock")
    private TopologyInfo latestKnownRealtimeTopologyInfo = null;

    public TopologyEntitiesListener(@Nonnull final TopologyLifecycleManager topologyManager,
                                    @Nonnull final LiveTopologyStore liveTopologyStore,
                                    @Nonnull final RepositoryNotificationSender sender,
                                    @Nonnull final Predicate<TopologyEntityDTO> entitiesFilter) {
        this.notificationSender = Objects.requireNonNull(sender);
        this.topologyManager = Objects.requireNonNull(topologyManager);
        this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
        this.entitiesFilter = Objects.requireNonNull(entitiesFilter);
    }


    /**
     * Consume a {@link TopologySummary} message. This method is synchronized to guard access to the
     * "last known topology info" object.
     *
     * @param topologySummary
     */
    @Override
    public void onTopologySummary(final TopologySummary topologySummary) {
        // the topology summaries will generally arrive before the rest of the topology data. We
        // will keep track of the most recently received one and use it to try to detect situations
        // where we may be processing stale realtime topologies.
        final TopologyInfo topologyInfo = topologySummary.getTopologyInfo();
        if (topologyInfo.getTopologyType() == TopologyType.REALTIME &&
                topologySummary.getResultCase() == ResultCase.SUCCESS) {
            synchronized (topologyInfoLock) {
                logger.info("Received topology summary. Setting latest known realtime topology" +
                    " id to {}", topologyInfo.getTopologyId());
                latestKnownRealtimeTopologyInfo = topologySummary.getTopologyInfo();
            }
        }
    }

    /**
     * Should this topology be processed? We will not process the topology if it is deemed to be
     * "stale". A topology is considered "stale" if there is a newer version of it available in kafka,
     * as reported by the {@link TopologySummary} messages.
     *
     * Only realtime topologies would be checked for staleness -- plan topologies will always be
     * recommended for processing.
     *
     * This method is synchronized to guard the access to the "last known topology info" object.
     *
     * @param incomingTopologyInfo the {@link TopologyInfo} for the topology to be considered for
     *                             processing.
     * @return true, if the topology should be processed.
     */
    private boolean shouldProcessTopology(@Nonnull final TopologyInfo incomingTopologyInfo) {
        // always process non-realtime topologies.
        // TODO: Does a plan topology ever come in here?
        if (incomingTopologyInfo.getTopologyType() != TopologyType.REALTIME) {
            return true;
        }

        synchronized (topologyInfoLock) {
            if (latestKnownRealtimeTopologyInfo == null) {
                return true; // might as well process it.
            }

            // for realtime topologies, only process if the topology id is the same or greater than the
            // last known topology id. NOTE: we can check the creation time instead, but we expect the
            // topology id's to be generated in increasing order, so we shouldn't need to.
            return (incomingTopologyInfo.getCreationTime() >= latestKnownRealtimeTopologyInfo.getCreationTime());
        }
    }

    @Override
    public void onTopologyNotification(@Nonnull TopologyInfo topologyInfo,
                                       @Nonnull RemoteIterator<DataSegment> entityIterator,
                                       @Nonnull final SpanContext tracingContext) {
        try {
            onTopologyNotificationInternal(topologyInfo, entityIterator, tracingContext);
        } catch (CommunicationException | InterruptedException | ArangoDBException e) {
            logger.error("Error processing realtime topology."
                + " Topology Id: " + topologyInfo.getTopologyId()
                + " Context Id: " + topologyInfo.getTopologyContextId(), e);
        }
    }

    private void onTopologyNotificationInternal(TopologyInfo topologyInfo,
                                                final RemoteIterator<DataSegment> entityIterator,
                                                @Nonnull final SpanContext tracingContext)
        throws CommunicationException, InterruptedException {
        final long topologyId = topologyInfo.getTopologyId();
        final long topologyContextId = topologyInfo.getTopologyContextId();
        logger.info("Received topology {} for context {} topology DTOs from TopologyProcessor",
                topologyId, topologyContextId);

        // check if we should skip this one
        if (!shouldProcessTopology(topologyInfo)) {
            logger.info("Skipping processing of topology {}, since it appears to be older than"
                    +" latest known realtime topology.", topologyId);
            TopologyEntitiesUtil.drainIterator(entityIterator, topologyId);
            return;
        }

        final DataMetricTimer timer = SharedMetrics.TOPOLOGY_DURATION_SUMMARY
            .labels(SharedMetrics.SOURCE_LABEL)
            .startTimer();
        final TopologyID tid = new TopologyID(topologyContextId, topologyId, TopologyID.TopologyType.SOURCE);
        TopologyCreator<TopologyEntityDTO> topologyCreator = null;
        try (TracingScope tracingScope = Tracing.trace("repository_handle_topology", tracingContext)) {
            topologyCreator = topologyManager.newSourceTopologyCreator(tid, topologyInfo);
            TopologyEntitiesUtil.createTopology(entityIterator, topologyId, topologyContextId, timer,
                tid, topologyCreator, notificationSender, entitiesFilter);
        }  catch (InterruptedException e) {
            logger.error("Thread interrupted receiving source topology " + topologyId, e);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Thread interrupted when receiving source topology " + topologyId +
                    " + for context " + topologyContextId + ": " + e.getMessage());
            if (topologyCreator != null) {
                topologyCreator.rollback();
            }
            throw e;
        } catch (Exception e) {
            logger.error("Error occurred during retrieving source topology " + topologyId, e);
            SharedMetrics.TOPOLOGY_COUNTER.labels(SharedMetrics.SOURCE_LABEL, SharedMetrics.FAILED_LABEL).increment();
            notificationSender.onSourceTopologyFailure(topologyId, topologyContextId,
                "Error receiving source topology " + topologyId +
                    " + for context " + topologyContextId + ": " + e.getMessage());
            if (topologyCreator != null) {
                topologyCreator.rollback();
            }
            throw e;
        }
    }

    @Override
    public void onEntitiesWithNewState(@Nonnull final EntitiesWithNewState entitiesWithNewState) {
        liveTopologyStore.setEntityWithUpdatedState(entitiesWithNewState);
    }
}
