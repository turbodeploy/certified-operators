package com.vmturbo.topology.processor.topology;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.SettingsManager;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.templates.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

/**
 * Stores topology snapshots per-target and broadcasts the results to listening components.
 */
@ThreadSafe
public class TopologyHandler {
    private final Logger logger = LogManager.getLogger();

    private final TopoBroadcastManager topoBroadcastManager;

    private final EntityStore entityStore;

    private final long realtimeTopologyContextId;

    private final IdentityProvider identityProvider;

    private final PolicyManager policyManager;

    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier;

    private final DiscoveredGroupUploader discoveredGroupUploader;

    private final SettingsManager settingsManager;

    public TopologyHandler(final long realtimeTopologyContextId,
                           @Nonnull final TopoBroadcastManager topoBroadcastManager,
                           @Nonnull final EntityStore entityStore,
                           @Nonnull final IdentityProvider identityProvider,
                           @Nonnull final PolicyManager policyManager,
                           @Nonnull final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier,
                           @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
                           @Nonnull final SettingsManager settingsManager) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.topoBroadcastManager = Objects.requireNonNull(topoBroadcastManager);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.policyManager = Objects.requireNonNull(policyManager);
        this.discoveredTemplateDeploymentProfileNotifier = Objects.requireNonNull(discoveredTemplateDeploymentProfileNotifier);
        this.discoveredGroupUploader = Objects.requireNonNull(discoveredGroupUploader);
        this.settingsManager = Objects.requireNonNull(settingsManager);
    }

    /**
     * A metric that tracks duration of broadcasts.
     */
    private static final DataMetricSummary TOPOLOGY_BROADCAST_SUMMARY = DataMetricSummary.builder()
        .withName("tp_broadcast_duration_seconds")
        .withHelp("Duration of a topology broadcast.")
        .build()
        .register();

    /**
     * Broadcast the current topology to other services.
     *
     * @return The count of the total number of entities broadcast.
     * @throws InterruptedException if thread has been interrupted during broadcasting
     */
    public synchronized TopologyBroadcastInfo broadcastLatestTopology() throws InterruptedException {

        try {
            discoveredTemplateDeploymentProfileNotifier.sendTemplateDeploymentProfileData();
        } catch (CommunicationException e) {
            logger.error("Failed to send templates data within topology broadcast");
        }

        try (DataMetricTimer timer = TOPOLOGY_BROADCAST_SUMMARY.startTimer()) {
            // TODO (roman, Dec 6 2016): Construct entity stream in the entity store
            // without ever having all of them in memory. Only worry is concurrent
            // modifications.
            TopologyGraph graph = new TopologyGraph(entityStore.constructTopology());
            TopologyFilterFactory topologyFilterFactory = new TopologyFilterFactory();
            GroupResolver groupResolver = new GroupResolver(topologyFilterFactory);

            // TODO: (dblinn, 1/10/2017): I'd like to have a reusable TopologyPipeline here.
            // that has a fixed sequence of stages that feed the output of one stage to the
            // input of the next. We could have stages for policy application, settings application,
            // supply chain validation, stitching, probe calculations, plan supply/demand adjustments etc.
            //
            // It would be a way to simulate the isolation you would naturally get in an SOA pipeline so
            // that if/when the day comes when we have to split things up into true independent services
            // it won't be impossible.
            //
            // TODO: Other places that construct topology should also do this (AnalysisService, TopologyController).

            // karthikt - In the pipeline approach, we would have to traverse the graph in each stage.
            //   If the graph is huge and there are many stages, it would inefficient. Another option
            //   is to have all the operations together and apply them at once. This will involve
            //   just one traversal of the graph. This assumes that these operations would not
            //   transform the graph(i.e changes the structure of the graph). If there is structural
            //   tranformation of graph, then the pipeline approach is the better one

            try {
                discoveredGroupUploader.processQueuedGroups();
            } catch (RuntimeException e) {
                // TODO: Should we continue to broadcast if we can't process the groups?
                logger.error("Failed to process discovered groups", e);
            }
            logger.info("Beginning policy application for context {}", realtimeTopologyContextId);
            try {
                policyManager.applyPolicies(graph, groupResolver);
            } catch (RuntimeException e) {
                // TODO: We probably shouldn't continue to broadcast if we cannot successfully apply policy information.
                logger.error("Unable to apply policies due to error: ", e);
            }

            logger.info("Start applying settings for topology context {}", realtimeTopologyContextId);
            try {
                Map<Long, List<Setting>> entitySettings  = settingsManager.applySettings(groupResolver, graph);
                logger.info("Finished applying settings. Sending the entitySetting mapping of size {} to Group component",
                    entitySettings.size());
                settingsManager.sendEntitySettings(identityProvider.getTopologyId(),
                    realtimeTopologyContextId, entitySettings);
            } catch (RuntimeException e) {
                // TODO: karthikt - Should we stop broadcast if we fail to apply settings?
                logger.error("Unable to apply settings due to error: ", e);
            }

            return broadcastTopology(realtimeTopologyContextId,
                identityProvider.getTopologyId(),
                graph.vertices()
                    .map(Vertex::getTopologyEntityDtoBuilder)
                    .map(TopologyEntityDTO.Builder::build)
                    .collect(Collectors.toList())
            );
        }
    }

    /**
     * Broadcast an arbitrary set of entities as a topology.
     *
     * @param topologyContextId The context ID of the topology.
     * @param topologyId The ID of the topology.
     * @param topology The {@link TopologyEntityDTO} objects in the topology.
     * @return The number of broadcast entities.
     * @throws InterruptedException when the broadcast is interrupted
     */
    public synchronized TopologyBroadcastInfo broadcastTopology(final long topologyContextId,
                                  final long topologyId,
                                  final Collection<TopologyEntityDTO> topology) throws InterruptedException {
        final TopologyType topologyType = topologyContextId == realtimeTopologyContextId
                        ? TopologyType.REALTIME
                        : TopologyType.PLAN;
        final TopologyBroadcast broadcast =
                topoBroadcastManager.broadcastTopology(topologyContextId, topologyId, topologyType);
        for (TopologyEntityDTO entity : topology) {
            broadcast.append(entity);
        }

        long sentCount = broadcast.finish();
        logger.info("Successfully sent {} entities within topology {} for context {}.", sentCount,
                broadcast.getTopologyId(), broadcast.getTopologyContextId());
        return new TopologyBroadcastInfo(broadcast, sentCount);
    }

    /**
     * A container for information about topology broadcasts.
     */
    @Immutable
    public static class TopologyBroadcastInfo {
        /**
         * The number of entities in the broadcast.
         */
        private final long entityCount;

        /**
         * The ID of the topology that was broadcast.
         */
        private final long topologyId;

        /**
         * The ID of the topology context for the topology that was broadcast.
         */
        private final long topologyContextId;

        public TopologyBroadcastInfo(@Nonnull final TopologyBroadcast broadcast,
                                     final long entityCount) {
            this.entityCount = entityCount;
            this.topologyId = broadcast.getTopologyId();
            this.topologyContextId = broadcast.getTopologyContextId();
        }

        public long getEntityCount() {
            return entityCount;
        }

        public long getTopologyId() {
            return topologyId;
        }

        public long getTopologyContextId() {
            return topologyContextId;
        }
    }
}
