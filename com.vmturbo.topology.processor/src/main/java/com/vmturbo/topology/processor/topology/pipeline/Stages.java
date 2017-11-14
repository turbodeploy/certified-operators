package com.vmturbo.topology.processor.topology.pipeline;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.SettingsManager;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PassthroughStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;

/**
 * A wrapper class for the various {@link Stage} and {@link PassthroughStage} implementations.
 * Since the stages are pretty small, it makes some sense to keep them in one place for now.
 */
public class Stages {

    /**
     * This stage uploads discovered groups (and clusters) to the group component.
     * We only do this for the live topology.
     */
    public static class UploadGroupsStage extends PassthroughStage<EntityStore> {

        private final DiscoveredGroupUploader discoveredGroupUploader;

        public UploadGroupsStage(@Nonnull final DiscoveredGroupUploader discoveredGroupUploader) {
            this.discoveredGroupUploader = discoveredGroupUploader;
        }

        @Nonnull
        @Override
        public void passthrough(final EntityStore input) {
            discoveredGroupUploader.processQueuedGroups();
        }
    }

    /**
     * This stage uploads discovered templates and deployment profiles to the plan orchestrator.
     * We only do this for the live topology.
     */
    public static class UploadTemplatesStage extends PassthroughStage<EntityStore> {

        private final DiscoveredTemplateDeploymentProfileNotifier notifier;

        public UploadTemplatesStage(@Nonnull final DiscoveredTemplateDeploymentProfileNotifier notifier) {
            this.notifier = notifier;
        }

        @Nonnull
        @Override
        public void passthrough(final EntityStore input) throws PipelineStageException {
            try {
                notifier.sendTemplateDeploymentProfileData();
            } catch (CommunicationException e) {
                throw new PipelineStageException(e);
            }
        }
    }

    /**
     * This stage stitches together the various sub-topologies discovered by individual targets
     * into a unified "complete" topology.
     *
     * Stitching can happen in both the live topology broadcast and the plan topology broadcast if
     * the plan is on top of the "live" topology.
     */
    public static class StitchingStage extends Stage<EntityStore, Map<Long, Builder>> {

        private final StitchingManager stitchingManager;

        public StitchingStage(@Nonnull final StitchingManager stitchingManager) {
            this.stitchingManager = stitchingManager;
        }

        @Nonnull
        @Override
        public Map<Long, TopologyEntityDTO.Builder> execute(@Nonnull final EntityStore entityStore) {
            return stitchingManager.stitch(entityStore).constructTopology();
        }
    }

    /**
     * This stage acquires an old topology from the repository.
     *
     * We only do this in plans, because the live topology pipeline doesn't depend
     * on old topologies.
     */
    public static class TopologyAcquisitionStage extends Stage<Long, Map<Long, Builder>> {

        private final RepositoryClient repository;

        public TopologyAcquisitionStage(@Nonnull final RepositoryClient repositoryClient) {
            this.repository = Objects.requireNonNull(repositoryClient);
        }

        @Nonnull
        @Override
        public Map<Long, Builder> execute(@Nonnull final Long topologyId) {
            // we need to gather the entire topology in order to perform editing below
            Iterable<RepositoryDTO.RetrieveTopologyResponse> dtos =
                    () -> repository.retrieveTopology(topologyId);
            return StreamSupport.stream(dtos.spliterator(), false)
                    .map(RepositoryDTO.RetrieveTopologyResponse::getEntitiesList)
                    .flatMap(List::stream)
                    .map(TopologyEntityDTO::toBuilder)
                    .collect(Collectors.toMap(TopologyEntityDTOOrBuilder::getOid, Function.identity()));
        }
    }

    /**
     * This stage applies a set of {@link ScenarioChange} objects to a topology.
     *
     * We only do this in plans because only plans have scenario changes on top of a topology.
     */
    public static class TopologyEditStage extends PassthroughStage<Map<Long, Builder>> {

        private final TopologyEditor topologyEditor;
        private final List<ScenarioChange> changes;

        public TopologyEditStage(@Nonnull final TopologyEditor topologyEditor,
                                 @Nonnull final List<ScenarioChange> scenarioChanges) {
            this.topologyEditor = Objects.requireNonNull(topologyEditor);
            this.changes = Objects.requireNonNull(scenarioChanges);
        }

        @Override
        public void passthrough(@Nonnull final Map<Long, TopologyEntityDTO.Builder> input) {
            topologyEditor.editTopology(input, changes);
        }

        @Override
        public boolean required() {
            return true;
        }

    }

    /**
     * This stage creates a {@link TopologyGraph} out of the topology entities.
     */
    public static class GraphCreationStage extends Stage<Map<Long, Builder>, TopologyGraph> {

        @Nonnull
        @Override
        public TopologyGraph execute(@Nonnull final Map<Long, TopologyEntityDTO.Builder> input) {
            return new TopologyGraph(input);
        }
    }

    /**
     * This stage applies policies to a {@link TopologyGraph}. This makes changes
     * to the commodities of entities in the {@link TopologyGraph} to reflect the
     * applied policies.
     */
    public static class PolicyStage extends PassthroughStage<TopologyGraph> {

        private final PolicyManager policyManager;

        public PolicyStage(@Nonnull final PolicyManager policyManager) {
            this.policyManager = policyManager;
        }

        @Override
        public void passthrough(@Nonnull final TopologyGraph input) {
            policyManager.applyPolicies(input, getContext().getGroupResolver());
        }
    }

    /**
     * This stages resolves per-entity settings in the {@link TopologyGraph}, applies
     * any topology-affecting settings to the graph, and sends the settings to the group
     * component.
     *
     * TODO (roman, Nov 13 2017): Split up the uploading of settings into a separate stage, and
     * don't run that stage in plans.
     */
    public static class SettingsApplicationStage extends PassthroughStage<TopologyGraph> {

        private SettingsManager settingsManager;

        public SettingsApplicationStage(@Nonnull final SettingsManager settingsManager) {
            this.settingsManager = settingsManager;
        }

        @Override
        public void passthrough(@Nonnull final TopologyGraph input) {
            // This method does a sync call to Group Component.
            // If GC has trouble, the topology broadcast would be delayed.
            settingsManager.applyAndSendEntitySettings(getContext().getGroupResolver(), input,
                    getContext().getTopologyInfo().getTopologyContextId(),
                    getContext().getTopologyInfo().getTopologyId());
        }
    }

    /**
     * This stage broadcasts the topology represented by a {@link TopologyGraph} out of the
     * topology processor.
     */
    public static class BroadcastStage extends Stage<TopologyGraph, TopologyBroadcastInfo> {

        private final Logger logger = LogManager.getLogger();

        private final TopoBroadcastManager broadcastManager;

        public BroadcastStage(@Nonnull final TopoBroadcastManager broadcastManager) {
            this.broadcastManager = broadcastManager;
        }

        @Nonnull
        @Override
        public TopologyBroadcastInfo execute(@Nonnull final TopologyGraph input)
                throws PipelineStageException, InterruptedException {
            final Iterator<TopologyEntityDTO> entities = input.vertices()
                    .map(vertex -> vertex.getTopologyEntityDtoBuilder().build())
                    .iterator();
            try {
                switch (getContext().getTopologyInfo().getTopologyType()) {
                    case REALTIME:
                        return broadcastTopology(broadcastManager::broadcastLiveTopology, entities);
                    case PLAN:
                        return broadcastTopology(broadcastManager::broadcastUserPlanTopology, entities);
                    default:
                        throw new IllegalStateException();
                }
            } catch (CommunicationException e) {
                throw new PipelineStageException(e);
            }
        }

        private TopologyBroadcastInfo broadcastTopology(
                @Nonnull Function<TopologyInfo, TopologyBroadcast> broadcastFunction,
                final Iterator<TopologyEntityDTO> topology)
                throws InterruptedException, CommunicationException {
            final TopologyBroadcast broadcast = broadcastFunction.apply(getContext().getTopologyInfo());
            for (Iterator<TopologyEntityDTO> it = topology; it.hasNext(); ) {
                final TopologyEntityDTO entity = it.next();
                broadcast.append(entity);
            }
            final long sentCount = broadcast.finish();
            logger.info("Successfully sent {} entities within topology {} for context {}.", sentCount,
                    broadcast.getTopologyId(), broadcast.getTopologyContextId());
            return new TopologyBroadcastInfo(broadcast, sentCount);
        }
    }
}
