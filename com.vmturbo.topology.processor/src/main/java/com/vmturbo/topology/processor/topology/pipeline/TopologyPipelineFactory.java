package com.vmturbo.topology.processor.topology.pipeline;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileNotifier;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.TopologyEditor;
import com.vmturbo.topology.processor.topology.pipeline.Stages.BroadcastStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.GraphCreationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.PolicyStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ExtractTopologyGraphStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.ScopeResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsApplicationStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsResolutionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.SettingsUploadStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.StitchingStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyAcquisitionStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopologyEditStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadGroupsStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.UploadTemplatesStage;

/**
 * A factory class for properly configured {@link TopologyPipeline} objects.
 * <p>
 * Users should not instantiate {@link TopologyPipeline}s themselves. Instead, they should
 * use the appropriately configured pipelines provided by this factory - e.g.
 * {@link TopologyPipelineFactory#liveTopology(TopologyInfo)}.
 */
public class TopologyPipelineFactory {

    private final TopoBroadcastManager topoBroadcastManager;

    private final PolicyManager policyManager;

    private final StitchingManager stitchingManager;

    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier;

    private final DiscoveredGroupUploader discoveredGroupUploader;

    private final EntitySettingsApplicator settingsApplicator;

    private final EntitySettingsResolver entitySettingsResolver;

    private final TopologyEditor topologyEditor;

    private final RepositoryClient repositoryClient;

    private final TopologyFilterFactory topologyFilterFactory;

    private final GroupServiceBlockingStub groupServiceClient;

    public TopologyPipelineFactory(@Nonnull final TopoBroadcastManager topoBroadcastManager,
               @Nonnull final PolicyManager policyManager,
               @Nonnull final StitchingManager stitchingManager,
               @Nonnull final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier,
               @Nonnull final DiscoveredGroupUploader discoveredGroupUploader,
               @Nonnull final EntitySettingsResolver entitySettingsResolver,
               @Nonnull final EntitySettingsApplicator settingsApplicator,
               @Nonnull final TopologyEditor topologyEditor,
               @Nonnull final RepositoryClient repositoryClient,
               @Nonnull final TopologyFilterFactory topologyFilterFactory,
               @Nonnull final GroupServiceBlockingStub groupServiceClient) {
        this.topoBroadcastManager = topoBroadcastManager;
        this.policyManager = policyManager;
        this.stitchingManager = stitchingManager;
        this.discoveredTemplateDeploymentProfileNotifier = discoveredTemplateDeploymentProfileNotifier;
        this.discoveredGroupUploader = discoveredGroupUploader;
        this.settingsApplicator = Objects.requireNonNull(settingsApplicator);
        this.entitySettingsResolver = entitySettingsResolver;
        this.topologyEditor = Objects.requireNonNull(topologyEditor);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.topologyFilterFactory = Objects.requireNonNull(topologyFilterFactory);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
    }

    /**
     * Create a pipeline that constructs and broadcasts the most up-to-date live topology.
     *
     * @param topologyInfo The source topology info values. This will be cloned and potentially
     *                     edited during pipeline execution.
     * @return The {@link TopologyPipeline}. This pipeline will accept an {@link EntityStore}
     *         and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    public TopologyPipeline<EntityStore, TopologyBroadcastInfo> liveTopology(
            @Nonnull final TopologyInfo topologyInfo) {
        final GroupResolver groupResolver = new GroupResolver(topologyFilterFactory);
        final TopologyPipelineContext context =
                new TopologyPipelineContext(groupResolver, topologyInfo);
        return TopologyPipeline.<EntityStore, TopologyBroadcastInfo>newBuilder(context)
                .addStage(new UploadGroupsStage(discoveredGroupUploader))
                .addStage(new UploadTemplatesStage(discoveredTemplateDeploymentProfileNotifier))
                .addStage(new StitchingStage(stitchingManager))
                .addStage(new GraphCreationStage())
                .addStage(new PolicyStage(policyManager))
                .addStage(SettingsResolutionStage.live(entitySettingsResolver))
                .addStage(new SettingsUploadStage(entitySettingsResolver))
                .addStage(new SettingsApplicationStage(settingsApplicator))
                .addStage(new ExtractTopologyGraphStage())
                .addStage(new BroadcastStage(topoBroadcastManager))
                .build();
    }

    /**
     * Create a pipeline that constructs and broadcasts the most up-to-date live topology AND
     * applies a set of changes to it.
     *
     * @param topologyInfo The source topology info values. This will be cloned and potentially
     *                     edited during pipeline execution.
     * @param changes The list of changes to apply.
     * @return The {@link TopologyPipeline}. This pipeline will accept an {@link EntityStore}
     *         and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    public TopologyPipeline<EntityStore, TopologyBroadcastInfo> planOverLiveTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final List<ScenarioChange> changes,
            @Nullable final PlanScope scope) {
        final TopologyFilterFactory topologyFilterFactory = new TopologyFilterFactory();
        final GroupResolver groupResolver = new GroupResolver(topologyFilterFactory);
        final TopologyPipelineContext context =
                new TopologyPipelineContext(groupResolver, topologyInfo);
        return TopologyPipeline.<EntityStore, TopologyBroadcastInfo>newBuilder(context)
                .addStage(new StitchingStage(stitchingManager))
                .addStage(new TopologyEditStage(topologyEditor, changes))
                .addStage(new GraphCreationStage())
                .addStage(new PolicyStage(policyManager))
                .addStage(new ScopeResolutionStage(groupServiceClient, scope))
                .addStage(SettingsResolutionStage.plan(entitySettingsResolver, changes))
                .addStage(new SettingsApplicationStage(settingsApplicator))
                .addStage(new ExtractTopologyGraphStage())
                .addStage(new BroadcastStage(topoBroadcastManager))
                .build();
    }

    /**
     * Create a pipeline that acquires a previously-broadcast topology and applies a set of changes
     * to it. The main purpose of this is plan-over-plan.
     *
     * @param topologyInfo The source topology info values. This will be cloned and potentially
     *                     edited during pipeline execution.
     * @param changes The list of changes to apply.
     * @return The {@link TopologyPipeline}. This pipeline will accept a long (the ID of the old
     *         topology) and return the {@link TopologyBroadcastInfo} of the successful broadcast.
     */
    public TopologyPipeline<Long, TopologyBroadcastInfo> planOverOldTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final List<ScenarioChange> changes,
            @Nullable final PlanScope scope) {
        final TopologyFilterFactory topologyFilterFactory = new TopologyFilterFactory();
        final GroupResolver groupResolver = new GroupResolver(topologyFilterFactory);
        final TopologyPipelineContext context =
                new TopologyPipelineContext(groupResolver, topologyInfo);
        return TopologyPipeline.<Long, TopologyBroadcastInfo>newBuilder(context)
                .addStage(new TopologyAcquisitionStage(repositoryClient))
                .addStage(new TopologyEditStage(topologyEditor, changes))
                .addStage(new GraphCreationStage())
                .addStage(new ScopeResolutionStage(groupServiceClient, scope))
                // TODO (roman, Nov 2017): We need to do policy and setting application for
                // plan-over-plan as well. However, the topology we get from the repository
                // already has some policies and settings applied to it. In order to run those
                // stages here we need to be able to apply policies/settings on top of existing ones.
                //
                // One approach is to clear settings/policies from a topology, and then run the
                // stages. The other approach is to extend the stages to handle already-existing
                // policies/settings.
                .addStage(new BroadcastStage(topoBroadcastManager))
                .build();
    }
}
