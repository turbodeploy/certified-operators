package com.vmturbo.market.component.api.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import io.opentracing.SpanContext;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityCosts;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Metadata;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.ChunkingReceiver;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.AnalysisStatusNotificationListener;
import com.vmturbo.market.component.api.AnalysisSummaryListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.ProjectedEntityCostsListener;
import com.vmturbo.market.component.api.ProjectedReservedInstanceCoverageListener;
import com.vmturbo.market.component.api.ProjectedTopologyListener;

/**
 * The notification receiver connecting to the Market Component.
 */
public class MarketComponentNotificationReceiver extends
        ComponentNotificationReceiver<ActionPlan> implements MarketComponent{

    /**
     * Projected topologies topic. Should be synchronized with kafka-config.yml
     */
    public static final String PROJECTED_TOPOLOGIES_TOPIC = "projected-topologies";

    /**
     * Projected costs topic. Should be synchronized with kafka-config.yml.
     */
    public static final String PROJECTED_ENTITY_COSTS_TOPIC = "projected-entity-costs";

    /**
     * Projected entity coverage topic. Should be synchronized with kafka-config.yml.
     */
    public static final String PROJECTED_ENTITY_RI_COVERAGE_TOPIC = "projected-entity-ri-coverage";

    /**
     * Action plans topic. Should be synchronized with kafka-config.yml
     */
    public static final String ACTION_PLANS_TOPIC = "action-plans";

    /**
     * Analysis results topic.
     */
    public static final String ANALYSIS_SUMMARY_TOPIC = "analysis-summary";

    /**
     * Analysis status topic. Should be synchronized with kafka-config.yml
     */
    public static final String ANALYSIS_STATUS_NOTIFICATION_TOPIC = "analysis-status-notification";

    private final Set<ActionsListener> actionsListenersSet;

    private final Set<ProjectedTopologyListener> projectedTopologyListenersSet;
    private final Set<ProjectedEntityCostsListener> projectedEntityCostsListenersSet;
    private final Set<ProjectedReservedInstanceCoverageListener> projectedEntityRiCoverageListenersSet;
    private final ChunkingReceiver<ProjectedTopologyEntity> projectedTopologyChunkReceiver;
    private final ChunkingReceiver<EntityCost> projectedEntityCostChunkReceiver;
    private final ChunkingReceiver<EntityReservedInstanceCoverage> projectedEntityRiCoverageChunkReceiver;
    private final MulticastNotificationReceiver<AnalysisSummary, AnalysisSummaryListener> analysisSummaryHandler;
    private final MulticastNotificationReceiver<AnalysisStatusNotification, AnalysisStatusNotificationListener> analysisStatusHandler;

    public MarketComponentNotificationReceiver(
            @Nullable final IMessageReceiver<ProjectedTopology> projectedTopologyReceiver,
            @Nullable final IMessageReceiver<ProjectedEntityCosts> projectedEntityCostsReceiver,
            @Nullable final IMessageReceiver<ProjectedEntityReservedInstanceCoverage> projectedEntityRiCoverageReceiver,
            @Nullable final IMessageReceiver<ActionPlan> actionPlanReceiver,
            @Nullable final IMessageReceiver<AnalysisSummary> analysisSummaryReceiver,
            @Nullable final IMessageReceiver<AnalysisStatusNotification> analysisStatusReceiver,
            @Nonnull final ExecutorService executorService,
            final int kafkaReceiverTimeoutSeconds) {
        super(actionPlanReceiver, executorService);
        if (projectedTopologyReceiver != null) {
            projectedTopologyListenersSet = Sets.newConcurrentHashSet();
            projectedTopologyReceiver.addListener(this::processProjectedTopology);
        } else {
            projectedTopologyListenersSet = Collections.emptySet();
        }

        if (projectedEntityCostsReceiver != null) {
            projectedEntityCostsListenersSet = Sets.newConcurrentHashSet();
            projectedEntityCostsReceiver.addListener(this::processProjectedEntityCosts);
        } else {
            projectedEntityCostsListenersSet = Collections.emptySet();
        }

        if (projectedEntityRiCoverageReceiver != null) {
            projectedEntityRiCoverageListenersSet = Sets.newConcurrentHashSet();
            projectedEntityRiCoverageReceiver.addListener(this::processProjectedEntityRiCoverage);
        } else {
            projectedEntityRiCoverageListenersSet = Collections.emptySet();
        }

        if (actionPlanReceiver != null) {
            actionsListenersSet = Sets.newConcurrentHashSet();
        } else {
            actionsListenersSet = Collections.emptySet();
        }

        projectedTopologyChunkReceiver = new ChunkingReceiver<>(executorService);
        projectedEntityCostChunkReceiver = new ChunkingReceiver<>(executorService);
        projectedEntityRiCoverageChunkReceiver = new ChunkingReceiver<>(executorService);

        if (analysisSummaryReceiver == null) {
            analysisSummaryHandler = null;
        } else {
            analysisSummaryHandler = new MulticastNotificationReceiver<>(analysisSummaryReceiver, executorService,
                    kafkaReceiverTimeoutSeconds, analysisSummary -> l -> l.onAnalysisSummary(analysisSummary));
        }
        // Initialize Set of listeners of Analysis Status.
        if (analysisStatusReceiver == null) {
            analysisStatusHandler = null;
        } else {
            analysisStatusHandler = new MulticastNotificationReceiver<>(analysisStatusReceiver, executorService,
              kafkaReceiverTimeoutSeconds, analysisStatusNotification -> l -> l.onAnalysisStatusNotification(analysisStatusNotification));
        }
    }

    @Override
    public void addActionsListener(@Nonnull final ActionsListener listener) {
        actionsListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    public void addProjectedTopologyListener(@Nonnull final ProjectedTopologyListener listener) {
        projectedTopologyListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    public void addProjectedEntityCostsListener(@Nonnull final ProjectedEntityCostsListener listener) {
        projectedEntityCostsListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    public void addProjectedEntityRiCoverageListener(
                    @Nonnull final ProjectedReservedInstanceCoverageListener listener) {
        projectedEntityRiCoverageListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    public void addAnalysisStatusListener(@Nonnull final AnalysisStatusNotificationListener listener) {
        if (analysisStatusHandler == null) {
            throw new IllegalStateException("The analysis status topic has not been subscribed to.");
        }
        analysisStatusHandler.addListener(Objects.requireNonNull(listener));
    }

    @Override
    public void addAnalysisSummaryListener(@Nonnull final AnalysisSummaryListener listener) {
        if (analysisSummaryHandler == null) {
            throw new IllegalStateException("The analysis summary topic has not been subscribed to.");
        }
        analysisSummaryHandler.addListener(Objects.requireNonNull(listener));
    }

    @Override
    protected void processMessage(@Nonnull final ActionPlan actions,
                                  @Nonnull final SpanContext tracingContext)
            throws ApiClientException, InterruptedException {
        for (final ActionsListener listener : actionsListenersSet) {
            listener.onActionsReceived(actions, tracingContext);
        }
    }

    /**
     * Process a chunk of a projected topology stream.
     *
     * @param topology the chunk of ProjectedTopology to process
     * @param commitCommand a Runnable command to be processed when the whole stream has been processed
     * @param tracingContext Distributed tracing context
     */
    private void processProjectedTopology(@Nonnull final ProjectedTopology topology,
                                          @Nonnull Runnable commitCommand,
                                          @Nonnull final SpanContext tracingContext) {
        final long topologyId = topology.getTopologyId();
        switch (topology.getSegmentCase()) {
            case METADATA:
                final Metadata start = topology.getMetadata();
                projectedTopologyChunkReceiver.startTopologyBroadcast(topologyId,
                        createProjectedTopologyChunkConsumers(start, tracingContext));
                break;
            case DATA:
                projectedTopologyChunkReceiver.processData(topologyId,
                        topology.getData().getEntitiesList());
                break;
            case END:
                projectedTopologyChunkReceiver.finishTopologyBroadcast(topologyId,
                        topology.getEnd().getTotalCount());
                commitCommand.run();
                break;
            default:
                getLogger().warn("Unknown broadcast data segment received: {}",
                        topology.getSegmentCase());
        }
    }

    /**
     * Process a chunk of a projected entity costs.
     *
     * @param projectedCostsSegment the chunk of {@link EntityCost} to process
     * @param commitCommand a Runnable command to be processed when the whole stream has been processed
     * @param tracingContext Distributed tracing context.
     */
    private void processProjectedEntityCosts(@Nonnull final ProjectedEntityCosts projectedCostsSegment,
                                             @Nonnull final Runnable commitCommand,
                                             @Nonnull final SpanContext tracingContext) {
        final long topologyId = projectedCostsSegment.getProjectedTopologyId();
        switch (projectedCostsSegment.getSegmentCase()) {
            case START:
                final ProjectedEntityCosts.Start start = projectedCostsSegment.getStart();
                projectedEntityCostChunkReceiver.startTopologyBroadcast(projectedCostsSegment.getProjectedTopologyId(),
                        createProjectedEntityCostsChunkConsumers(topologyId, start.getSourceTopologyInfo()));
                break;
            case DATA:
                projectedEntityCostChunkReceiver.processData(topologyId,
                        projectedCostsSegment.getData().getEntityCostsList());
                break;
            case END:
                projectedEntityCostChunkReceiver.finishTopologyBroadcast(topologyId,
                        projectedCostsSegment.getEnd().getTotalCount());
                commitCommand.run();
                break;
            default:
                getLogger().warn("Unknown broadcast data segment received: {}",
                        projectedCostsSegment.getSegmentCase());
        }
    }

    /**
     * Process a chunk of a projected entity reserved instance coverage.
     *
     * @param projectedCoverageSegment
     *            the chunk of {@link EntityReservedInstanceCoverage} to process
     * @param commitCommand
     *            a Runnable command to be processed when the whole stream has been
     *            processed
     * @param tracingContext Distributed tracing context
     */
    private void processProjectedEntityRiCoverage(
                    @Nonnull final ProjectedEntityReservedInstanceCoverage projectedCoverageSegment,
                    @Nonnull final Runnable commitCommand,
                    @Nonnull final SpanContext tracingContext) {
        final long topologyId = projectedCoverageSegment.getProjectedTopologyId();
        switch (projectedCoverageSegment.getSegmentCase()) {
            case START:
                final ProjectedEntityReservedInstanceCoverage.Start start =
                                projectedCoverageSegment.getStart();
                projectedEntityRiCoverageChunkReceiver.startTopologyBroadcast(
                                projectedCoverageSegment.getProjectedTopologyId(),
                                createProjectedEntityRiCoverageChunkConsumers(topologyId,
                                                start.getSourceTopologyInfo()));
                break;
            case DATA:
                projectedEntityRiCoverageChunkReceiver.processData(topologyId,
                                projectedCoverageSegment.getData().getProjectedRisCoverageList());
                break;
            case END:
                projectedEntityRiCoverageChunkReceiver.finishTopologyBroadcast(topologyId,
                                projectedCoverageSegment.getEnd().getTotalCount());
                commitCommand.run();
                break;
            default:
                getLogger().warn("Unknown broadcast data segment received: {}",
                                projectedCoverageSegment.getSegmentCase());
        }
    }

    private Collection<Consumer<RemoteIterator<ProjectedTopologyEntity>>> createProjectedTopologyChunkConsumers(
            final Metadata metadata, final SpanContext tracingContext) {
        return projectedTopologyListenersSet.stream().map(listener -> {
            final Consumer<RemoteIterator<ProjectedTopologyEntity>> consumer =
                    iterator -> listener.onProjectedTopologyReceived(metadata, iterator, tracingContext);
            return consumer;
        }).collect(Collectors.toList());
    }

    private Collection<Consumer<RemoteIterator<EntityCost>>> createProjectedEntityCostsChunkConsumers(
            final long topologyId, final TopologyInfo topologyInfo) {
        return projectedEntityCostsListenersSet.stream().map(listener -> {
            final Consumer<RemoteIterator<EntityCost>> consumer = iterator ->
                    listener.onProjectedEntityCostsReceived(topologyId, topologyInfo, iterator);
            return consumer;
        }).collect(Collectors.toList());
    }

    private Collection<Consumer<RemoteIterator<EntityReservedInstanceCoverage>>>
                    createProjectedEntityRiCoverageChunkConsumers(final long topologyId,
                                    final TopologyInfo topologyInfo) {
        return projectedEntityRiCoverageListenersSet.stream().map(listener -> {
            final Consumer<RemoteIterator<EntityReservedInstanceCoverage>> consumer =
                            iterator -> listener.onProjectedEntityRiCoverageReceived(topologyId,
                                            topologyInfo, iterator);
            return consumer;
        }).collect(Collectors.toList());
    }
}
