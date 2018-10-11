package com.vmturbo.market.component.api.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityCosts;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.ChunkingReceiver;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.PlanAnalysisTopologyListener;
import com.vmturbo.market.component.api.ProjectedEntityCostsListener;
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
     * Projected topologies topic. Should be synchronized with kafka-config.yml
     */
    public static final String PLAN_ANALYSIS_TOPOLOGIES_TOPIC = "plan-analysis-topologies";

    /**
     * Action plans topic. Should be synchronized with kafka-config.yml
     */
    public static final String ACTION_PLANS_TOPIC = "action-plans";

    private final Set<ActionsListener> actionsListenersSet;

    private final Set<ProjectedTopologyListener> projectedTopologyListenersSet;
    private final Set<ProjectedEntityCostsListener> projectedEntityCostsListenersSet;
    private final Set<PlanAnalysisTopologyListener> planAnalysisTopologyListenersSet;
    private final ChunkingReceiver<TopologyEntityDTO> planAnalysisTopologyChunkReceiver;
    private final ChunkingReceiver<ProjectedTopologyEntity> projectedTopologyChunkReceiver;
    private final ChunkingReceiver<EntityCost> projectedEntityCostChunkReceiver;

    public MarketComponentNotificationReceiver(
            @Nullable final IMessageReceiver<ProjectedTopology> projectedTopologyReceiver,
            @Nullable final IMessageReceiver<ProjectedEntityCosts> projectedEntityCostsReceiver,
            @Nullable final IMessageReceiver<ActionPlan> actionPlanReceiver,
            @Nullable final IMessageReceiver<Topology> planAnalysisTopologyReceiver,
            @Nonnull final ExecutorService executorService) {
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

        if (actionPlanReceiver != null) {
            actionsListenersSet = Sets.newConcurrentHashSet();
        } else {
            actionsListenersSet = Collections.emptySet();
        }

        planAnalysisTopologyChunkReceiver = new ChunkingReceiver<>(executorService);
        projectedTopologyChunkReceiver = new ChunkingReceiver<>(executorService);
        projectedEntityCostChunkReceiver = new ChunkingReceiver<>(executorService);
        if (planAnalysisTopologyReceiver != null) {
            planAnalysisTopologyListenersSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
            planAnalysisTopologyReceiver.addListener(this::processPlanAnalysisTopology);
        } else {
            planAnalysisTopologyListenersSet = Collections.emptySet();
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
    public void addPlanAnalysisTopologyListener(@Nonnull final PlanAnalysisTopologyListener listener) {
        planAnalysisTopologyListenersSet.add(Objects.requireNonNull(listener));
    }

    @Override
    protected void processMessage(@Nonnull final ActionPlan actions)
            throws ApiClientException, InterruptedException {
        for (final ActionsListener listener : actionsListenersSet) {
            listener.onActionsReceived(actions);
        }
    }

    /**
     * Process a chunk of a projected topology stream.
     *
     * @param topology the chunk of ProjectedTopology to process
     * @param commitCommand a Runnable command to be processed when the whole stream has been processed
     */
    private void processProjectedTopology(@Nonnull final ProjectedTopology topology, @Nonnull Runnable commitCommand) {
        final long topologyId = topology.getTopologyId();
        switch (topology.getSegmentCase()) {
            case START:
                final Start start = topology.getStart();
                projectedTopologyChunkReceiver.startTopologyBroadcast(topology.getTopologyId(),
                        createProjectedTopologyChunkConsumers(topologyId, start.getSourceTopologyInfo(), Sets.newHashSet(start.getSkippedEntitiesList())));
                break;
            case DATA:
                projectedTopologyChunkReceiver.processData(topology.getTopologyId(),
                        topology.getData().getEntitiesList());
                break;
            case END:
                projectedTopologyChunkReceiver.finishTopologyBroadcast(topology.getTopologyId(),
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
     */
    private void processProjectedEntityCosts(@Nonnull final ProjectedEntityCosts projectedCostsSegment,
                                             @Nonnull final Runnable commitCommand) {
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
     * Process a chunk of a plan analysis topology.
     *
     * @param topology the chunk of plan analysis topology to process
     * @param commitCommand a Runnable command to be processed when the whole stream has been processed
     */
    private void processPlanAnalysisTopology(@Nonnull final Topology topology, @Nonnull Runnable commitCommand) {
        getLogger().info("Processing plan analysis topology {}", topology::getTopologyId);
        final long topologyId = topology.getTopologyId();
        switch (topology.getSegmentCase()) {
            case START:
                final Topology.Start start = topology.getStart();
                planAnalysisTopologyChunkReceiver.startTopologyBroadcast(topology.getTopologyId(),
                        createPlanAnalysisTopologyChunkConsumers(start.getTopologyInfo()));
                break;
            case DATA:
                planAnalysisTopologyChunkReceiver.processData(topology.getTopologyId(),
                        topology.getData().getEntitiesList());
                break;
            case END:
                planAnalysisTopologyChunkReceiver.finishTopologyBroadcast(topology.getTopologyId(),
                        topology.getEnd().getTotalCount());
                commitCommand.run();
                break;
            default:
                getLogger().warn("Unknown broadcast data segment received: {}",
                        topology.getSegmentCase());
        }
    }

    private Collection<Consumer<RemoteIterator<ProjectedTopologyEntity>>> createProjectedTopologyChunkConsumers(
            final long topologyId, final TopologyInfo topologyInfo, final Set<Long> skippedEntities) {
        return projectedTopologyListenersSet.stream().map(listener -> {
            final Consumer<RemoteIterator<ProjectedTopologyEntity>> consumer =
                    iterator -> listener.onProjectedTopologyReceived(
                            topologyId, topologyInfo, skippedEntities, iterator);
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

    private Collection<Consumer<RemoteIterator<TopologyEntityDTO>>> createPlanAnalysisTopologyChunkConsumers(
            final TopologyInfo topologyInfo) {
        return planAnalysisTopologyListenersSet.stream().map(listener -> {
            final Consumer<RemoteIterator<TopologyEntityDTO>> consumer =
                    iterator -> listener.onPlanAnalysisTopology(
                            topologyInfo, iterator);
            return consumer;
        }).collect(Collectors.toList());
    }
}
