package com.vmturbo.action.orchestrator.store;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.action.ActionHistoryDaoImpl;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.EntityAndActionTypeBasedEntitySelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetByProbeCategoryResolver;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceBlockingStub;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * A factory for creating {@link ActionStore}s.
 * Creates an {@link LiveActionStore} (permits action mutability, kept in-memory)
 * for real-time topology contexts and creates {@link PlanActionStore}s
 * for plan topology contexts (any mutations to its actions by clients are lost, kept on-disk).
 */
public class ActionStoreFactory implements IActionStoreFactory {

    private final IActionFactory actionFactory;
    private final long realtimeTopologyContextId;
    /**
     * Required by {@link PlanActionStore}s to interact with the database.
     */
    private final DSLContext databaseDslContext;

    private final Channel topologyProcessorChannel;

    private final TopologyProcessor topologyProcessor;

    /**
     * For fetching entity type data from the repository in ActionExecutor
     */
    private final RepositoryClient repositoryClient;

    private final EntitySettingsCache entitySettingsCache;

    private static final String PLAN_CONTEXT_TYPE_NAME = "plan";
    private static final String LIVE_CONTEXT_TYPE_NAME = "live";

    private final ActionCapabilitiesStore actionCapabilitiesStore;

    /**
     * Create a new ActionStoreFactory.
     *  @param actionFactory The actionFactory to be passed to all store instances created
     *                      by the {@link this}.
     * @param realtimeTopologyContextId The context ID for the live topology context.
     * @param databaseDslContext The DSL context for use when interacting with the database.
     * @param topologyProcessorChannel Grpc Channel for topology processor.
     * @param topologyProcessor Topology processor client.
     * @param entitySettingsCache cache of entity settings.
     * @param repositoryClient
     */
    public ActionStoreFactory(@Nonnull final IActionFactory actionFactory,
                              final long realtimeTopologyContextId,
                              @Nonnull final DSLContext databaseDslContext,
                              @Nonnull final Channel topologyProcessorChannel,
                              @Nonnull final TopologyProcessor topologyProcessor,
                              @Nonnull final EntitySettingsCache entitySettingsCache,
                              @Nonnull final RepositoryClient repositoryClient) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.databaseDslContext = Objects.requireNonNull(databaseDslContext);
        this.topologyProcessorChannel = Objects.requireNonNull(topologyProcessorChannel);
        final ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesService =
                ProbeActionCapabilitiesServiceGrpc.newBlockingStub(topologyProcessorChannel);
        this.actionCapabilitiesStore = new ProbeActionCapabilitiesStore(actionCapabilitiesService);
        this.topologyProcessor = Objects.requireNonNull(topologyProcessor);
        this.entitySettingsCache = Objects.requireNonNull(entitySettingsCache);
        this.repositoryClient = repositoryClient;
    }

    /**
     * Creates an {@link LiveActionStore} for a real-time topology context
     * and an {@link PlanActionStore} otherwise.
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public ActionStore newStore(final long topologyContextId) {
        if (topologyContextId == realtimeTopologyContextId) {
            final ActionTargetSelector actionTargetSelector = new ActionTargetSelector(
                    new ActionTargetByProbeCategoryResolver(topologyProcessor, actionCapabilitiesStore),
                    new EntityAndActionTypeBasedEntitySelector(),
                    topologyProcessorChannel,
                    repositoryClient,
                    topologyContextId);
            return new LiveActionStore(actionFactory, topologyContextId,
                    new ActionSupportResolver(actionCapabilitiesStore, actionTargetSelector), entitySettingsCache,
                    new ActionHistoryDaoImpl(databaseDslContext));
        } else {
            return new PlanActionStore(actionFactory, databaseDslContext, topologyContextId);
        }
    }

    @Nonnull
    @Override
    public String getContextTypeName(long topologyContextId) {
        return (topologyContextId == realtimeTopologyContextId) ? LIVE_CONTEXT_TYPE_NAME : PLAN_CONTEXT_TYPE_NAME;
    }
}
