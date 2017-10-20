package com.vmturbo.action.orchestrator.store;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceBlockingStub;

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

    /**
     * Required by {@link LiveActionStore}s to translate market actions to real-world actions.
     */
    private final ActionTranslator actionTranslator;

    public static final String PLAN_CONTEXT_TYPE_NAME = "plan";
    public static final String LIVE_CONTEXT_TYPE_NAME = "live";

    /**
     * Create a new ActionStoreFactory.
     *
     * @param actionFactory The actionFactory to be passed to all store instances created
     *                      by the {@link this}.
     * @param actionTranslator The translator for use when translating market actions to real-world actions.
     * @param realtimeTopologyContextId The context ID for the live topology context.
     * @param databaseDslContext The DSL context for use when interacting with the database.
     */
    public ActionStoreFactory(@Nonnull final IActionFactory actionFactory,
                              @Nonnull final ActionTranslator actionTranslator,
                              final long realtimeTopologyContextId,
                              @Nonnull final DSLContext databaseDslContext,
                              @Nonnull final Channel topologyProcessorChannel) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.databaseDslContext = Objects.requireNonNull(databaseDslContext);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.topologyProcessorChannel = topologyProcessorChannel;
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
            final ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesServiceBlockingStub =
                    ProbeActionCapabilitiesServiceGrpc.newBlockingStub(topologyProcessorChannel);
            final ActionExecutor actionExecutor = new ActionExecutor(topologyProcessorChannel);
            final SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub =
                    SettingPolicyServiceGrpc.newBlockingStub(topologyProcessorChannel);
            return new LiveActionStore(actionFactory, topologyContextId,
                    settingPolicyServiceBlockingStub,
                    new ActionSupportResolver(actionCapabilitiesServiceBlockingStub,
                            actionExecutor));
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
