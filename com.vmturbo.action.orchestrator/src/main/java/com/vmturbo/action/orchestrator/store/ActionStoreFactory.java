package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;

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

    private final ActionHistoryDao actionHistoryDao;

    private final ActionTargetSelector actionTargetSelector;

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    private final ProbeCapabilityCache probeCapabilityCache;

    private static final String PLAN_CONTEXT_TYPE_NAME = "plan";
    private static final String LIVE_CONTEXT_TYPE_NAME = "live";

    private final LiveActionsStatistician actionsStatistician;

    private final ActionTranslator actionTranslator;

    private final Clock clock;

    private final UserSessionContext userSessionContext;

    /**
     * Create a new ActionStoreFactory.
     */
    public ActionStoreFactory(@Nonnull final IActionFactory actionFactory,
                              final long realtimeTopologyContextId,
                              @Nonnull final DSLContext databaseDslContext,
                              @Nonnull final ActionHistoryDao actionHistoryDao,
                              @Nonnull final ActionTargetSelector actionTargetSelector,
                              @Nonnull final ProbeCapabilityCache probeCapabilityCache,
                              @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache,
                              @Nonnull final LiveActionsStatistician actionsStatistician,
                              @Nonnull final ActionTranslator actionTranslator,
                              @Nonnull final Clock clock,
                              @Nonnull final UserSessionContext userSessionContext) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.databaseDslContext = Objects.requireNonNull(databaseDslContext);
        this.actionHistoryDao = actionHistoryDao;
        this.actionTargetSelector = actionTargetSelector;
        this.entitySettingsCache = Objects.requireNonNull(entitySettingsCache);
        this.actionsStatistician = Objects.requireNonNull(actionsStatistician);
        this.probeCapabilityCache = Objects.requireNonNull(probeCapabilityCache);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.clock = Objects.requireNonNull(clock);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
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
            return new LiveActionStore(actionFactory, topologyContextId,
                actionTargetSelector, probeCapabilityCache, entitySettingsCache, actionHistoryDao,
                actionsStatistician, actionTranslator, clock, userSessionContext);
        } else {
            return new PlanActionStore(actionFactory, databaseDslContext, topologyContextId,
                entitySettingsCache, actionTranslator, realtimeTopologyContextId, actionTargetSelector);
        }
    }

    @Nonnull
    @Override
    public String getContextTypeName(long topologyContextId) {
        return (topologyContextId == realtimeTopologyContextId) ? LIVE_CONTEXT_TYPE_NAME : PLAN_CONTEXT_TYPE_NAME;
    }
}
