package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.identity.IdentityService;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;

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

    private final LicenseCheckClient licenseCheckClient;

    private final AcceptedActionsDAO acceptedActionsStore;

    private final IdentityService<ActionInfo> actionIdentityService;

    /**
     * Create a new ActionStoreFactory.
     *
     * @param actionFactory the action factory
     * @param realtimeTopologyContextId the topology context id
     * @param actionIdentityService identity service to assign OIDs to actions
     * @param databaseDslContext dsl context
     * @param actionHistoryDao dao layer working with executed actions
     * @param actionTargetSelector selects which target/probe to execute each action against
     * @param probeCapabilityCache gets the target-specific action capabilities
     * @param entitySettingsCache an entity snapshot factory used for creating entity snapshot
     * @param actionsStatistician works with action stats
     * @param actionTranslator the action translator class
     * @param clock the {@link Clock}
     * @param userSessionContext the user session context
     * @param acceptedActionsDAO dao layer working with accepted actions
     */
    public ActionStoreFactory(@Nonnull final IActionFactory actionFactory,
            final long realtimeTopologyContextId,
            @Nonnull final DSLContext databaseDslContext,
            @Nonnull final ActionHistoryDao actionHistoryDao,
            @Nonnull final ActionTargetSelector actionTargetSelector,
            @Nonnull final ProbeCapabilityCache probeCapabilityCache,
            @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache,
            @Nonnull final LiveActionsStatistician actionsStatistician,
            @Nonnull final ActionTranslator actionTranslator, @Nonnull final Clock clock,
            @Nonnull final UserSessionContext userSessionContext,
            @Nonnull final AcceptedActionsDAO acceptedActionsDAO,
            @Nonnull final LicenseCheckClient licenseCheckClient,
            @Nonnull final IdentityService<ActionInfo> actionIdentityService) {
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
        this.licenseCheckClient = Objects.requireNonNull(licenseCheckClient);
        this.acceptedActionsStore = Objects.requireNonNull(acceptedActionsDAO);
        this.actionIdentityService = Objects.requireNonNull(actionIdentityService);
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
            return new LiveActionStore(actionFactory, topologyContextId, actionTargetSelector,
                    probeCapabilityCache, entitySettingsCache, actionHistoryDao,
                    actionsStatistician, actionTranslator, clock, userSessionContext,
                    licenseCheckClient, acceptedActionsStore, actionIdentityService);
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
