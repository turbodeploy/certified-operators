package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.identity.IdentityService;

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

    private final AtomicActionFactory atomicActionFactory;

    private final Clock clock;

    private final UserSessionContext userSessionContext;

    private final SupplyChainServiceBlockingStub supplyChainService;
    private final RepositoryServiceBlockingStub repositoryService;

    private final InvolvedEntitiesExpander involvedEntitiesExpander;

    private final LicenseCheckClient licenseCheckClient;

    private final AcceptedActionsDAO acceptedActionsStore;
    private final RejectedActionsDAO rejectedActionsStore;

    private final IdentityService<ActionInfo> actionIdentityService;
    private final ActionAuditSender externalAuditEventSender;
    private final ActionTopologyStore actionTopologyStore;

    private final boolean riskPropagationEnabled;

    /**
     * To create a new ActionStoreFactory, use the {@link #newBuilder()}.
     *
     * @param builder the builder with all consutrction parameters filled in.
     */
    private ActionStoreFactory(@Nonnull Builder builder) {
        this.actionFactory = Objects.requireNonNull(builder.actionFactory);
        this.realtimeTopologyContextId = builder.realtimeTopologyContextId;
        this.databaseDslContext = Objects.requireNonNull(builder.databaseDslContext);
        this.actionHistoryDao = builder.actionHistoryDao;
        actionTopologyStore = builder.actionTopologyStore;
        this.actionTargetSelector = builder.actionTargetSelector;
        this.entitySettingsCache = Objects.requireNonNull(builder.entitySettingsCache);
        this.actionsStatistician = Objects.requireNonNull(builder.actionsStatistician);
        this.probeCapabilityCache = Objects.requireNonNull(builder.probeCapabilityCache);
        this.actionTranslator = Objects.requireNonNull(builder.actionTranslator);
        this.atomicActionFactory = Objects.requireNonNull(builder.atomicActionFactory);
        this.clock = Objects.requireNonNull(builder.clock);
        this.userSessionContext = Objects.requireNonNull(builder.userSessionContext);
        this.supplyChainService = Objects.requireNonNull(builder.supplyChainService);
        this.repositoryService = Objects.requireNonNull(builder.repositoryService);
        this.licenseCheckClient = Objects.requireNonNull(builder.licenseCheckClient);
        this.acceptedActionsStore = Objects.requireNonNull(builder.acceptedActionsDAO);
        this.rejectedActionsStore = Objects.requireNonNull(builder.rejectedActionsDAO);
        this.actionIdentityService = Objects.requireNonNull(builder.actionIdentityService);
        this.involvedEntitiesExpander = Objects.requireNonNull(builder.involvedEntitiesExpander);
        this.externalAuditEventSender = Objects.requireNonNull(builder.actionAuditSender);
        this.riskPropagationEnabled =  builder.riskPropagationEnabled;
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
            return new LiveActionStore(actionFactory, topologyContextId, actionTopologyStore,
                    actionTargetSelector, probeCapabilityCache, entitySettingsCache,
                    actionHistoryDao, actionsStatistician, actionTranslator, atomicActionFactory,
                    clock, userSessionContext, licenseCheckClient, acceptedActionsStore,
                    rejectedActionsStore, actionIdentityService, involvedEntitiesExpander,
                    externalAuditEventSender, riskPropagationEnabled);
        } else {
            return new PlanActionStore(actionFactory, databaseDslContext, topologyContextId,
                entitySettingsCache, actionTranslator, realtimeTopologyContextId, actionTargetSelector,
                    licenseCheckClient);
        }
    }

    @Nonnull
    @Override
    public String getContextTypeName(long topologyContextId) {
        return (topologyContextId == realtimeTopologyContextId) ? LIVE_CONTEXT_TYPE_NAME : PLAN_CONTEXT_TYPE_NAME;
    }

    /**
     * Creates a builder for construction.
     * @return the builder for construction.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to simplify object construction. Construction requires a lot of parameters so it's
     * better to use a builder.
     */
    public static final class Builder {

        private IActionFactory actionFactory;
        private long realtimeTopologyContextId;
        private DSLContext databaseDslContext;
        private ActionHistoryDao actionHistoryDao;
        private ActionTargetSelector actionTargetSelector;
        private EntitiesAndSettingsSnapshotFactory entitySettingsCache;
        private ProbeCapabilityCache probeCapabilityCache;
        private LiveActionsStatistician actionsStatistician;
        private ActionTranslator actionTranslator;
        private AtomicActionFactory atomicActionFactory;
        private Clock clock;
        private UserSessionContext userSessionContext;
        private SupplyChainServiceBlockingStub supplyChainService;
        private RepositoryServiceBlockingStub repositoryService;
        private ActionTopologyStore actionTopologyStore;
        private LicenseCheckClient licenseCheckClient;
        private AcceptedActionsDAO acceptedActionsDAO;
        private RejectedActionsDAO rejectedActionsDAO;
        private IdentityService<ActionInfo> actionIdentityService;
        private InvolvedEntitiesExpander involvedEntitiesExpander;
        private ActionAuditSender actionAuditSender;
        private boolean riskPropagationEnabled;

        private Builder() {
        }

        /**
         * Sets the actionFactory on this builder.
         *
         * @param actionFactory the actionFactory.
         * @return the same builder with the actionFactory set.
         */
        public Builder withActionFactory(@Nonnull IActionFactory actionFactory) {
            this.actionFactory = actionFactory;
            return this;
        }

        /**
         * Set the {@link ActionTopologyStore}.
         *
         * @param actionTopologyStore The {@link ActionTopologyStore}.
         * @return The builder for method chaining.
         */
        public Builder withTopologyStore(@Nonnull ActionTopologyStore actionTopologyStore) {
            this.actionTopologyStore = actionTopologyStore;
            return this;
        }

        /**
         * Sets the realtimeTopologyContextId on this builder.
         *
         * @param realtimeTopologyContextId the realtimeTopologyContextId.
         * @return the same builder with the realtimeTopologyContextId set.
         */
        public Builder withRealtimeTopologyContextId(long realtimeTopologyContextId) {
            this.realtimeTopologyContextId = realtimeTopologyContextId;
            return this;
        }

        /**
         * Sets the databaseDslContext on this builder.
         *
         * @param databaseDslContext the databaseDslContext.
         * @return the same builder with the databaseDslContext set.
         */
        public Builder withDatabaseDslContext(@Nonnull DSLContext databaseDslContext) {
            this.databaseDslContext = databaseDslContext;
            return this;
        }

        /**
         * Sets the actionHistoryDao on this builder.
         *
         * @param actionHistoryDao the actionHistoryDao.
         * @return the same builder with the actionHistoryDao set.
         */
        public Builder withActionHistoryDao(@Nonnull ActionHistoryDao actionHistoryDao) {
            this.actionHistoryDao = actionHistoryDao;
            return this;
        }

        /**
         * Sets the actionTargetSelector on this builder.
         *
         * @param actionTargetSelector the actionTargetSelector.
         * @return the same builder with the actionTargetSelector set.
         */
        public Builder withActionTargetSelector(@Nonnull ActionTargetSelector actionTargetSelector) {
            this.actionTargetSelector = actionTargetSelector;
            return this;
        }

        /**
         * Sets the entitySettingsCache on this builder.
         *
         * @param entitySettingsCache the entitySettingsCache.
         * @return the same builder with the entitySettingsCache set.
         */
        public Builder withEntitySettingsCache(@Nonnull EntitiesAndSettingsSnapshotFactory entitySettingsCache) {
            this.entitySettingsCache = entitySettingsCache;
            return this;
        }

        /**
         * Sets the probeCapabilityCache on this builder.
         *
         * @param probeCapabilityCache the probeCapabilityCache.
         * @return the same builder with the probeCapabilityCache set.
         */
        public Builder withProbeCapabilityCache(@Nonnull ProbeCapabilityCache probeCapabilityCache) {
            this.probeCapabilityCache = probeCapabilityCache;
            return this;
        }

        /**
         * Sets the actionsStatistician on this builder.
         *
         * @param actionsStatistician the actionsStatistician.
         * @return the same builder with the actionsStatistician set.
         */
        public Builder withActionsStatistician(@Nonnull LiveActionsStatistician actionsStatistician) {
            this.actionsStatistician = actionsStatistician;
            return this;
        }

        /**
         * Sets the actionTranslator on this builder.
         *
         * @param actionTranslator the actionTranslator.
         * @return the same builder with the actionTranslator set.
         */
        public Builder withActionTranslator(@Nonnull ActionTranslator actionTranslator) {
            this.actionTranslator = actionTranslator;
            return this;
        }

        /**
         * Sets the atomicActionFactory on this builder.
         *
         * @param atomicActionFactory the atomicActionFactory
         * @return the same builder with the atomicActionFactory set.
         */
        public Builder withAtomicActionFactory(@Nonnull AtomicActionFactory atomicActionFactory) {
            this.atomicActionFactory = atomicActionFactory;
            return this;
        }

        /**
         * Sets the clock on this builder.
         *
         * @param clock the clock.
         * @return the same builder with the clock set.
         */
        public Builder withClock(@Nonnull Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Sets the userSessionContext on this builder.
         *
         * @param userSessionContext the userSessionContext.
         * @return the same builder with the userSessionContext set.
         */
        public Builder withUserSessionContext(@Nonnull UserSessionContext userSessionContext) {
            this.userSessionContext = userSessionContext;
            return this;
        }

        /**
         * Sets the supplyChainService on this builder.
         *
         * @param supplyChainService the supplyChainService.
         * @return the same builder with the supplyChainService set.
         */
        public Builder withSupplyChainService(@Nonnull SupplyChainServiceBlockingStub supplyChainService) {
            this.supplyChainService = supplyChainService;
            return this;
        }

        /**
         * Sets the repositoryService on this builder.
         *
         * @param repositoryService the repositoryService.
         * @return the same builder with the repositoryService set.
         */
        public Builder withRepositoryService(@Nonnull RepositoryServiceBlockingStub repositoryService) {
            this.repositoryService = repositoryService;
            return this;
        }

        /**
         * Sets the licenseCheckClient on this builder.
         *
         * @param licenseCheckClient the licenseCheckClient.
         * @return the same builder with the licenseCheckClient set.
         */
        public Builder withLicenseCheckClient(@Nonnull LicenseCheckClient licenseCheckClient) {
            this.licenseCheckClient = licenseCheckClient;
            return this;
        }

        /**
         * Sets accepted actions store to a builder.
         *
         * @param acceptedActionsStore accepted actions store to set
         * @return the builder itself for chained calls
         */
        public Builder withAcceptedActionsStore(
                @Nonnull AcceptedActionsDAO acceptedActionsStore) {
            this.acceptedActionsDAO = acceptedActionsStore;
            return this;
        }

        /**
         * Sets rejected actions store to a builder.
         *
         * @param rejectedActionsStore rejected actions store to set
         * @return the builder itself for chained calls
         */
        public Builder withRejectedActionsStore(@Nonnull RejectedActionsDAO rejectedActionsStore) {
            this.rejectedActionsDAO = rejectedActionsStore;
            return this;
        }

        /**
         * Sets the actionIdentityService on this builder.
         *
         * @param actionIdentityService the actionIdentityService.
         * @return the same builder with the actionIdentityService set.
         */
        public Builder withActionIdentityService(@Nonnull IdentityService<ActionInfo> actionIdentityService) {
            this.actionIdentityService = actionIdentityService;
            return this;
        }

        /**
         * Sets the involvedEntitiesExpander on this builder.
         *
         * @param involvedEntitiesExpander the involvedEntitiesExpander.
         * @return the same builder with the involvedEntitiesExpander set.
         */
        public Builder withInvolvedEntitiesExpander(@Nonnull InvolvedEntitiesExpander involvedEntitiesExpander) {
            this.involvedEntitiesExpander = involvedEntitiesExpander;
            return this;
        }

        /**
         * Sets action audit sender.
         *
         * @param actionAuditSender action audit sender to sent actions' on-generation events
         * @return the builder for chained calls
         */
        public Builder withActionAuditSender(@Nonnull ActionAuditSender actionAuditSender) {
            this.actionAuditSender = actionAuditSender;
            return this;
        }

        /**
         * Sets risk propagation feature flag.
         * @param riskPropagationEnabled risk propagation feature flag
         * @return the builder for chained calls
         */
        public Builder withRiskPropagationEnabledFlag(boolean riskPropagationEnabled) {
            this.riskPropagationEnabled = riskPropagationEnabled;
            return this;
        }

        /**
         * Constructs the ActionStoreFactory using all the accumulated values.
         *
         * @return the constructed ActionStoreFactory.
         */
        public ActionStoreFactory build() {
            ActionStoreFactory actionStoreFactory = new ActionStoreFactory(this);
            return actionStoreFactory;
        }
    }
}
