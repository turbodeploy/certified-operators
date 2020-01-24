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
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;

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

    private final SupplyChainServiceBlockingStub supplyChainService;
    private final RepositoryServiceBlockingStub repositoryService;

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
        this.actionTargetSelector = builder.actionTargetSelector;
        this.entitySettingsCache = Objects.requireNonNull(builder.entitySettingsCache);
        this.actionsStatistician = Objects.requireNonNull(builder.actionsStatistician);
        this.probeCapabilityCache = Objects.requireNonNull(builder.probeCapabilityCache);
        this.actionTranslator = Objects.requireNonNull(builder.actionTranslator);
        this.clock = Objects.requireNonNull(builder.clock);
        this.userSessionContext = Objects.requireNonNull(builder.userSessionContext);
        this.supplyChainService = Objects.requireNonNull(builder.supplyChainService);
        this.repositoryService = Objects.requireNonNull(builder.repositoryService);
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
                supplyChainService, repositoryService,
                actionTargetSelector, probeCapabilityCache, entitySettingsCache, actionHistoryDao,
                actionsStatistician, actionTranslator, clock, userSessionContext);
        } else {
            return new PlanActionStore(actionFactory, databaseDslContext, topologyContextId,
                supplyChainService, repositoryService,
                entitySettingsCache, actionTranslator, realtimeTopologyContextId);
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
        private Clock clock;
        private UserSessionContext userSessionContext;
        private SupplyChainServiceBlockingStub supplyChainService;
        private RepositoryServiceBlockingStub repositoryService;

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
