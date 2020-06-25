package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorDBConfig;
import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.AcceptedActionsStore;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionHistoryDaoImpl;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.approval.ApprovalCommunicationConfig;
import com.vmturbo.action.orchestrator.audit.AuditCommunicationConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.stats.ActionStatsConfig;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModel;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModelCreator;
import com.vmturbo.action.orchestrator.store.identity.IdentityServiceImpl;
import com.vmturbo.action.orchestrator.store.identity.RecommendationIdentityStore;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.topology.TpEntitiesWithNewStateListener;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Configuration for the ActionStore package.
 */
@Configuration
@Import({ActionOrchestratorDBConfig.class,
    ActionOrchestratorGlobalConfig.class,
    ActionExecutionConfig.class,
    GroupClientConfig.class,
    RepositoryClientConfig.class,
    ActionStatsConfig.class,
    ActionTranslationConfig.class,
    PlanOrchestratorClientConfig.class,
    TopologyProcessorConfig.class,
    UserSessionConfig.class,
    LicenseCheckClientConfig.class,
    ApprovalCommunicationConfig.class,
    AuditCommunicationConfig.class})
public class ActionStoreConfig {

    @Autowired
    private ActionOrchestratorDBConfig databaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private ActionExecutionConfig actionExecutionConfig;

    @Autowired
    private ActionStatsConfig actionStatsConfig;

    @Autowired
    private ActionTranslationConfig actionTranslationConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Autowired
    private ApprovalCommunicationConfig approvalCommunicationConfig;

    @Autowired
    private AuditCommunicationConfig auditCommunicationConfig;

    @Value("${entityTypeRetryIntervalMillis}")
    private long entityTypeRetryIntervalMillis;

    @Value("${entityTypeMaxRetries}")
    private long entityTypeMaxRetries;

    @Value("${minsToWaitForTopology:60}")
    private long minsToWaitForTopology;

    @Value("${actionExecution.concurrentAutomatedActions:5}")
    private int concurrentAutomatedActions;

    @Value("${minsActionAcceptanceTTL:1440}")
    private long minsActionAcceptanceTTL;

    @Value("${minsFrequencyOfCleaningAcceptedActionsStore:60}")
    private long minsFrequencyOfCleaningAcceptedActionsStore;

    @Value("${actionIdentityModelRequestChunkSize:1000}")
    private int actionIdentityModelRequestChunkSize;

    @Bean
    public IActionFactory actionFactory() {
        return new ActionFactory(actionModeCalculator());
    }

    @Bean
    public EntitiesAndSettingsSnapshotFactory entitySettingsCache() {
        return new EntitiesAndSettingsSnapshotFactory(
            groupClientConfig.groupChannel(),
            repositoryClientConfig.repositoryChannel(),
            tpConfig.realtimeTopologyContextId(),
            repositoryClientConfig.topologyAvailabilityTracker(),
            minsToWaitForTopology,
            TimeUnit.MINUTES, acceptedActionsStore());
    }

    @Bean
    public ActionModeCalculator actionModeCalculator() {
        return new ActionModeCalculator();
    }

    @Bean
    public ExecutorService automatedActionThreadpool() {
        return Executors.newFixedThreadPool(concurrentAutomatedActions);
    }

    @Bean
    public AutomatedActionExecutor automatedActionExecutor() {
        return new AutomatedActionExecutor(actionExecutionConfig.actionExecutor(),
                automatedActionThreadpool(), workflowConfig.workflowStore(),
                actionExecutionConfig.actionTargetSelector(), entitySettingsCache(),
                ScheduleServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    /**
     * Identity store for market recommendations.
     *
     * @return identity store
     */
    @Bean
    public RecommendationIdentityStore recommendationIdentityStore() {
        return new RecommendationIdentityStore(databaseConfig.dsl(),
                actionIdentityModelRequestChunkSize);
    }

    /**
     * Identity service for market recommendations.
     *
     * @return identity service
     */
    @Bean
    public IdentityServiceImpl<ActionInfo, ActionInfoModel> actionIdentityService() {
        return new IdentityServiceImpl<>(recommendationIdentityStore(),
                new ActionInfoModelCreator(), Clock.systemUTC(), 24 * 3600 * 1000);
    }

    /**
     * Returns the {@link ActionStoreFactory} bean.
     *
     * @return the {@link ActionStoreFactory} bean.
     */
    @Bean
    public IActionStoreFactory actionStoreFactory() {
        return ActionStoreFactory.newBuilder()
            .withActionFactory(actionFactory())
            .withRealtimeTopologyContextId(tpConfig.realtimeTopologyContextId())
            .withDatabaseDslContext(databaseConfig.dsl())
            .withActionHistoryDao(actionHistory())
            .withActionTargetSelector(actionExecutionConfig.actionTargetSelector())
            .withProbeCapabilityCache(actionExecutionConfig.targetCapabilityCache())
            .withEntitySettingsCache(entitySettingsCache())
            .withActionsStatistician(actionStatsConfig.actionsStatistician())
            .withActionTranslator(actionTranslationConfig.actionTranslator())
            .withClock(actionOrchestratorGlobalConfig.actionOrchestratorClock())
            .withUserSessionContext(userSessionConfig.userSessionContext())
            .withSupplyChainService(SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()))
            .withRepositoryService(RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()))
            .withLicenseCheckClient(licenseCheckClientConfig.licenseCheckClient())
            .withAcceptedActionStore(acceptedActionsStore())
            .withActionIdentityService(actionIdentityService())
            .withInvolvedEntitiesExpander(actionStatsConfig.involvedEntitiesExpander())
            .withActionAuditSender(auditCommunicationConfig.actionAuditSender())
            .build();
    }

    /**
     * Creates instance of {@link RegularAcceptedActionsStoreCleaner} which has internal logic
     * of regularly checking accepted actions and deleting expired acceptances.
     *
     * @return instance of {@link RegularAcceptedActionsStoreCleaner}.
     */
    @Bean
    public RegularAcceptedActionsStoreCleaner regularAcceptedActionsStoreCleaner() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("acceptedActions-cleaner-%d").build();
        return new RegularAcceptedActionsStoreCleaner(
                Executors.newSingleThreadScheduledExecutor(threadFactory), acceptedActionsStore(),
                minsActionAcceptanceTTL, minsFrequencyOfCleaningAcceptedActionsStore);
    }

    @Bean
    public IActionStoreLoader actionStoreLoader() {
        // For now, only plan action stores (kept in PersistentImmutableActionStores)
        // need to be re-loaded at startup.
        return new PlanActionStore.StoreLoader(databaseConfig.dsl(),
            actionFactory(),
            actionModeCalculator(),
            entitySettingsCache(),
            actionTranslationConfig.actionTranslator(),
            tpConfig.realtimeTopologyContextId(),
            SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()),
            RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()),
            actionExecutionConfig.actionTargetSelector(),
            licenseCheckClientConfig.licenseCheckClient());
    }

    @Bean
    public ActionStorehouse actionStorehouse() {
        ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory(),
                automatedActionExecutor(),
                actionStoreLoader(),
                approvalCommunicationConfig.approvalRequester());
        tpConfig.topologyProcessor()
            .addEntitiesWithNewStatesListener(new TpEntitiesWithNewStateListener(actionStorehouse,
                tpConfig.realtimeTopologyContextId()));
        return actionStorehouse;
    }

    /**
     * Cleans up stale plan data in the action orchestrator.
     *
     * @return The {@link PlanGarbageDetector}.
     */
    @Bean
    public PlanGarbageDetector actionPlanGarbageDetector() {
        ActionPlanGarbageCollector collector = new ActionPlanGarbageCollector(actionStorehouse());
        return planOrchestratorClientConfig.newPlanGarbageDetector(collector);
    }

    @Bean
    public ActionHistoryDao actionHistory() {
        return new ActionHistoryDaoImpl(databaseConfig.dsl(), actionModeCalculator());
    }

    /**
     * Creates DAO implementation for working with accepted actions.
     *
     * @return instance of {@link AcceptedActionsDAO}
     */
    @Bean
    public AcceptedActionsDAO acceptedActionsStore() {
        return new AcceptedActionsStore(databaseConfig.dsl());
    }
}
