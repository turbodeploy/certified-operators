package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
import com.vmturbo.action.orchestrator.action.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.action.RejectedActionsStore;
import com.vmturbo.action.orchestrator.approval.ApprovalCommunicationConfig;
import com.vmturbo.action.orchestrator.audit.AuditCommunicationConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.stats.ActionStatsConfig;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModel;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModelCreator;
import com.vmturbo.action.orchestrator.store.identity.IdentityServiceImpl;
import com.vmturbo.action.orchestrator.store.identity.RecommendationIdentityStore;
import com.vmturbo.action.orchestrator.topology.ActionTopologyListener;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.components.common.utils.RteLoggingRunnable;
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

    @Value("${entityTypeRetryIntervalMillis:1000}")
    private long entityTypeRetryIntervalMillis;

    @Value("${entityTypeMaxRetries:6}")
    private long entityTypeMaxRetries;

    @Value("${minsToWaitForTopology:60}")
    private long minsToWaitForTopology;

    @Value("${actionExecution.concurrentAutomatedActions:1000}")
    private int concurrentAutomatedActions;

    @Value("${minsActionAcceptanceTTL:1440}")
    private long minsActionAcceptanceTTL;

    @Value("${minsActionRejectionTTL:10080}")
    private long minsActionRejectionTTL;

    @Value("${minsFrequencyOfCleaningAcceptedActionsStore:60}")
    private long minsFrequencyOfCleaningAcceptedActionsStore;

    @Value("${actionIdentityModelRequestChunkSize:1000}")
    private int actionIdentityModelRequestChunkSize;

    @Value("${riskPropagationEnabled:true}")
    private boolean riskPropagationEnabled;

    @Value("${actionIdentityCachePurgeIntervalSec:1800}")
    private int identityCachePurgeIntervalSec;

    @Value("${queryTimeWindowForLastExecutedActionsMins:60}")
    private int queryTimeWindowForLastExecutedActionsMins;

    /**
     * If true, we will expect that the topology ID of the incoming live action plan strictly
     * matches the topology ID of the per-entity setting breakdown that the topology processor
     * uploads to the group component. This is slightly safer because we will know the market
     * generated the actions using the same settings. However, we may end up getting no settings
     * at all if the topology processor uploaded a newer version of the setting breakdown before the
     * action plan gets to the action orchestrator.
     */
    @Value("${actionSettingsStrictTopologyIdMatch:false}")
    private boolean actionSettingsStrictTopologyIdMatch;

    @Bean
    public IActionFactory actionFactory() {
        return new ActionFactory(actionModeCalculator());
    }

    /**
     * Class with the {@link AtomicActionSpec} received from the topology processor.
     *
     * @return {@link AtomicActionSpecsCache}
     */
    @Bean
    public AtomicActionSpecsCache actionMergeSpecsCache() {
        return new AtomicActionSpecsCache();
    }

    /**
     * Create atomic action factory.
     *
     * @return @link AtomicActionFactory}
     */
    @Bean
    public AtomicActionFactory atomicActionFactory() {
        return new AtomicActionFactory(actionMergeSpecsCache());
    }

    @Bean
    public EntitiesAndSettingsSnapshotFactory entitySettingsCache() {
        return new EntitiesAndSettingsSnapshotFactory(
            groupClientConfig.groupChannel(),
            repositoryClientConfig.repositoryChannel(),
            tpConfig.realtimeTopologyContextId(),
            repositoryClientConfig.topologyAvailabilityTracker(),
            minsToWaitForTopology,
            TimeUnit.MINUTES, acceptedActionsStore(),
            actionSettingsStrictTopologyIdMatch);
    }

    @Bean
    public ActionModeCalculator actionModeCalculator() {
        return new ActionModeCalculator();
    }

    @Bean
    public ExecutorService automatedActionThreadpool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                "auto-act-exec-%d").build();
        return Executors.newFixedThreadPool(concurrentAutomatedActions, threadFactory);
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
     * Executor service to perform cleanup tasks. We do not need a lot of threads here.
     *
     * @return the scheduler created
     */
    @Bean(destroyMethod = "shutdownNow")
    protected ScheduledExecutorService cleanupExecutorService() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                "act-cleanup-%d").build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

    /**
     * Identity service for market recommendations.
     *
     * @return identity service
     */
    @Bean
    public IdentityServiceImpl<ActionInfo, ActionInfoModel> actionIdentityService() {
        final IdentityServiceImpl<ActionInfo, ActionInfoModel> service = new IdentityServiceImpl<>(
                recommendationIdentityStore(), new ActionInfoModelCreator(), Clock.systemUTC(),
                24 * 3600 * 1000);
        cleanupExecutorService().scheduleWithFixedDelay(
                new RteLoggingRunnable(service::pruneObsoleteCache, "Prune action identity cache"),
                identityCachePurgeIntervalSec, identityCachePurgeIntervalSec, TimeUnit.SECONDS);
        return service;
    }

    /**
     * Topology processor listener, which forwards topology and entity state updates.
     *
     * @return The {@link ActionTopologyListener}.
     */
    @Bean
    public ActionTopologyListener tpListener() {
        final ActionTopologyListener topologyListener = new ActionTopologyListener(actionStorehouse(),
                tpConfig.actionTopologyStore(),
                tpConfig.realtimeTopologyContextId());
        tpConfig.topologyProcessor().addLiveTopologyListener(topologyListener);
        tpConfig.topologyProcessor().addEntitiesWithNewStatesListener(topologyListener);
        return topologyListener;
    }

    /**
     * Returns the {@link ActionStoreFactory} bean.
     *
     * @return the {@link ActionStoreFactory} bean.
     */
    @Bean
    public IActionStoreFactory actionStoreFactory() {
        return ActionStoreFactory.newBuilder()
            .withTopologyStore(tpConfig.actionTopologyStore())
            .withActionFactory(actionFactory())
            .withRealtimeTopologyContextId(tpConfig.realtimeTopologyContextId())
            .withDatabaseDslContext(databaseConfig.dsl())
            .withActionHistoryDao(actionHistory())
            .withActionTargetSelector(actionExecutionConfig.actionTargetSelector())
            .withProbeCapabilityCache(actionExecutionConfig.targetCapabilityCache())
            .withEntitySettingsCache(entitySettingsCache())
            .withActionsStatistician(actionStatsConfig.actionsStatistician())
            .withActionTranslator(actionTranslationConfig.actionTranslator())
            .withAtomicActionFactory(atomicActionFactory())
            .withClock(actionOrchestratorGlobalConfig.actionOrchestratorClock())
            .withUserSessionContext(userSessionConfig.userSessionContext())
            .withSupplyChainService(SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()))
            .withRepositoryService(RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()))
            .withLicenseCheckClient(licenseCheckClientConfig.licenseCheckClient())
            .withAcceptedActionsStore(acceptedActionsStore())
            .withRejectedActionsStore(rejectedActionsStore())
            .withActionIdentityService(actionIdentityService())
            .withInvolvedEntitiesExpander(actionStatsConfig.involvedEntitiesExpander())
            .withActionAuditSender(auditCommunicationConfig.actionAuditSender())
            .withRiskPropagationEnabledFlag(riskPropagationEnabled)
            .withQueryTimeWindowForLastExecutedActionsMins(queryTimeWindowForLastExecutedActionsMins)
            .build();
    }

    /**
     * Creates instance of {@link RegularActionsStoreCleaner} which has internal logic
     * of regularly checking accepted and rejection actions and deleting expired of them.
     *
     * @return instance of {@link RegularActionsStoreCleaner}.
     */
    @Bean
    public RegularActionsStoreCleaner regularActionsStoreCleaner() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("actionStore-cleaner-%d").build();
        return new RegularActionsStoreCleaner(
                Executors.newSingleThreadScheduledExecutor(threadFactory), acceptedActionsStore(),
                rejectedActionsStore(), minsActionAcceptanceTTL, minsActionRejectionTTL,
                minsFrequencyOfCleaningAcceptedActionsStore);
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

    /**
     * Creates DAO implementation for working with rejected actions.
     *
     * @return instance of {@link RejectedActionsDAO}
     */
    @Bean
    public RejectedActionsDAO rejectedActionsStore() {
        return new RejectedActionsStore(databaseConfig.dsl());
    }
}
