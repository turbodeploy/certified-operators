package com.vmturbo.action.orchestrator.store;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.action.orchestrator.execution.affected.entities.EntitiesInCoolDownPeriodCache;
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
import com.vmturbo.action.orchestrator.action.LoggingActionEventListener;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.action.RejectedActionsStore;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.approval.ApprovalCommunicationConfig;
import com.vmturbo.action.orchestrator.audit.AuditCommunicationConfig;
import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.execution.ActionCombiner;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.execution.ConditionalSubmitter;
import com.vmturbo.action.orchestrator.execution.affected.entities.AffectedEntitiesManager;
import com.vmturbo.action.orchestrator.stats.ActionStatsConfig;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionSpecsCache;
import com.vmturbo.action.orchestrator.store.atomic.PlanAtomicActionFactory;
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
import com.vmturbo.components.common.utils.RteLoggingRunnable;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;

/**
 * Configuration for the ActionStore package.
 */
@Configuration
@Import({ActionOrchestratorApiConfig.class,
    ActionOrchestratorDBConfig.class,
    ActionOrchestratorGlobalConfig.class,
    ActionExecutionConfig.class,
    GroupClientConfig.class,
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
    private ActionOrchestratorApiConfig actionOrchestratorApiConfig;

    @Autowired
    private ActionOrchestratorDBConfig databaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

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

    @Value("${actionExecution.isConditionalSubmitter:true}")
    private boolean isConditionalSubmitter;

    @Value("${actionExecution.isConditionalSubmitter.delaySecs:300}")
    private int conditionalSubmitterDelaySecs;

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

    @Value("${entitiesInCoolDownPeriodCacheSizeInMins:1440}")
    private int entitiesInCoolDownPeriodCacheSizeInMins;

    /**
     * Enable 'Scale for Performance' and 'Scale for Savings' settings.
     */
    @Value("${enableCloudScaleEnhancement:true}")
    private boolean enableCloudScaleEnhancement;

    @Bean
    public IActionFactory actionFactory() {
        return new ActionFactory(actionModeCalculator(),
                Arrays.asList(
                        loggingActionEventListener(),
                        affectedEntitiesManager()));
    }

    @Bean
    public LoggingActionEventListener loggingActionEventListener() {
        return new LoggingActionEventListener();
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
     * The time window within which actions will not be populated if they are already executed (SUCCEEDED).
     *
     * @return The time window within which actions will not be populated if
     * they are already executed (SUCCEEDED).
     */
    @Bean
    public int queryTimeWindowForLastExecutedActionsMins() {
        return queryTimeWindowForLastExecutedActionsMins;
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

    /**
     * Create atomic action factory for plans
     *
     * @return @link PlanAtomicActionFactory}
     */
    @Bean
    public PlanAtomicActionFactory planAtomicActionFactory() {
        return new PlanAtomicActionFactory(actionMergeSpecsCache());
    }

    @Bean
    public EntitiesAndSettingsSnapshotFactory entitySettingsCache() {
        return new EntitiesAndSettingsSnapshotFactory(
            groupClientConfig.groupChannel(),
            tpConfig.realtimeTopologyContextId(),
            acceptedActionsStore(),
            entitiesSnapshotFactory());
    }

    @Bean
    public EntitiesSnapshotFactory entitiesSnapshotFactory() {
        return new EntitiesSnapshotFactory(tpConfig.actionTopologyStore(), tpConfig.realtimeTopologyContextId(),
                minsToWaitForTopology, TimeUnit.MINUTES, new SupplyChainCalculator(),
                actionOrchestratorGlobalConfig.actionOrchestratorClock());
    }

    @Bean
    public ActionModeCalculator actionModeCalculator() {
        return new ActionModeCalculator(enableCloudScaleEnhancement);
    }

    @Bean
    public ActionCombiner actionCombiner() {
        return new ActionCombiner(tpConfig.actionTopologyStore());
    }

    @Bean
    public Executor automatedActionSubmitter() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("auto-act-exec-%d").build();

        return isConditionalSubmitter
                ? new ConditionalSubmitter(concurrentAutomatedActions, threadFactory,
                        conditionalSubmitterDelaySecs)
                : Executors.newFixedThreadPool(concurrentAutomatedActions, threadFactory);
    }

    @Bean
    public AutomatedActionExecutor automatedActionExecutor() {
        return new AutomatedActionExecutor(actionExecutionConfig.actionExecutor(),
                automatedActionSubmitter(), workflowConfig.workflowStore(),
                actionExecutionConfig.actionTargetSelector(), entitySettingsCache(),
                actionTranslationConfig.actionTranslator(), actionCombiner());
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
    public IdentityServiceImpl<ActionInfo, String, ActionInfoModel> actionIdentityService() {
        final IdentityServiceImpl<ActionInfo, String, ActionInfoModel> service =
            new IdentityServiceImpl<>(
                recommendationIdentityStore(), new ActionInfoModelCreator(),
                ActionInfoModel::getActionHexHash, actionOrchestratorGlobalConfig.actionOrchestratorClock(),
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
     * The {@link EntitySeverityCache}.
     *
     * @return The {@link EntitySeverityCache}.
     */
    @Bean
    public EntitySeverityCache entitySeverityCache() {
        return new EntitySeverityCache(tpConfig.actionTopologyStore(),
                actionOrchestratorApiConfig.entitySeverityNotificationSender(),
                riskPropagationEnabled);
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
            .withEntitySettingsCache(entitySettingsCache())
            .withActionTranslator(actionTranslationConfig.actionTranslator())
            .withClock(actionOrchestratorGlobalConfig.actionOrchestratorClock())
            .withUserSessionContext(userSessionConfig.userSessionContext())
            .withSeverityCache(entitySeverityCache())
            .withLicenseCheckClient(licenseCheckClientConfig.licenseCheckClient())
            .withAcceptedActionsStore(acceptedActionsStore())
            .withRejectedActionsStore(rejectedActionsStore())
            .withActionIdentityService(actionIdentityService())
            .withInvolvedEntitiesExpander(actionStatsConfig.involvedEntitiesExpander())
            .withRiskPropagationEnabledFlag(riskPropagationEnabled)
            .withWorkflowStore(workflowConfig.workflowStore())
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
            actionExecutionConfig.actionTargetSelector(),
            licenseCheckClientConfig.licenseCheckClient());
    }

    @Bean
    public ActionAutomationManager automationManager() {
        return new ActionAutomationManager(automatedActionExecutor(),
            approvalCommunicationConfig.approvalRequester());
    }

    /**
     * The actions storehouse.
     *
     * @return The actions storehouse.
     */
    @Bean
    public ActionStorehouse actionStorehouse() {
        ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory(),
                actionStoreLoader(), automationManager());
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
        return new ActionHistoryDaoImpl(databaseConfig.dsl(), actionModeCalculator(),
                actionOrchestratorGlobalConfig.actionOrchestratorClock());
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

    @Bean
    public EntitiesInCoolDownPeriodCache entitiesInCoolDownPeriodCache() {
        return new EntitiesInCoolDownPeriodCache(
                actionOrchestratorGlobalConfig.actionOrchestratorClock(),
                entitiesInCoolDownPeriodCacheSizeInMins);
    }

    /**
     * Returns manager responsible for affected entities.
     *
     * @return instance of {@link AffectedEntitiesManager}
     */
    @Bean
    public AffectedEntitiesManager affectedEntitiesManager() {
        return new AffectedEntitiesManager(
                actionHistory(),
                actionOrchestratorGlobalConfig.actionOrchestratorClock(),
                entitiesInCoolDownPeriodCacheSizeInMins,
                entitiesInCoolDownPeriodCache());
    }
}
