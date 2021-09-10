package com.vmturbo.topology.processor.actions;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.auth.api.AuthClientConfig;
import com.vmturbo.auth.api.authorization.kvstore.ComponentJwtStore;
import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.action.ActionConstraintsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionConstraintsServiceGrpc.ActionConstraintsServiceStub;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.AtomicActionSpecsUploadServiceGrpc;
import com.vmturbo.common.protobuf.action.AtomicActionSpecsUploadServiceGrpc.AtomicActionSpecsUploadServiceStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ActionExecutionREST.ActionExecutionServiceController;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.kvstore.PublicKeyStoreConfig;
import com.vmturbo.platform.sdk.common.MediationMessage.ActionApprovalResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.GetActionStateResponse;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology;

/**
 * Configuration for action execution.
 */
@Configuration
@Import({ControllableConfig.class,
        EntityConfig.class,
        OperationConfig.class,
        RepositoryConfig.class,
        TargetConfig.class,
        ActionOrchestratorClientConfig.class,
        BaseKafkaProducerConfig.class,
        AuthClientConfig.class,
        StitchingConfig.class, TopologyProcessorClientConfig.class})
public class ActionsConfig {

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private ActionMergeSpecsConfig actionMergeSpecsConfig;

    @Autowired
    private StitchingConfig stitchingConfig;

    @Autowired
    private TopologyProcessorClientConfig tpConfig;

    @Autowired
    private PublicKeyStoreConfig publicKeyStoreConfig;

    @Autowired
    private AuthClientConfig authClientConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;
    /**
     * Period of sending all the internal action state updates to external approval backend.
     */
    @Value("${actionStateUpdatesSendPeriodSec:30}")
    private long actionStateUpdatesSendPeriodSec;
    /**
     * Batch size for sending internal action state updtates to external approval backend.
     */
    @Value("${actionStateUpdatesBatchSize:100}")
    private int actionStateUpdatesBatchSize;
    /**
     * Period of retrieving action state changes from external approval backend.
     */
    @Value("${actionGetStatesPeriodSec:30}")
    private long actionGetStatesPeriodSec;
    /**
     * Period of sending action audit batches to SDK probe.
     */
    @Value("${actionAuditSendPeriodSec:30}")
    private long actionAuditSendPeriodSec;
    /**
     * Size of action audit events batch to send to external action audit probe.
     */
    @Value("${actionAuditBatchSize:50}")
    private int actionAuditBatchSize;
    /**
     * Max elements is queue contains state updates to send to external approval backend.
     */
    @Value("${maxSizeOfStateUpdatesQueue:500000}")
    private int maxSizeOfStateUpdatesQueue;

    @Value("${serializeCachedTopology:true}")
    private boolean serializeCachedTopology;

    /**
     * If true, the cluster information of entities involved in the action is sent as a part of
     * action message sent to the probe.
     */
    @Value("${populateActionClusterInfo:false}")
    private boolean populateActionClusterInfo;

    @Bean
    public ActionDataManager actionDataManager() {
        return new ActionDataManager(
                SearchServiceGrpc.newBlockingStub(repositoryConfig.repositoryChannel()),
                topologyToSdkEntityConverter(), entityRetriever(),  groupAndPolicyRetriever(),
                populateActionClusterInfo);
    }

    @Bean
    public TopologyToSdkEntityConverter topologyToSdkEntityConverter() {
        return new TopologyToSdkEntityConverter(entityConfig.entityStore(),
                targetConfig.targetStore(), groupConfig.groupScopeResolver());
    }

    @Bean
    public CachedTopology cachedTopology() {
        return new CachedTopology(serializeCachedTopology);
    }

    /**
     * Entity retriever. It is able to retrieve entities from toplogy cached by the previous
     * broadcast or fall back to repository request.
     *
     * @return the bean created
     */
    @Bean
    public EntityRetriever entityRetriever() {
        return new EntityRetriever(
                topologyToSdkEntityConverter(),
                repositoryConfig.repository(),
                cachedTopology(),
                realtimeTopologyContextId);
    }

    /**
     * Policy retriever. It is required to get information about policy for action execution.
     *
     * @return the bean created.
     */
    @Bean
    public GroupAndPolicyRetriever groupAndPolicyRetriever() {
        return new GroupAndPolicyRetriever(groupConfig.groupServiceBlockingStub(),
            groupConfig.policyRpcService());
    }

    @Bean
    public ComponentJwtStore jwtStore() {
        return new ComponentJwtStore(publicKeyStoreConfig.publicKeyStore(), targetConfig.getIdentityGeneratorPrefix(),
                targetConfig.keyProvider());
    }

    @Bean
    public SecureStorageClient secureStorageClient() {
        return new SecureStorageClient(authClientConfig.getAuthHost(),
                authClientConfig.getAuthPort(), authClientConfig.getAuthRoute(), jwtStore());
    }

    @Bean
    public ActionExecutionContextFactory actionExecutionContextFactory() {
        return new ActionExecutionContextFactory(actionDataManager(),
                entityConfig.entityStore(),
                entityRetriever(),
                targetConfig.targetStore(),
                probeConfig.probeStore(),
                groupAndPolicyRetriever(),
                secureStorageClient());
    }

    @Bean
    public ActionExecutionRpcService actionExecutionService() {
        return new ActionExecutionRpcService(
                operationConfig.operationManager(),
                actionExecutionContextFactory());
    }

    @Bean
    public ActionExecutionServiceController actionExecutionServiceController() {
        return new ActionExecutionServiceController(actionExecutionService());
    }

    @Bean
    public ActionConstraintsServiceStub actionConstraintsServiceStub() {
        return ActionConstraintsServiceGrpc.newStub(
            aoClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public ActionsServiceBlockingStub actionsServiceBlockingStub() {
        return ActionsServiceGrpc.newBlockingStub(aoClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub() {
        return SettingPolicyServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public ActionConstraintsUploader actionConstraintsUploader() {
        return new ActionConstraintsUploader(entityConfig.entityStore(),
                actionConstraintsServiceStub(),
                new GroupMemberRetriever(groupConfig.groupServiceBlockingStub()),
                settingPolicyServiceBlockingStub());
    }

    /**
     * Scheduler used for action orchestrator related tasks. It is Ok to have only 1 thread as
     * the main purpose is to send a message to remote SDK probe (not waiting for the response).
     *
     * @return scheduled thread pool.
     */
    @Bean
    public ScheduledExecutorService actionRelatedScheduler() {
        final ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("tp-aoc-sched-%d")
                .setUncaughtExceptionHandler((thread, throwable) -> LogManager.getLogger(getClass())
                        .error("Uncaught exception found in the thread " + thread.getName(),
                                throwable))
                .build();
        return Executors.newScheduledThreadPool(1, factory);
    }

    /**
     * Internal action state changes service.
     *
     * @return the service
     */
    @Bean
    public ActionUpdateStateService actionUpdateStateService() {
        return new ActionUpdateStateService(targetConfig.targetStore(),
                operationConfig.operationManager(),
                aoClientConfig.createActionStateUpdateListener(), actionRelatedScheduler(),
                actionStateUpdatesSendPeriodSec, actionStateUpdatesBatchSize,
                maxSizeOfStateUpdatesQueue);
    }

    /**
     * Asynchronous message sender for external action approval submission results.
     *
     * @return message sender
     */
    @Bean
    public IMessageSender<ActionApprovalResponse> externalActionApprovalResponseSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(TopologyProcessorClient.EXTERNAL_ACTION_APPROVAL_RESPONSE);
    }

    /**
     * Asynchronous message sender for external action state changes.
     *
     * @return message sender
     */
    @Bean
    public IMessageSender<GetActionStateResponse> externalStateUpdatesSender() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(TopologyProcessorClient.EXTERNAL_ACTION_UPDATES_TOPIC);
    }

    /**
     * Action approval service.
     *
     * @return action approval service.
     */
    @Bean
    public ActionApprovalService actionApprovalService() {
        return new ActionApprovalService(aoClientConfig.createActionApprovalRequestListener(),
                externalStateUpdatesSender(), externalActionApprovalResponseSender(),
                operationConfig.operationManager(), actionExecutionContextFactory(),
                targetConfig.targetStore(), actionRelatedScheduler(), actionGetStatesPeriodSec);
    }

    /**
     * Action audit service.
     *
     * @return action audit service
     */
    @Bean
    public ActionAuditService actionAuditService() {
        final int priority = Math.min(targetConfig.targetStore().priority(),
                probeConfig.probeStore().priority()) - 1;
        final ActionAuditService actionAuditService =
                new ActionAuditService(aoClientConfig.createActionEventsListener(),
                        operationConfig.operationManager(), actionExecutionContextFactory(),
                        actionRelatedScheduler(), actionAuditSendPeriodSec, actionAuditBatchSize,
                        priority, thinTargetCache());
        topologyProcessor().addTargetListener(actionAuditService);
        return actionAuditService;
    }

    /**
     * Topology processor bean.
     *
     * @return instance of topology processor
     */
    @Bean
    public TopologyProcessor topologyProcessor() {
        return tpConfig.topologyProcessor(
                TopologyProcessorSubscription.forTopic(Topic.Notifications));
    }

    /**
     * A cache for simple target information.
     *
     * @return instance of thin target cache
     */
    @Bean
    public ThinTargetCache thinTargetCache() {
        return new ThinTargetCache(topologyProcessor());
    }

    /**
     * Creates AtomicActionSpecsUploadServiceStub used by the atomic action specs upload service.
     *
     * @return AtomicActionSpecsUploadServiceStub
     */
    @Bean
    public AtomicActionSpecsUploadServiceStub atomicActionSpecsUploadServiceStub() {
        return AtomicActionSpecsUploadServiceGrpc.newStub(
                aoClientConfig.actionOrchestratorChannel());
    }

    /**
     * Service to broadcast and upload the {@link AtomicActionSpec}'s created for entities.
     *
     * @return {@link ActionMergeSpecsUploader}
     */
    @Bean
    public ActionMergeSpecsUploader actionMergeSpecsUploader() {
        return new ActionMergeSpecsUploader(actionMergeSpecsConfig.actionMergeSpecsRepository(),
                                            probeConfig.probeStore(),
                                            targetConfig.targetStore(),
                                            atomicActionSpecsUploadServiceStub());
    }
}
