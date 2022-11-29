package com.vmturbo.topology.processor.api;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTOMoles;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanExportNotification;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.workflow.WorkflowDTOMoles.WorkflowServiceMole;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import com.vmturbo.communication.ITransport;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.actions.ActionExecutionRpcService;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;
import com.vmturbo.topology.processor.cost.AliasedOidsUploader;
import com.vmturbo.topology.processor.cost.BilledCloudCostUploader;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumper;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.StaleOidManagerImpl;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.TestAggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.planexport.DiscoveredPlanDestinationUploader;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.rest.OperationController;
import com.vmturbo.topology.processor.rest.ProbeController;
import com.vmturbo.topology.processor.rest.TargetController;
import com.vmturbo.topology.processor.rpc.TargetHealthRetriever;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetSpecAttributeExtractor;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.topology.TopologyHandler;
import com.vmturbo.topology.processor.topology.pipeline.CachedTopology;
import com.vmturbo.topology.processor.workflow.DiscoveredWorkflowUploader;

/**
 * API server-side Spring configuration.
 */
@Configuration
@EnableWebMvc
public class TestApiServerConfig extends WebMvcConfigurerAdapter {

    public static final String FIELD_TEST_NAME = "test.name";

    @Value("#{environment['" + FIELD_TEST_NAME + "']}")
    public String testName;

    @Bean
    public ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("srv-" + testName + "-%d").build();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        return Executors.newCachedThreadPool(threadFactory());
    }

    /**
     * Clock.
     *
     * @return Clock.
     */
    @Bean
    public Clock clock() {
        return new MutableFixedClock(1_000_000);
    }

    /**
     * This bean performs registration of all configured websocket endpoints.
     *
     * @return bean
     */
    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }

    @Bean
    public SenderReceiverPair<Topology> liveTopologyConnection() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public SenderReceiverPair<Topology> planTopologyConnection() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public SenderReceiverPair<TopologyProcessorNotification> notificationsConnection() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public SenderReceiverPair<TopologySummary> topologySummaryConnection() {
        return new SenderReceiverPair<>();
    }

    /**
     * Creates a {@link SenderReceiverPair} to exchange {@link EntitiesWithNewState} messages.
     * @return SenderReceiverPair the message pair
     */
    @Bean
    public SenderReceiverPair<EntitiesWithNewState> entitiesWithNewStateConnection() {
        return new SenderReceiverPair<>();
    }

    /**
     * Creates a {@link SenderReceiverPair} to exchange {@link PlanExportNotification} messages.
     * @return SenderReceiverPair the message pair
     */
    @Bean
    public SenderReceiverPair<PlanExportNotification> planExportNotificationConnection() {
        return new SenderReceiverPair<>();
    }

    /**
     * Creates a {@link TopologyProcessorNotificationSender}.
     * @return {@link TopologyProcessorNotificationSender} the sender.
     */
    @Bean
    public TopologyProcessorNotificationSender topologyProcessorNotificationSender() {
        final TopologyProcessorNotificationSender backend =
                new TopologyProcessorNotificationSender(apiServerThreadPool(), clock(),
                        liveTopologyConnection(), planTopologyConnection(),
                        planTopologyConnection(), notificationsConnection(),
                        topologySummaryConnection(), entitiesWithNewStateConnection(),
                        planExportNotificationConnection());
        targetStore().addListener(backend);
        probeStore().addListener(backend);
        return backend;
    }

    @Bean
    public KeyValueStore keyValueStore() {
        return mock(KeyValueStore.class);
    }

    @Bean
    protected IdentityService identityService() {
        return new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
                mock(IdentityDatabaseStore.class), 10, new ConcurrentHashMap<>(), false),
                        new HeuristicsMatcher());
    }

    @Bean
    public EntityActionDao controllableDao() {
        return mock(EntityActionDao.class);
    }

    @Bean
    public ProbeInfoCompatibilityChecker compatibilityChecker() {
        return mock(ProbeInfoCompatibilityChecker.class);
    }

    @Bean
    public IdentityProvider identityProvider() {
            return Mockito.spy(new IdentityProviderImpl(keyValueStore(), compatibilityChecker(),
                0L, mock(IdentityDatabaseStore.class), 10, 0,  mock(StaleOidManagerImpl.class),
                    false));
    }

    @Bean
    protected TestProbeStore probeStore() {
        return new TestProbeStore(identityProvider());
    }

    @Bean
    protected IdentityStore<TargetSpec> targetIdentityStore() {
        return new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore()));
    }

    /**
     * Target DAO.
     *
     * @return {@link TargetDao}.
     */
    @Bean
    public TargetDao targetDao() {
        return mock(TargetDao.class);
    }

    /**
     * Target store.
     *
     * @return {@link TargetStore}.
     */
    @Bean
    public TargetStore targetStore() {
        return new CachingTargetStore(targetDao(), probeStore(), targetIdentityStore(),
                Clock.systemUTC(), binaryDiscoveryDumper());
    }

    /**
     * Target Health Retriever.
     *
     * @return {@link TargetHealthRetriever}.
     */
    @Bean
    public TargetHealthRetriever targetHealthRetriever() {
        return mock(TargetHealthRetriever.class);
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());
        converters.add(msgConverter);
    }

    @Bean
    public TargetController targetController() {
        return new TargetController(scheduler(), targetStore(), probeStore(), operationManager(),
                topologyHandler(), settingPolicyServiceBlockingStub(),
                workflowServiceBlockingStub(), targetHealthRetriever());
    }

    @Bean
    public ProbeController probeController() {
        return new ProbeController(probeStore());
    }

    @Bean
    public FakeRemoteMediation remoteMediation() {
        return new FakeRemoteMediation(targetStore());
    }

    @Bean
    public EntityValidator entityValidator() {
        return new EntityValidator(true);
    }

    @Bean
    public EntityStore entityRepository() {
        return new EntityStore(targetStore(), identityProvider(), 0.3F, true,
                Collections.singletonList(topologyProcessorNotificationSender()),
                Clock.systemUTC(), Collections.emptySet(), true
        );
    }

    @Bean
    public DiscoveredGroupUploader groupRecorder() {
        return mock(DiscoveredGroupUploader.class);
    }
    @Bean
    public DiscoveredWorkflowUploader workflowRecorder() {
        return mock(DiscoveredWorkflowUploader.class);
    }

    @Bean
    public DiscoveredCloudCostUploader cloudCostUploadRecorder() {
        return mock(DiscoveredCloudCostUploader.class);
    }

    @Bean
    public BilledCloudCostUploader billedCloudCostUploader() {
        return mock(BilledCloudCostUploader.class);
    }

    @Bean
    public AliasedOidsUploader aliasedOidsUploader() {
        return mock(AliasedOidsUploader.class);
    }

    @Bean
    public DiscoveredPlanDestinationUploader discoveredPlanDestinationUploader() {
        return mock(DiscoveredPlanDestinationUploader.class);
    }

    @Bean
    public DiscoveredTemplateDeploymentProfileUploader discoveredTemplatesUploader() {
        return mock(DiscoveredTemplateDeploymentProfileUploader.class);
    }

    @Bean
    public TopologyHandler topologyHandler() {
        return mock(TopologyHandler.class);
    }

    @Bean
    public DerivedTargetParser derivedTargetParser() {
        return mock(DerivedTargetParser.class);
    }

    @Bean
    public GroupScopeResolver groupScopeResolver() {
        GroupScopeResolver groupScopeResolver = mock(GroupScopeResolver.class);
        Mockito.when(groupScopeResolver.processGroupScope(any(), any(), any()))
                .then(AdditionalAnswers.returnsSecondArg());
        return groupScopeResolver;
    }

    @Bean
    public TargetDumpingSettings targetDumpingSettings() {
        TargetDumpingSettings targetDumpingSettings = mock(TargetDumpingSettings.class);
        Mockito.when(targetDumpingSettings.getDumpsToHold(any())).thenReturn(0);
        Mockito.doNothing().when(targetDumpingSettings).refreshSettings();
        return targetDumpingSettings;
    }

    @Bean
    AggregatingDiscoveryQueue discoveryQueue() {
        @SuppressWarnings("unchecked")
        final ITransport<MediationServerMessage, MediationClientMessage> transport =
                mock(ITransport.class);

        return new TestAggregatingDiscoveryQueue(transport);
    }

    /**
     * Returns mocked system notification producer.
     *
     * @return mocked system notification producer.
     */
    @Bean
    SystemNotificationProducer systemNotificationProducer() {
        SystemNotificationProducer systemNotificationProducer = mock(SystemNotificationProducer.class);
        return systemNotificationProducer;
    }

    @Bean
    public OperationManager operationManager() {

        return new OperationManager(identityProvider(),
            targetStore(),
            probeStore(),
            remoteMediation(),
            topologyProcessorNotificationSender(),
            entityRepository(),
            groupRecorder(),
            workflowRecorder(),
            cloudCostUploadRecorder(),
            billedCloudCostUploader(),
            aliasedOidsUploader(),
            discoveredPlanDestinationUploader(),
            discoveredTemplatesUploader(),
            controllableDao(),
            derivedTargetParser(),
            groupScopeResolver(),
            targetDumpingSettings(),
            systemNotificationProducer(),
            1L, 1L, 1L, 1L,
            5, 10, 1, 1,
            TheMatrix.instance(),
            binaryDiscoveryDumper(),
            false,
            licenseCheckClient(),
            100);
    }

    @Bean
    public LicenseCheckClient licenseCheckClient() {
        return mock(LicenseCheckClient.class);
    }

    @Bean
    public Scheduler scheduler() {
        return mock(Scheduler.class);
    }

    @Bean
    public OperationController operationController() {
        return new OperationController(operationManager(), scheduler(), targetStore());
    }

    /**
     * Creates a {@link BinaryDiscoveryDumper}.
     * @return {@link BinaryDiscoveryDumper} the dumper.
     */
    @Bean
    public BinaryDiscoveryDumper binaryDiscoveryDumper() {
        return Mockito.mock(BinaryDiscoveryDumper.class);
    }

    /**
     * Bean of test grpc server.
     *
     * @return the instance of started grpc server.
     */
    @Bean
    public GrpcTestServer grpcTestServer() {
        try {
            final GrpcTestServer testServer =
                    GrpcTestServer.newServer(searchService(), settingPolicyService(),
                            workflowService(), policyService());
            testServer.start();
            return testServer;
        } catch (IOException e) {
            throw new BeanCreationException("Failed to create test grpc server", e);
        }
    }

    /**
     * Search service.
     *
     * @return the instance of search service.
     */
    @Bean
    public SearchServiceMole searchService() {
        return Mockito.spy(new SearchServiceMole());
    }

    /**
     * SettingPolicy service.
     *
     * @return the instance of settingPolicy service.
     */
    @Bean
    public SettingPolicyServiceMole settingPolicyService() {
        return Mockito.spy(new SettingPolicyServiceMole());
    }

    /**
     * Policy service.
     *
     * @return the instance of policy service.
     */
    @Bean
    public PolicyDTOMoles.PolicyServiceMole policyService() {
        return Mockito.spy(new PolicyDTOMoles.PolicyServiceMole());
    }

    /**
     * Workflow service.
     *
     * @return the instance of workflow service.
     */
    @Bean
    public WorkflowServiceMole workflowService() {
        return Mockito.spy(new WorkflowServiceMole());
    }

    /**
     * Bean which can be used to interact with search rpc service in repository.
     *
     * @return a new SearchServiceBlockingStub
     */
    @Bean
    public SearchServiceBlockingStub searchServiceBlockingStub() {
        return SearchServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
    }

    /**
     * Bean which can be used to interact with setting policy rpc service in group-component.
     *
     * @return a new SettingPolicyServiceBlockingStub
     */
    @Bean
    public SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub() {
        return SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
    }

    /**
     * Bean which can be used to interact with policy rpc service in group-component.
     *
     * @return a new PolicyServiceBlockingStub
     */
    @Bean
    public PolicyServiceGrpc.PolicyServiceBlockingStub policyServiceBlockingStub() {
        return PolicyServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
    }

    /**
     * Bean which can be used to interact with group rpc service in group-component.
     *
     * @return a new GroupServiceBlockingStub
     */
    @Bean
    public GroupServiceGrpc.GroupServiceBlockingStub groupServiceBlockingStub() {
        return GroupServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
    }

    /**
     * Bean which can be used to interact with workflow rpc service in AO.
     *
     * @return a new WorkflowServiceBlockingStub
     */
    @Bean
    public WorkflowServiceBlockingStub workflowServiceBlockingStub() {
        return WorkflowServiceGrpc.newBlockingStub(grpcTestServer().getChannel());
    }

    /**
     * Return instance of action data manager.
     *
     * @return instance of {@link ActionDataManager}
     */
    @Bean
    public ActionDataManager actionDataManager() {
        return new ActionDataManager(searchServiceBlockingStub(), topologyToSdkEntityConverter(),
            entityRetriever(), groupAndPolicyRetriever(), false);
    }

    @Bean
    public TopologyToSdkEntityConverter topologyToSdkEntityConverter() {
        return new TopologyToSdkEntityConverter(entityRepository(), targetStore(),
                                                mock(GroupScopeResolver.class));
    }

    @Bean
    public FakeRepositoryClient repositoryClient() {
        // Fake the remote calls to the Repository service
        return new FakeRepositoryClient();
    }

    @Bean
    public EntityRetriever entityRetriever() {
        // Create an entity retriever with a real entity converter and a mock repository client
        // Since the repository client is a mock, the context ID doesn't matter - using zero
        return new EntityRetriever(topologyToSdkEntityConverter(), repositoryClient(),
                cachedTopology(), 0);
    }

    /**
     * The policy retriever object.
     *
     * @return the policy retriever object.
     */
    @Bean
    public GroupAndPolicyRetriever groupAndPolicyRetriever() {
        return new GroupAndPolicyRetriever(groupServiceBlockingStub(), policyServiceBlockingStub());
    }

    /**
     * The client for interacting with secure storage.
     *
     * @return the client for interacting with secure storage.
     */
    @Bean
    public SecureStorageClient secureStorageClient() {
        return Mockito.mock(SecureStorageClient.class);
    }

    /**
     * Cached topology.
     *
     * @return the bean created
     */
    @Bean
    public CachedTopology cachedTopology() {
        return new CachedTopology(false);
    }

    @Bean
    public ActionExecutionContextFactory actionExecutionContextFactory() {
        return new ActionExecutionContextFactory(actionDataManager(), entityRepository(),
                entityRetriever(), targetStore(), probeStore(), groupAndPolicyRetriever(),
                secureStorageClient());
    }

    @Bean
    public ActionExecutionRpcService actionExecutionRpcService() {
        return new ActionExecutionRpcService(
                operationManager(),
                secureStorageClient(),
                actionExecutionContextFactory());
    }

}
