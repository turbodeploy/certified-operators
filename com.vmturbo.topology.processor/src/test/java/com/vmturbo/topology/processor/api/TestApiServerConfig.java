package com.vmturbo.topology.processor.api;

import static org.mockito.Matchers.any;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import com.vmturbo.common.protobuf.search.SearchMoles.SearchServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.topology.processor.TestIdentityStore;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.actions.ActionExecutionRpcService;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.context.ActionExecutionContextFactory;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.controllable.EntityActionDao;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader;
import com.vmturbo.topology.processor.discoverydumper.TargetDumpingSettings;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.rest.OperationController;
import com.vmturbo.topology.processor.rest.ProbeController;
import com.vmturbo.topology.processor.rest.TargetController;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.DerivedTargetParser;
import com.vmturbo.topology.processor.targets.GroupScopeResolver;
import com.vmturbo.topology.processor.targets.KVBackedTargetStore;
import com.vmturbo.topology.processor.targets.TargetSpecAttributeExtractor;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.template.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.topology.TopologyHandler;
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

    @Bean
    public TopologyProcessorNotificationSender topologyProcessorNotificationSender() {
        final TopologyProcessorNotificationSender backend =
                new TopologyProcessorNotificationSender(apiServerThreadPool(), clock(),
                        liveTopologyConnection(), planTopologyConnection(),
                        planTopologyConnection(), notificationsConnection(),
                        topologySummaryConnection());
        targetStore().addListener(backend);
        probeStore().addListener(backend);
        return backend;
    }

    @Bean
    public KeyValueStore keyValueStore() {
        return Mockito.mock(KeyValueStore.class);
    }

    @Bean
    protected IdentityService identityService() {
        return new IdentityService(new IdentityServiceInMemoryUnderlyingStore(
                Mockito.mock(IdentityDatabaseStore.class)),
                        new HeuristicsMatcher());
    }

    @Bean
    public EntityActionDao controllableDao() {
        return Mockito.mock(EntityActionDao.class);
    }

    @Bean
    public ProbeInfoCompatibilityChecker compatibilityChecker() {
        return Mockito.mock(ProbeInfoCompatibilityChecker.class);
    }

    @Bean
    public IdentityProvider identityProvider() {
            return Mockito.spy(new IdentityProviderImpl(identityService(), keyValueStore(), compatibilityChecker(), 0L));
    }

    @Bean
    protected TestProbeStore probeStore() {
        return new TestProbeStore(identityProvider());
    }

    @Bean
    protected IdentityStore<TargetSpec> targetIdentityStore() {
        return new TestIdentityStore<>(new TargetSpecAttributeExtractor(probeStore()));
    }

    @Bean
    public TargetStore targetStore() {
        return new KVBackedTargetStore(keyValueStore(), probeStore(), targetIdentityStore());
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());
        converters.add(msgConverter);
    }

    @Bean
    public TargetController targetController() {
        return new TargetController(scheduler(), targetStore(), probeStore(), operationManager(), topologyHandler());
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
        return new EntityValidator();
    }

    @Bean
    public EntityStore entityRepository() {
        return new EntityStore(targetStore(), identityProvider(), Clock.systemUTC());
    }

    @Bean
    public DiscoveredGroupUploader groupRecorder() {
        return Mockito.mock(DiscoveredGroupUploader.class);
    }
    @Bean
    public DiscoveredWorkflowUploader workflowRecorder() {
        return Mockito.mock(DiscoveredWorkflowUploader.class);
    }

    @Bean
    public DiscoveredCloudCostUploader cloudCostUploadRecorder() {
        return Mockito.mock(DiscoveredCloudCostUploader.class);
    }

    @Bean
    public DiscoveredTemplateDeploymentProfileUploader discoveredTemplatesUploader() {
        return Mockito.mock(DiscoveredTemplateDeploymentProfileUploader.class);
    }

    @Bean
    public TopologyHandler topologyHandler() {
        return Mockito.mock(TopologyHandler.class);
    }

    @Bean
    public DerivedTargetParser derivedTargetParser() {
        return Mockito.mock(DerivedTargetParser.class);
    }

    @Bean
    public GroupScopeResolver groupScopeResolver() {
        GroupScopeResolver groupScopeResolver = Mockito.mock(GroupScopeResolver.class);
        Mockito.when(groupScopeResolver.processGroupScope(any(), any(), any()))
                .then(AdditionalAnswers.returnsSecondArg());
        return groupScopeResolver;
    }

    @Bean
    public TargetDumpingSettings targetDumpingSettings() {
        TargetDumpingSettings targetDumpingSettings = Mockito.mock(TargetDumpingSettings.class);
        Mockito.when(targetDumpingSettings.getDumpsToHold(any())).thenReturn(0);
        Mockito.doNothing().when(targetDumpingSettings).refreshSettings();
        return targetDumpingSettings;
    }

    /**
     * Returns mocked system notification producer.
     *
     * @return mocked system notification producer.
     */
    @Bean
    SystemNotificationProducer systemNotificationProducer() {
        SystemNotificationProducer systemNotificationProducer = Mockito.mock(SystemNotificationProducer.class);
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
            discoveredTemplatesUploader(),
            controllableDao(),
            derivedTargetParser(),
            groupScopeResolver(),
            targetDumpingSettings(),
            systemNotificationProducer(),
            1L, 1L, 1L,
            5, 1, 1,
            TheMatrix.instance());
    }

    @Bean
    public Scheduler scheduler() {
        return Mockito.mock(Scheduler.class);
    }

    @Bean
    public OperationController operationController() {
        return new OperationController(operationManager(), scheduler(), targetStore());
    }

    @Bean
    public SearchServiceBlockingStub searchServiceBlockingStub() {
        // Since SearchServiceBlockingStub is a final class, it cannot be mocked or spied. Therefore,
        // create a mocked Search Service, and then use that channel to create a SearchServiceBlockingStub
        final SearchServiceMole searchServiceSpy = Mockito.spy(new SearchServiceMole());
        GrpcTestServer grpcServer = GrpcTestServer.newServer(searchServiceSpy);
        try {
            grpcServer.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return SearchServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    @Bean
    public ActionDataManager actionDataManager() {
        return new ActionDataManager(searchServiceBlockingStub(), topologyToSdkEntityConverter());
    }

    @Bean
    public TopologyToSdkEntityConverter topologyToSdkEntityConverter() {
        return new TopologyToSdkEntityConverter(entityRepository(), targetStore(),
                                                Mockito.mock(GroupScopeResolver.class));
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
        return new EntityRetriever(topologyToSdkEntityConverter(), repositoryClient(), 0);
    }

    @Bean
    public ActionExecutionContextFactory actionExecutionContextFactory() {
        return new ActionExecutionContextFactory(actionDataManager(),
                entityRepository(),
                entityRetriever(),
                targetStore(), probeStore());
    }

    @Bean
    public ActionExecutionRpcService actionExecutionRpcService() {
        return new ActionExecutionRpcService(
                operationManager(),
                actionExecutionContextFactory());
    }

}
