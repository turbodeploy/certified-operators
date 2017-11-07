package com.vmturbo.topology.processor.api;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.topology.processor.TestProbeStore;
import com.vmturbo.topology.processor.actions.ActionExecutionRpcService;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.rest.OperationController;
import com.vmturbo.topology.processor.rest.ProbeController;
import com.vmturbo.topology.processor.rest.TargetController;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.targets.KVBackedTargetStore;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.plan.DiscoveredTemplateDeploymentProfileUploader;
import com.vmturbo.topology.processor.topology.TopologyHandler;

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
    public TopologyProcessorNotificationSender topologyProcessorNotificationSender() {
        final TopologyProcessorNotificationSender backend =
                new TopologyProcessorNotificationSender(apiServerThreadPool(),
                        liveTopologyConnection(), planTopologyConnection(),
                        planTopologyConnection(), notificationsConnection());
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
    public IdentityProvider identityProvider() {
            return Mockito.spy(new IdentityProviderImpl(identityService(), keyValueStore(), 0L));
    }

    @Bean
    protected TestProbeStore probeStore() {
        return new TestProbeStore(identityProvider());
    }

    @Bean
    public TargetStore targetStore() {
        return new KVBackedTargetStore(keyValueStore(), identityProvider(), probeStore());
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
        return new EntityStore(targetStore(), identityProvider(), entityValidator());
    }

    @Bean
    public DiscoveredGroupUploader groupRecorder() {
        return Mockito.mock(DiscoveredGroupUploader.class);
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
    public OperationManager operationManager() {
        return new OperationManager(identityProvider(), targetStore(), probeStore(),
                remoteMediation(), topologyProcessorNotificationSender(),
                entityRepository(), groupRecorder(), discoveredTemplatesUploader(),
            1L, 1L, 1L);
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
    public ActionExecutionRpcService actionExecutionRpcService() {
        return new ActionExecutionRpcService(entityRepository(), operationManager());
    }

}
