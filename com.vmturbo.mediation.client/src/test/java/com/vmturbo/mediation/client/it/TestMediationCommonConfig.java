package com.vmturbo.mediation.client.it;

import static com.vmturbo.topology.processor.communication.SdkServerConfig.REMOTE_MEDIATION_PATH;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.communication.WebsocketServerTransportManager.TransportHandler;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.MapKeyValueStore;
import com.vmturbo.mediation.common.tests.util.IRemoteMediation;
import com.vmturbo.sdk.server.common.SdkWebsocketServerTransportHandler;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.topology.processor.actions.ActionMergeSpecsRepository;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityProviderImpl;
import com.vmturbo.topology.processor.identity.IdentityService;
import com.vmturbo.topology.processor.identity.services.HeuristicsMatcher;
import com.vmturbo.topology.processor.identity.storage.IdentityDatabaseStore;
import com.vmturbo.topology.processor.identity.storage.IdentityServiceInMemoryUnderlyingStore;
import com.vmturbo.topology.processor.probes.ProbeInfoCompatibilityChecker;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.RemoteProbeStore;
import com.vmturbo.topology.processor.stitching.StitchingOperationStore;
import com.vmturbo.topology.processor.targets.CachingTargetStore;
import com.vmturbo.topology.processor.targets.KvTargetDao;
import com.vmturbo.topology.processor.targets.TargetDao;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Test Spring configuration for SDK server side.
 */
@Configuration
public class TestMediationCommonConfig {

    public static final String FIELD_TEST_NAME = "testName";

    @Value("${" + FIELD_TEST_NAME + "}")
    public String testName;

    /**
     * Redefinition of the KeyValueStore bean
     * to use the map-based one instead of the
     * consul-based one.
     *
     * @return The key-value store.
     */
    @Bean
    public KeyValueStore keyValueStore() {
        return new MapKeyValueStore();
    }

    /**
     * Redefinition target store and identity provider to make sure
     * we use the in-memory ones, not the ones that rely on
     * databases.
     *
     * @return The identity provider.
     */
    @Bean
    public IdentityProvider identityProvider() {
        return new IdentityProviderImpl(
                new IdentityService(
                        new IdentityServiceInMemoryUnderlyingStore(
                            Mockito.mock(IdentityDatabaseStore.class), 1), new HeuristicsMatcher()),
                keyValueStore(), compatibilityChecker(), 0L);
    }

    @Bean
    public ProbeInfoCompatibilityChecker compatibilityChecker() {
        return new ProbeInfoCompatibilityChecker();
    }

    @Bean
    public ProbeStore probeStore() {
        return new RemoteProbeStore(keyValueStore(), identityProvider(),
            stitchingOperationStore(), new ActionMergeSpecsRepository());
    }

    @Bean
    public TargetDao targetDao() {
        return new KvTargetDao(keyValueStore(), probeStore());
    }

    /**
     * {@link TargetStore}.
     *
     * @return {@link TargetStore}.
     */
    @Bean
    public TargetStore targetStore() {
        return new CachingTargetStore(targetDao(), probeStore(),
                Mockito.mock(IdentityStore.class));
    }

    @Bean
    public RemoteMediationServer remoteMediation() {
        return new TestRemoteMediationServer(probeStore());
    }

    @Bean
    public IRemoteMediation remoteMediationInterface() {
        return new RemoteMediationImpl(remoteMediation(), probeStore());
    }

    @Bean
    public StitchingOperationStore stitchingOperationStore() {
        return new StitchingOperationStore(new StitchingOperationLibrary());
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

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService sdkServerThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                        .setNameFormat(Objects.requireNonNull(testName) + "-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    /*
     * Endpoint configuration beans.
     */
    @Bean
    public TransportHandler remoteMediationTransportHandler() {
        return new SdkWebsocketServerTransportHandler(remoteMediation(), sdkServerThreadPool(),
                30);
    }

    /**
     * Endpoint itself.
     *
     * @return Endpoint itself.
     */
    @Bean
    public WebsocketServerTransportManager remoteMediationServerTransportManager() {
        return new WebsocketServerTransportManager(remoteMediationTransportHandler(),
                sdkServerThreadPool(), 30);
    }

    /**
     * This bean configures endpoint to bind it to a specific address (path).
     *
     * @return bean
     */
    @Bean
    public ServerEndpointRegistration remoteMediationEndpointRegistration() {
        return new ServerEndpointRegistration(REMOTE_MEDIATION_PATH, remoteMediationServerTransportManager());
    }
}
