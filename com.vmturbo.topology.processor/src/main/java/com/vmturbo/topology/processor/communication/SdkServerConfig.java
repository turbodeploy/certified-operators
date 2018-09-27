package com.vmturbo.topology.processor.communication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.communication.WebsocketServerTransportManager.TransportHandler;
import com.vmturbo.sdk.server.common.SdkWebsocketServerTransportHandler;
import com.vmturbo.topology.processor.GlobalConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;

/**
 * Configuration for the part of the server that communicates
 * with the probes over websocket.
 */
@Configuration
@Import({ProbeConfig.class, GlobalConfig.class})
public class SdkServerConfig {

    public static final String REMOTE_MEDIATION_PATH = "/remoteMediation";

    @Value("${negotiation.timeout.sec:30}")
    private long negotiationTimeoutSec;

    @Autowired
    private ProbeConfig probeConfig;
    @Autowired
    private GlobalConfig globalConfig;

    @Bean
    public RemoteMediationServer remoteMediation() {
        return new RemoteMediationServer(probeConfig.probeStore());
    }

    /**
     * Thread pool, used for SDK server tasks.
     *
     * @return thread pool
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService sdkServerThreadPool() {
        final ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("sdk-server-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    /*
     * Endpoint configuration beans.
     */
    @Bean
    public TransportHandler remoteMediationTransportHandler() {
        return new SdkWebsocketServerTransportHandler(remoteMediation(), sdkServerThreadPool(),
                negotiationTimeoutSec);
    }

    /**
     * Endpoint itself.
     *
     * @return Endpoint itself.
     */
    @Bean
    public WebsocketServerTransportManager remoteMediationServerTransportManager() {
        return new WebsocketServerTransportManager(remoteMediationTransportHandler(),
                sdkServerThreadPool());
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
