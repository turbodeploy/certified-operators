package com.vmturbo.topology.processor.communication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.communication.WebsocketServerTransportManager.TransportHandler;
import com.vmturbo.sdk.server.common.SdkWebsocketServerTransportHandler;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueue;
import com.vmturbo.topology.processor.communication.queues.AggregatingDiscoveryQueueImpl;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for the part of the server that communicates
 * with the probes over websocket.
 */
@Configuration
@Import({ProbeConfig.class, TargetConfig.class})
public class SdkServerConfig {

    public static final String REMOTE_MEDIATION_PATH = "/remoteMediation";

    @Value("${negotiation.timeout.sec:30}")
    private long negotiationTimeoutSec;

    @Value("${websocket.atomic.send.timeout.sec:30}")
    private long websocketAtomicSendTimeout;

    @Value("${applyPermitsToContainers:true}")
    private boolean applyPermitsToContainers;

    @Value("${discoveryWorkerPollingTimeoutSecs:10}")
    private long discoveryWorkerPollingTimeoutSecs;

    @Value("${maxConcurrentTargetDiscoveriesPerProbeCount:10}")
    private int maxConcurrentTargetDiscoveriesPerProbeCount;

    @Value("${maxConcurrentTargetIncrementalDiscoveriesPerProbeCount:10}")
    private int maxConcurrentTargetIncrementalDiscoveriesPerProbeCount;

    /**
     * Oldest version of client allowed to connect to TP. Empty string will be translated
     * to previous minor release. For example, 8.2.1 server will accept any client 8.1.0 or newer.
     * For major releases, empty string will be converted to the previous
     * major release. For example, for 9.0.0 if you specify no value here, any client 8.0.0 or
     * newer will be accepted. We default to 8.0.0 as 8.0 is the current version of Kubernetes probe
     * and those are updated separately from the rest of XL, thus may lag in version number by a
     * bit.
     */
    @Value("${oldestSupportedClientProtocolVersion:8.0.0}")
    private String oldestSupportedClientProtocolVersion;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private TargetConfig targetConfig;

    /**
     * Create the queue for target discoveries.
     *
     * @return {@link AggregatingDiscoveryQueue} which holds scheduled discoveries for targets.
     */
    @Bean
    public AggregatingDiscoveryQueue discoveryQueue() {
        return new AggregatingDiscoveryQueueImpl(probeConfig.probeStore());
    }

    /**
     * Return the appropriate type of RemoteMediationServer depending on whether we are controlling
     * permits at the container level or probe type leve.
     *
     * @return RemoteMediationServer based on value of applyPermitsToContainers.
     */
    @Bean
    public RemoteMediationServer remoteMediation() {
        return applyPermitsToContainers ? new RemoteMediationServerWithDiscoveryWorkers(
                probeConfig.probeStore(), targetConfig.probePropertyStore(),
                probeConfig.probeContainerChooser(), discoveryQueue(),
                maxConcurrentTargetDiscoveriesPerProbeCount,
                maxConcurrentTargetIncrementalDiscoveriesPerProbeCount,
                discoveryWorkerPollingTimeoutSecs)
                : new RemoteMediationServer(probeConfig.probeStore(),
                        targetConfig.probePropertyStore(),
                        probeConfig.probeContainerChooser());
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
                oldestSupportedClientProtocolVersion, negotiationTimeoutSec);
    }

    /**
     * Endpoint itself.
     *
     * @return Endpoint itself.
     */
    @Bean
    public WebsocketServerTransportManager remoteMediationServerTransportManager() {
        return new WebsocketServerTransportManager(remoteMediationTransportHandler(),
                sdkServerThreadPool(), websocketAtomicSendTimeout);
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

    public long getNegotiationTimeoutSec() {
        return negotiationTimeoutSec;
    }

    /**
     * Return whether permits are controlled at the container level or the probe type level.
     *
     * @return true if permits are being controlled at the container level.
     */
    public boolean getApplyPermitsToContainers() {
        return applyPermitsToContainers;
    }

    /**
     * Get the value of maxConcurrentTargetDiscoveriesPerProbeCount.
     *
     * @return maxConcurrentTargetDiscoveriesPerProbeCount value
     */
    public int getMaxConcurrentTargetDiscoveriesPerProbeCount() {
        return maxConcurrentTargetDiscoveriesPerProbeCount;
    }

    /**
     * Get the value of maxConcurrentTargetIncrementalDiscoveriesPerProbeCount.
     *
     * @return maxConcurrentTargetIncrementalDiscoveriesPerProbeCount value.
     */
    public int getMaxConcurrentTargetIncrementalDiscoveriesPerProbeCount() {
        return maxConcurrentTargetIncrementalDiscoveriesPerProbeCount;
    }
}
