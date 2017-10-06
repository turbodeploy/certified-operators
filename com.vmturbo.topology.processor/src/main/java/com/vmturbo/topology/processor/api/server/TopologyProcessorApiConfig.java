package com.vmturbo.topology.processor.api.server;

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

import com.vmturbo.topology.processor.GlobalConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Spring configuration for server-side API.
 */
@Configuration
@Import({ TargetConfig.class, GlobalConfig.class, ProbeConfig.class })
public class TopologyProcessorApiConfig {

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Value("${chunk.send.delay.msec:50}")
    private long chunkSendDelayMs;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("tp-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public TopologyProcessorNotificationSender topologyProcessorNotificationSender() {
        final TopologyProcessorNotificationSender backend =
                new TopologyProcessorNotificationSender(apiServerThreadPool(), chunkSendDelayMs);
        targetConfig.targetStore().addListener(backend);
        probeConfig.probeStore().addListener(backend);
        return backend;
    }

    /**
     * This bean configures endpoint to bind it to a specific address (path).
     *
     * @return bean
     */
    @Bean
    public ServerEndpointRegistration apiEndpointRegistration() {
        return new ServerEndpointRegistration(TopologyProcessorClient.WEBSOCKET_PATH,
                topologyProcessorNotificationSender().getWebsocketEndpoint());
    }

}
