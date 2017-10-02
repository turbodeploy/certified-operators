package com.vmturbo.repository;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.repository.listener.MarketTopologyListener;
import com.vmturbo.repository.topology.TopologyEventHandler;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * Configuration for integration with the {@link MarketComponentClient}.
 */
@Configuration
@Import({RepositoryApiConfig.class})
public class MarketConfig {

    @Value("${marketHost}")
    private String marketHost;

    @Value("${server.port}")
    private int httpPort;

    @Autowired
    private RepositoryApiConfig apiConfig;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    /**
     * This field will be set in the constructor with the depedancy injected by Spring.
     */
    TopologyEventHandler topologyEventHandler;

    TopologyProtobufsManager topologyProtobufsManager;

    @Bean
    public MarketTopologyListener MarketTopologyListener(
                final TopologyEventHandler topologyEventHandler,
                final TopologyProtobufsManager topologprotobufsManager) {
        return new MarketTopologyListener(
            topologyEventHandler,
            apiConfig.repositoryNotificationSender(),
            topologprotobufsManager);
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("repository-market-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean(destroyMethod = "close")
    public MarketComponent marketComponent() {
        final ComponentApiConnectionConfig connectionConfig = ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(marketHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build();
        final MarketComponent market =
                MarketComponentClient.rpcAndNotification(connectionConfig, apiServerThreadPool());
        market.addProjectedTopologyListener(MarketTopologyListener(topologyEventHandler, topologyProtobufsManager));
        return market;
    }
}
