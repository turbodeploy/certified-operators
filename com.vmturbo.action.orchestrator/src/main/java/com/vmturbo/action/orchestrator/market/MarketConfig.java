package com.vmturbo.action.orchestrator.market;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentClient;

/**
 * Configuration for integration with the {@link MarketComponentClient}.
 */
@Configuration
@Import({
    ActionOrchestratorApiConfig.class,
    ActionStoreConfig.class,
    ActionExecutionConfig.class,
})
public class MarketConfig {

    @Value("${marketHost}")
    private String marketHost;

    @Value("${server.port}")
    private int httpPort;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Autowired
    private ActionOrchestratorApiConfig apiConfig;

    @Autowired
    private ActionExecutionConfig executionConfig;

    @Autowired
    private ActionStoreConfig actionStoreConfig;

    @Bean
    public MarketActionListener marketActionListener() {
        return new MarketActionListener(apiConfig.actionOrchestratorNotificationSender(),
            actionStoreConfig.actionStorehouse()
        );
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("action-orchestrator-market-api-srv-%d").build();
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
        market.addActionsListener(marketActionListener());
        return market;
    }
}
