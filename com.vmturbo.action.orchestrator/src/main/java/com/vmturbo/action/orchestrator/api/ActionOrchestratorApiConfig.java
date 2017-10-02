package com.vmturbo.action.orchestrator.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClient;

/**
 * Spring configuration to provide the {@link com.vmturbo.market.component.api.MarketComponent} integration.
 */
@Configuration
public class ActionOrchestratorApiConfig {

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("action-orchestrator-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public ActionOrchestratorNotificationSender actionOrchestratorNotificationSender() {
        return new ActionOrchestratorNotificationSender(apiServerThreadPool());
    }

    /**
     * This bean configures endpoint to bind it to a specific address (path).
     *
     * @return bean
     */
    @Bean
    public ServerEndpointRegistration apiEndpointRegistration() {
        return new ServerEndpointRegistration(ActionOrchestratorClient.WEBSOCKET_PATH,
                actionOrchestratorNotificationSender().getWebsocketEndpoint());
    }

}
