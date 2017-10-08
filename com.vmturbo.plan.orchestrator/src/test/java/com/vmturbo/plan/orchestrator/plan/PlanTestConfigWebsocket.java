package com.vmturbo.plan.orchestrator.plan;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.action.orchestrator.api.PlanOrchestratorDTO.PlanNotification;
import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.components.api.server.BroadcastWebsocketTransportManager;
import com.vmturbo.components.api.server.WebsocketNotificationSender;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientImpl;

/**
 * Spring configuration for tests with websocket started.
 */
@Configuration
public class PlanTestConfigWebsocket extends PlanTestConfig {

    @Bean
    public WebsocketNotificationSender<PlanNotification> notificationSender() {
        return new WebsocketNotificationSender<>(planThreadPool());
    }

    @Bean
    public WebsocketServerTransportManager transportManager() {
        return BroadcastWebsocketTransportManager.createTransportManager(planThreadPool(),
                notificationSender());
    }

    @Override
    @Bean
    public PlanNotificationSender planNotificationSender() {
        return new PlanNotificationSender(planThreadPool(), notificationSender());
    }

    @Bean
    public ServerEndpointRegistration planApiEndpointRegistration() {
        return new ServerEndpointRegistration(PlanOrchestratorClientImpl.WEBSOCKET_PATH,
                transportManager());
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService planThreadPool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("plan-api-%d")
                .build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Override
    @Bean
    public PlanDao planDao() {
        return Mockito.spy(new PlanDaoImpl(dbConfig.dsl(),
                    planNotificationSender(),
                    repositoryClient(),
                    actionServiceClient(),
                    statsServiceClient()));
    }

    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }
}
