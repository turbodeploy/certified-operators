package com.vmturbo.repository;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.repository.api.impl.RepositoryNotificationReceiver;

/**
 * Spring configuration for API-related beans
 */
@Configuration
public class RepositoryApiConfig {

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("repository-notify-api-%d")
                        .build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public RepositoryNotificationSender repositoryNotificationSender() {
        return new RepositoryNotificationSender(threadPool());
    }

    /**
     * This bean configures endpoint to bind it to a specific address (path).
     *
     * @return bean
     */
    @Bean
    public ServerEndpointRegistration apiEndpointRegistration() {
        return new ServerEndpointRegistration(RepositoryNotificationReceiver.WEBSOCKET_PATH,
                repositoryNotificationSender().getWebsocketEndpoint());
    }

    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }
}
