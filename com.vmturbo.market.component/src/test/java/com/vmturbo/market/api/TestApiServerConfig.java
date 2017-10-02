package com.vmturbo.market.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.test.IntegrationTestServer;

/**
 * API server-side Spring configuration.
 */
@Configuration
@EnableWebMvc
public class TestApiServerConfig extends MarketApiConfig {

    @Value("#{environment['" + IntegrationTestServer.FIELD_TEST_NAME + "']}")
    public String testName;

    @Bean
    @Override
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("srv-" + testName + "-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
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
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(0);
    }
}
