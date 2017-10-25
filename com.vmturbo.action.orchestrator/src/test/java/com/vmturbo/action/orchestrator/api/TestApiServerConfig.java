package com.vmturbo.action.orchestrator.api;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.action.orchestrator.execution.ActionExecutor;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.EntitySeverityCache;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.api.test.SenderReceiverPair;

/**
 * API server-side Spring configuration.
 */
@Configuration
@EnableWebMvc
public class TestApiServerConfig extends WebMvcConfigurerAdapter {

    @Value("#{environment['" + IntegrationTestServer.FIELD_TEST_NAME + "']}")
    public String testName;

    // START bean definitions for API backend.

    @Bean
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("srv-" + testName + "-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public ActionOrchestratorNotificationSender actionOrchestratorApi() {
        return new ActionOrchestratorNotificationSender(apiServerThreadPool(),
                notificationsChannel());
    }

    @Bean
    public SenderReceiverPair<ActionOrchestratorNotification> notificationsChannel() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }

    // END bean definitions for API backend.

    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(0);
    }

    @Bean
    public ActionStore actionStore() {
        return Mockito.mock(ActionStore.class);
    }

    @Bean
    public EntitySeverityCache entitySeverityCache() {
        return Mockito.mock(EntitySeverityCache.class);
    }

    @Bean
    public ActionExecutor actionExecutor() {
        return Mockito.mock(ActionExecutor.class);
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());
        converters.add(msgConverter);
    }
}
