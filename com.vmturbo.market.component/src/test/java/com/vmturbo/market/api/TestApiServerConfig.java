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

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.component.api.impl.MarketComponentClient;

/**
 * API server-side Spring configuration.
 */
@Configuration
@EnableWebMvc
public class TestApiServerConfig {

    @Value("#{environment['" + IntegrationTestServer.FIELD_TEST_NAME + "']}")
    public String testName;

    @Bean
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

    @Bean
    public MarketNotificationSender marketNotificationSender() {
        return new MarketNotificationSender(apiServerThreadPool(), projectedTopologySender(),
                actionPlanSender());
    }

    @Bean
    public IMessageSender<ActionPlan> actionPlanSender() {
        return new SenderReceiverPair<ActionPlan>();
    }

    @Bean
    public IMessageSender<ProjectedTopology> projectedTopologySender() {
        return new SenderReceiverPair<ProjectedTopology>();
    }

}
