package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost.StubConfiguration;
import com.vmturbo.components.test.utilities.communication.MarketStub.MarketStubConfig;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.component.api.impl.MarketComponentClient;

/**
 * Stub implementation of market service. It is able to receive notifications instead of real
 * market component.
 */
public class MarketStub implements NotificationSenderStub<MarketStubConfig> {

    private MarketNotificationSender backend;

    public MarketNotificationSender getBackend() {
        return backend;
    }

    @Override
    public void initialize(@Nonnull final ApplicationContext context) {
        backend = context.getBean(MarketNotificationSender.class);
    }

    @Override
    public Class<MarketStubConfig> getConfiguration() {
        return MarketStubConfig.class;
    }

    @Override
    public void waitForEndpoints(int numOfEndpoints, long timeout, @Nonnull TimeUnit timeUnit)
            throws InterruptedException, TimeoutException {
        // no-op
    }

    @Configuration
    public static class MarketStubConfig extends StubConfiguration {

        @Bean
        public MarketNotificationSender marketApiBackend() {
            return new MarketNotificationSender(threadPool, topologySender(),
                    actionPlanSender());
        }

        @Bean
        public KafkaMessageProducer messageSender() {
            return new KafkaMessageProducer(DockerEnvironment.getDockerHostName() + ":" +
                    Integer.toString(DockerEnvironment.KAFKA_EXTERNAL_PORT));
        }

        @Bean
        public IMessageSender<ActionPlan> actionPlanSender() {
            return messageSender().messageSender(MarketComponentClient.ACTION_PLANS_TOPIC);
        }

        @Bean
        public IMessageSender<ProjectedTopology> topologySender() {
            return messageSender().messageSender(MarketComponentClient.PROJECTED_TOPOLOGIES_TOPIC);
        }
    }
}
