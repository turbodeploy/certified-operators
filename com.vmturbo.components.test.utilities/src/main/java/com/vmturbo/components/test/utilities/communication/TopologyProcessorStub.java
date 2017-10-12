package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost.StubConfiguration;
import com.vmturbo.components.test.utilities.communication.TopologyProcessorStub.TopologyProcessorStubConfig;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;

/**
 * Imitates the {@link TopologyProcessorStub} by running an instance of the
 * {@link TopologyProcessorNotificationSender} in a Tomcat server. The component under test
 * connects to this backend instead of the real topology processor, and the test writer
 * can then send topology broadcasts etc.
 */
public class TopologyProcessorStub implements  NotificationSenderStub<TopologyProcessorStubConfig> {

    /**
     * The actual backend. It's initialized in the Spring configuration.
     */
    private TopologyProcessorNotificationSender backend;

    public TopologyProcessorNotificationSender getBackend() {
        return backend;
    }

    @Override
    public void initialize(@Nonnull final ApplicationContext context) {
        backend = context.getBean(TopologyProcessorNotificationSender.class);
    }

    @Override
    public Class<TopologyProcessorStubConfig> getConfiguration() {
        return TopologyProcessorStubConfig.class;
    }

    @Override
    public void waitForEndpoints(int numOfEndpoints, long timeout, @Nonnull TimeUnit timeUnit)
            throws InterruptedException, TimeoutException {
        // NOOP
    }

    @Configuration
    public static class TopologyProcessorStubConfig extends StubConfiguration {

        @Bean
        public TopologyProcessorNotificationSender topologyProcessorApiBackend() {
            return new TopologyProcessorNotificationSender(threadPool, notificationSender(),
                    topologySender());
        }

        @Bean
        public KafkaMessageProducer messageSender() {
            return new KafkaMessageProducer(DockerEnvironment.getDockerHostName() + ":" +
                    Integer.toString(DockerEnvironment.KAFKA_EXTERNAL_PORT));
        }

        @Bean
        public IMessageSender<TopologyProcessorNotification> topologySender() {
            return messageSender().messageSender(TopologyProcessorClient.NOTIFICATIONS_TOPIC);
        }

        @Bean
        public IMessageSender<Topology> notificationSender() {
            return messageSender().messageSender(TopologyProcessorClient.TOPOLOGY_BROADCAST_TOPIC);
        }
    }
}
