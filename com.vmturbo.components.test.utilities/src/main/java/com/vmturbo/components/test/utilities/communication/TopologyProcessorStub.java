package com.vmturbo.components.test.utilities.communication;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.components.api.server.BroadcastWebsocketTransportManager;
import com.vmturbo.components.api.server.WebsocketNotificationSender;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost.StubConfiguration;
import com.vmturbo.components.test.utilities.communication.TopologyProcessorStub.TopologyProcessorStubConfig;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;

/**
 * Imitates the {@link TopologyProcessorStub} by running an instance of the
 * {@link TopologyProcessorNotificationSender} in a Tomcat server. The component under test
 * connects to this backend instead of the real topology processor, and the test writer
 * can then send topology broadcasts etc.
 */
public class TopologyProcessorStub extends
        AbstractNotificationSenderStub<TopologyProcessorStubConfig> {

    /**
     * The actual backend. It's initialized in the Spring configuration.
     */
    private TopologyProcessorNotificationSender backend;

    public TopologyProcessorNotificationSender getBackend() {
        return backend;
    }

    @Override
    public void initialize(@Nonnull final ApplicationContext context) {
        super.initialize(context);
        backend = context.getBean(TopologyProcessorNotificationSender.class);
    }

    @Override
    public Class<TopologyProcessorStubConfig> getConfiguration() {
        return TopologyProcessorStubConfig.class;
    }

    @Configuration
    public static class TopologyProcessorStubConfig extends StubConfiguration {

        @Bean
        public TopologyProcessorNotificationSender topologyProcessorApiBackend() {
            return new TopologyProcessorNotificationSender(threadPool, 10L, topologySender(),
                    notificationSender());
        }

        @Bean
        public ServerEndpointRegistration topologyApiEndpointRegistration() {
            return new ServerEndpointRegistration(TopologyProcessorClient.WEBSOCKET_PATH,
                    transportManager());
        }

        @Bean
        public WebsocketNotificationSender<TopologyProcessorNotification> topologySender() {
            return new WebsocketNotificationSender<>(threadPool);
        }

        @Bean
        public WebsocketNotificationSender<TopologyProcessorNotification> notificationSender() {
            return new WebsocketNotificationSender<>(threadPool);
        }

        @Bean
        public WebsocketServerTransportManager transportManager() {
            return BroadcastWebsocketTransportManager.createTransportManager(threadPool,
                    topologySender(), notificationSender());
        }
    }
}
