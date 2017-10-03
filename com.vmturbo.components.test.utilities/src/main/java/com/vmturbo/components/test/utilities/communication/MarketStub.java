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
import com.vmturbo.components.test.utilities.communication.MarketStub.MarketStubConfig;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;

/**
 * Stub implementation of market service. It is able to receive notifications instead of real
 * market component.
 */
public class MarketStub extends AbstractNotificationSenderStub<MarketStubConfig> {

    private MarketNotificationSender backend;

    public MarketNotificationSender getBackend() {
        return backend;
    }

    @Override
    public void initialize(@Nonnull final ApplicationContext context) {
        super.initialize(context);
        backend = context.getBean(MarketNotificationSender.class);
    }

    @Override
    public Class<MarketStubConfig> getConfiguration() {
        return MarketStubConfig.class;
    }

    @Configuration
    public static class MarketStubConfig extends StubConfiguration {

        @Bean
        public MarketNotificationSender marketApiBackend() {
            return new MarketNotificationSender(threadPool, 10L, topologySender(),
                    notificationSender());
        }

        @Bean
        public ServerEndpointRegistration marketApiEndpointRegistration() {
            return new ServerEndpointRegistration(MarketComponentClient.WEBSOCKET_PATH,
                    transportManager());
        }

        @Bean
        public WebsocketNotificationSender<MarketComponentNotification> notificationSender() {
            return new WebsocketNotificationSender<>(threadPool);
        }

        @Bean
        public WebsocketNotificationSender<MarketComponentNotification> topologySender() {
            return new WebsocketNotificationSender<>(threadPool);
        }

        @Bean
        public WebsocketServerTransportManager transportManager() {
            return BroadcastWebsocketTransportManager.createTransportManager(threadPool,
                    notificationSender(), topologySender());
        }
    }
}
