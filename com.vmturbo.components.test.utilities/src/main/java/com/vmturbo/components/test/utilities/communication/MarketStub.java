package com.vmturbo.components.test.utilities.communication;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.vmturbo.components.test.utilities.communication.MarketStub.MarketStubConfig;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost.StubConfiguration;
import com.vmturbo.market.component.api.MarketNotificationSender;
import com.vmturbo.market.component.api.impl.MarketComponentClient;

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

    @Configuration
    public static class MarketStubConfig extends StubConfiguration {

        @Bean
        public MarketNotificationSender marketApiBackend() {
            return new MarketNotificationSender(threadPool, 10L);
        }

        @Bean
        public ServerEndpointRegistration marketApiEndpointRegistration() {
            return new ServerEndpointRegistration(MarketComponentClient.WEBSOCKET_PATH,
                    marketApiBackend().getWebsocketEndpoint());
        }
    }
}
