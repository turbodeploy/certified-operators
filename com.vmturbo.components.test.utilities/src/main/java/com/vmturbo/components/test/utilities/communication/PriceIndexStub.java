package com.vmturbo.components.test.utilities.communication;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

import com.vmturbo.components.test.utilities.communication.ComponentStubHost.StubConfiguration;
import com.vmturbo.components.test.utilities.communication.PriceIndexStub.PriceIndexStubConfig;
import com.vmturbo.priceindex.api.PriceIndexNotificationSender;
import com.vmturbo.priceindex.api.impl.PriceIndexReceiver;

public class PriceIndexStub implements NotificationSenderStub<PriceIndexStubConfig> {

    private PriceIndexNotificationSender backend;

    public PriceIndexNotificationSender getBackend() {
        return backend;
    }

    @Override
    public void initialize(@Nonnull final ApplicationContext context) {
        backend = context.getBean(PriceIndexNotificationSender.class);
    }

    @Override
    public Class<PriceIndexStubConfig> getConfiguration() {
        return PriceIndexStubConfig.class;
    }

    @Configuration
    public static class PriceIndexStubConfig extends StubConfiguration {

        @Bean
        public PriceIndexNotificationSender priceIndexApiBackend() {
            return new PriceIndexNotificationSender(threadPool);
        }

        @Bean
        public ServerEndpointRegistration priceIndexApiEndpointRegistration() {
            return new ServerEndpointRegistration(PriceIndexReceiver.WEBSOCKET_PATH,
                    priceIndexApiBackend().getWebsocketEndpoint());
        }
    }
}
