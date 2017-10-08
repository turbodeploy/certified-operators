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
import com.vmturbo.components.test.utilities.communication.PriceIndexStub.PriceIndexStubConfig;
import com.vmturbo.market.PriceIndexNotificationSender;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.priceindex.api.impl.PriceIndexReceiver;

public class PriceIndexStub extends AbstractNotificationSenderStub<PriceIndexStubConfig> {

    private PriceIndexNotificationSender backend;

    public PriceIndexNotificationSender getBackend() {
        return backend;
    }

    @Override
    public void initialize(@Nonnull final ApplicationContext context) {
        super.initialize(context);
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
            return new PriceIndexNotificationSender(threadPool, notificationSender());
        }

        @Bean
        public ServerEndpointRegistration priceIndexApiEndpointRegistration() {
            return new ServerEndpointRegistration(PriceIndexReceiver.WEBSOCKET_PATH,
                    transportManager());
        }

        @Bean
        public WebsocketNotificationSender<PriceIndexMessage> notificationSender() {
            return new WebsocketNotificationSender<>(threadPool);
        }

        @Bean
        public WebsocketServerTransportManager transportManager() {
            return BroadcastWebsocketTransportManager.createTransportManager(threadPool,
                    notificationSender());
        }

    }
}
