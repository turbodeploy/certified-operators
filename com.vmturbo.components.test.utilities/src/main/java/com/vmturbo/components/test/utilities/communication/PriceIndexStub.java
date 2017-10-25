package com.vmturbo.components.test.utilities.communication;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost.StubConfiguration;
import com.vmturbo.components.test.utilities.communication.PriceIndexStub.PriceIndexStubConfig;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.priceindex.api.PriceIndexNotificationSender;
import com.vmturbo.priceindex.api.impl.PriceIndexNotificationReceiver;

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

    @Override
    public void waitForEndpoints(final int numOfEndpoints, final long timeout,
                                 @Nonnull final TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        // no op
    }

    @Configuration
    public static class PriceIndexStubConfig extends StubConfiguration {

        @Bean
        public PriceIndexNotificationSender priceIndexApiBackend() {
            return new PriceIndexNotificationSender(threadPool, priceIndexSender());
        }

        @Bean
        public KafkaMessageProducer kafkaMessageProducer() {
            return new KafkaMessageProducer(DockerEnvironment.getKafkaBootstrapServers());
        }

        @Bean
        public IMessageSender<PriceIndexMessage> priceIndexSender() {
            return kafkaMessageProducer().messageSender(PriceIndexNotificationReceiver.PRICE_INDICES_TOPIC);
        }

    }
}
