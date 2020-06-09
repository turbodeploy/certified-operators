package com.vmturbo.auth.api.licensing;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.auth.api.AuthClientConfig;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;

/**
 * Configures a {@link LicenseCheckClient} for use in your component.
 */
@Configuration
@Lazy
@Import({BaseKafkaConsumerConfig.class, AuthClientConfig.class})
public class LicenseCheckClientConfig {
    @Autowired
    private BaseKafkaConsumerConfig kafkaConsumerConfig;

    @Autowired
    private AuthClientConfig authClientConfig;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService threadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("license-check-client-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public IMessageReceiver<LicenseSummary> licenseSummaryConsumer() {
        return kafkaConsumerConfig.kafkaConsumer()
                .messageReceiverWithSettings(
                        new TopicSettings(LicenseCheckClient.LICENSE_SUMMARY_TOPIC, StartFrom.LAST_COMMITTED),
                        LicenseSummary::parseFrom);
    }

    @Bean
    public LicenseCheckClient licenseCheckClient() {
        return new LicenseCheckClient(licenseSummaryConsumer(), threadPool(),
                authClientConfig.authClientChannel());
    }
}
