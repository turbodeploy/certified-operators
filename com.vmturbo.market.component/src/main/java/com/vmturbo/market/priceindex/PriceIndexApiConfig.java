package com.vmturbo.market.priceindex;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.priceindex.api.PriceIndexNotificationSender;
import com.vmturbo.priceindex.api.impl.PriceIndexNotificationReceiver;

/**
 * Spring configuration to provide the {@link com.vmturbo.priceindex.api}
 * integration.
 */
@Configuration
@Import(BaseKafkaProducerConfig.class)
public class PriceIndexApiConfig {

    @Autowired
    BaseKafkaProducerConfig kafkaProducerConfig;

    /**
     * Constructs the sender thread pool.
     * Requires 1 thread.
     *
     * @return The sender single-threaded thread pool.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiSenderThreadPool() {
        // Requires more than 1 thread in order to work (proven empirically).
        return Executors.newCachedThreadPool(threadFactory());
    }

    @Bean
    public ThreadFactory threadFactory() {
        return new ThreadFactoryBuilder().setNameFormat("priceindex-api-sender-%d").build();
    }

    /**
     * Constructs the sender backend.
     *
     * @return The Sender API backend.
     */
    @Bean
    public PriceIndexNotificationSender priceIndexNotificationSender() {
        return new PriceIndexNotificationSender(apiSenderThreadPool(), priceIndexMessageSender());
    }

    /**
     * Create a wrapper around a kafka producer that sends PriceIndexMessage objects
     * @return a kafka producer wrapper object to send price indices with
     */
    @Bean
    public IMessageSender<PriceIndexMessage> priceIndexMessageSender() {
        return kafkaProducerConfig.kafkaMessageSender().messageSender(PriceIndexNotificationReceiver.PRICE_INDICES_TOPIC);
    }

}
