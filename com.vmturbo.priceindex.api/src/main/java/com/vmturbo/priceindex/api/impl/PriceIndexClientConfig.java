package com.vmturbo.priceindex.api.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;

@Configuration
@Lazy
@Import(BaseKafkaConsumerConfig.class)
public class PriceIndexClientConfig {
    @Autowired
    private BaseKafkaConsumerConfig kafkaConsumerConfig;

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService priceIndexClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("priceindex-api-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    protected IMessageReceiver<PriceIndexMessage> priceIndexClientMessageReceiver() {
        return kafkaConsumerConfig.kafkaConsumer().messageReceiver(PriceIndexNotificationReceiver.PRICE_INDICES_TOPIC,PriceIndexMessage::parseFrom);
    }

    @Bean
    public PriceIndexNotificationReceiver priceIndexReceiver() {
        return new PriceIndexNotificationReceiver(priceIndexClientMessageReceiver(),priceIndexClientThreadPool());
    }
}
