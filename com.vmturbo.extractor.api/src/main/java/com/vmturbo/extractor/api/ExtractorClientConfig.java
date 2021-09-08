package com.vmturbo.extractor.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;

/**
 * Client config for extractor.
 */
@Configuration
@Lazy
@Import({BaseKafkaConsumerConfig.class})
public class ExtractorClientConfig {

    @Value("${extractorHost:extractor}")
    private String extractorHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Autowired
    private BaseKafkaConsumerConfig baseKafkaConsumerConfig;

    /**
     * Make up channel to extractor.
     *
     * @return Channel to extractor.
     */
    @Bean
    public Channel extractorChannel() {
        return ComponentGrpcServer.newChannelBuilder(extractorHost, grpcPort)
                .keepAliveTime(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .build();
    }

    @Bean(destroyMethod = "shutdownNow")
    ExecutorService extractorClientThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("extractor-client-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }
}
