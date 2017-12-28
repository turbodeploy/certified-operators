package com.vmturbo.reporting.api;

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

import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportNotification;

/**
 * Client configuration to connect to reporting component.
 */
@Configuration
@Import({BaseKafkaConsumerConfig.class})
public class ReportingClientConfig {

    @Autowired
    private BaseKafkaConsumerConfig baseConsumerConfig;

    @Value("${reportingHost}")
    private String reportingHost;
    @Value("${server.grpcPort}")
    private int grpcPort;
    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    /**
     * GRPC channel to connect to reporting component's GRPC services.
     *
     * @return GRPC channel bean
     */
    @Bean
    public Channel reportingChannel() {
        return PingingChannelBuilder.forAddress(reportingHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public ExecutorService reportingThreadPool() {
        final ThreadFactory tf =
                new ThreadFactoryBuilder().setNameFormat("reporting-client-%d").build();
        return Executors.newCachedThreadPool(tf);
    }

    @Bean
    public IMessageReceiver<ReportNotification> reportNotificationIMessageReceiver() {
        return baseConsumerConfig.kafkaConsumer()
                .messageReceiver(ReportingNotificationReceiver.REPORT_GENERATED_TOPIC,
                        ReportNotification::parseFrom);
    }

    @Bean
    public ReportingNotificationReceiver reportingNotificationReceiver() {
        return new ReportingNotificationReceiver(reportNotificationIMessageReceiver(),
                reportingThreadPool());
    }
}
