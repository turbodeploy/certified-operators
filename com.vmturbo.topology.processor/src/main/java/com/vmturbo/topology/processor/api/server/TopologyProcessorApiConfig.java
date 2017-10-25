package com.vmturbo.topology.processor.api.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.topology.processor.GlobalConfig;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Spring configuration for server-side API.
 */
@Configuration
@Import({TargetConfig.class, GlobalConfig.class, ProbeConfig.class, BaseKafkaProducerConfig.class})
public class TopologyProcessorApiConfig {

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private BaseKafkaProducerConfig baseKafkaServerConfig;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("tp-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public IMessageSender<Topology> topologySender() {
        return baseKafkaServerConfig.kafkaMessageSender()
                .messageSender(TopologyProcessorClient.TOPOLOGY_BROADCAST_TOPIC);
    }

    @Bean
    public IMessageSender<TopologyProcessorNotification> notificationSender() {
        return baseKafkaServerConfig.kafkaMessageSender()
                .messageSender(TopologyProcessorClient.NOTIFICATIONS_TOPIC);
    }

    @Bean
    public TopologyProcessorNotificationSender topologyProcessorNotificationSender() {
        final TopologyProcessorNotificationSender backend =
                TopologyProcessorKafkaSender.create(apiServerThreadPool(),
                        baseKafkaServerConfig.kafkaMessageSender());
        targetConfig.targetStore().addListener(backend);
        probeConfig.probeStore().addListener(backend);
        return backend;
    }
}
