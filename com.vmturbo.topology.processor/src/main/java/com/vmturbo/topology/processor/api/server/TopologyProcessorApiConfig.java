package com.vmturbo.topology.processor.api.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.common.health.MessageProducerHealthMonitor;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.GlobalConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Spring configuration for server-side API.
 */
@Configuration
@Import({TargetConfig.class,
        GlobalConfig.class,
        ProbeConfig.class,
        BaseKafkaProducerConfig.class,
        ClockConfig.class})
public class TopologyProcessorApiConfig {

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private GlobalConfig globalConfig;

    @Autowired
    private BaseKafkaProducerConfig baseKafkaServerConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService apiServerThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("tp-api-srv-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    @Bean
    public TopologyProcessorNotificationSender topologyProcessorNotificationSender() {
        final TopologyProcessorNotificationSender backend =
                TopologyProcessorKafkaSender.create(apiServerThreadPool(),
                        baseKafkaServerConfig.kafkaMessageSender(), clockConfig.clock());
        targetConfig.targetStore().addListener(backend);
        probeConfig.probeStore().addListener(backend);
        return backend;
    }

    @Bean
    public MessageProducerHealthMonitor messageProducerHealthMonitor() {
        return new MessageProducerHealthMonitor(baseKafkaServerConfig.kafkaMessageSender());
    }
}
