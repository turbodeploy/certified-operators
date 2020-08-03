package com.vmturbo.topology.processor.scheduling;

import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.SchedulerREST;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * Configuration for the Scheduler package in TopologyProcessor.
 */
@Configuration
@Import({
    OperationConfig.class,
    TopologyConfig.class,
    StitchingConfig.class,
    KVConfig.class
})
public class SchedulerConfig {
    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private StitchingConfig stitchingConfig;

    @Value("${topologyBroadcastIntervalMinutes}")
    private long topologyBroadcastIntervalMinutes;

    @Bean
    public Scheduler scheduler() {
        return new Scheduler(operationConfig.operationManager(),
            targetConfig.targetStore(),
            probeConfig.probeStore(),
            topologyConfig.topologyHandler(),
            kvConfig.keyValueStore(),
            stitchingConfig.stitchingJournalFactory(),
                (name) -> Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat(name)
                            .build()),
                (name) -> Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat(name)
                            .build()),
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("realtime-broadcast-scheduler")
                            .build()),
            Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("target-operations-expiration-scheduler")
                            .build()),
            topologyBroadcastIntervalMinutes
        );
    }

    @Bean
    public ScheduleRpcService scheduleRpcService() {
        return new ScheduleRpcService(scheduler());
    }

    @Bean
    public SchedulerREST.ScheduleServiceController scheduleServiceController() {
        return new SchedulerREST.ScheduleServiceController(scheduleRpcService());
    }

}
