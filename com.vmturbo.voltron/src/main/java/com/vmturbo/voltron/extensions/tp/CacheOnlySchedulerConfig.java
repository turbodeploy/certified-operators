package com.vmturbo.voltron.extensions.tp;

import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.rpc.TopologyProcessorRpcConfig;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * A SchedulerConfig that returns a {@link CacheOnlyScheduler} instance.
 */
@Configuration
@Primary
public class CacheOnlySchedulerConfig extends SchedulerConfig  {

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private CacheOnlyOperationConfig operationConfig;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private StitchingConfig stitchingConfig;

    @Autowired
    private TopologyProcessorRpcConfig topologyProcessorRpcConfig;

    @Value("${topologyBroadcastIntervalMinutes:10}")
    private long topologyBroadcastIntervalMinutes;

    @Value("${numDiscoveriesMissedBeforeLogging:10}")
    private int numDiscoveriesMissedBeforeLogging;

    @Override
    @Bean
    public Scheduler scheduler() {
        CacheOnlyScheduler scheduler = new CacheOnlyScheduler(operationConfig.operationManager(),
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
                topologyBroadcastIntervalMinutes,
                numDiscoveriesMissedBeforeLogging
        );

        // Needed to inject the scheduler into the target health retriever so that we can
        // use the broadcast schedule information for each target when calculating the target
        // data health
        topologyProcessorRpcConfig.targetHealthRetriever().setScheduler(scheduler);
        return scheduler;
    }
}
