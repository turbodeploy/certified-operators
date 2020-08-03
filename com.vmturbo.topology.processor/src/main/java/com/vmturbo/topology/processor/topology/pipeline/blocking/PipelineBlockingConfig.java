package com.vmturbo.topology.processor.topology.pipeline.blocking;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * Configuration for the operation to unblock the topology pipeline.
 */
@Configuration
public class PipelineBlockingConfig {

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private SchedulerConfig schedulerConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Value("${enableDiscoveryResponsesCaching:true}")
    private boolean enableDiscoveryResponsesCaching;

    /**
     * The maximum number of failures we will allow for a target with a fast rediscovery interval
     * before allowing broadcasts.
     */
    @Value("${startupDiscovery.failureThreshold.fastRediscovery:3}")
    private int fastRediscoveryFailureThreshold;

    /**
     * The maximum number of failures we will allow for a target with a slow rediscovery interval
     * before allowing broadcasts.
     */
    @Value("${startupDiscovery.failureThreshold.slowRediscovery:1}")
    private int slowRediscoveryFailureThreshold;

    /**
     * The boundary between "fast" and "slow" rediscovery intervals. Anything less than this amount
     * will be considered fast.
     */
    @Value("${startupDiscovery.failureThreshold.fastToSlowBoundaryMin:15}")
    private int fastRediscoveryIntervalBoundary;

    /**
     * How long we will wait to successfully discover targets at startup before allowing broadcasts.
     */
    @Value("${startupDiscovery.maxDiscoveryWaitMins:360}")
    private long startupDiscoveryMaxDiscoveryWaitMinutes;

    /**
     * How long we will wait for a target's probe to register and a discovery to start.
     *
     * <p/>It is used to control how we unblock the initial broadcast after the topology processor
     * starts up. Some targets may not have associated probes in the deployment anymore, and will
     * never have successful/failed discoveries. We don't want to wait for those targets.
     */
    @Value("${startupDiscovery.maxProbeRegistrationWaitMins:10}")
    private long maxProbeRegistrationWaitMins;

    /**
     * The type of pipeline unblocking operation to use at startup.
     * See {@link PipelineBlockingConfig#pipelineUnblockLauncher()} for valid types.
     */
    @Value("${pipelineUnblockType:discovery}")
    private String pipelineUnblockType;


    /**
     * Factory for discovery unblocking.
     *
     * @return The {@link PipelineUnblockLauncher}.
     */
    @Bean
    PipelineUnblockLauncher pipelineUnblockLauncher() {
        return new PipelineUnblockLauncher(pipelineUnblock(), targetConfig.targetStore());
    }

    @Bean
    PipelineUnblock pipelineUnblock() {
        switch (pipelineUnblockType) {
            case "immediate":
                return new ImmediateUnblock(topologyConfig.pipelineExecutorService());
            case "discovery": default:
                final TargetShortCircuitSpec shortCircuitSpec = TargetShortCircuitSpec.newBuilder()
                        .setFastRediscoveryThreshold(fastRediscoveryFailureThreshold)
                        .setSlowRediscoveryThreshold(slowRediscoveryFailureThreshold)
                        .setFastSlowBoundary(fastRediscoveryIntervalBoundary, TimeUnit.MINUTES)
                        .build();
                return new DiscoveryBasedUnblock(topologyConfig.pipelineExecutorService(),
                        targetConfig.targetStore(),
                        probeConfig.probeStore(),
                        schedulerConfig.scheduler(),
                        operationConfig.operationManager(),
                        shortCircuitSpec,
                        startupDiscoveryMaxDiscoveryWaitMinutes,
                        maxProbeRegistrationWaitMins,
                        TimeUnit.MINUTES,
                        clockConfig.clock(),
                        identityProviderConfig.identityProvider(),
                        operationConfig.binaryDiscoveryDumper(),
                        enableDiscoveryResponsesCaching);
        }
    }
}
