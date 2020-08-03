package com.vmturbo.topology.processor.rpc;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.probe.ProbeDTOREST.ProbeRpcServiceController;
import com.vmturbo.common.protobuf.topology.DiscoveredGroupREST.DiscoveredGroupServiceController;
import com.vmturbo.common.protobuf.topology.StitchingREST.StitchingJournalServiceController;
import com.vmturbo.common.protobuf.topology.TopologyDTOREST;
import com.vmturbo.kvstore.KeyValueStoreConfig;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.communication.SdkServerConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.probes.ProbeRpcService;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.stitching.journal.JournalFilterFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalRpcService;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;
import com.vmturbo.topology.processor.topology.TopologyRpcService;

@Configuration
@Import({
    GroupConfig.class,
    EntityConfig.class,
    IdentityProviderConfig.class,
    ClockConfig.class,
    TopologyConfig.class,
    SchedulerConfig.class,
    ProbeConfig.class,
    TargetConfig.class,
    StitchingConfig.class,
    SdkServerConfig.class,
    OperationConfig.class
})
public class TopologyProcessorRpcConfig {

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private SchedulerConfig schedulerConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private StitchingConfig stitchingConfig;

    @Autowired
    private KeyValueStoreConfig keyValueStoreConfig;

    @Autowired
    private SdkServerConfig sdkServerConfig;

    @Value("${waitForBroadcastTimeoutMin:60}")
    private long waitForBroadcastTimeoutMin;

    @Autowired
    private OperationConfig operationConfig;

    @Bean
    public DiscoveredGroupRpcService discoveredGroupRpcService() {
        return new DiscoveredGroupRpcService(groupConfig.discoveredGroupUploader());
    }

    @Bean
    public TargetSearchRpcService targetSearchRpcService() {
        return new TargetSearchRpcService(targetConfig.targetStore(), probeConfig.probeStore(),
                operationConfig.operationManager());
    }

    /**
     * Discovered groups service controller.
     *
     * @return discovered groups service controller
     */
    @Bean
    public DiscoveredGroupServiceController debugServiceController() {
        return new DiscoveredGroupServiceController(discoveredGroupRpcService());
    }

    @Bean
    public TopologyRpcService topologyRpcService() {
        return new TopologyRpcService(topologyConfig.topologyHandler(),
            topologyConfig.pipelineExecutorService(),
            identityProviderConfig.identityProvider(),
            schedulerConfig.scheduler(),
            stitchingConfig.stitchingJournalFactory(),
            topologyConfig.realtimeTopologyContextId(),
            clockConfig.clock(),
            waitForBroadcastTimeoutMin,
            TimeUnit.MINUTES);
    }

    @Bean
    public JournalFilterFactory journalFilterFactory() {
        return new JournalFilterFactory(probeConfig.probeStore(), targetConfig.targetStore());
    }

    @Bean
    public StitchingJournalRpcService stitchingJournalRpcService() {
        return new StitchingJournalRpcService(
            topologyConfig.topologyHandler(),
            schedulerConfig.scheduler(),
            journalFilterFactory());
    }

    @Bean
    public TopologyDTOREST.TopologyServiceController topologyServiceController() {
        return new TopologyDTOREST.TopologyServiceController(topologyRpcService());
    }

    @Bean
    public StitchingJournalServiceController stitchingJournalServiceController() {
        return new StitchingJournalServiceController(stitchingJournalRpcService());
    }

    @Bean
    public ProbeRpcService probeService() {
        return new ProbeRpcService(targetConfig.probePropertyStore(),
            sdkServerConfig.remoteMediation());
    }

    @Bean
    public ProbeRpcServiceController probeServiceController() {
        return new ProbeRpcServiceController(probeService());
    }
}
