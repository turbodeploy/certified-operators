package com.vmturbo.topology.processor.stitching;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.stitching.poststitching.DiskCapacityCalculator;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.poststitching.SetCommodityMaxQuantityPostStitchingOperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for stitching classes in the TopologyProcessor.
 */
@Configuration
public class StitchingConfig {

    @Value("${historyHost}")
    private String historyHost;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Value("${maxValuesBackgroundLoadFrequencyMinutes}") // default to 3 hours
    private long maxValuesBackgroundLoadFrequencyMinutes;

    @Value("${maxValuesBackgroundLoadDelayOnInitFailureMinutes}")
    private long maxValuesBackgroundLoadDelayOnInitFailureMinutes;

    @Value("${diskIopsCapacitySsd}")
    private double diskIopsCapacitySsd;

    @Value("${diskIopsCapacity7200Rpm}")
    private double diskIopsCapacity7200Rpm;

    @Value("${diskIopsCapacity10kRpm}")
    private double diskIopsCapacity10kRpm;

    @Value("${diskIopsCapacity15kRpm}")
    private double diskIopsCapacity15kRpm;

    @Value("${diskIopsCapacityVseriesLun}")
    private double diskIopsCapacityVseriesLun;

    @Value("${hybridDiskIopsFactor}")
    private double hybridDiskIopsFactor;

    @Value("${flashAvailableDiskIopsFactor}")
    private double flashAvailableDiskIopsFactor;

    /**
     * No associated @Import because it adds a circular import dependency.
     */
    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Bean
    public StitchingOperationLibrary stitchingOperationLibrary() {
        return new StitchingOperationLibrary();
    }

    @Bean
    public StitchingOperationStore stitchingOperationStore() {
        return new StitchingOperationStore(stitchingOperationLibrary());
    }

    @Bean
    public PreStitchingOperationLibrary preStitchingOperationStore() {
        return new PreStitchingOperationLibrary();
    }

    @Bean
    public Channel historyChannel() {
        return GrpcChannelFactory.newChannelBuilder(historyHost, grpcPort).build();
    }

    @Bean
    public StatsHistoryServiceBlockingStub historyClient() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyChannel());
    }

    @Bean
    public DiskCapacityCalculator diskPropertyCalculator() {
        return new DiskCapacityCalculator(diskIopsCapacitySsd, diskIopsCapacity7200Rpm,
            diskIopsCapacity10kRpm, diskIopsCapacity15kRpm, diskIopsCapacityVseriesLun,
            hybridDiskIopsFactor, flashAvailableDiskIopsFactor);
    }

    @Bean
    public PostStitchingOperationLibrary postStitchingOperationStore() {
        return new PostStitchingOperationLibrary(
            new SetCommodityMaxQuantityPostStitchingOperationConfig(
                historyClient(),
                maxValuesBackgroundLoadFrequencyMinutes,
                maxValuesBackgroundLoadDelayOnInitFailureMinutes), diskPropertyCalculator());
    }

    @Bean
    public StitchingManager stitchingManager() {
        return new StitchingManager(stitchingOperationStore(), preStitchingOperationStore(),
            postStitchingOperationStore(), probeConfig.probeStore(), targetConfig.targetStore());
    }
}
