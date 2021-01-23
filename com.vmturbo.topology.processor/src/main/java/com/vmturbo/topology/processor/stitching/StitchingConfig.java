package com.vmturbo.topology.processor.stitching;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig;
import com.vmturbo.stitching.poststitching.DiskCapacityCalculator;
import com.vmturbo.stitching.poststitching.SetAutoSetCommodityCapacityPostStitchingOperation.MaxCapacityCache;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.cpucapacity.CpuCapacityConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.RandomEntityStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for stitching classes in the TopologyProcessor.
 */
@Configuration
@Import({ClockConfig.class})
public class StitchingConfig {

    @Value("${historyHost}")
    private String historyHost;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${maxValuesBackgroundLoadFrequencyMinutes:720}") // default to 12 hours
    private long maxValuesBackgroundLoadFrequencyMinutes;

    @Value("${armCapacityRefreshIntervalHours:6}")
    private long armCapacityRefreshIntervalHours;

    @Value("${maxValuesBackgroundLoadDelayOnInitFailureMinutes:30}")
    private long maxValuesBackgroundLoadDelayOnInitFailureMinutes;

    @Value("${diskIopsCapacitySsd:5000}")
    private double diskIopsCapacitySsd;

    @Value("${diskIopsCapacity7200Rpm:800}")
    private double diskIopsCapacity7200Rpm;

    @Value("${diskIopsCapacity10kRpm:1200}")
    private double diskIopsCapacity10kRpm;

    @Value("${diskIopsCapacity15kRpm:1500}")
    private double diskIopsCapacity15kRpm;

    @Value("${diskIopsCapacityVseriesLun:5000}")
    private double diskIopsCapacityVseriesLun;

    @Value("${arrayIopsCapacityFactor:1.0}")
    private double arrayIopsCapacityFactor;

    @Value("${hybridDiskIopsFactor:1.5}")
    private double hybridDiskIopsFactor;

    @Value("${flashAvailableDiskIopsFactor:1.3}")
    private double flashAvailableDiskIopsFactor;

    @Value("${resizeDownWarmUpIntervalHours:4}")
    private double resizeDownWarmUpIntervalHours;

    @Value("${stitchingJournalEnabled:false}")
    private boolean stitchingJournalEnabled;

    @Value("${journalMaxChangesetsPerOperation:100}")
    private int journalMaxChangesetsPerOperation;

    @Value("${journalNumEntitiesToRecord:8}")
    private int journalNumEntitiesToRecord;

    @Value("${journalsPerRecording:6}")
    private int journalsPerRecording;

    @Value("${maxQueryOnTPStartup:true}")
    private boolean maxQueryOnTPStartup;

    /**
     * Feature flag for whether to enable consistent scaling of containers when they are running on
     * nodes with different speeds (see OM-65078).
     */
    @Value("${enableConsistentScalingOnHeterogeneousProviders:false}")
    private boolean enableConsistentScalingOnHeterogeneousProviders;

    @Autowired
    private ClockConfig clockConfig;

    /**
     * No associated @Import because it adds a circular import dependency.
     */
    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private CpuCapacityConfig cpuCapacityConfig;

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
        return ComponentGrpcServer.newChannelBuilder(historyHost, grpcPort).build();
    }

    @Bean
    public StatsHistoryServiceBlockingStub historyClient() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyChannel());
    }

    @Bean
    public DiskCapacityCalculator diskPropertyCalculator() {
        return new DiskCapacityCalculator(diskIopsCapacitySsd, diskIopsCapacity7200Rpm,
            diskIopsCapacity10kRpm, diskIopsCapacity15kRpm, diskIopsCapacityVseriesLun,
            arrayIopsCapacityFactor, hybridDiskIopsFactor, flashAvailableDiskIopsFactor);
    }

    /**
     * Schedules capacity cache refresh.
     *
     * @return The {@link ScheduledExecutorService}.
     */
    @Bean
    public ScheduledExecutorService capacityCacheRefreshSvc() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("max-capacity-cache-refresh-%d")
            .build();
        return Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    /**
     * Cache for max capacities.
     *
     * @return The {@link MaxCapacityCache}.
     */
    @Bean
    public MaxCapacityCache maxCapacityCache() {
        return new MaxCapacityCache(StatsHistoryServiceGrpc.newStub(historyChannel()),
                capacityCacheRefreshSvc(), armCapacityRefreshIntervalHours, TimeUnit.HOURS);
    }

    @Bean
    public PostStitchingOperationLibrary postStitchingOperationStore() {
        PostStitchingOperationLibrary postStitchingOperationLibrary = new PostStitchingOperationLibrary(
            new CommodityPostStitchingOperationConfig(
                historyClient(),
                maxValuesBackgroundLoadFrequencyMinutes,
                maxValuesBackgroundLoadDelayOnInitFailureMinutes,
                maxQueryOnTPStartup),
                diskPropertyCalculator(),
                cpuCapacityConfig.cpucCapacityStore(),
                clockConfig.clock(),
                resizeDownWarmUpIntervalHours,
                maxCapacityCache(),
                enableConsistentScalingOnHeterogeneousProviders);
        maxCapacityCache().initializeFromStitchingOperations(postStitchingOperationLibrary);
        return postStitchingOperationLibrary;
    }

    @Bean
    public StitchingManager stitchingManager() {
        return new StitchingManager(stitchingOperationStore(), preStitchingOperationStore(),
            postStitchingOperationStore(), probeConfig.probeStore(), targetConfig.targetStore(),
                cpuCapacityConfig.cpucCapacityStore());
    }

    @Bean
    public StitchingJournalFactory stitchingJournalFactory() {
        if (stitchingJournalEnabled) {
            return new RandomEntityStitchingJournalFactory(clockConfig.clock(),
                journalNumEntitiesToRecord,
                journalMaxChangesetsPerOperation,
                journalsPerRecording);
        } else {
            return StitchingJournalFactory.emptyStitchingJournalFactory();
        }
    }

    /**
     * Feature flag for whether to enable consistent scaling of containers when they are running on
     * nodes with different speeds (see OM-65078).
     *
     * @return The feature flag value.
     */
    public boolean getEnableConsistentScalingOnHeterogeneousProviders() {
        return enableConsistentScalingOnHeterogeneousProviders;
    }
}
