package com.vmturbo.topology.processor.history;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceStub;
import com.vmturbo.commons.Units;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.history.percentile.PercentileEditor;
import com.vmturbo.topology.processor.history.percentile.PercentileHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.timeslot.TimeSlotEditor;
import com.vmturbo.topology.processor.history.timeslot.TimeslotHistoricalEditorConfig;
import com.vmturbo.topology.processor.topology.HistoryAggregator;

/**
 * Configuration for historical values aggregation sub-package.
 */
@Configuration
@Import({HistoryClientConfig.class, ClockConfig.class, KVConfig.class})
public class HistoryAggregationConfig {
    @Value("${historyAggregationMaxPoolSize}")
    private int historyAggregationMaxPoolSize = Runtime.getRuntime().availableProcessors();

    // TODO dmitry different per-editor values
    @Value("${historyAggregationLoadingChunkSize}")
    private int historyAggregationLoadingChunkSize = 1000;
    @Value("${historyAggregationCalculationChunkSize}")
    private int historyAggregationCalculationChunkSize = 10000;

    @Value("${historyAggregation.percentileEnabled:true}")
    private boolean percentileEnabled = true;
    @Value("${historyAggregation.percentileMaintenanceWindowHours}")
    private int percentileMaintenanceWindowHours = PercentileHistoricalEditorConfig.defaultMaintenanceWindowHours;
    @Value("${historyAggregation.percentileBuckets.VCPU:}")
    private String percentileBucketsVcpu;
    @Value("${historyAggregation.percentileBuckets.VMEM:}")
    private String percentileBucketsVmem;
    @Value("${historyAggregation.percentileBuckets.IMAGE_CPU:}")
    private String percentileBucketsImageCpu;
    @Value("${historyAggregation.percentileBuckets.IMAGE_MEM:}")
    private String percentileBucketsImageMem;
    @Value("${historyAggregation.percentileBuckets.IMAGE_STORAGE:}")
    private String percentileBucketsImageStorage;
    @Value("${historyAggregation.grpcChannelMaxMessageSizeKb:204800}")
    private int grpcChannelMaxMessageSizeKb;
    @Value("${historyAggregation.grpcStreamTimeoutSec:300}")
    private int grpcStreamTimeoutSec;
    @Value("${historyAggregation.blobReadWriteChunkSizeKb:128}")
    private int blobReadWriteChunkSizeKb;

    @Value("${historyAggregation.backgroundLoadingThreshold:5000}")
    private int backgroundLoadingThreshold = 5000;
    @Value("${historyAggregation.backgroundLoadingRetries:3}")
    private int backgroundLoadingRetries = 3;
    @Value("${historyAggregation.backgroundLoadingTimeoutMin:60}")
    private int backgroundLoadingTimeoutMin = 60;

    @Value("${historyAggregation.timeslotEnabled:true}")
    private boolean timeslotEnabled = true;
    @Value("${historyAggregation.timeslotMaintenanceWindowHours:23}")
    private int timeslotMaintenanceWindowHours = TimeslotHistoricalEditorConfig.defaultMaintenanceWindowHours;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private KVConfig kvConfig;

    /**
     * History component blocking client interface.
     *
     * @return history component bean
     */
    @Bean
    public StatsHistoryServiceBlockingStub historyClient() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyClientConfig.historyChannel());
    }

    /**
     * History component non-blocking client interface.
     *
     * @return history component bean
     */
    @Bean
    public StatsHistoryServiceStub nonBlockingHistoryClient() {
        return StatsHistoryServiceGrpc.newStub(historyClientConfig
                        .historyChannelWithMaxMessageSize((int)(grpcChannelMaxMessageSizeKb * Units.KBYTE)));
    }

    /**
     * Thread pool for loading and calculation of history values.
     *
     * @return thread pool
     */
    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService historyAggregationThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("history-aggregation-%d").build();
        return Executors.newFixedThreadPool(historyAggregationMaxPoolSize, threadFactory);
    }

    /**
     * Configuration for timeslot commodities editor.
     *
     * @return editor configuration
     */
    @Bean
    public TimeslotHistoricalEditorConfig timeslotEditorConfig() {
        return new TimeslotHistoricalEditorConfig(historyAggregationLoadingChunkSize,
                                                  historyAggregationCalculationChunkSize,
                                                  backgroundLoadingThreshold,
                                                  backgroundLoadingRetries,
                                                  backgroundLoadingTimeoutMin,
                                                  timeslotMaintenanceWindowHours,
                                                  clockConfig.clock());
    }

    /**
     * Configuration for percentile commodities editor.
     *
     * @return configuration bean
     */
    @Bean
    public PercentileHistoricalEditorConfig percentileEditorConfig() {
        return new PercentileHistoricalEditorConfig(historyAggregationCalculationChunkSize,
                        percentileMaintenanceWindowHours,
                grpcStreamTimeoutSec,
                blobReadWriteChunkSizeKb,
                ImmutableMap.of(CommodityType.VCPU, percentileBucketsVcpu,
                        CommodityType.VMEM, percentileBucketsVmem,
                        CommodityType.IMAGE_CPU, percentileBucketsImageCpu,
                        CommodityType.IMAGE_MEM, percentileBucketsImageMem,
                        CommodityType.IMAGE_STORAGE, percentileBucketsImageStorage),
                kvConfig, clockConfig.clock());
    }

    /**
     * Percentile commodities history editor.
     *
     * @return percentile editor bean
     */
    @Bean
    public IHistoricalEditor<?> percentileHistoryEditor() {
        return new PercentileEditor(percentileEditorConfig(), nonBlockingHistoryClient(),
                clockConfig.clock());
    }

    /**
     * Timeslot commodities history editor.
     *
     * @return timeslot editor bean
     */
    @Bean
    public IHistoricalEditor<?> timeslotHistoryEditor() {
        return new TimeSlotEditor(timeslotEditorConfig(), historyClient());
    }

    /**
     * Historical values aggregation topology pipeline stage.
     *
     * @return pipeline stage bean
     */
    @Bean
    public HistoryAggregator historyAggregationStage() {
        Set<IHistoricalEditor<?>> editors = new HashSet<>();
        if (percentileEnabled) {
            editors.add(percentileHistoryEditor());
        }
        if (timeslotEnabled) {
            editors.add(timeslotHistoryEditor());
        }
        return new HistoryAggregator(historyAggregationThreadPool(), ImmutableSet.copyOf(editors));
    }

}
