package com.vmturbo.topology.processor.history;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

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
import com.vmturbo.components.common.diagnostics.BinaryDiagsRestorable;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.history.percentile.PercentileEditor;
import com.vmturbo.topology.processor.history.percentile.PercentileHistoricalEditorConfig;
import com.vmturbo.topology.processor.history.percentile.PercentilePersistenceTask;
import com.vmturbo.topology.processor.history.timeslot.TimeSlotEditor;
import com.vmturbo.topology.processor.history.timeslot.TimeSlotLoadingTask;
import com.vmturbo.topology.processor.history.timeslot.TimeslotHistoricalEditorConfig;
import com.vmturbo.topology.processor.topology.HistoryAggregator;

/**
 * Configuration for historical values aggregation sub-package.
 */
@Configuration
@Import({HistoryClientConfig.class, ClockConfig.class, KVConfig.class, TopologyProcessorApiConfig.class})
public class HistoryAggregationConfig {
    @Value("${historyAggregationMaxPoolSize:8}")
    private int historyAggregationMaxPoolSize = Runtime.getRuntime().availableProcessors();

    // TODO dmitry different per-editor values
    @Value("${historyAggregationLoadingChunkSize:1000}")
    private int historyAggregationLoadingChunkSize = 1000;
    @Value("${historyAggregationCalculationChunkSize:2000}")
    private int historyAggregationCalculationChunkSize = 2000;

    @Value("${historyAggregation.percentileEnabled:true}")
    private boolean percentileEnabled = true;
    @Value("${historyAggregation.percentileMaintenanceWindowHours:24}")
    private int percentileMaintenanceWindowHours = PercentileHistoricalEditorConfig.DEFAULT_MAINTENANCE_WINDOW_HOURS;
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
    @Value("${historyAggregation.grpcTimeSlotChannelMaxMessageSizeKb:20480}")
    private int grpcTimeSlotChannelMaxMessageSizeKb;
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
    private int timeslotMaintenanceWindowHours = TimeslotHistoricalEditorConfig.DEFAULT_MAINTENANCE_WINDOW_HOURS;

    @Value("${realtimeTopologyContextId:7777777}")
    private long realtimeTopologyContextId;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private TopologyProcessorApiConfig tpApiConfig;

    /**
     * History component blocking client interface.
     *
     * @return history component bean
     */
    @Bean
    public StatsHistoryServiceBlockingStub historyClient() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyClientConfig
            .historyChannelWithMaxMessageSize((int)(grpcTimeSlotChannelMaxMessageSizeKb * Units.KBYTE)));
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
                                                  realtimeTopologyContextId,
                                                  backgroundLoadingThreshold,
                                                  backgroundLoadingRetries,
                                                  backgroundLoadingTimeoutMin,
                                                  timeslotMaintenanceWindowHours,
                                                  clockConfig.clock(), kvConfig);
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
                        realtimeTopologyContextId,
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
     * @param <E> type of the editor that is going to be created.
     * @return percentile editor bean
     */
    @Bean
    public <E extends IHistoricalEditor<?> & BinaryDiagsRestorable> E percentileHistoryEditor() {
        @SuppressWarnings("unchecked")
        final E result = (E)new PercentileEditor(percentileEditorConfig(),
                        nonBlockingHistoryClient(), clockConfig.clock(),
                        PercentilePersistenceTask::new);
        return result;
    }

    /**
     * Collection of editors which state need to be written to and restored from diagnostics.
     *
     * @return collection of editors that able to save and restore theirs state.
     */
    @Nonnull
    public Collection<BinaryDiagsRestorable> statefulEditors() {
        return Collections.singleton(percentileHistoryEditor());
    }

    /**
     * {@link ExecutorService} instance to do background loading tasks.
     *
     * @return {@link ExecutorService} instance to do background loading tasks.
     */
    @Bean
    protected ExecutorService backgroundHistoryLoadingPool() {
        return Executors.newCachedThreadPool();
    }

    /**
     * Timeslot commodities history editor.
     *
     * @return timeslot editor bean
     */
    @Bean
    public IHistoricalEditor<?> timeslotHistoryEditor() {
        return new TimeSlotEditor(timeslotEditorConfig(), historyClient(),
                        backgroundHistoryLoadingPool(), TimeSlotLoadingTask::new);
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
