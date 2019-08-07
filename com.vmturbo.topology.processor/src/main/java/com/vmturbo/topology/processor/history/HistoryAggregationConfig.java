package com.vmturbo.topology.processor.history;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.topology.processor.history.histutil.HistUtilizationEditor;
import com.vmturbo.topology.processor.history.maxvalue.MaxValueEditor;
import com.vmturbo.topology.processor.history.percentile.PercentileEditor;
import com.vmturbo.topology.processor.history.systemload.SystemLoadEditor;
import com.vmturbo.topology.processor.history.timeslot.TimeSlotEditor;
import com.vmturbo.topology.processor.topology.HistoryAggregationStage;

/**
 * Configuration for historical values aggregation sub-package.
 * TODO dmitry add to parent config when ready
 */
@Configuration
@Import({HistoryClientConfig.class})
public class HistoryAggregationConfig {
    @Value("${historyAggregationMaxPoolSize}")
    private int historyAggregationMaxPoolSize = Runtime.getRuntime().availableProcessors();

    // TODO dmitry different per-editor values
    @Value("${historyAggregationLoadingChunkSize}")
    private int historyAggregationLoadingChunkSize = 1000;
    @Value("${historyAggregationCalculationChunkSize}")
    private int historyAggregationCalculationChunkSize = 10000;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Bean
    public StatsHistoryServiceBlockingStub historyClient() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyClientConfig.historyChannel());
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService historyAggregationThreadPool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("history-aggregation-%d").build();
        return Executors.newFixedThreadPool(historyAggregationMaxPoolSize, threadFactory);
    }

    // TODO dmitry different per-editor-type configs from policies (observation windows, weights, etc)
    @Bean
    public CachingHistoricalEditorConfig historicalEditorConfig() {
        return new CachingHistoricalEditorConfig(historyAggregationLoadingChunkSize,
                                                 historyAggregationCalculationChunkSize);
    }

    @Bean
    public IHistoricalEditor<?> histUtilizationHistoryEditor() {
        return new HistUtilizationEditor(historicalEditorConfig(), historyClient());
    }

    @Bean
    public IHistoricalEditor<?> maxValuesHistoryEditor() {
        return new MaxValueEditor(historicalEditorConfig(), historyClient());
    }

    @Bean
    public IHistoricalEditor<?> percentileHistoryEditor() {
        return new PercentileEditor(historicalEditorConfig(), historyClient());
    }

    @Bean
    public IHistoricalEditor<?> timeSlotHistoryEditor() {
        return new TimeSlotEditor(historicalEditorConfig(), historyClient());
    }

    @Bean
    public IHistoricalEditor<?> systemLoadHistoryEditor() {
        return new SystemLoadEditor(null, historyClient());
    }

    @Bean
    public HistoryAggregationStage historyAggregationStage() {
        return new HistoryAggregationStage(historyAggregationThreadPool(), ImmutableSet
                        .of(histUtilizationHistoryEditor(), maxValuesHistoryEditor(),
                            percentileHistoryEditor(), timeSlotHistoryEditor(),
                            systemLoadHistoryEditor()));
    }
}
