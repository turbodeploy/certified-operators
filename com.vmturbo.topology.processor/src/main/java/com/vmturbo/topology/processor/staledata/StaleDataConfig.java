package com.vmturbo.topology.processor.staledata;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for the Stale Data package in Topology Processor.
 */
@Configuration
public class StaleDataConfig {
    @Value("${staleDataCheckFrequencyMinutes:5}")
    private int staleDataCheckFrequencyMinutes;

    /**
     * List of {@link Supplier}s for all the {@link StaleDataConsumer}s that should be used for the
     * active check.
     *
     * @return the consumer factories.
     */
    @Bean
    @Nonnull
    public List<Supplier<StaleDataConsumer>> staleDataConsumerFactories() {
        return ImmutableList.of(StaleDataLoggingConsumer::new);
    }

    /**
     * Single threaded executor service with thread named 'stale-data-task' to use as a scheduler.
     *
     * @return the executor service
     */
    @Bean
    @Nonnull
    public ScheduledExecutorService executorService() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("stale-data-task").build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

    public int getStaleDataCheckFrequencyMinutes() {
        return staleDataCheckFrequencyMinutes;
    }
}
