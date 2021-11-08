package com.vmturbo.topology.processor.staledata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.topology.processor.rpc.TargetHealthRetriever;

/**
 * Class that handles the scheduling of recurrent stale data checks.
 *
 * <p>This class contains a scheduler that will periodically run the {@link StaleDataActiveCheck}
 * every {@link StaleDataManager#staleDataCheckIntervalMs} milliseconds. The periodic task will
 * forward the health states coming from {@link TargetHealthRetriever} to any consumers for which
 * we have factories in {@link StaleDataManager#consumerFactories}.
 * </p>
 */
public class StaleDataManager {

    private static final Logger logger = LogManager.getLogger();

    private final List<Supplier<StaleDataConsumer>> consumerFactories;

    private final TargetHealthRetriever targetHealthRetriever;

    private final ScheduledExecutorService executorService;

    private final long staleDataCheckIntervalMs;

    private final ScheduledFuture<?> scheduledFuture;

    /**
     * Creates an instance of a {@link StaleDataManager}.
     *
     * @param consumerFactories - list of factories for consumers of the health results
     * @param targetHealthRetriever - reference to the health retriever
     * @param executor - executor service to schedule active checks
     * @param staleDataCheckIntervalMs - frequency of the active check in milliseconds
     */
    public StaleDataManager(@Nonnull List<Supplier<StaleDataConsumer>> consumerFactories,
            @Nonnull TargetHealthRetriever targetHealthRetriever,
            @Nonnull ScheduledExecutorService executor, long staleDataCheckIntervalMs) {
        this.consumerFactories = consumerFactories;
        this.targetHealthRetriever = targetHealthRetriever;
        this.executorService = executor;
        this.staleDataCheckIntervalMs = staleDataCheckIntervalMs;
        this.scheduledFuture = initialize();
    }

    @Nonnull
    private ScheduledFuture<?> initialize() {
        logger.info("Initializing Stale Data active check to run every {} minutes",
                TimeUnit.MILLISECONDS.toMinutes(staleDataCheckIntervalMs));

        return this.executorService.scheduleWithFixedDelay(
                new StaleDataActiveCheck(consumerFactories, targetHealthRetriever),
                staleDataCheckIntervalMs, staleDataCheckIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Nonnull
    public ScheduledFuture<?> getScheduledFuture() {
        return scheduledFuture;
    }

    /**
     * Run the active check immediately, producing all notifications the consumers may produce.
     *
     * @return true if the process succeeded
     */
    public boolean notifyImmediately() {
        logger.info("Running the StaleDataActiveCheck asynchronously");
        try {
            this.executorService.submit(
                    new StaleDataActiveCheck(consumerFactories, targetHealthRetriever));
            return true;
        } catch (RejectedExecutionException e) {
            logger.error("StaleDataActiveCheck cannot be scheduled for execution", e);
            return false;
        } catch (RuntimeException e) {
            logger.error("Unexpected exception in when executing the StaleDataActiveCheck", e);
            return false;
        }
    }

    /**
     * The active check task to be scheduled by the {@link StaleDataManager}.
     *
     * <p>This task gets the health of all targets and forwards it to all of the {@link
     * StaleDataConsumer}s registered.</p>
     */
    private static class StaleDataActiveCheck implements Runnable {

        private final List<Supplier<StaleDataConsumer>> consumerFactories;

        private final TargetHealthRetriever targetHealthRetriever;

        private StaleDataActiveCheck(@Nonnull List<Supplier<StaleDataConsumer>> consumerFactories,
                @Nonnull TargetHealthRetriever targetHealthRetriever) {
            this.consumerFactories = consumerFactories;
            this.targetHealthRetriever = targetHealthRetriever;
        }

        @Override
        public void run() {
            Map<Long, TargetHealth> targetToTargetHealth = new HashMap<>();
            try {
                logger.debug("The StaleDataActiveCheck is running");
                targetToTargetHealth =
                        this.targetHealthRetriever.getTargetHealth(Collections.emptySet(), true);
            } catch (Exception e) {
                logger.error("StaleDataActiveCheck failed get target health data due to ", e);
            }

            try {
                // first group all the targets that have TargetHealth == null and produce a warning
                logWarningForTargetsWithHealthNull(targetToTargetHealth);

                // then remove all nulls and create an ImmutableMap to pass to the consumers
                ImmutableMap<Long, TargetHealth> healthResultsToPush =
                        ImmutableMap.copyOf(targetToTargetHealth
                                .entrySet()
                                .stream()
                                .filter(e -> Objects.nonNull(e.getValue()))
                                .collect(Collectors.toMap(Entry::getKey, Entry::getValue))
                        );
                this.consumerFactories.parallelStream().map(Supplier::get).forEach(consumer -> {
                    try {
                        consumer.accept(healthResultsToPush);
                    } catch (Exception e) {
                        logger.error("Consumer {} failed due to ", consumer.getClass(), e);
                    }
                });
            } catch (Exception e) {
                logger.error("StaleDataActiveCheck failed to push to consumers due to ", e);
            }
        }

        private void logWarningForTargetsWithHealthNull(
                Map<Long, TargetHealth> targetToTargetHealth) {
            String targetIdsWithNullHealth = targetToTargetHealth
                    .entrySet()
                    .stream()
                    .filter(e -> Objects.isNull(e.getValue()))
                    .map(Entry::getKey)
                    .map(Object::toString)
                    .collect(Collectors.joining(","));

            if (!targetIdsWithNullHealth.isEmpty()) {
                logger.warn("Received null values in target health for targets: [{}]",
                        targetIdsWithNullHealth);
            }
        }
    }
}
