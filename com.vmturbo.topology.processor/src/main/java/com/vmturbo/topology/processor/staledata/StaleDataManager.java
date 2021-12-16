package com.vmturbo.topology.processor.staledata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * we have factories in {@link StaleDataManager#consumerFactories} or return health on demand when asked.
 * </p>
 */
public class StaleDataManager implements StaleDataConsumer, StalenessInformationProvider {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Maximum time to give synchronous health check before timing out.
     */
    private static final long SYNC_HEALTH_CHECK_TIMEOUT_SEC = 10;

    private final List<Supplier<StaleDataConsumer>> consumerFactories;

    private final TargetHealthRetriever targetHealthRetriever;

    private final ScheduledExecutorService executorService;

    private final long staleDataCheckIntervalMs;

    private final ScheduledFuture<?> scheduledFuture;

    private Map<Long, TargetHealth> lastKnownHealth;

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
        // TODO reconsider this concept of subscribers that is not very useful in practice,
        // use not push but pull model - data are typically needed before provider decides to produce them
        this.consumerFactories = new ArrayList<>(consumerFactories.size() + 1);
        this.consumerFactories.addAll(consumerFactories);
        this.consumerFactories.add(() -> this);
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
     * @return health check result
     */
    public boolean checkStaleData() {
        logger.info("Running the StaleDataActiveCheck synchronously");
        try {
            this.executorService.submit(
                            new StaleDataActiveCheck(consumerFactories, targetHealthRetriever))
                            .get(SYNC_HEALTH_CHECK_TIMEOUT_SEC, TimeUnit.SECONDS);
            return true;
        } catch (RejectedExecutionException e) {
            logger.error("StaleDataActiveCheck cannot be scheduled for execution", e);
        } catch (ExecutionException e) {
            logger.error("Failed to execute StaleDataActiveCheck", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            logger.error("Stale data check timed out", e);
        }
        return false;
    }

    @Override
    public TargetHealth getLastKnownTargetHealth(long targetOid) {
        Map<Long, TargetHealth> health;
        synchronized (this) {
            health = lastKnownHealth;
        }
        if (health == null) {
            checkStaleData();
        }
        TargetHealth targetHealth = null;
        synchronized (this) {
            if (lastKnownHealth != null) {
                targetHealth = lastKnownHealth.get(targetOid);
            }
        }
        return targetHealth;
    }

    @Override
    public synchronized void accept(Map<Long, TargetHealth> health) {
        lastKnownHealth = health;
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
