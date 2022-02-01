package com.vmturbo.sql.utils.jooq;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The event for scheduling purge procedures.
 */
public class PurgeEvent {

    private static final Logger logger = LogManager.getLogger();

    private final String name;
    private final Runnable procedure;
    private final ScheduledExecutorService scheduler;

    /**
     * Constructor for the purge event.
     *
     * @param name name of the event
     * @param procedure operation to perform
     */
    public PurgeEvent(@Nonnull String name, @Nonnull Runnable procedure) {
        this.name = name;
        this.procedure = procedure;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(name).build());
    }

    /**
     * Schedule the procedure to run every periodHours, starting at the given startHourOfDay.
     *
     * @param startHourOfDay hour of the day to start running the procedure
     * @param periodHours period between runs
     * @return this
     */
    public PurgeEvent schedule(int startHourOfDay, long periodHours) {
        final LocalDateTime currentDateTime = getCurrentDateTime();
        final LocalDateTime firstExecutionTime = currentDateTime.toLocalDate().plusDays(1)
                .atStartOfDay().plusHours(startHourOfDay);
        long initialDelaySeconds = currentDateTime.until(firstExecutionTime, ChronoUnit.SECONDS);
        long periodSeconds = TimeUnit.SECONDS.convert(periodHours, TimeUnit.HOURS);
        getScheduler().scheduleAtFixedRate(procedure, initialDelaySeconds, periodSeconds, TimeUnit.SECONDS);

        logger.info("Scheduled {} to run every {} hours starting at {}", this.name, periodHours,
                firstExecutionTime);
        return this;
    }

    @VisibleForTesting
    public Runnable getProcedure() {
        return procedure;
    }

    @VisibleForTesting
    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    @VisibleForTesting
    public LocalDateTime getCurrentDateTime() {
        return LocalDateTime.now();
    }
}
