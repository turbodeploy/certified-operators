package com.vmturbo.common.protobuf.utils;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A utility class helpful for timing of events for profiling purposes.
 * Contains high-resolution timers useful for timing one-off or repeated events.
 * The timers contained are auto-closeable and report their duration on-close.
 * <p/>
 * Usually used within a {@see
 * <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html">
 * try-with-resources block</a>}
 */
public class Timers {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Utility class. Cannnot construct.
     */
    private Timers() {

    }

    /**
     * A simple interface for obtaining the current time in nanos.
     */
    @FunctionalInterface
    public interface NanoTimer {
        /**
         * Get the current system time in nanoseconds.
         *
         * @return the current system time in nanoseconds.
         */
        long nanoTime();
    }

    /**
     * A timer useful for logging the duration of a long-running event.
     * Logs the duration of the event when the event completes.
     * (ie when {@link this#close()} is called).
     */
    public static class LoggingTimer implements AutoCloseable {
        private final String name;
        private final long startTime;
        private final Logger logger;
        private final NanoTimer nanoTimer;

        /**
         * Create a new {@link LoggingTimer}.
         *
         * @param name The name of the event being timed.
         */
        public LoggingTimer(@Nonnull final String name) {
            this.name = Objects.requireNonNull(name);
            this.nanoTimer = System::nanoTime;
            this.startTime = nanoTimer.nanoTime();
            this.logger = Timers.logger;
        }

        /**
         * Create a new {@link LoggingTimer} that logs to a specific timer.
         *
         * @param name The name of the event being timed.
         * @param logger The logger to log to when the event completes.
         * @param nanoTimer The timer to use for getting the current nano time.
         */
        public LoggingTimer(@Nonnull final String name,
                            @Nonnull final Logger logger,
                            @Nonnull final NanoTimer nanoTimer) {
            this.name = Objects.requireNonNull(name);
            this.startTime = nanoTimer.nanoTime();
            this.logger = Objects.requireNonNull(logger);
            this.nanoTimer = nanoTimer;
        }

        @Override
        public void close() {
            logger.info("Time for {}: {}", name, Duration.ofNanos(nanoTimer.nanoTime() - startTime));
        }
    }

    /**
     * A timer useful for timing repeated events that contain sub-phases.
     * Reports the duration of the event being timed to the parent timer when the event completes
     * (ie when {@link this#close()} is called).
     */
    public static class TimingMapTimer implements AutoCloseable {
        private final TimingMap parent;
        private final String name;
        private final long startTime;

        /**
         * Create a new {@link TimingMapTimer}.
         *
         * @param name The name of the event being tied.
         * @param parent The parent {@link TimingMap}.
         * @param nanoTime The time of creation in nanoseconds.
         */
        private TimingMapTimer(@Nonnull final String name, @Nonnull final TimingMap parent,
                               final long nanoTime) {
            this.parent = Objects.requireNonNull(parent);
            this.name = Objects.requireNonNull(name);
            this.startTime = nanoTime;
        }

        @Override
        public void close() {
            parent.addTime(this.name, this.startTime);
        }
    }

    /**
     * A {@link TimingMap} is useful for timing repeated events that consist of several
     * components.
     */
    public static class TimingMap {
        private final Map<String, SubTiming> timings = new HashMap<>();
        private final NanoTimer nanoTimer;

        /**
         * Create a new {@link TimingMap}.
         */
        public TimingMap() {
            nanoTimer = System::nanoTime;
        }

        /**
         * The timer to use for getting the current nano time.
         *
         * @param nanoTimer timer to use for getting the current nano time.
         */
        public TimingMap(@Nonnull final NanoTimer nanoTimer) {
            this.nanoTimer = Objects.requireNonNull(nanoTimer);
        }

        /**
         * Time a sub-event. Use within a try-with-resources block.
         *
         * @param name The name of the sub-event being timed.
         * @return An auto-closeable {@link TimingMapTimer} useful for timing a sub-event. When the timer
         *         closes it reports the duration of the sub-event to the parent TimingMap.
         */
        public TimingMapTimer time(@Nonnull final String name) {
            return new TimingMapTimer(name, this, nanoTimer.nanoTime());
        }

        /**
         * Add a timing to the {@link TimingMap}. For use by {@link TimingMapTimer}s in reporting
         * the duration of sub-events.
         *
         * @param name The name of the event to add.
         * @param startTime The start time of the event to add.
         */
        private void addTime(@Nonnull final String name, final long startTime) {
            final SubTiming st = timings.computeIfAbsent(name, n -> new SubTiming());
            st.addDuration(nanoTimer.nanoTime() - startTime);
        }

        @Override
        public String toString() {
            final long sum = timings.values().stream()
                .mapToLong(SubTiming::totalNanos)
                .sum();
            final long totalIterations = timings.values().stream()
                .mapToLong(SubTiming::totalIterations)
                .sum();
            return timings.entrySet().stream()
                .sorted(Comparator.comparingLong(entry -> -entry.getValue().totalNanos))
                .map(entry -> entry.getKey() + ": " + entry.getValue())
                .collect(Collectors.joining("\n"))
                + String.format("\nTOTAL: Total %s, Iterations: %s, Avg: %s",
                    Duration.ofNanos(sum),
                    totalIterations,
                    totalIterations == 0 ? 0 : Duration.ofNanos(sum).dividedBy(totalIterations));
        }
    }

    /**
     * A sub-timing for an event.
     */
    private static class SubTiming {
        private long totalNanos;
        private long totalIterations;

        private SubTiming() {
        }

        /**
         * Add a duration to this sub-timing.
         *
         * @param durationNanos The duration in nanos to add.
         */
        public void addDuration(long durationNanos) {
            totalNanos += durationNanos;
            totalIterations++;
        }

        @Override
        public String toString() {
            return String.format("Total: %s, Iterations: %s, Avg: %s",
                Duration.ofNanos(totalNanos),
                totalIterations,
                totalIterations == 0 ? 0 : Duration.ofNanos(totalNanos).dividedBy(totalIterations));
        }

        /**
         * Get total nanos.
         *
         * @return total nanos.
         */
        public long totalNanos() {
            return totalNanos;
        }

        /**
         * Get total iterations.
         *
         * @return total iterations.
         */
        public long totalIterations() {
            return totalIterations;
        }
    }
}
