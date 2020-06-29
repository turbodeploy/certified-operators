package com.vmturbo.components.common.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

/**
 * Timer utility for use in processing pipelines that are completed in chunks.
 *
 * <p>Stages are identified by names. Activating a stage opens a new timing segment for that stage,
 * and automatically closes whatever stage, if any, was active at that time. Thus, when processing
 * a chunk, one can activate stages as the processing progresses. Then the process is repeated on
 * the next chunk.</p>
 *
 * <p>N.B. This class does not handle parallel chunk processing. That would probably be a very
 * useful enhancement.</p>
 *
 * <p>The timer is capable of logging a variety of timing reports. Most typically, the STAGE_SUMMARY
 * style is used at the end of pipeline processing. It sums the accumulated segments for each stage
 * and reports the total time spent for each stage (in the order the stages were initially
 * activated), as well as an overall processing duration.</p>
 *
 * <p>All timings represent wall-clock time.</p>
 */
public class MultiStageTimer {
    // TODO Add support to conveniently log to prometheus, either at close or at stage switching boundaries

    /**
     * Timer states for indivindual timer stages.
     */
    enum TimerState {
        STARTED,
        STOPPED
    }

    private final Logger logger;

    // synchronizedMap to (partly) address needs of #async(String)... java has no concurrent map
    // that maintains insertion order, unfortunately. We need to synchronize when iterating
    private final Map<String, StageTimer> timers = new LinkedHashMap<>();
    private StageTimer currentTimer = null;


    /**
     * Create a new timer.
     *
     * @param logger logger to use for all logging requests
     */
    public MultiStageTimer(@Nullable Logger logger) {
        this.logger = logger;
    }

    /**
     * Activate the named stage (and deactivate the active stage, if any).
     *
     * @param stage enum whose name() is the stage to activate
     * @return this timer, to support fluent method calls
     */
    @Nonnull
    public MultiStageTimer start(@Nonnull Enum<?> stage) {
        return start(stage.name());
    }

    /**
     * Activate the named stage (and deactivate the active stage, if any).
     *
     * @param stage name of stage to activate
     * @return this timer
     */
    @Nonnull
    public MultiStageTimer start(@Nonnull String stage) {
        final StageTimer timer = getTimer(stage);
        if (timer != currentTimer && currentTimer != null) {
            currentTimer.stop();
        }
        timer.start();
        currentTimer = timer;
        return this;
    }


    /**
     * Activate the current stage, if there is one and it's not already active.
     *
     * @return this timer
     */
    @Nonnull
    public MultiStageTimer start() {
        if (currentTimer != null) {
            currentTimer.start();
        }
        return this;
    }

    /**
     * Deactivate the named stage, if it is active.
     *
     * @param stage enum whose name is the stage to deactivate
     * @return this timer
     */
    @Nonnull
    public MultiStageTimer stop(@Nonnull Enum<?> stage) {

        return stop(stage.name());
    }

    /**
     * Deactivate the named stage, if it is active.
     *
     * @param stage name of the stage to deactivate
     * @return this timer
     */
    @Nonnull
    public MultiStageTimer stop(@Nonnull String stage) {
        // called for side-effect of creating stage
        StageTimer timer = getTimer(stage);
        return this;
    }

    /**
     * Deactivate the current stage, if there is one and it is active.
     *
     * @return this timer
     */
    @Nonnull
    public MultiStageTimer stop() {
        if (currentTimer != null) {
            currentTimer.stop();
        }
        return this;
    }

    /**
     * Deactivate all timers.
     *
     * @return this timer
     */
    @Nonnull
    public synchronized MultiStageTimer stopAll() {
        synchronized (timers) {
            timers.values().forEach(StageTimer::stop);
        }
        this.currentTimer = null;
        return this;
    }

    /**
     * Create a timer that will be asynchronously added as a segment to a given timer stage.
     *
     * <p>This is clumsier than it ought to be. It's used in cases where the activity to be
     * timed is taking place in a separate thread, and the clumsiness comes in isolating it
     * from starting and stopping of stages.</p>
     *
     * <p>When the async timer is closed, the duration of its existance is added as a segment
     * for the indicated stage.</p>
     *
     * @param stage the stage to attribute this timer's lifetime to
     * @return an auto-closing timer
     */
    @Nonnull
    public AsyncTimer async(@Nonnull String stage) {
        return new AsyncTimer(stage);
    }

    /**
     * Detail options for logging requests.
     */
    public enum Detail {
        /**
         * logs total of all stage timings.
         */
        OVERALL_SUMMARY,
        /**
         * logs above, plus total time spent in each stage.
         */
        STAGE_SUMMARY,
        /**
         * logs above plus list of timing segments durations in each stage.
         */
        STAGE_DETAIL,
        /**
         * logs above, plus start and stop times for each segment.
         */
        SEGMENT_DETAIL
    }

    /**
     * Log a report at INFO level.
     *
     * @param message Message text to be included at top-line of report
     * @param detail  desired detail level
     * @return this timer
     */
    @Nonnull
    public MultiStageTimer info(@Nonnull String message, @Nonnull Detail detail) {
        return log(Level.INFO, message, detail);
    }

    /**
     * Log a report at DEBUG level.
     *
     * @param message Message text to be included at top-line of report
     * @param detail  desired detail level
     * @return this timer
     */
    public @Nonnull
    MultiStageTimer debug(@Nonnull String message, @Nonnull Detail detail) {
        return log(Level.DEBUG, message, detail);
    }

    /**
     * Log a report of the current stage at INFO level.
     *
     * @param message Message text to be included at top-line of report
     * @param detail  desired detail level (OVERALL_SUMMERY same as STAGE_SUMMARY)
     * @return this timer
     */
    public @Nonnull
    MultiStageTimer infoStage(@Nonnull String message, @Nonnull Detail detail) {
        return logStage(Level.INFO, message, detail);
    }

    /**
     * Log a report of the current stage at DEBUG level.
     *
     * @param message Message text to be included at top-line of report
     * @param detail  desired detail level (OVERALL_SUMMARY same as STAGE_SUMMARY)
     * @return this timer
     */
    public @Nonnull
    MultiStageTimer debugStage(@Nonnull String message, @Nonnull Detail detail) {
        return logStage(Level.DEBUG, message, detail);
    }

    /**
     * Log a timer report at the given log level.
     *
     * @param level   log level to use
     * @param message message text to include at top-level in report
     * @param detail  desired detail level
     * @return this timer
     */
    @Nonnull
    public MultiStageTimer log(@Nonnull Level level, @Nonnull String message,
                               @Nonnull Detail detail) {
        switch (detail) {
            case OVERALL_SUMMARY:
                logTimerOverall(level, message);
                return this;
            case STAGE_SUMMARY:
                logTimerOverall(level, message);
                logTimerStageSummaries(level);
                break;
            case STAGE_DETAIL:
                logTimerOverall(level, message);
                logTimerStageDetails(level);
                break;
            case SEGMENT_DETAIL:
                logTimerOverall(level, message);
                logTimerSegmentDetails(level);
                break;
        }
        return this;
    }

    /**
     * Visit the per-stage timers in this timer.
     *
     * @param visitor The visitor.
     */
    public void visit(@Nonnull final TimerVisitor visitor) {
        timers.forEach((stageName, stageTimer) -> {
            visitor.visitStage(stageName, stageTimer.state == TimerState.STOPPED, stageTimer.getTotalDuration().toMillis());
        });
    }

    /**
     * Visitor for the stages in the timer.
     */
    public interface TimerVisitor {
        /**
         * This method will be called for every stage in the timer.
         *
         * @param stageName The name of the stage.
         * @param stopped True if the timer for that stage is stopped.
         * @param totalDurationMs The total duration for the stage.
         */
        void visitStage(@Nonnull String stageName, boolean stopped, long totalDurationMs);
    }

    /**
     * Log a report of the current stage at the indicated log level.
     *
     * @param level   log level to use
     * @param message message text to be included at top-line in the report
     * @param detail  desired detailed level (OVERALL_SUMMARY same as STAGE_SUMMARY)
     * @return this timer
     */
    @Nonnull
    private MultiStageTimer logStage(@Nonnull Level level, @Nonnull String message,
                                     @Nonnull Detail detail) {
        if (currentTimer != null) {
            switch (detail) {
                case OVERALL_SUMMARY:
                case STAGE_SUMMARY:
                    logStageOverall(level, message, currentTimer);
                    break;
                case STAGE_DETAIL:
                    logStageDetails(level, message, currentTimer);
                    break;
                case SEGMENT_DETAIL:
                    logStageOverall(level, message, currentTimer);
                    logSegmentDetails(level, 2, currentTimer.getSegments());
            }
        }
        return this;
    }

    private void logTimerOverall(@Nonnull Level level, @Nonnull String message) {
        if (logger != null) {
            logger.log(level, "{} {}", message, formatDuration(getTotalTime()));
        }
    }

    private void logTimerStageSummaries(@Nonnull Level level) {
        synchronized (timers) {
            for (StageTimer timer : timers.values()) {
                logStageOverall(level, "  Stage {}:", timer);
            }
        }
    }

    private void logTimerStageDetails(@Nonnull Level level) {
        synchronized (timers) {
            for (StageTimer timer : timers.values()) {
                logTimerStageDetails(level, "  Stage {}:", timer);
            }
        }
    }

    private void logTimerStageDetails(@Nonnull Level level, @Nonnull String message,
                                      @Nonnull StageTimer timer) {
        final String details = timer.formatSegmentDurations();
        if (logger != null) {
            logger.log(level,
                message + " {} [{}]", timer.getName(),
                formatDuration(timer.getTotalDuration()), details);
        }
    }

    private void logTimerSegmentDetails(@Nonnull Level level) {
        synchronized (timers) {
            for (StageTimer timer : timers.values()) {
                logTimerSegmentDetails(level, timer);
            }
        }
    }

    private void logTimerSegmentDetails(@Nonnull Level level, @Nonnull StageTimer timer) {
        if (logger != null) {
            logger.log(level, "  Stage {}: {}",
                timer.getName(), formatDuration(timer.getTotalDuration()));
        }
        logSegmentDetails(level, 4, timer.getSegments());
    }

    private void logStageOverall(@Nonnull Level level, @Nonnull String message,
                                 @Nonnull StageTimer timer) {
        if (logger != null) {
            logger.log(level, message + " {}",
                timer.getName(), formatDuration(timer.getTotalDuration()));
        }
    }

    private void logStageDetails(@Nonnull Level level, @Nonnull String message,
                                 @Nonnull StageTimer timer) {
        if (logger != null) {
            logger.log(level, "{}: {} [{}]}", message, timer.getTotalDuration(),
                timer.formatSegmentDurations());
        }
    }

    private void logSegmentDetails(@Nonnull Level level, int indent,
                                   @Nonnull Collection<Segment> segments) {
        for (Segment segment : segments) {
            logSegmentDetails(level, 4, segment);
        }
    }

    private void logSegmentDetails(@Nonnull Level level, int indent, @Nonnull Segment segment) {
        if (logger != null) {
            logger.log(level, "{}- {} [{} => {}]", StringUtils.repeat(' ', indent),
                formatDuration(segment.getDuration()), segment.getStart(), segment.getStop());
        }
    }

    @Nonnull
    private synchronized Duration getTotalTime() {
        return timers.values().stream()
            .map(StageTimer::getTotalDuration)
            .reduce(Duration.ZERO, Duration::plus);
    }

    @Nonnull
    private static String formatDuration(@Nonnull Duration duration) {
        long millis = TimeUnit.SECONDS.toMillis(duration.getSeconds())
            + TimeUnit.NANOSECONDS.toMillis(duration.getNano());
        return DurationFormatUtils.formatDuration(millis, "H:mm:ss.SSS");
    }

    private synchronized StageTimer getTimer(@Nonnull String name) {
        return timers.computeIfAbsent(name, StageTimer::new);
    }

    /**
     * Class that accumulates intervals attributed to a named stage.
     */
    public static class StageTimer {

        private final String name;
        private List<Segment> segments = new ArrayList<>();
        private TimerState state = TimerState.STOPPED;

        /**
         * Create a new instance for a named stage.
         *
         * @param name name of stage
         */
        StageTimer(@Nonnull String name) {
            this.name = name;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        /**
         * Get segments accumulated for this timer stage.
         *
         * @return list of {@link Segment} objects
         */
        @Nonnull
        List<Segment> getSegments() {
            return segments;
        }

        /**
         * Begin accumulating a new segment for this stage, unless the timer is already running.
         */
        public synchronized void start() {
            if (state == TimerState.STOPPED) {
                segments.add(new Segment(Instant.now()));
                this.state = TimerState.STARTED;
            }
        }

        /**
         * Close out the current segment for this stage.
         */
        synchronized void stop() {
            if (state == TimerState.STARTED) {
                segments.get(segments.size() - 1).stop(Instant.now());
                this.state = TimerState.STOPPED;
            }
        }

        private void addSegment(Segment segment) {
            segments.add(segment);
        }

        /**
         * Get the total of all the segment durations accumulated for this stage.
         *
         * @return a {@link Duration} object representing the total duration
         */
        @Nonnull
        synchronized Duration getTotalDuration() {
            return segments.stream()
                    .map(Segment::getDuration)
                    .filter(Objects::nonNull)
                    .reduce(Duration.ZERO, Duration::plus);
        }

        /**
         * Format all the closed segment durations, and return them as a comma-separated string.
         *
         * @return comma-separated segment durations
         */
        @Nonnull
        String formatSegmentDurations() {
            return segments.stream()
                .map(Segment::getDuration)
                .filter(Objects::nonNull) // skip open segments
                .map(MultiStageTimer::formatDuration)
                .collect(Collectors.joining(", "));
        }
    }

    /**
     * Class to represent a single segment in a stage timer.
     */
    public static class Segment {
        private final Instant start;
        private Instant stop;

        /**
         * Create a new segment starting at the given instant.
         *
         * @param start the instant this segment starts
         */
        Segment(@Nonnull Instant start) {
            this.start = start;
        }

        /**
         * Close the current segment at the indicated instant.
         *
         * @param stop the instant this segment ends
         */
        void stop(@Nonnull Instant stop) {
            this.stop = stop;
        }

        @Nonnull
        public Instant getStart() {
            return start;
        }

        @Nonnull
        Instant getStop() {
            return stop;
        }

        /**
         * Get the duration of this segment.
         *
         * @return the {@link Duration} of this segment, or null if the segment has no end time
         */
        Duration getDuration() {
            return stop != null ? Duration.between(start, stop) : null;
        }
    }

    /**
     * An object whose lifetime will be attributed to a given timer stage.
     *
     * <p>Useful for timing operations performed in their own threads, without worrying
     * about activation/deactivation of timer stages.</p>
     */
    public class AsyncTimer implements AutoCloseable {
        private final Instant start;
        private final String stage;

        /**
         * Create a new instance.
         *
         * @param stage the timer stage to account for this timer's lifetime
         */
        AsyncTimer(String stage) {
            this.stage = stage;
            this.start = Instant.now();
        }

        @Override
        public void close() {
            final Segment segment = new Segment(start);
            segment.stop(Instant.now());
            getTimer(stage).addSegment(segment);
        }
    }
}
