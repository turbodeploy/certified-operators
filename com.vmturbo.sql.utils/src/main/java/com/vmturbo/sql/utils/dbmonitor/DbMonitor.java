package com.vmturbo.sql.utils.dbmonitor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * This class periodically logs information about state of the database, to provide information
 * that may be helpful in diagnosing DB-related failures.
 */
public abstract class DbMonitor {
    private static final String UNCLASSIFIED_QUERY = "<unclassified>";
    private static final String SLEEP_COMMAND = "Sleep";
    private static final String DAEMON_COMMAND = "Daemon";
    static Logger logger = LogManager.getLogger();

    private final ProcessListClassifier processListClassifier;
    final DSLContext dsl;
    private final int intervalSec;
    private final int longRunningThresholdSecs;

    private Map<Long, String> priorLongRunning = new HashMap<>();

    final String dbSchemaName;
    final String dbUsername;

    /**
     * Prometheus metric to capture connection counts collected by {@link MySQLDbMonitor}.
     */
    public static final DataMetricGauge CONNECTION_COUNTS = DataMetricGauge.builder()
            .withName("db_connections")
            .withHelp("Connection counts reported by database")
            .withLabelNames("schema", "category")
            .build()
            .register();

    /** connection count category for active connections of all variety. */
    public static final String CONNECTION_COUNT_ACTIVE = "active";
    /** connection count category for available connections (max - active). */
    public static final String CONNECTION_COUNT_AVAILABLE = "available";
    /** connection count category for max allowed connections (db config value). */
    public static final String CONNECTION_COUNT_MAX = "max_allowed";
    /** connection count category for long-running connections. */
    public static final String CONNECTION_COUNT_LONG_RUNNING = "long_running";

    /**
     * Data metric scope: GLOBAL
     * Database Prometheus metrics has two labels: schema and category. DB connection counts
     * are not specific to a schema and the "GLOBAL" scope will be used.
     */
    public static final String METRIC_SCOPE_GLOBAL = "GLOBAL";

    /**
     * Create a new instance of the monitor.
     *
     * @param processListClassifier    a {@link ProcessListClassifier} instance that can be used to
     *                                 classify DB threads
     * @param dsl                      a {@link DSLContext} configured with a datasource that can be
     *                                 used to execute operations on the database
     * @param intervalSec              interval in seconds to wait between polling operations
     * @param longRunningThresholdSecs time beyond which a non-idle connection is considered
     *                                 long-running and will be logged explicitly
     * @param dbSchemaName             database schema name
     * @param dbUsername               database username
     */
    public DbMonitor(
            ProcessListClassifier processListClassifier, DSLContext dsl, int intervalSec,
            int longRunningThresholdSecs, String dbSchemaName, String dbUsername) {
        this.processListClassifier = processListClassifier;
        this.dsl = dsl;
        this.intervalSec = intervalSec;
        this.longRunningThresholdSecs = longRunningThresholdSecs;
        this.dbSchemaName = dbSchemaName;
        this.dbUsername = dbUsername;
    }

    /**
     * Run the monitoring loop.
     *
     * @throws InterruptedException if interrupted
     */
    public void run() throws InterruptedException {
        while (true) {
            runOnePass();
            Thread.sleep(TimeUnit.SECONDS.toMillis(intervalSec));
        }
    }

    abstract void logConnectionCounts();

    abstract List<ProcessListClassifier.ProcessListRecord> getCurrentProcessList();

    private void runOnePass() {
        try {
            logConnectionCounts();
            final List<ProcessListClassifier.ProcessListRecord> processes = getCurrentProcessList();
            logConnectionsByClassification(processes);
            logLongRunningConnections(processes);
        } catch (DataAccessException e) {
            logger.error("Failed to retrieve DB monitor info: {}", e.getMessage());
        }
    }

    private void logConnectionsByClassification(List<ProcessListClassifier.ProcessListRecord> proceses) {
        final ListMultimap<Object, ProcessListClassifier.ProcessListRecord> byClassification =
                MultimapBuilder.ListMultimapBuilder.linkedHashKeys().arrayListValues().build();
        for (final ProcessListClassifier.ProcessListRecord record : proceses) {
            final Object cls = processListClassifier.classify(record).orElse(new Object() {
                @Override
                public String toString() {
                    return String.format("%s/%s", UNCLASSIFIED_QUERY, record.getDb());
                }
            });
            byClassification.put(cls, record);
        }
        for (final Object classification : byClassification.keySet()) {
            logger.info("{}: {}", classification, summarize(byClassification.get(classification)));
            CONNECTION_COUNTS.labels(dbSchemaName, classification.toString())
                    .setData((double)byClassification.get(classification).size());
        }
    }

    private void logLongRunningConnections(final List<ProcessListClassifier.ProcessListRecord> processes) {
        final Map<Long, String> currentLongRunning = new HashMap<>();
        for (ProcessListClassifier.ProcessListRecord process : processes) {
            if (isLongRunning(process)) {
                // we try to eliminate false positives for pooled connections by only logging
                // when a process shows up as long-running with the same "info" string in two
                // or more consecutive scans
                final long id = process.getId();
                final String info = process.getInfo();
                currentLongRunning.put(id, info);
                if (priorLongRunning.containsKey(id) && Objects.equals(priorLongRunning.get(id), info)) {
                    logLongRunner(process);
                }
            }
        }
        CONNECTION_COUNTS.labels(dbSchemaName, CONNECTION_COUNT_LONG_RUNNING)
                .setData((double)currentLongRunning.size());
        this.priorLongRunning = currentLongRunning;
    }

    private void logLongRunner(final ProcessListClassifier.ProcessListRecord process) {
        logger.warn("Long Running Connection: #{} {} {}({}) {}: {}",
                process.getId(), process.getDb(), process.getCommand(), process.getState(),
                formatSec(process.getTime()), process.getInfo());
    }

    private boolean isLongRunning(ProcessListClassifier.ProcessListRecord process) {
        return !ImmutableSet.of(SLEEP_COMMAND, DAEMON_COMMAND).contains(process.getCommand())
                && process.getTime() >= longRunningThresholdSecs;
    }

    private String summarize(List<ProcessListClassifier.ProcessListRecord> records) {
        final long totsec = records.stream()
                .collect(Collectors.summingLong(ProcessListClassifier.ProcessListRecord::getTime));
        String info = records.stream()
                .map(r -> !Strings.isNullOrEmpty(r.getInfo()) ? r.getTrimmedInfo(30)
                        : !Strings.isNullOrEmpty(r.getState()) ? r.getState()
                        : "-")
                .filter(Objects::nonNull)
                .findFirst()
                .orElse("-");
        String command = records.stream()
                .map(ProcessListClassifier.ProcessListRecord::getCommand)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse("-");
        return String.format("(%d)[%s/%s]: %s", records.size(), command, formatSec(totsec), info);
    }

    private static String formatSec(long sec) {
        final long secs = sec % 60;
        final long mins = (sec / 60) % 60;
        final long hrs = (sec / (60 * 60)) % 24;
        final long days = sec / (24 * 60 * 60);
        return days > 0 ? String.format("%d:%02d:%02d:%02d", days, hrs, mins, secs)
                : hrs > 0 ? String.format("%d:%02d:%02d", hrs, mins, secs)
                : String.format("%d:%02d", mins, secs);
    }
}
