package com.vmturbo.sql.utils.dbmonitor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder.ListMultimapBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.exception.DataAccessException;
import org.jooq.types.ULong;

import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.sql.utils.dbmonitor.ProcessListClassifier.ProcessListRecord;

/**
 * This class periodically logs information about state of the database, to provide information
 * that may be helpful in diagnosing DB-related failures.
 */
public class DbMonitor {
    private static final String UNCLASSIFIED_QUERY = "<unclassified>";
    private static final String SLEEP_COMMAND = "Sleep";
    private static final String DAEMON_COMMAND = "Daemon";
    private static Logger logger = LogManager.getLogger();

    private final ProcessListClassifier processListClassifier;
    private final DSLContext dsl;
    private final int intervalSec;
    private final int longRunningThresholdSecs;

    private Map<Long, String> priorLongRunning = new HashMap<>();

    private final String dbSchemaName;
    private final String dbUsername;

    /**
     * Prometheus metric to capture connection counts collected by {@link DbMonitor}.
     */
    private static final DataMetricGauge CONNECTION_COUNTS = DataMetricGauge.builder()
            .withName("db_connections")
            .withHelp("Connection counts reported by database")
            .withLabelNames("schema", "category")
            .build()
            .register();

    /** connection count category for active connections of all variety. */
    private static final String CONNECTION_COUNT_ACTIVE = "active";
    /** connection count category for available connections (max - active). */
    private static final String CONNECTION_COUNT_AVAILABLE = "available";
    /** connection count category for max allowed connections (db config value). */
    private static final String CONNECTION_COUNT_MAX = "max_allowed";
    /** connection count category for long-running connections. */
    private static final String CONNECTION_COUNT_LONG_RUNNING = "long_running";

    /**
     * Data metric scope: GLOBAL
     * Database Prometheus metrics has two labels: schema and category. DB connection counts
     * are not specific to a schema and the "GLOBAL" scope will be used.
     */
    private static final String METRIC_SCOPE_GLOBAL = "GLOBAL";

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

    private void runOnePass() {
        try {
            logConnectionCounts();
            final List<ProcessListRecord> processes = getCurrentProcessList();
            logConnectionsByClassification(processes);
            logLongRunningConnections(processes);
        } catch (DataAccessException e) {
            logger.error("Failed to retrieve DB monitor info: {}", e.getMessage());
        }
    }

    private void logConnectionCounts() {
        final String sql = "SELECT variable_value, @@GLOBAL.max_connections "
                + "FROM information_schema.global_status WHERE variable_name = 'Threads_connected'";
        try {
            final Record2<String, ULong> counts = (Record2<String, ULong>)dsl.fetchOne(sql);
            if (counts != null) {
                final Double threadConnected = Double.valueOf(counts.value1());
                logger.info("Global connection count {}/{}", counts.value1(), counts.value2());
                CONNECTION_COUNTS.labels(METRIC_SCOPE_GLOBAL, CONNECTION_COUNT_ACTIVE)
                        .setData(threadConnected);
                CONNECTION_COUNTS.labels(METRIC_SCOPE_GLOBAL, CONNECTION_COUNT_MAX)
                        .setData(counts.value2().doubleValue());
                CONNECTION_COUNTS.labels(METRIC_SCOPE_GLOBAL, CONNECTION_COUNT_AVAILABLE)
                        .setData(counts.value2().doubleValue() - threadConnected);
            } else {
                throw new DataAccessException("No record returned from query: " + sql);
            }
        } catch (DataAccessException | NumberFormatException e) {
            logger.warn("Failed to retrieve connection counts:", e);
        }
    }

    private void logConnectionsByClassification(List<ProcessListRecord> proceses) {
        final ListMultimap<Object, ProcessListRecord> byClassification =
                ListMultimapBuilder.linkedHashKeys().arrayListValues().build();
        for (final ProcessListRecord record : proceses) {
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

    private void logLongRunningConnections(final List<ProcessListRecord> processes) {
        final Map<Long, String> currentLongRunning = new HashMap<>();
        for (ProcessListRecord process : processes) {
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

    private void logLongRunner(final ProcessListRecord process) {
        logger.warn("Long Running Connection: #{} {} {}({}) {}: {}",
                process.getId(), process.getDb(), process.getCommand(), process.getState(),
                formatSec(process.getTime()), process.getInfo());
    }

    private List<ProcessListRecord> getCurrentProcessList() {
        try {
            final String sql = "SELECT id, db, command, time, state, info "
                    + "FROM information_schema.processlist WHERE user = '"
                    + dbUsername + "' ORDER BY id";
            final List<ProcessListRecord> processes = dsl.fetch(sql).into(ProcessListRecord.class);
            if (processes == null) {
                throw new DataAccessException("Null results object from query");
            } else {
                return processes;
            }
        } catch (DataAccessException e) {
            logger.warn("Failed to obtain process list data: {}", e);
            return Collections.emptyList();
        }
    }

    private boolean isLongRunning(ProcessListRecord process) {
        return !ImmutableSet.of(SLEEP_COMMAND, DAEMON_COMMAND).contains(process.getCommand())
                && process.getTime() >= longRunningThresholdSecs;
    }

    private String summarize(List<ProcessListRecord> records) {
        final long totsec = records.stream()
                .collect(Collectors.summingLong(ProcessListRecord::getTime));
        String info = records.stream()
                .map(r -> !Strings.isNullOrEmpty(r.getInfo()) ? r.getTrimmedInfo(30)
                        : !Strings.isNullOrEmpty(r.getState()) ? r.getState()
                        : "-")
                .filter(Objects::nonNull)
                .findFirst()
                .orElse("-");
        String command = records.stream()
                .map(ProcessListRecord::getCommand)
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
