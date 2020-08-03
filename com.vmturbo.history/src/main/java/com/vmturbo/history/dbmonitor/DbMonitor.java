package com.vmturbo.history.dbmonitor;

import java.util.ArrayList;
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

import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.dbmonitor.ProcessListClassifier.ProcessListRecord;

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
     */
    public DbMonitor(
            ProcessListClassifier processListClassifier, DSLContext dsl, int intervalSec,
            int longRunningThresholdSecs) {
        this.processListClassifier = processListClassifier;
        this.dsl = dsl;
        this.intervalSec = intervalSec;
        this.longRunningThresholdSecs = longRunningThresholdSecs;
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
        final String sql = "SELECT count(*), @@GLOBAL.max_connections FROM information_schema.processlist";
        try {
            final Record2<Long, ULong> counts = (Record2<Long, ULong>)dsl.fetchOne(sql);
            if (counts != null) {
                logger.info("Connection count {}/{}", counts.value1(), counts.value2());
                SharedMetrics.CONNECTION_COUNTS.labels(SharedMetrics.CONNECTION_COUNT_ACTIVE)
                        .setData(counts.value1().doubleValue());
                SharedMetrics.CONNECTION_COUNTS.labels(SharedMetrics.CONNECTION_COUNT_MAX)
                        .setData(counts.value2().doubleValue());
                SharedMetrics.CONNECTION_COUNTS.labels(SharedMetrics.CONNECTION_COUNT_AVALABLE)
                        .setData(counts.value2().doubleValue() - counts.value1().doubleValue());
            } else {
                throw new DataAccessException("No record returned from query");
            }
        } catch (DataAccessException e) {
            logger.warn("Failed to retrieve connection counts: {}", e);
        }
    }

    private void logConnectionsByClassification(List<ProcessListRecord> proceses) {
        final List<ProcessListRecord> longRunningQueries = new ArrayList<>();
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
            SharedMetrics.CONNECTION_COUNTS.labels(classification.toString())
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
                if (priorLongRunning.containsKey(id) && priorLongRunning.get(id).equals(info)) {
                    logLongRunner(process);
                }
            }
        }
        SharedMetrics.CONNECTION_COUNTS.labels(SharedMetrics.CONNECTION_COUNT_LONG_RUNNING)
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
                    + "FROM information_schema.processlist ORDER BY id";
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
