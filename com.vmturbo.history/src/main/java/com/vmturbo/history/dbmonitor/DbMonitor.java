package com.vmturbo.history.dbmonitor;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder.ListMultimapBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.exception.DataAccessException;

import com.vmturbo.history.dbmonitor.ProcessListClassifier.ProcessListRecord;

/**
 * This class periodically logs information about state of the database, to provide information
 * that may be helpful in diagnosing DB-related failures.
 */
public class DbMonitor {
    private static final String UNCLASSIFIED_QUERY = "<unclassified>";
    private static Logger logger = LogManager.getLogger();

    private final ProcessListClassifier processListClassifier;
    private final DSLContext dsl;
    private final int intervalSec;

    /**
     * Create a new instance of the monitor.
     *
     * @param processListClassifier a {@link ProcessListClassifier} instance that can be used to
     *                              classify DB threads
     * @param dsl                   a {@link DSLContext} configured with a datasource that can
     *                              be used to execute operations on the database
     * @param intervalSec           interval in seconds to wait between polling operations
     */
    public DbMonitor(
            ProcessListClassifier processListClassifier, DSLContext dsl, int intervalSec) {
        this.processListClassifier = processListClassifier;
        this.dsl = dsl;
        this.intervalSec = intervalSec;
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
            logConnections();
        } catch (DataAccessException e) {
            logger.error("Failed to retrieve DB monitor info: {}", e.getMessage());
        }
    }

    private void logConnectionCounts() {
        final String sql = "SELECT count(*), @@GLOBAL.max_connections FROM information_schema.processlist";
        try {
            final Record2<?, ?> counts = (Record2<?, ?>)dsl.fetchOne(sql);
            if (counts != null) {
                logger.info("Connection count {}/{}", counts.value1(), counts.value2());
            } else {
                throw new DataAccessException("No record returned from query");
            }
        } catch (DataAccessException e) {
            logger.warn("Failed to retrieve connection counts: {}", e);
        }
    }

    private void logConnections() {
        final String sql = "SELECT db, command, time_ms, state, info FROM information_schema.processlist";
        try {
            final List<ProcessListRecord> records = dsl.fetch(sql).into(ProcessListRecord.class);
            if (records == null) {
                throw new DataAccessException("Null results object from query");
            }
            final ListMultimap<Object, ProcessListRecord> byClassification =
                    ListMultimapBuilder.linkedHashKeys().arrayListValues().build();
            for (final ProcessListRecord record : records) {
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
            }
        } catch (DataAccessException e) {
            logger.warn("Failed to obtain process list data: {}", e);
        }

    }

    private String summarize(List<ProcessListRecord> records) {
        final double totmsec = records.stream()
                .collect(Collectors.summingDouble(ProcessListRecord::getTimeMs));
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
        return String.format("(%d)[%s/%s]: %s",
                records.size(), command, formatMsec(Math.round(totmsec)), info);
    }

    private static String formatMsec(long msec) {
        final long msecs = msec % 1000;
        final long secs = (msec / 1000) % 60;
        final long mins = (msec / (60 * 1000)) % 60;
        final long hrs = (msec / (60 * 60 * 1000)) % 24;
        final long days = msec / (24 * 60 * 60 * 1000);
        return days > 0 ? String.format("%d:%02d:%02d:%02d.%03d", days, hrs, mins, secs, msecs)
                : hrs > 0 ? String.format("%d:%02d:%02d.%03d", hrs, mins, secs, msecs)
                : String.format("%d:%02d.%03d", mins, secs, msecs);
    }
}
