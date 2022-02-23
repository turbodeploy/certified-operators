package com.vmturbo.sql.utils.dbmonitor;

import java.util.Collections;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.exception.DataAccessException;
import org.jooq.types.ULong;

import com.vmturbo.sql.utils.dbmonitor.ProcessListClassifier.ProcessListRecord;

/**
 * This class periodically logs information about state of the database, to provide information
 * that may be helpful in diagnosing DB-related failures.
 */
public class MySQLDbMonitor extends DbMonitor {
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
    public MySQLDbMonitor(
            ProcessListClassifier processListClassifier, DSLContext dsl, int intervalSec,
            int longRunningThresholdSecs, String dbSchemaName, String dbUsername) {
        super(processListClassifier, dsl, intervalSec, longRunningThresholdSecs, dbSchemaName, dbUsername);
    }

    void logConnectionCounts() {
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

    List<ProcessListRecord> getCurrentProcessList() {
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
            logger.warn("Failed to obtain process list data", e);
            return Collections.emptyList();
        }
    }
}
