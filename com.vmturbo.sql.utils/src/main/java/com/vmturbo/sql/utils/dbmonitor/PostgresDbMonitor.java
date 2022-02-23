package com.vmturbo.sql.utils.dbmonitor;

import java.util.Collections;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.vmturbo.sql.utils.dbmonitor.ProcessListClassifier.ProcessListRecord;

/**
 * This class periodically logs information about state of the database, to provide information
 * that may be helpful in diagnosing DB-related failures.
 */
public class PostgresDbMonitor extends DbMonitor {
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
    public PostgresDbMonitor(
            ProcessListClassifier processListClassifier, DSLContext dsl, int intervalSec,
            int longRunningThresholdSecs, String dbSchemaName, String dbUsername) {
        super(processListClassifier, dsl, intervalSec, longRunningThresholdSecs, dbSchemaName, dbUsername);
    }

    void logConnectionCounts() {
        final String activeConnectionsQuery = "select count(*) from pg_stat_activity;";
        final String maxConnectionsQuery = "SHOW max_connections;";
        try {
            final double nConnections = (Long)dsl.fetchOne(activeConnectionsQuery).get(0);
            final double maxConnections = Double.parseDouble((String)dsl.fetchOne(maxConnectionsQuery).get(0));
            logger.info("Global connection count {}/{}", nConnections, maxConnections);
            CONNECTION_COUNTS.labels(METRIC_SCOPE_GLOBAL, CONNECTION_COUNT_ACTIVE)
                    .setData(nConnections);
            CONNECTION_COUNTS.labels(METRIC_SCOPE_GLOBAL, CONNECTION_COUNT_MAX)
                    .setData(maxConnections);
            CONNECTION_COUNTS.labels(METRIC_SCOPE_GLOBAL, CONNECTION_COUNT_AVAILABLE)
                    .setData(maxConnections - nConnections);
        } catch (DataAccessException | NumberFormatException e) {
            logger.warn("Failed to retrieve connection counts:", e);
        }
    }

    List<ProcessListRecord> getCurrentProcessList() {
        try {
            final String sql = "SELECT pid as id, datname as db, state as command, now() - query_start as time, state_change as stat, query as info"
                    + " FROM pg_stat_activity where usename = '" + dbUsername + "' order by pid;";
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
