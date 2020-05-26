package com.vmturbo.components.common.health.sql;

/**
 * Subclass of SQLDBHealthMonitor that just sets the health monitor name to "PostgreSQL".
 */
public class PostgreSQLHealthMonitor extends SQLDBHealthMonitor {
    /**
     * Create a new instance.
     *
     * @param pollingIntervalSecs how long to wait between health checks
     * @param connectionFactory   source fo connections for health checks
     */
    public PostgreSQLHealthMonitor(double pollingIntervalSecs, SQLDBConnectionFactory connectionFactory) {
        super("PostgreSQL", pollingIntervalSecs, connectionFactory);
    }
}
