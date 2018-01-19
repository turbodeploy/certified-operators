package com.vmturbo.components.common.health.sql;

/**
 * Subclass of SQLDBHealthMonitor that just sets the health monitor name to "MariaDB"
 */
public class MariaDBHealthMonitor extends SQLDBHealthMonitor {
    public MariaDBHealthMonitor(double pollingIntervalSecs, SQLDBConnectionFactory connectionFactory) {
        super("MariaDB", pollingIntervalSecs, connectionFactory);
    }
}
