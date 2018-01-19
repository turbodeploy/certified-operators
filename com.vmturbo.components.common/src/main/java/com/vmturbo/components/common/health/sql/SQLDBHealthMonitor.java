package com.vmturbo.components.common.health.sql;

import java.sql.Connection;

import com.vmturbo.components.common.health.PollingHealthMonitor;

/**
 * SQLDBHealthMonitor provides a scheduled poll of the a sql db connection and reports
 * a SimpleHealthStatus based on the results.
 */
public class SQLDBHealthMonitor extends PollingHealthMonitor {

    private SQLDBConnectionFactory connectionFactory;

    /**
     * Constructor that takes a SQLDBConnectionFactory reference. The SQLDBConnectionFactory is a
     * functional interface that wraps a Supplier<Connection> typed method that also throws
     * Exceptions. It may seem weird to do it this way, but it's because the
     * java.sql.DataSource.getConnection() method signature we are using in some places throws an
     * SqlException, and this is a way to work it into a lambda expression.
     *
     * @param name a name for this health check to use
     * @param pollingIntervalSecs how long to wait in between health checks
     * @param connectionFactory the connection factory method to use to provision connections for
     *                          the health check function.
     */
    public SQLDBHealthMonitor(String name, double pollingIntervalSecs, SQLDBConnectionFactory connectionFactory) {
        super(name, pollingIntervalSecs);
        this.connectionFactory = connectionFactory;
    }

    /**
     * Check the SQL database health by verifying that a connection can be opened. Any problems
     * opening a connection will count as an "unhealthy" dependency status.
     * @return a "healthy" status if a db connection could be opened, "unhealthy" otherwise
     */
    @Override
    public void updateHealthStatus() {
        try {
            // just test the connection
            connectionFactory.get().close();
        } catch (Throwable t) {
            reportUnhealthy(t.toString());
            return;
        }

        reportHealthy();
    }

    /**
     * Functional interface that wraps the exception on the Connection.getConnection() function, to
     * facilitate use in a lambda expression. Java 8 will not let you use methods that throw
     * exceptions directly in lambdas.
     */
    @FunctionalInterface
    public interface SQLDBConnectionFactory {
        Connection get() throws Exception;
    }

}
