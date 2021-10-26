package com.vmturbo.sql.utils.pool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Configuration;

/**
 * A utility class for configuring a Hikari database connection pool.
 */
@Configuration("dbConnectionPool")
public class DbConnectionPoolConfig {

    private static final Logger logger = LogManager.getLogger();

    /**
     * This counter ensures the name of connection pool is unique.
     */
    private static final AtomicInteger poolCounter = new AtomicInteger(0);

    /**
     * Set the keep-alive time to 5 minutes.
     *
     * <p>This setting causes the connection pool to issue an `isValid()` statement
     *    periodically (every 5 minutes currently) to keep the connection active.</p>
     *
     * <p>Do not disable this setting! Without this, kube-proxy will drop an idle
     *    connection (by default, after 15 minutes). Since kube-proxy does not close
     *    dropped connections cleanly, it will cause connections to accumulate on the
     *    database side. Keep-alive prevents this from happening.
     *    See: https://github.com/brettwooldridge/HikariCP#frequently-used</p>
     */
    public static final int DEFAULT_KEEPALIVE_TIME_MINUTES = 5;

    /**
     * This property controls the amount of time that a connection can be out of the pool before a
     * message is logged indicating a possible connection leak. Any query that runs longer than this
     * amount of time will generate a warning in the logs.
     */
    private static final long LEAK_DETECTION_THRESHOLD_MSEC = TimeUnit.MINUTES.toMillis(2);

    private DbConnectionPoolConfig() {
        // prevent instantiation of the utility class
    }

    /**
     * Create a HikariDataSource, providing a pool of database connections.
     *
     * @param dbUrl the URL to use for connecting to the database
     * @param dbUsername the username to use when connecting to the database
     * @param dbPassword the password to use when connecting to the database
     * @param minPoolSize initial and minimum size for the DB connection pool
     * @param maxPoolSize maximum size for the DB connection pool
     * @param dbKeepAliveIntervalMinutes the keep-alive time in minutes
     * @param poolName The name of the connection pool.
     * @return a HikariDataSource, providing a pool of database connections
     */
    public static DataSource getPooledDataSource(final String dbUrl,
           final String dbUsername, final String dbPassword, final int minPoolSize,
           final int maxPoolSize, @Nullable final Integer dbKeepAliveIntervalMinutes,
           @Nonnull final String poolName) {
        HikariDataSource dataSource = new HikariDataSource();
        // Minimum keep-alive time is 1 minute
        final long keepAliveTimeMillis =
                dbKeepAliveIntervalMinutes != null && dbKeepAliveIntervalMinutes >= 1
                        ? TimeUnit.MINUTES.toMillis(dbKeepAliveIntervalMinutes)
                        : TimeUnit.MINUTES.toMillis(DEFAULT_KEEPALIVE_TIME_MINUTES);
        // Should be logged only once, on container startup
        logger.info("Initializing database connection pool: minPoolSize={}, maxPoolSize={}, "
                        + "keepAliveMillis={}", minPoolSize, maxPoolSize, keepAliveTimeMillis);
        dataSource.setDriverClassName("org.mariadb.jdbc.Driver");
        dataSource.setJdbcUrl(dbUrl);
        dataSource.setUsername(dbUsername);
        dataSource.setPassword(dbPassword);
        dataSource.setMinimumIdle(minPoolSize);
        dataSource.setMaximumPoolSize(maxPoolSize);
        dataSource.setKeepaliveTime(keepAliveTimeMillis);
        dataSource.setLeakDetectionThreshold(LEAK_DETECTION_THRESHOLD_MSEC);
        dataSource.setPoolName(poolName);
        return  dataSource;
    }

    /**
     * Derive the pool name based on the schema name targeted by the pool.
     *
     * @param dbSchemaName the name of the schema
     * @return the pool name based on the schema name targeted by the pool
     */
    public static String generatePoolName(String dbSchemaName) {
        return "pool-" + poolCounter.getAndIncrement() + "-" + dbSchemaName;
    }
}
