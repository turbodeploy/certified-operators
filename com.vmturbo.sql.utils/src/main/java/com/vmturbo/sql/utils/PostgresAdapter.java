package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.ds.PGSimpleDataSource;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * {@link DbAdapter} implementation for PostgreSQL endpoints.
 */
public class PostgresAdapter extends DbAdapter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Supported time units for retention policy, mapping from time unit to sql query.
     */
    private static final Map<ChronoUnit, String> SUPPORTED_TIME_UNIT_FOR_RETENTION_POLICY =
            ImmutableMap.of(
                    ChronoUnit.YEARS, "years",
                    ChronoUnit.MONTHS, "months",
                    ChronoUnit.WEEKS, "weeks",
                    ChronoUnit.DAYS, "days",
                    ChronoUnit.HOURS, "hours"
            );

    PostgresAdapter(final DbEndpoint config) {
        super(config);
    }

    @Override
    DataSource getDataSource(String url, String user, String password) throws InterruptedException {
        final PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        dataSource.setCurrentSchema(config.getSchemaName());
        return dataSource;
    }

    @Override
    protected void createNonRootUser() throws SQLException, UnsupportedDialectException, InterruptedException {
        try (Connection conn = getRootConnection(null)) {
            dropUser(conn, config.getUserName());
            execute(conn, String.format("CREATE USER \"%s\" WITH PASSWORD '%s'",
                    config.getUserName(), config.getPassword()));
            execute(conn, String.format("ALTER ROLE \"%s\" SET search_path TO \"%s\"",
                    config.getUserName(), config.getSchemaName()));
        }
    }

    private void dropUser(final Connection conn, final String user) throws SQLException {
        try {
            execute(conn, String.format("DROP USER IF EXISTS \"%s\"", user));
        } catch (SQLException e) {
            logger.error("DROP USER failed");
            throw e;
        }
    }

    @Override
    protected void performNonRootGrants(DbEndpointAccess access) throws SQLException, UnsupportedDialectException, InterruptedException {
        switch (access) {
            case ALL:
                performRWGrants();
                break;
            case READ_ONLY:
                performROGrants();
                break;
            case READ_WRITE_DATA:
                throw new UnsupportedOperationException(("Unsupported DB endpoint access: " + access.name()));
            default:
                throw new IllegalAccessError("Unknown DB endpoint access: " + access.name());
        }
    }

    private void performRWGrants() throws UnsupportedDialectException, SQLException, InterruptedException {
        try (Connection conn = getRootConnection(config.getDatabaseName())) {
            execute(conn, String.format("GRANT ALL PRIVILEGES ON SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
        }
    }

    private void performROGrants() throws SQLException, UnsupportedDialectException, InterruptedException {
        try (Connection conn = getRootConnection(config.getDatabaseName())) {
            execute(conn, String.format("GRANT CONNECT ON DATABASE \"%s\" TO \"%s\"",
                    config.getDatabaseName(), config.getUserName()));
            execute(conn, String.format("GRANT USAGE ON SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
            execute(conn, String.format("GRANT SELECT ON ALL TABLES IN SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
            execute(conn, String.format("ALTER DEFAULT PRIVILEGES IN SCHEMA \"%s\" "
                            + "GRANT SELECT ON TABLES TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
        }
    }

    @Override
    protected void createSchema() throws SQLException, UnsupportedDialectException, InterruptedException {
        try (Connection conn = getRootConnection(null)) {
            if (!databaseExists(conn, config.getDatabaseName())) {
                execute(conn, String.format("CREATE DATABASE \"%s\"", config.getDatabaseName()));
                try (Connection dbConn = getRootConnection(config.getDatabaseName())) {
                    execute(dbConn, "DROP SCHEMA public CASCADE");
                }
            }
        }
        try (Connection conn = getRootConnection(config.getDatabaseName())) {
            execute(conn, String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", config.getSchemaName()));
            setupTimescaleDb(conn);
        }
    }

    /**
     * Ensure that the currently connected database can make use of the TimescaleDB extension.
     *
     * <p>TODO: This should not be done for every POSTGRES endpoint, but for the moment it's
     * relatively harmless. The jOOQ {@link org.jooq.SQLDialect} enum does not make a distinction,
     * so we'll need a way to introduce that in our basic endpoint definitions. Perhaps something
     * like a list of required features?</p>
     *
     * @param conn db connection currently connected to the target database
     * @throws SQLException if there's a problem adding the extension
     * @throws InterruptedException if interrupted
     */
    protected void setupTimescaleDb(Connection conn) throws SQLException, InterruptedException {
        execute(conn, String.format("CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA %s",
                config.getSchemaName()));
    }

    private boolean databaseExists(final Connection conn, final String databaseName) throws SQLException {
        ResultSet results = conn.createStatement().executeQuery(
                String.format("SELECT * FROM pg_catalog.pg_database WHERE datname = '%s'",
                        databaseName));
        return results.next();
    }

    @Override
    public void setupRetentionPolicy(String table, ChronoUnit timeUnit, int retentionPeriod)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        // sanity check
        if (retentionPeriod <= 0) {
            logger.error("Invalid retention period provided: {}", retentionPeriod);
            return;
        }

        final String timeUnitSql = SUPPORTED_TIME_UNIT_FOR_RETENTION_POLICY.get(timeUnit);
        if (timeUnitSql == null) {
            logger.error("Unsupported time unit {}", timeUnit);
            return;
        }

        try (Connection conn = getNonRootConnection()) {
            // do it in a transaction
            conn.setAutoCommit(false);
            // first drop previous policy if it exists
            conn.createStatement().execute(String.format(
                    "SELECT remove_drop_chunks_policy('%s', if_exists => true)", table));
            // create new retention policy and get its background job id
            ResultSet resultSet = conn.createStatement().executeQuery(String.format(
                    "SELECT add_drop_chunks_policy('%s', INTERVAL '%d %s', "
                            + "cascade_to_materializations => FALSE)", table, retentionPeriod, timeUnitSql));
            if (!resultSet.next()) {
                logger.error("Unable to create add_drop_chunks_policy for table \"{}\" with period \"{} {}\"",
                        table, retentionPeriod, timeUnit);
                return;
            }

            final int jobId = resultSet.getInt("add_drop_chunks_policy");
            // set the drop_chunks background job to run every day, starting from next midnight
            conn.createStatement().execute(String.format("SELECT alter_job_schedule(%d, "
                    + "schedule_interval => INTERVAL '1 days', "
                    + "next_start => date_trunc('DAY', now()) + INTERVAL '1 day');", jobId));
            conn.commit();
            logger.info("Created retention policy for table \"{}\" with period \"{} {}\"", table,
                    retentionPeriod, timeUnit);
        }
    }
}
