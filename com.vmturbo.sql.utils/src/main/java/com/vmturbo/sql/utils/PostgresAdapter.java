package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * {@link DbAdapter} implementation for PostgreSQL endpoints.
 */
class PostgresAdapter extends DbAdapter {

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
}
