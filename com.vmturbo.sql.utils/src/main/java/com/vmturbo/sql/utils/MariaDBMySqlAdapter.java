package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.sql.DataSource;

import org.mariadb.jdbc.MariaDbDataSource;
import org.mariadb.jdbc.MariaDbPoolDataSource;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

// TODO This adapter implementation - and the whole DbEndpoint approach in general - has not yet
// been fully ported to work in MySQL/MariaDB. Initial focus on needs for XLR floodgate

/**
 * {@link DbAdapter} implementation for MySql and MariaDB endpoints.
 */
class MariaDBMySqlAdapter extends DbAdapter {

    MariaDBMySqlAdapter(final DbEndpointConfig config) {
        super(config);
    }

    @Override
    DataSource getDataSource(String url, String user, String password, boolean pooled)
            throws SQLException {
        if (pooled) {
            final MariaDbPoolDataSource dataSource = new MariaDbPoolDataSource();
            dataSource.setUrl(url);
            dataSource.setUser(user);
            dataSource.setPassword(password);
            dataSource.setDatabaseName(config.getDatabaseName());
            dataSource.setMinPoolSize(config.getMinPoolSize());
            dataSource.setMaxPoolSize(config.getMaxPoolSize());
            return dataSource;
        } else {
            final MariaDbDataSource dataSource = new MariaDbDataSource();
            dataSource.setUrl(url);
            dataSource.setUser(user);
            dataSource.setPassword(password);
            return dataSource;
        }
    }

    @Override
    protected void createSchema() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection(null)) {
            execute(conn, String.format("CREATE DATABASE IF NOT EXISTS %s",
                    config.getDatabaseName()));
        }
    }

    @Override
    protected void createNonRootUser() throws UnsupportedDialectException, SQLException {
        createUserIfNotExists(config.getUserName(), config.getPassword());
    }

    @Override
    protected String obscurePasswords(final String sql) {
        return sql.replaceAll("(\\bSET\\s+PASSWORD\\b.=\\s*')([^']*)'", "$1***'");
    }

    @Override
    protected void performNonRootGrants() throws SQLException, UnsupportedDialectException {
        String privileges = getPrivileges(config.getAccess());
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("GRANT %s ON `%s`.* TO '%s'@'%%'",
                    privileges, config.getDatabaseName(), config.getUserName()));
        }
    }

    /**
     * Create a user if it doesn't already exist.
     *
     * <p>This is required because there is no `IF NOT EXISTS` syntax on `CREATE USER` in
     * MySQL versions that we must support.</p>
     *
     * <p>If the `password` parameter is not null, it is set as the password for the user.
     * (We don't include it in `CREATE USER` because we want it re-done on restart, in case the
     * password has been reset to a value that doesn't match the configuration.</p>
     *
     * @param name     user name
     * @param password password, or null to not set a password
     * @throws UnsupportedDialectException for unsupported dialect
     * @throws SQLException                if the operation fails for any reason other than that the
     *                                     role already exists
     */
    private void createUserIfNotExists(String name, String password)
            throws UnsupportedDialectException, SQLException {
        try (Connection conn = getRootConnection(null)) {
            try {
                execute(conn, String.format("CREATE USER `%s`@`%%`", name));
            } catch (SQLException e) {
                if (e.getSQLState().equals("") || true) {
                    logger.info("Role {} already exists {}", name, e.getSQLState());
                } else {
                    throw e;
                }
            }
            if (password != null) {
                execute(conn, String.format("SET PASSWORD FOR `%s`@`%%` = password('%s')",
                        name, config.getPassword()));
            }
        }
    }

    private String getPrivileges(DbEndpointAccess access) {
        switch (access) {
            case ALL:
                return "ALL PRIVILEGES";
            case READ_ONLY:
                return "SELECT";
            case READ_WRITE_DATA:
                return "SELECT, INSERT, UPDATE, DELETE, SHOW VIEW";
            default:
                throw new IllegalArgumentException("Unknown DB endpoint access: " + access.name());
        }
    }

    @Override
    public void setConnectionUser(final Connection conn, final String userName) throws SQLException {
        conn.setClientInfo("ClientUser", userName);
    }

    @Override
    public String getConnectionUser(final Connection conn) throws SQLException {
        return conn.getClientInfo("ClientUser");
    }

    @Override
    protected Collection<String> getAllTableNames(final Connection conn) throws SQLException {
        ResultSet results = conn.createStatement().executeQuery(String.format(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' "
                        + "AND table_type = 'BASE TABLE' AND table_name != 'schema_version'",
                config.getSchemaName()));
        List<String> tables = new ArrayList<>();
        while (results.next()) {
            tables.add(results.getString(1));
        }
        return tables;
    }

    @Override
    protected void tearDown() {
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("DROP DATABASE IF EXISTS `%s`",
                    config.getDatabaseName()));
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop database {}", config.getDatabaseName(), e);
        }
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("DROP USER IF EXISTS `%s`@`%%`",
                    config.getUserName()));
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop user {}", config.getUserName(), e);
        }
    }
}
