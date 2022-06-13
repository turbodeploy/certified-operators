package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.mariadb.jdbc.MariaDbDataSource;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * {@link DbAdapter} implementation for MySql and MariaDB endpoints.
 */
class MySqlFamilyAdapter extends DbAdapter {

    MySqlFamilyAdapter(final DbEndpointConfig config) {
        super(config);
    }

    @Override
    protected DataSource createUnpooledDataSource(String url, String user, String password)
            throws SQLException {
        final MariaDbDataSource dataSource = new MariaDbDataSource();
        dataSource.setUrl(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        dataSource.setDatabaseName(config.getDatabaseName());
        return dataSource;
    }

    @Override
    protected void createSchema() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
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
        return sql.replaceAll("(\\bSET\\s+PASSWORD\\s+(?:FOR\\s+`[^`]*`@`[^`]+`)\\s+)?=\\s+password\\(\\s*'([^']*)'", "$1***'");
    }

    @Override
    protected void performNonRootGrants() throws SQLException, UnsupportedDialectException {
        String privileges = getPrivileges(config.getAccess());
        try (Connection conn = getPrivilegedConnection()) {
            execute(conn, String.format("GRANT %s ON `%s`.* TO '%s'@'%%'",
                    privileges, config.getDatabaseName(), config.getUserName()));
            execute(conn, "FLUSH PRIVILEGES");
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
        try (Connection conn = getRootConnection()) {
            try {
                // we need to include password on CREATE USER operation in order work when
                // MySQL password policies are active
                execute(conn, String.format("CREATE USER `%s`@`%%` IDENTIFIED BY '%s'",
                        name, password));
            } catch (SQLException e) {
                logger.info("Presuming role {} already exists {}", name, e.getSQLState());
            }
            if (password != null) {
                try {
                    execute(conn, String.format("SET PASSWORD FOR `%s`@`%%` = password('%s')",
                            name, config.getPassword()));
                } catch (SQLException e) {
                    // replace the thrown exception with one that loses the upstream stack trace,
                    // since otherwise we risk exposing password in logs
                    throw copySQLExceptionWithoutStack(
                            String.format("Failed to set password for user `%s`@'%%'", name), e);
                }
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
    public void tearDown() {
        try (Connection conn = getPrivilegedConnection()) {
            execute(conn, String.format("DROP USER `%s`@`%%`", config.getUserName()));
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop user {}", config.getUserName(), e);
        }
        try (Connection conn = getPrivilegedConnection()) {
            execute(conn, String.format("DROP DATABASE `%s`", config.getDatabaseName()));
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop database {}", config.getDatabaseName(), e);
        }
    }

    @Override
    protected String quote(final String name) {
        return "`" + name + "`";
    }
}
