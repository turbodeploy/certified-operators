package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import javax.sql.DataSource;

import org.mariadb.jdbc.MariaDbDataSource;

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
    DataSource getDataSource(String url, String user, String password)
            throws SQLException {
        final MariaDbDataSource dataSource = new MariaDbDataSource();
        dataSource.setUrl(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        dataSource.setDatabaseName(config.getDbDatabaseName());
        return dataSource;
    }

    @Override
    protected void createSchema() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection(null)) {
            execute(conn, String.format("CREATE DATABASE IF NOT EXISTS %s",
                    config.getDbDatabaseName()));
        }
    }

    @Override
    protected void createNonRootUser() throws UnsupportedDialectException, SQLException {
        try (Connection conn = getRootConnection(null)) {
            dropUser(conn, config.getDbUserName());
            execute(conn, String.format("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'",
                    config.getDbUserName(), config.getDbPassword()));
        }
    }

    private void dropUser(final Connection conn, final String user) {
        // DROP USER IF NOT EXISTS is not supported in MySQL until v5.7, and breaks jenkins builds
        if (!config.getDbDestructiveProvisioningEnabled()) {
            return;
        }
        try {
            execute(conn, String.format("DROP USER '%s@%%'", user));
        } catch (SQLException e) {
            logger.info("DROP USER failed; assuming user did not exist");
        }
    }

    @Override
    protected void performNonRootGrants(DbEndpointAccess access) throws SQLException, UnsupportedDialectException {
        String privileges = getPrivileges(access);
        try (Connection conn = getRootConnection(config.getDbDatabaseName())) {
            execute(conn, String.format("GRANT %s ON `%s`.* TO '%s'@'%%'",
                    privileges, config.getDbDatabaseName(), config.getDbUserName()));
        }
    }

    private String getPrivileges(DbEndpointAccess access) {
        switch (access) {
            case ALL:
                return "ALL PRIVILEGES";
            case READ_ONLY:
                return "SELECT";
            case READ_WRITE_DATA:
                throw new UnsupportedOperationException("Unsupported DB endpoint access: " + access.name());
            default:
                throw new IllegalArgumentException("Unknown DB endpoint access: " + access.name());
        }
    }

    @Override
    protected Collection<String> getAllTableNames(final Connection conn) {
        // TODO implement when we start to use mysql endpoints
        return Collections.emptySet();
    }

    @Override
    protected void dropDatabaseIfExists(final Connection conn) {
        // TODO implement when we start to use mysql endpoints
    }

    @Override
    protected void dropUserIfExists(final Connection conn) {
        // TODO implement when we start to use mysql endpoints
    }
}
