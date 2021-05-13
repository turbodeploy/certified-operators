package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

    /** Prefix for the name of the readers group role. */
    private static final String READERS_GROUP_ROLE_PREFIX = "readers";

    /** Prefix for the name of the writers group role. */
    private static final String WRITERS_GROUP_ROLE_PREFIX = "writers";

    private static final String DUPLICATE_DATABASE_SQLSTATE = "42P04";
    private static final String DUPLICATE_OBJECT_SQLSTATE = "42710";

    PostgresAdapter(final DbEndpointConfig config) {
        super(config);
    }

    @Override
    DataSource getDataSource(String url, String user, String password) {
        final PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        // The flyway migrator (version 4.x) does not quote postgres schemas by default.
        // This means that multi-tenant schema names (which contain "-", an illegal character
        // in Postgres) do not get migrated properly. Add the quotes manually.
        dataSource.setCurrentSchema("\"" + config.getSchemaName() + "\"");
        return dataSource;
    }

    @Override
    protected void createNonRootUser() throws SQLException, UnsupportedDialectException {
        final String userName = config.getUserName();
        createRoleIfNotExists(userName, config.getPassword());
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("ALTER USER \"%s\" SET search_path TO \"%s\"",
                    userName, config.getSchemaName()));
        }
    }

    @Override
    protected void createReadersGroup() throws SQLException, UnsupportedDialectException {
        // create readers group role if it doesn't exist
        final String readersGroupRoleName = getGroupName(READERS_GROUP_ROLE_PREFIX);
        createRoleIfNotExists(readersGroupRoleName, null);
        // grant privileges to read only user
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("GRANT CONNECT ON DATABASE \"%s\" TO \"%s\"",
                    config.getDatabaseName(), readersGroupRoleName));
            execute(conn, String.format("GRANT USAGE ON SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), readersGroupRoleName));
            execute(conn, String.format("GRANT SELECT ON ALL TABLES IN SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), readersGroupRoleName));
        }
    }

    @Override
    protected void createWritersGroup() throws SQLException, UnsupportedDialectException {
        // create writers group role if it doesn't exist
        final String writersGroupRoleName = getGroupName(WRITERS_GROUP_ROLE_PREFIX);
        createRoleIfNotExists(writersGroupRoleName, null);
        // grant privileges to writer user
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("GRANT CONNECT ON DATABASE \"%s\" TO \"%s\"",
                    config.getDatabaseName(), writersGroupRoleName));
            execute(conn, String.format("GRANT USAGE ON SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), writersGroupRoleName));
            execute(conn, String.format("GRANT ALL ON ALL TABLES IN SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), writersGroupRoleName));
        }
    }

    /**
     * Get the name of a group role for current db config.
     *
     * <p>THe name is created by starting with a prefix and appending the database name and, if
     * it's not the same, the schema name.</p>
     *
     * @param prefix prefix to use for group name
     * @return readers group role name
     */
    private String getGroupName(String prefix) {
        final String dbName = config.getDatabaseName();
        final String schemaName = config.getSchemaName();

        return dbName.equals(schemaName) ? String.join("_", prefix, dbName)
                : String.join("_", prefix, dbName, schemaName);
    }

    @Override
    protected String obscurePasswords(final String sql) {
        return sql.replaceAll("(\\bWITH\\s+PASSWORD\\s*')([^']*)'", "$1***'");
    }

    @Override
    protected void performNonRootGrants() throws SQLException, UnsupportedDialectException {
        final DbEndpointAccess access = config.getAccess();
        switch (access) {
            case ALL:
                performAllGrants();
                alterDefaultPrivileges();
                break;
            case READ_WRITE_DATA:
                performWriteGrants();
            case READ_ONLY:
                performROGrants();
                break;
            default:
                throw new IllegalAccessError("Unknown DB endpoint access: " + access.name());
        }
    }

    /**
     * We want the db-provisioning endpoint user to be the owner of the database and schema created
     * for the endpoint. This method should only be invoked when the current endpoint has
     * db-provisioning responsibility.
     */
    @Override
    protected void provisionForMigrations() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("ALTER DATABASE \"%s\" OWNER TO \"%s\"",
                    config.getDatabaseName(), config.getUserName()));
            execute(conn, String.format(String.format("ALTER SCHEMA \"%s\" OWNER TO \"%s\"",
                    config.getSchemaName(), config.getUserName())));
        }
    }

    private void performAllGrants() throws UnsupportedDialectException, SQLException {
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("GRANT ALL PRIVILEGES ON SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
        }
    }

    private void performWriteGrants() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
            // make this user a member of the writers group, so it inherits the privileges
            execute(conn, String.format("GRANT \"%s\" TO \"%s\"",
                    getGroupName(WRITERS_GROUP_ROLE_PREFIX), config.getUserName()));
        }
    }

    private void performROGrants() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
            // make this user a member of the readers group, so it inherits the privileges
            execute(conn, String.format("GRANT \"%s\" TO \"%s\"",
                    getGroupName(READERS_GROUP_ROLE_PREFIX), config.getUserName()));
        }
    }

    /**
     * Alter the default privileges for the schema so that the reader and writer groups
     * automatically - and therefore their members - automatically get needed access to newly
     * created tables.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem gaining access
     */
    private void alterDefaultPrivileges() throws UnsupportedDialectException, SQLException {
        // we must perform this operation if this endpoint's non-root user is capable of creating
        // new tables, and we must do it while logged in as that user.
        if (config.getAccess().canCreateNewTable()) {
            try (Connection conn = getNonRootConnection()) {
                execute(conn, String.format("ALTER DEFAULT PRIVILEGES IN SCHEMA \"%s\" "
                                + "GRANT SELECT ON TABLES TO \"%s\"",
                        config.getSchemaName(), getGroupName(READERS_GROUP_ROLE_PREFIX)));
                execute(conn, String.format("ALTER DEFAULT PRIVILEGES IN SCHEMA \"%s\" "
                                + "GRANT ALL ON TABLES TO \"%s\"",
                        config.getSchemaName(), getGroupName(WRITERS_GROUP_ROLE_PREFIX)));
            }
        }
    }

    @Override
    protected void createSchema() throws SQLException, UnsupportedDialectException {
        createDatabaseIfNotExists(config.getDatabaseName());
        try (Connection conn = getRootConnection()) {
            // we don't really want a "public" schema here, but more importantly, when it's created
            // during database creation, the timescaledb plugin is automatically installed in it,
            // and that prevents it being installed in the endpoint schema, where we want it.
            execute(conn, "DROP SCHEMA IF EXISTS public CASCADE");
            execute(conn, String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", config.getSchemaName()));
            setupTimescaleDb();
        }
    }

    /**
     * Create a database if it doesn't already exist.
     *
     * <p>This is required because there is no `IF NOT EXISTS` syntax on `CREATE DATABASE`</p>
     *
     * @param name database name
     * @throws SQLException                if the operation fails for any reason other than that the
     *                                     database already exists
     * @throws UnsupportedDialectException for unsupported dialect
     */
    private void createDatabaseIfNotExists(String name)
            throws UnsupportedDialectException, SQLException {
        try (Connection conn = getRootConnection(null)) {
            execute(conn, String.format("CREATE DATABASE \"%s\"", name));
        } catch (SQLException e) {
            if (e.getSQLState().equals(DUPLICATE_DATABASE_SQLSTATE)) {
                logger.info("Database {} already exists", name);
            } else {
                throw e;
            }
        }
    }

    /**
     * Create a user/role if it doesn't already exist.
     *
     * <p>This is required because there is no `IF NOT EXISTS` syntax on `CREATE ROLE`</p>
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
    private void createRoleIfNotExists(String name, String password)
            throws UnsupportedDialectException, SQLException {
        final String roleOrUser = password != null ? "USER" : "ROLE";
        try (Connection conn = getRootConnection(null)) {
            try {
                execute(conn, String.format("CREATE %s \"%s\"", roleOrUser, name));
            } catch (SQLException e) {
                if (e.getSQLState().equals(DUPLICATE_OBJECT_SQLSTATE)) {
                    logger.info("Role {} already exists", name);
                } else {
                    throw e;
                }
            }
            if (password != null) {
                execute(conn, String.format("ALTER USER \"%s\" WITH PASSWORD '%s'",
                        name, config.getPassword()));
            }
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
     * @throws SQLException                if there's a problem adding the extension
     * @throws UnsupportedDialectException on unsupported dialect
     */
    protected void setupTimescaleDb() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA \"%s\"",
                    config.getSchemaName()));
        }
    }

    @Override
    public void setupRetentionPolicy(String table, ChronoUnit timeUnit, int retentionPeriod)
            throws UnsupportedDialectException, SQLException {
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
                    "SELECT remove_retention_policy('%s', if_exists => true)", table));
            // create new retention policy and get its background job id
            ResultSet resultSet = conn.createStatement().executeQuery(String.format(
                    "SELECT add_retention_policy('%s', INTERVAL '%d %s')",
                    table, retentionPeriod, timeUnit));
            if (!resultSet.next()) {
                logger.error("Unable to create retention policy for table \"{}\" with period \"{} {}\"",
                        table, retentionPeriod, timeUnit);
                return;
            }

            final int jobId = resultSet.getInt("add_retention_policy");
            // set the retention job to run every day, starting from next midnight
            conn.createStatement().execute(String.format("SELECT alter_job(%d, "
                    + "schedule_interval => INTERVAL '1 days', "
                    + "next_start => date_trunc('DAY', now()) + INTERVAL '1 day');", jobId));
            conn.commit();
            logger.info("Created retention policy for table \"{}\" with period \"{} {}\"", table,
                    retentionPeriod, timeUnit);
        }
    }

    @Override
    public void setConnectionUser(final Connection conn, final String userName) throws SQLException {
        conn.getClientInfo().setProperty("ClientUser", userName);
    }

    @Override
    public String getConnectionUser(final Connection conn) throws SQLException {
        return conn.getClientInfo().getProperty("ClientUser");
    }

    @Override
    protected Collection<String> getAllTableNames(Connection conn) throws SQLException {
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
            execute(conn, String.format("DROP DATABASE IF EXISTS \"%s\" CASCADE",
                    config.getDatabaseName()));
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop database {}", config.getDatabaseName(), e);
        }
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("DROP USER IF EXISTS \"%s\" CASCADE",
                    config.getUserName()));
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop user {}", config.getUserName(), e);
        }
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("DROP ROLE IF EXISTS \"%s\" CASCADE",
                    getGroupName(READERS_GROUP_ROLE_PREFIX)));
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop readers group {}",
                    getGroupName(READERS_GROUP_ROLE_PREFIX), e);
        }
        try (Connection conn = getRootConnection()) {
            execute(conn, String.format("DROP ROLE IF EXISTS \"%s\" CASCADE",
                    getGroupName(WRITERS_GROUP_ROLE_PREFIX)));
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to drop writers group {}",
                    getGroupName(WRITERS_GROUP_ROLE_PREFIX), e);
        }
    }
}
