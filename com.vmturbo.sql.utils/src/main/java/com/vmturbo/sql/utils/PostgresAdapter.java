package com.vmturbo.sql.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

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

    PostgresAdapter(final DbEndpointConfig config) {
        super(config);
    }

    @Override
    protected DataSource createUnpooledDataSource(String url, String user, String password) {
        final PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(url);
        dataSource.setUser(user);
        dataSource.setPassword(password);
        // The flyway migrator (version 4.x) does not quote postgres schemas by default.
        // This means that multi-tenant schema names (which contain "-", an illegal character
        // in Postgres) do not get migrated properly. Add the quotes manually.
        // Also adding public so this is aligned with the search_path.
        dataSource.setCurrentSchema("\"" + config.getSchemaName() + "\",\"public\"");
        return dataSource;
    }

    @Override
    protected void createNonRootUser() throws SQLException, UnsupportedDialectException {
        final String userName = config.getUserName();
        setCreatedUser(createRoleIfNotExists(userName, true));
        setUserPassword(userName, config.getPassword());
        try (Connection conn = getPrivilegedConnection()) {
            execute(conn, String.format("GRANT USAGE, CREATE ON SCHEMA %s TO %s",
                    config.getSchemaName(), userName));
            execute(conn, String.format("GRANT ALL ON DATABASE %s TO %s",
                    config.getDatabaseName(), userName));
            execute(conn,
                    String.format(
                            "ALTER ROLE \"%s\" IN DATABASE \"%s\" SET search_path = \"%s\",\"public\"",
                            userName, config.getDatabaseName(), config.getSchemaName()));
        }
    }

    @Override
    protected void createReadersGroup() throws SQLException, UnsupportedDialectException {
        // create readers group role if it doesn't exist
        final String readersGroupRoleName = getGroupName(READERS_GROUP_ROLE_PREFIX);
        if (createRoleIfNotExists(readersGroupRoleName, false)) {
            createdGroups.add(readersGroupRoleName);
        }
        // grant privileges to read only user
        try (Connection conn = getPrivilegedConnection()) {
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
        if (createRoleIfNotExists(writersGroupRoleName, false)) {
            createdGroups.add(writersGroupRoleName);
        }
        // grant privileges to writer user
        try (Connection conn = getPrivilegedConnection()) {
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
                performWriteGrants();
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
        try (Connection conn = getPrivilegedConnection()) {
            execute(conn, String.format("GRANT ALL ON ALL TABLES IN SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
            execute(conn, String.format("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
            execute(conn, String.format("GRANT ALL ON ALL FUNCTIONS IN SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
        }
    }

    private void performAllGrants() throws UnsupportedDialectException, SQLException {
        try (Connection conn = getPrivilegedConnection()) {
            execute(conn, String.format("GRANT ALL PRIVILEGES ON SCHEMA \"%s\" TO \"%s\"",
                    config.getSchemaName(), config.getUserName()));
        }
    }

    private void performWriteGrants() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getPrivilegedConnection()) {
            // make this user a member of the writers group, so it inherits the privileges
            execute(conn, String.format("GRANT \"%s\" TO \"%s\"",
                    getGroupName(WRITERS_GROUP_ROLE_PREFIX), config.getUserName()));
        }
    }

    private void performROGrants() throws SQLException, UnsupportedDialectException {
        try (Connection conn = getPrivilegedConnection()) {
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
            try (Connection conn = getNonRootConnection(false)) {
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
        setCreatedDatabase(createDatabaseIfNotExists(config.getDatabaseName()));
        setCreatedSchema(createSchemaIfNotExists(config.getSchemaName(), false));
    }

    private boolean createSchemaIfNotExists(String schemaName, boolean setOwner)
            throws SQLException, UnsupportedDialectException {
        if (!schemaExists(schemaName)) {
            try (Connection conn = getPrivilegedConnection()) {
                execute(conn, String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"%s", schemaName,
                        setOwner ? " AUTHORIZATION \"" + config.getUserName() + "\"" : ""));
                return true;
            }
        } else {
            return false;
        }
    }

    private boolean schemaExists(String name) throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(String.format(
                    "SELECT 1 FROM information_schema.schemata WHERE schema_name = '%s'", name));
            // this tests if the result set has any rows; if so, schema already exists
            return rs.next();
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
    private boolean createDatabaseIfNotExists(String name)
            throws UnsupportedDialectException, SQLException {
        if (!databaseExists(name)) {
            try (Connection conn = getRootConnection()) {
                execute(conn, String.format("CREATE DATABASE \"%s\"", name));
                return true;
            }
        } else {
            return false;
        }
    }

    private boolean databaseExists(String name) throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(
                    String.format("SELECT 1 FROM pg_database WHERE datname = '%s'", name));
            // following tests if the result set has any rows; if so, database exists
            return rs.next();
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
     * @param name     role name
     * @param isUser   true if this role can log in as a user
     * @return true if the role was created
     * @throws SQLException                if the operation fails for any reason other than that the
     *                                     role already exists
     * @throws UnsupportedDialectException for unsupported dialect
     */
    private boolean createRoleIfNotExists(String name, boolean isUser)
            throws UnsupportedDialectException, SQLException {
        if (!roleExists(name)) {
            final String roleOrUser = isUser ? "USER" : "ROLE";
            try (Connection conn = getRootConnection()) {
                execute(conn, String.format("CREATE %s \"%s\"", roleOrUser, name));
                return true;
            }
        } else {
            return false;
        }
    }

    private boolean roleExists(String name) throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
            ResultSet rs = conn.createStatement().executeQuery(String.format(
                    "SELECT 1 FROM pg_roles WHERE rolname = '%s'", name));
            // following checks if result set has any rows; if so, role already exists
            return rs.next();
        }
    }

    private void setUserPassword(String userName, String password)
            throws SQLException, UnsupportedDialectException {
        try (Connection conn = getRootConnection()) {
            try {
                execute(conn, String.format("ALTER USER \"%s\" WITH PASSWORD '%s'",
                        userName, password));
            } catch (SQLException e) {
                throw copySQLExceptionWithoutStack(
                        String.format("Failed to set password for user `%s`@'%%'", userName),
                        e);
            }
        }
    }

    @Override
    protected void createPlugins() throws SQLException, UnsupportedDialectException {
        for (DbPlugin plugin : config.getPlugins()) {
            if (plugin instanceof PostgresPlugin) {
                Optional<String> pluginSchema = ((PostgresPlugin)plugin).getPluginSchema();
                if (pluginSchema.isPresent()) {
                    createSchemaIfNotExists(pluginSchema.get(), true);
                }
                try (Connection conn = getPrivilegedConnection()) {
                    String schemaName = pluginSchema.orElse(config.getSchemaName());
                    execute(conn, plugin.getInstallSQL(schemaName));

                    execute(conn,
                            String.format("GRANT ALL ON ALL TABLES IN SCHEMA \"%s\" TO \"%s\"",
                                    schemaName, config.getUserName()));
                    execute(conn,
                            String.format("GRANT ALL ON ALL ROUTINES IN SCHEMA \"%s\" TO \"%s\"",
                                    schemaName, config.getUserName()));
                }
            } else {
                logger.error("Non-Postgres plugin declared for Postgres endpoint: {}",
                        plugin.getName());
            }
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
            try (Statement statement = conn.createStatement()) {
                statement.execute(String.format(
                    "SELECT remove_retention_policy('%s', if_exists => true)", table));
            }
            // create new retention policy and get its background job id
            final int jobId;
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(String.format(
                    "SELECT add_retention_policy('%s', INTERVAL '%d %s')",
                    table, retentionPeriod, timeUnit))) {
                if (!resultSet.next()) {
                    logger.error("Unable to create retention policy for table \"{}\" with period \"{} {}\"",
                            table, retentionPeriod, timeUnit);
                    return;
                }

                jobId = resultSet.getInt("add_retention_policy");
            }
            // set the retention job to run every day, starting from next midnight
            try (Statement statement = conn.createStatement()) {
                statement.execute(String.format("SELECT alter_job(%d, "
                    + "schedule_interval => INTERVAL '1 days', "
                    + "next_start => date_trunc('DAY', now()) + INTERVAL '1 day');", jobId));
            }
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
    public void tearDown() {
        if (createdSchema.getValue().orElse(false)) {
            try (Connection conn = getRootConnection("postgres")) {
                execute(conn,
                        String.format("DROP SCHEMA IF EXISTS \"%s\"", config.getSchemaName()));
            } catch (SQLException | UnsupportedDialectException e) {
                throw new RuntimeException(e);
            }
        }
        if (createdDatabase.getValue().orElse(false)) {
            try (Connection conn = getRootConnection("postgres")) {
                execute(conn, String.format("DROP DATABASE IF EXISTS \"%s\"",
                        config.getDatabaseName()));
            } catch (UnsupportedDialectException | SQLException e) {
                logger.error("Failed to drop database {}", config.getDatabaseName(), e);
            }
        }
        if (createdUser.getValue().orElse(false)) {
            try (Connection conn = getRootConnection("postgres")) {
                execute(conn, String.format("DROP USER IF EXISTS \"%s\"",
                        config.getUserName()));
            } catch (UnsupportedDialectException | SQLException e) {
                logger.error("Failed to drop user {}", config.getUserName(), e);
            }
        }
        createdGroups.forEach(group -> {
            try (Connection conn = getRootConnection("postgres")) {
                execute(conn, String.format("DROP ROLE IF EXISTS \"%s\"", group));
            } catch (UnsupportedDialectException | SQLException e) {
                logger.error("Failed to drop group {}", group, e);
            }
        });
    }

    @Override
    protected String quote(final String name) {
        return "\"" + name + "\"";
    }
}
