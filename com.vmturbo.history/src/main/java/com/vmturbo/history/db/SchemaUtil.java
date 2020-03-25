package com.vmturbo.history.db;

import static org.jooq.impl.DSL.using;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.mariadb.jdbc.MariaDbDataSource;

import com.vmturbo.history.flyway.ResetChecksumsForMyIsamInfectedMigrations;
import com.vmturbo.history.flyway.V1_28_1_And_V1_35_1_Callback;
import com.vmturbo.sql.utils.flyway.ForgetMigrationCallback;

public class SchemaUtil {
    protected static final Logger logger = Logger.getLogger("com.vmturbo.history.db");

    /**
     * Mapping from SQL dialects to the corresponding java class name.
     */
    public static final ImmutableMap<SQLDialect, String> ADAPTER_TO_DRIVER = ImmutableMap.of(
        SQLDialect.MYSQL, "com.mysql.jdbc.Driver",
        SQLDialect.MARIADB, "org.mariadb.jdbc.Driver");

    /**
     * The names of the SQL adapters we support.
     */
    public static final ImmutableMap<String, SQLDialect> SUPPORTED_ADAPTERS = ImmutableMap.of(
        SQLDialect.MYSQL.getNameLC(), SQLDialect.MYSQL,
        SQLDialect.MARIADB.getNameLC(), SQLDialect.MARIADB);

    private static String COMM_SEP = "\\|";
    private static Pattern COMM_SPLITTER = Pattern.compile(COMM_SEP);
    private static String COMM_ATT_SEP = "/";
    private static Pattern COMM_ATT_SPLITTER = Pattern.compile(COMM_ATT_SEP);
    private static String COMM_GET_ALL = ".*";

    /**
     * Returns true if the dialect that this appliance is working against
     * is a of the MySQL family of databases.
     *
     * @param adapter
     * @return
     */
    public static boolean isMysqlAdapter(String adapter) {
        SQLDialect dialect = SUPPORTED_ADAPTERS.get(adapter);
        return dialect== SQLDialect.MYSQL || dialect== SQLDialect.MARIADB;
    }

    /**
     * Initializes the DB to a given version.
     * @param version The version to use.
     * @param clean Whether or not to clean the database first (drops all tables and data).
     * @param migrationLocationOverride If set, contains a specific classpath location to look for
     *                                  migration files.
     * @return The number of successfully applied migrations.
     */
    public static int initDb(@Nullable final Double version,
                             final boolean clean,
                             final Optional<String> migrationLocationOverride) {
        logger.info("Initializing vmtdb"
            + ((version != null) ? " at version " + version : "")
            + "...");

        Flyway fway = flyway(migrationLocationOverride);
        if (clean)
            fway.clean();
        if (version != null) {
            fway.setTarget(MigrationVersion.fromVersion(version.toString()));
        }

        int migs = fway.migrate();
        logger.info("Initialization done.");
        return migs;
    }

    /**
     * Default classpath locations for migrations.
     * @return
     */
    public static List<String> locations() {
        return Lists.newArrayList(
            "db/migration");
    }

    /**
     * Return a new instance of the migration utility Flyway.
     *
     * @param migrationLocationOverride - Migration locations in the classpath
     * @return The {@link Flyway} object.
     */
    private static Flyway flyway(@Nonnull final Optional<String> migrationLocationOverride) {
        Flyway fway = new Flyway();
        fway.setDataSource(DBConnectionPool.instance.getInternalPool());

        List<String> locations = migrationLocationOverride
            .map(Collections::singletonList)
            .orElseGet(SchemaUtil::locations);

        fway.setLocations(locations.toArray(new String[]{}));
        fway.setCallbacks(
                // V1.27 migrations collided when 7.17 and 7.21 branches were merged
                new ForgetMigrationCallback("1.27"),
                // three migrations were changed in order to remove mention of MyISAM DB engine
                new ResetChecksumsForMyIsamInfectedMigrations(),
                // V1.28.1 and V1.35.1 java migrations needed to change
                // V1.28.1 formerly supplied a checksum but no longer does
                new V1_28_1_And_V1_35_1_Callback()
        );
        return fway;
    }

    /**
     * Initializes the DB at the required version.
     *
     * Uses less migration-lookup paths. Intended to be an easy way to initialize
     * databases in development and not used in production.
     */
    public static void devInitDb() throws SQLException {
        Flyway fway = new Flyway();
        fway.setDataSource(defaultDataSource());
        fway.setLocations(locations().toArray(new String[]{}));
        fway.clean();
        fway.migrate();
    }

    /**
     * Clears the DB
     * @param locationsOverride If set, overrides the location in the classpath to look for migrations.
     */
    public static void clearDb(Optional<String> locationsOverride) {
        Flyway flyAway = flyway(locationsOverride);
        flyAway.clean();
    }

    /**
     * Clears the DB
     * @throws SQLException
     * @throws DataAccessException
     */
    public static void dropDb(String dbName) throws VmtDbException {
        Connection conn;
        try {
            conn = DBConnectionPool.instance.getConnection();
        } catch (VmtDbException e) {
            logger.info("There was a problem getting a connection from the connection pool");
            throw e;
        }
        dropDb(dbName, conn);
    }

    /**
     * Clears the DB
     * @throws SQLException
     * @throws DataAccessException
     */
    public static void dropDb(@Nonnull String dbName, @Nonnull Connection conn) throws VmtDbException {
        using(conn).execute("drop database if exists " + dbName + ";");
    }

    /**
     * Creates a DB with the given name using the given Connection, if conn is null use the
     * connection from DBConnectionPool.instance.getConnection().
     *
     * @param dbName The name to be given to the new Db.
     * @param conn The given Connection.
     * @throws VmtDbException Thrown if there is a problem connecting to the connection from
     * DBConnectionPool
     */
    public static void createDb(String dbName, Connection conn) throws VmtDbException {
        if (conn == null) {
            try {
                conn = DBConnectionPool.instance.getConnection();
            } catch (VmtDbException e) {
                logger.info("There was a problem getting a connection from the connection pool");
                throw e;
            }
        }
        using(conn).execute("CREATE DATABASE IF NOT EXISTS `" + dbName +
            "` DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = utf8_unicode_ci;");
    }

    /*
     * ------------------------------------------------------
     *   UTILITY FOR DEVELOPERS TO INITALIZE A DEV DATABASE
     * ------------------------------------------------------
     */

    public static Connection rootConnection() throws SQLException {
        try {
            return SchemaUtil.defaultDataSource().getConnection();
        } catch (SQLException e) {
            logger.error("Unable to retrieve root connection", e);
            throw e;
        }
    }

    /**
     * Returns a newly created DataSource using default values.
     */
    private static DataSource defaultDataSource() throws SQLException {
        final String url = BasedbIO.instance().getMySQLConnectionUrl();
        final MariaDbDataSource mysqlDS = new MariaDbDataSource(url);
        mysqlDS.setUser(BasedbIO.instance().getRootUsername());
        mysqlDS.setPassword(BasedbIO.instance().getRootPassword());
        return mysqlDS;
    }
}
