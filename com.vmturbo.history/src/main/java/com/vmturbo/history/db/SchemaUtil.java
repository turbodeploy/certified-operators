package com.vmturbo.history.db;

import static org.jooq.impl.DSL.using;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.mariadb.jdbc.MariaDbDataSource;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class SchemaUtil {
    protected static final Logger logger = Logger.getLogger("com.vmturbo.history.db");
    private static final String DEFAULT_USER_NAME = "root";


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
     * @param version
     */
    public static int initDb(Double version, boolean clean, String... additionallLocations) {
        logger.info("Initializing vmtdb"
            + ((version != null) ? " at version " + version : "")
            + "...");

        Flyway fway = flyway(additionallLocations);
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
     * @param addlLocations - Migration locations in the classpath
     * @return
     */
    private static Flyway flyway(String... addlLocations) {
        Flyway fway = new Flyway();
        fway.setDataSource(DBConnectionPool.instance.getInternalPool());

        List<String> locations = locations();
        locations.addAll(Arrays.asList(addlLocations));

        fway.setLocations(locations.toArray(new String[]{}));
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
     */
    public static void clearDb() {
        Flyway flyAway = flyway();
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
        using(conn).execute("CREATE DATABASE IF NOT EXISTS " + dbName +
            " DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = utf8_unicode_ci;");
    }

    /**
     * Phrase an url for a DB using the given attributes.
     *
     * @param adapter The adapter for the DBV.
     * @param hostName The DB host name.
     * @param portNumber The port number.
     * @param databaseName The DB name.
     * @return
     */
    public static String phraseURL(String adapter, String hostName, String portNumber,
                                   String databaseName) {
        if (isMysqlAdapter(adapter)) {
            return "jdbc:" + adapter + "://"+ hostName + ":" + portNumber
                + "/" + databaseName +
                "?useUnicode=true" +
                "&characterEncoding=UTF-8" +
                "&characterSetResults=UTF-8" +
                "&connectionCollation=utf8_unicode_ci";
        }
        return null;
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
        mysqlDS.setUser(DEFAULT_USER_NAME);
        mysqlDS.setPassword(BasedbIO.instance().getRootPassword());
        return mysqlDS;
    }
}
