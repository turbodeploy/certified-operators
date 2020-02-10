package com.vmturbo.history.db;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.vmturbo.sql.utils.JooqQueryTrimmer.trimJooqErrorMessage;
import static com.vmturbo.sql.utils.JooqQueryTrimmer.trimJooqQuery;
import static com.vmturbo.sql.utils.JooqQueryTrimmer.trimJooqQueryString;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Insert;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.DataChangedException;
import org.jooq.exception.DataTypeException;
import org.jooq.exception.DetachedException;
import org.jooq.exception.InvalidResultException;
import org.jooq.exception.MappingException;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.springframework.beans.factory.annotation.Value;

import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.Period;
import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.commons.Units;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.OptScope;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Basic access functions for JOOQ-based access to vmtdb database.
 **/
public abstract class BasedbIO {

    /**
     * Allows to map from one schema name to another. For example, in unit tests
     * we map 'vmtdb' to 'vmt_testdb'. This mapping affects all of the queries
     * created using JOOQ's dialect.
     *
     * <p>NOTE: Setting of this variable <b>must</b> be done <b>before</b> the
     * first call to JooqBuilder on a given instance.</p>
     */
    @VisibleForTesting
    public static String mappedSchemaForTests = null;

    public abstract String httpExecuteReport(String reportUrl) throws IOException;

    /** Style for executing queries */
    public enum Style {
        /**
         * Immediate execution; the caller will handle failures.  A call with
         * this style will attempt to wait no more than
         * {@link #retryTimeLimitImmediateMsec} milliseconds before either
         * succeeding or throwing an exception indicating failure.  (Because
         * of possible delays further down the stack, the maximum delay cannot
         * always be enforced.)  The {@code IMMEDIATE} style is intended to be
         * used by code that needs are relatively quick pass/fail result for
         * simple database accesses, perhaps because it is locking some external
         * resource.  It must have some way of recovering from failures, since
         * by their nature database transactions are expected to fail now and
         * then.
         */
        IMMEDIATE,

        /**
         * Patient execution; the caller will handle failures.
         * A call with this style will attempt to wait no more than
         * {@link #retryTimeLimitPatientMsec} milliseconds before either
         * succeeding or throwing an exception indicating failure.  (Because
         * of possible delays further down the stack, the maximum delay cannot
         * always be enforced.)  This style of execution gives the operation
         * 6 times as long as the {@code IMMEDIATE} style.  The expectations
         * about programming style described for the {@code IMMEDIATE} style
         * apply, though the application must be able to tolerate much longer
         * potential delays.  Conversely, {@code PATIENT} mode can be used with
         * transactions that are significantly more complex and would likely
         * time out frequently if executed in the {@code IMMEDIATE} style.
         */
        PATIENT,

        /**
         * Forced execution.  A call with this style will be repeated,
         * nominally indefinitely - but actually with a time limit of
         * {@link #retryTimeLimitForcedMsec}
         * milliseconds before giving up and throwing an exception, which
         * should be considered fatal for the particular query.
         */
        FORCED
    }

    /**
     * Basic description, intended to drive further action, of an exception
     * thrown by Jooq, JDBC, and related software.
     */
    public enum DBFailureType {
        /**
         * A failure that may go away if we retry the operation.
         */
        SOFT,

        /**
         * Invalid query.  Includes things like value too large for column,
         * constraint violation, etc.  Retrying this particular query will
         * fail.
         */
        HARD,

        /**
         * Some kind of server problem.  Probably requires fixes; retries
         * will probably fail.
         */
        SERVER,

        /**
         * Something unexpected.  Used, for example, for status values that
         * should not be found in thrown exceptions - e.g., success or
         * warning.
         */
        STRANGE
    }

    /**
     * Map an SQLSTATE value to a failure type.  SQLSTATE values are 5-character
     * strings consisting of digits and ASCII upper-case characters.  The
     * first two characters define a class of related errors.  To use this
     * table, first check whether the full 5-character string has an entry.
     * If not, check for an entry for the first two characters.  Otherwise,
     * assume {@code DBFailureType.SOFT}.
     */
    protected static final ImmutableMap<String, DBFailureType> STATE_TO_DBFAILURE =
            new ImmutableMap.Builder<String, DBFailureType>()

                    // By SQLSTATE class
                    .put("00", DBFailureType.STRANGE)   // Success
                    .put("01", DBFailureType.STRANGE)   // Warning
                    .put("02", DBFailureType.STRANGE)   // No results
                    .put("03", DBFailureType.HARD)      // SQL statement incomplete
                    .put("07", DBFailureType.HARD)      // Dynamic SQL error
                    .put("08", DBFailureType.SOFT)      // Problem with connection
                    .put("09", DBFailureType.SOFT)      // Triggered action exception
                    .put("0A", DBFailureType.HARD)      // Feature not supported
                    .put("0B", DBFailureType.SOFT)      // Invalid transaction start
                    .put("0D", DBFailureType.SOFT)      // Invalid target type
                    .put("0E", DBFailureType.HARD)      // Invalid schema path
                    .put("0F", DBFailureType.SOFT)      // Locator exception
                    .put("0G", DBFailureType.HARD)      // Reference to NULL table value
                    .put("0H", DBFailureType.SOFT)      // invalid SQLSTATE value
                    .put("0K", DBFailureType.SERVER)    // Resignal, Handler Not Active
                    .put("0W", DBFailureType.HARD)      // Bad Statement in Trigger
                    .put("0Z", DBFailureType.SERVER)    // Diagnostics Exception
                    .put("20", DBFailureType.HARD)      // Case not found
                    .put("21", DBFailureType.HARD)      // Cardinality violation
                    .put("22", DBFailureType.HARD)      // Data exception
                    .put("23", DBFailureType.HARD)      // Integrity constraint error
                    .put("24", DBFailureType.SOFT)      // Invalid cursor state
                    .put("25", DBFailureType.SOFT)      // Invalid transaction state
                    .put("26", DBFailureType.HARD)      // Invalid SQL statement name
                    .put("27", DBFailureType.HARD)      // Triggered data change error
                    .put("28", DBFailureType.HARD)      // Invalid AuthorizationID
                    .put("2A", DBFailureType.HARD)      // Direct SQL syntax issue
                    .put("2D", DBFailureType.SOFT)      // Invalid Transaction End
                    .put("2E", DBFailureType.HARD)      // Invalid Connection Name
                    .put("2F", DBFailureType.HARD)      // SQL Function Exception
                    .put("33", DBFailureType.HARD)      // Invalid SQL Descriptor Name
                    .put("34", DBFailureType.SOFT)      // Invalid Cursor Name
                    .put("35", DBFailureType.HARD)      // Invalid Condition Number
                    .put("36", DBFailureType.HARD)      // Cursor Sensitivity Exception
                    .put("37", DBFailureType.HARD)      // Problem in PREPARE
                    .put("38", DBFailureType.HARD)      // External Function Exception
                    .put("39", DBFailureType.HARD)      // External Function Exception
                    .put("3B", DBFailureType.HARD)      // Invalid SQL Descriptor Name
                    .put("3C", DBFailureType.SOFT)      // Ambiguous Cursor Name
                    .put("3D", DBFailureType.HARD)      // Invalid Catalog Name
                    .put("3F", DBFailureType.HARD)      // Invalid Schema Name
                    .put("40", DBFailureType.SOFT)      // Rollback issues
                    .put("42", DBFailureType.HARD)      // Syntax Error/Access Violation
                    .put("44", DBFailureType.HARD)      // WITH CHECK OPTION Violation
                    .put("45", DBFailureType.SOFT)      // Unhandled user exception
                    .put("46", DBFailureType.HARD)      // Java Errors
                    .put("HY", DBFailureType.SOFT)      // General error
                    .put("IM", DBFailureType.HARD)      // ODBC-specific errors

                    // The following are application-specific
                    .put("51", DBFailureType.SOFT)      // Invalid Application State
                    .put("53", DBFailureType.HARD)      // BAD Oprnd/ Inconsistent Spec
                    .put("54", DBFailureType.HARD)      // SQL or Product Limit Exceeded
                    .put("55", DBFailureType.SOFT)      // Object Not in prereq State
                    .put("56", DBFailureType.HARD)      // Miscellaneous SQL/Product Err
                    .put("57", DBFailureType.SOFT)      // Resource Not Available
                    .put("58", DBFailureType.SOFT)      // System Error

                    // Tailored exceptions to class-level results.
                    .put("40002", DBFailureType.HARD)   // integrity constraint
                    // violation in COMMIT
                    .put("40004", DBFailureType.HARD)   // triggered action exception
                    // during COMMIT
                    .put("IM004", DBFailureType.SOFT)   // allocate ENV handle failed
                    .put("IM005", DBFailureType.SOFT)   // allocate DBC handle failed
                    .put("IM006", DBFailureType.SOFT)   // SQLSetConnectAttr failed
                    .put("IM007", DBFailureType.SOFT)   // no known data source/driver
                    .put("IM013", DBFailureType.SOFT)   // trace file error
                    .put("IM015", DBFailureType.SOFT)   // corrupt file data source

                    .put("HY003", DBFailureType.HARD)   // invalid data type in ARD
                    .put("HY004", DBFailureType.HARD)   // invalid data type
                    .put("HY007", DBFailureType.HARD)   // statement not prepared
                    .put("HY009", DBFailureType.HARD)   // invalid use of null pointer
                    .put("HY012", DBFailureType.HARD)   // invalid transaction op code
                    .put("HY016", DBFailureType.HARD)   // can't modify an IRD
                    .put("HY017", DBFailureType.HARD)   // invalid use of handle
                    .put("HY020", DBFailureType.HARD)   // attempt to concatenate null
                    .put("HY021", DBFailureType.HARD)   // Inconsistent descriptor
                    .put("HY024", DBFailureType.HARD)   // invalid attribute value
                    .put("HY055", DBFailureType.HARD)   // bad string data
                    .put("HY090", DBFailureType.HARD)   // invalid string/buffer length
                    .put("HY091", DBFailureType.HARD)   // invalid descriptor field
                    .put("HY092", DBFailureType.HARD)   // invalid attribute/option id
                    .put("HY095", DBFailureType.HARD)   // Function type out of range
                    .put("HY103", DBFailureType.HARD)   // invalid retrieval code
                    .put("HY104", DBFailureType.HARD)   // invalid precision or scale
                    .put("HY105", DBFailureType.HARD)   // invalid parameter mode
                    .put("HY106", DBFailureType.HARD)   // invalid fetch orientation
                    .put("HY107", DBFailureType.HARD)   // Row value out of range
                    .put("HY109", DBFailureType.HARD)   // invalid cursor position
                    .put("HY110", DBFailureType.HARD)   // invalid driver completion
                    .put("HY111", DBFailureType.HARD)   // invalid bookmark value
                    .put("HYC00", DBFailureType.HARD)   // feature not implemented
                    .build();

    /**
     * A special exception to describe hard failures.
     */
    @SuppressWarnings("serial")
    public static class HardFailureException extends VmtDbException {
        public HardFailureException() {
            super(VmtDbException.SQL_EXEC_ERR);
        }

        public HardFailureException(Throwable cause) {
            super(VmtDbException.SQL_EXEC_ERR, cause);
        }
    }

    private static final Logger logger = LogManager.getLogger();

    /**
     * The maximum time, in milliseconds, we allow to elapse between the first
     * try to write a row (or a group of related rows) until we decide the
     * writes will never succeed and we give up.
     */
    @Value("${executeTimeoutMsec.FORCED:120000}") // two minutes
    protected long retryTimeLimitForcedMsec;

    /**
     * The maximum time, in milliseconds, we allow to elapse between the first
     * try to write a row (or a group of related rows) until we decide the
     * writes will never succeed and we give up for the IMMEDIATE style.
     */
    @Value("${executeTimeoutMsec.IMMEDIATE:20000}") // 20 seconds
    protected long retryTimeLimitImmediateMsec;

    /**
     * The maximum time, in milliseconds, we allow to elapse between the first
     * try to write a row (or a group of releted rows) until we decide the
     * writes will never succeed and we give up for the PATIENT style.
     */
    @Value("${executeTimeoutMsec.PATIENT:60000}") // one minute
    protected long retryTimeLimitPatientMsec;

    /**
     * Cache for computed value of whether there is a read-only user.  Used only
     * by {@link #checkHaveRoUser}.  Since this value can change, lock on {@link @haveRoUserLock}
     * before accessing it.
     */
    protected static Boolean haveRoUser = null;

    /** Lock controlling access to {@link #haveRoUser}. */
    protected static final Object haveRoUserLock = new Object();


    /**
     * Store a singleton instance BasedbIO for the current platform.
     *
     * For example, in OpsManager this will be an instance of PlatformdbIO, and under XL this will
     * be an instance of HistoryDbIO.
     *
     * Note that, other than the DB session, there is no dynamic state in the BasedbIO instance.
     * There should be no problem for multiple threads to use this singleton concurrently.
     *
     */
    private static BasedbIO sharedInstance;


    /**
     * Save an instance of a subclass of BasedbIO for use as a singleton by the rest of the system.
     *
     * When setting this instance more than once, the previous sharedInstance is ignored.
     *
     * @param instance a new instance of a subclass of BasedbIO to be shared among all users of DB
     * @throws IllegalArgumentException if you try to set this instance value more than once
     */
    public synchronized static void setSharedInstance(BasedbIO instance) {
        sharedInstance = instance;
    }

    public synchronized void setSchemaForTests(String testSchemaName) {
        mappedSchemaForTests = testSchemaName;
        initContextSettings();
    }

    /**
     * Return the current singleton instance of a BasedbIO subclass for the current environment.
     * This instance must be set before use. Otherwise, a RuntimeException will be thrown.
     *
     * @return the currrent
     */
    public synchronized static BasedbIO instance() {
        if (sharedInstance == null) {
            throw new RuntimeException("BasedbIO Class is not initialized with an instance");
        }
        return sharedInstance;
    }

    /**
     * All dbIO instances share an underlying static singleton connection pool to the database.
     *
     * Set the internal connection pool timeout to the given value.
     *
     * @param timeoutSeconds The timeout to set on the internal connection pool in seconds.
     *
     * @throws IllegalStateException if called when the connection pool is not initialized
     */
    protected synchronized static void setInternalConnectionPoolTimeoutSeconds(final int timeoutSeconds) {
        if (DBConnectionPool.instance == null) {
            throw new IllegalStateException("DBConnectionPool not initialized");
        }

        DBConnectionPool.instance.getInternalPool().setRemoveAbandonedTimeout(timeoutSeconds);
    }


    /**
     * All dbIO instances share an underlying static singleton connection pool to the database.
     *
     * Set the internal connection pool timeout to the given value.
     *
     * @return The query timeout for the internal connection pool. See
     *         https://tomcat.apache.org/tomcat-7.0-doc/api/org/apache/tomcat/jdbc/pool/PoolConfiguration.html#getRemoveAbandonedTimeout()
     *         for more details.
     * @throws IllegalStateException if called when the connection pool is not initialized
     */
    static int getInternalConnectionPoolTimeoutSeconds() throws IllegalStateException {
        if (DBConnectionPool.instance == null) {
            throw new IllegalStateException("DBConnectionPool not initialized");
        }

        return DBConnectionPool.instance.getInternalPool().getRemoveAbandonedTimeout();
    }

    /**
     * Check whether the internal connection pool has been initialized.
     *
     * @return whether the internal connection pool has been initialized.
     */
    static boolean isInternalConnectionPoolInitialized() {
        return DBConnectionPool.instance != null;
    }

    /** Cached JOOQ query builder instance. */
    private DSLContext builder = null;
    private Settings settings = null;

    /**
     * Return a new auto-committing database connection.  The connection is set
     * to execute each statement in its own implicit transaction.
     * <p>
     * <emph>Caution:</emph>  "Each statement in its own transaction" is an
     * informal statement.  The actual rules, as documented for JDBC (on
     * which JOOQ is built) are as follows:<br>
     * When autocommit is true the commit occurs when the statement completes.
     * The time when the statement completes depends on the type of SQL
     * Statement.
     * <ul>
     * <li> For DML statements, such as Insert, Update or Delete, and DDL
     * statements, the statement is complete as soon as it has finished
     * executing.
     * <li> For Select statements, the statement is complete when the associated
     * result set is closed.
     * <li> For CallableStatement objects or for statements that return
     * multiple results, the statement is complete when all of the associated
     * result sets have been closed, and all update counts and output
     * parameters have been retrieved.
     * </ul>
     *
     * @return A new auto-commiting database connection.
     * @throws VmtDbException if a connection cannot be allocated.
     */


    /**
     * Return a new transactional (non-autocommiting) database connection.  It's
     * up to the caller to commit or abort any transactions that use the
     * connection.
     *
     * @return A new transactional database connection.
     * @throws VmtDbException if a transactional connection cannot be allocated.
     */
    public Connection transConnection() throws VmtDbException {
        Connection conn = connection();
        try {
            if (conn != null) {
                conn.setAutoCommit(false);
            }
        }
        catch (SQLException ex) {
            logger.error("Can't make DB connection transactional! " + ex);
            safeClose(conn);
            throw new VmtDbException(ex.getErrorCode(), ex);
        }
        return conn;
    }

    /**
     * Get a new database connection, with autocommit enabled, from outside the connection pool.
     *
     * <p>This is primarily useful for operations that are expected to take a very long time to
     * execute, since pooled connections are subject to the pool's internal timeout after which
     * it abandons the connection and throws an exception to the caller.
     * The {@link #setInternalConnectionPoolTimeoutSeconds(int)} is an option in this case, but
     * it affects all pooled connections. This has one bad and one worse potential consequence:
     * </p>
     * <ul>
     * <li><b>Bad</b>:An operation that is not expected to take very long happens while the
     * timeout has been extended, and it gets stuck and takes much longer than it should to
     * hit that timeout.
     * </li>
     * <li><b>Worse</b>:An operation that is expected to take a very long time ends up
     * starting after the timeout has been set to its normal value (by another thread that
     * just happens to be performing a long operation at the same time), and it times out
     * before it has had a chance to complete.
     * </li>
     * </ul>
     *
     * @return the connection
     * @throws SQLException if the connection cannot be allocated
     */
    public Connection unpooledConnection() throws SQLException {
        return DriverManager.getConnection(getMySQLConnectionUrl(), getUserName(), getPassword());
    }

    /**
     * Get a new database connection, with autocommit disabled, from outside the conneciton pool.
     *
     * @return the connection
     * @throws SQLException if the connection cannot be allocated or autocommit cannot be disabled
     * @see #unpooledConnection() unpooledConnection() for more details
     */
    public Connection unpooledTransConnection() throws SQLException {
        final Connection conn = unpooledConnection();
        try {
            if (conn != null) {
                conn.setAutoCommit(false);
            }
        } catch (SQLException ex) {
            logger.error("Can't make DB connection transactional! " + ex);
            safeClose(conn);
            throw ex;
        }
        return conn;
    }

    /**
     * Return a new auto-committing database connection.  The connection is set
     * to execute each statement in its own implicit transaction.
     *
     * <p>Caution: "Each statement in its own transaction" is an
     * informal statement.  The actual rules applicable for read-only
     * connections, as documented for JDBC (on which JOOQ is built) are as
     * follows:</p>
     *
     * <p>When autocommit is true the commit occurs when the statement completes.
     * The time when the statement completes depends on the type of SQL
     * Statement.</p>
     *
     * <ul>
     * <li> For Select statements, the statement is complete when the associated
     * result set is closed.</li>
     * <li> For CallableStatement objects or for statements that return
     * multiple results, the statement is complete when all of the associated
     * result sets have been closed, and all output parameters have been
     * retrieved.</li>
     * </ul>
     *
     * @return A new read-only database connection.
     * @throws VmtDbException if a read-only connection cannot be allocated.
     */
    protected Connection readOnlyConnection() throws VmtDbException {
        initPool();
        return DBConnectionPool.instance
                .getConnection(getReadOnlyUserName(), getReadOnlyPassword());

    }

    /**
     * Close a DB connection, and swallow any exception that may occur.
     *
     * <p>This is intended to be used when a connection-returning method fails after allocating
     * its connection, and will throw an exception related to the actual failure.</p>
     *
     * @param conn connection to close
     */
    private void safeClose(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
        }
    }

    /**
     * Checks if the RO user exists, throws otherwise.
     *
     * @throws VmtDbException if there is no available read-only user.
     */
    public void checkHaveRoUser() throws VmtDbException {
        synchronized (haveRoUserLock) {
            if (haveRoUser == null) {
                try {
                    haveRoUser = (null != fetch("select * from version_info;",
                            readOnlyConnection()));
                }
                catch (Exception e) {
                    // ignore; leave haveRoUser null
                }
            }

            if (haveRoUser == null || !haveRoUser) {
                throw new VmtDbException(VmtDbException.SQL_EXEC_ERR,
                        "Read-only user not available");
            }
        }
    }

    /**
     * Returns a JOOQ query builder.  This is a {@link DSLContext} object not
     * associated with any connection.  It can be used to render queries into
     * SQL, but cannot execute them.  The builder depends on the SQL dialect
     * for the server that the generated SQL will be sent to; that dialect is
     * obtained from the {@code PersistenceManager} with which this operation
     * is associated.
     *
     * @return A {@link DSLContext} object that can render queries into SQL but
     * cannot execute them.
     */
    public synchronized DSLContext JooqBuilder() {
        // Test and set later is safe here because we're synchronized.
        if (builder == null) {
            SQLDialect dialect = initContextSettings();

            builder = DSL.using(dialect, settings);
        }
        return builder;
    }

    private SQLDialect initContextSettings() {
        SQLDialect dialect = SchemaUtil.SUPPORTED_ADAPTERS.get(getAdapter());
        if (dialect == null)
            throw new UnsupportedOperationException("Invalid SQL dialect");
        settings = new Settings()
            .withRenderFormatted(true)
            // Set withRenderSchema to false to avoid rendering schema name in Jooq generated SQL
            // statement. For example, with false withRenderSchema, statement
            // "SELECT * FROM vmtdb.entities" will be changed to "SELECT * FROM entities".
            // And dynamically set schema name in the constructed JDBC connection URL to support
            // multi-tenant database.
            .withRenderSchema(false);

        // set a schema mapping in order to support Unit tests:
        if (mappedSchemaForTests != null) {
            settings.withRenderMapping(new RenderMapping().withSchemata(new MappedSchema()
                    .withInput("vmtdb").withOutput(mappedSchemaForTests)));
        }
        return dialect;
    }

    /**
     * Create a @link DSLContext} for building Jooq operations using the singleton instance of
     * this class.
     *
     * @return A {@link DSLContext} for building Jooq operations that can render queries into SQL
     * but cannot execute them.
     */
    public static DSLContext getJooqBuilder() {
        return instance().JooqBuilder();
    }

    /**
     * Visible to package for tests
     *
     * @param conn
     * @return
     */
    public DSLContext using(Connection conn) {
        return DSL.using(conn, settings);
    }

    /**
     * Executes the given select using JOOQ's internal 'fetch' method
     * and returns a value of the specified type.
     *
     * This method expects queries that return no more than 1 row. A
     * runtime exception is thrown if more than one row is returned.
     *
     * @param select
     * @param clz - The expected result type
     * @return A single value of the specified type, or null if the query
     * returned empty.
     * @throws VmtDbException
     */
    public <T> T fetchFirstInto(Select<?> select, Class<T> clz) throws VmtDbException {
        return firstOrNull(execute(select), clz);
    }

    /**
     * Executes the given select using JOOQ's internal 'fetch' method
     * and returns a value of the specified type, using the given Connection.
     *<p>
     * This method expects queries that return no more than 1 row. A
     * runtime exception is thrown if more than one row is returned.
     *
     * @param select The given Select statement
     * @param clz The expected result type.
     * @param conn The given connection.  A single value of the specified type,
     * or null if the query returned empty.
     * @return A single value of the specified type, or null if the query
     * returned empty.
     * @throws VmtDbException
     */
    public <T> T fetchFirstInto(Select<?> select, Class<T> clz, Connection conn)
            throws VmtDbException {
        return firstOrNull(execute(select, conn), clz);
    }

    /**
     * Executes the given select using JOOQ's internal 'fetch' method
     * and returns a value of the specified type, or null if no value is
     * returned.
     *
     * If an error is thrown during the execution of the command, an attempt to repair
     * is made on the given table.
     * The calling thread will block until the repair is complete.
     *
     * @param table - The resource used in this query.
     * @param select - Query to execute.
     * @param clz - Requested return type.
     * @return
     * @throws VmtDbException - In case the execution or repair failed.
     */
    public <T> T fetchFirstIntoWithRepairAndRetry(Table<?> table, Select<?> select, Class<T> clz)
            throws VmtDbException {
        return firstOrNull(executeWithRepairAndRetry(table, select), clz);
    }

    /**
     * Returns the first result, or null if no such result exists.
     *
     * @param res
     * @param clz - Requested return type.
     * @return
     */
    public <T> T firstOrNull(Result<? extends Record> res, Class<T> clz) {
        return res.size() > 0 ? res.get(0).into(clz) : null;
    }

    /**
     * Executes the given query and returns the ResultSet in Jooq's data
     * format.
     *
     * <p>
     *  <h2>[WARNING]</h2> <b>Auto-commits!</b> Assumes that no transaction is necessary.
     * </p>
     *
     * @param sql
     * @return
     * @throws VmtDbException
     */
    public Result<Record> fetch(String sql) throws VmtDbException {
        checkNotNull(sql);
        Connection conn = connection();
        return fetch(sql, conn);
    }

    /**
     * Executes the given query <b>in read-only mode</b> and returns the ResultSet
     * in Jooq's data format.
     *
     * <p>
     *  <h2>[WARNING]</h2> <b>Auto-commits!</b> Assumes that no transaction is necessary.
     * </p>
     *
     * @param sql
     * @return
     * @throws VmtDbException
     */
    public Result<Record> fetchRO(String sql) throws VmtDbException {
        checkHaveRoUser();
        checkNotNull(sql);
        Connection conn = readOnlyConnection();
        return fetch(sql, conn);
    }

    /**
     * Executes the given query and returns the ResultSet in Jooq's data
     * format.
     *
     * <p>
     *  <h2>[WARNING]</h2> <b>Auto-commits!</b> Assumes that no transaction is necessary.
     * </p>
     *
     * @param sql
     * @return
     * @throws VmtDbException
     */
    public Result<Record> fetch(String sql, Connection conn) throws VmtDbException {
        checkNotNull(sql);
        try {
            if (logger.isTraceEnabled())
                print(sql);
            return using(conn).fetch(sql);
        }
        catch (DataAccessException e) {
            print(sql);
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
        finally {
            close(conn);
        }
    }

    /**
     * Fetch all of the rows of a given table.
     *
     * @param table
     * @return Corresponding all the records in the given table, represented as
     * the corresponding Record objects.
     * @throws VmtDbException
     */
    public <R extends Record> Result<R> fetch(Table<R> table) throws VmtDbException {
        return fetch(table, null);
    }

    /**
     * Fetch all of the rows of a given table.
     *
     * @param table
     * @param cond
     * @return
     * @throws VmtDbException
     */
    public <R extends Record> Result<R> fetch(Table<R> table, Condition cond)
            throws VmtDbException {
        checkNotNull(table);
        Connection conn = null;
        try {
            if (logger.isTraceEnabled())
                logger.trace("Fetching all records from table: " + table.getName());

            conn = connection();
            Result<R> result = cond != null ? using(conn).fetch(table, cond) : using(conn)
                    .fetch(table);

            if (logger.isTraceEnabled())
                logger.trace("Dbio query results:\n" + result);
            return result;
        }
        catch (DataAccessException e) {
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
        finally {
            close(conn);
        }
    }

    /**
     * executes the list of transactions. The transaction is performed for all queries,
     * so a failure in one will fail the entire execution.
     * @param queryList
     * @throws VmtDbException
     */

    public void execute(List<Query> queryList) throws VmtDbException {
        Connection conn = null;
        try {
            conn = transConnection();
            for(Query query : queryList){
                execute(query, conn);
            }
            conn.commit();
        }
        catch (SQLException e) {
            rollback(conn);
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
        catch (VmtDbException e) {
            rollback(conn);
            throw e;
        }
        finally {
            close(conn);
        }
    }

    public Result<? extends Record> executeTransaction(Query query) throws VmtDbException {
        Connection conn = null;
        try {
            conn = transConnection();
            Result<? extends Record> result = execute(query, conn);
            conn.commit();
            return result;
        }
        catch (SQLException e) {
            rollback(conn);
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
        catch (VmtDbException e) {
            rollback(conn);
            throw e;
        }
        finally {
            close(conn);
        }
    }

    /**
     * Executes an autoincrement insert query.
     *
     * @param query - A JOOQ insert statement to execute
     * @return The Id returned after executing the statement for InsertQuery,
     * or null otherwise.
     * @throws VmtDbException
     */
    protected int executeAutoIncrementQuery(Insert<?> query) throws VmtDbException {
        Connection conn = null;
        try {
            conn = transConnection();
            execute(query, conn);
            Result<? extends Record> idResult = using(conn).fetch("SELECT LAST_INSERT_ID()");
            int lastId = ((ULong) idResult.get(0).getValue(0)).intValue();
            conn.commit();
            return lastId;
        }
        catch (SQLException e) {
            rollback(conn);
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
        catch (VmtDbException e) {
            rollback(conn);
            throw e;
        }
        finally {
            close(conn);
        }
    }

    /**
     * Executes the given query.
     *
     * @param query - A JOOQ statement to execute
     * @return The ResultSet returned after executing the statement for ResultQuery,
     * or null otherwise.
     * @throws VmtDbException
     */
    public Result<? extends Record> execute(Query query) throws VmtDbException {
        checkNotNull(query);
        Connection conn = null;
        try {
            if (logger.isTraceEnabled())
                print(query);
            conn = connection();
            return execute(query, conn);
        }
        catch (DataAccessException e) {
            logger.error("Exception during query execution : " +
                query.getSQL(false) +  " with cause : "+
                e.getCause());
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
        finally {
            close(conn);
        }
    }

    /**
     * Executes the given query.
     *
     * If an error is thrown during the execution of the command, an attempt to repair
     * is made on the given table.
     * The calling thread will block until the repair is complete.
     *
     * @param table - The resource used in this query.
     * @param query - Query to execute.
     * @return
     * @throws VmtDbException - In case the execution or repair failed.
     */
    protected Result<? extends Record>
    executeWithRepairAndRetry(Table<?> table, ResultQuery<? extends Record> query)
            throws VmtDbException {
        try {
            return execute(query);
        }
        catch (VmtDbException dbe) {
            dbe.setResource(table.getName());
            if (internalTryRecover(dbe)) // 1 retry
                return execute(query);
            throw dbe;
        }
    }

    /**
     * Inspects whether the thrown exception requires trying to repair
     * the database.
     *
     * @param dbe
     * @return
     */
    protected boolean internalTryRecover(VmtDbException dbe) {
        return true;
    }

    /**
     * Executes a query using the given connection. Used for multiple inserts
     * that may require a rollback.
     *
     * <p><b>
     * USERS OF THIS FUNCTION ARE RESPONSIBLE FOR CLOSING THE CONNECTION,
     * STATEMENT AND TRANSACTION.
     * </b></p>
     *
     * @param query
     * @param conn
     * @return
     * @throws DataAccessException
     * @throws VmtDbException
     */
    public Result<? extends Record> execute(Query query, Connection conn)
            throws DataAccessException, VmtDbException {
        if (query instanceof ResultQuery<?>) {
            ResultQuery<? extends Record> resQuery = (ResultQuery<?>)query;
            Result<? extends Record> result = using(conn).fetch(resQuery);

            if (logger.isTraceEnabled())
                logger.trace("Dbio query results:\n" + result);

            return result;
        } else { // JOOQ doesn't have the parallel "no-result-query" interface
            using(conn).execute(query);
            return null;
        }
    }

    /**
     * Executes the given query directly.
     *
     * <p>
     *  <h2>[WARNING]</h2> <b>Auto-commits!</b> Assumes that no transaction is necessary.
     * </p>
     *
     * @param sql The SQL statement to execute.
     * @throws VmtDbException
     */
    public int execute(String sql) throws VmtDbException {
        checkNotNull(sql);
        Connection conn = null;
        try {
            conn = transConnection();
            int res = execute(sql, conn);
            conn.commit();
            return res;
        }
        catch (DataAccessException | SQLException e) {
            rollback(conn);
            print(sql);
            e.printStackTrace();
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
        finally {
            close(conn);
        }
    }

    public <T> T transaction(Function<DSLContext, T> unitOfWork) throws VmtDbException {
        Connection conn = null;
        try {
            conn = transConnection();
            T res = unitOfWork.apply(using(conn));
            conn.commit();
            return res;
        } catch (DataAccessException | SQLException e) {
            rollback(conn);
            e.printStackTrace();
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        } finally {
            close(conn);
        }

    }

    public int execute(String sql, Connection conn) {
        if (logger.isTraceEnabled())
            print(sql);

        return using(conn).execute(sql);
    }

    /**
     * Close a database connection, statement, and result set.  The effect
     * of closing a connection with an open transaction is unspecified by
     * JDBC; you should either commit or roll back any transction on
     * {@code conn} before closing it.  Any exceptions are logged and ignored;
     * any null arguments are ignored.
     *
     * @param conn The connection to close.  Ignored if null.
     * @param stmt The statement to close.  Ignored if null.
     * @param rSet The result set to close.  Ignored if null.
     */
    public void close(Connection conn, Statement stmt, ResultSet rSet) {
        try {
            if (rSet != null)
                rSet.close();
        }
        catch (Exception e) {
            logger.error("Exception closing " + rSet + "; ignored", e);
        }
        try {
            if (stmt != null)
                stmt.close();
        }
        catch (Exception e) {
            logger.error("Exception closing " + stmt + "; ignored", e);
        }
        try {
            if (conn != null)
                conn.close();
        }
        catch (Exception e) {
            logger.error("Exception closing " + conn + "; ignored", e);
        }
    }

    /**
     * Close a database connection and result set.  The effect of closing a
     * connection with an open transaction is unspecified by JDBC; you should
     * either commit or roll back any transction on {@code conn} before
     * closing it.  Any exceptions are logged and ignored; any null arguments
     * are ignored.
     *
     * @param conn The connection to close.  Ignored if null.
     * @param stmt The statement to close.  Ignored if null.
     */
    protected void close(Connection conn, Statement stmt) {
        close(conn, stmt, null);
    }

    /**
     * Close a database connection.  The effect of closing a connection with
     * an open transaction is unspecified by JDBC; you should either commit or
     * roll back any transction on {@code conn} before closing it.  Any
     * exceptions are logged and ignored.  Calling this method with a null
     * argument is a no-op.
     *
     * @param conn The connection to close.  If null, the call is a no-op.
     */
    public void close(Connection conn) {
        close(conn, null, null);
    }

    /**
     * Roll back the current transaction on a connection.  This is usually
     * called from exception handlers, so for convenience it if {@code conn}
     * is null it simply returns.  Any exceptions are logged and discarded.
     *
     * @param conn The connection on which to roll back the current transaction.
     * If null, the call is a no-op.
     */
    public void rollback(Connection conn) {
        try {
            if (conn != null) {
                conn.rollback();
            }
        }
        catch (Exception e) {
            logger.error("Error rolling back transaction on " + conn + "; ignored", e);
        }
    }

    /**
     * General-purpose query executor.  This method executes, in a single
     * transaction, a list of any combination of queries that return result
     * sets and queries that don't.  (The counts that are returned by many
     * calls that don't return result sets cannot be retrieved.)  The method
     * can do retries, depending on the {@code style} parameter.
     * <p>
     * When the style is {@code FORCED}, the method will keep trying,
     * nominally forever (though there's actually a large upper limit) to
     * execute the query.  Should it reach its upper time limit, or should it
     * determine in some other way that further attempts are futile, it logs a
     * fatal message and throws {@code UnsupportedOperationException}.  The
     * log message and exception describe the failure in detail.
     * <br>
     * TODO(leichter):  Create an action in this case - and perhaps even before
     * the final failure.
     * <p>
     * All other styles impose some significantly shorter limit on the total
     * time.  If the method is forced to give up in one of these other styles,
     * it throws a {@code VmtDbException} with a detailed description of the
     * query and of the last exception encountered in trying to execute it.
     * In this case, no errors are logged; all handing of the exception and
     * the failure is up to the caller.
     *
     * @param style The style of execution to be used.
     * @param queries The queries making up the transaction to be executed.
     * @return A list of results, parallel to the input list.  If the i'th
     * element of {@code queries} returns a result, the i'th element of the
     * returned list will be that result; otherwise that element will be null.
     * @throws HardFailureException if the transaction led to a hard failure,
     * so there is no point in retrying it.
     * @throws VmtDbException if the transaction could not be completed even
     * after multiple retries and the style was not
     * {@code FORCED}.
     * @throws UnsupportedOperationException if the transaction failed and the
     * style was {@code FORCED}.
     */
    public List<Result<? extends Record>> execute(Style style, List<Query> queries)
            throws VmtDbException, HardFailureException {
        trc("<execute(%s, List with %d elements)", style, queries.size());
        java.util.Date startTime = new java.util.Date();

        // The time limit imposed for the overall operation
        long timeLimit_ms;

        // If non-null, the time limit imposed for the execution of an
        // individual query.  We use a (weak) approximation; we could compute
        // this limit dynamically based on the time already used.  But there
        // seems little point.  At worst, we'll end up using a total of twice
        // the time we should have.
        switch (style) {
            case IMMEDIATE:
                timeLimit_ms = retryTimeLimitImmediateMsec;
                break;

            case PATIENT:
                timeLimit_ms = retryTimeLimitPatientMsec;
                break;

            case FORCED:
                timeLimit_ms = retryTimeLimitForcedMsec;
                break;

            default:
                // The following two assignments are irrelevant:  The call to
                // checkArgument() always throws - but needed to keep the
                // compiler from complaining that the variables might not have
                // be initialized.
                timeLimit_ms = 0;
                checkArgument(false, "Unsupported style value " + style);
        }
        Integer queryLimit_sec = (int)Math.max(1, TimeUnit.MILLISECONDS.toSeconds(timeLimit_ms));
        long deadline = System.currentTimeMillis() + timeLimit_ms;
        int tries;
        Connection conn = null;
        Exception lastException = null;

        for (tries = 0; System.currentTimeMillis() < deadline; tries++) {
            trc("=execute try %d", tries);
            Query lastQuery = null;

            try {
                // The first 20 times, or if we're not using FORCED style,
                // sleep only a little bit.  This covers transient failures.
                // After that, we sleep in 1-minute chunks.
                try {
                    Thread.sleep((long)((style != Style.FORCED || (tries < 20))
                            ? (tries * tries * Units.MILLISECOND_MS)
                            : (1. * Units.MINUTE_MS)));
                }
                catch (InterruptedException ex) {
                    // Nothing interesting here, just move along
                }
                conn = transConnection();

                List<Result<? extends Record>> results = new ArrayList<Result<? extends Record>>(
                        queries.size());
                for (Query query : queries) {
                    lastQuery = query;
                    if (queryLimit_sec != null) {
                        query.queryTimeout(queryLimit_sec);
                    }
                    if (tries < 2) {
                        trc("=execute %s query:%n%s", (queryLimit_sec == null)
                                ? "no time limit"
                                : "time limit " + queryLimit_sec + " seconds", query);
                    }
                    try (DataMetricTimer timer = Metrics.QUERY_EXECUTE_DURATION.startTimer();
                         OptScope scope = Tracing.addOpToTrace("query")) {
                        Tracing.log(query.getSQL());
                        if (query instanceof ResultQuery<?>) {
                            ResultQuery<? extends Record> resQuery = (ResultQuery<?>) query;
                            results.add(using(conn).fetch(resQuery));
                        } else {
                            using(conn).execute(query);
                            results.add(null);
                        }
                    }
                }
                lastQuery = null;
                trc("=execute committing");
                conn.commit();
                trc(">execute done");
                return results;
            }
            catch (Exception ex) {
                rollback(conn);

                lastException = ex;
                trc("=execute Exception during execute or commit %s", ex);
                final DBFailureType failureType = analyzeDBFailure(ex);
                Metrics.EXECUTE_ERROR_COUNTER.labels(failureType.name()).increment();
                switch (failureType) {
                    // Weird case - log and treat as a soft failure.
                    case STRANGE:
                        logger.error("This should not have been thrown!", ex);
                        /*
                         * Fall through...
                         */
                    case SOFT:
                        if (logger.isDebugEnabled() && ((tries % 10) == 0)) {
                            logger.debug(String.format("Try %d to execute queries failed: %s%n"
                                            + "Last known query was:%n"
                                            + "%s%n------%n",
                                    tries, trimJooqErrorMessage(ex), trimJooqQuery(lastQuery)), ex);
                        } else {
                            logSoftError("Query failed with soft error (try: {})." +
                                " Exception message: {}", tries, ex);
                        }
                        continue;
                    case SERVER:
                        logger.error(String.format("Server error accessing database; " +
                                "Last known query was:%n"
                                + "%s%n------%n", trimJooqQuery(lastQuery)), ex);
                        internalNotifyUser("Server internal error", trimJooqErrorMessage(ex));
                        continue;

                    case HARD:
                        logger.error(String.format("Hard error accessing database; "
                                + "Last known query was:%n"
                                + "%s%n------%n", trimJooqQuery(lastQuery)), ex);
                        throw new HardFailureException(ex);
                }
            }
            finally {
                close(conn);
                conn = null;
            }
        }

        // We didn't succeed.
        trc(">execute failed");
        String message = String
                .format("SQL query started at %s in style %s was unsuccessful after "
                                + "%d tries.  Query list was:%n",
                        startTime, style, tries);
        for (Query query : queries) {
            message = String.format("%s%s%n------%n", message, trimJooqQuery(query));
        }
        if (style == Style.FORCED) {
            internalNotifyUser("Database write failures",
                    "Couldn't write some data to the database in " +
                            formatElapsed(retryTimeLimitForcedMsec)
                            + "; giving up on it");
            logger.fatal(message);
            throw new UnsupportedOperationException(message, lastException);
        } else {
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, new Exception(message,
                    lastException));
        }
    }

    /**
     * Log a soft error encountered when performing a db operation, taking care to deal with JOOQ's
     * large and often largely useless messages.
     * <p>If we're logging at debug level or above, we'll log the complete exception message, in which
     * JOOQ will often copy the entire SQL statement that was being executed, formatted on multiple
     * lines and often including hundreds of lists of question-mark placeholders, where values
     * were bound for batch execution.</p>
     *
     * <p>At lower log levels, we include only the first line of the exception message, to avoid
     * the cruft mentioned above.</p>
     *
     * @param s Log message, including "{}" placeholders for tries count and exception message
     * @param tries tries count for this operation
     * @param e exception returned for operation
     */
    private void logSoftError(@Nonnull final String s, final int tries, @Nonnull final Throwable e) {
        logger.log(logger.isDebugEnabled() ? Level.DEBUG : Level.WARN,
                s, tries, trimJooqQueryString(e.getMessage()));
    }

    /**
     * Convenience override to execute a query.  This method
     * simply wraps the query given into a single-element list, passed
     * it to {@ref execute(Style, List<Query>)}, and returns the (single)
     * element of the returned result.
     *
     * @param style The style of execution to be used.
     * @param query The query to be executed.
     * @return If {@code query} returns a result, that result; otherwise null.
     * @throws HardFailureException if the transaction led to a hard failure,
     * so there is no point in retrying it.
     * @throws VmtDbException if the transaction could not be completed even
     * after multiple retries and the style was not
     * {@code FORCED}.
     * @throws UnsupportedOperationException if the query failed and the style
     * was {@code FORCED}.
     */
    public Result<? extends Record> execute(Style style, Query query) throws VmtDbException,
            HardFailureException {
        return execute(style, ImmutableList.of(checkNotNull(query))).get(0);
    }

    /**
     * Analyze an exception that (might have been) thrown by Jooq or JDBC or
     * some other database interface method and return the action to be taken.
     *
     * @param ex The exception to analyze.
     * @return The action to be taken for this exception.
     */
    public static DBFailureType analyzeDBFailure(Exception ex) {
        checkNotNull(ex);

        if (ex instanceof DataChangedException || ex instanceof DetachedException) {
            return DBFailureType.SOFT;
        } else if (ex instanceof DataTypeException || ex instanceof InvalidResultException
                || ex instanceof MappingException) {
            return DBFailureType.HARD;
        } else if (ex instanceof DataAccessException && ex.getCause() instanceof SQLException) {
            ex = (SQLException)ex.getCause();
        }

        Exception originalEx = ex;
        if (ex instanceof SQLException) {
            String sqlState = ((SQLException)ex).getSQLState();

            // SQLSTATE values are always 5 characters long, per standard.
            if (sqlState.length() != 5) {
                logger.error("Strange sqlState value: " + sqlState, originalEx);
                return DBFailureType.SOFT;
            }

            DBFailureType ret = STATE_TO_DBFAILURE.get(sqlState);
            if (ret == null) {
                ret = STATE_TO_DBFAILURE.get(sqlState.substring(0, 2));
            }
            if (ret != null) {
                return ret;
            }
        }
        return DBFailureType.SOFT;
    }

    /**
     * Initializes VMTDB to include vmturbo db, vmtplatform userid, and call SchemaUtil to
     * initialize vmtdb schema.
     *
     * We get a DB Connection for the "root" for use in the first two steps. Then we drop that and
     * get a connection using "vmtplatform" and "vmtdb" to initialize the schema.
     *
     * @param clearOldDb should the old db be cleared
     * @param version what version of schema is "current" - i.e. what should we
     * @param dbName The name of the database (e.g. "vmtdb".
     * @param migrationLocationOverride If set, overrides the location in the classpath we scan
     *                                  for flyway migrations.
     * @throws VmtDbException If there is a database error.
     */
    @VisibleForTesting
    public void init(boolean clearOldDb, Double version, String dbName,
                     Optional<String> migrationLocationOverride) throws VmtDbException {
        try {
            // Test DB connection first to the schema under given user credentials.
            DBConnectionPool.setConnectionPoolInstance(null);
            connection().close();
            logger.info("DB connection is available to schema '{}' from user '{}'.",
                dbName, getUserName());
        } catch (VmtDbException | SQLException e) {
            // VmtDbException or SQLException will be thrown if given db schema name or db user does
            // not exist or password has been changed. This is a valid case.
            initUsingRoot(clearOldDb, dbName);
        }

        // Initialize the tables at the latest schema:
        logger.info("Initialize tables...\n");
        SchemaUtil.initDb(version, clearOldDb, migrationLocationOverride);
    }

    /**
     * Initialize using root credentials to create database schema and user.
     *
     * @param clearOldDb True if old db be cleared, otherwise false.
     * @param dbName     The name of the database (e.g. "vmtdb").
     * @throws VmtDbException If there is a database error.
     */
    private void initUsingRoot(boolean clearOldDb, String dbName) throws VmtDbException {
        logger.info("Database schema '{}' or user '{}' does not exist or password has been changed. " +
            "Initializing schema and user under root credentials...", dbName, getUserName());
        // Attempt to retrieve root connection:
        try (Connection rootConn = getRootConnection()) {
            // if desired, remove the previous database
            if (clearOldDb) {
                SchemaUtil.dropDb(dbName, rootConn);
            }

            // Create the database
            logger.info("Initializing database '" + dbName + "'...");
            SchemaUtil.createDb(dbName, rootConn);
            execute("use " + dbName + ";", rootConn);

            createDBUser(rootConn, dbName, getRequestHost());
            // Create user on localhost for testing.
            createDBUser(rootConn, dbName, "localhost");
        } catch (SQLException sqle) {
            logger.error("=======================================================");
            logger.error("=  Unable to retrieve connection! Make sure MySQL is  =");
            logger.error("=  running and the root password is correct.          =");
            logger.error("=======================================================", sqle);
            throw new VmtDbException(VmtDbException.CONN_POOL_STARTUP,
                "Cannot retrieve root db connection", sqle);
        }
    }

    private void createDBUser(Connection rootConn, String dbName, String hostName) throws SQLException {
        // Allow given user to access the database (= grants.sql):
        final String requestUser = "'" + getUserName() + "'@'" + hostName + "'";
        logger.info("Initialize permissions for " + requestUser + " on '" + dbName + "'...");
        // Clean up existing db user, if it exists.
        try (PreparedStatement stmt = rootConn.prepareStatement(
            "DROP USER ?@?;")) {
            stmt.setString(1, getUserName());
            stmt.setString(2, hostName);
            stmt.execute();
            logger.info("Cleaned up {} db user.", requestUser);
        } catch (SQLException e) {
            // SQLException will be thrown when trying to drop not existed username% user in DB. It's valid case.
            logger.info("{} user is not in the DB, clean up is not needed.", requestUser);
        }
        // Create db user.
        try (PreparedStatement stmt = rootConn.prepareStatement(
            "CREATE USER ?@? IDENTIFIED BY ?;")) {
            stmt.setString(1, getUserName());
            stmt.setString(2, hostName);
            stmt.setString(3, getPassword());
            stmt.execute();
            logger.info("Created {} db user.", requestUser);
        }
        // Grant db user privileges
        try (PreparedStatement stmt = rootConn.prepareStatement(
            "GRANT ALL PRIVILEGES ON *.* TO ?@?;")) {
            stmt.setString(1, getUserName());
            stmt.setString(2, hostName);
            stmt.execute();
            logger.info("Granted all privileges to user {}.", requestUser);
        }
        // Flush user privileges
        try (PreparedStatement stmt = rootConn.prepareStatement("FLUSH PRIVILEGES;")) {
            stmt.execute();
            logger.info("Flushed all privileges to user {}.", requestUser);
        }
    }

    /**
     * @return The connection URL String to be used to connect to a MySQL instance.
     * The host, port and database name are properties of this manager.
     */
    abstract String getMySQLConnectionUrl();

    /**
     * Notify the user of a critical problem with the database.  Notifications
     * are appropriate when the database is apparently stuck or damaged in
     * some way that requires user intervention.
     *
     * @param eventName A name uniquely identifying the notification.  This has
     * two roles:  It will be the name for the notification as displayed to
     * the user; and it identifies individual notifications.  To clear a
     * notification, call {@link #internalClearNotification} with its identifying
     * {@code eventName}.  If you call this method with the {@code eventName}
     * of an already-active notification, the error string you pass will
     * replace the one in the notification.  It's best to limit this value to
     * no more than 25 characters.
     * @param error Some string explaining the error that occurred.  This
     * is typically the string form of an exception.  It's best to limit
     * this string to no more than 80 characters with no embedded newlines.
     * A period will be added to it.
     */
    protected abstract void internalNotifyUser(String eventName, String error);
    protected abstract void internalClearNotification(String eventName);

    /**
     * Initializes the JDBC connection pool based on the credentials specified
     * in this manager.
     * @throws VmtDbException
     */
    private void initPool() throws VmtDbException {
        synchronized (this) {
            if (DBConnectionPool.instance == null) {
                String driverName = null, url = null;

                //Initialize based on driver:
                if (SchemaUtil.isMysqlAdapter(getAdapter())) {
                    url = getMySQLConnectionUrl();
                    driverName = "org.mariadb.jdbc.Driver";
                } else {
                    throw new UnsupportedOperationException("Unsupported SQL dialect");
                }

                DBConnectionPool.setConnectionPoolInstance(new DBConnectionPool(url,
                        driverName,
                        getUserName(),
                        getPassword(),
                        getQueryTimeoutSeconds()));
            }
        }
    }


    /**
     * Return a new auto-committing database connection.  The connection is set
     * to execute each statement in its own implicit transaction.
     * <p>
     * <emph>Caution:</emph>  "Each statement in its own transaction" is an
     * informal statement.  The actual rules, as documented for JDBC (on
     * which JOOQ is built) are as follows:<br>
     * When autocommit is true the commit occurs when the statement completes.
     * The time when the statement completes depends on the type of SQL
     * Statement.
     * <ul>
     * <li> For DML statements, such as Insert, Update or Delete, and DDL
     * statements, the statement is complete as soon as it has finished
     * executing.
     * <li> For Select statements, the statement is complete when the associated
     * result set is closed.
     * <li> For CallableStatement objects or for statements that return
     * multiple results, the statement is complete when all of the associated
     * result sets have been closed, and all update counts and output
     * parameters have been retrieved.
     * </ul>
     *
     * @return A new auto-commiting database connection, or <code>null</code> if the pool is shutdown.
     * @throws VmtDbException if a connection cannot be allocated.
     */
    public Connection connection() throws VmtDbException {
        initPool();
        Connection conn = DBConnectionPool.instance.getConnection();
        // As connections are reused, set autocommit to true every time
        // as it could be set to false during previous calls by other
        // callers(via transConnection() or by explicitly calling
        // setAutoCommit()).
        try {
            if (conn != null) {
                conn.setAutoCommit(true);
            }
        } catch (SQLException ex) {
            logger.error("Exception while setting autoCommit flag.", ex);
            throw new RuntimeException(ex);
        }
        return conn;
    }

    /**
     * Retrieves a read-only connection using the read-only user.
     *
     * @return
     * @throws VmtDbException
     */
    public Connection getReadOnlyConnection() throws VmtDbException {
        initPool();
        return DBConnectionPool.instance
                .getConnection(getReadOnlyUserName(), getReadOnlyPassword());
    }

    /**
     * Retrieves a root connection without specifying a database. Uses hard-coded userid/password.
     *
     * TODO: userid/password should NOT be hard-coded here.
     *
     * @return a newly openeted {@link Connection} opened for the root MySQL schema.
     * @throws SQLException
     */
    public Connection getRootConnection() throws SQLException {

        // Loads the driver library
        Driver myDriver = new org.mariadb.jdbc.Driver();
        DriverManager.registerDriver(myDriver);

        return DriverManager.getConnection(getRootConnectionUrl(), getRootUsername(), getRootPassword());
    }

    /**
     * @return The connection URL String to be used to connect to a MySQL instance.
     * The host, port and database name are properties of this manager.
     */
    abstract String getRootConnectionUrl();

    public void shutdown() {
        if (DBConnectionPool.instance != null)
            DBConnectionPool.instance.shutdown();
    }

    // Accessors for connection parameters
    public abstract String getUserName();
    public abstract String getPassword();
    public abstract String getRootUsername();
    public abstract String getRootPassword();
    public abstract String getRequestHost();
    public abstract String getAdapter();
    public abstract String getHostName();
    public abstract String getPortNumber();
    public abstract String getDbSchemaName();
    public abstract String getReadOnlyUserName();
    public abstract String getReadOnlyPassword();
    public abstract int getQueryTimeoutSeconds();

    public abstract boolean isReportEnabled(int rptId);

    public abstract int addSubscription(String email, Period period, DayOfWeek dayOfWeek,
                                        String scope, ReportType reportType, int reportId,
                                        ReportOutputFormat format, Map<Integer, String> attrs,
                                        String userName) throws VmtDbException;

    public abstract boolean isDeveloper();

    /**
     * Format an elapsed time.  The result will have the form:
     * "d days hh:mm:ss" where the days are omitted if zero and
     * the hours, minutes, and seconds are two digits long.
     * <p>
     * It's amazing that this kind of formatting isn't part of a standard
     * Java library; but it isn't.  It also doesn't belong in this file; move
     * later.
     *
     * @param time_ms The elapsed time in milliseconds.  It will be rounded
     * to an even number of seconds before formatting.  Must be non-negative.
     */
    static public String formatElapsed(long time_ms) {
        checkArgument(time_ms >= 0, "Can't format a negative elapsed time");
        long time_sec = Math.round(time_ms * Units.MILLI);
        long days = time_sec / (long)Units.DAY;
        time_sec = time_sec % (long)Units.DAY;
        long hours = time_sec / (long)Units.HOUR;
        time_sec = time_sec % (long)Units.HOUR;
        long minutes = time_sec / (long)Units.MINUTE;
        time_sec = time_sec % (long)Units.MINUTE;
        return String.format("%s%02d:%02d:%02d", (days == 0) ? "" : (days + "days "), hours,
                minutes, time_sec);
    }

    /**
     * Pretty-prints the given query
     * @param query
     */
    protected void print(Query query) {
        print(query.toString());
    }

    protected void print(String query) {
        logger.info("---------");
        logger.info("DbIO query:\n" + query);
        logger.info("---------");
    }

    /**
     * Control low-level tracing.  Override this in subclasses to enable or
     * disable calls to {@link #trc} in that class (and deeper subclasses, if
     * you don't override it further).
     *
     * @return Whether to do any low-level tracing.
     */
    protected boolean doTrc() {
        return logger.isTraceEnabled();
    }

    /**
     * Write trace output to {@code System.out}.  Meant for debugging; use
     * {@link #doTrc} to turn this off for production.
     */
    protected void trc(String format, Object... ss) {
        if (doTrc()) {
            String s = String.format(format, ss);
            java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
            System.out.println(now + " t@" + Thread.currentThread().getId() + ": " + s);
        }
    }

    static class Metrics {

        private static final DataMetricCounter EXECUTE_ERROR_COUNTER = DataMetricCounter.builder()
            .withName("history_base_db_execute_failure_count")
            .withHelp("The number of BasedbIO execution failures, by type.")
            .withLabelNames("type")
            .build()
            .register();

        private static final DataMetricHistogram QUERY_EXECUTE_DURATION = DataMetricHistogram.builder()
            .withName("history_base_db_query_duration_seconds")
            .withHelp("Duration of BasedbIO requests.")
            .withBuckets(0.5, 1, 5, 10, 60)
            .build()
            .register();
    }
}
