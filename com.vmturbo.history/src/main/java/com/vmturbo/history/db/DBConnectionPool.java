package com.vmturbo.history.db;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;

public class DBConnectionPool {
	private static final Logger logger = Logger.getLogger("com.vmturbo.history.db");

	public static DBConnectionPool instance;

	private final static String QUERY_TIMEOUT_SECOND = "vmt.query.timeout.second";
	private final static Integer queryTimeoutSecond = Integer.getInteger(QUERY_TIMEOUT_SECOND);
	//If vmt.query.timeout.second system property is provided, e.g. -Dvmt.query.timeout.second = 10800
	//it will be used instead of default value (3 hours)
	public static int QUERY_TIMEOUT_SECONDS_DEFAULT = queryTimeoutSecond != null ? queryTimeoutSecond : 60 * 60 * 3; // 3 hours

	protected static final int CONNECT_TIMEOUT_MILLIS = 30 * 1000; // 30 seconds

	DataSource pool = null;

    public static void setConnectionPoolInstance(DBConnectionPool poolInstance) {
        DBConnectionPool.instance = poolInstance;
    }

	private Boolean isShutdown = new Boolean(true);

	/**
	 * Constructs a new DBConnectionPool and initializes the internal implementation.
	 *
	 * @param jdbcUrl - e.g. jdbc:mysql://localhost:3306/vmtdb?params1=val1&param2=val2
	 * @param driverName - e.g. com.mysql.jdbc.Driver
	 * @param user
	 * @param password
	 * @throws VmtDbException
	 */
	public DBConnectionPool(String jdbcUrl, String driverName, String user, String password, int queryTimeoutSeconds)
			throws VmtDbException {
		initPool(driverName, jdbcUrl, user, password, queryTimeoutSeconds);
	}

	/**
	 * Initializes the connection pool.
	 *
	 * @param driver
	 * @param jdbcUrl
	 * @param user
	 * @param password
	 * @throws VmtDbException
	 */
	protected synchronized void initPool(String driver, String jdbcUrl, String user, String password, int queryTimeoutSeconds)
			throws VmtDbException {
		if (isShutdown) {
			try {
				Class.forName(driver); // Loads the driver library

				PoolProperties p = new PoolProperties();
				p.setUrl(jdbcUrl);
				p.setDriverClassName(driver);
				p.setUsername(user);
				p.setPassword(password);
				p.setAlternateUsernameAllowed(true);
				p.setMaxWait(CONNECT_TIMEOUT_MILLIS);

				p.setTestOnBorrow(true);
				p.setValidationQuery("select 1;");

				p.setRemoveAbandonedTimeout(queryTimeoutSeconds); //Recover connection after timeout
				p.setRemoveAbandoned(true); // Try to recover unclosed and unused connections
				p.setLogAbandoned(true); // Print stack trace for unclosed connections

				pool = new DataSource(p);

				logger.info("DB connection pool initialized (" + driver + ", user: " + user + ")");
				isShutdown = false;
			} catch(Exception e) {
				throw new VmtDbException(VmtDbException.CONN_POOL_STARTUP, e);
			}
		}
	}

	/**
	 * Returns a new connection from the pool using the given username and password.
	 *
	 * @param username
	 * @param password
	 * @return
	 * @throws VmtDbException
	 */
	public synchronized Connection getConnection(String username, String password) throws VmtDbException {
		if(logger.isTraceEnabled())
			logger.trace("DB connectionPool.getConnection()");

		if (isShutdown)
			throw new VmtDbException(VmtDbException.CONN_POOL_DOWN);

		try {
			if(username!=null && password!=null)
				return pool.getConnection(username, password);
			else
				return pool.getConnection();
		} catch(SQLException e) {
			throw new VmtDbException(VmtDbException.CONN_POOL_GET_CONN_ERR, e);
		}
	}

	/**
	 * Returns a new connection from the connection pool.
	 *
	 * @return A valid connection to the DB provided by the internal connection pool.
	 * @throws VmtDbException - Err code {@link VmtDbException#CONN_POOL_DOWN}
	 * is thrown in case the pool is being shut down.
	 */
	public Connection getConnection() throws VmtDbException {
		return getConnection(null, null);
	}

	public synchronized void shutdown() {
		isShutdown = true;
		logger.info("Shutting down db connection pool");
	}

	/**
	 * Other utilities in the package may require the DataSource
	 */
	public DataSource getInternalPool() {
		return pool;
	}

}
