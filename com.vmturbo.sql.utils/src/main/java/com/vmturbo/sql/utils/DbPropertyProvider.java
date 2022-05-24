package com.vmturbo.sql.utils;

/**
 * Interface to provide key DB configuration property values needed for accessing a database.
 *
 * <p>This is primarily needed because we have not yet switched to a {@link DbEndpoint} strategy
 * for general use, and some property defaults are currently hidden in per-component subclasses of
 * {@link SQLDatabaseConfig}. Those classes cannot be scanned by Spring without performing DB
 * initialization, and therefore, they are effectively inaccessible to the Kibitzer module.
 * Extracting these properties into a separate config class within their component creates a bean
 * that can be used by Kibitzer without the unwanted side-effects.</p>
 */
public interface DbPropertyProvider {
    /**
     * Get the database name. Same as schema name for MariaDB/MySQL.
     *
     * @return the database name
     */
    String getDatabaseName();

    /**
     * Get the schema name.
     *
     * @return the schema name
     */
    String getSchemaName();

    /**
     * Get the login user name.
     *
     * @return the user name
     */
    String getUserName();

    /**
     * Get the login password.
     *
     * @return the password
     */
    String getPassword();
}
