package com.vmturbo.sql.utils;

/**
 * Interface for Postgres DB plugins, with an implementation of {@link #getInstallSQL(String)}
 * that works for Postgres plugins.
 */
public interface PostgresPlugin extends DbPlugin {

    @Override
    default String getInstallSQL(String schemaName) {
        return String.format("CREATE EXTENSION IF NOT EXISTS %s VERSION '%s' SCHEMA \"%s\"",
                getName(), getVersion(), schemaName);
    }
}
