package com.vmturbo.sql.utils;

import java.util.Optional;

/**
 * Interface for Postgres DB plugins, with an implementation of {@link #getInstallSQL(String)}
 * that works for Postgres plugins.
 */
public interface PostgresPlugin extends DbPlugin {

    /**
     * Return the name of the schema the extension should be installed into; if absent, the
     * extension should install into the schema whose endpoint is calling it.
     *
     * @return schema name for the extension if specified for the extension
     */
    Optional<String> getPluginSchema();

    @Override
    default String getInstallSQL(String schemaName) {
        return String.format("CREATE EXTENSION IF NOT EXISTS %s VERSION '%s' SCHEMA \"%s\"",
                getName(), getVersion(), schemaName);
    }
}
