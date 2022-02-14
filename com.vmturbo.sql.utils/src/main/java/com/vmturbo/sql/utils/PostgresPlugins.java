package com.vmturbo.sql.utils;

import java.util.Optional;

/**
 * Plugins available for Postgres endpoints.
 */
public enum PostgresPlugins implements PostgresPlugin {

    /** TimescaleDb plugin v2.0.1. */
    TIMESCALE_2_0_1("timescaledb", "2.0.1", null),

    /** pg_partman plugin v4.6.0. */
    PARTMAN_4_6_0("pg_partman", "4.6.0", "partman");

    private final String name;
    private final String version;
    private final String pluginSchema;

    PostgresPlugins(String name, String version, String pluginSchema) {
        this.name = name;
        this.version = version;
        this.pluginSchema = pluginSchema;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public Optional<String> getPluginSchema() {
        return Optional.ofNullable(pluginSchema);
    }
}
