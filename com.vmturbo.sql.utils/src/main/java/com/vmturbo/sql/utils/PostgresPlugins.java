package com.vmturbo.sql.utils;

/**
 * Plugins available for Postgres endpoints.
 */
public enum PostgresPlugins implements PostgresPlugin {

    /** TimescaleDb plugin v2.0.1. */
    TIMESCALE_2_0_1("timescaledb", "2.0.1");

    private final String name;
    private final String version;

    PostgresPlugins(String name, String version) {
        this.name = name;
        this.version = version;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getVersion() {
        return version;
    }
}
