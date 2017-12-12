package com.vmturbo.reports.component;

import org.flywaydb.core.Flyway;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for DB connection in the reporting component.
 */
@Configuration
public class ReportingDbConfig extends SQLDatabaseConfig {

    @Override
    public Flyway flyway() {
        // Migrations are handled in history component instead.
        return new Flyway();
    }
}