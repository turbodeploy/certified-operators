package com.vmturbo.reports.component;

import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.mariadb.jdbc.MariaDbDataSource;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.FlywayMigrator;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for DB connection in the reporting component.
 */
@Configuration
public class ReportingDbConfig extends SQLDatabaseConfig {

    /**
     * Schema name used for reporting-specific DB tables.
     */
    public static final String REPORTING_SCHEMA = "reporting";
    /**
     * Location of SQL migration files for reporting component own DB.
     */
    public static final String MIGRATIONS_LOCATION = "db/reporting/migration";

    @Value("${dbUsername}")
    private String dbUsername;
    @Value("${dbSchemaName}")
    private String vmtDbSchema;


    @Override
    public Flyway flyway() {
        // Migrations are handled in history component instead.
        return new Flyway();
    }

    @Bean
    public DataSource reportingDatasource() {
        final MariaDbDataSource dataSource = new MariaDbDataSource();
        final DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute,
                authRetryDelaySecs);
        try {
            dataSource.setUrl(getSQLConfigObject().getDbUrl() + '/' + vmtDbSchema);
            dataSource.setUser(dbUsername);
            dataSource.setPassword(dbPasswordUtil.getSqlDbRootPassword());
            return dataSource;
        } catch (SQLException e) {
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage()) ;
        }
    }

    @Bean
    public Flyway localFlyway() {
        return new FlywayMigrator(Duration.ofMinutes(1), Duration.ofSeconds(5), () -> {
            final Flyway flyway = new Flyway();
            flyway.setDataSource(dataSource());
            flyway.setSchemas(REPORTING_SCHEMA);
            flyway.setLocations(MIGRATIONS_LOCATION);
            return flyway;
        }).migrate();
    }
}
