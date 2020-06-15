package com.vmturbo.reports.component;

import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.jooq.impl.DefaultExecuteListenerProvider;
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

    @Bean
    public DataSource reportingDatasource() {
        final MariaDbDataSource dataSource = new MariaDbDataSource();
        final DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute,
                authRetryDelaySecs);
        try {
            // getSQLConfigObject().getDbUrl() already contains the path of the given vmtDbSchema.
            dataSource.setUrl(getSQLConfigObject().getDbUrl());
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

    /**
     * Return DSL context used by ReportingDbConfig.
     *
     * @return DSL context.
     */
    @Bean
    public DSLContext dsl() {
        // As reports component is to be refactored, we'll ignore reports component for multi-tenant
        // database support for now. Here we use root credentials to access database.
        // TODO: Will need to be modified to support multi-tenant database once reporting is fully
        // supported.
        return new DefaultDSLContext(reportingDBConfiguration());
    }

    private DefaultConfiguration reportingDBConfiguration() {
        DefaultConfiguration jooqConfiguration = new DefaultConfiguration();
        jooqConfiguration.set(connectionProvider());
        jooqConfiguration.set(new Settings().withRenderNameStyle(RenderNameStyle.LOWER));
        jooqConfiguration.set(new DefaultExecuteListenerProvider(exceptionTranslator()));
        jooqConfiguration.set(getSQLConfigObject().getSqlDialect());

        return jooqConfiguration;
    }
}
