package com.vmturbo.reports.component;

import java.sql.SQLException;
import java.time.Duration;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.jooq.conf.MappedSchema;
import org.jooq.impl.DefaultConfiguration;
import org.mariadb.jdbc.MariaDbDataSource;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.FlywayMigrator;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Reporting SQL connection configuration for tests.
 */
@Configuration
public class ReportingTestDbConfig extends TestSQLDatabaseConfig {

    /**
     * Test schema used for tests to access reporting schema.
     */
    private static final String REPORTING_TEST_SCHEMA = "reporting_test";

    @Bean
    public Flyway localFlyway() {
        return new FlywayMigrator(Duration.ofMinutes(1), Duration.ofSeconds(5), () -> {
            final Flyway flyway = new Flyway();
            flyway.setDataSource(flyway().getDataSource());
            flyway.setSchemas(REPORTING_TEST_SCHEMA);
            flyway.setLocations(ReportingDbConfig.MIGRATIONS_LOCATION);
            return flyway;
        }).migrate();
    }

    @Bean
    public DataSource reportingDatasource() {
        final MariaDbDataSource dataSource = new MariaDbDataSource();
        try {
            dataSource.setUrl(dbConfiguration().getDbUrl() + '/' + dbConfiguration().getTestSchemaName());
            dataSource.setUser("root");
            dataSource.setPassword("vmturbo");
            return dataSource;
        } catch (SQLException e) {
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage());
        }
    }

    @Bean
    public org.jooq.Configuration configuration() {
        final org.jooq.Configuration jooqConfiguration = dbConfiguration().getConfiguration();

        jooqConfiguration.settings()
                .getRenderMapping()
                .withSchemata(new MappedSchema().withInput(ReportingDbConfig.REPORTING_SCHEMA)
                        .withOutput(REPORTING_TEST_SCHEMA));
        return jooqConfiguration;
    }

    @PreDestroy
    public void cleanup() {
        localFlyway().clean();
        flyway().clean();
    }
}
