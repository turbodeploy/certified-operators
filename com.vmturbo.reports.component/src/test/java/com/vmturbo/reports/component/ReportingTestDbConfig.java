package com.vmturbo.reports.component;

import java.time.Duration;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.jooq.conf.MappedSchema;
import org.jooq.impl.DefaultConfiguration;
import org.mariadb.jdbc.MySQLDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.reports.util.SchemaUtil;
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
            flyway.setDataSource(dataSource());
            flyway.setSchemas(REPORTING_TEST_SCHEMA);
            flyway.setLocations(ReportingDbConfig.MIGRATIONS_LOCATION);
            return flyway;
        }).migrate();
    }

    @Bean
    public DataSource reportingDatasource() {
        final MySQLDataSource dataSource = new MySQLDataSource();
        dataSource.setUrl(getDbUrl() + '/' + testSchemaName());
        dataSource.setUser("root");
        dataSource.setPassword("vmturbo");
        return dataSource;
    }

    @Bean
    public DefaultConfiguration configuration() {
        final DefaultConfiguration jooqConfiguration = super.configuration();

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
