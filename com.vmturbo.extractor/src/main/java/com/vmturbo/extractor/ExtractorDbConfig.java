package com.vmturbo.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.vmturbo.extractor.flyway.ResetChecksumsForTimescaleDB201Migrations;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.SQLDatabaseConfig2;

/**
 * Config class that defines DB endpoints used by extractor component.
 */
@Configuration
@Import({SQLDatabaseConfig2.class, ExtractorGlobalConfig.class})
public class ExtractorDbConfig {
    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private ExtractorDbBaseConfig dbBaseConfig;

    @Autowired
    private ExtractorGlobalConfig extractorGlobalConfig;

    @Autowired
    private SQLDatabaseConfig2 dbConfig;

    /**
     * DB endpoint to use for topology ingestion.
     *
     * @return endpoint the endpoint
     */
    @Bean
    public DbEndpoint ingesterEndpoint() {
        return dbConfig.derivedDbEndpoint("dbs.extractor", dbBaseConfig.extractorDbEndpointBase())
                .withAccess(DbEndpointAccess.ALL)
                .withShouldProvision(true)
                .withRootAccessEnabled(true)
                .withEndpointEnabled(extractorGlobalConfig.requireDatabase())
                .withFlywayCallbacks(flywayCallbacks())
                .build();
    }

    /**
     * DB endpoint with read-only access, to be used to perform queries.
     *
     * @return read-only endpoint
     */
    @Bean
    public DbEndpoint queryEndpoint() {
        return dbConfig.derivedDbEndpoint("dbs.extractor.query",
                dbBaseConfig.extractorQueryDbEndpointBase())
                .withShouldProvisionUser(true)
                .withRootAccessEnabled(true)
                .withEndpointEnabled(extractorGlobalConfig.requireDatabase())
                .build();
    }


    /**
     * This endpoint is not used in our code. It's used to initialize the postgresdb user and
     * database that Grafana will use internally to store data (e.g. store locally saved/modified
     * dashboards). Grafana expects the user and database to exist when it starts up, and it's the
     * extractor component's responsibility to create them. The configuration properties (username,
     * password, and database name) are shared with the Grafana service configuration in k8s and
     * injected via the configmap.
     *
     * @return The {@link DbEndpoint}.
     */
    @Bean
    public DbEndpoint grafanaWriterEndpoint() {
        return dbConfig.dbEndpoint("dbs.grafana", SQLDialect.POSTGRES)
                .withSchemaName("grafana_writer")
                .withAccess(DbEndpointAccess.ALL)
                .withShouldProvision(true)
                .withMigrationLocations("db.migration.grafana")
                .withRootAccessEnabled(true)
                .withEndpointEnabled(r ->
                        r.apply("dbs.grafana.databaseName") != null
                                && r.apply("dbs.grafana.userName") != null
                                && r.apply("dbs.grafana.password") != null
                                && extractorGlobalConfig.featureFlags().isReportingEnabled())
                .build();
    }

    /**
     * This endpoint is not used in our code, but it's used to initialize the timescale datasource
     * in Grafana. This user has read-only access to the data written to by the ingester.
     *
     * @return The {@link DbEndpoint}.
     */
    @Bean
    public DbEndpoint grafanaQueryEndpoint() {
        return queryEndpoint();
    }

    /**
     * Callbacks to be configured for our Flyway migrations.
     *
     * <p>These can be used to handle issues such as problematic migrations that have been released
     * to customers and thus cannot generally be either replaced or removed from the migration
     * sequence.</p>
     *
     * <p>A component should define a {@link Primary} bean elsewhere in order to override the
     * empty default.</p>
     *
     * @return array of callback objects, in order in which they should be invoked
     */
    @Bean
    public FlywayCallback[] flywayCallbacks() {
        return new FlywayCallback[]{
                new ResetChecksumsForTimescaleDB201Migrations()
        };
    }
}
