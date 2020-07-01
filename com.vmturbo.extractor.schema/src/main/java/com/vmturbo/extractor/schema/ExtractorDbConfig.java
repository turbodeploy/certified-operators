package com.vmturbo.extractor.schema;

import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;

/**
 * Configuration of DB endpoints needed for extractor component.
 */
@Configuration
public class ExtractorDbConfig {
    private static final Logger logger = LogManager.getLogger();

    private static final String DEFAULT_MIGRATION_LOCATIONS = "db.migration";

    @Value("${dbMigrationLocation:}")
    private String migrationLocationsOverride;

    /**
     * Username for the user Grafana will use to store its internal data into postgres.
     */
    @Value("${grafanaDb.user:}")
    private String grafanaDataUsername;

    /**
     * Password for the user Grafana will use to store its internal data into postgres.
     * We need to create the user with this password so Grafana can log in properly.
     */
    @Value("${grafanaDb.password:}")
    private String grafanaDataPassword;

    /**
     * Database name for Grafana's internal data in postgres.
     */
    @Value("${grafanaDb.name:}")
    private String grafanaDataDbName;

    /**
     * Username for the user the Timescale datasource will use in Grafana. This user should have
     * read access to the entity and metrics tables for reports.
     */
    @Value("${grafanaReaderUsername:grafana_reader}")
    private String grafanaReaderUsername;

    /**
     * General R/W endpoint for persisting data from topologies.
     *
     * @return ingestion endpoint
     */
    @Bean
    public DbEndpoint ingesterEndpoint() {
        String migrationLocations = DEFAULT_MIGRATION_LOCATIONS;
        if (!StringUtils.isEmpty(migrationLocationsOverride)) {
            migrationLocations = migrationLocationsOverride;
        }
        return DbEndpoint.secondaryDbEndpoint("extractor", SQLDialect.POSTGRES)
                .withDbAccess(DbEndpointAccess.ALL)
                .withDbDestructiveProvisioningEnabled(true)
                .withDbMigrationLocations(migrationLocations)
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
    public Optional<DbEndpoint> grafanaWriterEndpoint() {
        if (!StringUtils.isAnyEmpty(grafanaDataDbName, grafanaDataPassword, grafanaDataUsername)) {
            logger.info("Creating database endpoint for Grafana. Database {}, user {}",
                    grafanaDataDbName, grafanaDataUsername);
            return Optional.of(DbEndpoint.secondaryDbEndpoint("grafana_writer", SQLDialect.POSTGRES)
                    .withDbAccess(DbEndpointAccess.ALL)
                    .withDbUserName(grafanaDataUsername)
                    .withDbPassword(grafanaDataPassword)
                    .withDbDatabaseName(grafanaDataDbName)
                    .withNoDbMigrations()
                    .build());
        } else {
            logger.info("Skipping database endpoint creation for Grafana.");
            return Optional.empty();
        }
    }

    /**
     * This endpoint is not used in our code, but it's used to initialize the timescale
     * datasource in Grafana. This user has read-only access to the data written to by the ingester.
     *
     * @return The {@link DbEndpoint}.
     */
    @Bean
    public DbEndpoint grafanaQueryEndpoint() {
        return DbEndpoint.secondaryDbEndpoint("grafana_reader", SQLDialect.POSTGRES)
                .like(ingesterEndpoint())
                .withDbAccess(DbEndpointAccess.READ_ONLY)
                .withDbUserName(grafanaReaderUsername)
                .withNoDbMigrations()
                .build();
    }

    /**
     * Read endpoint to same data, for in query execution.
     *
     * @return query endpoint
     */
    @Bean
    public DbEndpoint queryEndpoint() {
        return DbEndpoint.secondaryDbEndpoint("q", SQLDialect.POSTGRES)
                .like(ingesterEndpoint())
                .withDbAccess(DbEndpointAccess.READ_ONLY)
                .withNoDbMigrations()
                .build();
    }
}
