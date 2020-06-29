package com.vmturbo.extractor.schema;

import org.apache.commons.lang3.StringUtils;
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

    private static final String DEFAULT_MIGRATION_LOCATIONS = "db.migration";

    @Value("${dbMigrationLocation:}")
    private String migrationLocationsOverride;

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
