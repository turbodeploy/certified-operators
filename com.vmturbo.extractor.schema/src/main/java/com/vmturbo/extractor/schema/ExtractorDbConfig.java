package com.vmturbo.extractor.schema;

import org.jooq.SQLDialect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;

/**
 * Configuration of DB endpoints needed for extractor component.
 */
@Configuration
public class ExtractorDbConfig {

    /**
     * General R/W endpoint for persisting data from topologies.
     *
     * @return ingestion endpoint
     */
    @Bean
    public DbEndpoint ingesterEndpoint() {
        return DbEndpoint.primaryDbEndpoint(SQLDialect.POSTGRES)
                .withDbAccess(DbEndpointAccess.ALL)
                .withDbDestructiveProvisioningEnabled(true)
                .withDbMigrationLocations("db.migration")
                .build();
    }

    /**
     * R/W endpoint to same data, for in query execution.
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
