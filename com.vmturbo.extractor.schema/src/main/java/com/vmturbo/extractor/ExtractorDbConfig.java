package com.vmturbo.extractor;

import java.util.function.Supplier;

import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.SQLDatabaseConfig2;

/**
 * Configuration of DB endpoints needed for extractor component.
 */
@Configuration
@Import(SQLDatabaseConfig2.class)
public class ExtractorDbConfig {

    @Autowired
    private SQLDatabaseConfig2 sqlDatabaseConfig2;

    /**
     * General R/W endpoint for persisting data from topologies.
     *
     * @return ingestion endpoint
     */
    @Bean
    public Supplier<DbEndpoint> ingesterEndpoint() {
        return sqlDatabaseConfig2.primaryDbEndpoint(SQLDialect.POSTGRES)
                .withDbAccess(DbEndpointAccess.ALL)
                .withDbDestructiveProvisioningEnabled(true)
                .build();
    }

    /**
     * R/W endpoint to same data, for in query execution.
     *
     * @return query endpoint
     */
    @Bean
    public Supplier<DbEndpoint> queryEndpoint() {
        return sqlDatabaseConfig2.secondaryDbEndpoint("q", SQLDialect.POSTGRES)
                .like(ingesterEndpoint())
                .withDbAccess(DbEndpointAccess.READ_ONLY)
                .withNoDbMigrations()
                .build();
    }
}
