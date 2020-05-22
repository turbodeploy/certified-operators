package com.vmturbo.extractor;

import static com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess.READ_ONLY;

import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.DbEndpoint;
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
     * @throws UnsupportedDialectException should not happen
     */
    @Bean
    public DbEndpoint ingesterEndpoint() throws UnsupportedDialectException {
        return sqlDatabaseConfig2.primaryDbEndpoint(SQLDialect.POSTGRES);
    }

    /**
     * R/W endpoint to same data, for in query execution.
     *
     * @return query endpoint
     * @throws UnsupportedDialectException should not happen
     */
    @Bean
    public DbEndpoint queryEndpoint() throws UnsupportedDialectException {
        return sqlDatabaseConfig2.secondaryDbEndpoint("q", SQLDialect.POSTGRES)
                .like(ingesterEndpoint()).withAccess(READ_ONLY).noMigration();
    }
}
