package com.vmturbo.extractor.schema;

import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration of DB endpoints needed for extractor component.
 */
@Configuration
public class ExtractorDbBaseConfig extends DbEndpointsConfig {

    /** Default name of database for extractor database. */
    @Value("${dbs.extractor.databaseName:extractor}")
    private String extractorDatabaseName;

    /** Default name of schema for extractor database. */
    @Value("${dbs.extractor.schemaName:extractor}")
    private String extractorSchemaName;

    /** Default user name for read-only access to extractor DB. */
    @Value("${dbs.extractor.query.userName:query}")
    private String queryUserName;

    /**
     * Default host for postgres database.
     */
    @Value("${dbHost:timescaledb}")
    private String dbHost;

    /**
     * Abstract endpoint to use as base for active endpoints that access the extractor database.
     *
     * @return endpoint bound to extractor database
     */
    @Bean
    public DbEndpoint extractorDbEndpointBase() {
        return abstractDbEndpoint(null, SQLDialect.POSTGRES)
                .withDatabaseName(extractorDatabaseName)
                .withSchemaName(extractorSchemaName)
                .withHost(dbHost)
                .build();
    }

    /**
     * Abstract endpoint to use as a base for active endpoints that need read-only access to the
     * extractor database.
     *
     * @return  read-only endpoint bound to extractor database
     */
    @Bean
    public DbEndpoint extractorQueryDbEndpointBase() {
        return abstractDbEndpoint(null, SQLDialect.POSTGRES)
                .like(extractorDbEndpointBase())
                .withUserName(queryUserName)
                .withHost(dbHost)
                .withAccess(DbEndpointAccess.READ_ONLY)
                .build();
    }
}
