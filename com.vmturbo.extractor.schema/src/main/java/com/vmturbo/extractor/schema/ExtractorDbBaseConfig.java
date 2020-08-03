package com.vmturbo.extractor.schema;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.SQLDatabaseConfig2;

/**
 * Configuration of DB endpoints needed for extractor component.
 */
@Configuration
@Import(SQLDatabaseConfig2.class)
public class ExtractorDbBaseConfig {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Common tag name for configuring read-only query endpoint for metric data.
     *
     * <p>This endpoint is provisioned by extractor component but is also utilized in api.</p>
     */
    public static final String QUERY_ENDPOINT_TAG = "query";

    @Autowired
    private SQLDatabaseConfig2 dbConfig;

    /** Default name of database and schema for extractor database. */
    @Value("${extractorDatabaseName:extractor}")
    private String extractorDatabaseName;

    /**
     * Abstract endpoint to use as base for active endpoints that access the extractor database.
     *
     * @return ingestion endpoint
     */
    @Bean
    public DbEndpoint ingesterEndpointBase() {
        return dbConfig.abstractDbEndpoint()
                .withDbDatabaseName(extractorDatabaseName)
                .withDbSchemaName(extractorDatabaseName)
                .build();
    }
}
