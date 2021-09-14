package com.vmturbo.search;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.SearchDbBaseConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.SQLDatabaseConfig2;

/**
 * Configuration of {@link DbEndpoint} needed for component.
 *
 * <p>We only configure {@link DbEndpoint} here.
 * Initialization occurs in ApiComponent.onStartComponent()</p>
 */
@Configuration
@Import({ExtractorDbBaseConfig.class, SQLDatabaseConfig2.class, SearchDbBaseConfig.class})
public class SearchDBConfig {

    @Autowired
    private ExtractorDbBaseConfig extractorDbBaseConfig;

    @Autowired
    private SearchDbBaseConfig searchDbBaseConfig;

    @Autowired
    private SQLDatabaseConfig2 dbConfig;

    @Value("${enableSearchApi:false}")
    private boolean enableSearchApi;

    @Value("${apiPaginationDefaultLimit:100}")
    private int apiPaginationDefaultLimit;

    @Value("${apiPaginationMaxLimit:500}")
    private int apiPaginationMaxLimit;

    @Bean
    DbEndpoint queryEndpoint() {
        // TODO when implementing queries from MySQL, change call to searchDbBaseConfig.extractorMySqlDbEndpoint()
        return dbConfig.derivedDbEndpoint("dbs.extractor.query",
                extractorDbBaseConfig.extractorQueryDbEndpointBase())
                // extractor component is responsible for provisioning
                .withShouldProvision(false)
                .build();
    }

    @Bean
    public ApiQueryEngine apiQueryEngine() {
        return new ApiQueryEngine(queryEndpoint(), enableSearchApi, apiPaginationDefaultLimit, apiPaginationMaxLimit);
    }
}
