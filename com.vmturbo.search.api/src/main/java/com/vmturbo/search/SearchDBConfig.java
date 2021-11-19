package com.vmturbo.search;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.vmturbo.extractor.schema.SearchDbBaseConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpointsConfig;

/**
 * Configuration of {@link DbEndpoint} needed for component.
 *
 * <p>We only configure {@link DbEndpoint} here.
 * Initialization occurs in ApiComponent.onStartComponent()</p>
 */
@Configuration
@Import({DbEndpointsConfig.class, SearchDbBaseConfig.class})
public class SearchDBConfig {

    @Autowired
    private SearchDbBaseConfig searchDbBaseConfig;

    @Autowired
    private DbEndpointsConfig dbConfig;

    @Value("${enableSearchApi:false}")
    private boolean enableSearchApi;

    @Value("${apiPaginationDefaultLimit:100}")
    private int apiPaginationDefaultLimit;

    @Value("${apiPaginationMaxLimit:500}")
    private int apiPaginationMaxLimit;

    @Primary
    @Bean
    DbEndpoint queryEndpoint() {
        return dbConfig.derivedDbEndpoint("dbs.extractor.query",
                        searchDbBaseConfig.extractorMySqlDbEndpoint())
                // extractor component is responsible for provisioning
                .withShouldProvision(false)
                .build();
    }

    @Bean
    public ApiQueryEngine apiQueryEngine() {
        return new ApiQueryEngine(queryEndpoint(), enableSearchApi, apiPaginationDefaultLimit, apiPaginationMaxLimit);
    }
}
