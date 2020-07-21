package com.vmturbo.search;

import static com.vmturbo.extractor.schema.ExtractorDbBaseConfig.QUERY_ENDPOINT_TAG;

import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Configuration of {@link DbEndpoint} needed for component.
 *
 * <p>We only configure {@link DbEndpoint} here.
 * Initialization occurs in ApiComponent.onStartComponent()</p>
 */
@Configuration
@Import({ExtractorDbBaseConfig.class})
public class SearchDBConfig {

    @Autowired
    private ExtractorDbBaseConfig extractorDbBaseConfig;

    @Value("${enableSearchApi:false}")
    private boolean enableSearchApi;

    @Value("${apiPaginationDefaultLimit:100}")
    private int apiPaginationDefaultLimit;

    @Value("${apiPaginationMaxLimit:500}")
    private int apiPaginationMaxLimit;

    @Bean
    DbEndpoint queryEndpoint() {
        return DbEndpoint.secondaryDbEndpoint(QUERY_ENDPOINT_TAG, SQLDialect.POSTGRES)
                .like(extractorDbBaseConfig.ingesterEndpointBase())
                // extractor component is responsible for provisioning
                .withDbShouldProvision(false)
                .withNoDbMigrations()
                .build();
    }

    @Bean
    public ApiQueryEngine apiQueryEngine() {
        return new ApiQueryEngine(queryEndpoint(), enableSearchApi, apiPaginationDefaultLimit, apiPaginationMaxLimit);
    }

}
