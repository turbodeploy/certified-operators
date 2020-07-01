package com.vmturbo.search;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.extractor.schema.ExtractorDbConfig;
import com.vmturbo.sql.utils.DbEndpoint;

/**
 * Configuration of {@link DbEndpoint} needed for component.
 *
 * <p>We only configure {@link DbEndpoint} here.
 * Initialization occurs in ApiComponent.onStartComponent()</p>
 */
@Configuration
@Import({ExtractorDbConfig.class})
public class SearchDBConfig {

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    @Value("${enableSearchApi:false}")
    private boolean enableSearchApi;

    @Bean
    public ApiQueryEngine apiQueryEngine() {
        return new ApiQueryEngine(this.extractorDbConfig.queryEndpoint(), enableSearchApi);
    }

}
