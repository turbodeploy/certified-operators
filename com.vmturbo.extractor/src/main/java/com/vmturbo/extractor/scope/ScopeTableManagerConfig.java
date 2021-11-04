package com.vmturbo.extractor.scope;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.extractor.ExtractorDbConfig;

/**
 * Config class for the scope table manager.
 */
@Configuration
@Import({ExtractorDbConfig.class})
public class ScopeTableManagerConfig {
    private final Logger logger = LogManager.getLogger();

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    /**
     * Create a new {@link ScopeTableManager}. Schedule the task for processing the scope table if
     * feature flag is enabled.
     *
     * @return scope table manager
     */
    @Bean
    public ScopeTableManager scopeTableManager() {
        ScopeTableManager scopeTableManager = null;

        scopeTableManager = new ScopeTableManager(extractorDbConfig.ingesterEndpoint());
        return scopeTableManager;
    }
}
