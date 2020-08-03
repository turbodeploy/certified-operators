package com.vmturbo.topology.processor.actions;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The {@link ActionMergeSpecsConfig} provides {@link ActionMergeSpecsRepository} Bean.
 */
@Configuration
public class ActionMergeSpecsConfig {

    /**
     * Returns the singleton of the action merge specs repository.
     *
     * @return The singleton of the action merge specs repository.
     */
    @Bean
    public ActionMergeSpecsRepository actionMergeSpecsRepository() {
        return new ActionMergeSpecsRepository();
    }
}
