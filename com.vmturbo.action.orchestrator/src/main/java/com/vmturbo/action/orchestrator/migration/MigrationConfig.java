package com.vmturbo.action.orchestrator.migration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorDBConfig;

@Configuration
@Import({ActionOrchestratorDBConfig.class})
public class MigrationConfig {

    @Autowired
    private ActionOrchestratorDBConfig databaseConfig;

    @Bean
    public ActionMigrationsLibrary actionsMigrationsLibrary() {
        return new ActionMigrationsLibrary(databaseConfig.dsl());
    }
}
