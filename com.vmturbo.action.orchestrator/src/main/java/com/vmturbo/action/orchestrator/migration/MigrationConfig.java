package com.vmturbo.action.orchestrator.migration;

import com.vmturbo.sql.utils.SQLDatabaseConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SQLDatabaseConfig.class})
public class MigrationConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Bean
    public ActionMigrationsLibrary actionsMigrationsLibrary() {
        return new ActionMigrationsLibrary(databaseConfig.dsl());
    }
}
