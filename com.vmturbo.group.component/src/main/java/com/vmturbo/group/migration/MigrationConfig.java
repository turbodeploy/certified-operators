package com.vmturbo.group.migration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class})
public class MigrationConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Bean
    public GroupMigrationsLibrary groupMigrationsLibrary() {
        return new GroupMigrationsLibrary(databaseConfig.dsl());
    }
}
