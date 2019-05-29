package com.vmturbo.group.migration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.group.setting.SettingConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, GroupConfig.class, SettingConfig.class})
public class MigrationConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private SettingConfig settingConfig;

    @Bean
    public GroupMigrationsLibrary groupMigrationsLibrary() {
        return new GroupMigrationsLibrary(databaseConfig.dsl(), groupConfig.groupStore(),
            settingConfig.settingStore());
    }
}
