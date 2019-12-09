package com.vmturbo.group.migration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.GroupComponentDBConfig;
import com.vmturbo.group.setting.SettingConfig;

@Configuration
@Import({GroupComponentDBConfig.class, SettingConfig.class})
public class MigrationConfig {

    @Autowired
    private GroupComponentDBConfig databaseConfig;

    @Autowired
    private SettingConfig settingConfig;

    @Bean
    public GroupMigrationsLibrary groupMigrationsLibrary() {
        return new GroupMigrationsLibrary(databaseConfig.dsl(), settingConfig.settingStore());
    }
}
