package com.vmturbo.group.schedule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.setting.SettingConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Spring configuration of Schedule backend.
 */
@Configuration
@Import({SQLDatabaseConfig.class,  IdentityProviderConfig.class, SettingConfig.class})
public class ScheduleConfig {
    /** Database config. */
    @Autowired
    private SQLDatabaseConfig databaseConfig;

    /** Identity provider config. */
    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    /** Setting store. */
    @Autowired
    private SettingConfig settingConfig;

    /**
     * Schedule validator bean.
     * @return An instance of {@link ScheduleValidator}
     */
    @Bean
    public ScheduleValidator scheduleValidator() {
        return new DefaultScheduleValidator();
    }

    /**
     * Construct ScheduleStore bean.
     * @return An instance of {@link ScheduleStore}
     */
    public ScheduleStore scheduleStore() {
        return new ScheduleStore(
            databaseConfig.dsl(),
            scheduleValidator(),
            identityProviderConfig.identityProvider(),
            settingConfig.settingStore()
        );
    }

}
