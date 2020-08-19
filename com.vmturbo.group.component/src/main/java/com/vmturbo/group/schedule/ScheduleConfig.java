package com.vmturbo.group.schedule;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.GroupComponentDBConfig;
import com.vmturbo.group.IdentityProviderConfig;

/**
 * Spring configuration of Schedule backend.
 */
@Configuration
@Import({GroupComponentDBConfig.class,  IdentityProviderConfig.class})
public class ScheduleConfig {
    /** Database config. */
    @Autowired
    private GroupComponentDBConfig databaseConfig;

    /** Identity provider config. */
    @Autowired
    private IdentityProviderConfig identityProviderConfig;

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
            identityProviderConfig.identityProvider()
        );
    }

}
