package com.vmturbo.group.schedule;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.DbAccessConfig;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Spring configuration of Schedule backend.
 */
@Configuration
@Import({DbAccessConfig.class,  IdentityProviderConfig.class})
public class ScheduleConfig {
    /** Database config. */
    @Autowired
    private DbAccessConfig databaseConfig;

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
        try {
            return new ScheduleStore(
                databaseConfig.dsl(),
                scheduleValidator(),
                identityProviderConfig.identityProvider()
            );
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ScheduleStore", e);
        }
    }

}
