package com.vmturbo.group.group;

import java.util.concurrent.TimeUnit;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.flyway.V1_11_Callback;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({IdentityProviderConfig.class,
        SQLDatabaseConfig.class})
public class GroupConfig {

    @Value("${tempGroupExpirationTimeMins:10}")
    private int tempGroupExpirationTimeMins;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    /**
     * Define flyway callbacks to be active during migrations for group component.
     *
     * @return array of callback objects
     */
    @Bean
    @Primary
    public FlywayCallback[] flywayCallbacks() {
        return new FlywayCallback[] {
            new V1_11_Callback()
        };
    }

    @Bean
    public TemporaryGroupCache temporaryGroupCache() {
        return new TemporaryGroupCache(identityProviderConfig.identityProvider(),
                tempGroupExpirationTimeMins,
                TimeUnit.MINUTES);
    }

    @Bean
    public GroupDAO groupStore() {
        return new GroupDAO(databaseConfig.dsl(),
                        identityProviderConfig.identityProvider());
    }
}
