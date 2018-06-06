package com.vmturbo.group.group;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.policy.PolicyConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({IdentityProviderConfig.class,
        SQLDatabaseConfig.class,
        PolicyConfig.class})
public class GroupConfig {

    @Value("${tempGroupExpirationTimeMins:10}")
    private int tempGroupExpirationTimeMins;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private PolicyConfig policyConfig;

    @Bean
    public TemporaryGroupCache temporaryGroupCache() {
        return new TemporaryGroupCache(identityProviderConfig.identityProvider(),
                tempGroupExpirationTimeMins,
                TimeUnit.MINUTES);
    }

    @Bean
    public GroupStore groupStore() {
        return new GroupStore(databaseConfig.dsl(),
                policyConfig.policyStore(),
                identityProviderConfig.identityProvider());
    }
}
