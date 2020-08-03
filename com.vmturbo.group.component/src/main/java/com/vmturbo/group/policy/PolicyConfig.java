package com.vmturbo.group.policy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.GroupComponentDBConfig;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.group.GroupConfig;

@Configuration
@Import({GroupComponentDBConfig.class,
    IdentityProviderConfig.class,
    GroupConfig.class})
public class PolicyConfig {

    @Autowired
    private GroupComponentDBConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Bean
    public PolicyStore policyStore() {
        return new PolicyStore(databaseConfig.dsl(),
            identityProviderConfig.identityProvider(),
            policyValidator());
    }

    @Bean
    public PolicyValidator policyValidator() {
        return new PolicyValidator(groupConfig.groupStore());
    }

}
