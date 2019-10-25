package com.vmturbo.group.policy;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, IdentityProviderConfig.class, GroupConfig.class})
public class PolicyConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Bean
    public PolicyStore policyStore() {
        return new PolicyStore(databaseConfig.dsl(),
                discoveredPoliciesMapperFactory(),
                identityProviderConfig.identityProvider(),
                groupConfig.groupStore());
    }

    @Bean
    public DiscoveredPoliciesMapperFactory discoveredPoliciesMapperFactory() {
        return new DiscoveredPoliciesMapperFactory.DefaultDiscoveredPoliciesMapperFactory();
    }
}
