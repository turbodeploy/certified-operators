package com.vmturbo.group.policy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.group.GroupComponentDBConfig;
import com.vmturbo.group.IdentityProviderConfig;

@Configuration
@Import({GroupComponentDBConfig.class, IdentityProviderConfig.class})
public class PolicyConfig {

    @Autowired
    private GroupComponentDBConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public PolicyStore policyStore() {
        return new PolicyStore(databaseConfig.dsl(),
                discoveredPoliciesMapperFactory(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public DiscoveredPoliciesMapperFactory discoveredPoliciesMapperFactory() {
        return new DiscoveredPoliciesMapperFactory.DefaultDiscoveredPoliciesMapperFactory();
    }
}
