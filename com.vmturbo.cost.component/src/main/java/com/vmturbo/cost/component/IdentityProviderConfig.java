package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;

@Configuration
public class IdentityProviderConfig {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Bean
    public IdentityProvider identityProvider() {
        return new DefaultIdentityProvider(identityGeneratorPrefix);
    }
}
