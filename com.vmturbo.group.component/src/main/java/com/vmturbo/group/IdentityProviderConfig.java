package com.vmturbo.group;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.group.identity.IdentityProvider;

@Configuration
public class IdentityProviderConfig {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Bean
    public IdentityProvider identityProvider() {
        return new IdentityProvider(identityGeneratorPrefix);
    }
}
