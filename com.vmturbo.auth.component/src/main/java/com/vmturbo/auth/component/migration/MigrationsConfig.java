package com.vmturbo.auth.component.migration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.component.AuthKVConfig;
import com.vmturbo.auth.component.AuthRESTSecurityConfig;

@Configuration
@Import({AuthKVConfig.class})
public class MigrationsConfig {

    @Autowired
    private AuthKVConfig authKVConfig;

    @Bean
    public MigrationsLibrary migrationsList() {
        return new MigrationsLibrary(authKVConfig.authKeyValueStore());
    }
}
