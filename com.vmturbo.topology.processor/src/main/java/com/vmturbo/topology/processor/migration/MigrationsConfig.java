package com.vmturbo.topology.processor.migration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;

@Configuration
@Import({
        IdentityProviderConfig.class,
        ProbeConfig.class,
        StitchingConfig.class,
        SQLDatabaseConfig.class,
        KVConfig.class})
public class MigrationsConfig {

    @Autowired
    IdentityProviderConfig identityProviderConfig;

    @Autowired
    ProbeConfig probeConfig;

    @Autowired
    SQLDatabaseConfig sqlDatabaseConfig;

    @Autowired
    StitchingConfig stitchingConfig;

    @Autowired
    KVConfig kvConfig;

    @Bean
    public MigrationsLibrary migrationsList() {
        return new MigrationsLibrary(sqlDatabaseConfig.dsl(),
                probeConfig.probeStore(), stitchingConfig.historyClient(),
                identityProviderConfig.underlyingStore(),
                identityProviderConfig.identityProvider(),
                kvConfig.keyValueStore());
    }
}
