package com.vmturbo.topology.processor.migration;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.processor.DbAccessConfig;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

@Configuration
@Import({
        IdentityProviderConfig.class,
        ProbeConfig.class,
        StitchingConfig.class,
        DbAccessConfig.class,
        KVConfig.class, GroupConfig.class})
public class MigrationsConfig {

    @Autowired
    IdentityProviderConfig identityProviderConfig;

    @Autowired
    ProbeConfig probeConfig;

    @Autowired
    DbAccessConfig dbAccessConfig;

    @Autowired
    StitchingConfig stitchingConfig;

    @Autowired
    KVConfig kvConfig;

    @Autowired
    TargetConfig targetConfig;

    @Autowired
    GroupConfig groupConfig;

    @Bean
    public MigrationsLibrary migrationsList() {
        try {
            return new MigrationsLibrary(dbAccessConfig.dsl(),
                    probeConfig.probeStore(), stitchingConfig.historyClient(),
                    identityProviderConfig.identityProvider(),
                    kvConfig.keyValueStore(), targetConfig.targetStore(), targetConfig.targetDao(),
                    targetConfig.identityStore(), groupConfig.groupScopeResolver());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create MigrationsLibrary", e);
        }
    }
}
