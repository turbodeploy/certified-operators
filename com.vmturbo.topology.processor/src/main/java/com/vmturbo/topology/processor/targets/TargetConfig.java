package com.vmturbo.topology.processor.targets;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.sql.utils.SQLDatabaseConfig;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.probes.ProbeConfig;

/**
 * Configuration for the target package.
 */
@Configuration
@Import({ProbeConfig.class, KVConfig.class, SQLDatabaseConfig.class})
public class TargetConfig {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Bean
    public TargetStore targetStore() {
        return new KVBackedTargetStore(
                kvConfig.keyValueStore(),
                probeConfig.probeStore(),
                identityStore());
    }

    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    @Bean
    public PersistentIdentityStore persistentIdentityStore() {
        return new PersistentTargetSpecIdentityStore(databaseConfig.dsl());
    }

    @Bean
    public IdentityStore<TargetSpec> identityStore() {
        return new CachingIdentityStore<>(new TargetSpecAttributeExtractor(probeConfig.probeStore()),
                persistentIdentityStore(), identityInitializer());
    }

    @Bean
    public DerivedTargetParser derivedTargetParser() {
        return new DerivedTargetParser(probeConfig.probeStore(), targetStore());
    }
}
