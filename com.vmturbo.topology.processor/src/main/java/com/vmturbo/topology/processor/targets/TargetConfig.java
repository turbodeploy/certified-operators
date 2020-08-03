package com.vmturbo.topology.processor.targets;

import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.securekvstore.SecureKeyValueStoreConfig;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.probeproperties.KVBackedProbePropertyStore;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeConfig;

/**
 * Configuration for the target package.
 */
@Configuration
@Import({ProbeConfig.class, KVConfig.class, TopologyProcessorDBConfig.class, GroupClientConfig.class,
        RepositoryClientConfig.class, SecureKeyValueStoreConfig.class})
public class TargetConfig {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Value("${enableSecureStore:false}")
    private boolean enableSecureStore;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private KVConfig kvConfig;

    @Autowired
    private SecureKeyValueStoreConfig vaultKeyValueStoreConfig;

    @Autowired
    private TopologyProcessorDBConfig databaseConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Bean
    public TargetStore targetStore() {
        CachingTargetStore store = new CachingTargetStore(targetDao(),
                probeConfig.probeStore(),
                identityStore());
        probeConfig.probeStore().addListener(store);
        return store;
    }

    /**
     * Persists target-related information.
     *
     * @return The {@link TargetDao}.
     */
    @Bean
    public TargetDao targetDao() {
        final KeyValueStore kvStore = enableSecureStore ?
            vaultKeyValueStoreConfig.vaultKeyValueStore() : kvConfig.keyValueStore();
        return new KvTargetDao(kvStore, probeConfig.probeStore());
    }

    /**
     * Initializes identity generation with the correct prefix.
     *
     * @return The {@link IdentityInitializer}.
     */
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

    @Bean
    public GroupScopeResolver groupScopeResolver() {
        return new GroupScopeResolver(
                groupClientConfig.groupChannel(),
                repositoryClientConfig.repositoryChannel(),
                targetStore(),
                entityConfig.entityStore());
    }

    /**
     * Per-probe and per-target properties to deliver to probe instasnces.
     *
     * @return property store
     */
    @Bean
    public ProbePropertyStore probePropertyStore() {
        return new KVBackedProbePropertyStore(
                Objects.requireNonNull(probeConfig.probeStore()),
                Objects.requireNonNull(targetStore()),
                Objects.requireNonNull(kvConfig.keyValueStore()));
    }

}
