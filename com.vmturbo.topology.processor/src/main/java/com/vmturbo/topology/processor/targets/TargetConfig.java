package com.vmturbo.topology.processor.targets;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.authorization.keyprovider.EncryptionKeyProvider;
import com.vmturbo.auth.api.authorization.keyprovider.MasterKeyReader;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.components.crypto.CryptoFacility;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.PersistentIdentityStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.securekvstore.SecureKeyValueStoreConfig;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.KVConfig;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.probeproperties.GlobalProbePropertiesSettingsLoader;
import com.vmturbo.topology.processor.probeproperties.KVBackedProbePropertyStore;
import com.vmturbo.topology.processor.probeproperties.ProbePropertyStore;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.status.SQLTargetStatusStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.targets.status.TargetStatusTrackerImpl;

/**
 * Configuration for the target package.
 */
@Configuration
@SuppressFBWarnings
@Import({ProbeConfig.class, KVConfig.class, TopologyProcessorDBConfig.class,
    GroupClientConfig.class, RepositoryClientConfig.class, SecureKeyValueStoreConfig.class})
public class TargetConfig {

    @Value("${identityGeneratorPrefix:1}")
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

    @Autowired
    private ClockConfig clockConfig;

    @Value("${globalProbeSettingsLoadRetryIntervalSec:10}")
    private long globalProbeSettingsLoadRetryIntervalSec;
    @Value("${globalProbeSettingsLoadTimeoutSec:3}")
    private long globalProbeSettingsLoadTimeoutSec;

    @Bean
    public TargetStore targetStore() {
        CachingTargetStore store = new CachingTargetStore(targetDao(), probeConfig.probeStore(),
                identityStore());
        probeConfig.probeStore().addListener(store);
        return store;
    }

    /**
     * If true, use Kubernetes secrets to read in a master encryption key which is used to encrypt
     * and decrypt the internal, component-specific encryption keys.
     * If false, this data will be read from (legacy) persistent volumes.
     *
     * <p>Note: This feature flag is exposed in a static way to avoid having to refactor the
     * many static methods that already exist in {@link CryptoFacility}. This is expected to be a
     * short-lived situation, until enabling external secrets becomes the default.</p>
     */
    @Value("${" + BaseVmtComponentConfig.ENABLE_EXTERNAL_SECRETS_FLAG + ":false}")
    public void setKeyProviderStatic(boolean enableExternalSecrets){
        CryptoFacility.ENABLE_EXTERNAL_SECRETS = enableExternalSecrets;
        if (enableExternalSecrets) {
            CryptoFacility.encryptionKeyProvider =
                    new EncryptionKeyProvider(kvConfig.keyValueStore(), new MasterKeyReader());
        }
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

    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService globalSettingsLoadingThreadpool() {
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("global-settings-loader").build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }

    @Bean
    public GlobalProbePropertiesSettingsLoader globalProbePropertiesSettingsLoader() {
        return new GlobalProbePropertiesSettingsLoader(
                        SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()),
                        globalSettingsLoadingThreadpool(),
                        globalProbeSettingsLoadRetryIntervalSec,
                        globalProbeSettingsLoadTimeoutSec);
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
                Objects.requireNonNull(kvConfig.keyValueStore()),
                globalProbePropertiesSettingsLoader());
    }

    /**
     * Target status tracker listens target-related operations and persist certain details.
     *
     * @return an instance of the target status tracker
     */
    @Bean
    public TargetStatusTracker targetStatusTracker() {
        return new TargetStatusTrackerImpl(targetStatusStore(), targetStore(),
                probeConfig.probeStore(), clockConfig.clock());
    }

    /**
     * Persists target-status-related information.
     *
     * @return an instance of the target status DAO.
     */
    @Bean
    public TargetStatusStore targetStatusStore() {
        return new SQLTargetStatusStore(databaseConfig.dsl());
    }

}
