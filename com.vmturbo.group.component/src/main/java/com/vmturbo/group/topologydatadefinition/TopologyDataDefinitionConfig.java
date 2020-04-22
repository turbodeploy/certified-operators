package com.vmturbo.group.topologydatadefinition;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.group.GroupComponentDBConfig;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.IdentityStore;

/**
 * Configuration for TopologyDataDefinition RPC Service.
 */
@Configuration
@Import({GroupComponentDBConfig.class, IdentityProviderConfig.class, GroupConfig.class})
public class TopologyDataDefinitionConfig {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Autowired
    private GroupComponentDBConfig databaseConfig;

    /**
     * Initializes identity generation with the correct prefix.
     *
     * @return The {@link IdentityInitializer}.
     */
    @Bean
    public IdentityInitializer identityInitializer() {
        return new IdentityInitializer(identityGeneratorPrefix);
    }

    /**
     * Create a CachingIdentityStore for TopologyDataDefinition.
     *
     * @return IdentityStore based on TopologyDataDefinitions using new instances of
     * {@link TopologyDataDefinitionAttributeExtractor} and
     * {@link PersistentTopologyDataDefinitionIdentityStore}
     */
    @Bean
    public IdentityStore<TopologyDataDefinition> topologyDataDefinitionIdentityStore() {
        return new CachingIdentityStore<>(new TopologyDataDefinitionAttributeExtractor(),
            new PersistentTopologyDataDefinitionIdentityStore(), identityInitializer());
    }

    /**
     * Create a TopologyDataDefinitionStore.
     *
     * @return TopologyDataDefinitionStore needed by TopologyDataDefinitionRpcService.
     */
    @Bean
    public TopologyDataDefinitionStore topologyDataDefinitionStore() {
        return new TopologyDataDefinitionStore(databaseConfig.dsl(),
            topologyDataDefinitionIdentityStore());
    }
}
