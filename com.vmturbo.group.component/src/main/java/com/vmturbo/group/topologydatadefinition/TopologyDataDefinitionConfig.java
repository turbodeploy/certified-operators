package com.vmturbo.group.topologydatadefinition;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.group.DbAccessConfig;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.identity.store.CachingIdentityStore;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Configuration for TopologyDataDefinition RPC Service.
 */
@Configuration
@Import({DbAccessConfig.class, IdentityProviderConfig.class, GroupConfig.class})
public class TopologyDataDefinitionConfig {

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Autowired
    private DbAccessConfig databaseConfig;

    @Autowired
    private GroupConfig groupConfig;

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
     * Create a PersistentTopologyDataDefinitionIdentityStore from the db context.
     *
     * @return PersistentTopologyDataDefinitionIdentityStore based on the database context.
     */
    @Bean
    public PersistentTopologyDataDefinitionIdentityStore
            persistentTopologyDataDefinitionIdentityStore() {
        try {
            return new PersistentTopologyDataDefinitionIdentityStore(databaseConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create PersistentTopologyDataDefinitionIdentityStore", e);
        }
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
            persistentTopologyDataDefinitionIdentityStore(),
                identityInitializer());
    }

    /**
     * Create a TopologyDataDefinitionStore.
     *
     * @return TopologyDataDefinitionStore needed by TopologyDataDefinitionRpcService.
     */
    @Bean
    public TopologyDataDefinitionStore topologyDataDefinitionStore() {
        try {
            return new TopologyDataDefinitionStore(databaseConfig.dsl(),
                topologyDataDefinitionIdentityStore(), groupConfig.groupStore());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create TopologyDataDefinitionStore", e);
        }
    }
}
