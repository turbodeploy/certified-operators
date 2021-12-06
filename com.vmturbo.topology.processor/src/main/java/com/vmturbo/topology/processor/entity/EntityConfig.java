package com.vmturbo.topology.processor.entity;

import com.google.common.collect.Lists;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.EntityCustomTagsServiceGrpc.EntityCustomTagsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.EntityInfoREST;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for the entity repository.
 */
@Configuration
@Import({TargetConfig.class, IdentityProviderConfig.class, ClockConfig.class, ControllableConfig.class})
public class EntityConfig {

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private ControllableConfig controllableConfig;

    @Autowired
    private TopologyProcessorNotificationSender sender;

    @Value("${validationOldValuesCacheEnabled:true}")
    private boolean oldValuesCacheEnabled;

    /**
     * Enable entity details support.
     */
    @Value("${entityDetailsEnabled:false}")
    private boolean entityDetailsEnabled;

    /**
     * Ratio of overlap between among key entity type of two targets for us to declare them
     * duplicates. Set to 0 or negative to disable.
     */
    @Value("${targetDeduplicationOverlapRatio:0.3f}")
    private float targetDeduplicationOverlapRatio;

    /**
     * Kubernetes targets all have different probe types for historical reasons. Set this to true
     * in order to treat them all as the same type for the purposes of detecing duplicate targets.
     * In other words, if this is true, all Kubernetes targets are compared to one another when
     * checking for duplicate targets.
     */
    @Value("${targetDeduplicationMergeKubernetesProbeTypes:true}")
    private boolean targetDeduplicationMergeKubernetesProbeTypes;

    @Value("${accountForVendorAutomation:false}")
    private boolean accountForVendorAutomation;

    @Value("${useSerializedEntities:true}")
    private boolean useSerializedEntities;

    @Bean
    public EntityStore entityStore() {
        EntityStore store = new EntityStore(targetConfig.targetStore(),
            identityProviderConfig.identityProvider(),
            targetDeduplicationOverlapRatio,
            targetDeduplicationMergeKubernetesProbeTypes,
            Lists.newArrayList(sender, controllableConfig.entityMaintenanceTimeDao()),
            clockConfig.clock(),
            accountForVendorAutomation, useSerializedEntities);
        store.setEntityDetailsEnabled(entityDetailsEnabled);
        return store;
    }

    @Bean
    public EntityValidator entityValidator() {
        return new EntityValidator(oldValuesCacheEnabled);
    }

    @Bean
    public EntityRpcService entityInfoRpcService() {
        return new EntityRpcService(entityStore(), targetConfig.targetStore());
    }

    @Bean
    public EntityInfoREST.EntityServiceController entityInfoServiceController() {
        return new EntityInfoREST.EntityServiceController(entityInfoRpcService());
    }

    @Bean
    public EntityCustomTagsMerger entityCustomTagsMerger(EntityCustomTagsServiceBlockingStub entityCustomTagsService) {
        return new EntityCustomTagsMerger(entityCustomTagsService);
    }
}
