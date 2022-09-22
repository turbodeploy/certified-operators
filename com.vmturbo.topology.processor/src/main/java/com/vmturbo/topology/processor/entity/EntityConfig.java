package com.vmturbo.topology.processor.entity;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.EntityCustomTagsServiceGrpc.EntityCustomTagsServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.EntityInfoREST;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    @Value("${useSerializedEntities:false}")
    private boolean useSerializedEntities;

    // 43,55,56,57,58,59
    @Value("${reducedEntityTypes:}")
    private String reducedEntityTypes;

    @Bean
    public EntityStore entityStore() {

        final Set<EntityType> reducedEntityTypeSet = Stream.of(reducedEntityTypes.split(","))
                // "".split(",") will return [""]
                .filter(StringUtils::isNotBlank)
                .map(Integer::valueOf)
                .map(EntityType::forNumber)
                .filter(Objects::nonNull)
                .collect(ImmutableSet.toImmutableSet());

        EntityStore store = new EntityStore(targetConfig.targetStore(),
            identityProviderConfig.identityProvider(),
            targetDeduplicationOverlapRatio,
            targetDeduplicationMergeKubernetesProbeTypes,
            Lists.newArrayList(sender, controllableConfig.entityMaintenanceTimeDao()),
            clockConfig.clock(),
            reducedEntityTypeSet,
            useSerializedEntities);
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
