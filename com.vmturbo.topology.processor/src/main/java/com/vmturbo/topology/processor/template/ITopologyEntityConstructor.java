package com.vmturbo.topology.processor.template;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Create topology entity from a template.
 */
public interface ITopologyEntityConstructor {

    /**
     * Create topology entities from a template.
     *
     * @param template template
     * @param topology topology
     * @param originalTopologyEntity original TopologyEntity
     * @param isReplaced is replaced
     * @param identityProvider identity provider
     * @return topology entities
     * @throws TopologyEntityConstructorException error creating topology
     *             entities
     */
    @Nonnull
    TopologyEntityDTO.Builder createTopologyEntityFromTemplate(
            @Nonnull Template template, @Nullable Map<Long, TopologyEntity.Builder> topology,
            @Nullable TopologyEntity.Builder originalTopologyEntity, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException;
}
