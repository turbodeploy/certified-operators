package com.vmturbo.topology.processor.template;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Responsible for creating a TopologyEntityDTO from TemplateDTO. And those entities should be unplaced.
 * And also it will try to keep all commodity constrains from the original topology entity.
 */
public interface TopologyEntityConstructor {
    /**
     * Creating a TopologyEntityDTO from TemplateDTO.
     *
     * @param template {@link Template}
     * @param topologyEntityBuilder builder of TopologyEntityDTO which could contains some setting already.
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @param originalTopologyEntity the original topology entity which this template want to keep its
     *                               commodity constrains. It could be null, if it is new adding template.
     * @return {@link TopologyEntityDTO.Builder}
     */
    TopologyEntityDTO.Builder createTopologyEntityFromTemplate(
        @Nonnull final Template template,
        @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
        @Nonnull final Map<Long, TopologyEntity.Builder> topology,
        @Nullable final TopologyEntityDTO originalTopologyEntity);
}
