package com.vmturbo.topology.processor.template;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

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
     * @param originalTopologyEntity the original topology entity which this template want to keep its
     *                               commodity constrains. It could be null, if it is new adding template.
     * @return {@link TopologyEntityDTO.Builder}
     */
    TopologyEntityDTO.Builder createTopologyEntityFromTemplate(
        @Nonnull final Template template,
        @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
        @Nullable final TopologyEntityDTO originalTopologyEntity);
}
