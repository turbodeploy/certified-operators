package com.vmturbo.topology.processor.template;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Responsible for creating a TopologyEntityDTO from TemplateDTO. And those entities should be unplaced.
 */
public interface TopologyEntityConstructor {
    /**
     * Creating a TopologyEntityDTO from TemplateDTO.
     *
     * @param template {@link Template}
     * @param topologyEntityBuilder builder of TopologyEntityDTO which could contains some setting already.
     * @return {@link TopologyEntityDTO.Builder}
     */
    TopologyEntityDTO.Builder createTopologyEntityFromTemplate(@Nonnull final Template template,
                                                       @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder);
}
