package com.vmturbo.action.orchestrator.topology;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ActionPartialEntity.ActionEntityTypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.topology.graph.util.BaseGraphEntity;

/**
 * The topology entity representation in the action orchestrator. Contains only the essential
 * information that the action orchestrator needs to know.
 */
public class ActionGraphEntity extends BaseGraphEntity<ActionGraphEntity> {

    private final ActionEntityTypeSpecificInfo actionEntityInfo;

    private ActionGraphEntity(@Nonnull final TopologyEntityDTO src) {
        super(src);

        this.actionEntityInfo = TopologyDTOUtil.makeActionTypeSpecificInfo(src)
                .map(ActionEntityTypeSpecificInfo.Builder::build)
                .orElse(null);
    }

    public ActionEntityTypeSpecificInfo getActionEntityInfo() {
        return actionEntityInfo;
    }

    /**
     * Builder for {@link ActionGraphEntity}.
     */
    public static class Builder extends BaseGraphEntity.Builder<Builder, ActionGraphEntity> {

        /**
         * Create a new entity builder.
         *
         * @param dto The {@link TopologyEntityDTO}.
         */
        public Builder(@Nonnull final TopologyEntityDTO dto) {
            super(dto, new ActionGraphEntity(dto));
        }
    }
}
