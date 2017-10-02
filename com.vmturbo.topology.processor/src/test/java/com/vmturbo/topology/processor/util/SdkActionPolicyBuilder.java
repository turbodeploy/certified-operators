package com.vmturbo.topology.processor.util;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionCapability;
import com.vmturbo.platform.common.dto.ActionExecution.ActionPolicyDTO.ActionPolicyElement;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class for creating sdk specific action policies.
 */
public class SdkActionPolicyBuilder {

    private SdkActionPolicyBuilder() {}

    /**
     * Builds action policy with specified capability and entity entityType.
     *
     * @param capability to set to action policy
     * @param entityType entity entityType to set to action policy
     * @return built action policy
     */
    public static @Nonnull ActionPolicyDTO build(@Nonnull ActionCapability capability,
            @Nonnull EntityType entityType, @Nonnull ActionType actionType) {
        return ActionPolicyDTO.newBuilder()
                .addPolicyElement(ActionPolicyElement.newBuilder()
                        .setActionType(actionType)
                        .setActionCapability(capability))
                .setEntityType(entityType)
                .build();
    }
}
