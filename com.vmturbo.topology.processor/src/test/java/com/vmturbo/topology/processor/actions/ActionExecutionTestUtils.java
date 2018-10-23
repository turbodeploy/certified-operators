package com.vmturbo.topology.processor.actions;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ActionExecutionTestUtils {

    public static ActionEntity createActionEntity(long id) {
        final EntityType defaultEntityType = EntityType.VIRTUAL_MACHINE;
        return createActionEntity(id, defaultEntityType);
    }

    public static ActionEntity createActionEntity(long id, EntityType entityType) {
        return ActionEntity.newBuilder()
                    .setId(id)
                    .setType(entityType.getNumber())
                    .build();
    }
}
