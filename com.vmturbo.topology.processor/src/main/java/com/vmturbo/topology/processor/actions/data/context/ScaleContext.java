package com.vmturbo.topology.processor.actions.data.context;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * A class for collecting data needed for Scale action execution.
 */
public class ScaleContext extends ChangeProviderContext {

    /**
     * Construct new instance of {@code ScaleContext}.
     *
     * @param request Action execution request.
     * @param dataManager {@link ActionDataManager} instance.
     * @param entityStore {@link EntityStore} instance.
     * @param entityRetriever {@link EntityRetriever} instance.
     */
    public ScaleContext(@Nonnull final ExecuteActionRequest request,
                        @Nonnull final ActionDataManager dataManager,
                        @Nonnull final EntityStore entityStore,
                        @Nonnull final EntityRetriever entityRetriever) {
        super(request, dataManager, entityStore, entityRetriever);
    }

    @Override
    protected ActionType getActionItemType(final EntityType srcEntityType) {
        return ActionType.SCALE;
    }

    @Override
    protected boolean isCrossTargetMove() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getPrimaryEntityId() {
        return getActionInfo().getScale().getTarget().getId();
    }
}
