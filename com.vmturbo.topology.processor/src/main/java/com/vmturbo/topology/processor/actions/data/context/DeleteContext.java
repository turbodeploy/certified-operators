package com.vmturbo.topology.processor.actions.data.context;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 * A class for collecting data needed for Delete action execution.
 */
public class DeleteContext extends AbstractActionExecutionContext {

    protected DeleteContext(@Nonnull final ExecuteActionRequest request,
                            @Nonnull final ActionDataManager dataManager,
                            @Nonnull final EntityStore entityStore,
                            @Nonnull final EntityRetriever entityRetriever) {
        super(Objects.requireNonNull(request),
              Objects.requireNonNull(dataManager),
              Objects.requireNonNull(entityStore),
              Objects.requireNonNull(entityRetriever));
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possibility of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    @Override
    protected long getPrimaryEntityId() {
        return getActionInfo().getDelete().getTarget().getId();
    }
}
