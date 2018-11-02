package com.vmturbo.topology.processor.actions.data.context;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Builder;
import com.vmturbo.topology.processor.actions.ActionExecutionException;
import com.vmturbo.topology.processor.actions.data.ActionDataManager;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 *  A class for collecting data needed for Provision action execution
 */
public class ProvisionContext extends AbstractActionExecutionContext {

    public ProvisionContext(@Nonnull final ExecuteActionRequest request,
                            @Nonnull final ActionDataManager dataManager,
                            @Nonnull final EntityStore entityStore,
                            @Nonnull final EntityRetriever entityRetriever)
            throws ActionExecutionException {
        super(request, dataManager, entityStore, entityRetriever);
    }

    /**
     * Get the SDK (probe-facing) type of the over-arching action being executed
     *
     * @return the SDK (probe-facing) type of the over-arching action being executed
     */
    @Nonnull
    @Override
    public ActionType getSDKActionType() {
        return ActionType.PROVISION;
    }

    /**
     * Get the type of the over-arching action being executed
     *
     * @return the type of the over-arching action being executed
     */
    @Nonnull
    @Override
    public ActionDTO.ActionType getActionType() {
        return ActionDTO.ActionType.PROVISION;
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link com.vmturbo.common.protobuf.ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possiblity of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    @Override
    protected long getPrimaryEntityId() {
        return getActionInfo().getProvision().getEntityToClone().getId();
    }

    /**
     * Create builders for the actionItemDTOs needed to execute this action and populate them with
     * data. A builder is returned so that further modifications can be made by subclasses, before
     * the final build is done.
     * <p>
     * The default implementation creates a single {@link Builder}
     *
     * @return a list of {@link Builder ActionItemDTO builders}
     * @throws ActionExecutionException if the data required for action execution cannot be retrieved
     */
    @Override
    protected List<Builder> initActionItems() throws ActionExecutionException {
        List<Builder> builders = super.initActionItems();
        // TODO: Create an action item for each provider of the entity to clone. For each action item
        // the provider should be the new service entity and current service entity should be empty.
        return  builders;
    }
}
