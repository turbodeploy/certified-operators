package com.vmturbo.topology.processor.actions.data.context;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;

/**
 *  A class for collecting data needed for Resize action execution
 */
public class ResizeContext extends AbstractActionExecutionContext {

    public ResizeContext(@Nonnull final ExecuteActionRequest request,
                         @Nonnull final ActionDataManager dataManager,
                         @Nonnull final EntityStore entityStore,
                         @Nonnull final EntityRetriever entityRetriever) {
        super(request, dataManager, entityStore, entityRetriever);
    }

    /**
     * Get the type of the over-arching action being executed
     *
     * @return the type of the over-arching action being executed
     */
    @Nonnull
    @Override
    public ActionDTO.ActionType getActionType() {
        return ActionDTO.ActionType.RESIZE;
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possiblity of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    @Override
    protected long getPrimaryEntityId() {
        return getActionInfo().getResize().getTarget().getId();
    }

    /**
     * Create builders for the actionItemDTOs needed to execute this action and populate them with
     *   data. A builder is returned so that further modifications can be made by subclasses, before
     *   the final build is done.
     *
     * This implementation additionally sets commodity data needed for resize action execution.
     *
     * @return a list of {@link ActionItemDTO.Builder ActionItemDTO builders}
     */
    @Override
    protected List<ActionItemDTO.Builder> initActionItemBuilders() {
        List<ActionItemDTO.Builder> builders = super.initActionItemBuilders();
        ActionItemDTO.Builder actionItemBuilder = getPrimaryActionItemBuilder(builders);
        Resize resizeInfo = getActionInfo().getResize();

        // Handle resizes on commodity attributes other than just capacity. Note that we
        // are converting TopologyDTO.CommodityAttribute enum to a classic ActionExecution
        // .ActionItemDTO.CommodityAttribute enum here -- we are matching on the ordinal number,
        // which is set up (but not enforced) to match in the two enums.
        actionItemBuilder.setCommodityAttribute(
                CommodityAttribute.forNumber(resizeInfo.getCommodityAttribute().getNumber()));

        final CommodityType commodityType = resizeInfo.getCommodityType();
        actionItemBuilder.setCurrentComm(commodityBuilderFromType(commodityType)
                .setCapacity(resizeInfo.getOldCapacity()));

        // TODO (roman, May 15 2017): Right now we forward the capacity from the resize
        // action directly to the probe. We may need to round things off to certain increments
        // depending on which commodity we're dealing with. This will be partially mitigated
        // by sending correct increments to the market. (OM-16571)
        actionItemBuilder.setNewComm(commodityBuilderFromType(commodityType)
                .setCapacity(resizeInfo.getNewCapacity()));

        return builders;
    }

    private CommodityDTO.Builder commodityBuilderFromType(@Nonnull final CommodityType commodityType) {
        return CommodityDTO.newBuilder()
                .setCommodityType(CommodityDTO.CommodityType.forNumber(commodityType.getType()))
                .setKey(commodityType.getKey());
    }
}
