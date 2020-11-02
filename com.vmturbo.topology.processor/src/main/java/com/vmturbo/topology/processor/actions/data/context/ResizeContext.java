package com.vmturbo.topology.processor.actions.data.context;

import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.VCpuData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.VMemData;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 *  A class for collecting data needed for Resize action execution
 */
public class ResizeContext extends AbstractActionExecutionContext {

    private static final Logger logger = LogManager.getLogger();

    public ResizeContext(@Nonnull final ExecuteActionRequest request,
                         @Nonnull final ActionDataManager dataManager,
                         @Nonnull final EntityStore entityStore,
                         @Nonnull final EntityRetriever entityRetriever,
                         @Nonnull final TargetStore targetStore,
                         @Nonnull final ProbeStore probeStore) {
        super(request, dataManager, entityStore, entityRetriever, targetStore, probeStore);
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
     * @throws ContextCreationException if exception faced while building action
     */
    @Override
    protected List<ActionItemDTO.Builder> initActionItemBuilders() throws ContextCreationException {
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
        actionItemBuilder.setCurrentComm(commodityForResizeAction(commodityType,
            resizeInfo.getOldCapacity(), resizeInfo));

        // TODO (roman, May 15 2017): Right now we forward the capacity from the resize
        // action directly to the probe. We may need to round things off to certain increments
        // depending on which commodity we're dealing with. This will be partially mitigated
        // by sending correct increments to the market. (OM-16571)
        actionItemBuilder.setNewComm(commodityForResizeAction(commodityType,
            resizeInfo.getNewCapacity(), resizeInfo));
        // Indicate whether this resize is related to a scaling group
        if (resizeInfo.hasScalingGroupId()) {
            actionItemBuilder.setConsistentScalingCompliance(true);
        }

        return builders;
    }

    /**
     * Create the commodity (current / new) for the resize action.
     *
     * Todo: Currently we set the hot add / hot remove flag on the commodity. This is needed for
     * the resize action execution of VMM. we will need to set more info on the commodity,
     * once more probes are supported in XL. For example:
     *     ActiveSession data for action execution of HorizonView probe
     * See MediationDTOUtil::createCommodityDTOFromComm in classic.
     */
    private CommodityDTO.Builder commodityForResizeAction(@Nonnull final CommodityType commodityType,
                                                          float capacity,
                                                          @Nonnull Resize resizeInfo) {
        CommodityDTO.CommodityType sdkCommodityType = CommodityDTO.CommodityType.forNumber(commodityType.getType());
        CommodityDTO.Builder commodity = CommodityDTO.newBuilder()
            .setCommodityType(sdkCommodityType)
            .setKey(commodityType.getKey());

        // Note: the meaning of the capacity value is determined by the commodityAttribute setting
        switch (resizeInfo.getCommodityAttribute()) {
            case CAPACITY:
                commodity.setCapacity(capacity);
                break;
            case LIMIT:
                commodity.setLimit(capacity);
                break;
            case RESERVED:
                commodity.setReservation(capacity);
                break;
            default:
                logger.error("Invalid commodityAttribute: " + resizeInfo.getCommodityAttribute().name());
                break;
        }

        // set VMem/Vcpu data which includes info on hot add/hot remove
        if (sdkCommodityType == CommodityDTO.CommodityType.VMEM) {
            commodity.setVmemData(VMemData.newBuilder()
                .setHotAddSupported(resizeInfo.getHotAddSupported()).build());
        } else if (sdkCommodityType == CommodityDTO.CommodityType.VCPU) {
            commodity.setVcpuData(VCpuData.newBuilder()
                .setHotAddSupported(resizeInfo.getHotAddSupported())
                .setHotRemoveSupported(resizeInfo.getHotRemoveSupported()).build());
        }

        return commodity;
    }
}
