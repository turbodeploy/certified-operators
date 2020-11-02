package com.vmturbo.topology.processor.actions.data.context;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.topology.ActionExecution;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.CommodityAttribute;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Build Merge action execution context.
 */
public class AtomicResizeContext extends AbstractActionExecutionContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Constructor for MergedResizeContext.
     * @param request   Action execution request
     * @param dataManager action data manager
     * @param entityStore entity store
     * @param entityRetriever entity retriever
     * @param targetStore the target store.
     * @param probeStore the probe store.
     */
    public AtomicResizeContext(@Nonnull final ActionExecution.ExecuteActionRequest request,
                               @Nonnull final ActionDataManager dataManager,
                               @Nonnull final EntityStore entityStore,
                               @Nonnull final EntityRetriever entityRetriever,
                               @Nonnull final TargetStore targetStore,
                               @Nonnull final ProbeStore probeStore) {
        super(request, dataManager, entityStore, entityRetriever, targetStore, probeStore);
    }

    @Override
    protected long getPrimaryEntityId() {
        return getActionInfo().getAtomicResize().getExecutionTarget().getId();
    }

    @Override
    protected List<ActionItemDTO.Builder> initActionItemBuilders() throws ContextCreationException {
        // Primary entity is the workload controller entity where the merged actions will be executed
        final EntityDTO fullEntityDTO = getFullEntityDTO(getPrimaryEntityId());

        List<ActionItemDTO.Builder> builders = new ArrayList<>();

        // Build ActionItemDTO for each resize info in the merged action
        AtomicResize atomicResizeInfo = getActionInfo().getAtomicResize();
        for (ResizeInfo resizeInfo : atomicResizeInfo.getResizesList()) {
            long originalActionEntityId = resizeInfo.getTarget().getId();
            EntityDTO originalEntityDTO = getFullEntityDTO(originalActionEntityId);
            ActionItemDTO.Builder actionItemBuilder = buildActionItem(fullEntityDTO, originalEntityDTO);

            actionItemBuilder.setCommodityAttribute(
                    CommodityAttribute.forNumber(resizeInfo.getCommodityAttribute().getNumber()));

            final CommodityType commodityType = resizeInfo.getCommodityType();
            actionItemBuilder.setCurrentComm(
                    commodityForResizeAction(commodityType,
                                                resizeInfo.getOldCapacity(),
                                                resizeInfo));

            actionItemBuilder.setNewComm(
                    commodityForResizeAction(commodityType,
                                                resizeInfo.getNewCapacity(),
                                                resizeInfo));

            builders.add(actionItemBuilder);
            logger.trace("created action item for {}:{}:{}",
                        originalEntityDTO.getEntityType(), originalEntityDTO.getDisplayName(),
                        commodityType);
        }

        return builders;
    }

    private ActionItemDTO.Builder buildActionItem(final EntityDTO targetEntityDTO,
              final EntityDTO originalEntityDTO) throws ContextCreationException {
        final ActionItemDTO.Builder actionItemBuilder = ActionItemDTO.newBuilder();
        actionItemBuilder.setActionType(getSDKActionType());
        actionItemBuilder.setUuid(Long.toString(getActionId()));
        actionItemBuilder.setTargetSE(targetEntityDTO);
        actionItemBuilder.setCurrentSE(originalEntityDTO);

        // Add additional data for action execution
        actionItemBuilder.addAllContextData(getContextData());

        //TODO: is this needed?
        //getHost(originalEntityDTO).ifPresent(actionItemBuilder::setHostedBySE);

        return actionItemBuilder;
    }

    private CommodityDTO.Builder commodityForResizeAction(@Nonnull final CommodityType commodityType,
                                                          float capacity,
                                                          @Nonnull ResizeInfo resizeInfo) {
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

        return commodity;
    }
}
