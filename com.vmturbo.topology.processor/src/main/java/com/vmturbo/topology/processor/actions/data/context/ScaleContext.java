package com.vmturbo.topology.processor.actions.data.context;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A class for collecting data needed for Scale action execution.
 */
public class ScaleContext extends ChangeProviderContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Construct new instance of {@code ScaleContext}.
     *
     * @param request Action execution request.
     * @param dataManager {@link ActionDataManager} instance.
     * @param entityStore {@link EntityStore} instance.
     * @param entityRetriever {@link EntityRetriever} instance.
     * @param targetStore the target store.
     * @param probeStore the probe store.
     */
    public ScaleContext(@Nonnull final ExecuteActionRequest request,
                        @Nonnull final ActionDataManager dataManager,
                        @Nonnull final EntityStore entityStore,
                        @Nonnull final EntityRetriever entityRetriever,
                        @Nonnull final TargetStore targetStore,
                        @Nonnull final ProbeStore probeStore) {
        super(request, dataManager, entityStore, entityRetriever, targetStore, probeStore);
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

    @Override
    protected List<Builder> initActionItemBuilders() throws ContextCreationException {
        List<ActionItemDTO.Builder> builders = new ArrayList<>();
        logger.debug("Get target entity from repository for action {}", getActionId());
        final EntityDTO fullEntityDTO = getFullEntityDTO(getPrimaryEntityId());
        // ActionItemDTOs translated from ChangeProvider list.
        final List<ChangeProvider> changeProviderList
                = getActionInfo().getScale().getChangesList();
        for (ChangeProvider change: changeProviderList) {
            builders.add(actionItemDtoBuilder(change, getActionId(), fullEntityDTO));
        }
        // ActionItemDTOs translated from ResizeInfo list for commodity resize.
        List<ResizeInfo> resizeInfoList = getActionInfo().getScale().getCommodityResizesList();
        for (ResizeInfo resizeInfo : resizeInfoList) {
            final CommodityType commodityType = resizeInfo.getCommodityType();
            final CommodityDTO.CommodityType sdkCommodityType
                    = CommodityDTO.CommodityType.forNumber(commodityType.getType());
            CommodityDTO.Builder curCommodity = CommodityDTO.newBuilder()
                    .setCommodityType(sdkCommodityType)
                    .setCapacity(resizeInfo.getOldCapacity());
            CommodityDTO.Builder newCommodity = CommodityDTO.newBuilder()
                    .setCommodityType(sdkCommodityType)
                    .setCapacity(resizeInfo.getNewCapacity());
            builders.add(ActionItemDTO.newBuilder()
                    .setActionType(getSDKActionType())
                    .setUuid(Long.toString(getActionId()))
                    .setTargetSE(fullEntityDTO)
                    .setCurrentComm(curCommodity)
                    .setNewComm(newCommodity)
                    .addAllContextData(getContextData()));
            logger.trace("created action item for {}:{}:{}",
                    fullEntityDTO.getEntityType(), fullEntityDTO.getDisplayName(),
                    commodityType);
        }
        return builders;
    }
}
