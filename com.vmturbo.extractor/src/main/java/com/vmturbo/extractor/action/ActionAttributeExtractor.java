package com.vmturbo.extractor.action;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.schema.json.common.ActionEntity;
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.DeleteInfo;
import com.vmturbo.extractor.schema.json.common.MoveChange;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.reporting.ActionAttributes;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Responsible for extracting action attributes (mostly type-specific info) shared by the
 * reporting and data extraction code (in {@link com.vmturbo.extractor.schema.json.common}.
 */
public class ActionAttributeExtractor {

    /**
     * Extract the {@link ActionAttributes} from an action.
     *
     * @param actionSpec The {@link ActionSpec}.
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information about
     *                      entities involved in the action.
     * @return The {@link ActionAttributes}.
     */
    @Nonnull
    public ActionAttributes extractAttributes(@Nonnull final ActionSpec actionSpec,
            @Nullable final TopologyGraph<SupplyChainEntity> topologyGraph) {
        ActionAttributes actionAttributes = new ActionAttributes();
        populateAttributes(actionSpec, topologyGraph, actionAttributes::setMoveInfo,
                actionAttributes::setResizeInfo, actionAttributes::setDeleteInfo);
        return actionAttributes;
    }

    /**
     * Extract action attributes from an action, and populate them in the {@link Action} input.
     * We pass in the {@link Action} to avoid creating an intermediate object.
     *
     * @param actionSpec The {@link ActionSpec}.
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information about
     *                      entities involved in the action.
     * @param actionToPopulate The {@link Action} to set the properties in. This gets modified by
     *                         this method.
     */
    public void populateActionAttributes(@Nonnull final ActionSpec actionSpec,
            @Nullable final TopologyGraph<SupplyChainEntity> topologyGraph,
            @Nonnull final Action actionToPopulate) {
        populateAttributes(actionSpec, topologyGraph, actionToPopulate::setMoveInfo,
                actionToPopulate::setResizeInfo, actionToPopulate::setDeleteInfo);
    }

    /**
     * Create a {@link ActionEntity} instance based on given {@link ActionDTO.ActionEntity} with
     * type field set.
     *
     * @param actionEntity {@link ActionDTO.ActionEntity}
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @return {@link ActionEntity}
     */
    @Nonnull
    public static ActionEntity getActionEntityWithType(ActionDTO.ActionEntity actionEntity,
                                                TopologyGraph<SupplyChainEntity> topologyGraph) {
        final ActionEntity ae = getActionEntityWithoutType(actionEntity, topologyGraph);
        ae.setType(ExportUtils.getEntityTypeJsonKey(actionEntity.getType()));
        return ae;
    }

    private void populateAttributes(ActionSpec actionSpec,
            TopologyGraph<SupplyChainEntity> topologyGraph,
            Consumer<Map<String, MoveChange>> moveSetter,
            Consumer<Map<String, CommodityChange>> resizeSetter,
            Consumer<DeleteInfo> deleteSetter) {
        final ActionDTO.Action recommendation = actionSpec.getRecommendation();
        final ActionType actionType = ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation());
        final ActionDTO.ActionInfo actionInfo = recommendation.getInfo();
        switch (actionType) {
            case MOVE:
            case SCALE:
                moveSetter.accept(getMoveInfo(recommendation, topologyGraph));
                break;
            case RESIZE:
                if (actionInfo.hasAtomicResize()) {
                    resizeSetter.accept(getAtomicResizeInfo(actionInfo.getAtomicResize(), topologyGraph));
                } else {
                    resizeSetter.accept(getNormalResizeInfo(actionInfo.getResize()));
                }
                break;
            case DELETE:
                deleteSetter.accept(getDeleteInfo(actionInfo.getDelete(),
                        recommendation.getExplanation().getDelete()));
                break;
            // add additional info for other action types if needed
            default:
                break;
        }

    }

    /**
     * Get move specific action info.
     *
     * @param recommendation action from AO
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @return map of move change by entity type
     */
    private Map<String, MoveChange> getMoveInfo(ActionDTO.Action recommendation,
                                                TopologyGraph<SupplyChainEntity> topologyGraph) {
        final Map<String, MoveChange> moveInfo = new HashMap<>();
        final Map<String, Integer> lastIndex = new HashMap<>();
        for (ChangeProvider change : ActionDTOUtil.getChangeProviderList(recommendation)) {
            final MoveChange moveChange = new MoveChange();
            // type is already specified in json key
            moveChange.setFrom(getActionEntityWithoutType(change.getSource(), topologyGraph));
            moveChange.setTo(getActionEntityWithoutType(change.getDestination(), topologyGraph));
            // resource (like volume of a VM)
            if (change.hasResource()) {
                moveChange.setResource(Collections.singletonList(
                        getActionEntityWithType(change.getResource(), topologyGraph)));
            }

            String entityTypeJsonKey = ExportUtils.getEntityTypeJsonKey(change.getSource().getType());
            // if multiple changes for same type of provider, key will be: STORAGE, STORAGE_1, etc.
            // (though I haven't see such a case in real environment or customer diags)
            if (moveInfo.containsKey(entityTypeJsonKey)) {
                Integer newIndex = lastIndex.getOrDefault(entityTypeJsonKey, 0) + 1;
                lastIndex.put(entityTypeJsonKey, newIndex);
                entityTypeJsonKey = entityTypeJsonKey + "_" + newIndex;
            }
            moveInfo.put(entityTypeJsonKey, moveChange);
        }
        return moveInfo;
    }

    /**
     * Add resize specific action info.
     *
     * @param resize action from AO
     * @return map of resize change by commodity type
     */
    private Map<String, CommodityChange> getNormalResizeInfo(Resize resize) {
        final int commodityTypeInt = resize.getCommodityType().getType();
        final CommodityChange commodityChange = new CommodityChange();
        commodityChange.setFrom(resize.getOldCapacity());
        commodityChange.setTo(resize.getNewCapacity());
        if (resize.hasCommodityAttribute()) {
            commodityChange.setAttribute(resize.getCommodityAttribute().name());
        }
        ClassicEnumMapper.getCommodityUnits(commodityTypeInt, null)
                .ifPresent(commodityChange::setUnit);
        return Collections.singletonMap(ExportUtils.getCommodityTypeJsonKey(commodityTypeInt),
                commodityChange);
    }

    /**
     * Add AtomicResize specific action info.
     *
     * @param atomicResize action from AO
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @return map of resize change by commodity type
     */
    private Map<String, CommodityChange> getAtomicResizeInfo(AtomicResize atomicResize,
                                                             TopologyGraph<SupplyChainEntity> topologyGraph) {
        final Map<String, CommodityChange> resizeInfo = new HashMap<>();
        for (ResizeInfo resize : atomicResize.getResizesList()) {
            final CommodityChange commodityChange = new CommodityChange();
            commodityChange.setFrom(resize.getOldCapacity());
            commodityChange.setTo(resize.getNewCapacity());
            if (resize.hasCommodityAttribute()) {
                commodityChange.setAttribute(resize.getCommodityAttribute().name());
            }
            final int commodityTypeInt = resize.getCommodityType().getType();
            ClassicEnumMapper.getCommodityUnits(commodityTypeInt, resize.getTarget().getType())
                    .ifPresent(commodityChange::setUnit);

            // set target (where this commodity comes from) for each sub action, since it may be
            // different from main target
            commodityChange.setTarget(getActionEntityWithType(resize.getTarget(), topologyGraph));
            // assumes same type of commodity is only resized once in atomic action
            resizeInfo.put(ExportUtils.getCommodityTypeJsonKey(commodityTypeInt), commodityChange);
        }
        return resizeInfo;
    }

    /**
     * Get the type specific info for delete action.
     *
     * @param delete delete action
     * @param deleteExplanation delete explanation
     * @return {@link DeleteInfo}
     */
    private DeleteInfo getDeleteInfo(Delete delete, DeleteExplanation deleteExplanation) {
        DeleteInfo deleteInfo = new DeleteInfo();
        if (deleteExplanation.getSizeKb() > 0) {
            deleteInfo.setFileSize(deleteExplanation.getSizeKb() / (double)Units.NUM_OF_KB_IN_MB);
            deleteInfo.setUnit("MB");
        }
        if (delete.hasFilePath()) {
            deleteInfo.setFilePath(delete.getFilePath());
        }
        return deleteInfo;
    }

    /**
     * Create a {@link ActionEntity} instance based on given {@link ActionDTO.ActionEntity} without
     * type field set.
     *
     * @param actionEntity {@link ActionDTO.ActionEntity}
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @return {@link ActionEntity}
     */
    private static ActionEntity getActionEntityWithoutType(ActionDTO.ActionEntity actionEntity,
                                                    @Nullable TopologyGraph<SupplyChainEntity> topologyGraph) {
        final ActionEntity ae = new ActionEntity();
        ae.setOid(actionEntity.getId());
        if (topologyGraph != null) {
            topologyGraph.getEntity(actionEntity.getId())
                .ifPresent(e -> ae.setName(e.getDisplayName()));
        }
        return ae;
    }
}
