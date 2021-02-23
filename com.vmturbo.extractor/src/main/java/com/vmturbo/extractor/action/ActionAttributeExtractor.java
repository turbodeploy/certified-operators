package com.vmturbo.extractor.action;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
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
import com.vmturbo.extractor.action.percentile.ActionPercentileData;
import com.vmturbo.extractor.action.percentile.ActionPercentileDataRetriever;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.schema.json.common.ActionAttributes;
import com.vmturbo.extractor.schema.json.common.ActionEntity;
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.DeleteInfo;
import com.vmturbo.extractor.schema.json.common.MoveChange;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Responsible for extracting action attributes (mostly type-specific info) shared by the
 * reporting and data extraction code (in {@link com.vmturbo.extractor.schema.json.common}.
 */
public class ActionAttributeExtractor {

    private final ActionPercentileDataRetriever actionPercentileDataRetriever;

    ActionAttributeExtractor(@Nonnull final ActionPercentileDataRetriever actionPercentileDataRetriever) {
        this.actionPercentileDataRetriever = actionPercentileDataRetriever;
    }

    /**
     * Extract {@link ActionAttributes} from a collection of {@link ActionSpec}s.
     *
     * @param actionSpecs The {@link ActionSpec}s, arranged by id.
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information about
     *                      entities involved in the action.
     * @return The {@link ActionAttributes}, arranged by id (as in the actionSpecs input).
     *         Any actions that fail to be mapped will be absent from this map.
     */
    @Nonnull
    public Long2ObjectMap<ActionAttributes> extractAttributes(
            @Nonnull final List<ActionSpec> actionSpecs,
            @Nullable final TopologyGraph<SupplyChainEntity> topologyGraph) {
        final Long2ObjectMap<ActionAttributes> attrs = new Long2ObjectOpenHashMap<>(actionSpecs.size());
        actionSpecs.forEach(e -> attrs.put(e.getRecommendation().getId(), new ActionAttributes()));
        populateAttributes(actionSpecs, topologyGraph, attrs);
        return attrs;
    }

    /**
     * Populate the {@link ActionAttributes} in an action.
     *
     * @param actionSpecs The {@link ActionSpec}s, arranged by id.
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information about
     *                      entities involved in the action.
     * @param attributes The attributes to populate.
     * @param <T> The subtype of {@link ActionAttributes} returned by this method.
     */
    public <T extends ActionAttributes> void populateAttributes(
            @Nonnull final List<ActionSpec> actionSpecs,
            @Nullable final TopologyGraph<SupplyChainEntity> topologyGraph,
            @Nonnull final Long2ObjectMap<T> attributes) {
        final ActionPercentileData percentileChanges =
                actionPercentileDataRetriever.getActionPercentiles(actionSpecs);
        actionSpecs.forEach(actionSpec -> {
            final T actionAttrs = attributes.get(actionSpec.getRecommendation().getId());
            if (actionAttrs != null) {
                populateAttributes(actionSpec, actionAttrs, topologyGraph, percentileChanges);
            }
        });
    }

    private <T extends ActionAttributes> void populateAttributes(ActionSpec actionSpec,
            T attributes,
            TopologyGraph<SupplyChainEntity> topologyGraph, ActionPercentileData actionPercentileData) {
        final ActionDTO.Action recommendation = actionSpec.getRecommendation();
        final ActionType actionType = ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation());
        final ActionDTO.ActionInfo actionInfo = recommendation.getInfo();
        switch (actionType) {
            case MOVE:
            case SCALE:
                attributes.setMoveInfo(getMoveInfo(recommendation, topologyGraph));
                break;
            case RESIZE:
                if (actionInfo.hasAtomicResize()) {
                    attributes.setResizeInfo(getAtomicResizeInfo(actionInfo.getAtomicResize(), topologyGraph,
                            actionPercentileData));
                } else {
                    attributes.setResizeInfo(getNormalResizeInfo(actionInfo.getResize(),
                            actionPercentileData));
                }
                break;
            case DELETE:
                attributes.setDeleteInfo(getDeleteInfo(actionInfo.getDelete(),
                        recommendation.getExplanation().getDelete()));
                break;
            // add additional info for other action types if needed
            default:
                break;
        }
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
            if (!change.getResourceList().isEmpty()) {
                moveChange.setResource(change.getResourceList().stream()
                        .map(rsrc -> getActionEntityWithType(rsrc, topologyGraph))
                        .collect(Collectors.toList()));
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
     * @param actionPercentileData Used to look up percentile changes for the resized commodities.
     * @return map of resize change by commodity type
     */
    @Nonnull
    private Map<String, CommodityChange> getNormalResizeInfo(Resize resize,
            @Nonnull final ActionPercentileData actionPercentileData) {
        final int commodityTypeInt = resize.getCommodityType().getType();
        final CommodityChange commodityChange = new CommodityChange();
        commodityChange.setFrom(resize.getOldCapacity());
        commodityChange.setTo(resize.getNewCapacity());
        if (resize.hasCommodityAttribute()) {
            commodityChange.setAttribute(resize.getCommodityAttribute().name());
        }
        commodityChange.setPercentileChange(
                actionPercentileData.getChange(resize.getTarget().getId(), resize.getCommodityType()));
        CommodityTypeMapping.getCommodityUnits(commodityTypeInt, null)
                .ifPresent(commodityChange::setUnit);
        return Collections.singletonMap(ExportUtils.getCommodityTypeJsonKey(commodityTypeInt),
                commodityChange);
    }

    /**
     * Add AtomicResize specific action info.
     *
     * @param atomicResize action from AO
     * @param topologyGraph The {@link TopologyGraph} to use to obtain entity information from.
     * @param actionPercentileData Used to look up percentile changes for the resized commodities.
     * @return map of resize change by commodity type
     */
    @Nonnull
    private Map<String, CommodityChange> getAtomicResizeInfo(AtomicResize atomicResize,
            TopologyGraph<SupplyChainEntity> topologyGraph,
            ActionPercentileData actionPercentileData) {
        final Map<String, CommodityChange> resizeInfo = new HashMap<>();
        for (ResizeInfo resize : atomicResize.getResizesList()) {
            final CommodityChange commodityChange = new CommodityChange();
            commodityChange.setFrom(resize.getOldCapacity());
            commodityChange.setTo(resize.getNewCapacity());
            if (resize.hasCommodityAttribute()) {
                commodityChange.setAttribute(resize.getCommodityAttribute().name());
            }
            final int commodityTypeInt = resize.getCommodityType().getType();
            CommodityTypeMapping.getCommodityUnits(commodityTypeInt, resize.getTarget().getType())
                    .ifPresent(commodityChange::setUnit);

            // set target (where this commodity comes from) for each sub action, since it may be
            // different from main target
            commodityChange.setTarget(getActionEntityWithType(resize.getTarget(), topologyGraph));
            commodityChange.setPercentileChange(
                    actionPercentileData.getChange(resize.getTarget().getId(), resize.getCommodityType()));
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
