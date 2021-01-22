package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.RiskUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.PendingActionWriter.IActionWriter;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.export.schema.Action;
import com.vmturbo.extractor.export.schema.ActionEntity;
import com.vmturbo.extractor.export.schema.ActionSavings;
import com.vmturbo.extractor.export.schema.CommodityChange;
import com.vmturbo.extractor.export.schema.DeleteInfo;
import com.vmturbo.extractor.export.schema.ExportedObject;
import com.vmturbo.extractor.export.schema.MoveChange;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Extract pending actions in AO and send to Kafka.
 */
class DataExtractionPendingActionWriter implements IActionWriter {

    private static final Logger logger = LogManager.getLogger();

    private Map<Long, Policy> policyById;
    private final ExtractorKafkaSender extractorKafkaSender;
    private final RelatedEntitiesExtractor relatedEntitiesExtractor;
    private final TopologyGraph<SupplyChainEntity> topologyGraph;
    private final MutableLong lastActionExtraction;
    private final List<Action> actions;
    private final String exportTimeFormatted;
    private final long exportTimeInMillis;

    DataExtractionPendingActionWriter(ExtractorKafkaSender extractorKafkaSender,
                               DataExtractionFactory dataExtractionFactory,
                               DataProvider dataProvider,
                               Clock clock,
                               MutableLong lastActionExtraction) {
        this.extractorKafkaSender = extractorKafkaSender;
        this.topologyGraph = dataProvider.getTopologyGraph();
        if (dataProvider.getTopologyGraph() != null && dataProvider.getSupplyChain() != null) {
            this.relatedEntitiesExtractor = dataExtractionFactory.newRelatedEntitiesExtractor(
                    dataProvider.getTopologyGraph(), dataProvider.getSupplyChain(), dataProvider.getGroupData());
        } else {
            this.relatedEntitiesExtractor = null;
        }
        this.lastActionExtraction = lastActionExtraction;
        this.exportTimeInMillis = clock.millis();
        this.exportTimeFormatted = ExportUtils.getFormattedDate(exportTimeInMillis);
        this.actions = new ArrayList<>();
    }

    @Override
    public boolean requirePolicy() {
        return true;
    }

    @Override
    public void acceptPolicy(Map<Long, Policy> policyById) {
        this.policyById = policyById;
    }

    @Override
    public void recordAction(ActionOrchestratorAction aoAction) {
        final ActionSpec actionSpec = aoAction.getActionSpec();
        final ActionDTO.Action recommendation = actionSpec.getRecommendation();
        final Action action = new Action();
        action.setOid(recommendation.getId());
        action.setCreationTime(ExportUtils.getFormattedDate(actionSpec.getRecommendationTime()));
        action.setState(actionSpec.getActionState().name());
        action.setCategory(actionSpec.getCategory().name());
        action.setMode(actionSpec.getActionMode().name());
        action.setDescription(actionSpec.getDescription());
        action.setSeverity(actionSpec.getSeverity().name());

        // set risk description
        try {
            final String riskDescription = RiskUtil.createRiskDescription(actionSpec,
                    policyId -> policyById.get(policyId),
                    entityId -> topologyGraph.getEntity(entityId)
                            .map(SupplyChainEntity::getDisplayName)
                            .orElse(null));
            action.setExplanation(riskDescription);
        } catch (UnsupportedActionException e) {
            logger.error("Cannot calculate risk for unsupported action {}", actionSpec, e);
        }

        // set target and related
        try {
            ActionDTO.ActionEntity primaryEntity = ActionDTOUtil.getPrimaryEntity(recommendation, true);
            action.setTarget(getActionEntityWithType(primaryEntity));
            if (relatedEntitiesExtractor != null) {
                action.setRelated(relatedEntitiesExtractor.extractRelatedEntities(primaryEntity.getId()));
            }
        } catch (UnsupportedActionException e) {
            // this should not happen
            logger.error("Unable to get primary entity for unsupported action {}", actionSpec, e);
        }

        // set action savings if available
        if (recommendation.hasSavingsPerHour()) {
            CurrencyAmount savingsPerHour = recommendation.getSavingsPerHour();
            ActionSavings actionSavings = new ActionSavings();
            actionSavings.setUnit(CostProtoUtil.getCurrencyUnit(savingsPerHour));
            actionSavings.setAmount(savingsPerHour.getAmount());
            action.setSavings(actionSavings);
        }

        final ActionType actionType = ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation());
        action.setType(actionType.name());
        final ActionDTO.ActionInfo actionInfo = recommendation.getInfo();
        switch (actionType) {
            case MOVE:
            case SCALE:
                action.setMoveInfo(getMoveInfo(recommendation));
                break;
            case RESIZE:
                if (actionInfo.hasAtomicResize()) {
                    action.setResizeInfo(getAtomicResizeInfo(actionInfo.getAtomicResize()));
                } else {
                    action.setResizeInfo(getNormalResizeInfo(actionInfo.getResize()));
                }
                break;
            case DELETE:
                action.setDeleteInfo(getDeleteInfo(actionInfo.getDelete(),
                        recommendation.getExplanation().getDelete()));
                break;
            // add additional info for other action types if needed
            default:
                break;
        }

        actions.add(action);
    }

    @Override
    public void write(MultiStageTimer timer) {
        final List<ExportedObject> exportedObjects = actions.stream()
                .map(action -> {
                    ExportedObject exportedObject = new ExportedObject();
                    exportedObject.setTimestamp(exportTimeFormatted);
                    exportedObject.setAction(action);
                    return exportedObject;
                }).collect(Collectors.toList());
        // send to Kafka
        timer.start("Send actions to Kafka");
        extractorKafkaSender.send(exportedObjects);
        timer.stop();
        // update last extraction time
        lastActionExtraction.setValue(exportTimeInMillis);
    }

    /**
     * Get move specific action info.
     *
     * @param recommendation action from AO
     * @return map of move change by entity type
     */
    private Map<String, MoveChange> getMoveInfo(ActionDTO.Action recommendation) {
        final Map<String, MoveChange> moveInfo = new HashMap<>();
        final Map<String, Integer> lastIndex = new HashMap<>();
        for (ChangeProvider change : ActionDTOUtil.getChangeProviderList(recommendation)) {
            final MoveChange moveChange = new MoveChange();
            // type is already specified in json key
            moveChange.setFrom(getActionEntityWithoutType(change.getSource()));
            moveChange.setTo(getActionEntityWithoutType(change.getDestination()));
            // resource (like volume of a VM)
            if (change.getResourceCount() > 0) {
                moveChange.setResource(change.getResourceList().stream()
                                .map(this::getActionEntityWithType).collect(Collectors.toList()));
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
     * @return map of resize change by commodity type
     */
    private Map<String, CommodityChange> getAtomicResizeInfo(AtomicResize atomicResize) {
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
            commodityChange.setTarget(getActionEntityWithType(resize.getTarget()));
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
     * Create a {@link ActionEntity} instance based on given {@link ActionDTO.ActionEntity} with
     * type field set.
     *
     * @param actionEntity {@link ActionDTO.ActionEntity}
     * @return {@link ActionEntity}
     */
    private ActionEntity getActionEntityWithType(ActionDTO.ActionEntity actionEntity) {
        final ActionEntity ae = getActionEntityWithoutType(actionEntity);
        ae.setType(ExportUtils.getEntityTypeJsonKey(actionEntity.getType()));
        return ae;
    }

    /**
     * Create a {@link ActionEntity} instance based on given {@link ActionDTO.ActionEntity} without
     * type field set.
     *
     * @param actionEntity {@link ActionDTO.ActionEntity}
     * @return {@link ActionEntity}
     */
    private ActionEntity getActionEntityWithoutType(ActionDTO.ActionEntity actionEntity) {
        final ActionEntity ae = new ActionEntity();
        ae.setOid(actionEntity.getId());
        topologyGraph.getEntity(actionEntity.getId()).ifPresent(e -> ae.setName(e.getDisplayName()));
        return ae;
    }
}
