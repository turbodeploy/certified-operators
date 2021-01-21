package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.ArrayList;
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
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.RiskUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.PendingActionWriter.IActionWriter;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.schema.json.export.Action;
import com.vmturbo.extractor.schema.json.export.ActionSavings;
import com.vmturbo.extractor.schema.json.export.ExportedObject;
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
    private final ActionAttributeExtractor actionAttributeExtractor;

    DataExtractionPendingActionWriter(ExtractorKafkaSender extractorKafkaSender,
            DataExtractionFactory dataExtractionFactory,
            DataProvider dataProvider,
            Clock clock,
            MutableLong lastActionExtraction,
            ActionAttributeExtractor actionAttributeExtractor) {
        this.extractorKafkaSender = extractorKafkaSender;
        this.topologyGraph = dataProvider.getTopologyGraph();
        this.actionAttributeExtractor = actionAttributeExtractor;
        if (dataProvider.getSupplyChain() != null) {
            this.relatedEntitiesExtractor = dataExtractionFactory.newRelatedEntitiesExtractor(
                    dataProvider.getTopologyGraph(), dataProvider.getSupplyChain(), dataProvider.getGroupData());
            logger.error("No supply chain available for pending action extraction."
                    + " Related entities data may be incomplete.");
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
            action.setTarget(ActionAttributeExtractor.getActionEntityWithType(primaryEntity, topologyGraph));
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

        // Add type-specific attributes to the action.
        actionAttributeExtractor.populateActionAttributes(actionSpec, topologyGraph, action);

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
     * Get reason commodities for the given action.
     *
     * @param action action from AO
     * @return list of reason commodities
     */
    private static List<String> getReasonCommodities(ActionDTO.Action action) {
        return ActionDTOUtil.getReasonCommodities(action)
                .map(ReasonCommodity::getCommodityType)
                .map(CommodityType::getType)
                .map(ExportUtils::getCommodityTypeJsonKey)
                .collect(Collectors.toList());
    }
}
