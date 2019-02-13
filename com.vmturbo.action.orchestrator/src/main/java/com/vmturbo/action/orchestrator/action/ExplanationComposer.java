package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.components.common.ClassicEnumMapper.COMMODITY_TYPE_MAPPINGS;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A utility with static methods that assist in composing explanations for actions.
 */
public class ExplanationComposer {
    private static final Logger logger = LogManager.getLogger();

    private static final ImmutableBiMap<CommodityDTO.CommodityType, String> COMMODITY_TYPE_TO_STRING_MAPPER =
            ImmutableBiMap.copyOf(COMMODITY_TYPE_MAPPINGS).inverse();

    public static final String MOVE_COMPLIANCE_EXPLANATION_FORMAT =
            "{0} can not satisfy the request for resource(s) ";
    public static final String MOVE_EVACUATION_EXPLANATION_FORMAT =
            "{0} can be suspended to improve efficiency";
    public static final String MOVE_INITIAL_PLACEMENT_EXPLANATION =
        "Place an unplaced entity on a supplier";
    public static final String MOVE_PERFORMANCE_EXPLANATION =
        "Improve overall performance";
    public static final String ACTIVATE_EXPLANATION_WITH_REASON_COMM = "Address high utilization of ";
    public static final String ACTIVATE_EXPLANATION_WITHOUT_REASON_COMM = "Add more resource to satisfy the increased demand";
    public static final String DEACTIVATE_EXPLANATION = "Improve infrastructure efficiency";
    public static final String RECONFIGURE_EXPLANATION =
        "Enable supplier to offer requested resource(s) ";
    public static final String PROVISION_BY_DEMAND_EXPLANATION =
        "No current supplier has enough capacity to satisfy demand for ";
    public static final String ACTION_TYPE_ERROR =
        "Can not give a proper explanation as action type is not defiend";
    public static final String INCREASE_RI_UTILIZATION =
            "Increase RI Utilization.";
    public static final String WASTED_COST = "Wasted Cost";

    /**
     * Private to prevent instantiation.
     */
    private ExplanationComposer() {
    }

    /**
     * Compose explanation for various types of actions. Explanation appears below the action
     * details. In Classic, this is called as risk.
     *
     * @param action the action to mansplain
     * @return the explanation sentence
     */
    public static String composeExplanation(ActionDTO.Action action) {
        Explanation explanation = action.getExplanation();
        switch (explanation.getActionExplanationTypeCase()) {
            case MOVE:
                // if we only have one source entity, we'll use it in the explanation builder. if
                // multiple, we won't bother because we don't have enough info to attribute
                // commodities to specific sources
                List<ActionEntity> source_entities = action.getInfo().getMove().getChangesList().stream()
                        .map(ChangeProvider::getSource)
                        .collect(Collectors.toList());
                Optional<ActionEntity> optionalSourceEntity = source_entities.size() == 1
                        ? Optional.of(source_entities.get(0))
                        : Optional.empty();

                MoveExplanation moveExp = explanation.getMove();
                List<ChangeProviderExplanation> changeExplanations =
                                moveExp.getChangeProviderExplanationList();
                ChangeProviderExplanation firstChangeProviderExplanation =
                                changeExplanations.get(0);
                if (firstChangeProviderExplanation.hasInitialPlacement()) {
                    return buildPerformanceExplanation();
                }
                StringJoiner sj = new StringJoiner(", ", ActionDTOUtil.TRANSLATION_PREFIX, "");
                changeExplanations.stream()
                                .map(provider -> changeExplanationBuilder(optionalSourceEntity, provider))
                                .forEach(sj::add);
                return sj.toString();
            case RESIZE:
                return buildResizeExplanation(action);
            case ACTIVATE:
                if (!explanation.getActivate().hasMostExpensiveCommodity()) {
                    return ACTIVATE_EXPLANATION_WITHOUT_REASON_COMM;
                } else {
                    return buildActivateExplanation(explanation.getActivate()
                                                    .getMostExpensiveCommodity());
                }
            case DEACTIVATE:
                return buildDeactivateExplanation();
            case RECONFIGURE:
                return buildReconfigureExplanation(explanation
                    .getReconfigure().getReconfigureCommodityList());
            case PROVISION:
                ProvisionExplanation provExp = explanation.getProvision();
                switch (provExp.getProvisionExplanationTypeCase()) {
                    case PROVISION_BY_DEMAND_EXPLANATION:
                        return buildProvisionByDemandExplanation(
                            provExp.getProvisionByDemandExplanation()
                                .getCommodityMaxAmountAvailableList());
                    case PROVISION_BY_SUPPLY_EXPLANATION:
                        return buildProvisionBySupplyExplanation(provExp
                            .getProvisionBySupplyExplanation()
                            .getMostExpensiveCommodity());
                    default:
                        return ACTION_TYPE_ERROR;
                }
            default:
                return ACTION_TYPE_ERROR;
        }
    }

    private static String changeExplanationBuilder(Optional<ActionEntity> sourceEntity,
                                                   ChangeProviderExplanation changeExp) {
        switch (changeExp.getChangeProviderExplanationTypeCase()) {
            case COMPLIANCE:
                return buildComplianceExplanation(sourceEntity,
                    changeExp.getCompliance().getMissingCommoditiesList());
            case CONGESTION:
                return buildCongestionExplanation(changeExp.getCongestion());
            case EVACUATION:
                return buildEvacuationExplanation(
                    changeExp.getEvacuation().getSuspendedEntity());
            case PERFORMANCE:
                return buildPerformanceExplanation();
            case EFFICIENCY:
                return buildEfficiencyExplanation(changeExp.getEfficiency());
            default:
                return ACTION_TYPE_ERROR;
        }
    }

    /**
     * Build move explanation for compliance. This should look something like:
     *
     *   "{entity name} can not satisfy the request for resource(s) {comma-separated commodities list}"
     *
     * @param commodityTypes a list of missing commodity types
     * @return explanation
     */
    public static String buildComplianceExplanation(Optional<ActionEntity> optionalSourceEntity,
                                                    List<TopologyDTO.CommodityType> commodityTypes) {
        StringBuilder sb = new StringBuilder();
        sb.append(MessageFormat.format(MOVE_COMPLIANCE_EXPLANATION_FORMAT,
                optionalSourceEntity.isPresent()
                        ? buildEntityNameOrType(optionalSourceEntity.get())
                        : "Current supplier"));
        sb.append(commodityTypes.stream()
            .map(ActionDTOUtil::getCommodityDisplayName)
            .collect(Collectors.joining(" ")));

        return sb.toString();
    }

    /**
     * Build a explanation for congestion. This should end up looking like:
     * For on-prem entities:
     * "{comma-delimited congested commodities list} congestion"
     * For cloud entities, if some commodities resized, then the below explanation will appear:
     * "{comma-delimited congested commodities list} congestion. Underutilized {comma-delimited underutilized commodities list}"
     * For cloud entities, if no commodities resized and if the destination is an RI, then:
     * "Increase RI Utilization"
     *
     * @param congestion The congestion change provider explanation
     * @return explanation
     */
    public static String buildCongestionExplanation(Congestion congestion) {
        List<TopologyDTO.CommodityType> congestedComms =  congestion.getCongestedCommoditiesList();
        List<TopologyDTO.CommodityType> underUtilizedComms =  congestion.getUnderUtilizedCommoditiesList();
        // For the cloud, we should have either congested commodities or increase RI utilization
        // A blank explanation should not occur.
        String congestionExplanation = "";
        if (!congestedComms.isEmpty() || !underUtilizedComms.isEmpty()) {
            congestionExplanation += buildCommodityUtilizationExplanation(congestedComms, underUtilizedComms);
        } else if (congestion.getIsRiCoverageIncreased()) {
            congestionExplanation += INCREASE_RI_UTILIZATION;
        }
        return congestionExplanation;
    }

    /**
     * Build move explanation for evacuation, which should be along the lines of:
     *
     *     "{entity name} can be suspended to improve efficiency"
     *
     * @param entityOID the suspended entity oid
     * @return explanation
     */
    public static String buildEvacuationExplanation(long entityOID) {
        return MessageFormat.format(MOVE_EVACUATION_EXPLANATION_FORMAT,
                ActionDTOUtil.createTranslationBlock(entityOID, "displayName", "Current supplier"));
    }

    /**
     * Build move explanation for initial placement: "Place an unplaced entity on a supplier"
     *
     * @return explanation
     */
    public static String buildInitialPlacementExplanation() {
        return new StringBuilder().append(MOVE_INITIAL_PLACEMENT_EXPLANATION).toString();
    }

    /**
     * Build move explanation for performance, i.e. "Improve overall performance"
     *
     * @return explanation
     */
    public static String buildPerformanceExplanation() {
        return MOVE_PERFORMANCE_EXPLANATION;
    }

    /**
     * Build resize explanation. This has a few forms.
     *
     * For a resize down: "Underutilized {commodity name} in {entity name}"
     *
     * For a resize up: "{commodity name} congestion in {entity name}"
     *
     * @param action the resize action
     * @return explanation
     */
    public static String buildResizeExplanation(ActionDTO.Action action) {
        // verify it's a resize.
        if (! action.getInfo().hasResize()) {
            logger.warn("Can't build resize explanation for non-resize action {}", action.getId());
            return "";
        }
        // now modeling this behavior after ActionGeneratorImpl.notifyRightSize() in classic.
        // NOT addressing special cases for: Ready Queue, Reserved Instance, Cloud Template, Fabric
        // Interconnect, VStorage, VCPU. If we really need those, we can add them in as necessary.
        ResizeExplanation resizeExplanation = action.getExplanation().getResize();
        boolean isResizeDown = resizeExplanation.getStartUtilization() < resizeExplanation.getEndUtilization();

        // since we may show entity name, we are going to build a translatable explanation
        StringBuilder sb = new StringBuilder(ActionDTOUtil.TRANSLATION_PREFIX);
        Resize resize = action.getInfo().getResize();
        String commodityType = ActionDTOUtil.getCommodityDisplayName(resize.getCommodityType());
        // if we have a target, we will try to show it's name in the explanation
        String targetClause = resize.hasTarget()
                ? " in "+ buildEntityTypeAndName(resize.getTarget())
                : "";

        if (isResizeDown) {
            sb.append("Underutilized ").append(commodityType).append(targetClause);
        }
        else {
            sb.append(commodityType).append(" congestion").append(targetClause);
        }
        return sb.toString();
    }

    /**
     * As of now [1/27/2019], the efficiency message is only used for cloud entities.
     *
     * If some commodities resized, then the below explanation will appear:
     * "{comma-delimited congested commodities list} congestion. Underutilized {comma-delimited underutilized commodities list}"
     * If no commodities resized and if the destination is an RI, then:
     * "Increase RI Utilization"
     * If none of the above conditions are true, then:
     * "Wasted Cost"
     *
     * @param efficiency The efficiency change provider explanation
     * @return
     */
    public static String buildEfficiencyExplanation(Efficiency efficiency) {
        List<TopologyDTO.CommodityType> overUtilizedComms =  efficiency.getCongestedCommoditiesList();
        List<TopologyDTO.CommodityType> underUtilizedComms =  efficiency.getUnderUtilizedCommoditiesList();
        boolean isUtilizationDrivenAction = !overUtilizedComms.isEmpty() || !underUtilizedComms.isEmpty();
        String efficiencyExplanation = "";
        if (isUtilizationDrivenAction || efficiency.hasIsRiCoverageIncreased()) {
            if (isUtilizationDrivenAction) {
                efficiencyExplanation += buildCommodityUtilizationExplanation(
                        overUtilizedComms, underUtilizedComms);
            } else if (efficiency.getIsRiCoverageIncreased()) {
                efficiencyExplanation += INCREASE_RI_UTILIZATION;
            }
        } else {
            efficiencyExplanation = WASTED_COST;
        }
        return efficiencyExplanation;
    }

    /**
     * Returnd a string of the form:
     * "{comma-delimited congested commodities list} congestion. Underutilized {comma-delimited underutilized commodities list}"
     *
     * @param congestedComms the congested commodities list
     * @param underUtilizedComms the under-utilized commodities list
     * @return
     */
    public static String buildCommodityUtilizationExplanation(
            @Nonnull List<TopologyDTO.CommodityType> congestedComms,
            @Nonnull List<TopologyDTO.CommodityType> underUtilizedComms) {
        boolean areCongestedCommoditiesPresent = !congestedComms.isEmpty();
        boolean areUnderUtilizedCommoditiesPresent = !underUtilizedComms.isEmpty();
        String commUtilizationExplanation = "";
        if (areCongestedCommoditiesPresent) {
            commUtilizationExplanation = congestedComms.stream().map(
                    c -> ActionDTOUtil.getCommodityDisplayName(c))
                    .collect(Collectors.joining(", ")) + " congestion";
            if (areUnderUtilizedCommoditiesPresent) {
                commUtilizationExplanation += ". ";
            }
        }
        if (areUnderUtilizedCommoditiesPresent) {
            commUtilizationExplanation += "Underutilized " + underUtilizedComms.stream().map(
                    c -> ActionDTOUtil.getCommodityDisplayName(c)).collect(Collectors.joining(", "));
        }
        return commUtilizationExplanation;
    }

    /**
     * Given an {@link ActionEntity}, create a translation fragment that shows the entity type and name.
     *
     * e.g. For a VM named "Bill", create a fragment that would translate to "Virtual Machine Bill".
     *
     * @param entity
     * @return
     */
    private static String buildEntityTypeAndName(ActionEntity entity) {
        return ActionDTOUtil.upperUnderScoreToMixedSpaces(EntityType.forNumber(entity.getType()).name())
                +" "+ ActionDTOUtil.createTranslationBlock(entity.getId(), "displayName", "");
    }

    /**
     * Given an {@link ActionEntity}, create a translation fragment that shows the entity name, if
     * available, otherwise will show the entity type if for some reason the entity cannot be found
     * when the text is translated.
     *
     * For example, for a VM named "Bill", the fragment will render "Bill" if the entity name field
     * is available, otherwise it will render "Virtual Machine".
     *
     * @param entity
     * @return
     */
    private static String buildEntityNameOrType(ActionEntity entity) {
        return ActionDTOUtil.createTranslationBlock(entity.getId(), "displayName",
                ActionDTOUtil.upperUnderScoreToMixedSpaces(EntityType.forNumber(entity.getType()).name()));
    }

    /**
     * Build activate explanation. e.g. "Address high utilization of {commodity type}"
     *
     * @param commodityType the most expensive commodity type
     * @return explanation
     */
    public static String buildActivateExplanation(final int commodityType) {
        return new StringBuilder().append(ACTIVATE_EXPLANATION_WITH_REASON_COMM)
            .append(COMMODITY_TYPE_TO_STRING_MAPPER.get(CommodityType.forNumber(commodityType))).toString();
    }

    /**
     * Build deactivate explanation, e.g. "Improve infrastructure efficiency."
     *
     * @return explanation
     */
    public static String buildDeactivateExplanation() {
        return DEACTIVATE_EXPLANATION;
    }

    /**
     * Build reconfigure explanation, i.e.
     *
     *     "Enable supplier to offer requested resource(s) {comma-delimited commodity names}"
     *
     * @param commodityTypes a list of missing commodity types
     * @return explanation
     */
    public static String buildReconfigureExplanation(
        @Nonnull final Collection<TopologyDTO.CommodityType> commodityTypes) {
        StringBuilder sb = new StringBuilder().append(RECONFIGURE_EXPLANATION);
        sb.append(commodityTypes.stream().map(commodityType ->
                ActionDTOUtil.getCommodityDisplayName(commodityType))
                .collect(Collectors.joining(", ")));
        return sb.toString();
    }

    /**
     * Build provision explanation for addressing high demand. This looks like:
     *
     * "No current supplier has enough capacity to satisfy demand for [{commodity name} whose requested
     * amount is {request amount} but max available is {max available}]" (where the bracketed
     * section is repeated once per commodity in demand)
     *
     * @param entries a list of entries containing commodity base type, the requested amount of
     * it and the max available amount of it.
     * @return explanation
     */
    public static String buildProvisionByDemandExplanation(List<CommodityMaxAmountAvailableEntry> entries) {
        StringBuilder sb = new StringBuilder().append(PROVISION_BY_DEMAND_EXPLANATION);
        entries.forEach(entry -> sb
            .append(COMMODITY_TYPE_TO_STRING_MAPPER.get(CommodityType.forNumber(entry.getCommodityBaseType())))
            .append(" ")
            .append(" whose requested amount is ")
            .append(entry.getRequestedAmount())
            .append(" but max available is ")
            .append(entry.getMaxAmountAvailable()).append(" "));
        return sb.toString();
    }

    /**
     * Build provision explanation for addressing high utilization, e.g. "{commodity type} congestion"
     *
     * @param commodity the most expensive commodity base type
     * @return explanation
     */
    public static String buildProvisionBySupplyExplanation(int commodity) {
        return new StringBuilder()
                .append(COMMODITY_TYPE_TO_STRING_MAPPER.get(CommodityType.forNumber(commodity)))
                .append(" congestion")
                .toString();
    }

}
