package com.vmturbo.action.orchestrator.action;

import java.util.List;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * A utility with static methods that assist in composing explanations for actions.
 */
public class ExplanationComposer {
    public static final String MOVE_COMPLIANCE_EXPLANATION =
        "Current supplier can not satisfy the request for resource(s) ";
    public static final String MOVE_CONGESTION_EXPLANATION =
        "Congestion on resource(s) ";
    public static final String MOVE_EVACUATION_EXPLANATION =
        "Current supplier can be suspended to improve efficiency";
    public static final String MOVE_INITIAL_PLACEMENT_EXPLANATION =
        "Place an unplaced entity on a supplier";
    public static final String MOVE_PERFORMANCE_EXPLANATION =
        "Improve utilization of infrastructure resources to achieve better application performance";
    public static final String RESIZE_EXPLANATION = "Address the issue of ";
    public static final String ACTIVATE_EXPLANATION = "Address high utilization of ";
    public static final String DEACTIVATE_EXPLANATION = "Improve infrastructure efficiency";
    public static final String RECONFIGURE_EXPLANATION =
        "Enable supplier to offer requested resource(s) ";
    public static final String PROVISION_BY_DEMAND_EXPLANATION =
        "No current supplier has enough capacity to satisfy demand for ";
    public static final String PROVISION_BY_SUPPLY_EXPLANATION = "High utilization on resource(s) ";
    public static final String ACTION_TYPE_ERROR =
        "Can not give a proper explanation as action type is not defiend";

    /**
     * Private to prevent instantiation.
     */
    private ExplanationComposer() {
    }

    /**
     * Compose explanation for various types of actions.
     *
     * @param explanation the explanation object generated from market component
     * @return the explanation sentence
     */
    public static String composeExplanation(Explanation explanation) {
        switch (explanation.getActionExplanationTypeCase()) {
            case MOVE:
                MoveExplanation moveExp = explanation.getMove();
                List<ChangeProviderExplanation> changeExplanations =
                                moveExp.getChangeProviderExplanationList();
                ChangeProviderExplanation firstChangeProviderExplanation =
                                changeExplanations.get(0);
                if (firstChangeProviderExplanation.hasInitialPlacement()) {
                    return buildPerformanceExplanation();
                }
                return changeExplanations.stream()
                                .map(ExplanationComposer::changeExplanationBuilder)
                                .collect(Collectors.joining(", "));
            case RESIZE:
                return buildResizeExplanation(
                    explanation.getResize().getStartUtilization(),
                    explanation.getResize().getEndUtilization());
            case ACTIVATE:
                return buildActivateExplanation(
                    explanation.getActivate().getMostExpensiveCommodity());
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

    private static String changeExplanationBuilder(ChangeProviderExplanation changeExp) {
        switch (changeExp.getChangeProviderExplanationTypeCase()) {
            case COMPLIANCE:
                return buildComplianceExplanation(
                    changeExp.getCompliance().getMissingCommoditiesList());
            case CONGESTION:
                return buildCongestionExplanation(
                    changeExp.getCongestion().getCongestedCommoditiesList());
            case EVACUATION:
                return buildEvacuationExplanation(
                    changeExp.getEvacuation().getSuspendedEntity());
            case PERFORMANCE:
                return buildPerformanceExplanation();
            default:
                return ACTION_TYPE_ERROR;
        }
    }

    /**
     * Build move explanation for compliance.
     * @param commodities a list of missing commodity base types
     * @return explanation
     */
    public static String buildComplianceExplanation(List<Integer> commodities) {
        StringBuilder sb = new StringBuilder().append(MOVE_COMPLIANCE_EXPLANATION);
        commodities.forEach(c -> sb.append(CommodityType.forNumber(c)).append(" "));
        return sb.toString();
    }

    /**
     * Build move explanation for congestion.
     * @param commodities a list of congested commodity base types
     * @return explanation
     */
    public static String buildCongestionExplanation(List<Integer> commodities) {
        StringBuilder sb = new StringBuilder().append(MOVE_CONGESTION_EXPLANATION);
        commodities.forEach(c -> sb.append(CommodityType.forNumber(c)));
        return sb.toString();
    }

    /**
     * Build move explanation for evacuation.
     * @param entityOID the suspended entity oid
     * @return explanation
     */
    public static String buildEvacuationExplanation(long entityOID) {
        return new StringBuilder().append(MOVE_EVACUATION_EXPLANATION).toString();
        // TODO: we need to think about how to map suspended entity oid to its
        // display name. Such mapping requires queries to repository and looking
        // up in database. We already make this query in ActionSpecMapper when
        // converting the ActionSpec to ActionApiDTO which is consumed by UI.
        // We should not do the duplicate the work here.
    }

    /**
     * Build move explanation for initial placement.
     * @return explanation
     */
    public static String buildInitialPlacementExplanation() {
        return new StringBuilder().append(MOVE_INITIAL_PLACEMENT_EXPLANATION).toString();
    }

    /**
     * Build move explanation for performance.
     * @return explanation
     */
    public static String buildPerformanceExplanation() {
        return new StringBuilder().append(MOVE_PERFORMANCE_EXPLANATION).toString();
    }

    /**
     * Build resize explanation.
     * @param startUtil the utilization before resize
     * @param endUtil the utilization after resize
     * @return explanation
     */
    public static String buildResizeExplanation(float startUtil, float endUtil) {
        StringBuilder sb = new StringBuilder().append(RESIZE_EXPLANATION);
        if (startUtil >= endUtil) {
            sb.append("overutilization from ").append(startUtil).append(" to ").append(endUtil);
        } else {
            sb.append("underutilization from ").append(startUtil).append(" to ").append(endUtil);
        }
        return sb.toString();
    }

    /**
     * Build activate explanation.
     * @param commodity the most expensive commodity base type
     * @return explanation
     */
    public static String buildActivateExplanation(int commodity) {
        return new StringBuilder().append(ACTIVATE_EXPLANATION)
            .append(CommodityType.forNumber(commodity)).toString();
    }

    /**
     * Build deactivate explanation.
     * @return explanation
     */
    public static String buildDeactivateExplanation() {
        return new StringBuilder().append(DEACTIVATE_EXPLANATION).toString();
    }

    /**
     * Build reconfigure explanation.
     * @param commodities a list of missing commodity base types
     * @return explanation
     */
    public static String buildReconfigureExplanation(List<Integer> commodities) {
        StringBuilder sb = new StringBuilder().append(RECONFIGURE_EXPLANATION);
        commodities.forEach(c -> sb.append(CommodityType.forNumber(c)));
        return sb.toString();
    }

    /**
     * Build provision explanation for addressing high demand.
     * @param entries a list of entries containing commodity base type, the requested amount of
     * it and the max available amount of it.
     * @return explanation
     */
    public static String buildProvisionByDemandExplanation(List<CommodityMaxAmountAvailableEntry> entries) {
        StringBuilder sb = new StringBuilder().append(PROVISION_BY_DEMAND_EXPLANATION);
        entries.forEach(entry -> sb
            .append(CommodityType.forNumber(entry.getCommodityBaseType()))
            .append(" ")
            .append(" whose requested amount is ")
            .append(entry.getRequestedAmount())
            .append(" but max available is ")
            .append(entry.getMaxAmountAvailable()).append(" "));
        return sb.toString();
    }

    /**
     * Build provision explanation for addressing high utilization.
     * @param commodity the most expensive commodity base type
     * @return explanation
     */
    public static String buildProvisionBySupplyExplanation(int commodity) {
        return new StringBuilder().append(PROVISION_BY_SUPPLY_EXPLANATION)
            .append(CommodityType.forNumber(commodity)).toString();
    }
}
