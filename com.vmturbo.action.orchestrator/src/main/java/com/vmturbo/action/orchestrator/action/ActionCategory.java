package com.vmturbo.action.orchestrator.action;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.Compliance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.Evacuation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.InitialPlacement;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation.Performance;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionBySupplyExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ActivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeactivateExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;

public class ActionCategory {

    public static final String CATEGORY_ERROR = "Can not decide the category";

    public static final Set<Integer> SEGMENTATION_COMMODITY_SET =
                    new HashSet<Integer>(Arrays.asList(CommodityType.SEGMENTATION_VALUE,
                                    CommodityType.DRS_SEGMENTATION_VALUE));

    // TODO: Both action orchestrator and UI have to use the four category strings below
    public static final String CATEGORY_PERFORMANCE_ASSURANCE = "Performance Assurance";
    public static final String CATEGORY_EFFICIENCY_IMPROVEMENT = "Efficiency Improvement";
    public static final String CATEGORY_PREVENTION = "Prevention";
    public static final String CATEGORY_COMPLIANCE = "Compliance";

    /**
     * Categorize action based on its explanation.
     *
     * @param explanation the explanation of the action
     * @return category
     */
    public static String assignActionCategory(Explanation explanation) {
        switch (explanation.getActionExplanationTypeCase()) {
            case MOVE:
                MoveExplanation moveExp = explanation.getMove();
                switch (moveExp.getMoveExplanationTypeCase()) {
                    case COMPLIANCE:
                        return CATEGORY_COMPLIANCE;
                    case CONGESTION:
                        return CATEGORY_PERFORMANCE_ASSURANCE;
                    case EVACUATION:
                    case INITIALPLACEMENT:
                        return CATEGORY_EFFICIENCY_IMPROVEMENT;
                    case PERFORMANCE:
                        return CATEGORY_PREVENTION;
                    default:
                        return CATEGORY_ERROR;
                }
            case RESIZE:
                if (explanation.getResize().getStartUtilization() >= explanation.getResize()
                                .getEndUtilization()) {
                    // resize up is to assure performance
                    return CATEGORY_PERFORMANCE_ASSURANCE;
                } else {
                    // resize down is to improve efficiency
                    return CATEGORY_EFFICIENCY_IMPROVEMENT;
                }
            case ACTIVATE:
                if (SEGMENTATION_COMMODITY_SET
                                .contains(explanation.getActivate().getMostExpensiveCommodity())) {
                    // if activation is due to segmentation commodity(DRS is a subclass of it)
                    return CATEGORY_COMPLIANCE;
                } else {
                    return CATEGORY_PERFORMANCE_ASSURANCE;
                }
            case DEACTIVATE:
                return CATEGORY_EFFICIENCY_IMPROVEMENT;
            case RECONFIGURE:
                return CATEGORY_COMPLIANCE;
            case PROVISION:
                ProvisionExplanation provExp = explanation.getProvision();
                switch (provExp.getProvisionExplanationTypeCase()) {
                    case PROVISION_BY_DEMAND_EXPLANATION:
                        return CATEGORY_PERFORMANCE_ASSURANCE;
                    case PROVISION_BY_SUPPLY_EXPLANATION:
                        if (SEGMENTATION_COMMODITY_SET
                                        .contains(provExp.getProvisionBySupplyExplanation()
                                                        .getMostExpensiveCommodity())) {
                            // if activation is due to segmentation commodity(DRS is a subclass of it)
                            return CATEGORY_COMPLIANCE;
                        } else {
                            return CATEGORY_PERFORMANCE_ASSURANCE;
                        }
                    default:
                        return CATEGORY_ERROR;
                }
            default:
                return CATEGORY_ERROR;
        }
    }

}
