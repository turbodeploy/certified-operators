package com.vmturbo.action.orchestrator.action;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

public class ActionCategoryExtractor {

    private static final Set<Integer> SEGMENTATION_COMMODITY_SET = ImmutableSet.of(
        CommodityType.SEGMENTATION_VALUE, CommodityType.DRS_SEGMENTATION_VALUE);

    /**
     * Categorize action based on its explanation.
     *
     * @param action the actionDTO
     * @return category
     */
    public static ActionCategory assignActionCategory(ActionDTO.Action action) {
        Explanation explanation = action.getExplanation();
        switch (explanation.getActionExplanationTypeCase()) {
            case MOVE:
            case SCALE:
                List<ChangeProviderExplanation> changeExplanations =
                    ActionDTOUtil.getChangeProviderExplanationList(explanation);
                List<ChangeProviderExplanation> primaryExplanations = changeExplanations.stream()
                    .filter(ChangeProviderExplanation::getIsPrimaryChangeProviderExplanation)
                    .collect(Collectors.toList());
                if (!primaryExplanations.isEmpty()) {
                    changeExplanations = primaryExplanations;
                }
                ChangeProviderExplanation firstChangeExplanation = changeExplanations.get(0);
                if (firstChangeExplanation.hasInitialPlacement()
                       || firstChangeExplanation.hasEvacuation()) {
                    return ActionCategory.EFFICIENCY_IMPROVEMENT;
                }

                // TODO (roman, Mar 26 2018): We should pick a category based on some criteria
                // (e.g. the highest severity sub-action) instead of based on order.
                return changeExplanations.stream()
                                .map(ChangeProviderExplanation::getChangeProviderExplanationTypeCase)
                                .map(typeCase -> {
                                    switch (typeCase) {
                                        case COMPLIANCE:
                                            return Optional.of(ActionCategory.COMPLIANCE);
                                        case CONGESTION:
                                            return Optional.of(ActionCategory.PERFORMANCE_ASSURANCE);
                                        case PERFORMANCE:
                                            return Optional.of(ActionCategory.PREVENTION);
                                        case EFFICIENCY:
                                            return Optional.of(ActionCategory.EFFICIENCY_IMPROVEMENT);
                                        default:
                                            return Optional.<ActionCategory>empty();
                                    }
                                })
                                .filter(Optional::isPresent).map(Optional::get)
                                .findFirst().orElse(ActionCategory.UNKNOWN);
            case RESIZE:
                if (action.getInfo().getResize().getOldCapacity() <= action.getInfo().getResize().getNewCapacity()) {
                    // resize up is to assure performance
                    return ActionCategory.PERFORMANCE_ASSURANCE;
                } else {
                    // resize down is to improve efficiency
                    return ActionCategory.EFFICIENCY_IMPROVEMENT;
                }
            case ATOMICRESIZE:
                for (com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo resize
                            : action.getInfo().getAtomicResize().getResizesList()) {
                    if (resize.getOldCapacity() <= resize.getNewCapacity()) {
                        // resize up is to assure performance - performance takes priority
                        return ActionCategory.PERFORMANCE_ASSURANCE;
                    }
                }
                return ActionCategory.EFFICIENCY_IMPROVEMENT;
            case ACTIVATE:
                if (SEGMENTATION_COMMODITY_SET
                                .contains(explanation.getActivate().getMostExpensiveCommodity())) {
                    // if activation is due to segmentation commodity(DRS is a subclass of it)
                    return ActionCategory.COMPLIANCE;
                } else if (action.hasExecutorInfo() && (action.getExecutorInfo().hasUser()
                        || action.getExecutorInfo().hasSchedule())) {
                    return ActionCategory.EFFICIENCY_IMPROVEMENT;
                } else {
                    return ActionCategory.PERFORMANCE_ASSURANCE;
                }
            case DEACTIVATE:
                if (action.hasExecutorInfo() && (action.getExecutorInfo().hasUser()
                        || action.getExecutorInfo().hasSchedule())) {
                    return ActionCategory.SAVING;
                } else {
                    return ActionCategory.EFFICIENCY_IMPROVEMENT;
                }
            case RECONFIGURE:
                if (action.getInfo().getReconfigure().getIsProvider()) {
                    return action.getInfo().getReconfigure().getIsAddition()
                        ? ActionCategory.PERFORMANCE_ASSURANCE
                            : ActionCategory.EFFICIENCY_IMPROVEMENT;
                }
                return ActionCategory.COMPLIANCE;

            case PROVISION:
                ProvisionExplanation provExp = explanation.getProvision();
                switch (provExp.getProvisionExplanationTypeCase()) {
                    case PROVISION_BY_DEMAND_EXPLANATION:
                        return ActionCategory.PERFORMANCE_ASSURANCE;
                    case PROVISION_BY_SUPPLY_EXPLANATION:
                        if (SEGMENTATION_COMMODITY_SET
                                        .contains(provExp.getProvisionBySupplyExplanation()
                                                        .getMostExpensiveCommodityInfo().getCommodityType().getType())) {
                            // if activation is due to segmentation commodity(DRS is a subclass of it)
                            return ActionCategory.COMPLIANCE;
                        } else {
                            return ActionCategory.PERFORMANCE_ASSURANCE;
                        }
                    default:
                        return ActionCategory.UNKNOWN;
                }
            case DELETE:
                return ActionCategory.EFFICIENCY_IMPROVEMENT;
            case BUYRI:
            case ALLOCATE:
                return ActionCategory.EFFICIENCY_IMPROVEMENT;
            default:
                return ActionCategory.UNKNOWN;
        }
    }

}
