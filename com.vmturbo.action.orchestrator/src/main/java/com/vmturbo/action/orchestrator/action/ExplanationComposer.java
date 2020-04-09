package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyCommodityTypes;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.buildEntityNameOrType;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.buildEntityTypeAndName;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.getCommodityDisplayName;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.BuyRIExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.ChangeProviderExplanationTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Congestion;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Efficiency;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation.Evacuation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ProvisionExplanation.ProvisionByDemandExplanation.CommodityMaxAmountAvailableEntry;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ScaleExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.commons.Pair;

/**
 * A utility with static methods that assist in composing explanations for actions.
 */
public class ExplanationComposer {
    private static final Logger logger = LogManager.getLogger();

    private static final String MOVE_COMPLIANCE_EXPLANATION_FORMAT =
        "{0} can not satisfy the request for resource(s) ";
    private static final String MOVE_EVACUATION_SUSPENSION_EXPLANATION_FORMAT =
        "{0} can be suspended to improve efficiency";
    private static final String MOVE_EVACUATION_AVAILABILITY_EXPLANATION_FORMAT =
        "{0} is not available";
    private static final String MOVE_INITIAL_PLACEMENT_EXPLANATION =
        "Place an unplaced entity on a supplier";
    private static final String MOVE_PERFORMANCE_EXPLANATION =
        "Improve overall performance";
    private static final String IMPROVE_OVERALL_EFFICIENCY =
        "Improve overall efficiency";
    private static final String ACTIVATE_EXPLANATION_WITH_REASON_COMM = "Address high utilization of ";
    private static final String ACTIVATE_EXPLANATION_WITHOUT_REASON_COMM = "Add more resource to satisfy the increased demand";
    private static final String DEACTIVATE_EXPLANATION = "Improve infrastructure efficiency";
    private static final String RECONFIGURE_REASON_COMMODITY_EXPLANATION =
        "Enable supplier to offer requested resource(s) ";
    private static final String REASON_SETTINGS_EXPLANATION =
        "{0} doesn''t comply with {1}";
    private static final String ACTION_TYPE_ERROR =
        "Can not give a proper explanation as action type is not defined";
    private static final String INCREASE_RI_UTILIZATION =
        "Increase RI Utilization.";
    private static final String WASTED_COST = "Wasted Cost";
    private static final String DELETE_WASTED_FILES_EXPLANATION = "Idle or non-productive";
    private static final String DELETE_WASTED_VOLUMES_EXPLANATION = "Increase savings";
    private static final String ALLOCATE_EXPLANATION = "Virtual Machine can be covered by {0} RI";

    /**
     * Private to prevent instantiation.
     */
    private ExplanationComposer() {}

    /**
     * Compose a short explanation for an action. The short explanation does not contain commodity
     * keys or entity names/ids, and does not require translation. The short explanation can be
     * used where we don't want entity-specific information in the explanation (e.g. as a group
     * criteria for action stats), or in other places where full details are not necessary.
     *
     * @param action the action to explain
     * @return the short explanation sentence
     */
    @Nonnull
    @VisibleForTesting
    public static String shortExplanation(@Nonnull ActionDTO.Action action) {
        return internalComposeExplanation(action, true, Collections.emptyMap());
    }

    /**
     * This method should be used only for tests.
     *
     * @param action the action to explain
     * @return the explanation sentence
     */
    @Nonnull
    @VisibleForTesting
    static String composeExplanation(@Nonnull final ActionDTO.Action action) {
        return internalComposeExplanation(action, false, Collections.emptyMap());
    }

    /**
     * Compose a full explanation for an action.
     *
     * @param action the action to explain
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return the explanation sentence
     */
    @Nonnull
    public static String composeExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName) {
        return internalComposeExplanation(action, false, settingPolicyIdToSettingPolicyName);
    }

    /**
     * Compose explanation for various types of actions. Explanation appears below the action
     * description. In Classic, this is called risk.
     *
     * @param action the action to explain
     * @param keepItShort generate short explanation or not
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return the explanation sentence
     */
    @Nonnull
    private static String internalComposeExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName) {
        Explanation explanation = action.getExplanation();
        switch (explanation.getActionExplanationTypeCase()) {
            case MOVE:
            case SCALE:
                return buildMoveExplanation(action, keepItShort, settingPolicyIdToSettingPolicyName);
            case ALLOCATE:
                return buildAllocateExplanation(action);
            case RESIZE:
                return buildResizeExplanation(action, keepItShort);
            case ACTIVATE:
                return buildActivateExplanation(action);
            case DEACTIVATE:
                return buildDeactivateExplanation();
            case RECONFIGURE:
                return buildReconfigureExplanation(action, keepItShort, settingPolicyIdToSettingPolicyName);
            case PROVISION:
                return buildProvisionExplanation(action, keepItShort);
            case DELETE:
                return buildDeleteExplanation(action);
            case BUYRI:
                return buildBuyRIExplanation(action, keepItShort);
            default:
                return ACTION_TYPE_ERROR;
        }
    }

    private static String buildBuyRIExplanation(ActionDTO.Action action, boolean keepItShort) {
        final BuyRIExplanation buyRI = action.getExplanation().getBuyRI();
        if (buyRI.getTotalAverageDemand() <= 0) {
            return "Invalid total demand.";
        }
        StringBuilder sb = new StringBuilder();
        float coverageIncrease = (buyRI.getCoveredAverageDemand() / buyRI.getTotalAverageDemand())
                * 100;
        sb.append("Increase RI Coverage");
        if (!keepItShort) {
            sb.append(" by ")
                .append(Math.round(coverageIncrease))
                .append("%.");
        }
        return sb.toString();
    }

    /**
     * Build move explanation.
     *
     * @param action the action to explain
     * @param keepItShort generate short explanation or not
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return the explanation sentence
     */
    private static String buildMoveExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName) {
        final Explanation explanation = action.getExplanation();
        // if we only have one source entity, we'll use it in the explanation builder. if
        // multiple, we won't bother because we don't have enough info to attribute
        // commodities to specific sources
        List<ActionEntity> source_entities = ActionDTOUtil.getChangeProviderList(action)
            .stream()
            .map(ChangeProvider::getSource)
            .collect(Collectors.toList());
        Optional<ActionEntity> optionalSourceEntity = source_entities.size() == 1
            ? Optional.of(source_entities.get(0))
            : Optional.empty();

        List<ChangeProviderExplanation> changeExplanations = ActionDTOUtil
            .getChangeProviderExplanationList(explanation);
        ChangeProviderExplanation firstChangeProviderExplanation =
            changeExplanations.get(0);
        if (firstChangeProviderExplanation.hasInitialPlacement()) {
            return buildPerformanceExplanation();
        }
        StringJoiner sj = new StringJoiner(", ", keepItShort ? "" : ActionDTOUtil.TRANSLATION_PREFIX, "");
        // Use primary change explanations if available
        List<ChangeProviderExplanation> primaryChangeExplanation = changeExplanations.stream()
            .filter(ChangeProviderExplanation::getIsPrimaryChangeProviderExplanation)
            .collect(Collectors.toList());
        if (!primaryChangeExplanation.isEmpty()) {
            changeExplanations = primaryChangeExplanation;
        }
        changeExplanations.stream()
            .map(provider -> {
                try {
                    return changeExplanationBuilder(optionalSourceEntity,
                        ActionDTOUtil.getPrimaryEntity(action, false), provider, keepItShort,
                        settingPolicyIdToSettingPolicyName);
                } catch (UnsupportedActionException e) {
                    logger.error("Cannot build action explanation", e);
                    return ACTION_TYPE_ERROR;
                }
            })
            .forEach(sj::add);
        getScalingGroupExplanation(explanation).ifPresent(sj::add);
        return sj.toString();
    }

    /**
     * If the explanation contains a scaling group ID, append scaling group information to the
     * action explanation.
     * @param explanation action explanation
     * @return Optional containing the scaling group explanation if the explanation contains
     * scaling group information, else Optional.empty.  If scaling group information is present
     * but there is no mapping available, the scaling group ID itself is returned.
     */
    private static Optional<String> getScalingGroupExplanation(
        final Explanation explanation) {
        String scalingGroupName = null;
        if (explanation.hasMove()) {
            MoveExplanation moveExplanation = explanation.getMove();
            if (moveExplanation.hasScalingGroupId()) {
                scalingGroupName = moveExplanation.getScalingGroupId();
            }
        } else if (explanation.hasScale()) {
            ScaleExplanation scaleExplanation = explanation.getScale();
            if (scaleExplanation.hasScalingGroupId()) {
                scalingGroupName = scaleExplanation.getScalingGroupId();
            }
        } else if (explanation.hasResize()) {
            ResizeExplanation resizeExplanation = explanation.getResize();
            if (resizeExplanation.hasScalingGroupId()) {
                scalingGroupName = resizeExplanation.getScalingGroupId();
            }
        } else if (explanation.hasReconfigure()) {
            ReconfigureExplanation reconfigureExplanation = explanation.getReconfigure();
            if (reconfigureExplanation.hasScalingGroupId()) {
                scalingGroupName = reconfigureExplanation.getScalingGroupId();
            }
        }
        if (scalingGroupName != null) {
            return Optional.of(" (Scaling Groups: " + scalingGroupName + ")");
        } else {
            return Optional.empty();
        }
    }

    /**
     * Build move change provider explanation.
     *
     * @param optionalSourceEntity the source entity
     * @param target the target entity
     * @param changeProviderExplanation the reason that we change provider
     * @param keepItShort generate short explanation or not
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return the explanation sentence
     */
    private static String changeExplanationBuilder(
            Optional<ActionEntity> optionalSourceEntity,
            @Nonnull final ActionEntity target,
            ChangeProviderExplanation changeProviderExplanation,
            final boolean keepItShort,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName) {
        switch (changeProviderExplanation.getChangeProviderExplanationTypeCase()) {
            case COMPLIANCE:
                if (!changeProviderExplanation.getCompliance().getReasonSettingsList().isEmpty()) {
                    return buildReasonSettingsExplanation(target, changeProviderExplanation.getCompliance()
                        .getReasonSettingsList(), keepItShort, settingPolicyIdToSettingPolicyName);
                } else {
                    return buildComplianceReasonCommodityExplanation(optionalSourceEntity,
                        changeProviderExplanation.getCompliance().getMissingCommoditiesList(), keepItShort);
                }
            case CONGESTION:
                return buildCongestionExplanation(changeProviderExplanation.getCongestion(), keepItShort);
            case EVACUATION:
                return buildEvacuationExplanation(changeProviderExplanation.getEvacuation(), keepItShort);
            case PERFORMANCE:
                return buildPerformanceExplanation();
            case EFFICIENCY:
                return buildEfficiencyExplanation(changeProviderExplanation.getEfficiency(), keepItShort);
            default:
                return ACTION_TYPE_ERROR;
        }
    }

    /**
     * Build move explanation for compliance.
     *
     * e.g. full explanation (no prefix):
     *      "{entity:1:displayName:Physical Machine} can not satisfy the request for resource(s) Mem, CPU"
     * e.g. short explanation:
     *      "Current supplier can not satisfy the request for resource(s) Mem, CPU"
     *
     * @param optionalSourceEntity the source entity
     * @param reasonCommodities a list of commodities that causes this action
     * @param keepItShort generate short explanation or not
     * @return the explanation sentence
     */
    private static String buildComplianceReasonCommodityExplanation(
            @Nonnull final Optional<ActionEntity> optionalSourceEntity,
            @Nonnull final List<ReasonCommodity> reasonCommodities, final boolean keepItShort) {
        StringBuilder sb = new StringBuilder();
        sb.append(MessageFormat.format(MOVE_COMPLIANCE_EXPLANATION_FORMAT,
                optionalSourceEntity.isPresent() && !keepItShort
                        ? buildEntityNameOrType(optionalSourceEntity.get())
                        : "Current supplier"));
        sb.append(reasonCommodities.stream()
            .map(ReasonCommodity::getCommodityType)
            .map(commType -> commodityDisplayName(commType, keepItShort))
            .collect(Collectors.joining(", ")));
        return sb.toString();
    }

    @Nonnull
    private static String commodityDisplayName(@Nonnull final CommodityType commType, final boolean keepItShort) {
        if (keepItShort) {
            return UICommodityType.fromType(commType).displayName();
        } else {
            return getCommodityDisplayName(commType);
        }
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
     * @param keepItShort Defines whether to generate short explanation or not.
     * @return explanation
     */
    private static String buildCongestionExplanation(Congestion congestion,
            final boolean keepItShort) {
        List<ReasonCommodity> congestedComms =  congestion.getCongestedCommoditiesList();
        // For the cloud, we should have either congested commodities or increase RI utilization
        // A blank explanation should not occur.
        String congestionExplanation = "";
        if (!congestedComms.isEmpty()) {
            congestionExplanation += buildCommodityUtilizationExplanation(congestedComms, ChangeProviderExplanationTypeCase.CONGESTION, keepItShort);
        }
        return congestionExplanation;
    }

    /**
     * Build move explanation for evacuation, which should be along the lines of:
     *
     *     "{entity name} can be suspended to improve efficiency"
     *     or
     *     "{entity name} is not available"
     *
     * @param evacuation The evacuation change provider explanation
     * @param keepItShort Defines whether to generate short explanation or not.
     * @return explanation
     */
    private static String buildEvacuationExplanation(Evacuation evacuation, boolean keepItShort) {
        return MessageFormat.format(
            evacuation.getIsAvailable() ? MOVE_EVACUATION_SUSPENSION_EXPLANATION_FORMAT :
                MOVE_EVACUATION_AVAILABILITY_EXPLANATION_FORMAT,
            keepItShort ? "Current supplier" :
                ActionDTOUtil.createTranslationBlock(evacuation.getSuspendedEntity(), "displayName", "Current supplier"));
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
    private static String buildPerformanceExplanation() {
        return MOVE_PERFORMANCE_EXPLANATION;
    }

    /**
     * Build Allocate action explanation.
     *
     * @param action The action to explain.
     * @return The explanation sentence.
     */
    private static String buildAllocateExplanation(@Nonnull final ActionDTO.Action action) {
        final String instanceSizeFamily = action.getExplanation().getAllocate()
                .getInstanceSizeFamily();
        return MessageFormat.format(ALLOCATE_EXPLANATION, instanceSizeFamily);
    }

    /**
     * Build resize explanation. This has a few forms.
     *
     * For a resize down: "Underutilized {commodity name} in {entity name}"
     *
     * For a resize up: "{commodity name} congestion in {entity name}"
     *
     * @param action the resize action
     * @param keepItShort Defines whether to generate short explanation or not.
     * @return explanation
     */
    private static String buildResizeExplanation(ActionDTO.Action action, final boolean keepItShort) {
        // verify it's a resize.
        if (! action.getInfo().hasResize()) {
            logger.warn("Can't build resize explanation for non-resize action {}", action.getId());
            return "";
        }
        // now modeling this behavior after ActionGeneratorImpl.notifyRightSize() in classic.
        // NOT addressing special cases for: Ready Queue, Reserved Instance, Cloud Template, Fabric
        // Interconnect, VStorage, VCPU. If we really need those, we can add them in as necessary.
        ResizeExplanation resizeExplanation = action.getExplanation().getResize();
        boolean isResizeDown = action.getInfo().getResize().getOldCapacity() > action.getInfo().getResize().getNewCapacity();

        // since we may show entity name, we are going to build a translatable explanation
        StringBuilder sb = new StringBuilder(keepItShort ? "" : ActionDTOUtil.TRANSLATION_PREFIX);
        Resize resize = action.getInfo().getResize();
        String commodityType = commodityDisplayName(resize.getCommodityType(), keepItShort);
        // if we have a target, we will try to show it's name in the explanation
        String targetClause = resize.hasTarget() && !keepItShort
                ? " in "+ buildEntityTypeAndName(resize.getTarget())
                : "";

        commodityType = commodityType +
            (resize.getCommodityAttribute() == CommodityAttribute.RESERVED ? " reservation" : "");
        if (isResizeDown) {
            sb.append("Underutilized ").append(commodityType).append(targetClause);
        }
        else {
            sb.append(commodityType).append(" congestion").append(targetClause);
        }
        getScalingGroupExplanation(action.getExplanation()).ifPresent(sb::append);
        return sb.toString();
    }

    /**
     * Build a delete explanation.
     *
     * @param action the delete action
     * @return String giving the explanation for the action
     */
    private static String buildDeleteExplanation(ActionDTO.Action action) {
        if (action.getInfo().getDelete().getTarget().getEnvironmentType() == EnvironmentType.CLOUD) {
            return DELETE_WASTED_VOLUMES_EXPLANATION;
        } else {
            return DELETE_WASTED_FILES_EXPLANATION;
        }
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
    public static String buildEfficiencyExplanation(Efficiency efficiency, final boolean keepItShort) {
        String efficiencyExplanation = "";
        if (efficiency.getIsRiCoverageIncreased()) {
            efficiencyExplanation = INCREASE_RI_UTILIZATION;
        } else if (!efficiency.getUnderUtilizedCommoditiesList().isEmpty()) {
            efficiencyExplanation = buildCommodityUtilizationExplanation(
                efficiency.getUnderUtilizedCommoditiesList(), ChangeProviderExplanationTypeCase.EFFICIENCY, keepItShort);
        } else if (efficiency.getIsWastedCost()) {
            efficiencyExplanation = WASTED_COST;
        } else {

            efficiencyExplanation = IMPROVE_OVERALL_EFFICIENCY;
        }
        return efficiencyExplanation;
    }

    /**
     * Returns a string of the form:
     * "{comma-delimited congested commodities list} congestion"
     *  OR
     *  "Underutilized {comma-delimited underutilized commodities list}"
     *
     * @param reasonCommodities the reason commodities list
     * @param explanationType the explanation type - congestion / efficiency
     * @param keepItShort should the explanation be kept short
     * @return the explanation
     */
    private static String buildCommodityUtilizationExplanation(
        @Nonnull List<ReasonCommodity> reasonCommodities,
        @Nonnull ChangeProviderExplanationTypeCase explanationType,
        final boolean keepItShort) {
        final String commaSeparatedReasonCommodities = reasonCommodities.stream()
                            .map(c -> buildExplanationWithTimeSlots(c, keepItShort))
                            .collect(Collectors.joining(", "));
        if (explanationType == ChangeProviderExplanationTypeCase.EFFICIENCY) {
            return "Underutilized " + commaSeparatedReasonCommodities;
        } else if (explanationType == ChangeProviderExplanationTypeCase.CONGESTION) {
            return commaSeparatedReasonCommodities + " congestion";
        } else {
            return ACTION_TYPE_ERROR;
        }
    }

    /**
     * Build activate explanation. e.g. "Address high utilization of {commodity type}"
     *
     * @param action the action to explain
     * @return the explanation sentence
     */
    private static String buildActivateExplanation(@Nonnull final ActionDTO.Action action) {
        final Explanation explanation = action.getExplanation();
        if (!explanation.getActivate().hasMostExpensiveCommodity()) {
            return ACTIVATE_EXPLANATION_WITHOUT_REASON_COMM;
        } else {
            return ACTIVATE_EXPLANATION_WITH_REASON_COMM +
                UICommodityType.fromType(explanation.getActivate().getMostExpensiveCommodity()).apiStr();
        }
    }

    /**
     * Build deactivate explanation, e.g. "Improve infrastructure efficiency."
     *
     * @return explanation
     */
    private static String buildDeactivateExplanation() {
        return DEACTIVATE_EXPLANATION;
    }

    /**
     * Build reconfigure explanation.
     *
     * @param action the action to explain
     * @param keepItShort generate short explanation or not
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return the explanation sentence
     */
    private static String buildReconfigureExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName) {
        final Explanation explanation = action.getExplanation();
        final ReconfigureExplanation reconfigExplanation = explanation.getReconfigure();
        final StringBuilder sb = new StringBuilder();
        if (!reconfigExplanation.getReasonSettingsList().isEmpty()) {
            sb.append((keepItShort ? "" : ActionDTOUtil.TRANSLATION_PREFIX))
                .append(buildReasonSettingsExplanation(action.getInfo().getReconfigure().getTarget(),
                    reconfigExplanation.getReasonSettingsList(), keepItShort,
                    settingPolicyIdToSettingPolicyName));
        } else {
            sb.append(buildReconfigureReasonCommodityExplanation(
                    reconfigExplanation.getReconfigureCommodityList(), keepItShort));
        }
        getScalingGroupExplanation(action.getExplanation()).ifPresent(sb::append);
        return sb.toString();
    }

    /**
     * Build reconfigure explanation due to reason commodities.
     *
     * e.g. full explanation:
     *      "Enable supplier to offer requested resource(s) Ballooning, Network Commodity test_network"
     * e.g. short explanation:
     *      "Enable supplier to offer requested resource(s) Ballooning, Network Commodity"
     *
     * @param commodityTypes a list of missing reason commodities
     * @param keepItShort generate short explanation or not
     * @return the explanation sentence
     */
    private static String buildReconfigureReasonCommodityExplanation(
        @Nonnull final Collection<ReasonCommodity> commodityTypes, final boolean keepItShort) {
        return RECONFIGURE_REASON_COMMODITY_EXPLANATION +
            commodityTypes.stream().map(reason ->
                commodityDisplayName(reason.getCommodityType(), keepItShort))
                .collect(Collectors.joining(", "));
    }

    /**
     * Build explanation due to reason settings.
     *
     * e.g. full explanation (no prefix):
     *      "{entity:1:displayName:Virtual Machine} doesn't comply to settingName"
     * e.g. short explanation:
     *      "Current entity doesn't comply to setting"
     *
     * @param target the target entity
     * @param reasonSettings a list of settingPolicyIds that causes this action
     * @param keepItShort generate short explanation or not
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return the explanation sentence
     */
    private static String buildReasonSettingsExplanation(
            @Nonnull final ActionEntity target,
            @Nonnull final List<Long> reasonSettings,
            final boolean keepItShort,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName) {
        return MessageFormat.format(REASON_SETTINGS_EXPLANATION,
            keepItShort ? "Current entity" : buildEntityNameOrType(target),
            keepItShort ? "setting" : reasonSettings.stream()
                .map(settingPolicyIdToSettingPolicyName::get).collect(Collectors.joining(", ")));
    }

    /**
     * Build provision explanation.
     *
     * @param action the action to explain
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildProvisionExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort) {
        final Explanation explanation = action.getExplanation();
        ProvisionExplanation provExp = explanation.getProvision();
        switch (provExp.getProvisionExplanationTypeCase()) {
            case PROVISION_BY_DEMAND_EXPLANATION:
                return buildProvisionByDemandExplanation(
                    provExp.getProvisionByDemandExplanation()
                        .getCommodityMaxAmountAvailableList(),
                    action.getInfo().getProvision().getEntityToClone(),
                    keepItShort);
            case PROVISION_BY_SUPPLY_EXPLANATION:
                return buildProvisionBySupplyExplanation(provExp
                    .getProvisionBySupplyExplanation()
                    .getMostExpensiveCommodityInfo().getCommodityType());
            default:
                return ACTION_TYPE_ERROR;
        }
    }

    /**
     * Build provision explanation for addressing high demand.
     * e.g. Cpu, Mem congestion in 'pm_test'
     *
     * @param entries a list of entries containing commodity base type, the requested amount of
     * it and the max available amount of it.
     * @param entity entity to clone
     * @param keepItShort compose a short explanation if true
     * @return explanation
     */
    private static String buildProvisionByDemandExplanation(
                @Nonnull final List<CommodityMaxAmountAvailableEntry> entries,
                @Nonnull final ActionEntity entity, final boolean keepItShort) {
        return (keepItShort ? "" : ActionDTOUtil.TRANSLATION_PREFIX) +
            beautifyCommodityTypes(entries.stream().map(CommodityMaxAmountAvailableEntry::getCommodityBaseType)
                .map(baseType -> CommodityType.newBuilder().setType(baseType).build())
                .collect(Collectors.toList())) +
            " congestion" +
            (keepItShort ? "" : " in '" + buildEntityNameOrType(entity) + "'");
    }

    /**
     * Build provision explanation for addressing high utilization.
     * e.g. Storage Latency congestion
     *
     * @param commodity the most expensive commodity
     * @return explanation
     */
    private static String buildProvisionBySupplyExplanation(@Nonnull final CommodityType commodity) {
        return getCommodityDisplayName(commodity) + " congestion";
    }

    /**
     * Build congestion explanation with time slots, if needed.
     *
     * @param reasonCommodity Commodity causing the move
     * @param keepItShort Short of long explanation
     * @return Explanation string
     */
    @Nonnull
    private static String buildExplanationWithTimeSlots(@Nonnull final ReasonCommodity reasonCommodity,
                                                        final boolean keepItShort) {
        String commodityDisplayName = commodityDisplayName(reasonCommodity.getCommodityType(),
            keepItShort);
        if (!reasonCommodity.hasTimeSlot()) {
            return commodityDisplayName;
        }
        final Pair<Long, Long> timeSlotEndPoints = getTimeSlotEndPoints(reasonCommodity);
        if (timeSlotEndPoints == null) {
            return commodityDisplayName;
        }
        final SimpleDateFormat formatter = new SimpleDateFormat("hh:mm a");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        final Date today = new Date();
        today.setTime(timeSlotEndPoints.first);
        final String startStr = formatter.format(today);
        today.setTime(timeSlotEndPoints.second);
        final String endStr = formatter.format(today);
        commodityDisplayName += " at " + startStr + " - " + endStr;
        return commodityDisplayName;
    }

    /**
     * Convert slot number to times within the day.
     *
     * @param reasonCommodity Commodity with time slot
     * @return Array with start/end time corresponding to the slot number, or null if we failed
     * to convert it.
     */
    @Nullable
    private static Pair<Long, Long> getTimeSlotEndPoints(@Nonnull final ReasonCommodity reasonCommodity) {
        final int totalSlotNumber = reasonCommodity.getTimeSlot().getTotalSlotNumber();
        if (totalSlotNumber == 0) {
            logger.error("Total number of time slots is 0 in ReasonCommodity {}",
                () -> reasonCommodity);
            return null;
        }
        final int slot = reasonCommodity.getTimeSlot().getSlot();
        if (slot < 0 || slot >= totalSlotNumber) {
            logger.error("Time slot {} is not a positive number less than configured number of " +
                    "time slots {} in ReasonCommodity {}", () -> slot, () -> totalSlotNumber,
                () -> reasonCommodity);
            return null;
        }
        final Duration eachSlotDuration = Duration.ofHours(24 / totalSlotNumber);
        final long start = (slot * eachSlotDuration.toMillis());
        final long end = start + eachSlotDuration.toMillis();
        return new Pair<>(start, end);
    }
}
