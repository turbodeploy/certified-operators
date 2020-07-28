package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyCommodityType;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.buildEntityNameOrType;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.buildEntityTypeAndName;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.getCommodityDisplayName;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
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
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.commons.Pair;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

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
    private static final String MOVE_PERFORMANCE_EXPLANATION =
        "Improve overall performance";
    private static final String IMPROVE_OVERALL_EFFICIENCY =
        "Improve overall efficiency";
    private static final String ACTIVATE_EXPLANATION_WITH_REASON_COMM =
        "Address high utilization of ";
    private static final String ACTIVATE_EXPLANATION_WITHOUT_REASON_COMM =
        "Add more resource to satisfy the increased demand";
    private static final String DEACTIVATE_EXPLANATION = "Improve infrastructure efficiency";
    private static final String RECONFIGURE_REASON_COMMODITY_EXPLANATION =
        "Enable supplier to offer requested resource(s) ";
    private static final String REASON_SETTINGS_EXPLANATION =
        "{0} doesn''t comply with {1}";
    private static final String ACTION_TYPE_ERROR =
        "Can not give a proper explanation as action type is not defined";
    private static final String INCREASE_RI_UTILIZATION =
        "Increase RI Utilization";
    private static final String WASTED_COST = "Wasted Cost";
    private static final String DELETE_WASTED_FILES_EXPLANATION = "Idle or non-productive";
    private static final String DELETE_WASTED_VOLUMES_EXPLANATION = "Increase savings";
    private static final String ALLOCATE_EXPLANATION = "Virtual Machine can be covered by {0} RI";
    private static final String UNDERUTILIZED_EXPLANATION = "Underutilized ";
    private static final String CONGESTION_EXPLANATION = " Congestion";
    private static final String BUY_RI_EXPLANATION = "Increase RI Coverage";
    private static final String INVALID_BUY_RI_EXPLANATION = "Invalid total demand";

    // Short explanations
    private static final String OVERUTILIZED_RESOURCES_CATEGORY = "Overutilized resources";
    private static final String UNDERUTILIZED_RESOURCES_CATEGORY = "Underutilized resources";
    private static final String RECONFIGURE_EXPLANATION_CATEGORY = "Misconfiguration";
    private static final String REASON_SETTING_EXPLANATION_CATEGORY = "Setting policy compliance";
    private static final String REASON_COMMODITY_EXPLANATION_CATEGORY =  " compliance";
    private static final String SEGMENTATION_COMMODITY_EXPLANATION_CATEGORY =  "Placement policy compliance";
    private static final String ALLOCATE_CATEGORY = "Virtual Machine RI Coverage";
    private static final String ACTION_ERROR_CATEGORY = "";

    /**
     * Private to prevent instantiation.
     */
    private ExplanationComposer() {}

    /**
     * Compose risks (short explanations) for an action. The short explanation does not contain commodity
     * keys or entity names/ids, and does not require translation. The short explanation can be
     * used where we don't want entity-specific information in the explanation (e.g. as a group
     * criteria for action stats), or in other places where full details are not necessary.
     *
     * @param action the action to explain
     * @return a set of short explanation sentences
     */
    @Nonnull
    @VisibleForTesting
    public static Set<String> composeRelatedRisks(@Nonnull ActionDTO.Action action) {
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
        return internalComposeExplanation(action, false, Collections.emptyMap())
            .iterator().next();
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
        return internalComposeExplanation(action, false, settingPolicyIdToSettingPolicyName)
            .iterator().next();
    }

    /**
     * Compose explanations for various types of actions. Explanation appears below the action
     * description. In Classic, this is called risk.
     * When keepItShort is true, it may return multiple sentences.
     * When keepItShort is false, it only returns one sentence.
     *
     * @param action the action to explain
     * @param keepItShort generate short explanation or not
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return a set of explanation sentences
     */
    @Nonnull
    private static Set<String> internalComposeExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName) {
        final Explanation explanation = action.getExplanation();
        switch (explanation.getActionExplanationTypeCase()) {
            case MOVE:
            case SCALE:
                return buildMoveExplanation(action, settingPolicyIdToSettingPolicyName, keepItShort);
            case ALLOCATE:
                return Collections.singleton(buildAllocateExplanation(action, keepItShort));
            case ATOMICRESIZE:
                //invoked when the action spec is created from the action view
                return buildAtomicResizeExplanation(action, keepItShort);
            case RESIZE:
                return Collections.singleton(buildResizeExplanation(action, keepItShort));
            case ACTIVATE:
                return Collections.singleton(buildActivateExplanation(action, keepItShort));
            case DEACTIVATE:
                return Collections.singleton(buildDeactivateExplanation());
            case RECONFIGURE:
                return Collections.singleton(buildReconfigureExplanation(
                    action, settingPolicyIdToSettingPolicyName, keepItShort));
            case PROVISION:
                return buildProvisionExplanation(action, keepItShort);
            case DELETE:
                return Collections.singleton(buildDeleteExplanation(action));
            case BUYRI:
                return Collections.singleton(buildBuyRIExplanation(action, keepItShort));
            default:
                return Collections.singleton(keepItShort ? ACTION_ERROR_CATEGORY : ACTION_TYPE_ERROR);
        }
    }

    /**
     * Build move explanation.
     *
     * @param action the action to explain
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param keepItShort compose a short explanation if true
     * @return a set of explanation sentences
     */
    private static Set<String> buildMoveExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            final boolean keepItShort) {
        final Set<String> moveExplanations = buildMoveCoreExplanation(
            action, settingPolicyIdToSettingPolicyName, keepItShort);
        if (keepItShort) {
            return moveExplanations;
        }
        return Collections.singleton(ActionDTOUtil.TRANSLATION_PREFIX +
            String.join(", ", moveExplanations) +
            getScalingGroupExplanation(action.getExplanation()).orElse(""));
    }

    /**
     * Build move core explanation.
     *
     * @param action the action to explain
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param keepItShort generate short explanation or not
     * @return a set of explanation sentences
     */
    private static Set<String> buildMoveCoreExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            final boolean keepItShort) {
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
            return Collections.singleton(buildPerformanceExplanation());
        }

        // Use primary change explanations if available
        List<ChangeProviderExplanation> primaryChangeExplanation = changeExplanations.stream()
            .filter(ChangeProviderExplanation::getIsPrimaryChangeProviderExplanation)
            .collect(Collectors.toList());
        if (!primaryChangeExplanation.isEmpty()) {
            changeExplanations = primaryChangeExplanation;
        }

        return changeExplanations.stream().flatMap(changeExplanation -> {
            try {
                return buildMoveChangeExplanation(optionalSourceEntity,
                    ActionDTOUtil.getPrimaryEntity(action, false), changeExplanation,
                    settingPolicyIdToSettingPolicyName, keepItShort).stream();
            } catch (UnsupportedActionException e) {
                logger.error("Cannot build action explanation", e);
                return Stream.of(keepItShort ? ACTION_ERROR_CATEGORY : ACTION_TYPE_ERROR);
            }
        }).collect(Collectors.toSet());
    }

    /**
     * If the explanation contains a scaling group ID, append scaling group information to the
     * action explanation.
     * @param explanation action explanation
     * @return Optional containing the scaling group explanation if the explanation contains
     * scaling group information, else Optional.empty.  If scaling group information is present
     * but there is no mapping available, the scaling group ID itself is returned.
     */
    private static Optional<String> getScalingGroupExplanation(final Explanation explanation) {
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
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param keepItShort generate short explanation or not
     * @return a set of explanation sentences
     */
    private static Set<String> buildMoveChangeExplanation(
            @Nonnull final Optional<ActionEntity> optionalSourceEntity, @Nonnull final ActionEntity target,
            @Nonnull final ChangeProviderExplanation changeProviderExplanation,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName, final boolean keepItShort) {
        switch (changeProviderExplanation.getChangeProviderExplanationTypeCase()) {
            case COMPLIANCE:
                if (!changeProviderExplanation.getCompliance().getReasonSettingsList().isEmpty()) {
                    return Collections.singleton(buildReasonSettingsExplanation(target,
                        changeProviderExplanation.getCompliance().getReasonSettingsList(),
                        settingPolicyIdToSettingPolicyName, keepItShort));
                } else {
                    return buildComplianceReasonCommodityExplanation(optionalSourceEntity,
                        changeProviderExplanation.getCompliance().getMissingCommoditiesList(), keepItShort);
                }
            case CONGESTION:
                return buildCongestionExplanation(changeProviderExplanation.getCongestion(), keepItShort);
            case EVACUATION:
                return Collections.singleton(buildEvacuationExplanation(
                    changeProviderExplanation.getEvacuation(), keepItShort));
            case PERFORMANCE:
                return Collections.singleton(buildPerformanceExplanation());
            case EFFICIENCY:
                return buildEfficiencyExplanation(changeProviderExplanation.getEfficiency(), keepItShort);
            default:
                return Collections.singleton(keepItShort ? ACTION_ERROR_CATEGORY : ACTION_TYPE_ERROR);
        }
    }

    /**
     * Build reason commodity explanation.
     * e.g. full explanation:
     *      "{entity:1:displayName:Physical Machine} can not satisfy the request for resource(s) Mem, CPU"
     * e.g. short explanation:
     *      Set("Cluster compliance", "Network compliance", "Placement policy compliance")
     *
     * @param optionalSourceEntity the source entity
     * @param reasonCommodities a list of commodities that causes this action
     * @param keepItShort generate short explanation or not
     * @return the explanation sentence
     */
    private static Set<String> buildComplianceReasonCommodityExplanation(
            @Nonnull final Optional<ActionEntity> optionalSourceEntity,
            @Nonnull final List<ReasonCommodity> reasonCommodities, final boolean keepItShort) {
        if (keepItShort) {
            return reasonCommodities.stream().map(reasonCommodity -> {
                if (reasonCommodity.getCommodityType().getType() ==
                        CommodityDTO.CommodityType.SEGMENTATION_VALUE) {
                    return SEGMENTATION_COMMODITY_EXPLANATION_CATEGORY;
                } else {
                    return commodityDisplayName(reasonCommodity.getCommodityType(), true) +
                        REASON_COMMODITY_EXPLANATION_CATEGORY;
                }
            }).collect(Collectors.toSet());
        }

        return Collections.singleton(MessageFormat.format(MOVE_COMPLIANCE_EXPLANATION_FORMAT,
            optionalSourceEntity.map(ActionDTOUtil::buildEntityNameOrType).orElse("Current supplier")) +
            reasonCommodities.stream().map(ReasonCommodity::getCommodityType)
                .map(commType -> commodityDisplayName(commType, false))
                .collect(Collectors.joining(", ")));
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
     * e.g. full explanation:
     *      "Underutilized CPU, Mem" or "CPU, Mem Congestion"
     * e.g. short explanation:
     *      Set("CPU Congestion", "Mem Congestion")
     *
     * @param congestion The congestion change provider explanation
     * @param keepItShort Defines whether to generate short explanation or not.
     * @return explanation
     */
    private static Set<String> buildCongestionExplanation(
            @Nonnull final Congestion congestion, final boolean keepItShort) {
        final List<ReasonCommodity> congestedCommodities = congestion.getCongestedCommoditiesList();
        if (!congestedCommodities.isEmpty()) {
            return buildCommodityUtilizationExplanation(congestedCommodities,
                ChangeProviderExplanationTypeCase.CONGESTION, keepItShort);
        }
        return Collections.emptySet();
    }

    /**
     * Build move explanation for evacuation, which should be along the lines of:
     * e.g. full explanation:
     *      "{entity name} can be suspended to improve efficiency" or
     *      "{entity name} is not available"
     * e.g. short explanation:
     *      "Underutilized resources"
     *
     * @param evacuation The evacuation change provider explanation
     * @param keepItShort Defines whether to generate short explanation or not.
     * @return explanation
     */
    private static String buildEvacuationExplanation(Evacuation evacuation, boolean keepItShort) {
        if (keepItShort) {
            return UNDERUTILIZED_RESOURCES_CATEGORY;
        }
        return MessageFormat.format(
            evacuation.getIsAvailable() ? MOVE_EVACUATION_SUSPENSION_EXPLANATION_FORMAT :
                MOVE_EVACUATION_AVAILABILITY_EXPLANATION_FORMAT,
            ActionDTOUtil.createTranslationBlock(evacuation.getSuspendedEntity(), "displayName", "Current supplier"));
    }

    /**
     * Build move explanation for performance.
     * e.g. "Improve overall performance"
     *
     * @return explanation
     */
    private static String buildPerformanceExplanation() {
        return MOVE_PERFORMANCE_EXPLANATION;
    }

    /**
     * Build move explanation for efficiency.
     * As of now [1/27/2019], the efficiency message is only used for cloud entities.
     *
     * e.g. full explanation:
     *      "Underutilized CPU, Mem" or
     *      "Increase RI Utilization" or
     *      "Wasted Cost" or
     *      "Improve overall efficiency"
     * e.g. short explanation:
     *      Set("Underutilized CPU", "Underutilized Mem") or
     *      Set("Increase RI Utilization") or
     *      Set("Wasted Cost") or
     *      Set("Improve overall efficiency")
     *
     * @param efficiency the efficiency change provider explanation
     * @param keepItShort compose a short explanation if true
     * @return a set of explanation sentences
     */
    private static Set<String> buildEfficiencyExplanation(
            @Nonnull final Efficiency efficiency, final boolean keepItShort) {
        if (efficiency.getIsRiCoverageIncreased()) {
            return Collections.singleton(INCREASE_RI_UTILIZATION);
        } else if (!efficiency.getUnderUtilizedCommoditiesList().isEmpty()) {
            return buildCommodityUtilizationExplanation(efficiency.getUnderUtilizedCommoditiesList(),
                ChangeProviderExplanationTypeCase.EFFICIENCY, keepItShort);
        } else if (efficiency.getIsWastedCost()) {
            return Collections.singleton(WASTED_COST);
        } else {
            return Collections.singleton(IMPROVE_OVERALL_EFFICIENCY);
        }
    }

    /**
     * Build commodity utilization explanation.
     * e.g. full explanation:
     *      "Underutilized CPU, Mem" or "CPU, Mem Congestion"
     * e.g. short explanation:
     *      Set("Underutilized CPU", "Underutilized Mem") or
     *      Set("CPU Congestion", "Mem Congestion")
     *
     * @param reasonCommodities the reason commodities list
     * @param explanationType the explanation type - congestion / efficiency
     * @param keepItShort compose a short explanation if true
     * @return the explanation
     */
    private static Set<String> buildCommodityUtilizationExplanation(
            @Nonnull final List<ReasonCommodity> reasonCommodities,
            @Nonnull final ChangeProviderExplanationTypeCase explanationType,
            final boolean keepItShort) {
        final Stream<String> reasonCommodityStream = reasonCommodities.stream()
            .map(commodityType -> buildExplanationWithTimeSlots(commodityType, keepItShort));
        final Set<String> commodities;
        if (keepItShort) {
            commodities = reasonCommodityStream.collect(Collectors.toSet());
        } else {
            commodities = Collections.singleton(
                reasonCommodityStream.collect(Collectors.joining(", ")));
        }

        if (explanationType == ChangeProviderExplanationTypeCase.EFFICIENCY) {
            return commodities.stream().map(commodity -> UNDERUTILIZED_EXPLANATION + commodity)
                .collect(Collectors.toSet());
        } else if (explanationType == ChangeProviderExplanationTypeCase.CONGESTION) {
            return commodities.stream().map(commodity -> commodity + CONGESTION_EXPLANATION)
                .collect(Collectors.toSet());
        } else {
            return Collections.singleton(keepItShort ? ACTION_ERROR_CATEGORY : ACTION_TYPE_ERROR);
        }
    }

    /**
     * Build allocate action explanation.
     * e.g. full explanation:
     *      "Virtual Machine can be covered by m4 RI"
     * e.g. short explanation:
     *      "Virtual Machine RI Coverage"
     *
     * @param action the action to explain
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildAllocateExplanation(@Nonnull final ActionDTO.Action action,
                                                   final boolean keepItShort) {
        if (keepItShort) {
            return ALLOCATE_CATEGORY;
        }
        final String instanceSizeFamily = action.getExplanation().getAllocate()
                .getInstanceSizeFamily();
        return MessageFormat.format(ALLOCATE_EXPLANATION, instanceSizeFamily);
    }

    /**
     * Build atomic resize action explanation.
     * e.g. full explanation:
     *   "Merged on Workload Controller twitter-cass-tweet -
     *     VCPU Congestion in Container Spec istio-proxy
     *     [Resize DOWN VCPU from 5,328 to 328],
     *     VMem Congestion in Container Spec twitter-cass-tweet
     *     [Resize DOWN VMem from 512.0 MB to 128.0 MB],
     *     VCPU Congestion in Container Spec twitter-cass-tweet
     *     [Resize UP VCPU from 666 to 966],
     *     VMem Congestion in Container Spec istio-proxy
     *     [Resize DOWN VMem from 1.0 GB to 128.0 MB]"
     *

     * e.g. short explanation:
     *      "Merged on Workload Controller twitter-cass-tweet"
     *
     * @param action the action to explain
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static Set<String> buildAtomicResizeExplanation(
            @Nonnull final ActionDTO.Action action,
            final boolean keepItShort) {

        if (!action.getInfo().hasAtomicResize()) {
            logger.warn("Cannot build atomic resize explanation for non-resize action {}", action.getId());
            return Collections.emptySet();
        }

        AtomicResize atomicResize = action.getInfo().getAtomicResize();

        final String coreExplanation = buildAtomicResizeCoreExplanation(action);
        if (keepItShort) {
            return Collections.singleton(coreExplanation);
        }

        final Set<String> resizeInfoExplanations = new HashSet<>();
        for (ResizeInfo resize : atomicResize.getResizesList()) {

            String resizeExplanation;

            final String commodityType = commodityDisplayName(resize.getCommodityType(), keepItShort)
                    + (resize.getCommodityAttribute() == CommodityAttribute.RESERVED ? " reservation" : "");

            final boolean isResizeDown = action.getInfo().getResize().getOldCapacity()
                                            > action.getInfo().getResize().getNewCapacity();
            if (isResizeDown) {
                resizeExplanation = UNDERUTILIZED_EXPLANATION + commodityType;
            } else {
                resizeExplanation = commodityType + CONGESTION_EXPLANATION;
            }

            StringBuilder sb = new StringBuilder();
            String targetClause = resize.hasTarget()
                    ? " in " + buildEntityTypeAndName(resize.getTarget())
                    : "";
            sb.append(resizeExplanation).append(targetClause);

            CommodityDTO.CommodityType commodity = CommodityDTO.CommodityType
                    .forNumber(resize.getCommodityType().getType());

            String format_capacity =  "{0} [Resize {1} {2} from {3} to {4}]";
            String format_reservation = "{0} [Resize {1} {2} reservation from {3} to {4}]";

            String explanation = MessageFormat.format(
                    resize.getCommodityAttribute() == CommodityAttribute.CAPACITY ? format_capacity : format_reservation,
                    sb.toString(),
                    resize.getNewCapacity() > resize.getOldCapacity() ? "UP" : "DOWN",
                    beautifyCommodityType(resize.getCommodityType()),
                    ActionDescriptionBuilder.formatResizeActionCommodityValue(
                            commodity, resize.getTarget().getType(), resize.getOldCapacity()),
                    ActionDescriptionBuilder.formatResizeActionCommodityValue(
                            commodity, resize.getTarget().getType(), resize.getNewCapacity())
            );

            resizeInfoExplanations.add(explanation);
        }

        StringBuilder allExplanations = new StringBuilder();
        allExplanations.append(coreExplanation).append(" - ");
        allExplanations.append(String.join(", ", resizeInfoExplanations));

        return Collections.singleton(allExplanations.toString());
    }

    private static String buildAtomicResizeCoreExplanation(ActionDTO.Action action) {
        StringBuilder sb = new StringBuilder(ActionDTOUtil.TRANSLATION_PREFIX);
        AtomicResize atomicResize = action.getInfo().getAtomicResize();
        String targetClause = atomicResize.hasExecutionTarget()
                ? " Merged on " + buildEntityTypeAndName(atomicResize.getExecutionTarget())
                : "";

        sb.append(targetClause);
        return sb.toString();
    }

    /**
     * Build resize explanation.
     * e.g. full explanation:
     *      resize down: "Underutilized Mem in {entity name}"
     *      resize up: "Mem Congestion in {entity name}"
     * e.g. short explanation:
     *      resize down: "Underutilized Mem"
     *      resize up: "Mem Congestion"
     *
     * @param action the resize action
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildResizeExplanation(@Nonnull final ActionDTO.Action action,
                                                 final boolean keepItShort) {
        // verify it's a resize.
        if (!action.getInfo().hasResize()) {
            logger.warn("Can't build resize explanation for non-resize action {}", action.getId());
            return "";
        }

        final String resizeExplanation = buildResizeCoreExplanation(action, keepItShort);
        if (keepItShort) {
            return resizeExplanation;
        }

        // since we may show entity name, we are going to build a translatable explanation
        StringBuilder sb = new StringBuilder(ActionDTOUtil.TRANSLATION_PREFIX);
        Resize resize = action.getInfo().getResize();
        // if we have a target, we will try to show it's name in the explanation
        String targetClause = resize.hasTarget()
                ? " in "+ buildEntityTypeAndName(resize.getTarget())
                : "";
        sb.append(resizeExplanation).append(targetClause);
        getScalingGroupExplanation(action.getExplanation()).ifPresent(sb::append);
        return sb.toString();
    }

    /**
     * Build resize core explanation.
     * e.g. full explanation:
     *      resize down: "Underutilized {commodity name} in {entity name}"
     *      resize up: "{commodity name} Congestion in {entity name}"
     * e.g. short explanation:
     *      resize down: "Underutilized {commodity name}"
     *      resize up: "{commodity name} Congestion"
     *
     * @param action the resize action
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildResizeCoreExplanation(ActionDTO.Action action, final boolean keepItShort) {
        final Resize resize = action.getInfo().getResize();
        final String commodityType = commodityDisplayName(resize.getCommodityType(), keepItShort) +
            (resize.getCommodityAttribute() == CommodityAttribute.RESERVED ? " reservation" : "");

        // now modeling this behavior after ActionGeneratorImpl.notifyRightSize() in classic.
        // NOT addressing special cases for: Ready Queue, Reserved Instance, Cloud Template, Fabric
        // Interconnect, VStorage, VCPU. If we really need those, we can add them in as necessary.
        final boolean isResizeDown = action.getInfo().getResize().getOldCapacity() >
            action.getInfo().getResize().getNewCapacity();
        if (isResizeDown) {
            return UNDERUTILIZED_EXPLANATION + commodityType;
        } else {
            return commodityType + CONGESTION_EXPLANATION;
        }
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
     * Build Buy RI explanation.
     * e.g. full explanation:
     *      "Increase RI Coverage by 50%"
     * e.g. short explanation:
     *      "Increase RI Coverage"
     *
     * @param action the action to explain
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildBuyRIExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort) {
        final BuyRIExplanation buyRI = action.getExplanation().getBuyRI();
        if (buyRI.getTotalAverageDemand() <= 0) {
            return INVALID_BUY_RI_EXPLANATION;
        }
        if (keepItShort) {
            return BUY_RI_EXPLANATION;
        }
        float coverageIncrease = (buyRI.getCoveredAverageDemand() / buyRI.getTotalAverageDemand()) * 100;
        return BUY_RI_EXPLANATION + " by " + Math.round(coverageIncrease) + "%";
    }

    /**
     * Build activate explanation.
     * e.g. full explanation:
     *      "Address high utilization of Mem" or "Add more resource to satisfy the increased demand"
     * e.g. short explanation:
     *      "CPU congestion" or "Overutilized resources"
     *
     * @param action the action to explain
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildActivateExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort) {
        final Explanation explanation = action.getExplanation();
        if (!explanation.getActivate().hasMostExpensiveCommodity()) {
            return keepItShort ? OVERUTILIZED_RESOURCES_CATEGORY : ACTIVATE_EXPLANATION_WITHOUT_REASON_COMM;
        } else {
            final String commodityName =
                UICommodityType.fromType(explanation.getActivate().getMostExpensiveCommodity()).apiStr();
            return keepItShort ? commodityName + CONGESTION_EXPLANATION :
                ACTIVATE_EXPLANATION_WITH_REASON_COMM + commodityName;
        }
    }

    /**
     * Build deactivate explanation.
     * e.g. Improve infrastructure efficiency
     *
     * @return the explanation sentence
     */
    private static String buildDeactivateExplanation() {
        return DEACTIVATE_EXPLANATION;
    }

    /**
     * Build reconfigure explanation.
     * e.g. full explanation:
     *      "Enable supplier to offer requested resource(s) Ballooning, Network Commodity test_network"
     *   or "{entity:1:displayName:Virtual Machine} doesn't comply to settingName"
     * e.g. short explanation:
     *      "Misconfiguration"
     *
     * @param action the action to explain
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildReconfigureExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            final boolean keepItShort) {
        if (keepItShort) {
            return RECONFIGURE_EXPLANATION_CATEGORY;
        }

        final Explanation explanation = action.getExplanation();
        final ReconfigureExplanation reconfigExplanation = explanation.getReconfigure();
        final StringBuilder sb = new StringBuilder();
        if (!reconfigExplanation.getReasonSettingsList().isEmpty()) {
            sb.append(ActionDTOUtil.TRANSLATION_PREFIX)
                .append(buildReasonSettingsExplanation(action.getInfo().getReconfigure().getTarget(),
                    reconfigExplanation.getReasonSettingsList(),
                    settingPolicyIdToSettingPolicyName, false));
        } else {
            sb.append(buildReconfigureReasonCommodityExplanation(
                reconfigExplanation.getReconfigureCommodityList()));
        }
        getScalingGroupExplanation(action.getExplanation()).ifPresent(sb::append);
        return sb.toString();
    }

    /**
     * Build reconfigure explanation due to reason commodities.
     *
     * e.g. full explanation:
     *      "Enable supplier to offer requested resource(s) Ballooning, Network Commodity test_network"
     *
     * @param commodityTypes a list of missing reason commodities
     * @return the explanation sentence
     */
    private static String buildReconfigureReasonCommodityExplanation(
            @Nonnull final Collection<ReasonCommodity> commodityTypes) {
        return RECONFIGURE_REASON_COMMODITY_EXPLANATION +
            commodityTypes.stream().map(reason ->
                getCommodityDisplayName(reason.getCommodityType()))
                .collect(Collectors.joining(", "));
    }

    /**
     * Build reason setting explanation.
     * e.g. full explanation:
     *      "{entity:1:displayName:Virtual Machine} doesn't comply to settingName"
     * e.g. short explanation:
     *      "Setting policy compliance"
     *
     * @param target the target entity
     * @param reasonSettings a list of settingPolicyIds that causes this action
     * @param keepItShort generate short explanation or not
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @return the explanation sentence
     */
    private static String buildReasonSettingsExplanation(
            @Nonnull final ActionEntity target, @Nonnull final List<Long> reasonSettings,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName, final boolean keepItShort) {
        if (keepItShort) {
            return REASON_SETTING_EXPLANATION_CATEGORY;
        }
        return MessageFormat.format(REASON_SETTINGS_EXPLANATION, buildEntityNameOrType(target),
            reasonSettings.stream().map(settingPolicyIdToSettingPolicyName::get).collect(Collectors.joining(", ")));
    }

    /**
     * Build provision explanation.
     * e.g. full explanation:
     *      "CPU, Mem Congestion in '{entity:1:displayName:Physical Machine}'"
     * e.g. short explanation:
     *      Set("CPU Congestion", "Mem Congestion")
     *
     *
     * @param action the action to explain
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static Set<String> buildProvisionExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort) {
        final Explanation explanation = action.getExplanation();
        final ProvisionExplanation provisionExplanation = explanation.getProvision();
        switch (provisionExplanation.getProvisionExplanationTypeCase()) {
            case PROVISION_BY_DEMAND_EXPLANATION:
                final Set<String> commodityNames =
                    getProvisionByDemandCommodityNames(provisionExplanation, keepItShort);
                if (keepItShort) {
                    return commodityNames.stream()
                        .map(commodityName -> commodityName + CONGESTION_EXPLANATION)
                        .collect(Collectors.toSet());
                } else {
                    return Collections.singleton(ActionDTOUtil.TRANSLATION_PREFIX +
                        String.join(", ", commodityNames) + CONGESTION_EXPLANATION + " in '" +
                        buildEntityNameOrType(action.getInfo().getProvision().getEntityToClone()) + "'");
                }
            case PROVISION_BY_SUPPLY_EXPLANATION:
                return Collections.singleton(buildProvisionBySupplyExplanation(provisionExplanation, keepItShort));
            default:
                return Collections.singleton(keepItShort ? ACTION_ERROR_CATEGORY : ACTION_TYPE_ERROR);
        }
    }

    /**
     * Return a set of commodity names of a provision by demand explanation.
     *
     * @param provisionExplanation provision explanation
     * @param keepItShort compose a short explanation if true
     * @return a set of commodity names
     */
    private static Set<String> getProvisionByDemandCommodityNames(
            @Nonnull final ProvisionExplanation provisionExplanation, final boolean keepItShort) {
        return provisionExplanation.getProvisionByDemandExplanation()
            .getCommodityMaxAmountAvailableList().stream()
            .map(CommodityMaxAmountAvailableEntry::getCommodityBaseType)
            .map(baseType -> CommodityType.newBuilder().setType(baseType).build())
            .map(commodityType -> commodityDisplayName(commodityType, keepItShort))
            .collect(Collectors.toSet());
    }

    /**
     * Build provision by supply explanation.
     * e.g. "Storage Latency Congestion"
     *
     * @param provisionExplanation provision explanation
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildProvisionBySupplyExplanation(
            @Nonnull final ProvisionExplanation provisionExplanation, final boolean keepItShort) {
        return commodityDisplayName(provisionExplanation.getProvisionBySupplyExplanation()
            .getMostExpensiveCommodityInfo().getCommodityType(), keepItShort) + CONGESTION_EXPLANATION;
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
        if (keepItShort || !reasonCommodity.hasTimeSlot()) {
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
