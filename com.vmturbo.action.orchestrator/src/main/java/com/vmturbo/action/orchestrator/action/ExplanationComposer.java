package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.beautifyAtomicActionsCommodityType;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.getCommodityDisplayName;
import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.ENTITY_WITH_ADDITIONAL_COMMODITY_CHANGES;
import static java.util.stream.Collectors.toList;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.AtomicResizeExplanation.ResizeExplanationPerEntity.ResizeExplanationPerCommodity;
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
import com.vmturbo.common.protobuf.action.ActionDTOUtil.EntityField;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.commons.Pair;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.util.BaseGraphEntity;

/**
 * A utility with static methods that assist in composing explanations for actions.
 */
public class ExplanationComposer {
    private static final Logger logger = LogManager.getLogger();

    private static final String TAINT_MOVE_COMPLIANCE_EXPLANATION_FORMATION =
        "{0} cannot tolerate taints {1} on {2}";
    private static final String LABEL_MOVE_COMPLIANCE_EXPLANATION_FORMAT =
        "{0} cannot satisfy nodeSelector {1}";
    private static final String MOVE_COMPLIANCE_EXPLANATION_FORMAT =
        "{0} can not satisfy the request for resource(s) ";
    private static final String MOVE_EVACUATION_RECONFIGURE_EXPLANATION_FORMAT =
        "{0} reconfigure consolidation to improve efficiency";
    private static final String MOVE_EVACUATION_SUSPENSION_EXPLANATION_FORMAT =
        "{0} can be suspended to improve efficiency";
    private static final String MOVE_EVACUATION_AVAILABILITY_EXPLANATION_FORMAT =
        "{0} is not available";
    private static final String MOVE_PERFORMANCE_EXPLANATION =
        "Improve overall performance";
    private static final String IMPROVE_OVERALL_EFFICIENCY =
            "Improve overall efficiency";
    private static final String NO_COST_SCALING_COMMODITY_TYPE =
            "Scaling to free {0}";
    private static final String ACTIVATE_EXPLANATION_WITH_REASON_COMM =
        "Address high utilization of ";
    private static final String ACTIVATE_EXPLANATION_WITHOUT_REASON_COMM =
        "Add more resource to satisfy the increased demand";
    private static final String DEACTIVATE_EXPLANATION = "Improve infrastructure efficiency";
    private static final String RECONFIGURE_REASON_COMMODITY_EXPLANATION_FOR_CONTAINER_POD =
        "Configure node or pod to update resource(s) Taint, node-selector, affinity or anti-affinity";
    private static final String RECONFIGURE_TAINT_COMMODITY_EXPLANATION =
        "The pod does not tolerate taint(s) on any node";
    private static final String RECONFIGURE_LABEL_COMMODITY_EXPLANATION =
        "Zero nodes match Pod's node selector";
    private static final String RECONFIGURE_VMPMACCESS_COMMODITY_EXPLANATION =
        "Zero nodes match Pod's affinity or anti-affinity";
    private static final String  RECONFIGURE_REASON_COMMODITY_EXPLANATION =
        "Configure supplier to update resource(s) ";
    private static final String RECONFIGURE_REASON_NODE_NOT_READY =
        "The node is in a NotReady status";
    private static final String REASON_SETTINGS_EXPLANATION =
        "{0} doesn''t comply with {1}";
    private static final String REASON_SETTINGS_VCPU_RECONFIG_EXPLANATION = "{0} out of compliance";
    private static final String SINGLE_DELETED_POLICY_MSG = "a compliance policy that used to exist";
    private static final String MULTIPLE_DELETED_POLICY_MSG = "compliance policies that used to exist";
    private static final String ACTION_TYPE_ERROR =
        "Can not give a proper explanation as action type is not defined";
    private static final String EXPLANATION_ERROR =
        "Can not give a proper explanation";
    private static final String INCREASE_RI_UTILIZATION =
        "Increase RI Utilization";
    private static final String PER_COMMODITY_ATOMIC_RESIZE_CONGESTION_EXPLANATION = "{0} Congestion";
    private static final String PER_COMMODITY_ATOMIC_RESIZE_UNDERUTILIZATION_EXPLANATION = "Underutilized {0}";
    private static final String WASTED_COST = "Cost Reduction";
    private static final String DELETE_WASTED_FILES_EXPLANATION = "Idle or non-productive";
    private static final String DELETE_WASTED_VOLUMES_EXPLANATION = "Increase savings";
    private static final String DELETE_WASTED_AZURE_APP_SERVICE_PLANS_EXPLANATION = "Increase savings";
    private static final String ALLOCATE_EXPLANATION = "Virtual Machine can be covered by {0} RI";
    private static final String UNDERUTILIZED_EXPLANATION = "Underutilized ";
    private static final String CONGESTION_EXPLANATION = " Congestion";
    private static final String BUY_RI_EXPLANATION = "Increase RI Coverage";
    private static final String INVALID_BUY_RI_EXPLANATION = "Invalid total demand";

    // Short explanations
    private static final String OVERUTILIZED_RESOURCES_CATEGORY = "Overutilized resources";
    private static final String UNDERUTILIZED_RESOURCES_CATEGORY = "Underutilized resources";
    private static final String RECONFIGURE_EXPLANATION_CATEGORY = "Misconfiguration";
    private static final String RECONFIGURE_SETTING_CHANGE_CATEGORY = "Required setting change";
    private static final String REASON_SETTING_EXPLANATION_CATEGORY = "Setting policy compliance";
    private static final String CSG_COMPLIANCE_EXPLANATION_CATEGORY = "CSG compliance";
    private static final String REASON_COMMODITY_EXPLANATION_CATEGORY =  " compliance";
    private static final String SEGMENTATION_COMMODITY_EXPLANATION_CATEGORY =  "Placement policy compliance";
    private static final String ALLOCATE_CATEGORY = "Virtual Machine RI Coverage";
    private static final String ACTION_ERROR_CATEGORY = "";

    private static final String STORAGE_ACCESS_TO_IOPS = "IOPS";
    private static final Function<String, String> convertStorageAccessToIops = (commodity) ->
        commodity.equals("Storage Access") ? STORAGE_ACCESS_TO_IOPS : commodity;

    // Explanation overrides for cloud migration
    private static final String CLOUD_MIGRATION_LIFT_AND_SHIFT_EXPLANATION = "Lift & Shift migration";
    private static final String CLOUD_MIGRATION_OPTIMIZED_EXPLANATION = "Optimized migration";

    private static final Map<ChangeProviderExplanation.WastedCostCategory, String> wastedCostCategoryExplanations =
            ImmutableMap.of(
                    ChangeProviderExplanation.WastedCostCategory.COMPUTE, "Compute",
                    ChangeProviderExplanation.WastedCostCategory.STORAGE, "Storage");
    private static final String MOVE_COMPLIANCE_ACTION = "MoveCompliance";
    private static final String RECONFIGURE_ACTION = "Reconfigure";
    private static final Map<Integer, Map<String, String>> podActionExplanations =
            ImmutableMap.of(
                    CommodityDTO.CommodityType.TAINT_VALUE, ImmutableMap.of(MOVE_COMPLIANCE_ACTION, TAINT_MOVE_COMPLIANCE_EXPLANATION_FORMATION,
                            RECONFIGURE_ACTION, RECONFIGURE_TAINT_COMMODITY_EXPLANATION),
                    CommodityDTO.CommodityType.LABEL_VALUE, ImmutableMap.of(MOVE_COMPLIANCE_ACTION, LABEL_MOVE_COMPLIANCE_EXPLANATION_FORMAT,
                            RECONFIGURE_ACTION, RECONFIGURE_LABEL_COMMODITY_EXPLANATION),
                    CommodityDTO.CommodityType.VMPM_ACCESS_VALUE, ImmutableMap.of(MOVE_COMPLIANCE_ACTION, "",
                            RECONFIGURE_ACTION,RECONFIGURE_VMPMACCESS_COMMODITY_EXPLANATION)
            );

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
     * @param relatedActions RelatedActions for the given action
     * @return a set of short explanation sentences
     */
    @Nonnull
    @VisibleForTesting
    public static Set<String> composeRelatedRisks(@Nonnull ActionDTO.Action action,
                                                  @Nonnull List<ActionDTO.RelatedAction> relatedActions) {
        return internalComposeExplanation(action, true, Collections.emptyMap(), Collections.emptyMap(),
            Optional.empty(), null, relatedActions);
    }

    /**
     * This method should be used only for tests.
     *
     * @param action the action to explain
     * @param relatedActions RelatedActions for the given action
     * @return the explanation sentence
     */
    @Nonnull
    @VisibleForTesting
    static String composeExplanation(@Nonnull final ActionDTO.Action action,
                                     @Nonnull List<ActionDTO.RelatedAction> relatedActions) {
        return internalComposeExplanation(action, false, Collections.emptyMap(), Collections.emptyMap(),
                Optional.empty(), null, relatedActions).iterator().next();
    }

    /**
     * Compose a full explanation for an action.
     *
     * @param action the action to explain
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param topologyInfo Info about plan topology for explanation override.
     * @param relatedActions RelatedActions for the given action
     * @return the explanation sentence
     */
    @Nonnull
    public static String composeExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            @Nullable final Map<Long, String> vcpuScalingActionsPolicyIdToName,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            @Nullable final TopologyInfo topologyInfo,
            @Nonnull List<ActionDTO.RelatedAction> relatedActions) {
        return internalComposeExplanation(action, false,
            settingPolicyIdToSettingPolicyName, vcpuScalingActionsPolicyIdToName, topology, topologyInfo, relatedActions)
            .iterator().next();
    }

    /**
     * Get a set of commodity keys for TAINT commodities from the given reason commodities.
     *
     * @param reasonCommodities a given list of reason commodities
     * @param commTypeValue given commodity to match in reasoncommodities
     * @return set of commodity keys
     */
    @Nonnull
    static Set<String> getCommodityReasons(@Nonnull final List<ReasonCommodity> reasonCommodities,
                                           @Nonnull final int commTypeValue) {
        return reasonCommodities.stream()
                .map(ReasonCommodity::getCommodityType)
                .filter(commodityType -> commTypeValue == commodityType.getType())
                .map(CommodityType::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Create a map of commodityType and commodityKey from the reasonCommodities filtering on non null Key commodity which is either
     * TAINT, LABEL or VMPMACCESS commodity which can impact Reconfigure action
     * @param reasonCommodities a given list of reason commodities
     * @return Multimap <commodityType, commodityKey>
     * */

    @Nonnull
    static Map<Integer, Set<String>> categorizeContainerPodCommodityReasons(@Nonnull final List<ReasonCommodity> reasonCommodities)
    {

        return reasonCommodities.stream()
                .filter(reasonCommodity -> reasonCommodity.getCommodityType().hasKey())
                .filter(reasonCommodity -> podActionExplanations.containsKey(reasonCommodity.getCommodityType().getType()))
                .collect(Collectors.groupingBy(reasonCommodity -> reasonCommodity.getCommodityType().getType(),
                Collectors.mapping(reasonCommodity -> reasonCommodity.getCommodityType().getKey(), Collectors.toSet())));
    }


    /**
     * Gets explanation override string for cloud migration case if applicable.
     *
     * @param topologyInfo TopologyInfo to check if this is a cloud migration case.
     * @return Overridden explanation for cloud migration, or null.
     */
    @Nullable
    private static String getPlanExplanationOverride(@Nullable final TopologyInfo topologyInfo) {
        if (topologyInfo == null || !TopologyDTOUtil.isCloudMigrationPlan(topologyInfo)) {
            return null;
        }
        if (TopologyDTOUtil.isResizableCloudMigrationPlan(topologyInfo)) {
            return CLOUD_MIGRATION_OPTIMIZED_EXPLANATION;
        }
        return CLOUD_MIGRATION_LIFT_AND_SHIFT_EXPLANATION;
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
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param topologyInfo Info about plan topology for explanation override.
     * @param relatedActions RelatedActions for the given action
     * @return a set of explanation sentences
     */
    @Nonnull
    private static Set<String> internalComposeExplanation(
            @Nonnull final ActionDTO.Action action, final boolean keepItShort,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            @Nullable final Map<Long, String> vcpuScalingActionsPolicyIdToName,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            @Nullable final TopologyInfo topologyInfo,
            @Nonnull List<ActionDTO.RelatedAction> relatedActions) {
        final Explanation explanation = action.getExplanation();
        switch (explanation.getActionExplanationTypeCase()) {
            case MOVE:
            case SCALE:
                return buildMoveExplanation(action, settingPolicyIdToSettingPolicyName,
                    topology, keepItShort, topologyInfo);
            case ALLOCATE:
                return Collections.singleton(buildAllocateExplanation(action, keepItShort));
            case ATOMICRESIZE:
                //invoked when the action spec is created from the action view
                return buildAtomicResizeExplanation(action, topology, keepItShort);
            case RESIZE:
                return Collections.singleton(buildResizeExplanation(action, relatedActions, topology, keepItShort));
            case ACTIVATE:
                return Collections.singleton(buildActivateExplanation(action, keepItShort));
            case DEACTIVATE:
                return Collections.singleton(buildDeactivateExplanation(action, topology));
            case RECONFIGURE:
                return Collections.singleton(buildReconfigureExplanation(
                    action, settingPolicyIdToSettingPolicyName, vcpuScalingActionsPolicyIdToName, topology, keepItShort));
            case PROVISION:
                return buildProvisionExplanation(action, topology, keepItShort);
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
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort compose a short explanation if true
     * @param topologyInfo Info about plan topology for explanation override.
     * @return a set of explanation sentences
     */
    private static Set<String> buildMoveExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            final boolean keepItShort,
            @Nullable final TopologyInfo topologyInfo) {
        final String explanationOverride = getPlanExplanationOverride(topologyInfo);
        if (explanationOverride != null) {
            return ImmutableSet.of(explanationOverride);
        }
        final Set<String> moveExplanations = buildMoveCoreExplanation(
            action, settingPolicyIdToSettingPolicyName, topology, keepItShort);
        if (keepItShort) {
            return moveExplanations;
        }
        String explanation = moveExplanations.stream().filter(exp -> !exp.isEmpty()).collect(Collectors.joining(", "));
        return Collections.singleton(ActionDTOUtil.TRANSLATION_PREFIX +
             explanation + getScalingGroupExplanation(action.getExplanation(), explanation).orElse(""));
    }

    /**
     * Build move core explanation.
     *
     * @param action the action to explain
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort generate short explanation or not
     * @return a set of explanation sentences
     */
    private static Set<String> buildMoveCoreExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            final boolean keepItShort) {
        final Explanation explanation = action.getExplanation();
        // if we only have one source entity, we'll use it in the explanation builder. if
        // multiple, we won't bother because we don't have enough info to attribute
        // commodities to specific sources
        List<ActionEntity> source_entities = ActionDTOUtil.getChangeProviderList(action)
            .stream()
            .map(ChangeProvider::getSource)
            .collect(toList());
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
            .collect(toList());
        if (!primaryChangeExplanation.isEmpty()) {
            changeExplanations = primaryChangeExplanation;
        }

        return changeExplanations.stream().flatMap(changeExplanation -> {
            try {
                return buildMoveChangeExplanation(optionalSourceEntity,
                    ActionDTOUtil.getPrimaryEntity(action), changeExplanation,
                    settingPolicyIdToSettingPolicyName, topology, keepItShort).stream();
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
     * @param stringExplanation the string explanation so far
     * @return Optional containing the scaling group explanation if the explanation contains
     * scaling group information, else Optional.empty.  If scaling group information is present
     * but there is no mapping available, the scaling group ID itself is returned.
     */
    private static Optional<String> getScalingGroupExplanation(final Explanation explanation,
                                                               final String stringExplanation) {
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
            String prefix = stringExplanation.isEmpty() ? "Comply to " : "(";
            String suffix = stringExplanation.isEmpty() ? "" : ")";
            String leadingSpace = stringExplanation.isEmpty() ? "" : " ";
            return Optional.of(leadingSpace + prefix + "Auto Scaling Groups: "
                + scalingGroupName + suffix);
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
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort generate short explanation or not
     * @return a set of explanation sentences
     */
    private static Set<String> buildMoveChangeExplanation(
            @Nonnull final Optional<ActionEntity> optionalSourceEntity, @Nonnull final ActionEntity target,
            @Nonnull final ChangeProviderExplanation changeProviderExplanation,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            final boolean keepItShort) {
        switch (changeProviderExplanation.getChangeProviderExplanationTypeCase()) {
            case COMPLIANCE:
                if (!changeProviderExplanation.getCompliance().getReasonSettingsList().isEmpty()) {
                    return Collections.singleton(buildReasonSettingsExplanation(target,
                        changeProviderExplanation.getCompliance().getReasonSettingsList(),
                        settingPolicyIdToSettingPolicyName, topology, keepItShort));
                } else if (!changeProviderExplanation.getCompliance().getMissingCommoditiesList().isEmpty()) {
                    return buildComplianceReasonCommodityExplanation(optionalSourceEntity, target,
                        changeProviderExplanation.getCompliance().getMissingCommoditiesList(),
                        topology, keepItShort);
                } else if (changeProviderExplanation.getCompliance().hasIsCsgCompliance()) {
                    if (keepItShort) {
                        return Collections.singleton(CSG_COMPLIANCE_EXPLANATION_CATEGORY);
                    }
                    // We don't need an explanation for CSG compliance actions
                    return Collections.singleton("");
                }
                return Collections.singleton(EXPLANATION_ERROR);
            case CONGESTION:
                return buildCongestionExplanation(changeProviderExplanation.getCongestion(), keepItShort);
            case EVACUATION:
                return Collections.singleton(buildEvacuationExplanation(
                    changeProviderExplanation.getEvacuation(), topology, keepItShort));
            case PERFORMANCE:
                return Collections.singleton(buildPerformanceExplanation());
            case EFFICIENCY:
                return buildEfficiencyExplanation(target, changeProviderExplanation.getEfficiency(), keepItShort);
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
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort generate short explanation or not
     * @return the explanation sentence
     */
    private static Set<String> buildComplianceReasonCommodityExplanation(
            @Nonnull final Optional<ActionEntity> optionalSourceEntity,
            @Nonnull final ActionEntity target,
            @Nonnull final List<ReasonCommodity> reasonCommodities,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            final boolean keepItShort) {
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

        final String providerName = optionalSourceEntity
                .map(e -> buildEntityNameOrType(e, topology))
                .orElse("Current supplier");

        if (target.getType() == EntityType.CONTAINER_POD_VALUE) {
            final Map<Integer, Set<String>> mapCommodityTypeReason = categorizeContainerPodCommodityReasons(reasonCommodities);
            if(mapCommodityTypeReason.size() == 1){
                Map<String, String> actionExplanationMap = podActionExplanations.get(mapCommodityTypeReason.keySet().iterator().next());
                //if Container pod move  action is due to missing only Taint commodity
                if(mapCommodityTypeReason.containsKey(CommodityDTO.CommodityType.TAINT_VALUE))
                {
                    final Set<String> taintReasons = mapCommodityTypeReason.get(CommodityDTO.CommodityType.TAINT_VALUE);
                    return Collections.singleton(
                        MessageFormat.format(actionExplanationMap.get(MOVE_COMPLIANCE_ACTION),
                                buildEntityTypeAndName(target, topology),
                                String.join(", ", taintReasons),
                                providerName));
                }
                //if Container pod move  action is due to missing only Label commodity
                if(mapCommodityTypeReason.containsKey(CommodityDTO.CommodityType.LABEL_VALUE))
                {
                    final Set<String> labelReasons = mapCommodityTypeReason.get(CommodityDTO.CommodityType.LABEL_VALUE);
                    return Collections.singleton(
                        MessageFormat.format(LABEL_MOVE_COMPLIANCE_EXPLANATION_FORMAT, providerName,
                                String.join(", ",labelReasons)));
                }
            }
        }


        return Collections.singleton(MessageFormat.format(MOVE_COMPLIANCE_EXPLANATION_FORMAT, providerName)
            + reasonCommodities.stream()
            .map(ReasonCommodity::getCommodityType)
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
        Set<String> result = Sets.newHashSet();
        final List<ReasonCommodity> congestedCommodities = congestion.getCongestedCommoditiesList();
        if (!congestedCommodities.isEmpty()) {
            result.addAll(buildCommodityUtilizationExplanation(congestedCommodities,
                ChangeProviderExplanationTypeCase.CONGESTION, keepItShort));
            if (congestion.getIsWastedCost()) {
                StringBuilder wastedCost = new StringBuilder();
                if (congestion.hasWastedCostCategory()) {
                    wastedCost.append(wastedCostCategoryExplanations.get(congestion.getWastedCostCategory())).append(" ");
                }
                wastedCost.append(WASTED_COST);
                result.add(wastedCost.toString());
            }
            if (!congestion.getUnderUtilizedCommoditiesList().isEmpty()) {
                result.addAll(buildCommodityUtilizationExplanation(congestion.getUnderUtilizedCommoditiesList(),
                        ChangeProviderExplanationTypeCase.EFFICIENCY, keepItShort));
            }
        }
        return result;
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
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort Defines whether to generate short explanation or not.
     * @return explanation
     */
    private static String buildEvacuationExplanation(Evacuation evacuation,
                                                     @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
                                                     boolean keepItShort) {
        if (keepItShort) {
            return UNDERUTILIZED_RESOURCES_CATEGORY;
        }
        return MessageFormat.format(
            evacuation.getIsAvailable()
                ? ((evacuation.hasEvacuationExplanation() && evacuation.getEvacuationExplanation().hasReconfigureRemoval())
                    ? MOVE_EVACUATION_RECONFIGURE_EXPLANATION_FORMAT : MOVE_EVACUATION_SUSPENSION_EXPLANATION_FORMAT)
                : MOVE_EVACUATION_AVAILABILITY_EXPLANATION_FORMAT,
            buildNameBlock(evacuation.getSuspendedEntity(), "Current supplier", topology));
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
     * @param actionEntity the action entity.
     * @param efficiency   the efficiency change provider explanation.
     * @param keepItShort  compose a short explanation if true.
     * @return a set of explanation sentences
     */
    private static Set<String> buildEfficiencyExplanation(
            @Nonnull final ActionEntity actionEntity,
            @Nonnull final Efficiency efficiency, final boolean keepItShort) {
        Set<String> result = new LinkedHashSet<>();
        if (efficiency.getIsRiCoverageIncreased()) {
            result.add(INCREASE_RI_UTILIZATION);
        }
        if (!efficiency.getUnderUtilizedCommoditiesList().isEmpty()) {
            result.addAll(buildCommodityUtilizationExplanation(efficiency.getUnderUtilizedCommoditiesList(),
                    ChangeProviderExplanationTypeCase.EFFICIENCY, keepItShort));
        }
        if (efficiency.getIsWastedCost()) {
            StringBuilder wastedCost = new StringBuilder();
            if (efficiency.hasWastedCostCategory()) {
                wastedCost.append(wastedCostCategoryExplanations.get(efficiency.getWastedCostCategory())).append(" ");
            }
            wastedCost.append(WASTED_COST);
            result.add(wastedCost.toString());
        }
        if (ENTITY_WITH_ADDITIONAL_COMMODITY_CHANGES.contains(actionEntity.getType())) {
            if (!efficiency.getScaleUpCommodityList().isEmpty()) {
                Collection<String> scalingUpCommodities = efficiency.getScaleUpCommodityList().stream().map(
                        scalingUpCommodity -> commodityDisplayName(scalingUpCommodity, keepItShort)
                ).collect(toList());
                String concatenatedCommodityNames = String.join(", ", scalingUpCommodities);
                result.add(MessageFormat.format(NO_COST_SCALING_COMMODITY_TYPE, concatenatedCommodityNames));
            }
        }
        if (result.isEmpty()) {
            result.add(IMPROVE_OVERALL_EFFICIENCY);
        }
        return result;
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
            .map(commodityType -> convertStorageAccessToIops.apply(buildExplanationWithTimeSlots(commodityType, keepItShort)));
        final Set<String> commodities;
        if (keepItShort) {
            commodities = reasonCommodityStream.collect(Collectors.toSet());
        } else {
            commodities = Collections.singleton(
                reasonCommodityStream.collect(Collectors.joining(", ")));
        }

        if (explanationType == ChangeProviderExplanationTypeCase.EFFICIENCY) {
            return commodities.stream().map(commodity -> UNDERUTILIZED_EXPLANATION + convertStorageAccessToIops.apply(commodity))
                .collect(Collectors.toSet());
        } else if (explanationType == ChangeProviderExplanationTypeCase.CONGESTION) {
            return commodities.stream().map(commodity -> convertStorageAccessToIops.apply(commodity) + CONGESTION_EXPLANATION)
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
     *
     * <p>e.g. full explanation for merged actions on workload controller:
     *   "Controller Resize -
     *     Resize DOWN VCPU Limit from 5,328 to 328,
     *     Resize DOWN VMem Limit from 1.0 GB to 128.0 MB
     *     in Container Spec istio-proxy;
     *     Resize DOWN VMem Request from 512.0 MB to 128.0 MB,
     *     Resize UP VCPU Request from 666 to 966
     *     in Container Spec twitter-cass-tweet"
     *
     * <p>e.g. short explanation for merged actions on workload controller:
     *      "Controller Resize"
     *
     * <p>e.g. short explanation for merged actions on container spec:
     *     "Container Resize"
     *
     * @param action the action to explain
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static Set<String> buildAtomicResizeExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            final boolean keepItShort) {

        if (!action.getInfo().hasAtomicResize()) {
            logger.warn("Cannot build atomic resize explanation for non-resize action {}", action.getId());
            return Collections.emptySet();
        }

        AtomicResize atomicResize = action.getInfo().getAtomicResize();
        final List<String> resizeInfoExplanations = new ArrayList<>();

        final Map<Long, List<ResizeExplanationPerEntity>> resizeExplanationByTarget
                = action.getExplanation().getAtomicResize().getPerEntityExplanationList().stream()
                    .collect(Collectors.groupingBy(ResizeExplanationPerEntity::getTargetId));

        // group the resize infos by target entity
        final Map<ActionEntity, List<ResizeInfo>> resizeInfoByTarget
                = atomicResize.getResizesList().stream().collect(Collectors.groupingBy(resize -> resize.getTarget()));

        Set<String> risks = new HashSet<>();
        resizeInfoByTarget.forEach((target, resizeInfoList) -> {
            List<String> explanations = new ArrayList<>();

            // explanation string for all the commodity resizes per target
            for (ResizeInfo resize : resizeInfoList) {
                List<ResizeExplanationPerEntity> expPerEntity =
                        resizeExplanationByTarget.get(resize.getTarget().getId());
                Optional<ResizeExplanationPerCommodity> commodityExp =
                        expPerEntity.stream().map(exp -> exp.getPerCommodityExplanation())
                                .filter(commExp -> commExp.getCommodityType().equals(resize.getCommodityType()))
                                .findFirst();

                if (resize.getNewCapacity() > resize.getOldCapacity()) {
                    CommodityType reason = resize.getCommodityType();
                    if (commodityExp.isPresent() && commodityExp.get().hasReason()) {
                        reason = commodityExp.get().getReason();
                    }
                    String explanation = MessageFormat.format(
                            PER_COMMODITY_ATOMIC_RESIZE_CONGESTION_EXPLANATION,
                            beautifyAtomicActionsCommodityType(reason));

                    explanations.add(explanation);
                } else {
                    String explanation = MessageFormat.format(
                            PER_COMMODITY_ATOMIC_RESIZE_UNDERUTILIZATION_EXPLANATION,
                            beautifyAtomicActionsCommodityType(resize.getCommodityType()));

                    explanations.add(explanation);
                }
            }

            String targetClause = " in " + buildEntityTypeAndName(target, topology);
            risks.addAll(explanations);

            resizeInfoExplanations.add(String.join(", ", explanations) + targetClause);
        });

        // This generic explanation string is used as a filter criterion for grouping actions.
        if (keepItShort) {
            return risks;
        }
        // Combined explanation for all the targets
        StringBuilder allExplanations = new StringBuilder(ActionDTOUtil.TRANSLATION_PREFIX);
        allExplanations.append(String.join("; ", resizeInfoExplanations));

        return Collections.singleton(allExplanations.toString());
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
     * @param relatedActions RelatedActions for the given action
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildResizeExplanation(@Nonnull final ActionDTO.Action action,
                                                 @Nonnull List<ActionDTO.RelatedAction> relatedActions,
                                                 @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
                                                 final boolean keepItShort) {
        // verify it's a resize.
        if (!action.getInfo().hasResize()) {
            logger.warn("Can't build resize explanation for non-resize action {}", action.getId());
            return "";
        }

        if (!relatedActions.isEmpty()) {
            return buildResizeCoreExplanationUsingRelatedActions(action, relatedActions);
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
                ? " in " + buildEntityTypeAndName(resize.getTarget(), topology)
                : "";
        sb.append(resizeExplanation).append(targetClause);
        getScalingGroupExplanation(action.getExplanation(), sb.toString()).ifPresent(sb::append);
        return sb.toString();
    }

    /**
     * Build RESIZE explanation for actions whose risk is due to the related actions.
     * <p> Example: For Namespace resize actions that resize up VCPU quota,
     * the explanation is that the resize is recommended due to the congestion in the VCPUs for containers
     * in that namespace. The risk explanation is as follows:
     * 'VCPU congestion in Related Workload Controller'
     *
     * @param action            ActionDTO for the resize action
     * @param relatedActions    List of {@link ActionDTO.RelatedAction}s
     * @return                  Explanation string
     */
    private static String buildResizeCoreExplanationUsingRelatedActions(@Nonnull ActionDTO.Action action,
                                                                        @Nonnull List<ActionDTO.RelatedAction> relatedActions) {

        Map<CommodityType, List<EntityType>> commToEntityTypes = new HashMap<>();

        relatedActions.stream()
                .filter(ra -> ra.hasBlockingRelation())
                .forEach(ra -> {
                    CommodityType commType = ra.getBlockingRelation().getResize().getCommodityType();
                    EntityType entityType = EntityType.forNumber(ra.getActionEntity().getType());
                    commToEntityTypes.computeIfAbsent(commType, v -> new ArrayList<>()).add(entityType);
                });

        // If commodity types from Related actions are unavailable for any reason, then default to
        // creating the resize explanation using the primary action.
        // So for the namespace action, the explanation will be 'VCPU Limit Quota Congestion'
        // instead of 'VCPU congestion in Related Workload Controller'
        if (commToEntityTypes.isEmpty()) {
            logger.warn("{}::{} : Invalid related action data, cannot find impacted commodities {}",
                    ApiEntityType.fromType(action.getInfo().getResize().getTarget().getType()).displayName(),
                    action.getInfo().getResize().getTarget().getId(), relatedActions);
           return buildResizeCoreExplanation(action, true);
        }

        CommodityType comm = commToEntityTypes.keySet().stream().findFirst().get();
        String commodityType = ActionDTOUtil.getAtomicResizeCommodityDisplayName(comm);

        Set<String> entityTypeNames = commToEntityTypes.get(comm).stream()
                    .map(entityType -> ApiEntityType.fromType(entityType.getNumber()).displayName())
                    .collect(Collectors.toSet());

        final boolean isResizeDown = action.getInfo().getResize().getOldCapacity() >
                    action.getInfo().getResize().getNewCapacity();
        if (isResizeDown) {
            return UNDERUTILIZED_EXPLANATION + commodityType + " in Related "
                        +  Strings.join(entityTypeNames, ',');
        } else {
            return commodityType + CONGESTION_EXPLANATION + " in Related "
                        +  Strings.join(entityTypeNames, ',');
        }
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
        final String commodityType = convertStorageAccessToIops.apply(commodityDisplayName(resize.getCommodityType(), keepItShort)) +
            (resize.getCommodityAttribute() == CommodityAttribute.RESERVED ? " reservation" : "");

        // now modeling this behavior after ActionGeneratorImpl.notifyRightSize() in classic.
        // NOT addressing special cases for: Ready Queue, Reserved Instance, Cloud Template, Fabric
        // Interconnect, VStorage, VCPU. If we really need those, we can add them in as necessary.
        final boolean isResizeDown = action.getInfo().getResize().getOldCapacity() >
            action.getInfo().getResize().getNewCapacity();
        if (isResizeDown) {
            return UNDERUTILIZED_EXPLANATION + commodityType;
        } else {
            // if reason present in explanation use it.
            if (action.getExplanation().getResize().hasReason()) {
                final String reasonCommodityType = convertStorageAccessToIops.apply(
                        commodityDisplayName(action.getExplanation().getResize().getReason(), keepItShort)) +
                        (resize.getCommodityAttribute() == CommodityAttribute.RESERVED ? " reservation" : "");
                return reasonCommodityType + CONGESTION_EXPLANATION;
            }
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
        if (action.getInfo().getDelete().getTarget().getEnvironmentType()
                == EnvironmentType.CLOUD) {
            // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
            //  can remove APPLICATION_COMPONENT case when legacy support not needed
            int deletionTargetEntityType = action.getInfo().getDelete().getTarget().getType();
            if (EntityType.VIRTUAL_MACHINE_SPEC_VALUE == deletionTargetEntityType ||
                    EntityType.APPLICATION_COMPONENT_VALUE == deletionTargetEntityType) {
                return DELETE_WASTED_AZURE_APP_SERVICE_PLANS_EXPLANATION;
            } else {
                return DELETE_WASTED_VOLUMES_EXPLANATION;
            }
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
        final double coverageIncrease = (buyRI.getCoveredAverageDemand() / buyRI.getTotalAverageDemand()) * 100;
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
     * When there are daemons,
     * e.g. suspend daemon on suspended providerDisplayName.
     *
     * @param action the action to explain
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     *
     * @return the explanation sentence
     */
    private static String buildDeactivateExplanation(@Nonnull final ActionDTO.Action action,
                                                     @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology) {
        final Explanation explanation = action.getExplanation();
        final Explanation.DeactivateExplanation exp = explanation.getDeactivate();
        if (exp.hasReasonEntity()) {
            return String.format("Suspend %s on suspended %s",
                    buildEntityNameOrReturnDefault(action.getInfo().getDeactivate().getTarget().getId(), "provider", topology),
                    buildEntityNameOrReturnDefault(exp.getReasonEntity(), "provider", topology));
        } else {
            return DEACTIVATE_EXPLANATION;
        }
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
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static String buildReconfigureExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
            @Nullable final Map<Long, String> vcpuScalingActionsPolicyIdToName,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            final boolean keepItShort) {
        if (keepItShort) {
            return action.getInfo().getReconfigure().getSettingChangeCount() > 0 ?
                    RECONFIGURE_SETTING_CHANGE_CATEGORY : RECONFIGURE_EXPLANATION_CATEGORY;
        }

        final Explanation explanation = action.getExplanation();
        final ReconfigureExplanation reconfigExplanation = explanation.getReconfigure();
        final StringBuilder sb = new StringBuilder();
        if (!reconfigExplanation.getReasonSettingsList().isEmpty()) {
            sb.append(ActionDTOUtil.TRANSLATION_PREFIX)
                .append(buildReasonSettingsExplanation(action.getInfo(), action.getInfo().getReconfigure().getTarget(),
                    reconfigExplanation.getReasonSettingsList(),
                    settingPolicyIdToSettingPolicyName, vcpuScalingActionsPolicyIdToName, topology, false));
        } else {
            sb.append(buildReconfigureReasonCommodityExplanation(
                reconfigExplanation.getReconfigureCommodityList(), action.getInfo().getReconfigure().getTarget().getType()));
        }
        getScalingGroupExplanation(action.getExplanation(), sb.toString()).ifPresent(sb::append);
        return sb.toString();
    }


    /**
     * Build reconfigure explanation due to reason commodities.
     *
     * e.g. full explanation:
     *      "Enable supplier to offer requested resource(s) Ballooning, Network Commodity test_network"
     *
     * @param reasonCommodities a list of missing reason commodities
     * @return the explanation sentence
     */
    private static String buildReconfigureReasonCommodityExplanation(
            @Nonnull final List<ReasonCommodity> reasonCommodities,
            @Nonnull final Integer targetType) {

        if (targetType.equals(EntityType.CONTAINER_POD_VALUE)) {
            final Map<Integer, Set<String>> mapCommodityTypeReason =
                    categorizeContainerPodCommodityReasons(reasonCommodities);
            if (mapCommodityTypeReason.size() == 1) {
                Map<String, String> actionExplanationMap =
                        podActionExplanations.get(mapCommodityTypeReason.keySet().iterator().next());
                return actionExplanationMap.get(RECONFIGURE_ACTION);
            } else if (mapCommodityTypeReason.size() > 1) {
                return RECONFIGURE_REASON_COMMODITY_EXPLANATION_FOR_CONTAINER_POD;
            }
        }

        if (FeatureFlags.ENABLE_RECONFIGURE_ACTION_FOR_NOTREADY_NODE.isEnabled()
                && targetType.equals(EntityType.VIRTUAL_MACHINE_VALUE)
                && reasonCommodities.stream()
                        .map(ReasonCommodity::getCommodityType)
                        .allMatch(comm -> comm.getType() == CommodityDTO.CommodityType.CLUSTER_VALUE
                                && comm.getKey().contains("NotReady"))) {
            return RECONFIGURE_REASON_NODE_NOT_READY;
        }

        return RECONFIGURE_REASON_COMMODITY_EXPLANATION + reasonCommodities.stream()
                .map(reason -> getCommodityDisplayName(reason.getCommodityType()))
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
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort generate short explanation or not
     * @return the explanation sentence
     */
    private static String buildReasonSettingsExplanation(
                                                         @Nonnull final ActionEntity target,
                                                         @Nonnull final List<Long> reasonSettings,
                                                         @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
                                                         @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
                                                         final boolean keepItShort) {
        return buildReasonSettingsExplanation(null, target, reasonSettings, settingPolicyIdToSettingPolicyName, null, topology, keepItShort);
    }

    private static String buildReasonSettingsExplanation(@Nullable final ActionInfo actionInfo,
                                                         @Nonnull final ActionEntity target,
                                                         @Nonnull final List<Long> reasonSettings,
                                                         @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
                                                         @Nullable final Map<Long, String> vcpuScalingActionsPolicyIdToName,
                                                         @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
                                                         final boolean keepItShort) {
        if (keepItShort) {
            return REASON_SETTING_EXPLANATION_CATEGORY;
        }

        return evaluateAndFormatReasonSettingsExplanation(actionInfo, target, reasonSettings,
                settingPolicyIdToSettingPolicyName, vcpuScalingActionsPolicyIdToName, topology);
    }

    /**
     * Evaluate and format reason settings explanation.
     *
     * @param target the target entity
     * @param reasonSettings a list of settingPolicyIds that causes this action
     * @param settingPolicyIdToSettingPolicyName a map from settingPolicyId to settingPolicyName
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @return the explanation sentence
     */
    private static String evaluateAndFormatReasonSettingsExplanation(@Nullable final ActionInfo actionInfo,
                                                                     @Nonnull final ActionEntity target,
                                                                     @Nonnull final List<Long> reasonSettings,
                                                                     @Nonnull final Map<Long, String> settingPolicyIdToSettingPolicyName,
                                                                     @Nullable final Map<Long, String> vcpuScalingActionsPolicyIdToName,
                                                                     @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology) {
        Set policyNamesForAction = new HashSet<>();
        final int numAllPoliciesForAction = reasonSettings.size();
        int numNonDeletedPoliciesForAction = 0;
        for (Long policyId : reasonSettings) {
            final String policyName = settingPolicyIdToSettingPolicyName.get(policyId);
            if (policyName != null) {
                numNonDeletedPoliciesForAction++;
                policyNamesForAction.add(policyName);
            }
        }
        final int numDeletedPoliciesForAction = numAllPoliciesForAction
                                                - numNonDeletedPoliciesForAction;

        StringBuilder explanation = new StringBuilder();
        if (!policyNamesForAction.isEmpty()) {
            explanation.append(policyNamesForAction.stream().collect(Collectors.joining(", ")));
        }
        final String entityInformation = buildEntityNameOrType(target, topology);
        if (numDeletedPoliciesForAction == 1) { // as in the case of a now deleted policy,
            // that was instrumental in the action recommendation.
            if (numDeletedPoliciesForAction < numAllPoliciesForAction) {
                explanation.append(", " + SINGLE_DELETED_POLICY_MSG);
            } else {
                explanation.append(SINGLE_DELETED_POLICY_MSG);
            }
            return MessageFormat.format(REASON_SETTINGS_EXPLANATION,
                                        entityInformation, explanation.toString());

        } else if (numDeletedPoliciesForAction > 1) { // as in the case of a now deleted policies,
            // that was instrumental in the action recommendation.
            if (numDeletedPoliciesForAction < numAllPoliciesForAction) {
                explanation.append(", " + MULTIPLE_DELETED_POLICY_MSG);
            } else { // all policies deleted
                explanation.append(MULTIPLE_DELETED_POLICY_MSG);
            }
            return MessageFormat.format(REASON_SETTINGS_EXPLANATION,
                                        entityInformation, explanation.toString());
        } else {
            // Here the same check is being made as ActionTranslator, but they serve different purposes
            // The check here is specifically so that any actions that generate BOTH cloud and on prem
            // Actions will not result in cloud actions to generate this specific risk description
            if (vcpuScalingActionsPolicyIdToName != null
                    && !vcpuScalingActionsPolicyIdToName.isEmpty()
                    && target.getEnvironmentType() != EnvironmentType.CLOUD
                    && target.getType() == EntityType.VIRTUAL_MACHINE_VALUE
                    && actionInfo.hasReconfigure()
                    && actionInfo.getReconfigure().getSettingChangeCount() > 0) {
                String vcpuScalingSettingString = reasonSettings.stream().map(
                        vcpuScalingActionsPolicyIdToName::get).filter(Objects::nonNull).collect(Collectors.joining(", "));
                return MessageFormat.format(REASON_SETTINGS_VCPU_RECONFIG_EXPLANATION,
                        vcpuScalingSettingString);
            }
            String reasonSettingsString = reasonSettings.stream().map(
                    settingPolicyIdToSettingPolicyName::get).collect(Collectors.joining(", "));
            return MessageFormat.format(REASON_SETTINGS_EXPLANATION, entityInformation,
                    reasonSettingsString);
        }
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
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @param keepItShort compose a short explanation if true
     * @return the explanation sentence
     */
    private static Set<String> buildProvisionExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology,
            final boolean keepItShort) {
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
                        buildEntityNameOrType(action.getInfo().getProvision().getEntityToClone(),
                            topology) + "'");
                }
            case PROVISION_BY_SUPPLY_EXPLANATION:
                Optional<ActionGraphEntity> entity = topology.flatMap(topo -> topo.getEntity(action.getInfo().getProvision().getEntityToClone().getId()));
                return Collections.singleton(buildProvisionBySupplyExplanation(action, provisionExplanation, keepItShort, topology));
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
     * @param action action to compose explanation for.
     * @param provisionExplanation provision explanation.
     * @param keepItShort compose a short explanation if true.
     * @param topology containing all the traders.
     * @return the explanation sentence
     */
    private static String buildProvisionBySupplyExplanation(
            @Nonnull final ActionDTO.Action action,
            @Nonnull final ProvisionExplanation provisionExplanation, final boolean keepItShort,
            @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology) {
        if (provisionExplanation.hasReasonEntity()) {
            return String.format("Clone %s on cloned %s",
                    buildEntityNameOrReturnDefault(action.getInfo().getProvision().getEntityToClone().getId(), "entity", topology),
                    buildEntityNameOrReturnDefault(provisionExplanation.getReasonEntity(), "provider", topology));
        } else {
            return commodityDisplayName(provisionExplanation.getProvisionBySupplyExplanation()
                    .getMostExpensiveCommodityInfo().getCommodityType(), keepItShort) + CONGESTION_EXPLANATION;
        }
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
        if (reasonCommodity.hasSuffix()) {
            commodityDisplayName += " " + StringUtils.capitalize(reasonCommodity.getSuffix().name().toLowerCase());
        }
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

    /**
     * Given an {@link ActionEntity}, create a translation fragment that shows the entity type and name.
     * <p/>
     * e.g. For a VM named "Bill", create a fragment that would translate to "Virtual Machine Bill".
     * <p/>
     * If the topology is present and the entity is in it, we directly insert the entity name via lookup.
     * Otherwise we insert a format string for later substitution when the entity information is available.
     *
     * @param entity an {@link ActionEntity}
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @return the translation
     */
    private static String buildEntityTypeAndName(@Nonnull final ActionEntity entity,
                                                @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology) {
        return topology
            .flatMap(topo -> topo.getEntity(entity.getId()))
            .map(e -> ActionDTOUtil.upperUnderScoreToMixedSpaces(EntityType.forNumber(entity.getType()).name())
                + " " + e.getDisplayName())
            .orElse(ActionDTOUtil.buildEntityTypeAndName(entity));
    }

    /**
     * Given an {@link ActionEntity}, create a translation fragment that shows the entity name, if
     * available in the topology, otherwise return the default value passed.
     *
     * @param entityOid OID of entity
     * @param defaultName The default name to use if the entity is not available.
     * @param topology A minimal topology graph containing the relevant topology.
     *      *                 May be empty if no relevant topology is available.
     * @return the translation
     */
    private static String buildEntityNameOrReturnDefault(final long entityOid,
                                                         @Nonnull final String defaultName,
                                                         @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology) {
        return topology
                .flatMap(topo -> topo.getEntity(entityOid))
                .map(BaseGraphEntity::getDisplayName)
                .orElse(defaultName);
    }

    /**
     * Given an {@link ActionEntity}, create a translation fragment that shows the entity name, if
     * available, otherwise will show the entity type if for some reason the entity cannot be found
     * when the text is translated.
     * <p/>
     * If the topology is present and the entity is in it, we directly insert the entity name via lookup.
     * Otherwise we insert a format string for later substitution when the entity information is available.
     *
     * @param entity an {@link ActionEntity}
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @return the translation
     */
    private static String buildEntityNameOrType(ActionEntity entity,
                                               @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology) {
        return topology
            .flatMap(topo -> topo.getEntity(entity.getId()))
            .map(BaseGraphEntity::getDisplayName)
            .orElse(ActionDTOUtil.buildEntityNameOrType(entity));
    }

    /**
     * Given an {@link ActionEntity}, create a translation fragment that shows the entity name, if
     * available in the topology, otherwise will build a translation block for later translation
     * of the name in the API component.
     *
     * @param entityOid OID of entity
     * @param defaultName The default name to use if the entity is not available.
     * @param topology A minimal topology graph containing the relevant topology.
     *                 May be empty if no relevant topology is available.
     * @return the translation
     */
    private static String buildNameBlock(final long entityOid,
                                         @Nonnull final String defaultName,
                                         @Nonnull final Optional<TopologyGraph<ActionGraphEntity>> topology) {
        return topology
            .flatMap(topo -> topo.getEntity(entityOid))
            .map(BaseGraphEntity::getDisplayName)
            .orElse(ActionDTOUtil.createTranslationBlock(entityOid, EntityField.DISPLAY_NAME, defaultName));
    }
}
