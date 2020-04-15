package com.vmturbo.common.protobuf;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;

/**
 * Utilities for extracting information from plan-related protobufs
 * (in com.vmturbo.common.protobuf/.../protobuf/plan)
 */
public class PlanDTOUtil {

    /**
     * Plans involving these projects are "lightweight" and do not require persistence in the
     * various components of the system that save per-plan data.
     */
    public static final Set<PlanProjectType> NO_PERSISTENCE_PLAN_PROJECT = ImmutableSet.of(
        PlanProjectType.RESERVATION_PLAN,
        PlanProjectType.CLUSTER_HEADROOM);

    /**
     * Return whether or not a {@link TopologyInfo} describes a transient plan.
     * Transient plans are those that don't require saving anything to disk. The plan orchestrator
     * listens to get the necessary information and do the necessary post-processing directly.
     *
     * <p/>Components can use this method to decide whether or not to persist data for plans.
     *
     * @param topologyInfo The {@link TopologyInfo}.
     * @return True if the plan is transient.
     */
    public static boolean isTransientPlan(@Nonnull final TopologyInfo topologyInfo) {
        return NO_PERSISTENCE_PLAN_PROJECT.contains(topologyInfo.getPlanInfo().getPlanProjectType());
    }

    /**
     * Return the OIDs of entities involved in list of {@link ScenarioChange}.
     *
     * @param scenarioChanges The target scenario changes.
     * @return The set of involved entity IDs. This does not include groups, templates, etc.
     */
    @Nonnull
    public static Set<Long> getInvolvedEntities(
            @Nonnull final List<ScenarioChange> scenarioChanges) {
        return scenarioChanges.stream()
                .flatMap(change -> PlanDTOUtil.getInvolvedEntities(change).stream())
                .collect(Collectors.toSet());
    }

    /**
     * Return the OIDs of groups involved in a list of {@link ScenarioChange}s.
     *
     * @param scenarioChanges The target scenario changes.
     * @return The set of involved group IDs.
     */
    @Nonnull
    public static Set<Long> getInvolvedGroups(
            @Nonnull final List<ScenarioChange> scenarioChanges) {
        return scenarioChanges.stream()
                .flatMap(change -> PlanDTOUtil.getInvolvedGroups(change).stream())
                .collect(Collectors.toSet());
    }

    /**
     * Return the OIDs of groups involved in a {@link ScenarioChange}. This will include groups
     * referred to in topology addition / removal / replace / settings, max utilization and
     * utilization changes.
     *
     * @param scenarioChange The target {@link ScenarioChange}.
     * @return The set of involved group IDs.
     */
    @Nonnull
    public static Set<Long> getInvolvedGroups(@Nonnull final ScenarioChange scenarioChange) {
        final ImmutableSet.Builder<Long> groupBuilder = ImmutableSet.builder();
        if (scenarioChange.hasTopologyAddition() && scenarioChange.getTopologyAddition().hasGroupId()) {
            groupBuilder.add(scenarioChange.getTopologyAddition().getGroupId());
        } else if (scenarioChange.hasTopologyRemoval() && scenarioChange.getTopologyRemoval().hasGroupId()) {
            groupBuilder.add(scenarioChange.getTopologyRemoval().getGroupId());
        } else if (scenarioChange.hasTopologyReplace() && scenarioChange.getTopologyReplace().hasRemoveGroupId()) {
            groupBuilder.add(scenarioChange.getTopologyReplace().getRemoveGroupId());
        } else if (scenarioChange.hasSettingOverride() && scenarioChange.getSettingOverride().hasGroupOid()) {
            groupBuilder.add(scenarioChange.getSettingOverride().getGroupOid());
        } else if (scenarioChange.hasPlanChanges()) {
            // several of the plan change types do include group info, but we are only fetching
            // group details for max utilization at the moment.
            PlanChanges planChanges = scenarioChange.getPlanChanges();
            if (planChanges.getMaxUtilizationLevel().hasGroupOid()) {
                groupBuilder.add(planChanges.getMaxUtilizationLevel().getGroupOid());
            } else if (planChanges.getUtilizationLevel().hasGroupOid()) {
                groupBuilder.add(planChanges.getUtilizationLevel().getGroupOid());
            }

            groupBuilder.addAll(getInvolvedGroupsUUidsFromIgnoreConstraints(planChanges));
        }

        return groupBuilder.build();
    }

    /**
     * Return the OIDs of groups involved in {@link PlanChanges.IgnoreConstraint} list
     * as target group ids.
     *
     * @param planChanges
     * @return
     */
    @VisibleForTesting
    static Set<Long> getInvolvedGroupsUUidsFromIgnoreConstraints(PlanChanges planChanges) {
        if (CollectionUtils.isEmpty(planChanges.getIgnoreConstraintsList())) {
            return Collections.emptySet();
        }
        return planChanges.getIgnoreConstraintsList().stream()
            .filter(PlanChanges.IgnoreConstraint::hasIgnoreGroup)
            .map(PlanChanges.IgnoreConstraint::getIgnoreGroup)
            .map(PlanChanges.ConstraintGroup::getGroupUuid)
            .collect(Collectors.toSet());
    }

    /**
     * Return the OIDs of entities involved in a {@link ScenarioChange}.
     *
     * @param scenarioChange The target scenario change.
     * @return The set of involved entity IDs. This does not include groups, templates, etc.
     */
    @Nonnull
    public static Set<Long> getInvolvedEntities(@Nonnull final ScenarioChange scenarioChange) {
        ImmutableSet.Builder<Long> entitiesBuilder = ImmutableSet.builder();
        if (scenarioChange.hasTopologyAddition()) {
            if (scenarioChange.getTopologyAddition().hasEntityId()) {
                entitiesBuilder.add(scenarioChange.getTopologyAddition().getEntityId());
            }
        }

        if (scenarioChange.hasTopologyRemoval()) {
            if (scenarioChange.getTopologyRemoval().hasEntityId()) {
                entitiesBuilder.add(scenarioChange.getTopologyRemoval().getEntityId());
            }
        }

        if (scenarioChange.hasTopologyReplace()) {
            if (scenarioChange.getTopologyReplace().hasRemoveEntityId()) {
                entitiesBuilder.add(scenarioChange.getTopologyReplace().getRemoveEntityId());
            }
        }

        return entitiesBuilder.build();
    }

    public static Set<Long> getInvolvedTemplates(@Nonnull final List<ScenarioChange> scenarioChanges) {
        return scenarioChanges.stream()
            .flatMap(change -> PlanDTOUtil.getInvolvedTemplates(change).stream())
            .collect(Collectors.toSet());
    }

    /**
     * Return the OIDS of templates which are part of {@link ScenarioChange}. And the scenario change
     * could be Topology Addition or Topology Replace.
     *
     * @param scenarioChange The target scenario changes.
     * @return A set of involved template OIDs.
     */
    @Nonnull
    public static Set<Long> getInvolvedTemplates(@Nonnull final ScenarioChange scenarioChange) {
        ImmutableSet.Builder<Long> templatesBuilder = ImmutableSet.builder();
        if (scenarioChange.hasTopologyAddition()) {
            if (scenarioChange.getTopologyAddition().hasTemplateId()) {
                templatesBuilder.add(scenarioChange.getTopologyAddition().getTemplateId());
            }
        }

        if (scenarioChange.hasTopologyReplace()) {
            if (scenarioChange.getTopologyReplace().hasAddTemplateId()) {
                templatesBuilder.add(scenarioChange.getTopologyReplace().getAddTemplateId());
            }
        }
        return templatesBuilder.build();
    }

    /**
     * Return whether or not a {@link PlanStatus} is terminal (i.e. no more state transitions, the
     * plan is done running).
     *
     * @param planStatus The status of the plan.
     * @return True if the status is terminal. False otherwise.
     */
    public static boolean isTerminalStatus(@Nonnull final PlanStatus planStatus) {
        return planStatus == PlanStatus.SUCCEEDED ||
            planStatus == PlanStatus.FAILED || planStatus == PlanStatus.STOPPED;
    }
}
