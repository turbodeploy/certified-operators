package com.vmturbo.topology.processor.group.policy;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.PolicyDetailCase;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.InitialPlacementConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.PolicyChange;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Responsible for the application of policies that affect the operation of the market.
 *
 * Policies are enforced by placing commodities on entities that control the behavior
 * of the affected entities.
 */
public class PolicyManager {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The policy service from which to retrieve policy definitions.
     */
    final PolicyServiceBlockingStub policyService;

    final GroupServiceBlockingStub groupServiceBlockingStub;

    /**
     * The factory to use to create policies.
     */
    final PolicyFactory policyFactory;

    /**
     * The factory to create policy for initial placement purpose.
     */
    final InitialPlacementPolicyFactory initialPlacementPolicyFactory;

    private static final DataMetricSummary POLICY_APPLICATION_SUMMARY = DataMetricSummary.builder()
        .withName("tp_policy_application_duration_seconds")
        .withHelp("Duration of applying policies to a topology.")
        .build()
        .register();

    /**
     * Create a new policy manager.
     *
     * @param policyService The service to use to retrieve policy definitions.
     * @param groupServiceBlockingStub Stub to access the remote group service.
     * @param policyFactory The factory used to create policies.
     */
    public PolicyManager(@Nonnull final PolicyServiceBlockingStub policyService,
                         @Nonnull final GroupServiceBlockingStub groupServiceBlockingStub,
                         @Nonnull final PolicyFactory policyFactory,
                         @Nonnull final InitialPlacementPolicyFactory initialPlacementPolicyFactory) {
        this.policyService = Objects.requireNonNull(policyService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.policyFactory = Objects.requireNonNull(policyFactory);
        this.initialPlacementPolicyFactory = Objects.requireNonNull(initialPlacementPolicyFactory);
    }

    /**
     * Apply policies from the policy service to the entities in the topology graph.
     * This is used in the realtime pipeline.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param groupResolver The resolver for the groups that the policy applies to.
     */
    public void applyPolicies(@Nonnull final TopologyGraph graph,
                    @Nonnull final GroupResolver groupResolver) {
        applyPolicies(graph, groupResolver, Collections.emptyList());
    }

    /**
     * Apply policies from the policy service, possibly modified by plan changes,
     * to the entities in the topology graph.
     *
     * TODO: This is a stub implementation that only exists right now to exercise
     * group resolution for policy-groups.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param groupResolver The resolver for the groups that the policy applies to.
     * @param changes list of plan changes to be applied to the policies
     */
    public void applyPolicies(@Nonnull final TopologyGraph graph,
                              @Nonnull final GroupResolver groupResolver,
                              List<ScenarioChange> changes) {
        try (DataMetricTimer timer = POLICY_APPLICATION_SUMMARY.startTimer()) {
            final long startTime = System.currentTimeMillis();

            final Iterator<PolicyResponse> policyIter =
                    policyService.getAllPolicies(PolicyRequest.newBuilder().build());
            final ImmutableList<PolicyResponse> policyResponses = ImmutableList.copyOf(policyIter);
            List<Policy> planOnlyPolicies = planOnlyPolicies(changes);
            final Map<Long, Group> groupsById = groupsById(policyResponses, planOnlyPolicies);
            final Map<PolicyDetailCase, Integer> policyTypeCounts = Maps.newEnumMap(PolicyDetailCase.class);

            applyServerPolicies(graph, groupResolver, changes,
                            policyResponses, groupsById, policyTypeCounts);
            applyPlanOnlyPolicies(graph, groupResolver, planOnlyPolicies,
                            groupsById, policyTypeCounts);

            applyPolicyForInitialPlacement(graph, groupResolver, changes, policyTypeCounts);

            final long durationMs = System.currentTimeMillis() - startTime;
            logger.info("Completed application of {} policies in {}ms.", policyTypeCounts, durationMs);
        }
    }

    private Map<Long, Group> policyGroupsById(Collection<Policy> policies) {
        Set<Long> groupIds = policies.stream()
                        .map(GroupProtoUtil::getPolicyGroupIds)
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());

        final Map<Long, Group> groupsById = new HashMap<>(groupIds.size());
        if (!groupIds.isEmpty()) {
            groupServiceBlockingStub.getGroups(GetGroupsRequest.newBuilder()
                .addAllId(groupIds)
                .build())
            .forEachRemaining(group -> groupsById.put(group.getId(), group));

            if (groupsById.size() != groupIds.size()) {
                // Some desired groups are not found.
                // Throw an exception for now.
                // TODO (roman, Oct 20 2017): We can just not apply the policies that
                // are missing groups.
                throw new IllegalStateException("Policies have non-existing groups.");
            }
        }
        return groupsById;
    }

    /**
     * Obtain all {@link Group}s referenced by policies.
     *
     * @param policyResponses policies persisted in the policy service.
     * @param otherPolicies other policies, such as plan-only policies, that are not
     * persisted in the policy service.
     * @return mapping from group oid to group
     */
    private Map<Long, Group> groupsById(List<PolicyResponse> policyResponses, List<Policy> otherPolicies) {
        Set<Policy> policies = policyResponses.stream()
                        .filter(PolicyResponse::hasPolicy)
                        .map(PolicyResponse::getPolicy)
                        .collect(Collectors.toSet());
        policies.addAll(otherPolicies);
        return policyGroupsById(policies);
    }

    private void applyServerPolicies(
                    @Nonnull final TopologyGraph graph,
                    @Nonnull final GroupResolver groupResolver,
                    @Nonnull List<ScenarioChange> changes,
                    @Nonnull List<PolicyResponse> policyResponses,
                    @Nonnull Map<Long, Group> groupsById,
                    @Nonnull Map<PolicyDetailCase, Integer> policyTypeCounts
                    ) {
        // Map from policy ID to whether it is enabled or disabled in the plan
        Map<Long, Boolean> policyOverrides = changes.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .filter(change -> change.getPlanChanges().hasPolicyChange())
                .map(change -> change.getPlanChanges().getPolicyChange())
                .filter(PolicyChange::hasPolicyId)
                .collect(Collectors.toMap(
                        PolicyChange::getPolicyId, PolicyChange::getEnabled));
        for (PolicyResponse response : policyResponses) {
            PlacementPolicy policy = policyFactory.newPolicy(response.getPolicy(), groupsById);
            long policyId = policy.getPolicyDefinition().getId();
            Boolean enabledOverride = policyOverrides.get(policyId);
            if (enabledOverride != null) {
                Boolean enabled = policy.getPolicyDefinition().getEnabled();
                if (enabled != enabledOverride) {
                    // Create a copy of the policy with the 'enable' field taken from the plan
                    Policy altPolicyDefinition =
                            Policy.newBuilder(policy.getPolicyDefinition())
                                .setEnabled(enabledOverride)
                                .build();
                    policy = policyFactory.newPolicy(altPolicyDefinition, groupsById);
                }
            }
            applyPolicy(groupResolver, policy, graph, policyTypeCounts);
        }
    }

    private void applyPlanOnlyPolicies(
                    @Nonnull final TopologyGraph graph,
                    @Nonnull final GroupResolver groupResolver,
                    @Nonnull List<Policy> planOnlyPolicies,
                    @Nonnull Map<Long, Group> groupsById,
                    @Nonnull Map<PolicyDetailCase, Integer> policyTypeCounts) {
        for (Policy policyDefinition : planOnlyPolicies) {
            // Policy definition needs an ID, but plan-only policies don't have one
            policyDefinition = Policy.newBuilder(policyDefinition).setId(0).build();
            PlacementPolicy policy = policyFactory.newPolicy(policyDefinition, groupsById);
            applyPolicy(groupResolver, policy, graph, policyTypeCounts);
        }
    }

    private List<Policy> planOnlyPolicies(List<ScenarioChange> changes) {
        return changes.stream()
                        .filter(ScenarioChange::hasPlanChanges)
                        .filter(change -> change.getPlanChanges().hasPolicyChange())
                        .map(change -> change.getPlanChanges().getPolicyChange())
                        .filter(PolicyChange::hasPlanOnlyPolicy)
                        .map(PolicyChange::getPlanOnlyPolicy)
                        .collect(Collectors.toList());
    }

    /**
     * Create and apply BindToGroup policy for initial placement. If there are InitialPlacementConstraints in
     * PlanChanges, it will call InitialPlacementPolicyFactory to create BindToGroup policy and apply
     * this policy.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param groupResolver The resolver for the groups that the policy applies to.
     * @param scenarioChanges list of plan changes to be applied to the policies.
     * @param policyTypeCounts policy type count map.
     */
    private void applyPolicyForInitialPlacement(@Nonnull final TopologyGraph graph,
                                                @Nonnull final GroupResolver groupResolver,
                                                @Nonnull final List<ScenarioChange> scenarioChanges,
                                                @Nonnull Map<PolicyDetailCase, Integer> policyTypeCounts) {
        final List<InitialPlacementConstraint> constraints = scenarioChanges.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .map(ScenarioChange::getPlanChanges)
                .map(PlanChanges::getInitialPlacementConstraintsList)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        if (!constraints.isEmpty()) {
            final PlacementPolicy policy =
                    initialPlacementPolicyFactory.generatePolicy(graph, constraints);
            applyPolicy(groupResolver, policy, graph, policyTypeCounts);
        }
    }

    /**
     * Apply a policy and handle any resulting errors.
     * TODO: What should we do when a policy cannot be applied? Is this sufficient ground to completely
     * cancel a broadcast?
     *
     * @param groupResolver The resolver for the groups that the policy applies to.
     * @param policy The policy to be applied.
     * @param topologyGraph The topology graph on which to apply the policies.
     * @param policyTypeCounts counts how many policies of each type were applied
     */
    private void applyPolicy(@Nonnull final GroupResolver groupResolver,
                             @Nonnull final PlacementPolicy policy,
                             @Nonnull final TopologyGraph topologyGraph,
                             @Nonnull Map<PolicyDetailCase, Integer> policyTypeCounts) {
        try {
            policy.apply(groupResolver, topologyGraph);
            final PolicyDetailCase policyType = policy.getPolicyDefinition().getPolicyDetailCase();
            int curCountOfType = policyTypeCounts.getOrDefault(policyType, 0);
            policyTypeCounts.put(policyType, curCountOfType + 1);
        } catch (GroupResolutionException e) {
            logger.error("Failed to resolve group: ", e);
        } catch (PolicyApplicationException e) {
            logger.error("Failed to apply policy: ", e);
        }
    }
}
