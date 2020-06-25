package com.vmturbo.topology.processor.group.policy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.PolicyChange;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.BindToComplementaryGroupPolicy;
import com.vmturbo.topology.processor.group.policy.application.BindToGroupPolicy;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicy;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory;

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
    private final PolicyServiceBlockingStub policyService;

    private final GroupServiceBlockingStub groupServiceBlockingStub;

    /**
     * The factory to use to create policies.
     */
    private final PolicyFactory policyFactory;

    private final PolicyApplicator policyApplicator;

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
                         @Nonnull final PolicyApplicator policyApplicator) {
        this.policyService = Objects.requireNonNull(policyService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.policyFactory = Objects.requireNonNull(policyFactory);
        this.policyApplicator = Objects.requireNonNull(policyApplicator);
    }

    /**
     * Handler for policies that reference invalid groups.
     */
    @FunctionalInterface
    private interface InvalidGroupHandler {
        void policyWithMissingGroups(@Nonnull Policy policy, @Nonnull Set<Long> missingGroups);
    }

    private int clearPoliciesWithMissingGroups(@Nonnull final List<Policy> policies,
                                               @Nonnull final Map<Long, Grouping> existingGroupsById,
                                               @Nonnull final InvalidGroupHandler invalidGroupHandler) {
        final MutableInt numRemoved = new MutableInt(0);
        policies.removeIf(policy -> {
            final Set<Long> policyGroups = GroupProtoUtil.getPolicyGroupIds(policy);
            if (!existingGroupsById.keySet().containsAll(policyGroups)) {
                final Set<Long> missingGroups = Sets.difference(policyGroups, existingGroupsById.keySet());
                invalidGroupHandler.policyWithMissingGroups(policy, missingGroups);
                numRemoved.increment();
                return true;
            }
            return false;
        });
        return numRemoved.intValue();
    }

    /**
     * Apply policies from the policy service, possibly modified by plan changes,
     * to the entities in the topology graph.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param groupResolver The resolver for the groups that the policy applies to.
     * @param changes list of plan changes to be applied to the policies
     * @param topologyInfo Information about the topology under construction.
     * @return Map from (type of policy) -> (num of policies of the type)
     */
    public PolicyApplicator.Results applyPolicies(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                                  @Nonnull final GroupResolver groupResolver,
                                                  @Nonnull final List<ScenarioChange> changes,
                                                  @Nonnull final TopologyInfo topologyInfo) {
        try (DataMetricTimer timer = POLICY_APPLICATION_SUMMARY.startTimer()) {
            final long startTime = System.currentTimeMillis();
            final List<Policy> livePolicies = new ArrayList<>();
            policyService.getPolicies(PolicyRequest.getDefaultInstance())
                .forEachRemaining(policyResp -> {
                    if (policyResp.hasPolicy()) {
                        livePolicies.add(policyResp.getPolicy());
                    }
                });
            final List<Policy> planOnlyPolicies = planOnlyPolicies(changes);
            final Map<Long, Grouping> groupsById = policyGroupsById(Stream.concat(livePolicies.stream(), planOnlyPolicies.stream()));

            int invalidPolicyCount = 0;
            invalidPolicyCount += clearPoliciesWithMissingGroups(livePolicies, groupsById, (policy, missingGroups) -> {
                if (policy.hasTargetId()) {
                    // TODO (roman, Feb 3 2020) OM-55089: Mark entities discovered by the target non-controllable.
                    throw new IllegalStateException(String.format(
                        "Policy %d (%s) discovered by target %d refers to non-existent groups %s.",
                        policy.getId(), policy.getPolicyInfo().getName(), policy.getTargetId(), missingGroups));
                } else {
                    logger.warn("User policy {} ({}) refers to non-existent groups {}." +
                        " Not applying the policy.", policy.getId(), policy.getPolicyInfo().getName(), missingGroups);
                }
            });

            invalidPolicyCount += clearPoliciesWithMissingGroups(planOnlyPolicies, groupsById, (policy, missingGroups) -> {
                logger.warn("Plan-only policy {} ({}) refers to non-existent groups {}." +
                    " Not applying the policy.", policy.getId(), policy.getPolicyInfo().getName(), missingGroups);
            });

            final Map<PolicyDetailCase, Integer> policyTypeCounts = Maps.newEnumMap(PolicyDetailCase.class);


            final List<PlacementPolicy> policiesToApply =
                new ArrayList<>();

            getServerPolicies(changes, livePolicies, groupsById)
                .forEach(policiesToApply::add);

            getPlanOnlyPolicies(planOnlyPolicies, groupsById)
                .forEach(policiesToApply::add);

            final PolicyApplicator.Results results =
                policyApplicator.applyPolicies(policiesToApply, groupResolver, graph);

            final long durationMs = System.currentTimeMillis() - startTime;
            logger.info("Completed application of {} policies in {}ms.", policyTypeCounts.size(), durationMs);

            if (!results.getErrors().isEmpty()) {
                logger.error(results.getErrors().size() + " policies could not be applied " +
                    "(error messages printed at debug level).");
                logger.debug(() -> results.getErrors().entrySet().stream()
                    .map(entry -> entry.getKey().getPolicyDefinition().getId() + " : " + entry.getValue().getMessage())
                    .collect(Collectors.joining("\n")));
            }

            results.addInvalidPolicyCount(invalidPolicyCount);

            return results;
        }
    }

    /**
     * Get the commodityType associated with every placement policy.
     *
     * @param graph         The topology graph on which to apply the policies.
     * @param groupResolver The resolver for the groups that the policy applies to.
     * @return map from policy id to the commodityType.
     */
    public Table<Long, Integer, TopologyDTO.CommodityType> getPlacementPolicyIdToCommodityType(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final GroupResolver groupResolver) {

        final List<Policy> livePolicies = new ArrayList<>();
        policyService.getAllPolicies(PolicyRequest.getDefaultInstance())
                .forEachRemaining(policyResp -> {
                    if (policyResp.hasPolicy()) {
                        livePolicies.add(policyResp.getPolicy());
                    }
                });
        final Map<Long, Grouping> groupsById = policyGroupsById(livePolicies.stream());
        clearPoliciesWithMissingGroups(livePolicies, groupsById, (policy, missingGroups) -> {
        });

        final List<PlacementPolicy> policiesToApply =
                new ArrayList<>();

        getServerPolicies(new ArrayList<>(), livePolicies, groupsById)
                .forEach(policiesToApply::add);

        Table<Long, Integer, TopologyDTO.CommodityType> results = HashBasedTable.create();
        policiesToApply.stream().filter(policy -> policy instanceof BindToGroupPolicy
                || policy instanceof BindToComplementaryGroupPolicy).forEach(policy ->

                        results.put(policy
                            .getPolicyDefinition().getId(),
                                getProviderType(policy),
                    policyApplicator
                            .commodityTypeAssociatedWithPlacementPolicy(policy,
                                    groupResolver, graph)));

        return results;
    }


    private Map<Long, Grouping> policyGroupsById(@Nonnull final Stream<Policy> policies) {
        final Set<Long> groupIds = policies.map(GroupProtoUtil::getPolicyGroupIds)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

        final Map<Long, Grouping> groupsById = new HashMap<>(groupIds.size());
        if (!groupIds.isEmpty()) {
            groupServiceBlockingStub.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(
                            GroupFilter.newBuilder().addAllId(groupIds).setIncludeHidden(true))
                    .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                    .build()).forEachRemaining(group -> groupsById.put(group.getId(), group));

            if (groupsById.size() != groupIds.size()) {
                logger.warn("The following groups are missing from the groups service. " +
                    "Policies involving those groups will not apply: {}", Sets.difference(groupIds, groupsById.keySet()));
            }
        }
        return groupsById;
    }

    /**
     * Get the procvider type associated with the placement policy. Should only be called
     * for BindToGroupPolicy and BindToComplementaryGroupPolicy.
     * @param placementPolicy placement policy of interest
     * @return the Entity type of the provider group of this policy.
     */
    private int getProviderType(PlacementPolicy placementPolicy) {
        Grouping providerGroup;
        if (placementPolicy instanceof BindToGroupPolicy) {
            BindToGroupPolicy policy = (BindToGroupPolicy)placementPolicy;
            providerGroup =  policy.getProviderPolicyEntities().getGroup();
        } else if  (placementPolicy instanceof BindToComplementaryGroupPolicy) {
            BindToComplementaryGroupPolicy policy = (BindToComplementaryGroupPolicy)placementPolicy;
            providerGroup = policy.getProviderPolicyEntities().getGroup();
        } else {
            // should never reach here. The method should be called only for BindToGroupPolicy
            // and BindToComplementaryGroupPolicy policy.
            return 0;
        }
        GroupProtoUtil.checkEntityTypeForPolicy(providerGroup);
        // Resolve the relevant groups
        final ApiEntityType providerEntityType = GroupProtoUtil.getEntityTypes(providerGroup).iterator().next();
        return providerEntityType.typeNumber();

    }


    private Stream<PlacementPolicy> getServerPolicies(
                    @Nonnull List<ScenarioChange> changes,
                    @Nonnull List<Policy> livePolicies,
                    @Nonnull Map<Long, Grouping> groupsById) {
        // Map from policy ID to whether it is enabled or disabled in the plan
        Map<Long, Boolean> policyOverrides = changes.stream()
                .filter(ScenarioChange::hasPlanChanges)
                .filter(change -> change.getPlanChanges().hasPolicyChange())
                .map(change -> change.getPlanChanges().getPolicyChange())
                .filter(PolicyChange::hasPolicyId)
                .collect(Collectors.toMap(
                        PolicyChange::getPolicyId, PolicyChange::getEnabled));

        return livePolicies.stream()
            .map(policyProto -> {
                PlacementPolicy policy = policyFactory.newPolicy(policyProto, groupsById,
                        Collections.emptySet(),
                    Collections.emptySet());
                final Policy policyDef = policy.getPolicyDefinition();
                final long policyId = policyDef.getId();
                final Boolean enabledOverride = policyOverrides.get(policyId);
                if (enabledOverride != null) {
                    final Boolean enabled = policyDef.getPolicyInfo().getEnabled();
                    if (enabled != enabledOverride) {
                        // Create a copy of the policy with the 'enable' field taken from the plan
                        final Policy altPolicyDef = policyDef.toBuilder()
                            .setPolicyInfo(policyDef.getPolicyInfo().toBuilder()
                                .setEnabled(enabledOverride))
                            .build();
                        policy = policyFactory.newPolicy(altPolicyDef, groupsById,
                                Collections.emptySet(),
                            Collections.emptySet());
                    }
                }
                return policy;
            });
    }

    private Stream<PlacementPolicy> getPlanOnlyPolicies(
                    @Nonnull List<Policy> planOnlyPolicies,
                    @Nonnull Map<Long, Grouping> groupsById) {
        return planOnlyPolicies.stream()
            .map(policyDefinition -> {
                // Policy definition needs an ID, but plan-only policies don't have one
                policyDefinition = Policy.newBuilder(policyDefinition).setId(0).build();
                return policyFactory.newPolicy(policyDefinition, groupsById,
                    Collections.emptySet(), Collections.emptySet());
            });
    }

    private List<Policy> planOnlyPolicies(@Nonnull final List<ScenarioChange> changes) {
        return changes.stream()
            .filter(ScenarioChange::hasPlanChanges)
            .filter(change -> change.getPlanChanges().hasPolicyChange())
            .map(change -> change.getPlanChanges().getPolicyChange())
            .filter(PolicyChange::hasPlanOnlyPolicy)
            .map(PolicyChange::getPlanOnlyPolicy)
            .collect(Collectors.toList());
    }
}
