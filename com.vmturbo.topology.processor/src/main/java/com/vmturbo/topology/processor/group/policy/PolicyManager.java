package com.vmturbo.topology.processor.group.policy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy.PolicyDetailCase;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
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
                         @Nonnull final PolicyFactory policyFactory) {
        this.policyService = Objects.requireNonNull(policyService);
        this.groupServiceBlockingStub = Objects.requireNonNull(groupServiceBlockingStub);
        this.policyFactory = Objects.requireNonNull(policyFactory);
    }

    /**
     * Apply policies from the policy service to the entities in the topology graph.
     *
     * TODO: This is a stub implementation that only exists right now to exercise group resolution for policy-groups.
     *
     * @param graph The topology graph on which to apply the policies.
     * @param groupResolver The resolver for the groups that the policy applies to.
     */
    public void applyPolicies(@Nonnull final TopologyGraph graph,
                              @Nonnull final GroupResolver groupResolver) {
        try (DataMetricTimer timer = POLICY_APPLICATION_SUMMARY.startTimer()) {
            final long startTime = System.currentTimeMillis();

            final Iterator<PolicyResponse> policyIter =
                    policyService.getAllPolicies(PolicyRequest.newBuilder().build());
            final ImmutableList<PolicyResponse> policyResponses = ImmutableList.copyOf(policyIter);

            final Map<PolicyDetailCase, Integer> policyTypeMap = new HashMap<>();
            Set<Long> groupIds = policyResponses.stream()
                    .filter(PolicyResponse::hasPolicy)
                    .flatMap(resp -> GroupProtoUtil.getPolicyGroupIds(resp.getPolicy()).stream())
                    .collect(Collectors.toSet());

            final Map<Long, Group> groupsById = new HashMap<>(groupIds.size());
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
            for (PolicyResponse response : policyResponses) {
                PlacementPolicy policy = policyFactory.newPolicy(response.getPolicy(), groupsById);
                applyPolicy(groupResolver, policy, graph);

                final PolicyDetailCase policyType = response.getPolicy().getPolicyDetailCase();
                int curCountOfType = policyTypeMap.computeIfAbsent(policyType, pt -> 0);
                policyTypeMap.put(policyType, curCountOfType + 1);
            }

            final long durationMs = System.currentTimeMillis() - startTime;
            logger.info("Completed application of {} policies in {}ms.", policyTypeMap, durationMs);
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
     */
    private void applyPolicy(@Nonnull final GroupResolver groupResolver,
                             @Nonnull final PlacementPolicy policy,
                             @Nonnull final TopologyGraph topologyGraph) {
        try {
            policy.apply(groupResolver, topologyGraph);
        } catch (GroupResolutionException e) {
            logger.error("Failed to resolve group: ", e);
        } catch (PolicyApplicationException e) {
            logger.error("Failed to apply policy: ", e);
        }
    }
}
