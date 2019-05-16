package com.vmturbo.topology.processor.group.policy.application;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.mutable.MutableInt;
import org.immutables.value.Value;

import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicyApplication.PolicyApplicationResults;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Responsible for applying a list of {@link PlacementPolicy}s using the right
 * {@link PlacementPolicyApplication}s in the right order.
 */
public class PolicyApplicator {

    /**
     * The order in which to apply policies.
     */
    private static final LinkedHashSet<PolicyDetailCase> APPLICATION_ORDER;

    static {
        // Linked hash set will preserve order.
        APPLICATION_ORDER = new LinkedHashSet<>();
        // We want to apply merge policies before must-not-run-together policies, because
        // the must-not-run-together policies are affected by cluster/DC boundaries.
        APPLICATION_ORDER.add(PolicyDetailCase.MERGE);
        APPLICATION_ORDER.add(PolicyDetailCase.MUST_NOT_RUN_TOGETHER);
        // Add the rest in any order.
        // This won't add anything twice, because APPLICATION_ORDER is a set.
        APPLICATION_ORDER.addAll(Arrays.asList(PolicyDetailCase.values()));
    }

    private final PolicyFactory policyFactory;

    public PolicyApplicator(@Nonnull final PolicyFactory policyFactory) {
        this.policyFactory = policyFactory;
    }

    /**
     * Apply the provided placement policies in the required order.
     * Does not apply disabled policies.
     *
     * @param policies The policies to apply.
     * @param groupResolver The {@link GroupResolver} to use to resolve groups.
     * @param topologyGraph The {@link TopologyGraph} for the currently constructed topology.
     * @return A {@link Results} object containing the results of the bulk application.
     */
    @Nonnull
    public Results applyPolicies(@Nonnull final List<PlacementPolicy> policies,
                                 @Nonnull final GroupResolver groupResolver,
                                 @Nonnull final TopologyGraph topologyGraph) {
        final Map<PolicyDetailCase, List<PlacementPolicy>> policiesByType =
            policies.stream()
                .collect(Collectors.groupingBy(policy ->
                    policy.getPolicyDefinition().getPolicyInfo().getPolicyDetailCase()));

        final ImmutableResults.Builder resultsBldr = ImmutableResults.builder();
        final Map<CommodityDTO.CommodityType, MutableInt> totalAddedCommodities = new HashMap<>();

        APPLICATION_ORDER.forEach(policyClass -> {
            final List<PlacementPolicy> policiesOfClass =
                policiesByType.getOrDefault(policyClass, Collections.emptyList());
            if (!policiesOfClass.isEmpty()) {
                try (DataMetricTimer timer = Metrics.POLICY_APPLICATION_SUMMARY.labels(policyClass.name()).startTimer()) {
                    final PlacementPolicyApplication application =
                        policyFactory.newPolicyApplication(policyClass, groupResolver, topologyGraph);

                    final PolicyApplicationResults res =
                        application.apply(policiesOfClass);
                    resultsBldr.putAllErrors(res.errors());
                    resultsBldr.putAppliedCounts(policyClass, policiesOfClass.size() - res.errors().size());

                    res.addedCommodities().forEach((commType, numAdded) ->
                        totalAddedCommodities.computeIfAbsent(commType, k -> new MutableInt(0))
                            .add(numAdded));
                }
            }
        });

        totalAddedCommodities.forEach((commType, totalNumAdded) -> {
            resultsBldr.putAddedCommodityCounts(commType, totalNumAdded.toInteger());
        });

        // Go through the remaining policies, regardless of the order.
        return resultsBldr.build();
    }

    /**
     * The results of applying a collection of policies.
     */
    @Value.Immutable
    public interface Results {

        /**
         * The number of policies successfully applied, broken down by the type of policy.
         */
        Map<PolicyDetailCase, Integer> appliedCounts();

        /**
         * The number of commodities added to the topology, broken down by the type of commodity.
         */
        Map<CommodityDTO.CommodityType, Integer> addedCommodityCounts();

        /**
         * (Any policies that encountered errors) -> (error encountered by the policy).
         */
        Map<PlacementPolicy, PolicyApplicationException> errors();
    }

    private static class Metrics {

        static final DataMetricSummary POLICY_APPLICATION_SUMMARY = DataMetricSummary.builder()
            .withName("tp_policy_application_duration_seconds")
            .withHelp("Duration of applying policies of particular types.")
            .withLabelNames("type")
            .build()
            .register();

    }
}
