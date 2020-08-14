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

import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicyApplication.PolicyApplicationResults;

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

    /**
     * Public constructor.
     *
     * @param policyFactory Factory class for policies.
     */
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
                                 @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        final Map<PolicyDetailCase, List<PlacementPolicy>> policiesByType =
            policies.stream()
                .collect(Collectors.groupingBy(policy ->
                    policy.getPolicyDefinition().getPolicyInfo().getPolicyDetailCase()));

        final Results results = new Results();
        final Map<CommodityDTO.CommodityType, MutableInt> totalAddedCommodities = new HashMap<>();

        APPLICATION_ORDER.forEach(policyClass -> {
            final List<PlacementPolicy> policiesOfClass =
                policiesByType.getOrDefault(policyClass, Collections.emptyList());
            if (!policiesOfClass.isEmpty()) {
                try (DataMetricTimer timer = Metrics.POLICY_APPLICATION_SUMMARY.labels(policyClass.name()).startTimer()) {
                    final PlacementPolicyApplication<?> application =
                        policyFactory.newPolicyApplication(policyClass, groupResolver, topologyGraph);

                    final PolicyApplicationResults res =
                        application.apply(policiesOfClass);
                    results.putAllErrors(res.errors());
                    results.putAppliedCounts(policyClass, policiesOfClass.size() - res.errors().size());

                    res.addedCommodities().forEach((commType, numAdded) ->
                        results.putAddedCommodityCounts(commType, policyClass, numAdded));
                }
            }
        });

        // Go through the remaining policies, regardless of the order.
        return results;
    }

    /**
     * Get the commodity type associated with a placement policy.
     *
     * @param policy The input policy.
     * @param groupResolver The {@link GroupResolver} to use to resolve groups.
     * @param topologyGraph The {@link TopologyGraph} for the currently constructed topology.
     * @return commodity type associated with the policy.
     */
    public TopologyDTO.CommodityType commodityTypeAssociatedWithPlacementPolicy(
            @Nonnull final PlacementPolicy policy,
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        final PlacementPolicyApplication application =
                policyFactory.newPolicyApplication(policy.getPolicyDefinition()
                        .getPolicyInfo().getPolicyDetailCase(), groupResolver, topologyGraph);

        return application.commoditySold(policy).getCommodityType();
    }

    /**
     * The results of applying a collection of policies.
     * Used to help print summary information about policy application in a broadcast.
     */
    public static class Results {

        /**
         * The number of policies successfully applied, broken down by the type of policy.
         */
        private final Map<PolicyDetailCase, Integer> appliedCounts = new HashMap<>();

        /**
         * The number of commodities added to the topology, broken down by the type of commodity.
         */
        private final Map<PolicyDetailCase, Map<CommodityDTO.CommodityType, Integer>> addedCommodityCounts = new HashMap<>();

        /**
         * (Any policies that encountered errors) -> (error encountered by the policy).
         */
        private final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();

        private int invalidPolicyCount = 0;

        /**
         * Record the number of commodities of a particular type added by policies.
         *
         * @param commType The commodity type.
         * @param policyType The type of policy.
         * @param commodityCount The number of commodities added.
         */
        public void putAddedCommodityCounts(@Nonnull final CommodityType commType,
                                            @Nonnull final PolicyDetailCase policyType,
                                            final int commodityCount) {
            addedCommodityCounts.computeIfAbsent(policyType, k -> new HashMap<>())
                .put(commType, commodityCount);
        }

        /**
         * Add error information about applied policies.
         *
         * @param errors Map of failed policy -> exception that caused the failure.
         */
        public void putAllErrors(@Nonnull final Map<PlacementPolicy, PolicyApplicationException> errors) {
            this.errors.putAll(errors);
        }

        /**
         * Record the number of policies applied for a particular {@link PolicyDetailCase}.
         *
         * @param policyClass The {@link PolicyDetailCase}.
         * @param count The number of policies of the policy class that were applied.
         */
        public void putAppliedCounts(@Nonnull final PolicyDetailCase policyClass, final int count) {
            this.appliedCounts.put(policyClass, count);
        }

        /**
         * Get errors encountered during policy application.
         *
         * @return Map from failed policy to the exception that the policy failed with.
         */
        public Map<PlacementPolicy, PolicyApplicationException> getErrors() {
            return errors;
        }

        /**
         * Get counts of successfully applied policies.
         *
         * @return Map from the type of policy to the number of applied policies of that type.
         */
        public Map<PolicyDetailCase, Integer> getAppliedCounts() {
            return appliedCounts;
        }

        /**
         * Get counts of commodities added by a specific policy, broken down by commodity type.
         *
         * @param type The type of policy.
         * @return Map of commodity type -> number of commodities of that type added by policies.
         */
        public Map<CommodityDTO.CommodityType, Integer> getAddedCommodityCounts(@Nonnull final PolicyDetailCase type) {
            return addedCommodityCounts.getOrDefault(type, Collections.emptyMap());
        }

        /**
         * Get counts of commodities added by all policies, broken down by commodity type.
         *
         * @return Map of commodity type -> number of commodities of that type added by policies.
         */
        public Map<CommodityDTO.CommodityType, Integer> getTotalAddedCommodityCounts() {
            Map<CommodityDTO.CommodityType, Integer> ret = new HashMap<>();
            addedCommodityCounts.forEach((type, commsAdded) -> {
                commsAdded.forEach((commType, numAdded) -> {
                    ret.compute(commType, (k, oldVal) -> {
                        if (oldVal == null) {
                            return numAdded;
                        } else {
                            return numAdded + oldVal;
                        }
                    });
                });
            });
            return ret;
        }

        /**
         * Get the number of invalid policies in this round of policy applications.
         *
         * @return The number of invalid policies.
         */
        public int getInvalidPolicyCount() {
            return invalidPolicyCount;
        }

        /**
         * Record the number of invalid policies in this round of policy application.
         *
         * @param invalidPolicyCount The number of invalid policies.
         */
        public void addInvalidPolicyCount(final int invalidPolicyCount) {
            this.invalidPolicyCount += invalidPolicyCount;
        }
    }

    /**
     * Metrics used for policy application.
     */
    private static class Metrics {

        static final DataMetricSummary POLICY_APPLICATION_SUMMARY = DataMetricSummary.builder()
            .withName("tp_policy_application_duration_seconds")
            .withHelp("Duration of applying policies of particular types.")
            .withLabelNames("type")
            .build()
            .register();

    }
}
