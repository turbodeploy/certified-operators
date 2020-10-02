package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * This is legacy code carried over from the RI coverage allocator. The rules definition was deprecated
 * within the allocator and is reproduced here to allow use by the RI buy analysis.
 */
public interface StaticRICoverageRules {

    /**
     * An ordered set of RI coverage rule configurations for AWS. These rules are based on
     * https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/apply_ri.html
     */
    List<RICoverageRule> AWS_RI_COVERAGE_RULES = ImmutableList.of(
            // First AWS rule is zonal RIs within the same account
            RICoverageRule.builder()
                    .sharedScope(false)
                    .zoneScoped(true)
                    .platformFlexible(false)
                    .sizeFlexible(false)
                    .build(),
            // Second AWS rule is zonal RIs within the same billing family
            RICoverageRule.builder()
                    .sharedScope(true)
                    .zoneScoped(true)
                    .platformFlexible(false)
                    .sizeFlexible(false)
                    .build(),
            // Third AWS rule is regional RIs within the same account. We do not set
            // isSizeFlexible, given RIs at this scope can be size flexible or non-size
            // flexible. Therefore, VMs should be able to match to either ISF or non-ISF RIs
            RICoverageRule.builder()
                    .sharedScope(false)
                    .zoneScoped(false)
                    .platformFlexible(false)
                    .build(),
            // Fourth/last AWS rule is regional RIs within the billing family. We do not
            // set isSizeFlexible here for the same reason as above
            RICoverageRule.builder()
                    .sharedScope(true)
                    .zoneScoped(false)
                    .platformFlexible(false)
                    .build());

    /**
     * An ordered set of RI coverage rule configurations for Azure.
     */
    List<RICoverageRule> AZURE_RI_COVERAGE_RULES = ImmutableList.of(
            // First Azure rule is non-size flexible RIs in the same account
            RICoverageRule.builder()
                    .sharedScope(false)
                    .zoneScoped(false)
                    .platformFlexible(true)
                    .sizeFlexible(false)
                    .build(),
            // Second Azure rule is non-size flexible RIs in the billing family (EA family)
            RICoverageRule.builder()
                    .sharedScope(true)
                    .zoneScoped(false)
                    .platformFlexible(true)
                    .sizeFlexible(false)
                    .build(),
            // Third Azure rule is size flexible RIs in the same account
            RICoverageRule.builder()
                    .sharedScope(false)
                    .zoneScoped(false)
                    .platformFlexible(true)
                    .sizeFlexible(true)
                    .build(),
            // Last Azure rule is size flexible RIs in the billing family (EA family)
            RICoverageRule.builder()
                    .sharedScope(true)
                    .zoneScoped(false)
                    .platformFlexible(true)
                    .sizeFlexible(true)
                    .build());

}
