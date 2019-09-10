package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;


/**
 * This class describes the scope within which the recommendation algorithm should operate -- it will consider
 * purchasing Reserved Instances for VMs which meet the given criteria.
 */
public class ReservedInstanceAnalysisScope {
    /**
     * Which platforms should be considered (eg LINUX, RHEL, WINDOWS).
     */
    private final ImmutableSet<OSType> platforms;

    /**
     * OID of the regions that should be analyzed (eg us-east-1, us-west-1).
     */
    private final ImmutableSet<Long> regions;

    /**
     * Which tenancy types should be considered (eg DEFAULT and/or DEDICATED).
     * Note: HOST tenancy is not supported.
     */
    private final ImmutableSet<Tenancy> tenancies;

    /**
     * The accounts that need to be analyzed.
     */
    private final ImmutableSet<Long> accounts;

    /**
     * The target for the percentage of usage to be covered. A positive number
     * indicates to cover at least this percentage of usage with RIs. A negative number indicates
     * that the algorithm should choose itself how much to cover for the lowest possible cost.
     */
    private final float preferredCoverage;

    /**
     * If it is true it overrides the coverage to maximize the percentage of the workload covered
     * by RIs. If it is false the maximum savings is considered. the default is false unless
     * it is changed.
     */
    private final boolean overrideRICoverage;

    /**
     * The type of RI to be bought which includes offering class, payment option, term, purchase date.
     */
    private final RIPurchaseProfile riPurchaseProfile;

    /**
     * Create an object describing the scope of analysis to be performed.
     *
     * @param platforms             the set of platforms to analyze. If null, all platforms.
     * @param regions               The set of regions to analyze. If null, all supported regions.
     * @param tenancies             The set of tenancies to analyze. If null, all supported
     *                              tenancies (currently DEFAULT and DEDICATED, but not HOST).
     * @param accounts              The set of accounts to analyze. If null, consider all accounts
     *                              in scope.
     * @param preferredCoverage     The target for the percentage of usage to be covered. A
     *                              positive number indicates to cover at least this percentage
     *                              of workload with RIs. A negative number indicates that the
     *                              algorithm should choose itself how much to cover for the
     *                              lowest possible cost.
     * @param overrideRICoverage    The coverage can be overriden by a percentage. false override
     *                              coverage means default maximum savings and true override
     *                              coverage means specific coverage
     * @param profile     The type of RI to be bought which includes offering class,
     *                              payment option, term, purchase date.
     */
    public ReservedInstanceAnalysisScope(@Nullable Collection<OSType> platforms,
                         @Nullable Collection<Long> regions,
                         @Nullable Collection<Tenancy> tenancies,
                         @Nullable Collection<Long> accounts,
                         float preferredCoverage,
                         boolean overrideRICoverage,
                         @Nullable RIPurchaseProfile profile) {
            this.platforms = (platforms==null) ?
                    ImmutableSet.copyOf(OSType.values()) :
                    ImmutableSet.copyOf(platforms);
            this.regions = (regions == null) ?
                    ImmutableSet.copyOf(getAllSupportedRegions())
                    : ImmutableSet.copyOf(regions);
            this.tenancies = (tenancies == null) ?
                    ImmutableSet.of(Tenancy.DEFAULT, Tenancy.DEDICATED)
                    : ImmutableSet.copyOf(tenancies);
            // Set accounts to null to include all accounts in scope.
            this.accounts = (accounts == null) ? null
                    : ImmutableSet.copyOf(accounts);
        this.preferredCoverage = preferredCoverage;
        this.overrideRICoverage = overrideRICoverage;
        this.riPurchaseProfile = profile;
    }

    /**
     * Create analysis scope based on the StartBuyRIAnalysisRequest.
     *
     * @param startAnalysisRequest The start analysis request which contains the scope of analysis
     *                             like platforms, regions, tenancies, accounts
     */
    public ReservedInstanceAnalysisScope(StartBuyRIAnalysisRequest startAnalysisRequest) {
        // TODO: Should the default values of preferred coverage and override coverage come from
        // startBuyAnalysisRequest?
        this(startAnalysisRequest.getPlatformsList(),
                startAnalysisRequest.getRegionsList(),
                startAnalysisRequest.getTenanciesList(),
                startAnalysisRequest.getAccountsList(), -1, false,
                startAnalysisRequest.getPurchaseProfile());
    }

    /**
     * Get all Regions for which we support RI analysis.
     *
     * @return the set of supported regions.
     */
    private Set<Long> getAllSupportedRegions() {
        return Collections.emptySet();
        /*
        return RepositoryRegistry.vmtMANAGER.getInstances(DataCenter.class).stream()
                .filter(dc -> dc.getCspType() == CSPType.AWS)
                .collect(Collectors.toSet());
        */
    }

    @Nonnull
    public Set<OSType> getPlatforms() {
        return platforms;
    }

    @Nonnull
    public ImmutableSet<Long> getRegions() {
        return regions;
    }

    @Nonnull
    public ImmutableSet<Tenancy> getTenancies() {
        return tenancies;
    }

    /**
     * Get list of accounts in scope.
     *
     * @return the list of accounts in scope. Returning null means all accounts are in scope.
     */
    @Nullable
    public ImmutableSet<Long> getAccounts() {
        return accounts;
    }

    public float getPreferredCoverage() {
        return preferredCoverage;
    }

    public boolean getOverrideRICoverage() {
        return overrideRICoverage;
    }

    public RIPurchaseProfile getRiPurchaseProfile() {
        return riPurchaseProfile;
    }
}
