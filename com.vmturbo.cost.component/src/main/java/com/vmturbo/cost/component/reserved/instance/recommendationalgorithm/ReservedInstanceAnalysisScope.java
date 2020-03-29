package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class describes the scope within which the recommendation algorithm should operate -- it will consider
 * purchasing Reserved Instances for VMs which meet the given criteria.
 */
public class ReservedInstanceAnalysisScope {

    private static final Logger logger = LogManager.getLogger();

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
     * The topology on which this analysis is going to be performed.
     */
    private final TopologyInfo topologyInfo;
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
     * @param profile               The type of RI to be bought which includes offering class,
     *                              payment option, term, purchase date.
     * @param topologyInfo          The topology for the analysis.
     */
    @VisibleForTesting
    protected ReservedInstanceAnalysisScope(@Nullable Collection<OSType> platforms,
                                            @Nullable Collection<Long> regions,
                                            @Nullable Collection<Tenancy> tenancies,
                                            @Nullable Collection<Long> accounts,
                                            float preferredCoverage,
                                            boolean overrideRICoverage,
                                            @Nullable RIPurchaseProfile profile,
                                            @Nonnull TopologyInfo topologyInfo) {
        if (CollectionUtils.isNotEmpty(platforms) && platforms.contains(OSType.UNKNOWN_OS)) {
            logger.warn("ReservedInstanceAnalysisScope platform contains illegal UNKNOWN_OS, removing it");
            // because platforms may be immutable, create a copy.
            Collection<OSType> newPlatforms = new ArrayList();
            for (OSType osType : platforms) {
                if (osType != OSType.UNKNOWN_OS) {
                    newPlatforms.add(osType);
                }
            }
            platforms = newPlatforms;
        }
        this.platforms = (CollectionUtils.isEmpty(platforms)) ?
            ImmutableSet.copyOf(Arrays.stream(OSType.values()).filter(x -> x != OSType.UNKNOWN_OS).collect(Collectors.toSet())) :
            ImmutableSet.copyOf(platforms);
        this.regions = (regions == null)
            ? ImmutableSet.copyOf(getAllSupportedRegions())
            : ImmutableSet.copyOf(regions);
        if (CollectionUtils.isNotEmpty(tenancies) && tenancies.contains(Tenancy.HOST)) {
            logger.warn("ReservedInstanceAnalysisScope tenancy contains illegal HOST, removing it");
            // because tenancies may be immutable, create a copy.
            Collection<Tenancy> newTenancies = new ArrayList();
            for (Tenancy tenancy: tenancies) {
                if (tenancy != Tenancy.HOST) {
                    newTenancies.add(tenancy);
                }
            }
            tenancies = newTenancies;
        }
        this.tenancies = CollectionUtils.isEmpty(tenancies)
            ? ImmutableSet.of(Tenancy.DEFAULT, Tenancy.DEDICATED)
            : ImmutableSet.copyOf(tenancies);
        // Set accounts to null to include all accounts in scope.
        this.accounts = (accounts == null) ? null : ImmutableSet.copyOf(accounts);
        this.preferredCoverage = preferredCoverage;
        this.overrideRICoverage = overrideRICoverage;
        this.riPurchaseProfile = profile;
        this.topologyInfo = topologyInfo;
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
            startAnalysisRequest.getPurchaseProfile(),
            startAnalysisRequest.getTopologyInfo());
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

    public TopologyInfo getTopologyInfo() {
        return topologyInfo;
    }

    /**
     * Determines whether {@code accountOid} is in scope.
     *
     * @param accountOid The target account OID
     * @return True, if the account is in scope or if account analysis is not constrained.
     * False otherwise
     */
    public boolean isAccountInScope(long accountOid) {
        return accounts.isEmpty() || accounts.contains(accountOid);
    }

    /**
     * Determines whether {@code regionOid} is in scope.
     *
     * @param regionOid The target region OID
     * @return True, if the region is in scope or if region analysis is not constrained.
     * False otherwise
     */
    public boolean isRegionInScope(long regionOid) {
        return regions.isEmpty() || regions.contains(regionOid);
    }

    /**
     * Determines whether {@code platofrm} is in scope.
     *
     * @param platform The target platform
     * @return True, if the platform is in scope or if platform analysis is not constrained.
     * False otherwise
     */
    public boolean isPlatformInScope(@Nonnull OSType platform) {
        return platforms.isEmpty() || platforms.contains(platform);
    }

    /**
     * Determines whether {@code tenancy} is in scope.
     *
     * @param tenancy The target tenancy
     * @return True, if the tenancy is in scope or if tenancy analysis is not constrained.
     * False otherwise
     */
    public boolean isTenancyInScope(@Nonnull Tenancy tenancy) {
        return tenancies.isEmpty() || tenancies.contains(tenancy);
    }
}
