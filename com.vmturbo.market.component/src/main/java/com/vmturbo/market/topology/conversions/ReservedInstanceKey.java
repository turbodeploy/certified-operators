package com.vmturbo.market.topology.conversions;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class is what distinguishes one {@link ReservedInstanceAggregate} from another.
 * Two identical ISF RI's will have the "same" ReservedInstanceKey.
 * This will help in aggregating multiple ri's into a single CBTP.
 */
public class ReservedInstanceKey {
    private final Tenancy tenancy;
    private final OSType os;
    private final long regionId;
    private final long zoneId;
    private final String family;
    private final long tierId;
    private final boolean instanceSizeFlexible;
    private final boolean platformFlexible;
    private final Set<Long> scopedAccounts;
    private final boolean shared;

    Tenancy getTenancy() {
        return tenancy;
    }

    OSType getOs() {
        return os;
    }

    long getRegionId() {
        return regionId;
    }

    String getFamily() {
        return family;
    }

    /**
     * Get scoped accounts ids.
     * If RI is shared it is only billing family ID
     * If RI is scoped it is collection of account IDs
     *
     * @return scoped accounts ids
     */
    public Set<Long> getAccountScopeId() {
        return scopedAccounts;
    }

    long getZoneId() {
        return zoneId;
    }

    boolean getShared() {
        return shared;
    }

    /**
     * Constructor.
     *
     * @param tenancy the tenancy
     * @param os the OS
     * @param regionId the region ID
     * @param zoneId the zone ID
     * @param family the family
     * @param tierId the tier ID
     * @param instanceSizeFlexible is instance flexible flag
     * @param platformFlexible is platform flexible flag
     * @param scopedAccounts the set of scoped accounts
     * @param shared is shared flag
     */
    public ReservedInstanceKey(final Tenancy tenancy, final OSType os, final long regionId,
            final long zoneId, final String family, final long tierId,
            final boolean instanceSizeFlexible, final boolean platformFlexible,
            final Set<Long> scopedAccounts, final boolean shared) {
        this.tenancy = tenancy;
        this.os = os;
        this.regionId = regionId;
        this.zoneId = zoneId;
        this.family = family;
        this.tierId = tierId;
        this.instanceSizeFlexible = instanceSizeFlexible;
        this.platformFlexible = platformFlexible;
        this.scopedAccounts = scopedAccounts;
        this.shared = shared;
    }

    /**
     * Constructor.
     *
     * @param riData the {@link ReservedInstanceData}
     * @param family the family
     * @param billingFamilyId the billing family
     */
    public ReservedInstanceKey(final ReservedInstanceData riData, final String family,
            final long billingFamilyId) {
        this(riData.getReservedInstanceBought().getReservedInstanceBoughtInfo(),
                riData.getReservedInstanceSpec().getReservedInstanceSpecInfo(), family,
                billingFamilyId);
    }

    /**
     * Constructor.
     *
     * @param riBoughtInfo the {@link ReservedInstanceBoughtInfo}
     * @param riSpec the {@link ReservedInstanceSpecInfo}
     * @param family the family
     * @param billingFamilyId the billing family ID
     */
    public ReservedInstanceKey(final ReservedInstanceBoughtInfo riBoughtInfo,
            final ReservedInstanceSpecInfo riSpec, final String family,
            final long billingFamilyId) {
        this(riBoughtInfo.getReservedInstanceScopeInfo(), riSpec.getTenancy(),
                riSpec.getPlatformFlexible() ? OSType.UNKNOWN_OS : riSpec.getOs(),
                riSpec.getRegionId(), riBoughtInfo.getAvailabilityZoneId(), family,
                riSpec.getTierId(), riSpec.getSizeFlexible(), riSpec.getPlatformFlexible(),
                billingFamilyId);
    }

    /**
     * Constructor.
     *
     * @param scopeInfo the {@link ReservedInstanceScopeInfo}
     * @param tenancy the tenancy
     * @param os the OS
     * @param regionId the region ID
     * @param zoneId the zone ID
     * @param family the family
     * @param tierId the tier ID
     * @param instanceSizeFlexible the instance flexibility flag
     * @param platformFlexible the platform flexibility flag
     * @param billingFamilyId the billing family ID
     */
    public ReservedInstanceKey(final ReservedInstanceScopeInfo scopeInfo, final Tenancy tenancy,
            final OSType os, final long regionId, final long zoneId, final String family,
            final long tierId, final boolean instanceSizeFlexible, final boolean platformFlexible,
            final long billingFamilyId) {
        // If the RI is shared, then it is applicable across the billing family, hence the
        // accountScopeId is set to the billingFamilyId. Otherwise the RI is applicable only to the
        // owning account and the accountScopeId is set to the owning account's id.
        this(tenancy, os, regionId, zoneId, family, tierId, instanceSizeFlexible, platformFlexible,
                scopeInfo.getShared() ? ImmutableSet.of(billingFamilyId)
                        : ImmutableSet.copyOf(scopeInfo.getApplicableBusinessAccountIdList()),
                scopeInfo.getShared());
    }

    @Override
    public int hashCode() {
        if (isInstanceSizeFlexible()) {
            return Objects.hash(tenancy, os, regionId, family, zoneId, scopedAccounts);
        } else {
            return Objects.hash(tenancy, os, regionId, tierId, zoneId, scopedAccounts);
        }

    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        // Compare operating systems, compares the tenancy, os,
        // region, zone to check if 2 RIAggregates are the same.
        // In case of instance size flexible check family. Otherwise checks if
        // the tierId are the same.
        final ReservedInstanceKey other = (ReservedInstanceKey)obj;
        if (isInstanceSizeFlexible()) {
            return this.tenancy == other.tenancy &&
                    this.os == other.os &&
                    this.regionId == other.regionId &&
                    this.family.equals(other.family) &&
                    this.zoneId == other.zoneId &&
                    Objects.equals(scopedAccounts, other.scopedAccounts);
        } else {
            return this.tenancy == other.tenancy &&
                    this.os == other.os &&
                    this.regionId == other.regionId &&
                    this.tierId == other.tierId &&
                    this.zoneId == other.zoneId &&
                    Objects.equals(scopedAccounts, other.scopedAccounts);
        }
    }

    @Override
    public String toString() {
        return "ReservedInstanceKey{" + "tenancy=" + tenancy + ", os=" + os + ", regionId=" +
                regionId + ", zoneId=" + zoneId + ", family='" + family + '\'' + ", tierId=" +
                tierId + ", instanceSizeFlexible=" + instanceSizeFlexible +
                ", accountScopeId=" + StringUtils.join(scopedAccounts, "|") + ", shared=" + shared + '}';
    }

    boolean isInstanceSizeFlexible() {
        return instanceSizeFlexible;
    }

    boolean isPlatformFlexible() {
        return platformFlexible;
    }
}
