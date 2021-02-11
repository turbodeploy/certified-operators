package com.vmturbo.market.topology.conversions;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
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
     * Constructor for ReservedInstanceKey.
     * @param riData reserved instance data
     * @param family the family name of the RI Template.
     * @param billingFamilyId billing family id of the account to which the RI belongs.
     */
    public ReservedInstanceKey(ReservedInstanceData riData, String family, long billingFamilyId) {
        final ReservedInstanceSpecInfo riSpec =
                riData.getReservedInstanceSpec().getReservedInstanceSpecInfo();
        final ReservedInstanceBoughtInfo riBoughtInfo = riData.getReservedInstanceBought()
                .getReservedInstanceBoughtInfo();
        this.tenancy = riSpec.getTenancy();
        this.platformFlexible = riSpec.getPlatformFlexible();
        this.os = platformFlexible ? OSType.UNKNOWN_OS : riSpec.getOs();
        this.regionId = riSpec.getRegionId();
        this.family = family;
        this.zoneId = riBoughtInfo.getAvailabilityZoneId();
        this.instanceSizeFlexible = riSpec.getSizeFlexible();
        this.tierId = riSpec.getTierId();
        // If the RI is shared, then it is applicable across the billing family, hence the
        // accountScopeId is set to the billingFamilyId. Otherwise the RI is applicable only to the
        // owning account and the accountScopeId is set to the owning account's id.
        this.shared = riBoughtInfo.getReservedInstanceScopeInfo().getShared();
        if (shared) {
            scopedAccounts = ImmutableSet.of(billingFamilyId);
        } else {
            scopedAccounts = ImmutableSet.copyOf(riBoughtInfo.getReservedInstanceScopeInfo().getApplicableBusinessAccountIdList());
        }
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
    public boolean equals(Object obj) {
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
        ReservedInstanceKey other = (ReservedInstanceKey)obj;
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
