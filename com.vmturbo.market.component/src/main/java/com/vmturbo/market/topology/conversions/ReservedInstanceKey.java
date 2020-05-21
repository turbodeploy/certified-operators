package com.vmturbo.market.topology.conversions;

import java.util.Objects;

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
    private final long riBoughtId;
    private final boolean instanceSizeFlexible;
    private final boolean platformFlexible;
    private final long accountScopeId;
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

    long getAccountScopeId() {
        return accountScopeId;
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
        final long accountId = riBoughtInfo.getBusinessAccountId();
        this.instanceSizeFlexible = riSpec.getSizeFlexible();
        if (!instanceSizeFlexible) {
            this.riBoughtId = riData.getReservedInstanceBought().getId();
        } else {
            this.riBoughtId = -1L;
        }
        // If the RI is shared, then it is applicable across the billing family, hence the
        // accountScopeId is set to the billingFamilyId. Otherwise the RI is applicable only to the
        // owning account and the accountScopeId is set to the owning account's id.
        this.shared = riBoughtInfo.getReservedInstanceScopeInfo().getShared();
        if (shared) {
            accountScopeId = billingFamilyId;
        } else {
            accountScopeId = accountId;
        }
    }

    @Override
    public int hashCode() {
        if (isInstanceSizeFlexible()) {
            return Objects.hash(tenancy, os, regionId, family, zoneId, accountScopeId);
        } else {
            return Objects.hash(riBoughtId);
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

        // In case of instance size flexible operating systems, compares the tenancy, os,
        // region, zone, family to check if 2 RIAggregates are the same. Otherwise checks if
        // the RIBoughtIds are the same.
        ReservedInstanceKey other = (ReservedInstanceKey)obj;
        if (isInstanceSizeFlexible()) {
            return this.tenancy == other.tenancy &&
                    this.os == other.os &&
                    this.regionId == other.regionId &&
                    this.family.equals(other.family) &&
                    this.zoneId == other.zoneId &&
                    this.accountScopeId == other.accountScopeId;
        } else {
            return this.riBoughtId == other.riBoughtId;
        }
    }

    @Override
    public String toString() {
        return "ReservedInstanceKey{" + "tenancy=" + tenancy + ", os=" + os + ", regionId=" +
                regionId + ", zoneId=" + zoneId + ", family='" + family + '\'' + ", riBoughtId=" +
                riBoughtId + ", instanceSizeFlexible=" + instanceSizeFlexible +
                ", accountScopeId=" + accountScopeId + ", shared=" + shared + '}';
    }

    boolean isInstanceSizeFlexible() {
        return instanceSizeFlexible;
    }

    boolean isPlatformFlexible() {
        return platformFlexible;
    }
}
