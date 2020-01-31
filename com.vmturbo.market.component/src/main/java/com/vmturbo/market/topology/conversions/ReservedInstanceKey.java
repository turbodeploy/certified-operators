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
    private final long accountId;
    private final String family;
    private final long riBoughtId;
    private final boolean instanceSizeFlexible;

    public long getRiBoughtId() {
        return riBoughtId;
    }

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

    long getAccount() {
        return accountId;
    }

    long getZoneId() {
        return zoneId;
    }

    /**
     * Constructor for ReservedInstanceKey.
     * @param riData reserved instance data
     * @param family the family name of the RI Template.
     */
    public ReservedInstanceKey(ReservedInstanceData riData, String family) {
        final ReservedInstanceSpecInfo riSpec =
                riData.getReservedInstanceSpec().getReservedInstanceSpecInfo();
        final ReservedInstanceBoughtInfo riBoughtInfo = riData.getReservedInstanceBought()
                .getReservedInstanceBoughtInfo();
        this.tenancy = riSpec.getTenancy();
        this.os = riSpec.getOs();
        this.regionId = riSpec.getRegionId();
        this.family = family;
        this.zoneId = riBoughtInfo.getAvailabilityZoneId();
        this.accountId = riBoughtInfo.getBusinessAccountId();
        this.instanceSizeFlexible = riSpec.getSizeFlexible();
        if (!instanceSizeFlexible) {
            this.riBoughtId = riData.getReservedInstanceBought().getId();
        } else {
            this.riBoughtId = -1L;
        }
    }

    @Override
    public int hashCode() {
        if (isInstanceSizeFlexible()) {
            return Objects.hash(tenancy, os, regionId, family, zoneId, accountId);
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
                    this.accountId == other.accountId;
        } else {
            return this.riBoughtId == other.riBoughtId;
        }
    }

    boolean isInstanceSizeFlexible() {
        return instanceSizeFlexible;
    }
}


