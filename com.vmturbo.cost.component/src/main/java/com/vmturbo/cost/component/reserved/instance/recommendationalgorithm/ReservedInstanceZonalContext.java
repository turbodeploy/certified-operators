package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;


/*
 * This class represents the zonal context of a reserved instance.
 * This class provides the interface to the HistoryProvider, which accesses the DB.
 * Mirror image of ReservedInstanceDBContext; however, points to objects not strings.
 */
public class ReservedInstanceZonalContext extends ReservedInstanceContext {

    // availability zone id; e.g. aws-us-east-1a
    private final long availabilityZone;

    /**
     * Constructor
     *
     * @param masterAccount master account
     * @param platform
     * @param tenancy
     * @param computeTier
     * @param availabilityZone Availability zone.
     */
    public ReservedInstanceZonalContext(@Nonnull long masterAccount,
                                        @Nonnull OSType platform,
                                        @Nonnull Tenancy tenancy,
                                        @Nonnull TopologyEntityDTO computeTier,
                                        long availabilityZone) {
        super(masterAccount, platform, tenancy, computeTier);
        this.availabilityZone = availabilityZone;
    }


    public long getAvailabilityZone() {
        return availabilityZone;
    }

    @Override
    public boolean isInstanceSizeFlexible() {
        return platform == OSType.LINUX && tenancy == Tenancy.DEFAULT;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final ReservedInstanceZonalContext context = (ReservedInstanceZonalContext) o;
        return super.equals(o) && Objects.equals(availabilityZone, context.availabilityZone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(computeTier, availabilityZone, platform, tenancy, masterAccount);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder
        .append("instanceType=").append(computeTier)
        .append(" zone=").append(availabilityZone)
        .append(" platform=").append(platform.name())
        .append(" tenancy=").append(tenancy.name())
        .append(" masterAccount=").append(masterAccount);
        return builder.toString();
    }
}
