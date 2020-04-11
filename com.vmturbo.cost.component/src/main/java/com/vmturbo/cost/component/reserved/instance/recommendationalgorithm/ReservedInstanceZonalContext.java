package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class represents the zonal context of a reserved instance.
 * This class provides the interface to the HistoryProvider, which accesses the DB.
 * Mirror image of ReservedInstanceDBContext; however, points to objects not strings.
 */
public class ReservedInstanceZonalContext extends ReservedInstanceContext {

    // availability zone id; e.g. aws-us-east-1a
    private final long availabilityZoneId;

    /**
     * Constructor.
     *
     * @param accountId account ID.
     * @param platform OS type.
     * @param tenancy tenancy.
     * @param computeTier template or instance type.
     * @param availabilityZoneId availability zone.
     */
    public ReservedInstanceZonalContext(long accountId,
                                        @Nonnull OSType platform,
                                        @Nonnull Tenancy tenancy,
                                        TopologyEntityDTO computeTier,
                                        long availabilityZoneId) {
        super(accountId, platform, tenancy, computeTier);
        this.availabilityZoneId = availabilityZoneId;
    }


    public long getAvailabilityZoneId() {
        return availabilityZoneId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ReservedInstanceZonalContext context = (ReservedInstanceZonalContext)o;
        return super.equals(o) && Objects.equals(availabilityZoneId, context.availabilityZoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(computeTier, availabilityZoneId, platform, tenancy, accountId);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("zoneId=").append(availabilityZoneId)
            .append(" computeTierId=").append(computeTier.getOid())
            .append(" platform=").append(platform.name())
            .append(" tenancy=").append(tenancy.name())
            .append(" accountId=").append(accountId);
        return builder.toString();
    }
}
