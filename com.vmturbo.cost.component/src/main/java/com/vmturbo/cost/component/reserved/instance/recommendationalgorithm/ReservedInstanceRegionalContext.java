package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;
import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/*
 * This class scopes zonal contexts by regional context together to do analysis.
 * For example, for instance size flexible, the RI regional contexts will be all instance types in
 * the same family across all availability zone of the region.
 * For instance size not flexible, the RI regional contexts will be all the same instance types
 * across all availability zone of the region.
 */
public class ReservedInstanceRegionalContext extends ReservedInstanceContext {

    private static final Logger logger = LogManager.getLogger();

    // region: e.g. aws-us-east-1
    private final long region;

    /**
     * Constructor
     *
     * @param masterAccount
     * @param platform
     * @param tenancy
     * @param computeTier
     * @param region
     */
    public ReservedInstanceRegionalContext(@Nonnull long masterAccount,
                                           @Nonnull OSType platform,
                                           @Nonnull Tenancy tenancy,
                                           @Nonnull TopologyEntityDTO computeTier,
                                           long region
                                           ) {
        super(masterAccount, platform, tenancy, computeTier);
        this.region = region;
    }

    @Nonnull
    public long getRegion() {
        return region;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final ReservedInstanceRegionalContext context = (ReservedInstanceRegionalContext) o;
        return super.equals(o) && Objects.equals(region, context.getRegion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(region, platform, tenancy, computeTier, masterAccount);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("region=").append(region)
                .append(" instanceType=").append(computeTier)
            .append(" platform=").append(platform.name())
            .append(" tenancy=").append(tenancy.name())
            .append(" masterAccount=").append(masterAccount);
        return builder.toString();
    }
}
