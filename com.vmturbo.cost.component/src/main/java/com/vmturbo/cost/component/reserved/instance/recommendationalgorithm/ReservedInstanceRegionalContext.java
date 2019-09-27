package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class defines regional contexts for reserved instances.
 * For example, for instance size flexible, the RI regional contexts will be all instance types in
 * the same family across all availability zone of the region.
 * For instance size not flexible, the RI regional contexts will be all the same instance types
 * across all availability zone of the region.
 */
public class ReservedInstanceRegionalContext extends ReservedInstanceContext {

    private static final Logger logger = LogManager.getLogger();

    // region: e.g. aws-us-east-1
    private final long regionId;

    /**
     * Constructor.
     *
     * @param masterAccountId master account ID.
     * @param platform OS type.
     * @param tenancy  tenancy.
     * @param computeTier template or instance type.
     * @param regionId  region ID.
     */
    public ReservedInstanceRegionalContext(@Nonnull long masterAccountId,
                                           @Nonnull OSType platform,
                                           @Nonnull Tenancy tenancy,
                                           @Nonnull TopologyEntityDTO computeTier,
                                           long regionId) {
        super(masterAccountId, platform, tenancy, computeTier);
        this.regionId = regionId;
    }

    public long getRegionId() {
        return regionId;
    }

    @Override
    public boolean isInstanceSizeFlexible() {
        return platform == OSType.LINUX &&
            (tenancy == Tenancy.DEFAULT || tenancy == Tenancy.DEDICATED);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return  true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ReservedInstanceRegionalContext context = (ReservedInstanceRegionalContext)o;
        return super.equals(o) && Objects.equals(regionId, context.getRegionId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(regionId, platform, tenancy, computeTier, masterAccountId);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("regionId=").append(regionId)
            .append(" computeTierId=").append(computeTier.getOid())
            .append(" platform=").append(platform.name())
            .append(" tenancy=").append(tenancy.name())
            .append(" masterAccountId=").append(masterAccountId);
        return builder.toString();
    }
}
