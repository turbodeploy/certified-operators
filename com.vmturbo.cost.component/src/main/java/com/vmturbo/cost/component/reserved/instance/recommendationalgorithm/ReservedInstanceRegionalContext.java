package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceUtil;
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
    private final TopologyEntityDTO region;

    /**
     * Constructor, we only save the Region OID and DisplayName, not whole DTO.
     *
     * @param accountId account ID.
     * @param platform OS type.
     * @param tenancy tenancy.
     * @param computeTier template or instance type.
     * @param region region.
     */
    public ReservedInstanceRegionalContext(@Nonnull long accountId,
                                           @Nonnull OSType platform,
                                           @Nonnull Tenancy tenancy,
                                           @Nonnull TopologyEntityDTO computeTier,
                                           @Nonnull TopologyEntityDTO region) {
        super(accountId, platform, tenancy, computeTier);
        this.region = Objects.requireNonNull(region, "Region is null for RI.");
    }

    /**
     * Returns the region for this regional context.
     *
     * @return region for this regional context.
     */
    public TopologyEntityDTO getRegion() {
        return region;
    }

    /**
     * Get the region's OID.
     *
     * @return the region's OID.
     */
    public long getRegionId() {
        return region.getOid();
    }

    /**
     * Get the region's display name.
     *
     * @return The region's display name.
     */
    public String getRegionDisplayName() {
        return region.getDisplayName();
    }

    @Override
    public boolean isInstanceSizeFlexible() {
        return ReservedInstanceUtil.LINUX_BASED_OS_SET.contains(this.platform) && tenancy == Tenancy.DEFAULT;
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
        return super.equals(o) && Objects.equals(getRegionId(), context.getRegionId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRegionId(), platform, tenancy, computeTier, accountId);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("regionId=").append(getRegionId())
            .append(" computeTierId=").append(computeTier.getOid())
            .append(" platform=").append(platform.name())
            .append(" tenancy=").append(tenancy.name())
            .append(" accountId=").append(accountId);
        return builder.toString();
    }
}
