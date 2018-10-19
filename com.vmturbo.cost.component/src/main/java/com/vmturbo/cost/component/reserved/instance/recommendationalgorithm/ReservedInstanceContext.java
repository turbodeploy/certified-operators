package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;
import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/*
 * This abstract class provides Reserved Instance contextual support for Platform and Tenancy.
 */
public abstract class ReservedInstanceContext {

    /**
     * Billing family (master account) id of the reserved instance.
     * Each reserved instance belongs to an account, each account belongs to a billing family.
     */
    protected long masterAccount;

    /**
     * Operating Systems such as Linux, Windows.
     */
    protected final OSType platform;

    /**
     * Underlying resources such as DEFAULT (shared), DEDICATED.
     */
    protected final Tenancy tenancy;

    /**
     * Instance type or template; e.g. m4.xlarge. This is the instance type that the RI analyzer
     * is recommending to buy RIs for.  For instance size flexible RI, the instance type is the
     * smallest instance type in the family; otherwise,the instance type is the type of the demand.
     */
    protected final TopologyEntityDTO computeTier;

    /**
     * Constructor
     *
     * @param masterAccount master account
     * @param platform what is the OS? e.g. LINUX or WINDOWS
     * @param tenancy how are underlying resources used? e.g. DEFAULT (shared), DEDICATED
     * @param computeTier computeTier
     */
    public ReservedInstanceContext(@Nonnull long masterAccount,
                                   @Nonnull OSType platform,
                                   @Nonnull Tenancy tenancy,
                                   @Nonnull TopologyEntityDTO computeTier) {
        this.masterAccount = masterAccount;
        this.platform = Objects.requireNonNull(platform, "Platform is null for RI.");
        this.tenancy = Objects.requireNonNull(tenancy, "Tenancy is null for RI.");
        this.computeTier= Objects.requireNonNull(computeTier, "Compute Tier is null for RI.");
    }

    public long getMasterAccount() {
        return masterAccount;
    }

    @Nonnull
    public OSType getPlatform() {
        return platform;
    }

    @Nonnull
    public Tenancy getTenancy() {
        return tenancy;
    }

    @Nonnull
    public TopologyEntityDTO getComputeTier() {
        return computeTier;
    }

    /**
     * Determine if a Reserved Instance is instance size flexible.
     */
    public boolean isInstanceSizeFlexible() {
        return platform == OSType.LINUX && tenancy == Tenancy.DEFAULT;
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof ReservedInstanceContext)) {
            return false;
        }
        final ReservedInstanceContext context = (ReservedInstanceContext) o;
        return  Objects.equals(masterAccount, context.getMasterAccount()) &&
                Objects.equals(platform, context.getPlatform()) &&
                Objects.equals(tenancy, context.getTenancy()) &&
                Objects.equals(computeTier, context.getComputeTier());
    }
}
