package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This abstract class provides Reserved Instance contextual support for Platform and Tenancy.
 */
public abstract class ReservedInstanceContext {

    /**
     * The business account associated with the RI or demand.
     */
    protected long accountId;

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
     * This instance variable is the TopologyEntityDTO instead of the OID, because the lookup returns
     * an Optional, which makes checking for isPresent() ugly for each use, instead of checking only
     * once at the definition.
     */
    protected final TopologyEntityDTO computeTier;

    /**
     * Constructor.
     *
     * @param accountId The account ID
     * @param platform what is the OS? e.g. LINUX or WINDOWS
     * @param tenancy how are underlying resources used? e.g. DEFAULT (shared), DEDICATED
     * @param computeTier computeTier
     */
    public ReservedInstanceContext(@Nonnull long accountId,
                                   @Nonnull OSType platform,
                                   @Nonnull Tenancy tenancy,
                                   @Nonnull TopologyEntityDTO computeTier) {
        this.accountId = accountId;
        this.platform = Objects.requireNonNull(platform, "Platform is null for RI.");
        this.tenancy = Objects.requireNonNull(tenancy, "Tenancy is null for RI.");
        this.computeTier = computeTier;
    }

    public long getAccountId() {
        return accountId;
    }

    @Nonnull
    public OSType getPlatform() {
        return platform;
    }

    @Nonnull
    public Tenancy getTenancy() {
        return tenancy;
    }

    public TopologyEntityDTO getComputeTier() {
        return computeTier;
    }

    /**
     * Determine if a Reserved Instance is instance size flexible.
     * @return always return false as default.
     */
    public boolean isInstanceSizeFlexible() {
        return false;
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof ReservedInstanceContext)) {
            return false;
        }
        final ReservedInstanceContext context = (ReservedInstanceContext)o;
        return  Objects.equals(accountId, context.getAccountId()) &&
                Objects.equals(platform, context.getPlatform()) &&
                Objects.equals(tenancy, context.getTenancy()) &&
                Objects.equals(computeTier, context.getComputeTier());
    }
}
