package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * The Stable Marriage algorithm base context.
 */
public class SMAContext {
    /*
     * OS; e.g. windows or Linux.
     */
    private final OSType osType;
    /*
     * Cloud Region ID.
     */
    private final long regionId;
    /*
     * BillingAccount ID: e.g. master account in AWS or EA in Azure
     */
    private final long billingAccountId;
    /*
     * Tenancy
     */
    private final Tenancy tenancy;

    /**
     * SMAContext constructor.
     * @param osType operating system
     * @param regionId the corresponding region
     * @param billingAccountId corresponding billing account
     * @param tenancy the corresponding tenancy
     */
    public SMAContext(@Nonnull OSType osType,
                      long regionId,
                      long billingAccountId,
                      @Nonnull Tenancy tenancy) {
        this.osType = Objects.requireNonNull(osType);
        this.regionId = regionId;
        this.billingAccountId = billingAccountId;
        this.tenancy = Objects.requireNonNull(tenancy);
    }

    @Nonnull
    public OSType getOs() {
        return osType;
    }

    @Nonnull
    public long getRegionId() {
        return regionId;
    }

    public long getBillingAccountId() {
        return billingAccountId;
    }

    @Nonnull
    public Tenancy getTenancy() {
        return tenancy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(osType, regionId, billingAccountId, tenancy);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SMAContext that = (SMAContext)obj;
        return osType == that.osType &&
                Objects.equals(regionId, that.regionId) &&
                Objects.equals(billingAccountId, that.billingAccountId) &&
                tenancy == that.tenancy;
    }

    @Override
    public String toString() {
        return "SMAContext{" +
            "billingAccount='" + billingAccountId + '\'' +
            ", region='" + regionId + '\'' +
            ", os=" + osType +
            ", tenancy=" + tenancy +
            '}';
    }
}
