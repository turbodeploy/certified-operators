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
     * CSP  cloud service provider; e.g. AWS or Azure
     */
    private final SMACSP csp;
    /*
     * OS; e.g. windows or Linux.
     */
    private final OSType osType;
    /*
     * Cloud Region ID.
     */
    private final long regionId;
    /*
     * Billing family ID: e.g. in Azure it is the enterprise agreement, in AWS it is the master account.
     */
    private final long billingFamilyId;
    /*
     * Tenancy
     */
    private final Tenancy tenancy;

    /**
     * SMAContext constructor.
     * @param csp  csp
     * @param osType operating system
     * @param regionId the corresponding region
     * @param billingFamilyId corresponding billing family
     * @param tenancy the corresponding tenancy
     */
    public SMAContext(@Nonnull SMACSP csp,
                      @Nonnull OSType osType,
                      long regionId,
                      long billingFamilyId,
                      @Nonnull Tenancy tenancy) {
        this.csp = Objects.requireNonNull(csp);
        this.osType = Objects.requireNonNull(osType);
        this.regionId = regionId;
        this.billingFamilyId = billingFamilyId;
        this.tenancy = Objects.requireNonNull(tenancy);
    }

    @Nonnull
    public SMACSP getCsp() {
        return csp;
    }

    @Nonnull
    public OSType getOs() {
        return osType;
    }

    @Nonnull
    public long getRegionId() {
        return regionId;
    }

    public long getBillingFamilyId() {
        return billingFamilyId;
    }

    @Nonnull
    public Tenancy getTenancy() {
        return tenancy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(osType, regionId, billingFamilyId, tenancy);
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
                Objects.equals(billingFamilyId, that.billingFamilyId) &&
                tenancy == that.tenancy;
    }

    @Override
    public String toString() {
        return "SMAContext{" +
            "csp=" + csp +
            ", billingFamilyId='" + billingFamilyId + '\'' +
            ", regionId='" + regionId + '\'' +
            ", os=" + osType +
            ", tenancy=" + tenancy +
            '}';
    }
}
