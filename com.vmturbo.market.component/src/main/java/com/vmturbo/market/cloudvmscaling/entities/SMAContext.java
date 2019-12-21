package com.vmturbo.market.cloudvmscaling.entities;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * The Stable Marriage algorithm base context.
 */
public class SMAContext {
    /*
     * CSP  cloud service provider; e.g. AWS or Azure
     */
    private final SMACSP csp;
    /*
     * OS; e.g. windows or Linux
     */
    private final SMAPlatform os;
    /*
     * Cloud Region
     */
    private final long region;
    /*
     * BillingAccount: e.g. master account in AWS or EA in Azure
     */
    private final long billingAccount;
    /*
     * Tenancy
     */
    private final SMATenancy tenancy;

    /**
     * SMAContext constructor.
     *
     * @param csp  csp
     * @param os operating system
     * @param region the corresponding region
     * @param billingAccount corresponding billing account
     * @param tenancy the corresponding tenancy
     */
    public SMAContext(@Nonnull SMACSP csp,
                      @Nonnull SMAPlatform os,
                      @Nonnull long region,
                      @Nonnull long billingAccount,
                      @Nonnull SMATenancy tenancy) {
        this.csp = Objects.requireNonNull(csp);
        this.os = Objects.requireNonNull(os);
        this.region = Objects.requireNonNull(region);
        this.billingAccount = Objects.requireNonNull(billingAccount);
        this.tenancy = Objects.requireNonNull(tenancy);
    }

    @Nonnull
    public SMACSP getCsp() {
        return csp;
    }

    @Nonnull
    public SMAPlatform getOs() {
        return os;
    }

    @Nonnull
    public long getRegion() {
        return region;
    }

    public long getBillingAccount() {
        return billingAccount;
    }

    @Nonnull
     SMATenancy getTenancy() {
        return tenancy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(csp, os, region, billingAccount, tenancy);
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
        return csp == that.csp &&
                os == that.os &&
                Objects.equals(region, that.region) &&
                Objects.equals(billingAccount, that.billingAccount) &&
                tenancy == that.tenancy;
    }

    @Override
    public String toString() {
        return "SMAContext{" +
                "csp=" + csp +
                ", os=" + os +
                ", region='" + region + '\'' +
                ", billingAccount='" + billingAccount + '\'' +
                ", tenancy=" + tenancy +
                '}';
    }
}
