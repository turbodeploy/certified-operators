package com.vmturbo.market.cloudvmscaling.entities;

import java.util.Objects;

import javax.annotation.Nonnull;

/**
 * SMA representation of a provide, either on-demand or discounted.
 * This is a template for a specific context.
 */
public class SMATemplate {
    /*
     * Unique identifier of Template
     */
    private final long oid;
    /*
     * name of the template; e.g. t2.micro
     * Only set in constructor and JUnit tests
     */
    private final String name;
    /*
     * name of the template family; e.g. "t2" from t2.micro
     */
    private final String family;
    /*
     * on-demand cost
     */
    private  SMACost onDemandCost;
    /*
     * discounted (or RI) cost
     * For Azure, because RI does not include license cost.
     * The discountedCost is zero for AWS.
     */
    private final SMACost discountedCost;
    /*
     * number of coupons
     */
    private final int coupons;

    /**
     * Constructor of the SMATemplate.
     *
     * @param oid the oid of the template. unique per context
     * @param name the template name
     * @param family the family the template belongs to.
     * @param onDemandCost the on-demand cost.
     * @param discountedCost the discounted cost.
     * @param coupons the number of the coupons needed for SMA template.
     */
    public SMATemplate(@Nonnull final long oid,
                       @Nonnull final String name,
                       @Nonnull final String family,
                       @Nonnull SMACost onDemandCost,
                       @Nonnull SMACost discountedCost,
                       final int coupons) {
        this.oid = Objects.requireNonNull(oid, "OID is null");
        this.name = Objects.requireNonNull(name, "name is null");
        this.family = Objects.requireNonNull(family, "family is null");
        this.onDemandCost = Objects.requireNonNull(onDemandCost, "onDemandCost is null!");
        this.discountedCost = Objects.requireNonNull(discountedCost, "discountedCost is null!");
        this.coupons = coupons;
    }

    @Nonnull
    public long getOid() {
        return oid;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public String getFamily() {
        return family;
    }

    @Nonnull
    public SMACost getOnDemandCost() {
        return onDemandCost;
    }

    @Nonnull
    public SMACost getDiscountedCost() {
        return discountedCost;
    }

    public int getCoupons() {
        return coupons;
    }

    /**
     * compute the net cost based on available coupons, the template On-demand, discounted cost.
     *
     * @param availableCoupons available coupons
     * @return cost after applying coupons.
     */
    public float getNetCost(float availableCoupons) {
        if (availableCoupons > coupons || coupons == 0) {
            return discountedCost.getTotal();
        } else {
            float discountPercentage = availableCoupons / coupons;
            return (discountedCost.getTotal() * discountPercentage) +
                    (onDemandCost.getTotal() * (1 - discountPercentage));

        }
    }

    @Override
    public String toString() {
        return "SMATemplate{" +
                "OID='" + oid + "'" +
                ", name='" + name + '\'' +
                ", family='" + family + '\'' +
                ", onDemandCost=" + onDemandCost +
                ", discountedCost=" + discountedCost +
                ", coupons=" + coupons +
                '}';
    }

}
