package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;

/**
 * SMA representation of a provide, either on-demand or discounted.
 * This is a template for a specific context.
 */
public class SMATemplate {

    private static final Logger logger = LogManager.getLogger();
    /*
     * Instance variables set in the contstructor.
     */
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
     * number of coupons
     */
    private final int coupons;

    /*
     * What context am I in?  For debugging purposes only.
     */
    private final SMAContext context;

    /*
     * The compute tier in XL data structures.  Needed to compute costs.
     */
    private final TopologyEntityDTO computeTier;

    /*
     * instance variables set outside of the construtor
     */
    /*
     *  Map from business account to on-demand cost
     */
    private Map<Long, SMACost> onDemandCosts = new HashMap<>();

    /*
     * Map from business account to discounted cost (only Azure).
     * For AWS, discountedCosts is always 0.
     */
    private Map<Long, SMACost> discountedCosts = new HashMap<>();

    public Map<Long, SMACost> getDiscountedCosts() {
        return discountedCosts;
    }

    public Map<Long, SMACost> getOnDemandCosts() {
        return onDemandCosts;
    }

    /**
     * Constructor of the SMATemplate.
     *
     * @param oid the oid of the template. unique per context
     * @param name the template name
     * @param family the family the template belongs to.
     * @param coupons the number of the coupons needed for SMA template.
     * @param context what context is this template in?
     * @param computeTier link back to XL data structures for compute tier.  Needed to compute cost.
     */
    public SMATemplate(final long oid,
                       @Nonnull final String name,
                       @Nonnull final String family,
                       final int coupons,
                       @Nonnull final SMAContext context,
                       TopologyEntityDTO computeTier
                       ) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name is null");
        this.family = Objects.requireNonNull(family, "family is null");
        this.coupons = coupons;
        this.context = Objects.requireNonNull(context);
        this.computeTier = computeTier;
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

    public int getCoupons() {
        return coupons;
    }

    public TopologyEntityDTO getComputeTier() {
        return computeTier;
    }

    /**
     * Given a business account ID, set the on-demand cost.
     * @param businessAccountId business account ID
     * @param cost cost
     */
    public void setOnDemandCost(long businessAccountId, @Nonnull SMACost cost) {
        onDemandCosts.put(businessAccountId, Objects.requireNonNull(cost));
    }

    /**
     * Given a business account ID, set the discounted cost.
     * @param businessAccountId business account ID
     * @param cost discounted cost
     */
    public void setDiscountedCost(long businessAccountId, @Nonnull SMACost cost) {
        this.discountedCosts.put(businessAccountId, Objects.requireNonNull(cost));
    }

    /**
     * Lookup the on-demand total cost for the business account.
     * @param businessAccountId the business account ID.
     * @return on-demand total cost or 0 if not found.
     */
    public float getOnDemandTotalCost(long businessAccountId) {
        SMACost cost = onDemandCosts.get(businessAccountId);
        if (cost == null) {
            logger.warn("getOnDemandTotalCost: OID={} name={} has no discounted cost for businessAccountId={}!",
                oid, name, businessAccountId);
            return 0f;
        }
        return SMAUtils.round(cost.getTotal());
    }


    /**
     * Lookup the discounted total cost for the business account.
     * @param businessAccountId the business account ID.
     * @return discounted total cost or 0 if not found.
     */
    public float getDiscountedTotalCost(long businessAccountId) {
        SMACost cost = discountedCosts.get(businessAccountId);
        if (cost == null) {
            logger.warn("getDiscountedTotalCost: OID={} name={} has no discounted cost for businessAccountId={}!",
                oid, name, businessAccountId);
            return 0f;
        }
        return SMAUtils.round(cost.getTotal());
    }

    /**
     * compute the net cost based on discounted coupons and onDemandCost.
     * For AWS, the cost is only non-discounted portion times the onDemand cost.
     * For Azure, their may be a non zero discounted cost, which is applied to the discounted coupons.
     *
     * @param businessAccountId business account ID
     * @param discountedCoupons discounted coupons
     * @return cost after applying discounted coupons.
     */
    public float getNetCost(long businessAccountId, float discountedCoupons) {
        float netCost = 0f;
        if (discountedCoupons > coupons || coupons == 0) {
            netCost = getDiscountedTotalCost(businessAccountId);
        } else {
            float discountPercentage = discountedCoupons / coupons;
            netCost = (getDiscountedTotalCost(businessAccountId) * discountPercentage) +
                    (getOnDemandTotalCost(businessAccountId) * (1 - discountPercentage));
        }
        return SMAUtils.round(netCost);
    }

    @Override
    public String toString() {
        return "SMATemplate{" +
                "OID='" + oid + "'" +
                ", name='" + name + '\'' +
                ", family='" + family + '\'' +
                ", onDemandCosts=" + onDemandCosts.size() +
                ", discountedCosts=" + discountedCosts.size() +
                ", coupons=" + coupons +
                '}';
    }
}
