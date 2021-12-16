package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine.CostContext;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

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
     * Unique identifier of Template within the scope of a context
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
    private final float coupons;

    /*
     * The compute tier in XL data structures.  Needed to compute costs.
     */
    private TopologyEntityDTO computeTier;

    /*
     * instance variables set outside of the construtor
     */
    /*
     *  Map from business account to on-demand cost
     */
    private Map<Long, Map<OSType, SMACost>> onDemandCosts = new HashMap();

    /*
     * Map from business account to discounted cost (only Azure).
     * For AWS, discountedCosts is always 0.
     */
    private Map<Long, Map<OSType, SMACost>> discountedCosts = new HashMap();

    public Map<Long, Map<OSType, SMACost>> getOnDemandCosts() {
        return onDemandCosts;
    }

    public Map<Long, Map<OSType, SMACost>> getDiscountedCosts() {
        return discountedCosts;
    }

    /**
     * The scaling penalty that will be applied as a tie breaker in the
     * natural template selection when the template prices are the same.
     */
    private final float scalingPenalty;

    /**
     * Constructor of the SMATemplate.
     *
     * @param oid the oid of the template. unique per context
     * @param name the template name
     * @param family the family the template belongs to.
     * @param coupons the number of the coupons needed for SMA template.
     * @param computeTier link back to XL data structures for compute tier.  Needed to compute cost.
     * @param scalingPenalty The scaling penalty from the compute tier info
     *                      that will be applied as a tie breaker.
     */
    public SMATemplate(final long oid,
                       @Nonnull final String name,
                       @Nonnull final String family,
                       final float coupons,
                       TopologyEntityDTO computeTier,
                       final float scalingPenalty
                       ) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name is null");
        this.family = Objects.requireNonNull(family, "family is null");
        this.coupons = coupons;
        this.computeTier = computeTier;
        this.scalingPenalty = scalingPenalty;
    }

    public void setComputeTier(final TopologyEntityDTO computeTier) {
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

    public float getCoupons() {
        return coupons;
    }

    public TopologyEntityDTO getComputeTier() {
        return computeTier;
    }

    /**
     * Given a business account ID, set the on-demand cost.
     * @param businessAccountId business account ID
     * @param osType OS
     * @param cost cost
     */
    public void setOnDemandCost(long businessAccountId, @Nonnull OSType osType, @Nonnull SMACost cost) {
        onDemandCosts.putIfAbsent(businessAccountId, new HashMap<>());
        onDemandCosts.get(businessAccountId).put(osType, Objects.requireNonNull(cost));
    }

    /**
     * Given a business account ID, set the discounted cost.
     * @param businessAccountId business account ID
     * @param osType OS.
     * @param cost discounted cost
     */
    public void setDiscountedCost(long businessAccountId, @Nonnull OSType osType, @Nonnull SMACost cost) {
        this.discountedCosts.putIfAbsent(businessAccountId, new HashMap<>());
        this.discountedCosts.get(businessAccountId).put(osType, Objects.requireNonNull(cost));
    }



    /**
     * Getter for the scalingPenalty.
     *
     * @return the scaling penalty.
     */
    public float getScalingPenalty() {
        return scalingPenalty;
    }


    @Override
    public String toString() {
        return "SMATemplate{" +
            "OID='" + oid + "'" +
            ", name='" + name + '\'' +
            ", family='" + family + '\'' +
            ", coupons=" + coupons +
            ", onDemandCosts=" + onDemandCosts +
            ", discountedCosts=" + discountedCosts +
            ", scalingPenalty=" + scalingPenalty
                + '}';
    }

    /**
     * toString without dumping the details of the cost maps.  Dumps the set of account IDs and
     * OSTypes that there is cost information for.
     * @return the templates fields with the list of business accounts and OSTypes in the on-demand costs.
     */
    public String toStringWithOutCost() {
        return "SMATemplate{" +
            "OID='" + oid + "'" +
            ", name='" + name + '\'' +
            ", family='" + family + '\'' +
            ", coupons=" + coupons +
            ", scalingPenalty=" + scalingPenalty
                + '}';
    }

    /**
     * Determine if two templates are equivalent.  They are equivalent if they have same oid.
     * @return hash code based on oid
     */
    @Override
    public int hashCode() {
        return Objects.hash(oid);
    }

    /**
     * Determine if two templates are equivalent.  They are equivalent if they have same oid.
     * @param obj the other RI
     * @return true if the RI's are equivalent.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SMATemplate that = (SMATemplate)obj;
        return oid == that.oid;
    }
}
