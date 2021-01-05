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
    private final int coupons;

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

    /**
     * Constructor of the SMATemplate.
     *
     * @param oid the oid of the template. unique per context
     * @param name the template name
     * @param family the family the template belongs to.
     * @param coupons the number of the coupons needed for SMA template.
     * @param computeTier link back to XL data structures for compute tier.  Needed to compute cost.
     */
    public SMATemplate(final long oid,
                       @Nonnull final String name,
                       @Nonnull final String family,
                       final int coupons,
                       TopologyEntityDTO computeTier
                       ) {
        this.oid = oid;
        this.name = Objects.requireNonNull(name, "name is null");
        this.family = Objects.requireNonNull(family, "family is null");
        this.coupons = coupons;
        this.computeTier = computeTier;
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

    public int getCoupons() {
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
     * Lookup the on-demand total cost for the business account.
     * @param costContext instance containing all the parameters for cost lookup.
     * @return on-demand total cost or Float.MAX_VALUE if not found.
     */
    public float getOnDemandTotalCost(CostContext costContext) {
        Map<OSType, SMACost> costMap = onDemandCosts.get(costContext.getBusinessAccount());
        SMACost cost = costMap != null ? costMap.get(costContext.getOsType()) : null;
        if (cost == null) {
            logger.debug("getOnDemandTotalCost: OID={} name={} has no on demand cost for {}",
                oid, name, costContext);
            return Float.MAX_VALUE;
        }
        return getOsLicenseModelBasedCost(cost, costContext.getOsLicenseModel());
    }

    /**
     * Lookup the discounted total cost for the business account.
     *
     * @param costContext instance containing all the parameters for cost lookup.
     * @return discounted total cost or Float.MAX_VALUE if not found.
     */
    public float getDiscountedTotalCost(CostContext costContext) {
        Map<OSType, SMACost> costMap = discountedCosts.get(costContext.getBusinessAccount());
        SMACost cost = costMap != null ? costMap.get(costContext.getOsType()) : null;
        if (cost == null) {
            logger.debug("getDiscountedTotalCost: OID={} name={} has no discounted cost for {}",
                oid, name, costContext);
            return Float.MAX_VALUE;
        }
        return getOsLicenseModelBasedCost(cost, costContext.getOsLicenseModel());
    }

    private static float getOsLicenseModelBasedCost(final SMACost cost,
                                                    final LicenseModel osLicenseModel) {
        if (osLicenseModel == LicenseModel.LICENSE_INCLUDED) {
            return SMAUtils.round(cost.getTotal());
        } else {
            return SMAUtils.round(cost.getCompute());
        }
    }

    /**
     * compute the net cost based on discounted coupons and onDemandCost.
     * For AWS, the cost is only non-discounted portion times the onDemand cost.
     * For Azure, their may be a non zero discounted cost, which is applied to the discounted coupons.
     *
     * @param costContext instance containing all the parameters for cost lookup.
     * @param discountedCoupons discounted coupons
     * @return cost after applying discounted coupons.
     */
    public float getNetCost(CostContext costContext, float discountedCoupons) {
        final float netCost;
        if (coupons == 0) {
            // If a template has 0 coupons, then it can't be discounted by a RI, and the on-demand
            // cost is returned.  E.g. Standard_A2m_v2.
            netCost = getOnDemandTotalCost(costContext);
        } else if (discountedCoupons >= coupons) {
            netCost = getDiscountedTotalCost(costContext);
        } else {
            float discountPercentage = discountedCoupons / coupons;
            netCost = (getDiscountedTotalCost(costContext) * discountPercentage)
                    + (getOnDemandTotalCost(costContext) * (1 - discountPercentage));
        }
        return SMAUtils.round(netCost);
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
            '}';
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
            '}';
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
