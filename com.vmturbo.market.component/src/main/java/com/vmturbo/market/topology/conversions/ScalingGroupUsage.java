package com.vmturbo.market.topology.conversions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.commons.Pair;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.OnDemandMarketTier;
import com.vmturbo.market.topology.RiDiscountedMarketTier;

/**
 * Contains top usage information on a per ScalingGroup basis.
 */
public class ScalingGroupUsage {
    private static final Logger logger = LogManager.getLogger();
    String name_;  // Owning scaling group's name
    Map<Integer, Double> topUsage_;  // maps commodity type to top usage value
    Map<Long, Pair<Long, Class>> providerIdMap_;

    /**
     * Constructor.
     * @param name name of the enclosing ScalingGroup, for tracing purposes.
     */
    public ScalingGroupUsage(final String name) {
        this.name_ = name;
        this.topUsage_ = new HashMap<>();
        this.providerIdMap_ = new HashMap<>();
    }

    /**
     * Save the provider ID of the compute tier for the given OID.
     * @param oid member OID whose provider ID to save.
     * @param providerId provider ID.
     */
    public void setProviderId(final Long oid, final Pair<Long, Class> providerId) {
        if (logger.isTraceEnabled()) {
            logger.trace("Adding provider OID {}/{} to scaling group {} provider map for OID {}",
                providerId.first, providerId.second, name_, oid);
        }
        this.providerIdMap_.put(oid, providerId);
    }

    /**
     * Return the provider ID of the compute tier for the given member OID.
     * @param entityOid member OID to check
     * @return {@link Pair} whose first element is the provider ID, and second element is the class
     * of the provider's type.  Valid types are {@link MarketTier}, {@link RiDiscountedMarketTier},
     * and {@link OnDemandMarketTier}.  If the provider ID is not available, return null.
     */
    public Pair<Long, Class> getProviderId(Long entityOid) {
        Pair<Long, Class> result = providerIdMap_.get(entityOid);
        if (logger.isTraceEnabled()) {
            logger.trace("getProviderId in scaling group {} returned {}",
                name_, result != null
                    ? String.format("%s/%s", result.first, result.second)
                    : "NOT IN CACHE");
        }
        return result;
    }

    /**
     * Get maximum usage for the specified commodity in the scaling group.
     * @param commBought commodity to get usage for.
     * @return {@link Optional} if the max usage is available for the specified commodity, return
     * it, else return empty.
     */
    public Optional<Double> getUsageForCommodity(final CommodityBoughtDTO commBought) {
        Optional<Double> result;
        if (commBought == null) {
            result = Optional.empty();
        } else {
            result = Optional.ofNullable(topUsage_.get(commBought.getCommodityType().getType()));
            if (logger.isTraceEnabled()) {
                logger.trace("getUsageForCommodity type {} in scaling group {} returned {}",
                    commBought.getCommodityType().getType(), name_,
                    result.isPresent() ? result.get() : "NOT IN CACHE");
            }
        }
        return result;
    }

    /**
     * Add usage for a commodity bought by a scaling group member and track the maximum usage for
     * the group.
     * @param commBought commodity bought
     * @param commBoughtQuantities a pair of quantities (used, peak used).  We do not level the
     *                             peak used, so it is ignored at this time.
     */
    public void addUsage(final CommodityBoughtDTO commBought,
                         final Pair<Float, Float> commBoughtQuantities) {
        Integer commodityType = commBought.getCommodityType().getType();
        Double currentUsage = topUsage_.getOrDefault(commodityType, 0D);
        Double newUsage = Math.max(currentUsage, commBoughtQuantities.first);
        topUsage_.put(commodityType, newUsage);
        if (logger.isTraceEnabled()) {
            logger.trace("Adding precalculated usage for commodity {} to scaling group {}, used = {}, new max used = {}",
                commodityType, name_, currentUsage, newUsage);
        }
    }
}
