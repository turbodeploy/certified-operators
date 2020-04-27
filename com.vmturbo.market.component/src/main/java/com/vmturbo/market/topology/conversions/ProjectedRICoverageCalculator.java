package com.vmturbo.market.topology.conversions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.market.topology.RiDiscountedMarketTier;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommodityBoughtTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Calculates the projected RI Coverage.
 */
public class ProjectedRICoverageCalculator {
    private static final Logger logger = LogManager.getLogger();
    private final Map<Long, TraderTO> oidToOriginalTraderTOMap;
    private final CloudTopologyConverter cloudTc;
    private final CommodityConverter commodityConverter;
    // A map of the topology entity dto id to its reserved instance coverage
    private final Map<Long, EntityReservedInstanceCoverage> projectedReservedInstanceCoverage = Maps.newHashMap();

    /**
     * Used to calculates the projected RI coverage
     *
     * @param oidToOriginalTraderTOMap mapping of oid to traders which were input into m2
     * @param cloudTc cloud topology converter
     * @param commodityConverter commodity converter
     */
    public ProjectedRICoverageCalculator(@Nonnull Map<Long, TraderTO> oidToOriginalTraderTOMap,
                                         @Nonnull CloudTopologyConverter cloudTc,
                                         @Nonnull CommodityConverter commodityConverter) {
        this.oidToOriginalTraderTOMap = oidToOriginalTraderTOMap;
        this.cloudTc = cloudTc;
        this.commodityConverter = commodityConverter;
    }

    /**
     * Calculates the projected RI coverage for the given projected trader and adds it to
     * projectedReservedInstanceCoverage.
     *
     * @param projectedTrader the projected trader for which RI Coverage is to be created
     */
    public void calculateProjectedRiCoverage(@Nonnull TraderTO projectedTrader) {
        // Calculate projected RI Coverage
        final TraderTO originalTraderTO = oidToOriginalTraderTOMap.get(projectedTrader.getOid());
        // original trader can be null in case of provisioned entity
        if (originalTraderTO != null) {
            final Optional<EntityReservedInstanceCoverage> originalRiCoverage =
                cloudTc.getRiCoverageForEntity(originalTraderTO.getOid());

            // projected RI coverage does not support provisioned traders, due to requiring
            // the original trader state for correct RI coverage capacity calculation. Currently,
            // cloud entities cannot be provisioned.
            calculateProjectedRiCoverage(originalTraderTO, projectedTrader, originalRiCoverage);
        }
    }

    /**
     * Calculates the projected RI coverage for the given projected trader and adds it to
     * projectedReservedInstanceCoverage.
     * The existing RI Coverage is used if the trader stayed on the same RI and the coverage
     * remained the same.
     * A new projected coverage is created in these cases:
     * 1. If the trader is a new trader (like in add workload plan), and is placed on an RITier.
     * 2. If the trader moved from on demand tier to RI tier.
     * 3. If the trader moved from RI tier 1 to RI tier 2. In this case, use coupons of RI2. Coupons
     * used by trader of RI1 were already relinquished in the relinquishCoupons method.
     * 4. If the trader stayed on same RI, but coverage changed.
     *
     * @param originalTraderTO the original trader (before going into market). The original trader is
     *                         used to determine the projected trader's state (IDLE VMs will be converted
     *                         to ACTIVE in the market) and to determine whether the RI coverage has
     *                         changed.
     * @param projectedTraderTO the projected trader for which RI Coverage is to be created
     * @param originalRiCoverage the original trader's original RI Coverage (before going into market)
     */
    private void calculateProjectedRiCoverage(@Nonnull TraderTO originalTraderTO,
                                              @Nonnull TraderTO projectedTraderTO,
                                              @Nonnull Optional<EntityReservedInstanceCoverage> originalRiCoverage) {
        final long entityId = projectedTraderTO.getOid();
        boolean isValidProjectedTrader = isValidProjectedTraderForCoverageCalculation(projectedTraderTO);
        if (isValidProjectedTrader) {
            final Optional<Integer> projectedCoverageCapacity =
                cloudTc.getReservedInstanceCoverageCapacity(projectedTraderTO, originalTraderTO.getState());
            if (projectedCoverageCapacity.map(c -> c > 0).orElse(false)) {
                final EntityReservedInstanceCoverage.Builder riCoverageBuilder =
                    EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(entityId)
                        .setEntityCouponCapacity(projectedCoverageCapacity.get());
                // Are any of the shopping lists of the projected trader placed on discounted tier?
                final Optional<ShoppingListTO> projectedRiTierSl =
                    getShoppingListSuppliedByRiTier(projectedTraderTO, true);
                if (projectedRiTierSl.isPresent()) {
                    // Destination is RI. Get the projected RI Tier and the projected coupon comm bought.
                    RiDiscountedMarketTier projectedRiTierDestination = (RiDiscountedMarketTier)
                        cloudTc.getMarketTier(projectedRiTierSl.get().getCouponId());

                    Optional<CommodityBoughtTO> projectedCouponCommBought = getCouponCommBought(
                        projectedRiTierSl.get());
                    if (!projectedCouponCommBought.isPresent()) {
                        logger.error("Projected trader {} is placed on RI {}, but it's RI shopping list does not have " +
                                "projectedCouponCommBought",
                            projectedTraderTO.getDebugInfoNeverUseInCode(),
                            projectedRiTierDestination != null ? projectedRiTierDestination.getDisplayName() : "");
                        return;
                    }
                    final Optional<MarketTier> originalRiTierDestination =
                        originalTraderTO.getShoppingListsList()
                            .stream()
                            .map(sl -> sl.getSupplier())
                            .map(traderId -> cloudTc.getMarketTier(traderId))
                            .filter(Objects::nonNull)
                            .filter(mt -> mt.hasRIDiscount())
                            .findFirst();

                    if (!originalRiTierDestination.isPresent() ||
                        !originalRiTierDestination.get().equals(projectedRiTierDestination)) {
                        // Entity is initially placed on an riTier
                        // OR has moved from a regular tier to an riTier
                        // OR from one riTer to another.
                        // So, in all these cases, create a new RICoverage object.
                        // The information of the number of coupons to use is present in the coupon
                        // commodity bought of the trader
                        final Map<Long, Double> riIdtoCouponsOfRIUsed = projectedRiTierDestination
                            .useCoupons(entityId, projectedCouponCommBought.get().getQuantity());
                        riCoverageBuilder.putAllCouponsCoveredByRi(riIdtoCouponsOfRIUsed);
                    } else {
                        // Entity stayed on the same RI Tier. Check if the coverage changed.
                        if (!originalRiCoverage.isPresent()) {
                            logger.error("{} does not have original RI coverage", originalTraderTO.getDebugInfoNeverUseInCode());
                            return;
                        }

                        float projectedNumberOfCouponsBought = projectedCouponCommBought.get().getQuantity();
                        float originalNumberOfCouponsBought = TopologyConversionUtils.getTotalNumberOfCouponsCovered(originalRiCoverage.get());
                        // If original == projected, the original claimed RIs were not relinquished
                        // in relinquishCoupons(). Therefore, the projectedRiTierDestination does
                        // not need to be updated here
                        if (!TopologyConversionUtils.areFloatsEqual(
                            projectedCouponCommBought.get().getQuantity(),
                            originalNumberOfCouponsBought)) {
                            // Coverage changed. Create a new RICoverage object
                            final Map<Long, Double> riIdtoCouponsOfRIUsed = projectedRiTierDestination
                                .useCoupons(entityId, projectedNumberOfCouponsBought);
                            riCoverageBuilder.putAllCouponsCoveredByRi(riIdtoCouponsOfRIUsed);
                        } else {
                            // Coverage did not change. Use the original ri coverage.
                            riCoverageBuilder.putAllCouponsCoveredByRi(
                                originalRiCoverage.get().getCouponsCoveredByRiMap());
                        }
                    }
                }
                projectedReservedInstanceCoverage.put(entityId, riCoverageBuilder.build());
            }
        } else if (originalRiCoverage.isPresent()) {
            // If we are here, the projected trader is not valid and the entity was originally on
            // an RI. So we copy the same coverage.
            // Since the EntityReservedInstanceCoverage is a protobuf, it is immutable and hence
            // safe to copy into projectedReservedInstanceCoverage.
            projectedReservedInstanceCoverage.put(entityId, originalRiCoverage.get());
        }
    }

    /**
     * This method goes over all the traders and relinquishes coupons of the
     * RIDiscountedMarketTiers in 3 cases. If a trader moved
     * 1. From RI to On demand. Relinquish the coupons the trader was using of the RI.
     * 2. From RI1 to RI2.  Relinquish the coupons the trader was using of the RI1.
     * 3. From RI1 to RI1 with change in coverage.  Relinquish the original number of coupons the
     * trader was using of the RI1.
     *
     * @param projectedTraders All the projected traders
     */
    public void relinquishCoupons(@Nonnull final List<TraderTO> projectedTraders) {
        for (TraderTO projectedTrader : projectedTraders) {
            TraderTO originalTrader = oidToOriginalTraderTOMap.get(projectedTrader.getOid());
            // Original trader might be null in case of a provisioned trader
            if (originalTrader != null && isValidProjectedTraderForCoverageCalculation(projectedTrader)) {
                // If the VM was using an RI before going into market, then original trader will
                // have a shopping list supplied by RIDiscountedMarketTier because in this case
                // while constructing shopping lists of original trader, we make the supplier of
                // the compute shopping list as the trader representing RIDiscountedMarketTier
                Optional<ShoppingListTO> originalRiTierSl = getShoppingListSuppliedByRiTier(originalTrader, false);
                if (originalRiTierSl.isPresent()) {
                    // Originally trader was placed on RI
                    RiDiscountedMarketTier originalRiTier = (RiDiscountedMarketTier)
                        cloudTc.getMarketTier(originalRiTierSl.get().getSupplier());
                    Optional<EntityReservedInstanceCoverage> originalRiCoverage =
                        cloudTc.getRiCoverageForEntity(originalTrader.getOid());
                    if (!originalRiCoverage.isPresent()) {
                        logger.error("{} does not have original RI coverage", originalTrader.getDebugInfoNeverUseInCode());
                        return;
                    }
                    final Set<Long> originalRiIds = originalRiCoverage.get()
                        .getCouponsCoveredByRiMap().keySet();
                    final Set<RiDiscountedMarketTier> originalRiTiers = originalRiIds.stream()
                        .map(riId -> cloudTc.getRiDataById(riId))
                        .filter(Objects::nonNull)
                        .map(riData ->
                            cloudTc.getRIDiscountedMarketTierIDFromRIData(riData))
                        .filter(Objects::nonNull)
                        .map(cloudTc::getMarketTier)
                        .filter(Objects::nonNull)
                        .filter(RiDiscountedMarketTier.class::isInstance)
                        .map(RiDiscountedMarketTier.class::cast)
                        .collect(Collectors.toSet());
                    logger.debug("Original RIDiscountedMarketTiers for projected trader {} are {}",
                        originalTrader::getDebugInfoNeverUseInCode,
                        () -> originalRiTiers.stream()
                            .map(tier -> tier.getRiAggregate().getDisplayName())
                            .collect(Collectors.toList()));
                    Optional<ShoppingListTO> projectedRiTierSl = getShoppingListSuppliedByRiTier(projectedTrader, true);
                    if (projectedRiTierSl.isPresent() && originalRiTier != null) {
                        if (projectedRiTierSl.get().getCouponId() != originalRiTierSl.get().getSupplier()) {
                            // Entity moved from one RI to another.
                            originalRiTiers.forEach(ri ->
                                ri.relinquishCoupons(originalRiCoverage.get()));
                        } else {
                            // Entity stayed on same RI. Did coverage change? If yes relinquish
                            Optional<CommodityBoughtTO> projectedCouponCommBought = getCouponCommBought(projectedRiTierSl.get());
                            if (!projectedCouponCommBought.isPresent()) {
                                // We use the original trader in this error message here because the projected trader does not have debug info
                                logger.error("{} does not have projected coupon commodity bought.", originalTrader.getDebugInfoNeverUseInCode());
                                return;
                            }
                            float originalNumberOfCouponsBought = TopologyConversionUtils.getTotalNumberOfCouponsCovered(originalRiCoverage.get());
                            if (!TopologyConversionUtils.areFloatsEqual(
                                projectedCouponCommBought.get().getQuantity(),
                                originalNumberOfCouponsBought)) {
                                originalRiTiers.forEach(ri ->
                                    ri.relinquishCoupons(originalRiCoverage.get()));
                            }
                        }
                    } else {
                        // Moved from RI to on demand
                        originalRiTiers.forEach(ri ->
                            ri.relinquishCoupons(originalRiCoverage.get()));
                    }
                }
            }
        }
    }

    /**
     * Adds the provided Buy RI coverage to the projected RI coverage.
     * @param entityBuyRICoverage The RI coverage to add, formatted as
     * {@literal <Entity OID, RI ID, Coverage Amount>}
     */
    public void addBuyRICoverageToProjectedRICoverage(@Nonnull Table<Long, Long, Double> entityBuyRICoverage) {
        entityBuyRICoverage.rowMap().forEach((entityOid, buyRICoverage) -> {
            if (projectedReservedInstanceCoverage.containsKey(entityOid)) {
                final EntityReservedInstanceCoverage entityRICoverage =
                    projectedReservedInstanceCoverage.get(entityOid);

                projectedReservedInstanceCoverage.put(entityOid,
                    entityRICoverage.toBuilder()
                        .putAllCouponsCoveredByBuyRi(buyRICoverage)
                        .build());
                logger.debug("Updated projected RI coverage for entity with buy RI coverage" +
                        "(Entity OID={}, Buy RI coverage={})",
                    () -> entityOid,
                    () -> Joiner.on(", ")
                        .withKeyValueSeparator("=>")
                        .join(buyRICoverage));
            } else {
                // It is expected that calculateProjectedRiCoverage() will add a record
                // for every entity in the projected topology
                logger.error("Projected RI coverage for entity could not be found in adding buy RI coverage" +
                    "(Entity OID={})", entityOid);
            }
        });
    }

    /**
     * Adds the provided Existing RI coverage to the projected RI coverage.
     * @param entityExistingRICoverage The RI coverage to add to projected coverage.
     */
    public void addRICoverageToProjectedRICoverage(@Nonnull Map<Long, EntityReservedInstanceCoverage> entityExistingRICoverage) {
        // With Market not running as in OCP Plan Option #3, the projected RI Coverage map is empty.
        projectedReservedInstanceCoverage.putAll(entityExistingRICoverage);
        logger.debug("Updated projected RI coverage for entity with existing RI coverage");
    }

    /**
     * Gets the projected RI coverage for an entity.
     * @param entityId the id of the entity for which projectedRICoverage is needed.
     *
     * @return A map of entity id to its projected reserved instance coverage
     */
    public EntityReservedInstanceCoverage getProjectedRICoverageForEntity(long entityId) {
        return projectedReservedInstanceCoverage.get(entityId);
    }

    /**
     * Gets the unmodifiable version of projected RI coverage.
     *
     * @return A map of entity id to its projected reserved instance coverage
     */
    public Map<Long, EntityReservedInstanceCoverage> getProjectedReservedInstanceCoverage() {
        return Collections.unmodifiableMap(projectedReservedInstanceCoverage);
    }

    /**
     * Is the projectedTraderTO valid?
     * It is considered invalid if its compute SL is supplied by RI. It should only be supplied
     * by a regular compute tier. Inside m2, we replace the CBTP by TP in AnalysisToProtobuf::replaceNewSupplier.
     *
     * @param projectedTraderTO the trader to check if it is valid
     * @return True if the projected trader is valid. False otherwise.
     */
    private boolean isValidProjectedTraderForCoverageCalculation(TraderTO projectedTraderTO) {
        Optional<MarketTier> computeSupplier = cloudTc.getComputeTier(projectedTraderTO);
        if (computeSupplier.isPresent() && computeSupplier.get().hasRIDiscount()) {
            logger.warn("Projected trader for {} has compute SL supplied by " +
                    "RI {} instead of on-demand tier. This may be because of undiscovered costs " +
                    "or very restrictive tier exclusion policy causing reconfigure action on the VM.",
                projectedTraderTO.getDebugInfoNeverUseInCode(), computeSupplier.get().getDisplayName());
            return false;
        }
        return true;
    }

    /**
     * Finds the shopping list of trader which is supplied by a RI Tier. RI Tier is an
     * RiDiscountedMarketTier.
     *
     * @param trader the trader for whose shopping lists will be scanned
     * @param isProjectedTrader is the trader a projected trader
     * @return Optional of the ShoppingListTO
     */
    private Optional<ShoppingListTO> getShoppingListSuppliedByRiTier(@Nonnull TraderTO trader, boolean isProjectedTrader) {
        if (isProjectedTrader) {
            return trader.getShoppingListsList().stream().filter(sl -> sl.hasCouponId()).findFirst();
        } else {
            return trader.getShoppingListsList().stream().filter(sl -> sl.hasSupplier() &&
                cloudTc.getRiDiscountedMarketTier(sl.getSupplier()).isPresent()).findFirst();
        }
    }

    /**
     * Finds the Coupon comm bought from a shopping list.
     *
     * @param sl the shopping list whose commodities are scanned for
     * @return Optional of CommodityBoughtTO
     */
    private Optional<CommodityBoughtTO> getCouponCommBought(@Nonnull ShoppingListTO sl) {
        return sl.getCommoditiesBoughtList().stream().filter(c -> commodityConverter
            .marketToTopologyCommodity(c.getSpecification())
            .orElse(CommodityType.getDefaultInstance()).getType() ==
            CommodityDTO.CommodityType.COUPON_VALUE).findFirst();
    }
}