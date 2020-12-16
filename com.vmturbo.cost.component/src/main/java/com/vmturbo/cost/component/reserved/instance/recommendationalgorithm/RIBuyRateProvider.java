package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * This class provides a lookup for on-demand and RI spec rates based on the demand context and
 * target purchase account.
 */
@ThreadSafe
public class RIBuyRateProvider {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Hours in a month.
     */
    public static final int HOURS_IN_A_MONTH = 730;

    /**
     * Months in a year.
     */
    public static final int MONTHS_IN_A_YEAR = 12;

    /*
     * Internal data structures populated at construction time to represent the data in
     * PriceTableStore,  ReservedInstanceBoughtStore and ReservedInstanceSpecStore.
     */
    //Internal data structures for accessing on-Demand Prices by BusinessAccount and RegionalContex oid
    private final PriceHolder<OnDemandPriceTable, PriceTable> onDemandRates;

    //Internal data structures for accessing RI Prices by BusinessAccount and Spec oid
    private final PriceHolder<ReservedInstancePrice, ReservedInstancePriceTable>
            reservedInstanceSpecRates;

    //PriceTable oid -> BusinessAccount oid mapping
    private final Map<Long, Long> priceTableKeyOidByBusinessAccountOid;

    /**
     * Constructor.  Build all the internal data structures.
     * @param priceTableStore price table store
     * @param baPriceTableStore Business Account oids to their respective price table key store
     * @param primaryAccounts list of BAs to use for price lookup
     */
    public RIBuyRateProvider(@Nonnull PriceTableStore priceTableStore,
            @Nonnull BusinessAccountPriceTableKeyStore baPriceTableStore,
            Set<Long> primaryAccounts) {

        Objects.requireNonNull(priceTableStore);
        Objects.requireNonNull(baPriceTableStore);

        // compute internal data structures
        priceTableKeyOidByBusinessAccountOid = ImmutableMap.copyOf(
                baPriceTableStore.fetchPriceTableKeyOidsByBusinessAccount(primaryAccounts));

        Set<Long> priceTablesOids = priceTableKeyOidByBusinessAccountOid.values().stream()
                .collect(Collectors.toSet());

        onDemandRates = new PriceHolder<>(priceTableStore.getPriceTables(priceTablesOids),
                priceTableKeyOidByBusinessAccountOid, PriceTable::getOnDemandPriceByRegionIdMap);

        reservedInstanceSpecRates =
                new PriceHolder<>(priceTableStore.getRiPriceTables(priceTablesOids),
                        priceTableKeyOidByBusinessAccountOid,
                        ReservedInstancePriceTable::getRiPricesBySpecIdMap);
    }

    /**
     * Looks up the on-demand and RI spec rates for a {@link RIBuyRegionalContext}. Note: this method
     * still needs to lookup rates for a target purchase account, instead of using a merged price
     * table.
     *
     * @param primaryAccountOid account oid
     * @param regionalContext The regional context
     * @return The pricing provider results or null if either the on-demand or RI rates cannot be found.
     */
    @Nullable
    public PricingProviderResult findRates(long primaryAccountOid,
            @Nonnull RIBuyRegionalContext regionalContext) {
        float onDemandRate = lookupOnDemandRate(primaryAccountOid, regionalContext);
        if (onDemandRate == 0) {
            return null;
        }
        Pair<Float, Float> riPairOfRates =
                lookupReservedInstanceRate(primaryAccountOid,
                        regionalContext.riSpecToPurchase(),
                        regionalContext.analysisTag());
        if (riPairOfRates.getLeft() == Float.MAX_VALUE) {
            return null;
        }

        return ImmutablePricingProviderResult.builder()
                .onDemandRate(onDemandRate)
                .reservedInstanceUpfrontRate(riPairOfRates.getLeft())
                .reservedInstanceRecurringRate(riPairOfRates.getRight())
                .build();
    }

    /**
     * Lookup the rate for the provided compute tier in the regional context.
     * An ISF RI Buy recommendation can have different compute tiers covered.
     * We want to calculate the on demand cost of each of those different compute tier demand.
     * The ReservedInstanceRegionalContext is constructed using the compute tier of the current
     * template.
     *
     * @param primaryAccountOid account oid
     * @param regionalContext The regional context
     * @param computerTier The target compute tier
     * @return The on-demand rate for the target computer tier or 0.0, if the on-demand rate cannot
     * be determined.
     */
    public float lookupOnDemandRate(long primaryAccountOid,
            @Nonnull RIBuyRegionalContext regionalContext,
            @Nonnull TopologyEntityDTO computerTier) {

        float onDemandRate = 0f;

        final OnDemandPriceTable onDemandPriceTable =
                onDemandRates.get(primaryAccountOid, regionalContext.regionOid());
        if (onDemandPriceTable == null) {
            logger.warn("{}lookupOnDemandRate() could not find on-demand rates for region in context={}",
                    regionalContext.analysisTag(), regionalContext.contextTag());
            return onDemandRate;
        }
        Map<Long, ComputeTierPriceList> onDemandMapByTier = onDemandPriceTable.getComputePricesByTierIdMap();
        if (onDemandMapByTier == null) {
            logger.warn("{}lookupOnDemandRate() could not find on-demand rate by tier in context={}",
                    regionalContext.analysisTag(), regionalContext.contextTag());
            return onDemandRate;
        }
        ComputeTierPriceList computeTierRates = onDemandMapByTier.get(computerTier.getOid());
        if (computeTierRates == null) {
            logger.warn("{}getRates() could not find on-demand rate by tier={}({}) in region={}",
                    regionalContext.analysisTag(),
                    computerTier.getDisplayName(),
                    computerTier.getOid(),
                    regionalContext.regionOid());
            return onDemandRate;
        }
        ComputeTierConfigPrice baseRate = computeTierRates.getBasePrice();
        if (baseRate == null) {
            logger.warn("{}lookupOnDemandRate() could not find on-demand base rate for tier={} in region={}",
                    regionalContext.analysisTag(),
                    computerTier.getDisplayName(),
                    regionalContext.regionOid());
            return onDemandRate;
        }
        List<Price> prices = baseRate.getPricesList();
        if (prices.size() > 1) {
            logger.warn("{}lookupOnDemandRate for accountID={} regionId={} tier={} prices.size()={} > 1",
                    regionalContext.analysisTag(),
                    regionalContext.accountGroupingId().tag(),
                    regionalContext.regionOid(),
                    computerTier.getDisplayName(),
                    prices.size());
        }
        for (Price price : prices) {
            onDemandRate += computeOnDemandRate(price, regionalContext, "base");
        }

        final ReservedInstanceSpecInfo riSpecInfo = regionalContext.riSpecToPurchase().getReservedInstanceSpecInfo();
        for (ComputeTierConfigPrice adjustment:  computeTierRates.getPerConfigurationPriceAdjustmentsList()) {
            if (adjustment.getTenancy() == riSpecInfo.getTenancy() &&
                    // If this RI is platform flexible, no rate will match the UNKNOWN OS type of the
                    // RI spec. Therefore, we will only use the base rate
                    adjustment.getGuestOsType() == riSpecInfo.getOs()) {
                for (Price price : adjustment.getPricesList()) {
                    onDemandRate += computeOnDemandRate(price, regionalContext, "adjustment");
                }
            }
        }
        logger.debug("{}lookupOnDemandRate() onDemandRate={} for context={}",
                regionalContext.analysisTag(), onDemandRate,
                regionalContext.contextTag());
        return onDemandRate;
    }

    /**
     * Find the on-demand rate for this regional context.
     *
     * @param primaryAccountOid account oid
     * @param regionalContext regional context.
     * @return on-demand rate or 0 if not found.
     */
    public float lookupOnDemandRate(long primaryAccountOid,
            @Nonnull RIBuyRegionalContext regionalContext) {
        return lookupOnDemandRate(primaryAccountOid, regionalContext,
                regionalContext.computeTier());
    }

    /**
     * Given a Price, compute the hourly rate.
     *
     * @param price the price from the price table
     * @param regionalContext regional context (for logging)
     * @param type base or adjustment for logging
     * @return onDemand rate
     */
    private float computeOnDemandRate(
            @Nonnull Price price,
            @Nonnull RIBuyRegionalContext regionalContext,
            @Nonnull String type) {
        float rate = 0;
        Unit unit = price.getUnit();
        CurrencyAmount currencyAmount = price.getPriceAmount();
        if (!currencyAmount.hasAmount()) {
            logger.warn("{}computeOnDemandRate() type={} currencyAmount={} has no amount for context={}",
                    regionalContext.analysisTag(), type, currencyAmount, regionalContext.contextTag());
            return rate;
        }
        double amount = currencyAmount.getAmount();
        int divisor = 1;
        if (unit == Unit.MONTH) {
            divisor = HOURS_IN_A_MONTH;
        } else if (unit == Unit.TOTAL) {
            logger.warn("{}computeOnDemandRate() type={} unit == Unit.TOTAL for context={}",
                    regionalContext.analysisTag(), type, regionalContext.contextTag());
            return rate;
        } else if (unit != Unit.HOURS) {
            logger.warn("{}computeOnDemandRate() type={} unit != Unit.HOURS for context={}",
                    regionalContext.analysisTag(), type, regionalContext.contextTag());
            return rate;
        }
        rate = new Double(amount / divisor).floatValue();
        logger.trace("{}computeOnDemandRate() type={} hourly rate={} divisor={} for context={}",
                regionalContext.analysisTag(), type, rate, divisor, regionalContext.contextTag());
        return rate;
    }

    private Pair<Float, Float> lookupReservedInstanceRate(long primaryAccountOid,
            @Nonnull ReservedInstanceSpec riSpec, @Nonnull String logTag) {

        final ReservedInstancePrice riPrice =
                reservedInstanceSpecRates.get(primaryAccountOid, riSpec.getId());
        if (riPrice == null) {
            logger.warn("lookupReservedInstanceRate() can't find rate for ReservedInstanceSpecId={}",
                    riSpec.getId());
            return Pair.of(Float.MAX_VALUE, 0f);
        }
        Price upFrontPrice = riPrice.getUpfrontPrice();
        Price hourlyPrice = riPrice.getRecurringPrice();
        CurrencyAmount upFrontCurrencyAmount = upFrontPrice.getPriceAmount();
        CurrencyAmount hourlyCurrencyAmount = hourlyPrice.getPriceAmount();
        double upFrontAmount = upFrontCurrencyAmount.getAmount();
        double hourlyAmount = hourlyCurrencyAmount.getAmount();
        float upFrontAmortizedCost = 0f;
        if (upFrontAmount > 0) {
            final int riSpecTermInYears = riSpec.getReservedInstanceSpecInfo()
                    .getType()
                    .getTermYears();
            upFrontAmortizedCost = new Double(upFrontAmount).floatValue() /
                (riSpecTermInYears * MONTHS_IN_A_YEAR * HOURS_IN_A_MONTH);
        }
        float riRate = upFrontAmortizedCost + new Double(hourlyAmount).floatValue();
        logger.debug("{}lookupReservedInstanceRate() riRate={} = hourlyAmount={} + upFrontAmortized{} (upFront={}) for specId={}",
            logTag, riRate, hourlyAmount, upFrontAmortizedCost, upFrontAmount, riSpec.getId());
        final Pair pair = Pair.of(upFrontAmortizedCost, new Double(hourlyAmount).floatValue());
        return pair;
    }


    /**
     * A pricing result for on-demand and RI rate lookup.
     */
    @Value.Immutable
    public interface PricingProviderResult {

        /**
         * The on-demand rate of the target compute tier.
         *
         * @return The on-demand rate.
         */
        float onDemandRate();

        /**
         * The up-front rate for an RI spec, amortized over the term. For example, if the up-front
         * cost is $12 and it's a one year RI spec, this method with return $1.
         *
         * @return The up-front rate, amoritzed over the life of the RI spec.
         */
        float reservedInstanceUpfrontRate();

        /**
         * The recurring rate for an RI spec.
         *
         * @return The recurring rate.
         */
        float reservedInstanceRecurringRate();

        /**
         * The amortized rate for the target RI spec.
         *
         * @return The amortized rate for the RI spec.
         */
        @Value.Derived
        default float reservedInstanceRate() {
            return reservedInstanceUpfrontRate() + reservedInstanceRecurringRate();
        }
    }
}
