package com.vmturbo.cloud.commitment.analysis.pricing;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.PricingResolver;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CloudRateExtractorFactory;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle.ComputePrice;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * The default implementation of {@link CloudCommitmentPricingAnalyzer}.
 */
public class DefaultPricingAnalyzer implements CloudCommitmentPricingAnalyzer {

    private static final int HOURS_IN_A_MONTH = 730;

    private static final int MONTHS_IN_A_YEAR = 12;

    private final Logger logger = LogManager.getLogger();

    private final CloudRateExtractor cloudRateExtractor;

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology;

    private final Map<Long, AccountPricingData> accountPricingDataMap;

    private final Map<Long, ReservedInstancePriceTable> reservedInstancePriceTableMap;

    private DefaultPricingAnalyzer(@Nonnull CloudRateExtractor cloudRateExtractor,
                                   @Nonnull CloudTopology<TopologyEntityDTO> cloudTierTopology,
                                   @Nonnull Map<Long, AccountPricingData> accountPricingDataMap,
                                   @Nonnull Map<Long, ReservedInstancePriceTable> riPriceTableMap) {

        this.cloudRateExtractor = cloudRateExtractor;
        this.cloudTierTopology = cloudTierTopology;
        this.accountPricingDataMap = accountPricingDataMap;
        this.reservedInstancePriceTableMap = riPriceTableMap;
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public CloudTierPricingData getTierDemandPricing(final ScopedCloudTierInfo cloudTierDemand) {

        final long accountId = cloudTierDemand.accountOid();
        final ComputeTierDemand computeTierDemand = (ComputeTierDemand)cloudTierDemand.cloudTierDemand();
        final long tierOid = computeTierDemand.cloudTierOid();
        final Optional<TopologyEntityDTO> tier = cloudTierTopology.getEntity(tierOid);
        final OSType os = computeTierDemand.osType();

        if (!tier.isPresent()) {
            logger.error("No cloud tier found for demand {}. Skipping calculating pricing data for this demand",
                    cloudTierDemand.cloudTierDemand());
            return CloudTierPricingData.EMPTY_PRICING_DATA;
        }

        if (!accountPricingDataMap.containsKey(accountId)) {
            logger.error("No pricing found for cloud tier demand with account oid {} while"
                    + " running CCA for tier {}", accountId, tier.get().getDisplayName());
            return CloudTierPricingData.EMPTY_PRICING_DATA;
        }
        AccountPricingData accountPricingData = accountPricingDataMap.get(accountId);
        Optional<ComputePrice> onDemandRate = getOnDemandRate(cloudTierDemand, accountPricingData, tier.get(), os,
                cloudRateExtractor);
        if (!onDemandRate.isPresent()) {
            logger.error("No on demand rate found for this cloud tier demand {} in account {}", tier.get().getDisplayName(), accountId);
            return CloudTierPricingData.EMPTY_PRICING_DATA;
        }

        final OptionalDouble reservedLicenseRate = getReservedLicenseRate(accountPricingData, tier.get(), os,
                cloudRateExtractor);
        return ComputeTierPricingData.builder()
                .onDemandComputeRate(onDemandRate.get().hourlyComputeRate())
                .onDemandLicenseRate(onDemandRate.get().hourlyLicenseRate())
                .reservedLicenseRate(reservedLicenseRate)
                .build();
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public CloudCommitmentPricingData getCloudCommitmentPricing(final CloudCommitmentSpecData cloudCommitmentSpecData,
                                                                final Set<Long> potentialPurchasingAccountOids) {

        final Pair<Float, Float> riPairOfRates = getReservedInstanceRate(
                cloudCommitmentSpecData, potentialPurchasingAccountOids);
        if (riPairOfRates == null) {
            logger.error("No cloud commitment spec pricing found for spec {}", cloudCommitmentSpecData.spec().toString());
            return null;
        }
        return RIPricingData.builder()
                .hourlyUpFrontRate(riPairOfRates.getLeft())
                .hourlyRecurringRate(riPairOfRates.getRight())
                .build();
    }

    /**
     * This method resolves the on the demand rate for a given cloud tier demand and applies any discount/price adjustment
     * in the system.
     *
     * @param aggregateCloudTierDemand The cloud tier demand for which we want to get pricing info.
     * @param accountPricingData Account Pricing Data having reference to the discount applicator.
     * @param tier The tier the demand is scoped to.
     * @param osType The OS type of the demand.
     * @param cloudRateExtractor The price table class encapsulating methods to resolve pricing rates.
     *
     * @return An optional float containing the on demand rate for the spec.
     */
    private Optional<ComputePrice> getOnDemandRate(@Nonnull ScopedCloudTierInfo aggregateCloudTierDemand,
                                                   @Nonnull AccountPricingData accountPricingData,
                                                   @Nonnull TopologyEntityDTO tier,
                                                   @Nonnull OSType osType,
                                                   @Nonnull CloudRateExtractor cloudRateExtractor) {

        long regionOid = aggregateCloudTierDemand.regionOid();
        ComputePriceBundle computePriceBundle = cloudRateExtractor.getComputePriceBundle(tier, regionOid, accountPricingData);
        if (computePriceBundle == null || computePriceBundle.getPrices().size() == 0) {
            logger.error("Something went wrong while trying to retrieve the compute tier price list."
                    + " No on demand rates will be returned for this tier {}", tier.getDisplayName());
            return Optional.empty();
        }

        return computePriceBundle.getPrices().stream()
                .filter(price -> (osType == price.osType())
                        || (osType == OSType.UNKNOWN_OS && price.isBasePrice()))
                .findFirst();
    }

    @Nonnull
    private OptionalDouble getReservedLicenseRate(AccountPricingData accountPricingData,
                                                  TopologyEntityDTO tier,
                                                  OSType osType,
                                                  CloudRateExtractor cloudRateExtractor) {

        Set<CoreBasedLicensePriceBundle> reservedLicenseSet = cloudRateExtractor.getReservedLicensePriceBundles(accountPricingData, tier);
        if (reservedLicenseSet == null) {
            logger.error("Something went wrong while trying to retrieve the reserved license price."
                    + " No reserved licensing price will be returned for this cloud tier {}", tier.getDisplayName());
            return OptionalDouble.empty();
        }

        return reservedLicenseSet.stream()
                .filter(CoreBasedLicensePriceBundle::hasPrice)
                .filter(priceBundle -> osType == priceBundle.osType())
                .findFirst()
                .map(priceBundle -> OptionalDouble.of(priceBundle.price().get()))
                .orElse(OptionalDouble.empty());
    }

    /**
     * Iterates over all the RI Price tables and gets the cheapest RI rate.
     *
     * @param cloudCommitmentSpecData The cloud commitment spec data having specific info regarding RIs.
     * @param businessAccountIds The set of business account ids to iterate over to find the chespest rate.
     *
     * @return A pair of RI license rates with the left value being the upfront amortized cost and the
     * right value being the hourly cost.
     */
    private Pair<Float, Float> getReservedInstanceRate(
            @Nonnull CloudCommitmentSpecData<ReservedInstanceSpec> cloudCommitmentSpecData,
            @Nonnull Set<Long> businessAccountIds) {

        float cheapestTotalCost = Integer.MAX_VALUE;
        Pair<Float, Float> pair = null;
        for (Long accountId : businessAccountIds) {
            final ReservedInstancePriceTable riPriceTable = reservedInstancePriceTableMap.get(accountId);
            if (riPriceTable == null) {
                logger.error("No RI price table found for account id {}", accountId);
                continue;
            }
            final ReservedInstancePrice riPrice = riPriceTable.getRiPricesBySpecIdMap().get(
                    cloudCommitmentSpecData.specId());
            if (riPrice == null) {
                logger.warn(
                        "lookupReservedInstanceRate() can't find rate for ReservedInstanceSpecId={}",
                        cloudCommitmentSpecData.specId());
                continue;
            }
            Price upFrontPrice = riPrice.getUpfrontPrice();
            Price hourlyPrice = riPrice.getRecurringPrice();
            CurrencyAmount upFrontCurrencyAmount = upFrontPrice.getPriceAmount();
            CurrencyAmount hourlyCurrencyAmount = hourlyPrice.getPriceAmount();
            double upFrontAmount = upFrontCurrencyAmount.getAmount();
            double hourlyAmount = hourlyCurrencyAmount.getAmount();
            float upFrontAmortizedCost = 0f;
            ReservedInstanceSpecInfo riSpecInfo = cloudCommitmentSpecData.spec().getReservedInstanceSpecInfo();
            if (upFrontAmount > 0) {
                final int riSpecTermInYears = riSpecInfo.getType().getTermYears();
                upFrontAmortizedCost = new Double(upFrontAmount).floatValue() / (riSpecTermInYears * MONTHS_IN_A_YEAR * HOURS_IN_A_MONTH);
            }
            float riRate = upFrontAmortizedCost + new Double(hourlyAmount).floatValue();
            if (riRate < cheapestTotalCost) {
                cheapestTotalCost = riRate;
                pair = Pair.of(upFrontAmortizedCost, new Double(hourlyAmount).floatValue());
                logger.debug(
                        "lookupReservedInstanceRate() riRate={} = hourlyAmount={} + upFrontAmortized{} (upFront={}) for specId={}. updated"
                                + " This is the rate without any discounts.", riRate, hourlyAmount,
                        upFrontAmortizedCost, upFrontAmount, cloudCommitmentSpecData.specId());
            }
            logger.debug(
                    "lookupReservedInstanceRate() riRate={} = hourlyAmount={} + upFrontAmortized{} (upFront={}) for specId={}."
                            + " This is the cheapest rate without any discounts.", riRate,
                    hourlyAmount, upFrontAmortizedCost, upFrontAmount, cloudCommitmentSpecData.specId());
        }
        return pair;
    }

    /**
     * A factory class for creating {@link DefaultPricingAnalyzer} instances.
     */
    public static class DefaultPricingAnalyzerFactory implements CloudCommitmentPricingAnalyzerFactory {

        private final PricingResolver<TopologyEntityDTO> pricingResolver;

        private final TopologyEntityInfoExtractor topologyEntityInfoExtractor;

        private final CloudRateExtractorFactory cloudRateExtractorFactory;

        /**
         * Constructs a new pricing analyzer factory.
         * @param pricingResolver The pricing resolver to use.
         * @param topologyEntityInfoExtractor The topology entity info extractor.
         * @param cloudRateExtractorFactory The cloud rate extractor factory.
         */
        public DefaultPricingAnalyzerFactory(@Nonnull PricingResolver<TopologyEntityDTO> pricingResolver,
                                             @Nonnull TopologyEntityInfoExtractor topologyEntityInfoExtractor,
                                             @Nonnull CloudRateExtractorFactory cloudRateExtractorFactory) {
            this.pricingResolver = Objects.requireNonNull(pricingResolver);
            this.topologyEntityInfoExtractor = Objects.requireNonNull(topologyEntityInfoExtractor);
            this.cloudRateExtractorFactory = Objects.requireNonNull(cloudRateExtractorFactory);
        }

        @Override
        public CloudCommitmentPricingAnalyzer newPricingAnalyzer(@Nonnull final CloudTopology<TopologyEntityDTO> cloudTierTopology)
                throws CloudCostDataRetrievalException {

            Preconditions.checkNotNull(cloudTierTopology);

            final CloudRateExtractor cloudRateExtractor = cloudRateExtractorFactory.newRateExtractor(
                    cloudTierTopology, topologyEntityInfoExtractor);

            // HACK - the pricing resolver will look for all account IDs in the cloud topology.
            // The cloud tier topology does not contain account accounts. It will pass an empty
            // set into the business account price table key store, which will return all
            // account -> price table key mappings.
            final Map<Long, AccountPricingData> accountPricingDataMap = ImmutableMap.copyOf(
                    pricingResolver.getAccountPricingDataByBusinessAccount(cloudTierTopology));

            // HACK - the same hack applies here as above. Pricing resolver should accept the set of
            // account OIDs to search for, instead of the entire topology. However, this would require
            // caching within the price table resolver to avoid duplicate loading of price tables.
            final Map<Long, ReservedInstancePriceTable> riPriceTableMap = ImmutableMap.copyOf(
                    pricingResolver.getRIPriceTablesByAccount(cloudTierTopology));

            return new DefaultPricingAnalyzer(
                    cloudRateExtractor,
                    cloudTierTopology,
                    accountPricingDataMap,
                    riPriceTableMap);
        }
    }
}
