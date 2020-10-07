package com.vmturbo.cost.component.cca;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.pricing.CCAPriceHolder;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingAnalyzer;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.RIPricingData;
import com.vmturbo.cloud.commitment.analysis.pricing.TierDemandPricingData;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecData;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.PricingResolver;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle.ComputePrice;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * Class used to resolve pricing for given demand and specs.
 */
public class LocalCloudCommitmentPricingAnalyzer implements CloudCommitmentPricingAnalyzer {

    private final PricingResolver resolverPricing;

    /**
     * Hours in a month.
     */
    public static final int HOURS_IN_A_MONTH = 730;

    /**
     * Months in a year.
     */
    public static final int MONTHS_IN_A_YEAR = 12;

    private static final Logger logger = LogManager.getLogger();

    private final BusinessAccountPriceTableKeyStore baPriceTableKeyStore;

    private final PriceTableStore priceTableStore;

    private final TopologyEntityInfoExtractor topologyEntityInfoExtractor;

    private final SetOnce<CCAPriceHolder> ccaPriceHolderSetOnce = new SetOnce<>();

    /**
     * Constructor for the class. The pricing resolver is used to get the price tables and the price
     * table store and business account price table key store are required to get the RI price tables.
     *
     * @param pricingResolver The pricing resolver used to resolve business account -> Account Pricing data.
     * @param businessAccountPriceTableKeyStore The business account price table key store.
     * @param priceTableStore The price table store.
     * @param topologyEntityInfoExtractor The topology entity info extractor.
     */
    public LocalCloudCommitmentPricingAnalyzer(@Nonnull PricingResolver pricingResolver,
            @Nonnull BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore,
            @Nonnull PriceTableStore priceTableStore, @Nonnull TopologyEntityInfoExtractor topologyEntityInfoExtractor) {
        this.resolverPricing = Objects.requireNonNull(pricingResolver);
        this.baPriceTableKeyStore = businessAccountPriceTableKeyStore;
        this.priceTableStore = priceTableStore;
        this.topologyEntityInfoExtractor = topologyEntityInfoExtractor;
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
    private Optional<Float> getOnDemandRate(ScopedCloudTierDemand aggregateCloudTierDemand, AccountPricingData accountPricingData,
            TopologyEntityDTO tier, OSType osType, CloudRateExtractor cloudRateExtractor) {

        long regionOid = aggregateCloudTierDemand.regionOid();
        ComputePriceBundle computePriceBundle = cloudRateExtractor.getComputePriceBundle(tier, regionOid, accountPricingData);
        if (computePriceBundle == null || computePriceBundle.getPrices().size() == 0) {
            logger.error("Something went wrong while trying to retrieve the compute tier price list."
                    + " No on demand rates will be returned for this tier {}", tier.getDisplayName());
            return Optional.empty();
        }
        float onDemandRate = 0f;
        for (ComputePrice price: computePriceBundle.getPrices()) {
            // In case of unknown OS, use the base price.
           if (osType == OSType.UNKNOWN_OS && price.isBasePrice()) {
               onDemandRate += price.getHourlyPrice();
           }
           if (price.getOsType() == osType) {
               onDemandRate += price.getHourlyPrice();
           }
       }
        logger.debug("lookupOnDemandRate() onDemandRate={}",
                onDemandRate);
        return Optional.of(onDemandRate);
    }

    private Float getReservedLicenseRate(ScopedCloudTierDemand aggregateCloudTierDemand, AccountPricingData accountPricingData,
            TopologyEntityDTO tier, OSType osType, CloudRateExtractor cloudRateExtractor) {
        if (tier.getTypeSpecificInfo() == null) {
            logger.error("The compute tier has no type specific info for resolving the number of cores."
                    + "No on demand licensing price will be returned for this tier {}", tier.getDisplayName());
            return null;
        }
        long regionOid = aggregateCloudTierDemand.regionOid();
        ComputePriceBundle reservedLicenseBundle = cloudRateExtractor.getReservedLicensePriceBundle(accountPricingData, regionOid, tier);
        if (reservedLicenseBundle == null) {
            logger.error("Something went wrong while trying to retrieve the reserved Pricing Bundle."
                    + " No reserved licensing price will be returned for this cloud tier {}", tier.getDisplayName());
            return null;
        }
        float reservedLicenseRate = 0.0f;
        for (ComputePrice price: reservedLicenseBundle.getPrices()) {
            // In case of Unknown OS use the base rate for reserved license pricing.
            if (osType == OSType.UNKNOWN_OS && price.isBasePrice()) {
                reservedLicenseRate += price.getHourlyPrice();
            }
            if (price.getOsType() == osType) {
                reservedLicenseRate += price.getHourlyPrice();
            }
        }
        logger.debug("getReservedLicenseRate() reservedLicenseRate={}",
                reservedLicenseRate);
        return reservedLicenseRate;
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
    private Pair<Float, Float> getReservedInstanceRate(@Nonnull CloudCommitmentSpecData<ReservedInstanceSpec> cloudCommitmentSpecData,
             @Nonnull Set<Long> businessAccountIds) {
        float cheapestTotalCost = Integer.MAX_VALUE;
        Optional<CCAPriceHolder> ccaPriceHolder = ccaPriceHolderSetOnce.getValue();
        if (!ccaPriceHolder.isPresent()) {
            logger.error("No cca pricing info retrieved. Returning null rates fot this spec {}", cloudCommitmentSpecData.toString());
        }
        Pair<Float, Float> pair = null;
        for (Long accountId : businessAccountIds) {
            ReservedInstancePriceTable riPriceTable = ccaPriceHolder.get().getRiPriceTableMap().get(
                    accountId);
            if (riPriceTable == null) {
                logger.error("No RI price table found for account id {}", accountId);
                return null;
            }
            final ReservedInstancePrice riPrice = riPriceTable.getRiPricesBySpecIdMap().get(
                    cloudCommitmentSpecData.specId());
            if (riPrice == null) {
                logger.warn(
                        "lookupReservedInstanceRate() can't find rate for ReservedInstanceSpecId={}",
                        cloudCommitmentSpecData.specId());
                return Pair.of(Float.MAX_VALUE, 0f);
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

    @Override
    public TierDemandPricingData getTierDemandPricing(ScopedCloudTierDemand cloudTierDemand, CloudTopology<TopologyEntityDTO> cloudTopology) {
        if (!ccaPriceHolderSetOnce.getValue().isPresent()) {
            try {
                constructCCAPriceHolder(cloudTopology);
            } catch (CloudCostDataRetrievalException e) {
                logger.error("Exception encountered while constructing the CCA Price Holder", e);
            }
        }
        Optional<CCAPriceHolder> ccaPriceHolder = ccaPriceHolderSetOnce.getValue();
        if (!ccaPriceHolder.isPresent()) {
            logger.error("No cca pricing info retrieved. Returning null rates fot this demand {}", cloudTierDemand.cloudTierDemand());
            return null;
        }
        CloudRateExtractor cloudRateExtractor = ccaPriceHolder.get().getCloudRateExtractor();
        long accountId = cloudTierDemand.accountOid();
        ComputeTierDemand computeTierDemand = (ComputeTierDemand)cloudTierDemand.cloudTierDemand();
        long tierOid = computeTierDemand.cloudTierOid();
        Optional<TopologyEntityDTO> tier = cloudTopology.getEntity(tierOid);
        OSType os = computeTierDemand.osType();
        if (!tier.isPresent()) {
            logger.error("No cloud tier found for demand {}. Skipping calculating pricing data for this demand",
                    cloudTierDemand.cloudTierDemand());
            return null;
        }
        Map<Long, AccountPricingData> accountPricingDataMap = ccaPriceHolder.get().getAccountPricingDataMap();
        if (!accountPricingDataMap.containsKey(accountId)) {
            logger.error("No pricing found for cloud tier demand with account oid {} while"
                    + " running CCA for tier {}", accountId, tier.get().getDisplayName());
            return null;
        }
        AccountPricingData accountPricingData = accountPricingDataMap.get(accountId);
        Optional<Float> onDemandRate = getOnDemandRate(cloudTierDemand, accountPricingData, tier.get(), os,
                cloudRateExtractor);
        if (!onDemandRate.isPresent()) {
            logger.error("No on demand rate found for this cloud tier demand {} in account {}", tier.get().getDisplayName(), accountId);
            return null;
        }
        Float reservedLicenseRate = getReservedLicenseRate(cloudTierDemand, accountPricingData, tier.get(), os,
                cloudRateExtractor);
        if (reservedLicenseRate == null) {
            logger.error("No reserved license rates found for this cloud tier demand {} in account {}", tier.get().getDisplayName(), accountId);
            return null;
        }
        TierDemandPricingData demandPricingData = TierDemandPricingData.builder()
                .onDemandRate(onDemandRate.get()).reservedLicenseRate(reservedLicenseRate).build();
        return demandPricingData;
    }

    @Override
    public CloudCommitmentPricingData getCloudCommitmentPricing(CloudCommitmentSpecData cloudCommitmentSpecData, Set<Long> businessAccountOids,
            CloudTopology<TopologyEntityDTO> cloudTopology) {
        if (ccaPriceHolderSetOnce.getValue() == null || !ccaPriceHolderSetOnce.getValue().isPresent()) {
            try {
                constructCCAPriceHolder(cloudTopology);
            } catch (CloudCostDataRetrievalException e) {
                logger.error("Exception encountered while constructing the CCA Price Holder", e);
            }
        }
        Pair<Float, Float> riPairOfRates = getReservedInstanceRate(cloudCommitmentSpecData, businessAccountOids);
        if (riPairOfRates == null) {
            logger.error("No cloud commitment spec pricing found for spec {}", cloudCommitmentSpecData.spec().toString());
            return null;
        }
        return RIPricingData.builder().reservedInstanceUpfrontRate(riPairOfRates.getLeft())
                .reservedInstanceRecurringRate(riPairOfRates.getRight()).build();
    }

    /**
     * Given a cloud topology contruct the CCA price holder which holds pricing info for on demand and cloud commitment pricing.
     *
     * @param cloudTopology the cloud topology.
     *
     * @throws CloudCostDataRetrievalException A cloud cost data retrieval exception.
     */
    public void constructCCAPriceHolder(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology)
            throws CloudCostDataRetrievalException {
        Map<Long, AccountPricingData> accountPricingDataMap = resolverPricing.getAccountPricingDataByBusinessAccount(cloudTopology);
        Set<Long> baOids = cloudTopology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE).stream()
                .map(TopologyEntityDTO::getOid).collect(Collectors.toSet());
        Map<Long, Long> priceTableKeyOidByBusinessAccountOid = ImmutableMap.copyOf(
                baPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(baOids));
        Map<Long, ReservedInstancePriceTable> riPriceTableByPriceTableKeyOid =
                priceTableStore.getRiPriceTables(priceTableKeyOidByBusinessAccountOid.values());
        // The RI price table indexed by the business account oid.
        Map<Long, ReservedInstancePriceTable> riPriceTableByBusinessAccountOid = new HashMap<>();
        for (Map.Entry<Long, Long> entry: priceTableKeyOidByBusinessAccountOid.entrySet()) {
            Long businessAccount = entry.getKey();
            Long priceTableKeyOid = entry.getValue();
            ReservedInstancePriceTable priceTable = riPriceTableByPriceTableKeyOid.get(priceTableKeyOid);
            riPriceTableByBusinessAccountOid.put(businessAccount, priceTable);
        }
        ccaPriceHolderSetOnce.trySetValue(new CCAPriceHolder(accountPricingDataMap, riPriceTableByBusinessAccountOid,
                new CloudRateExtractor(cloudTopology, topologyEntityInfoExtractor)));
    }
}
