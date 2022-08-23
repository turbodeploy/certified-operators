package com.vmturbo.cost.component.savings.calculator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;

/**
 * This class is used for getting the "end range" of the price tier of a given storage amount.
 * For example, if disks of 4 - 8 GB are charged the same amount, the "end-range" of the price tier
 * is 8 GB. The "end-range" value for a 6 GB disk is 8 GB. The end range values are retrieved from
 * the price table which describes the price structure. The price structure is the same for all
 * accounts and region. That is why we can cache the value by storage tier in memory to avoid
 * querying the price table in subsequent calls.
 */
public class StorageAmountResolver {
    private final Logger logger = LogManager.getLogger();
    private final BusinessAccountPriceTableKeyStore priceTableKeyStore;
    private final PriceTableStore priceTableStore;

    // Maps provider ID (service tier OID) to a sorted list of "end range" prices.
    // E.g. if the list has {2, 4, 8}, it means disks 0-2 GB will have the same price, 2-4 GB will
    // have the same price and 4-8 GB will have the same price.
    private final Map<Long, NavigableSet<Long>> cachedProviderPriceTierMap = new HashMap<>();

    /**
     * Constructor.
     *
     * @param priceTableKeyStore price tble key store
     * @param priceTableStore price table store
     */
    public StorageAmountResolver(@Nonnull BusinessAccountPriceTableKeyStore priceTableKeyStore,
            @Nonnull PriceTableStore priceTableStore) {
        this.priceTableKeyStore = priceTableKeyStore;
        this.priceTableStore = priceTableStore;
    }

    /**
     * Maps a given storage amount to its corresponding "end range" of its price tier.
     *
     * @param storageAmount storage amount
     * @param accountId account ID
     * @param regionId region ID
     * @param providerId provider ID
     * @return the end range amount of the price tier of this disk
     */
    public double getEndRangeInPriceTier(double storageAmount, long accountId, long regionId, long providerId) {
        NavigableSet<Long> priceList = cachedProviderPriceTierMap.get(providerId);
        if (priceList == null) {
            populatePriceMap(accountId, regionId);
            priceList = cachedProviderPriceTierMap.get(providerId);
            if (priceList == null) {
                // Price table is not available.
                logger.warn("Price table is not available when getting end range value "
                        + "for storage amount {}, account ID {}, region ID {} and provider Id {}.",
                        storageAmount, accountId, regionId, providerId);
                return storageAmount;
            }
        }
        if (logger.isTraceEnabled()) {
            printCache();
        }
        if (!priceList.isEmpty()) {
            Long ceilingEndRange = priceList.ceiling(Double.valueOf(storageAmount).longValue());
            if (ceilingEndRange == null) {
                return storageAmount;
            }
            return ceilingEndRange;
        } else {
            return storageAmount;
        }
    }

    /**
     * Keep a copy of the "end range" value of each price tier in memory so that subsequent queries
     * won't require access to the price table.
     *
     * @param accountId account ID
     * @param regionId region ID
     */
    private void populatePriceMap(long accountId, long regionId) {
        Map<Long, StorageTierPriceList> priceListMap = getStoragePriceTiers(accountId, regionId);
        if (priceListMap == null) {
            return;
        }

        for (Entry<Long, StorageTierPriceList> entry : priceListMap.entrySet()) {
            Long storageTierId = entry.getKey();
            cachedProviderPriceTierMap.computeIfAbsent(storageTierId, p -> new TreeSet<>());
            NavigableSet<Long> priceList = cachedProviderPriceTierMap.get(storageTierId);
            entry.getValue().getCloudStoragePriceList().forEach(list ->
                    list.getPricesList().forEach(price -> {
                        if (price.getUnit() == Unit.MONTH) {
                            priceList.add(price.getEndRangeInUnits());
                        }
                    }));
        }
    }

    /**
     * Get a map of storage tier ID to storage tier price list from the price table.
     *
     * @param accountId account ID
     * @param regionId region ID
     * @return map of storage tier ID to storage tier price list
     */
    @Nullable
    Map<Long, StorageTierPriceList> getStoragePriceTiers(long accountId, long regionId) {
        // Get the price table OID given the account ID.
        final Optional<Long> priceTableKeyOpt = priceTableKeyStore.fetchPriceTableKeyOidForAccount(accountId);
        if (!priceTableKeyOpt.isPresent()) {
            return null;
        }
        Long priceTableKey = priceTableKeyOpt.get();

        // Get the price table using the price table key.
        PriceTable priceTable = priceTableStore.getPriceTables(
                Collections.singletonList(priceTableKey)).get(priceTableKey);
        if (priceTable == null) {
            return null;
        }

        // Get the on-demand price table of the region ID from the price table.
        final Optional<OnDemandPriceTable> onDemandPriceTable =
                Optional.ofNullable(priceTable.getOnDemandPriceByRegionIdMap().get(regionId));

        // Return the storage price lists of all storage tiers. It is a map of storage tier to
        // price list.
        return onDemandPriceTable.map(OnDemandPriceTable::getCloudStoragePricesByTierIdMap)
                .orElse(null);
    }

    /**
     * Print the cache for debug purpose.
     */
    private void printCache() {
        StringBuilder cache = new StringBuilder();
        cachedProviderPriceTierMap.forEach((k, v) ->
                cache.append("storageTier: ").append(k)
                        .append("   end ranges: ")
                        .append(v.stream().map(x -> Long.toString(x)).collect(Collectors.joining(",")))
                        .append("\n"));
        logger.trace(cache);
    }
}
