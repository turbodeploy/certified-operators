package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;

/**
 * Table of {@link PriceData} elements indexed by business account and region IDs. The table keeps
 * prices that are based on provisioned commodity capacity separate from prices that are based on
 * historically consumed commodity value.
 */
public class AccountRegionPriceTable {
    private final Table<Long, Long, List<PriceData>> provisionedBasedTable = HashBasedTable.create();
    private final Table<Long, Long, List<PriceData>> consumedBasedTable = HashBasedTable.create();

    /**
     * Create new instance of {@code AccountRegionPriceDataTable} using
     * {@code StorageResourceCost} object.
     *
     * @param resource Source {@link StorageResourceCost} object.
     */
    public AccountRegionPriceTable(@Nonnull final StorageResourceCost resource) {
        for (StorageTierPriceData priceData : resource.getStorageTierPriceDataList()) {
            for (CostDTO.CostTuple costTuple : priceData.getCostTupleListList()) {
                final long businessAccountId = costTuple.getBusinessAccountId();
                final long regionId = costTuple.getRegionId();
                final PriceData price = new PriceData(priceData.getUpperBound(),
                        costTuple.getPrice(), priceData.getIsUnitPrice(),
                        priceData.getIsAccumulativeCost(),
                        priceData.getAppliedToHistoricalQuantity(), regionId);
                final Table<Long, Long, List<PriceData>> table = priceData.getAppliedToHistoricalQuantity()
                        ? consumedBasedTable
                        : provisionedBasedTable;
                List<PriceData> priceDataList = table.get(businessAccountId, regionId);
                if (priceDataList == null) {
                    priceDataList = new ArrayList<>();
                    table.put(businessAccountId, regionId, priceDataList);
                }
                priceDataList.add(price);
            }
        }
        sortTables();
    }

    /**
     * Merge contents of another table to this table.
     *
     * @param tableToMerge Table to merge.
     */
    public void merge(@Nonnull final AccountRegionPriceTable tableToMerge) {
        provisionedBasedTable.putAll(tableToMerge.provisionedBasedTable);
        consumedBasedTable.putAll(tableToMerge.consumedBasedTable);
        sortTables();
    }

    private void sortTables() {
        // make sure price list is ascending based on upper bound, because we need to get the
        // first resource range that can satisfy the requested amount, and the price
        // corresponds to that range will be used as price
        provisionedBasedTable.values().forEach(Collections::sort);
        consumedBasedTable.values().forEach(Collections::sort);
    }

    /**
     * Checks if table contains prices that are based on provisioned commodity capacity.
     *
     * @return {@code true} if table contains provisioned-based prices.
     */
    public boolean containsProvisionedBasedPrices() {
        return !provisionedBasedTable.isEmpty();
    }

    /**
     * Checks if table contains business account with a given ID.
     *
     * @param accountId Business account ID to check.
     * @return {@code true} if table contains business account with the ID.
     */
    public boolean containsAccount(final long accountId) {
        return provisionedBasedTable.containsRow(accountId)
                || consumedBasedTable.containsRow(accountId);
    }

    /**
     * Gets list of {@link PriceData} elements associated with given business account and region.
     * The list contains both provisioning based and consumption based prices.
     *
     * @param accountId Business account ID.
     * @param regionId Region ID.
     * @return List of {@link PriceData} elements for account and region.
     */
    @Nullable
    public List<PriceData> getPriceDataList(
            final long accountId,
            final long regionId) {
        final List<PriceData> provisionedBasedList = provisionedBasedTable.get(accountId, regionId);
        final List<PriceData> consumedBasedList = consumedBasedTable.get(accountId, regionId);
        if (provisionedBasedList == null) {
            return consumedBasedList;
        }
        if (consumedBasedList == null) {
            return provisionedBasedList;
        }
        return Stream.concat(provisionedBasedList.stream(), consumedBasedList.stream())
                .collect(Collectors.toList());
    }
}
