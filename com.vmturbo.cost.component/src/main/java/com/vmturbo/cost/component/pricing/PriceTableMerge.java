package com.vmturbo.cost.component.pricing;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;

/**
 * An operation to merge multiple {@link PriceTable}s into one.
 *
 * Encapsulates the logic independently of the {@link PriceTableStore} implementation for better
 * reuse and unit testing.
 */
public class PriceTableMerge {

    private static final Logger logger = LogManager.getLogger();

    private PriceTableMerge() {}

    /**
     * Create a new {@link PriceTableMergeFactory} for use in production.
     *
     * @return A {@link PriceTableMergeFactory} to be used in the actual component's code.
     */
    @Nonnull
    public static PriceTableMergeFactory newFactory() {
        return new PriceTableMergeFactory() {
            @Nonnull
            @Override
            public PriceTableMerge newMerge() {
                return new PriceTableMerge();
            }
        };
    }

    /**
     * Merge a number of price tables - coming from probes of different categories (e.g. AWS and
     * Azure) - into a single global price table that can be used for cost calculation.
     *
     * The underlying assumption is that the input {@link PriceTable}s are from different
     * probe categories, and do not contain duplicate IDs.
     *
     * @param priceTables A collection of {@link PriceTable}s.
     * @return A single {@link PriceTable} containing the information of all the input price tables.
     */
    @Nonnull
    public PriceTable merge(@Nonnull final Collection<PriceTable> priceTables) {
        if (priceTables.isEmpty()) {
            return PriceTable.getDefaultInstance();
        } else if (priceTables.size() == 1) {
            return priceTables.iterator().next();
        } else {
            final Iterator<PriceTable> priceTableIterator = priceTables.iterator();
            final PriceTable.Builder mergeBuilder = priceTableIterator.next().toBuilder();
            while (priceTableIterator.hasNext()) {
                final PriceTable nextPriceTable = priceTableIterator.next();
                final Map<Long, OnDemandPriceTable> srcOnDemandPriceTables = nextPriceTable.getOnDemandPriceByRegionIdMap();
                srcOnDemandPriceTables.forEach((regionId, priceTable) -> {
                    if (mergeBuilder.containsOnDemandPriceByRegionId(regionId)) {
                        // This shouldn't happen, because different price tables should be coming from
                        // different probe categories (e.g. AWS and Azure) and shouldn't have overlapping
                        // region IDs.
                        logger.error("Region {} exists in two separate price tables (for on " +
                            "demand instances)! This means region ID assignment isn't working as " +
                            "expected. Ignoring one of them.", regionId);
                    } else {
                        mergeBuilder.putOnDemandPriceByRegionId(regionId, priceTable);
                    }
                });

                final Map<Long, SpotInstancePriceTable> srcSpotPriceTables = nextPriceTable.getSpotPriceByRegionIdMap();
                srcSpotPriceTables.forEach((regionId, priceTable) -> {
                    if (mergeBuilder.containsSpotPriceByRegionId(regionId)) {
                        // This shouldn't happen, because different price tables should be coming from
                        // different probe categories (e.g. AWS and Azure) and shouldn't have overlapping
                        // region IDs.
                        logger.error("Region {} exists in two separate price tables (for " +
                            "spot instances)! This means region ID assignment isn't working as " +
                            "expected. Ignoring one of them.", regionId);
                    } else {
                        mergeBuilder.putSpotPriceByRegionId(regionId, priceTable);
                    }
                });

                // We ignore the license tables, because right now it's not clear that we need
                // them at all. They may be removed from the PriceTable altogether.
            }
            return mergeBuilder.build();
        }
    }

    /**
     * A factory for {@link PriceTableMerge}, mainly for unit testing/mocking purposes.
     */
    public interface PriceTableMergeFactory {

        @Nonnull
        PriceTableMerge newMerge();

    }
}
