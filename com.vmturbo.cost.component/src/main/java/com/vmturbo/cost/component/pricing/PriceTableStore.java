package com.vmturbo.cost.component.pricing;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;

/**
 * The persistence and retrieval layer for price tables discovered by the Topology Processor
 * via the Cost probes.
 *
 * <p>Individual cost probes discover price tables. The Topology Processor is responsible for
 * uploading the price tables - one per probe type - to the cost component. The
 * {@link PriceTableStore} keeps the price tables safe and secret, and provides a retrieval
 * mechanism for the global price table that can be used for cost calculation.
 *
 * <p>Note: Right now (Aug 20, 2018) in the first stage of the Cloud in XL design, each cost probe
 * discovers a price table for every target, but the price tables are the same across targets for
 * the same probe type (e.g. AWS, Azure). This is why we store them organized by probe type
 * instead of target ID.
 */
public interface PriceTableStore {

    /**
     * Get the merged global price table that the cost calculation library can use to calculate
     * costs for entities. Since entities discovered by different service provider probes will
     * have different IDs, we can easily merge all price tables into a single large table without
     * losing any information.
     *
     * @return The global price table.
     */
    @Nonnull
    PriceTable getMergedPriceTable();

    /**
     * Get the merged reserved instance price table that the RI purchase algorithm can use to
     * calculate costs for suggested RI purchases.
     *
     * @return The global reserved instance price table.
     */
    @Nonnull
    ReservedInstancePriceTable getMergedRiPriceTable();

    /**
     * Gets a list of price tables corresponding to the list of oids.
     *
     * @param oids The oids whose price tables to retrieve.
     * @return The map of price tables by price id.
     */
    @Nonnull
    Map<Long, PriceTable> getPriceTables(Collection<Long> oids);

    /**
     * Get the reserved instance price tables.
     *
     * @param oids The oids whose price tables to retrieve.
     * @return The map of price tables by price id.
     */
    @Nonnull
    Map<Long, ReservedInstancePriceTable> getRiPriceTables(Collection<Long> oids);

    /**
     * Put a new collection of probe type -> price table associations into the store. This
     * completely overwrites the existing probe type -> price table associations.
     *
     * <p>In the future we may want to have methods to update price tables as well as overwrite them.
     *
     * <p>We keep the price tables separated by probe type to make it easy to delete prices no longer
     * offered by a particular service provider.
     *
     * @param tablesByProbeType The new {@link PriceTable}s by probe type. These will
     *                          completely overwrite the existing price tables by probe type, and any
     *                          existing probe types that are not found in this map will be deleted.
     */
    void putProbePriceTables(@Nonnull Map<PriceTableKey, PriceTables> tablesByProbeType);

    /**
     * Receive a map of PriceTableKey to long. where the long value is the checksum of
     * price table data which is uploaded to DB.
     *
     * <p>If no list provided is empty all the priceTableKey and its checksumm will be returned.
     * This is used to determine which pricetable data has changes and need a new upload.
     *
     * @param priceTableKeyList list of {@link PriceTableKey} used for querying DB.
     * @return Map of {@link PriceTableKey} and long where long is checksum value of price table data.
     */
    Map<PriceTableKey, Long> getChecksumByPriceTableKeys(@Nonnull Collection<PriceTableKey> priceTableKeyList);

    /**
     * Holds the {@link PriceTable}, {@link ReservedInstancePriceTable} and {@link Long}
     * checksum value for uploading to DB.
     */
    class PriceTables {
        private PriceTable priceTable;
        private ReservedInstancePriceTable riPriceTable;
        private Long checkSum;

        public PriceTables(@Nonnull PriceTable priceTable, @Nonnull ReservedInstancePriceTable riPriceTable, Long checkSum) {
            this.priceTable = priceTable;
            this.riPriceTable = riPriceTable;
            this.checkSum = checkSum;
        }

        public PriceTables(PriceTable priceTable) {
            this.priceTable = priceTable;
        }

        public PriceTables(ReservedInstancePriceTable riPriceTable) {
            this.riPriceTable = riPriceTable;
        }

        public PriceTable getPriceTable() {
            if (priceTable == null) {
                return PriceTable.getDefaultInstance();
            }
            return priceTable;
        }

        public ReservedInstancePriceTable getRiPriceTable() {
            if (riPriceTable == null) {
                return ReservedInstancePriceTable.getDefaultInstance();
            }
            return riPriceTable;
        }

        public Long getCheckSum() {
            return checkSum;
        }

    }
}
