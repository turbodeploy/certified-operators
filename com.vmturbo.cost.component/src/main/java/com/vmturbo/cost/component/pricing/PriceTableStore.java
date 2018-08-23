package com.vmturbo.cost.component.pricing;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;

/**
 * The persistence and retrieval layer for price tables discovered by the Topology Processor
 * via the Cost probes.
 *
 * Individual cost probes discover price tables. The Topology Processor is responsible for
 * uploading the price tables - one per probe type - to the cost component. The
 * {@link PriceTableStore} keeps the price tables safe and secret, and provides a retrieval
 * mechanism for the global price table that can be used for cost calculation.
 *
 * Note: Right now (Aug 20, 2018) in the first stage of the Cloud in XL design, each cost probe
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
     * Put a new price table associated with a particular probe type into the store.
     * <p>
     * We keep the price tables separated by probe type to make it easy to delete prices no longer
     * offered by a particular service provider.
     *
     * @param probeType The type of the probe, as reported by the probe during its registration
     *                  with the Topology Processor.
     * @param priceTable The price table discovered by the probe. This price table will completely
     *                   replace the previous price table for this probe type in the store.
     * @return An optional containing the previous price table for the probe type, if any, or
     *         an empty optional if there was no previous price table.
     */
    @Nonnull
    Optional<PriceTable> putPriceTable(@Nonnull final String probeType, @Nonnull final PriceTable priceTable);

}
