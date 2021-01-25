package com.vmturbo.cost.component.reserved.instance;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;

/**
 * This class is responsible for periodically checking for unreferenced RI specs, cleaning up any
 * where found. Cleanup of RI specs cannot be done during insertion/update by the store itself, due
 * to having multiple sources for RI spec creation.
 */
public class ReservedInstanceSpecCleanup {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final PriceTableStore priceTableStore;

    private final BuyReservedInstanceStore buyReservedInstanceStore;

    private final boolean isCleanupEnabled;

    private final Object cleanupLock = new Object();

    /**
     * Constructs a new cleanup instance.
     * @param reservedInstanceSpecStore The RI spec store.
     * @param reservedInstanceBoughtStore The RI bought store.
     * @param priceTableStore The price table store.
     * @param buyReservedInstanceStore The Buy RI store.
     * @param isCleanupEnabled Indicates whether cleanup is enabled.
     */
    public ReservedInstanceSpecCleanup(@Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore,
                                       @Nonnull ReservedInstanceBoughtStore reservedInstanceBoughtStore,
                                       @Nonnull PriceTableStore priceTableStore,
                                       @Nonnull BuyReservedInstanceStore buyReservedInstanceStore,
                                       boolean isCleanupEnabled) {

        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
        this.priceTableStore = Objects.requireNonNull(priceTableStore);
        this.buyReservedInstanceStore = Objects.requireNonNull(buyReservedInstanceStore);
        this.isCleanupEnabled = isCleanupEnabled;
    }

    /**
     * Checks for any unreferenced RI specs, deleting any that are found.
     */
    public void cleanupUnreferencedRISpecs() {

        if (isCleanupEnabled) {
            synchronized (cleanupLock) {

                logger.info("Running RI spec cleanup");

                try {
                    final Stopwatch cleanupTime = Stopwatch.createStarted();
                    final Set<Long> allRISpecIds = reservedInstanceSpecStore.getAllReservedInstanceSpec()
                            .stream()
                            .map(ReservedInstanceSpec::getId)
                            .collect(ImmutableSet.toImmutableSet());
                    final Set<Long> buyRISpecs = buyReservedInstanceStore.getAllReferencedRISpecIds();
                    final Set<Long> priceTableRISpecs = priceTableStore.getMergedRiPriceTable()
                            .getRiPricesBySpecIdMap()
                            .keySet();
                    final Set<Long> reservedInstanceRISpecs = reservedInstanceBoughtStore
                            .getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER)
                            .stream()
                            .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                            .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                            .collect(ImmutableSet.toImmutableSet());

                    final Set<Long> allReferencedSpecs = Sets.union(priceTableRISpecs,
                            Sets.union(reservedInstanceRISpecs, buyRISpecs));
                    final Set<Long> unreferencedRISpecs = Sets.difference(allRISpecIds, allReferencedSpecs);

                    if (!unreferencedRISpecs.isEmpty()) {
                        reservedInstanceSpecStore.deleteReservedInstanceSpecsById(unreferencedRISpecs);

                        logger.info("Removed {} unreferenced RI specs in {}",
                                unreferencedRISpecs.size(), cleanupTime);
                    } else {
                        logger.debug("No unreferenced RI specs found");
                    }
                } catch (Exception e) {
                    logger.error("Error during unreferenced RI spec cleanup", e);
                }

            }
        } else {
            logger.debug("RI spec cleanup is disabled");
        }
    }

}
