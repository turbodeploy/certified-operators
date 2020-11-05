package com.vmturbo.cost.component.reserved.instance;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceCostStat;
import com.vmturbo.cost.component.reserved.instance.filter.EntityReservedInstanceMappingFilter;
import com.vmturbo.cost.component.util.BusinessAccountHelper;

/**
 * Abstract class for updating RIs table by reserved instance bought data from Topology Processor.
 */
public abstract class AbstractReservedInstanceStore {
    // A temporary column name used for query reserved instance count map.
    protected static final String RI_SUM_COUNT = "ri_sum_count";

    // A temporary column name used for query reserved instance amortized cost.
    protected static final String RI_AMORTIZED_SUM = "ri_amortized_sum";

    // A temporary column name used for query reserved instance recurring cost.
    protected static final String RI_RECURRING_SUM = "ri_recurring_sum";

    // A temporary column name used for query reserved instance fixed cost.
    protected static final String RI_FIXED_SUM = "ri_fixed_sum";

    private final Logger logger = LogManager.getLogger(getClass());

    private final IdentityProvider identityProvider;

    private final DSLContext dsl;

    private final ReservedInstanceCostCalculator reservedInstanceCostCalculator;

    private final AccountRIMappingStore accountRIMappingStore;
    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;
    protected final BusinessAccountHelper businessAccountHelper;

    /**
     * Creates {@link AbstractReservedInstanceStore} instance.
     * @param dsl DSL context.
     * @param identityProvider identity provider.
     * @param reservedInstanceCostCalculator RI cost calculator.
     * @param accountRIMappingStore Account RI mapping store
     * @param entityReservedInstanceMappingStore The Entity to ReservedInstance mapping store
     * @param businessAccountHelper BusinessAccountHelper
     */
    public AbstractReservedInstanceStore(@Nonnull DSLContext dsl, @Nonnull IdentityProvider identityProvider,
                                         @Nonnull final ReservedInstanceCostCalculator reservedInstanceCostCalculator,
                                         @Nonnull final AccountRIMappingStore accountRIMappingStore,
                                         @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
                                         @Nonnull final BusinessAccountHelper businessAccountHelper) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.reservedInstanceCostCalculator = Objects.requireNonNull(reservedInstanceCostCalculator);
        this.accountRIMappingStore = accountRIMappingStore;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
        this.businessAccountHelper = businessAccountHelper;
    }

    /**
     * Gets logger.
     *
     * @return {@link Logger}.
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Gets identity provider.
     *
     * @return {@link IdentityProvider}.
     */
    public IdentityProvider getIdentityProvider() {
        return identityProvider;
    }

    /**
     * Gets DSL context.
     *
     * @return {@link DSLContext}.
     */
    public DSLContext getDsl() {
        return dsl;
    }

    /**
     * Gets reserved instance cost calculator.
     *
     * @return {@link ReservedInstanceCostCalculator}.
     */
    public ReservedInstanceCostCalculator getReservedInstanceCostCalculator() {
        return reservedInstanceCostCalculator;
    }

    /**
     * Convert aggregated costs records to the reserved instance cost stats.
     *
     * @param riAggregatedCostResult records with cost values to convert.
     * @return {@link ReservedInstanceCostStat}.
     */
    protected static Cost.ReservedInstanceCostStat convertToRICostStat(final Result<Record3<BigDecimal, BigDecimal, BigDecimal>> riAggregatedCostResult) {
        return Cost.ReservedInstanceCostStat.newBuilder()
                        .setAmortizedCost(getValueByName(riAggregatedCostResult, RI_AMORTIZED_SUM))
                        .setRecurringCost(getValueByName(riAggregatedCostResult, RI_RECURRING_SUM))
                        .setFixedCost(getValueByName(riAggregatedCostResult, RI_FIXED_SUM))
                        .setSnapshotTime(Clock.systemUTC().instant().toEpochMilli()).build();
    }

    private static Double getValueByName(final Result<Record3<BigDecimal, BigDecimal, BigDecimal>> riAggregatedCostResult, String valueName) {
        return riAggregatedCostResult.getValues(valueName, Double.class).stream().filter(s -> s != null)
                        .findAny().orElse(0D);
    }

    protected List<ReservedInstanceBought> adjustAvailableCouponsForPartialCloudEnv(
            final List<ReservedInstanceBought> reservedInstances) {
        if (reservedInstances.isEmpty()) {
            return reservedInstances;
        }

        logger.info("Adjusting available coupons for partial cloud environment for {} reserved instances.",
                reservedInstances.size());

        final Set<Long> discoveredBaOids = businessAccountHelper.getDiscoveredBusinessAccounts();
        final List<ReservedInstanceBought> riFromUndiscoveredAccounts = reservedInstances.stream()
                .filter(ri -> !discoveredBaOids.contains(
                        ri.getReservedInstanceBoughtInfo().getBusinessAccountId()))
                .collect(Collectors.toList());
        final List<Long> undiscoveredRiIds = riFromUndiscoveredAccounts.stream()
                .map(ReservedInstanceBought::getId)
                .collect(Collectors.toList());
        // Retrieve the total  used coupons from discovered workloads for each RI
        final Map<Long, Double> riToDiscoveredUsageMap = entityReservedInstanceMappingStore
                .getReservedInstanceUsedCouponsMapByFilter(
                        EntityReservedInstanceMappingFilter.newBuilder().riBoughtFilter(
                                Cost.ReservedInstanceBoughtFilter.newBuilder()
                                        .addAllRiBoughtId(undiscoveredRiIds).build()).build());

        // Retrieve the total  used coupons from undiscovered accounts for each RI
        final Map<Long, Double> riToUndiscoveredAccountsUsage =
                accountRIMappingStore.getUndiscoveredAccountUsageForRI();
        if (riToUndiscoveredAccountsUsage.isEmpty()) {
            logger.warn("No RI usage for undiscovered accounts recorded.");
        }
        // Update the capacities for each RI
        return reservedInstances.stream().map(ReservedInstanceBought::toBuilder).peek(riBuilder -> {
            final ReservedInstanceBoughtCoupons.Builder riCouponsBuilder =
                    riBuilder.getReservedInstanceBoughtInfoBuilder()
                            .getReservedInstanceBoughtCouponsBuilder();
            if (undiscoveredRiIds.contains(riBuilder.getId())) {
                final double coupons = riToDiscoveredUsageMap.getOrDefault(riBuilder.getId(), 0d);
                riCouponsBuilder.setNumberOfCoupons(coupons).setNumberOfCouponsUsed(coupons);
            } else {
                final double couponsUsedByUndiscoveredAccounts =
                        riToUndiscoveredAccountsUsage.getOrDefault(riBuilder.getId(), 0d);
                if (couponsUsedByUndiscoveredAccounts > 0) {
                    double numberOfCoupons = riCouponsBuilder.getNumberOfCoupons()
                            - couponsUsedByUndiscoveredAccounts;
                    if (numberOfCoupons < 0) {
                        logger.warn(
                                "Coupons used by undiscovered accounts {} is greater than number of coupons {} for RI {}. Number of coupons will be set to 0.",
                                couponsUsedByUndiscoveredAccounts,
                                riCouponsBuilder.getNumberOfCoupons(), riBuilder.getId());
                        numberOfCoupons = 0;
                    }
                    riCouponsBuilder.setNumberOfCoupons(numberOfCoupons);
                }
            }
            logger.trace("ReservedInstanceBought after adjusting number of used coupons: {}",
                    riBuilder);
        }).map(ReservedInstanceBought.Builder::build).collect(Collectors.toList());
    }
}
