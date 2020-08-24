package com.vmturbo.cost.component.reserved.instance;

import java.math.BigDecimal;
import java.time.Clock;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Result;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceCostStat;
import com.vmturbo.cost.component.identity.IdentityProvider;

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

    /**
     * Creates {@link AbstractReservedInstanceStore} instance.
     *
     * @param dsl DSL context.
     * @param identityProvider identity provider.
     * @param reservedInstanceCostCalculator RI cost calculator.
     */
    public AbstractReservedInstanceStore(@Nonnull DSLContext dsl, @Nonnull IdentityProvider identityProvider,
        @Nonnull final ReservedInstanceCostCalculator reservedInstanceCostCalculator) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.reservedInstanceCostCalculator = Objects.requireNonNull(reservedInstanceCostCalculator);
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
}
