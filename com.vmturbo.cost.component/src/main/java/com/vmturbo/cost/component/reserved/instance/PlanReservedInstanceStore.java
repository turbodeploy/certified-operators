package com.vmturbo.cost.component.reserved.instance;

import static com.vmturbo.cost.component.db.Tables.RESERVED_INSTANCE_SPEC;
import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.TableRecord;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.PlanReservedInstanceBought;
import com.vmturbo.cost.component.db.tables.records.PlanReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;

/**
 * This class is used to update plan RI table by plan reserved instance bought data
 * from Topology Processor.
 */
public class PlanReservedInstanceStore extends AbstractReservedInstanceStore {

    /**
     * Creates {@link PlanReservedInstanceStore} instance.
     *
     * @param dsl DSL context.
     * @param identityProvider identity provider.
     * @param reservedInstanceCostCalculator RI cost calculator.
     */
    public PlanReservedInstanceStore(@Nonnull DSLContext dsl, @Nonnull IdentityProvider identityProvider,
        @Nonnull final ReservedInstanceCostCalculator reservedInstanceCostCalculator) {
        super(dsl, identityProvider, reservedInstanceCostCalculator);
    }

    /**
     * Inserts new records to the plan RI bought table.
     *
     * @param newReservedInstances list of reserved instance bought
     * @param planId plan id
     */
    public void insertPlanReservedInstanceBought(@Nonnull final List<ReservedInstanceBought> newReservedInstances, long planId) {
        final List<ReservedInstanceBoughtInfo> newReservedInstanceBoughtInfos = newReservedInstances.stream()
                        .map(ReservedInstanceBought::getReservedInstanceBoughtInfo).collect(Collectors.toList());
        final Set<TableRecord<?>> records = new HashSet<>();
        final Map<String, Double> probeRIIDToAmortizedCost =
                        getReservedInstanceCostCalculator().calculateReservedInstanceAmortizedCost(newReservedInstanceBoughtInfos);

        final DSLContext dsl = getDsl();
        for (ReservedInstanceBoughtInfo reservedInstanceInfo : newReservedInstanceBoughtInfos) {
            records.add(dsl.newRecord(Tables.PLAN_RESERVED_INSTANCE_BOUGHT,
                            new PlanReservedInstanceBoughtRecord(
                                            getIdentityProvider().next(),
                                            planId,
                                            reservedInstanceInfo.getReservedInstanceSpec(),
                                            reservedInstanceInfo,
                                            reservedInstanceInfo.getNumBought(),
                                            reservedInstanceInfo.getReservedInstanceBoughtCost().getFixedCost().getAmount(),
                                            reservedInstanceInfo.getReservedInstanceBoughtCost().getRecurringCostPerHour().getAmount(),
                                            probeRIIDToAmortizedCost.getOrDefault(reservedInstanceInfo.getProbeReservedInstanceId(), 0D))));
        }
        dsl.batchInsert(records).execute();
    }

    /**
     * Get the sum count of reserved instance bought by RI spec ID.
     *
     * @param planId plan ID.
     * @return a Map which key is reservedInstance spec ID and value is the sum count of reserved instance bought
     * which belong to this spec.
     */
    public Map<String, Long> getPlanReservedInstanceCountByRISpecIdMap(Long planId) {
        final Result<Record2<ReservedInstanceBoughtInfo, BigDecimal>> riCountMap =
                        getDsl().select(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_BOUGHT_INFO,
                                        (sum(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.COUNT)).as(RI_SUM_COUNT))
                                        .from(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
                                        .where(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID.eq(planId))
                                        .groupBy(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID).fetch();
        final Map<String, Long> countsByTemplate = new HashMap<>();
        for (Record2<ReservedInstanceBoughtInfo, BigDecimal> record : riCountMap) {
            final ReservedInstanceBoughtInfo riInfo = record.value1();
            final String key = riInfo.getDisplayName();
            final long count = record.value2().longValue();
            countsByTemplate.compute(key, (k, v) -> v == null ? count : v + count);
        }
        return countsByTemplate;
    }

    /**
     * Delete RI stats from the DB for the specified plan.
     *
     * @param planId plan ID.
     * @return count of deleted rows.
     */
    public int deletePlanReservedInstanceStats(Long planId) {
        getLogger().info("Deleting data from plan reserved instance bought for planId : " + planId);
        final int rowsDeleted = getDsl().deleteFrom(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
                        .where(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID
                                        .eq(planId)).execute();
        return rowsDeleted;

    }

    /**
     * Get the Plan reserved instance cost stats for each RI Type. The amortized, fixed and recurring costs are
     * multiplied by the number of counts of the RI Type to get the total costs for that RI Type.
     *
     * @param planId plan ID
     * @return List of type ReservedInstanceCostStat containing the cost stats.
     */
    public Cost.ReservedInstanceCostStat getPlanReservedInstanceAggregatedCosts(long planId) {
        final PlanReservedInstanceBought planRIBoughtTable = Tables.PLAN_RESERVED_INSTANCE_BOUGHT;
        final Result<Record3<BigDecimal, BigDecimal, BigDecimal>> riAggregatedCostResult =
                        getDsl().select(sum(planRIBoughtTable.PER_INSTANCE_AMORTIZED_COST_HOURLY.mul(planRIBoughtTable.COUNT))
                                                        .as(RI_AMORTIZED_SUM),
                                        sum(planRIBoughtTable.PER_INSTANCE_RECURRING_COST_HOURLY.mul(planRIBoughtTable.COUNT))
                                                        .as(RI_RECURRING_SUM),
                                        sum(planRIBoughtTable.PER_INSTANCE_FIXED_COST.mul(planRIBoughtTable.COUNT))
                                                        .as(RI_FIXED_SUM))
                                 .from(planRIBoughtTable)
                                 .join(RESERVED_INSTANCE_SPEC)
                                     .on(planRIBoughtTable.RESERVED_INSTANCE_SPEC_ID.eq(RESERVED_INSTANCE_SPEC.ID))
                                 .where(planRIBoughtTable.PLAN_ID.eq(planId))
                                 .fetch();

        final Cost.ReservedInstanceCostStat reservedInstanceCostStat = convertToRICostStat(riAggregatedCostResult);
        return reservedInstanceCostStat;
    }

    /**
     * Returns reserved instance bought list for the specified plan.
     *
     * @param planId plan ID.
     * @return list of {@link ReservedInstanceBought}.
     */
    public List<ReservedInstanceBought> getReservedInstanceBoughtByPlanId(long planId) {
        final List<PlanReservedInstanceBoughtRecord> records = getDsl().selectFrom(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
                        .where(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID.eq(planId))
                        .fetch();
        return records.stream().map(this::toReservedInstanceBoughtProto).collect(Collectors.toList());
    }

    private ReservedInstanceBought toReservedInstanceBoughtProto(PlanReservedInstanceBoughtRecord record) {
        return ReservedInstanceBought.newBuilder()
                        .setId(record.getId())
                        .setReservedInstanceBoughtInfo(record.getReservedInstanceBoughtInfo())
                        .build();

    }
}
