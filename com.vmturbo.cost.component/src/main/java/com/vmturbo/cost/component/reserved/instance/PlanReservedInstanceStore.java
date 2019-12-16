package com.vmturbo.cost.component.reserved.instance;

import static org.jooq.impl.DSL.sum;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.TableRecord;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.identity.IdentityProvider;

/**
 * This class is used to update plan RI table by plan reserved instance bought data
 * from Topology Processor.
 */
public class PlanReservedInstanceStore {
    private static final Logger logger = LogManager.getLogger();

    // A temporary column name used for query reserved instance count map.
    private static final String RI_SUM_COUNT = "ri_sum_count";

    private final IdentityProvider identityProvider;

    private final DSLContext dsl;

    /**
     * Creates {@link PlanReservedInstanceStore} instance.
     *
     * @param dsl DSL context.
     * @param identityProvider identity provider.
     */
    public PlanReservedInstanceStore(@Nonnull DSLContext dsl, @Nonnull IdentityProvider identityProvider) {
        this.dsl = Objects.requireNonNull(dsl);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    /**
     * Inserts new records to the plan RI bought table.
     *
     * @param newReservedInstances list of reserved instance bought
     * @param planId plan id
     */
    public void insertPlanReservedInstanceBought(@Nonnull final List<ReservedInstanceBought> newReservedInstances, long planId) {
        final Set<TableRecord<?>> records = new HashSet<>();
        for (ReservedInstanceBought riBought : newReservedInstances) {
            ReservedInstanceBoughtInfo reservedInstanceInfo = riBought.getReservedInstanceBoughtInfo();
            records.add(dsl.newRecord(Tables.PLAN_RESERVED_INSTANCE_BOUGHT,
                            new PlanReservedInstanceBoughtRecord(
                                            identityProvider.next(),
                                            planId,
                                            reservedInstanceInfo.getReservedInstanceSpec(),
                                            reservedInstanceInfo,
                                            reservedInstanceInfo.getNumBought())));
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
                        dsl.select(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_BOUGHT_INFO,
                                        (sum(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.COUNT)).as(RI_SUM_COUNT))
                                        .from(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
                                        .where(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID.eq(planId))
                                        .groupBy(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.RESERVED_INSTANCE_SPEC_ID).fetch();
        final Map<ReservedInstanceBoughtInfo, Long> riSpecMap = new HashMap<>();
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
        logger.info("Deleting data from plan reserved instance bought for planId : " + planId);
        final int rowsDeleted = dsl.deleteFrom(Tables.PLAN_RESERVED_INSTANCE_BOUGHT)
                        .where(Tables.PLAN_RESERVED_INSTANCE_BOUGHT.PLAN_ID
                                        .eq(planId)).execute();
        return rowsDeleted;

    }
}
