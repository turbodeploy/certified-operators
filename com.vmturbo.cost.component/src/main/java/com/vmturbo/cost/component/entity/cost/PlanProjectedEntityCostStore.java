package com.vmturbo.cost.component.entity.cost;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;

import org.jooq.DSLContext;
import org.jooq.InsertValuesStep3;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityCostRecord;
import com.vmturbo.cost.component.util.EntityCostFilter;

/**
 * Storage for plan projected per-entity costs.
 */
public class PlanProjectedEntityCostStore extends AbstractProjectedEntityCostStore {

    private final DSLContext dslContext;

    private final int chunkSize;

    /**
     * Creates {@link PlanProjectedEntityCostStore} instance.
     *
     * @param context DSL context.
     * @param chunkSize chunk size.
     */
    public PlanProjectedEntityCostStore(@Nonnull final DSLContext context, final int chunkSize) {
        this.dslContext = context;
        this.chunkSize = chunkSize;
    }

    /**
     * Update PlanProjectedEntityCostsTable with new plan records send from market.
     *
     * @param topoInfo the topology information
     * @param entityCosts a list of projected entity cost
     */
    public void updatePlanProjectedEntityCostsTableForPlan(@Nonnull final TopologyInfo topoInfo,
                                                       @Nonnull final List<EntityCost> entityCosts) {
        dslContext.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            Iterators.partition(entityCosts.iterator(), chunkSize).forEachRemaining(e -> {
                // If we expand the Market Action table we need to modify this insert
                // statement and the subsequent "values" bindings.
                InsertValuesStep3<PlanProjectedEntityCostRecord, Long, Long, EntityCost> step =
                        transactionDsl.insertInto(Tables.PLAN_PROJECTED_ENTITY_COST,
                                                  Tables.PLAN_PROJECTED_ENTITY_COST.PLAN_ID,
                                                  Tables.PLAN_PROJECTED_ENTITY_COST.ASSOCIATED_ENTITY_ID,
                                                  Tables.PLAN_PROJECTED_ENTITY_COST.ENTITY_COST);
                for (EntityCost cost : e) {
                    step = step.values(topoInfo.getTopologyContextId(),
                                       cost.getAssociatedEntityId(),
                                       cost);
                }
                step.execute();
            });
        });
    }

    /**
     * Delete records from following plan cost related tables when a plan is deleted:
     *      plan_projected_entity_cost
     *      plan_projected_entity_to_reserved_instance_mapping
     *      plan_projected_reserved_instance_coverage
     *      plan_projected_reserved_instance_utilization
     *
     * @param planId Plan ID.
     * @return Count of total deleted rows.
     */
    public int deletePlanProjectedCosts(final long planId) {
       int[] rowsDeleted = dslContext.batch(
               dslContext.deleteFrom(
                       Tables.PLAN_PROJECTED_ENTITY_COST)
                       .where(Tables.PLAN_PROJECTED_ENTITY_COST.PLAN_ID.eq(planId)),
               dslContext.deleteFrom(
                       Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING)
                       .where(Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING.PLAN_ID
                               .eq(planId)),

               dslContext.deleteFrom(
                       Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE)
                       .where(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE.PLAN_ID.eq(planId)),
               dslContext.deleteFrom(
                       Tables.PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION)
                       .where(Tables.PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION.PLAN_ID.eq(planId))
       ).execute();

        int totalDeleted = Arrays.stream(rowsDeleted).sum();
        getLogger().info("Deleted {} rows (total: {}) from plan {} projected cost tables.",
                Arrays.toString(rowsDeleted), totalDeleted, planId);
        return totalDeleted;
    }

    /**
     * Return entity costs for the specified plan as collection of stat records.
     *
     * @param groupByList group by list
     * @param filter entity cost filter
     * @param planId plan ID
     * @return collection of {@link StatRecord}
     */
    @Nonnull
    public Collection<StatRecord> getPlanProjectedStatRecordsByGroup(@Nonnull final List<GroupBy> groupByList,
        @Nonnull final EntityCostFilter filter, long planId) {
        final Set<EntityCost> entityCosts = getPlanProjectedEntityCosts(planId);
        return aggregateByGroup(groupByList, EntityCostToStatRecordConverter.convertEntityToStatRecord(entityCosts.stream()
                        .map(entityCost -> applyFilter(entityCost, filter))
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toSet())));
    }

    /**
     * Return entity costs for the specified plan.
     *
     * @param planId plan ID
     * @return set of {@link EntityCost}
     */
    @Nonnull
    private Set<EntityCost> getPlanProjectedEntityCosts(long planId) {
        final Result<Record1<EntityCost>> records = dslContext.select(Tables.PLAN_PROJECTED_ENTITY_COST.ENTITY_COST)
                        .from(Tables.PLAN_PROJECTED_ENTITY_COST)
                        .where(Tables.PLAN_PROJECTED_ENTITY_COST.PLAN_ID.eq(planId))
                        .fetch();
        return records.stream().map(Record1::value1).collect(Collectors.toSet());
    }
}
