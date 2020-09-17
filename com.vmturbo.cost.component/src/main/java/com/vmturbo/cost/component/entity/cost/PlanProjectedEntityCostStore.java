package com.vmturbo.cost.component.entity.cost;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Batch;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep3;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityCostRecord;
import com.vmturbo.cost.component.util.EntityCostFilter;

/**
 * Storage for plan projected per-entity costs.
 */
public class PlanProjectedEntityCostStore extends AbstractProjectedEntityCostStore implements DiagsRestorable<Void> {

    private static final  Logger logger = LogManager.getLogger();

    private static final String planEntityCostDumpFile = "planEntityCost_dump";

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
     * Insert new plan records sent from market into the PlanProjectedEntityCostsTable.
     *
     * @param topoInfo the topology information
     * @param entityCosts a list of projected entity cost
     */
    public void insertPlanProjectedEntityCostsTableForPlan(@Nonnull final TopologyInfo topoInfo,
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
     * Updates plan projected entity costs, used primarily for MPC BuyRI on plan completion,
     * where BuyRI discounts need to be updated in the projected entity costs table for the plan.
     *
     * @param planId ID of the plan for which costs need to be updated.
     * @param costsPerEntity Set of costs for all plan entities that need to be updated.
     * @return Total number of updated records.
     */
    public int updatePlanProjectedEntityCosts(long planId,
            @Nonnull final Map<Long, EntityCost> costsPerEntity) {
        final List<PlanProjectedEntityCostRecord> entityCostRecords = new ArrayList<>();
        costsPerEntity.forEach((entityId, entityCost) -> {
            entityCostRecords.add(new PlanProjectedEntityCostRecord(planId, entityId, entityCost));
                });
        final Batch batch = dslContext.batchUpdate(entityCostRecords);
        batch.execute();
        return batch.size();
    }

    /**
     * Delete records from following plan cost related tables when a plan is deleted.
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
        final Collection<StatRecord> records = EntityCostToStatRecordConverter.convertEntityToStatRecord(entityCosts.stream()
                .map(entityCost -> applyFilter(entityCost, filter))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet()));
        if (groupByList.isEmpty()) {
            return records;
        }
        return aggregateByGroup(groupByList, records);
    }

    /**
     * Get the projected entity costs for a set of entities.
     *
     * @param entityIds The entities to retrieve the costs for. An empty set will get no results.
     * @param planId    Id of the Plan
     * @return A map of (id) -> (projected entity cost). Entities in the input that do not have an
     * associated projected costs will not have an entry in the map.
     */
    @Nonnull
    public Map<Long, EntityCost> getPlanProjectedEntityCosts(@Nonnull final Set<Long> entityIds, final long planId) {
        if (entityIds.isEmpty()) {
            return Collections.emptyMap();
        }

        return getPlanProjectedEntityCosts(planId).stream()
                .filter(ec -> entityIds.contains(ec.getAssociatedEntityId()))
                .collect(Collectors.toMap(EntityCost::getAssociatedEntityId, Function.identity()));
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

    /**
     * Return the plan ids we have data for.
     *
     * @return The set of plan ids.
     */
    @Nonnull
    public Set<Long> getPlanIds() {
        return dslContext.selectDistinct(Tables.PLAN_PROJECTED_ENTITY_COST.PLAN_ID)
            .from(Tables.PLAN_PROJECTED_ENTITY_COST)
            .fetch().stream()
            .map(Record1::value1)
            .collect(Collectors.toSet());
    }

    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nullable Void context) throws DiagnosticsException {
        // TODO to be implemented as part of OM-58627
    }

    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender) throws DiagnosticsException {
        dslContext.transaction(transactionContext -> {
            final DSLContext transaction = DSL.using(transactionContext);
            Stream<PlanProjectedEntityCostRecord> latestRecords = transaction.selectFrom(Tables.PLAN_PROJECTED_ENTITY_COST).stream();
            latestRecords.forEach(s -> {
                try {
                    appender.appendString(s.formatJSON());
                } catch (DiagnosticsException e) {
                    logger.error("Exception encountered while appending plan projected entity cost records" +
                            " to the diags dump", e);
                }
            });
        });
    }

    @Nonnull
    @Override
    public String getFileName() {
        return planEntityCostDumpFile;
    }
}
