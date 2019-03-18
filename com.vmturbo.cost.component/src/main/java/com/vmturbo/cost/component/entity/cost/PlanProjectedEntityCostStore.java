package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.cost.component.db.tables.PlanProjectedEntityCost.PLAN_PROJECTED_ENTITY_COST;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.InsertValuesStep3;
import org.jooq.impl.DSL;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.db.tables.pojos.PlanProjectedEntityCost;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityCostRecord;

public class PlanProjectedEntityCostStore {

    private final DSLContext dslContext;

    private final int chunkSize;

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
                        transactionDsl.insertInto(PLAN_PROJECTED_ENTITY_COST,
                                                  PLAN_PROJECTED_ENTITY_COST.PLAN_ID,
                                                  PLAN_PROJECTED_ENTITY_COST.ASSOCIATED_ENTITY_ID,
                                                  PLAN_PROJECTED_ENTITY_COST.ENTITY_COST);
                for (EntityCost cost : entityCosts) {
                    step = step.values(topoInfo.getTopologyContextId(),
                                       cost.getAssociatedEntityId(),
                                       cost);
                }
                step.execute();
            });
        });
    }
}
