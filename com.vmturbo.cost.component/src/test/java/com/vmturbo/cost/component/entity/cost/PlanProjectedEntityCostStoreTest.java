package com.vmturbo.cost.component.entity.cost;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityCostRecord;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit tests for the plan projected entity cost store.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class PlanProjectedEntityCostStoreTest {
    private static final long PLAN_ID = 1L;
    private static final double DELTA = 0.01;

    @Autowired
    protected TestSQLDatabaseConfig dbConfig;
    private Flyway flyway;
    private PlanProjectedEntityCostStore store;
    private DSLContext dsl;
    private final int chunkSize = 10;
    private static final EntityCost VM_COST = EntityCost.newBuilder()
                    .setAssociatedEntityId(7L)
                    .setAssociatedEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(100).setCurrency(840)))
            .build();

    private static final EntityCost VOLUME_COST = EntityCost.newBuilder()
                    .setAssociatedEntityId(8L)
                    .setAssociatedEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .addComponentCost(ComponentCost.newBuilder()
                    .setCategory(CostCategory.STORAGE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setAmount(50).setCurrency(840)))
            .build();

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        store = new PlanProjectedEntityCostStore(dsl, chunkSize);
        updateCostsTable();
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testUpdateProjectedEntityCostsTable() {
        final List<PlanProjectedEntityCostRecord> records = dsl
                .selectFrom(Tables.PLAN_PROJECTED_ENTITY_COST).fetch();
        assertEquals(2, records.size());
        final Optional<PlanProjectedEntityCostRecord> rcdOpt = records.stream().filter(r -> r.getAssociatedEntityId() == 7).findAny();
        assertTrue(rcdOpt.isPresent());
        final PlanProjectedEntityCostRecord rcd = rcdOpt.get();
        assertEquals(PLAN_ID, rcd.getPlanId(), DELTA);
        final EntityCost entityCost = rcd.getEntityCost();
        assertEquals(EntityType.VIRTUAL_MACHINE_VALUE, entityCost.getAssociatedEntityType());
        final ComponentCost componentCost = entityCost.getComponentCostList().get(0);
        assertEquals(CostCategory.ON_DEMAND_COMPUTE, componentCost.getCategory());
        assertEquals(840, componentCost.getAmount().getCurrency());
        assertEquals(100.0000, componentCost.getAmount().getAmount(), DELTA);
    }

    private void updateCostsTable() {
        TopologyInfo topoInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(PLAN_ID)
                .setTopologyId(0l)
                .setTopologyType(TopologyType.PLAN)
                .build();
        List<EntityCost> cost = Arrays.asList(VM_COST, VOLUME_COST);
        store.updatePlanProjectedEntityCostsTableForPlan(topoInfo, cost);
    }

    /**
     * Tests plan projected entity costs deletion.
     */
    @Test
    public void testDeletePlanProjectedEntityCosts() {
        int rows = store.deletePlanProjectedEntityCost(PLAN_ID);
        assertEquals(2, rows);
    }

    /**
     * Tests grouped plan projected stats record getting.
     */
    @Test
    public void testGetPlanProjectedStatRecordsByGroup() {
        final EntityCostFilter filter = EntityCostFilterBuilder.newBuilder(TimeFrame.LATEST)
                        .latestTimestampRequested(true)
                        .entityIds(Arrays.asList(7L, 8L))
                        .costCategoryFilter(CostCategoryFilter.newBuilder().addCostCategory(CostCategory.STORAGE)
                                        .build())
                        .build();
        final List<GroupBy> groupByList = Arrays.asList(GroupBy.ENTITY_TYPE, GroupBy.COST_CATEGORY);
        final Collection<StatRecord> records = store.getPlanProjectedStatRecordsByGroup(groupByList, filter, PLAN_ID);
        assertEquals(1, records.size());
        StatRecord record = records.iterator().next();
        assertEquals(8L, record.getAssociatedEntityId());
        assertEquals(CostCategory.STORAGE, record.getCategory());
        assertEquals(50, record.getValues().getAvg(), DELTA);
        assertEquals(StringConstants.DOLLARS_PER_HOUR, record.getUnits());
    }
}
