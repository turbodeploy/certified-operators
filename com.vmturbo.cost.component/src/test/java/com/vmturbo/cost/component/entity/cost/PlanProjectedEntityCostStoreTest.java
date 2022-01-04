package com.vmturbo.cost.component.entity.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.util.TriConsumer;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.StatValue;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityCostRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedReservedInstanceCoverageRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedReservedInstanceUtilizationRecord;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Unit tests for the plan projected entity cost store.
 */
@RunWith(Parameterized.class)
public class PlanProjectedEntityCostStoreTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public PlanProjectedEntityCostStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    private static final long RT_TOPO_CONTEXT_ID = 777777L;
    private static final long PLAN_ID = 1L;
    private static final double DELTA = 0.01;

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

    @Test
    public void testUpdateProjectedEntityCostsTable() {
        initializeCostStore(chunkSize);
        commonUpdateProjectedEntityCostsTableVerification();
    }

    /**
     * Test listing plan IDs the store knows about.
     */
    @Test
    public void testListPlanIds() {
        PlanProjectedEntityCostStore store = initializeCostStore(chunkSize);
        assertThat(store.getPlanIds(), containsInAnyOrder(PLAN_ID));
    }

    @Test
    public void testChunkedUpdateProjectedEntityCostsTable() {
        // This is a chunk test, so need to complete test setup. Use a chunk size of 1 to force
        // processing in multiple chunks.
        initializeCostStore(1);
        commonUpdateProjectedEntityCostsTableVerification();
    }

    private void commonUpdateProjectedEntityCostsTableVerification() {
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

    private PlanProjectedEntityCostStore initializeCostStore(final int chunkSize) {
        PlanProjectedEntityCostStore store = new PlanProjectedEntityCostStore(dsl, chunkSize);
        TopologyInfo topoInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(PLAN_ID)
                .setTopologyId(0l)
                .setTopologyType(TopologyType.PLAN)
                .build();
        List<EntityCost> cost = Arrays.asList(VM_COST, VOLUME_COST);
        store.insertPlanProjectedEntityCostsTableForPlan(topoInfo, cost);

        // Insert 1 row into plan_projected_entity_to_reserved_instance_mapping.
        List<PlanProjectedEntityToReservedInstanceMappingRecord> entityToRiMappingRecords =
                new ArrayList<>();
        long entityId = 73320835644009L;
        long reservedInstanceId = 706683383732672L;
        double usedCoupons = 4d;
        entityToRiMappingRecords.add(dsl.newRecord(
                Tables.PLAN_PROJECTED_ENTITY_TO_RESERVED_INSTANCE_MAPPING,
                new PlanProjectedEntityToReservedInstanceMappingRecord(entityId,
                        topoInfo.getTopologyContextId(),  reservedInstanceId, usedCoupons)));
        dsl.batchInsert(entityToRiMappingRecords).execute();

        // Insert 1 row into plan_projected_reserved_instance_coverage.
        List<PlanProjectedReservedInstanceCoverageRecord> riCoverageRecords = new ArrayList<>();
        long regionId = 73320835643877L;
        long zoneId = 73320835643820L;
        long accountId = 73320835644295L;
        double totalCoupons = 4d;
        riCoverageRecords.add(dsl.newRecord(
                Tables.PLAN_PROJECTED_RESERVED_INSTANCE_COVERAGE,
                new PlanProjectedReservedInstanceCoverageRecord(entityId,
                        topoInfo.getTopologyContextId(), regionId, zoneId, accountId, totalCoupons,
                        usedCoupons)));
        dsl.batchInsert(riCoverageRecords).execute();

        // Insert 1 row into plan_projected_reserved_instance_utilization.
        List<PlanProjectedReservedInstanceUtilizationRecord> riUtilizationRecords =
                new ArrayList<>();
        riUtilizationRecords.add(dsl.newRecord(
                Tables.PLAN_PROJECTED_RESERVED_INSTANCE_UTILIZATION,
                new PlanProjectedReservedInstanceUtilizationRecord(reservedInstanceId,
                        topoInfo.getTopologyContextId(), regionId, zoneId,
                        accountId, totalCoupons, usedCoupons)));
        dsl.batchInsert(riUtilizationRecords).execute();
        return store;
    }

    /**
     * Tests plan projected entity costs deletion.
     */
    @Test
    public void testDeletePlanProjectedEntityCosts() {
        final PlanProjectedEntityCostStore store = initializeCostStore(chunkSize);
        int rows = store.deletePlanProjectedCosts(PLAN_ID);
        // 2 rows in plan_projected_entity_cost, 1 each in
        // plan_projected_entity_to_reserved_instance_mapping,
        // plan_projected_reserved_instance_coverage,
        // plan_projected_reserved_instance_utilization
        assertEquals(5, rows);
    }

    /**
     * Tests grouped plan projected stats record getting.
     */
    @Test
    public void testGetPlanProjectedStatRecordsByGroup() {
        final PlanProjectedEntityCostStore store = initializeCostStore(chunkSize);

        final TriConsumer<String, Double, Collection<StatRecord>> checkResult =
                (String expectedUnits, Double expectedAmount, Collection<StatRecord> entityCostStatRecords) -> {
                    Assert.assertEquals(1, entityCostStatRecords.size());
                    final StatRecord statRecord = entityCostStatRecords.iterator().next();
                    final StatValue values = statRecord.getValues();
                    Assert.assertEquals(8L, statRecord.getAssociatedEntityId());
                    Assert.assertEquals(CostCategory.STORAGE, statRecord.getCategory());
                    Assert.assertEquals(expectedUnits, statRecord.getUnits());
                    Assert.assertEquals(expectedAmount, values.getMax(), DELTA);
                    Assert.assertEquals(expectedAmount, values.getMin(), DELTA);
                    Assert.assertEquals(expectedAmount, values.getTotal(), DELTA);
                    Assert.assertEquals(expectedAmount, values.getAvg(), DELTA);
                };

        final Object[][] testCases = {
                {TimeFrame.LATEST, 50D, true},
                {TimeFrame.HOUR, 50D, true},
                {TimeFrame.DAY, 1200D, true},
                {TimeFrame.MONTH, 36500D, true},
                {TimeFrame.YEAR, 438000D, true},
                {TimeFrame.LATEST, 50D, false},
                {TimeFrame.HOUR, 50D, false},
                {TimeFrame.DAY, 50D, false},
                {TimeFrame.MONTH, 50D, false},
                {TimeFrame.YEAR, 50D, false}
        };

        Stream.of(testCases).forEach(data -> {
            final TimeFrame timeFrame = (TimeFrame)data[0];
            final double amount = (double)data[1];
            final boolean totalValuesRequested = (boolean)data[2];

            final EntityCostFilter filter = EntityCostFilterBuilder.newBuilder(timeFrame,
                    RT_TOPO_CONTEXT_ID)
                    .latestTimestampRequested(true)
                    .entityIds(Arrays.asList(7L, 8L))
                    .totalValuesRequested(totalValuesRequested)
                    .costCategoryFilter(CostCategoryFilter.newBuilder()
                            .addCostCategory(CostCategory.STORAGE)
                            .build())
                    .build();
            final List<GroupBy> groupByList = Arrays.asList(GroupBy.ENTITY_TYPE,
                    GroupBy.COST_CATEGORY);
            final Collection<StatRecord> records = store.getPlanProjectedStatRecordsByGroup(
                    groupByList, filter, PLAN_ID);

            final String units =
                    totalValuesRequested ? timeFrame.getUnits() : TimeFrame.HOUR.getUnits();

            checkResult.accept(units, amount, records);
        });
    }
}
