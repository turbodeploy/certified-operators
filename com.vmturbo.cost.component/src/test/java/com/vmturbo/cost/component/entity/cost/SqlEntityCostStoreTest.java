package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.trax.Trax.trax;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class SqlEntityCostStoreTest {
    public static final long ID1 = 1L;
    public static final long ID2 = 2L;
    private final int ASSOCIATED_ENTITY_TYPE1 = 1;
    private final int ASSOCIATED_ENTITY_TYPE2 = 2;

    private final LocalDateTime curTime = LocalDateTime.now();
    private final ComponentCost componentCost = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(3.111).setCurrency(1))
            .setCategory(CostCategory.ON_DEMAND_COMPUTE)
            .setCostSource(CostSource.ON_DEMAND_RATE)
            .build();
    private final ComponentCost componentCost1 =
            ComponentCost.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder().setAmount(2.111).setCurrency(1))
                    .setCategory(CostCategory.IP)
                    .setCostSource(CostSource.ON_DEMAND_RATE)
                    .build();
    private final EntityCost entityCost = EntityCost.newBuilder()
            .setAssociatedEntityId(ID1)
            .addComponentCost(componentCost)
            .addComponentCost(componentCost1)
            .setTotalAmount(CurrencyAmount.newBuilder().setAmount(1.111).setCurrency(1).build())
            .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE1)
            .build();
    private final EntityCost entityCost1 = EntityCost.newBuilder()
            .setAssociatedEntityId(ID2)
            .addComponentCost(componentCost)
            .addComponentCost(componentCost1)
            .setTotalAmount(CurrencyAmount.newBuilder().setAmount(1.111).setCurrency(1).build())
            .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE2)
            .build();
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;
    private Flyway flyway;
    private SqlEntityCostStore store;
    private DSLContext dsl;

    /**
     * The clock can't start at too small of a number because TIMESTAMP starts in 1970, but
     * epoch millis starts in 1969.
     */
    private MutableFixedClock clock = new MutableFixedClock(1_000_000_000);

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        store = new SqlEntityCostStore(dsl, clock, 1);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testGetCostWithDates() throws DbException, InvalidEntityCostsException {

        // insert
        saveCosts();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(now.minusHours(1l), now);
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetCostWithEntityCostFilter() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(), Collections.emptySet(),
            now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(), now.plusDays(1l)
            .toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(), TimeFrame.LATEST);

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterHourEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(), Collections.emptySet(),
            now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                now.plusDays(1l).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(), TimeFrame.HOUR);

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterMonthEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(), Collections.emptySet(),
            now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                now.plusDays(1l).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(), TimeFrame.MONTH);

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterDayEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(), Collections.emptySet(),
            now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                now.plusDays(1l).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(), TimeFrame.DAY);

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetCostWithDatesAndEntityIds() throws DbException, InvalidEntityCostsException {

        // insert
        saveCosts();

        final LocalDateTime now = LocalDateTime.now(clock);
        // get by date with ids.
        Map<Long, Map<Long, EntityCost>> resultsWithIds = store.getEntityCosts(ImmutableSet.of(1L, 2L), now.minusHours(1l), now);
        validateResults(resultsWithIds, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterWithEntityId() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter =
                new EntityCostFilter(ImmutableSet.of(1L, 2L), Collections.emptySet(),
                        now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        TimeFrame.LATEST);

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterWithEntityTypeFilter() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter =
                new EntityCostFilter(ImmutableSet.of(1L, 2L), ImmutableSet.of(1),
                        now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        TimeFrame.LATEST);

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);

        assertEquals(1, results.size());

        // ensure in the same timestamp, we have expected entity costs
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.size() == 1));

        // ensure we have the right entity costs.
        assertTrue(results.values().stream().allMatch(entityCosts ->
                isSameEntityCosts(entityCosts.get(ID1), entityCost)));

        // ensure in the same timestamp, every entity cost have expected component costs
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.values().stream()
                .allMatch(entityCost -> entityCost.getComponentCostCount() == 2)));

        // ensure the components are the same
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.values().stream().allMatch(entityCost ->
                entityCost.getComponentCostList().contains(componentCost)
                        && entityCost.getComponentCostList().contains(componentCost1))));

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetCostsWithTwoTimeStamps() throws InterruptedException, InvalidEntityCostsException, DbException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(now.minusHours(1l), now);
        validateResults(results, 2, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());

    }

    @Test
    public void testGetLatestEntityCost() throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getLatestEntityCost(Collections.emptySet(), Collections.emptySet());
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetLatestEntityCostWithOidFilter() throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getLatestEntityCost(ImmutableSet.of(1l), Collections.emptySet());
        assertEquals(1, results.size());
        // ensure in the same timestamp, we have expected entity costs
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.size() == 1));

        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.values().stream()
                .allMatch(entityCost -> entityCost.getComponentCostCount() == 2)));

        Map<Long, EntityCost> costsBySourceAndCategory = store.getLatestEntityCost(1l, CostCategory.ON_DEMAND_COMPUTE,
                new HashSet<>(Arrays.asList(CostSource.ON_DEMAND_RATE)));

        assertEquals(costsBySourceAndCategory.get(1L).getComponentCostCount(), 1);

        assertEquals(costsBySourceAndCategory.get(1L).getComponentCost(0), componentCost);
        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testGetLatestEntityCostWithTypeFilter() throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getLatestEntityCost(Collections.emptySet(), ImmutableSet.of(1));
        assertEquals(1, results.size());
        // ensure in the same timestamp, we have expected entity costs
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.size() == 1));
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.values().stream()
                .allMatch(entityCost -> entityCost.getComponentCostCount() == 2)));
        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());
    }

    @Test
    public void testStoreCostJournal() throws DbException {
        final CostJournal<TopologyEntityDTO> journal1 = mockCostJournal(ID1, ASSOCIATED_ENTITY_TYPE1,
                ImmutableMap.of(CostCategory.ON_DEMAND_COMPUTE, 7.0, CostCategory.ON_DEMAND_LICENSE, 3.0));
        store.persistEntityCost(ImmutableMap.of(ID1, journal1));

        final Map<Long, Map<Long, EntityCost>> costs =
                store.getEntityCosts(LocalDateTime.now(clock), LocalDateTime.now(clock));
        final Map<Long, EntityCost> costMap = costs.get(clock.millis());
        assertNotNull(costMap);
        final EntityCost entityCost = costMap.get(ID1);
        assertNotNull(entityCost);
        assertThat(entityCost.getAssociatedEntityType(), is(ASSOCIATED_ENTITY_TYPE1));
        assertThat(entityCost.getAssociatedEntityId(), is(ID1));
        assertThat(entityCost.getComponentCostCount(), is(2));
        assertThat(entityCost.getComponentCostList(), containsInAnyOrder(
                ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .setCostSource(CostSource.ON_DEMAND_RATE)
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                                .setAmount(7.0))
                        .build(),
                ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_LICENSE)
                        .setCostSource(CostSource.ON_DEMAND_RATE)
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                                .setAmount(3.0))
                        .build()));
    }

    private CostJournal<TopologyEntityDTO> mockCostJournal(final long entityId, final int entityType,
                                                           final Map<CostCategory, Double> costsByCategory) {
        final TopologyEntityDTO entity = TopologyEntityDTO.newBuilder()
                .setOid(entityId)
                .setEntityType(entityType)
                .build();
        final CostJournal<TopologyEntityDTO> journal = mock(CostJournal.class);
        when(journal.getEntity()).thenReturn(entity);
        when(journal.getCategories()).thenReturn(costsByCategory.keySet());
        for (final CostCategory category : CostCategory.values()) {
            when(journal.getHourlyCostBySourceAndCategory(category,Optional.of(CostSource.ON_DEMAND_RATE)))
                    .thenReturn(trax(costsByCategory.getOrDefault(category, 0.0)));
            when(journal.getHourlyCostForCategory(category))
                    .thenReturn(trax(costsByCategory.getOrDefault(category, 0.0)));
        }
        return journal;
    }

    private void validateResults(final Map<Long, Map<Long, EntityCost>> map
            , final int expectedSizeOfEntries
            , final int expectedSizeOfEntityCosts
            , final int expectedSizeOfComponentCosts) {
        // ensure have expected entries (timestamps)
        assertEquals(expectedSizeOfEntries, map.size());

        // ensure in the same timestamp, we have expected entity costs
        assertTrue(map.values().stream().allMatch(entityCosts -> entityCosts.size() == expectedSizeOfEntityCosts));

        // ensure we have the right entity costs.
        assertTrue(map.values().stream().allMatch(entityCosts ->
                isSameEntityCosts(entityCosts.get(ID1), entityCost) && isSameEntityCosts(entityCosts.get(ID2), entityCost1)));

        // ensure in the same timestamp, every entity cost have expected component costs
        assertTrue(map.values().stream().allMatch(entityCosts -> entityCosts.values().stream()
                .allMatch(entityCost -> entityCost.getComponentCostCount() == expectedSizeOfComponentCosts)));

        // ensure the components are the same
        assertTrue(map.values().stream().allMatch(entityCosts -> entityCosts.values().stream().allMatch(entityCost ->
                entityCost.getComponentCostList().contains(componentCost)
                        && entityCost.getComponentCostList().contains(componentCost1))));
    }

    private boolean isSameEntityCosts(final EntityCost entityCost, final EntityCost entityCost1) {
        return entityCost.getAssociatedEntityId() == entityCost1.getAssociatedEntityId()
                && entityCost.getAssociatedEntityId() == entityCost1.getAssociatedEntityId()
                && entityCost.getComponentCostCount() == entityCost1.getComponentCostCount();
    }

    private void saveCosts() throws DbException, InvalidEntityCostsException {
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1));
    }

    private void saveCostsWithTwoTimeStamps() throws DbException, InvalidEntityCostsException {
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1));
        clock.changeInstant(clock.instant().plusMillis(1000));
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1));
    }
}
