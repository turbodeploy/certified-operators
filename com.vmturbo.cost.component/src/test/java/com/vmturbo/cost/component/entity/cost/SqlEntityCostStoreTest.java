package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
import static com.vmturbo.trax.Trax.trax;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
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
    private final LocalDateTime now = LocalDateTime.now(clock);
    private final EntityCostFilter globalDefaultCostFilter = new EntityCostFilter(Collections.emptySet(),
            Collections.emptySet(),
            now.minusHours(1l).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
            now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
            TimeFrame.LATEST, Collections.emptySet());

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

        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(globalDefaultCostFilter);
        validateResults(results, 1, 2, 4);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetCostWithEntityCostFilter() throws DbException, InvalidEntityCostsException {
        // insert
        saveCosts();

        // get by date

        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(globalDefaultCostFilter);
        validateResults(results, 1, 2, 4);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterHourEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(), Collections.emptySet(),
                now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                now.plusDays(1l).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(), TimeFrame.HOUR, Collections.emptySet());

        // insert
        saveCosts();

        // get by date

        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterMonthEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(), Collections.emptySet(),
                now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                now.plusDays(1L).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(), TimeFrame.MONTH, Collections.emptySet());

        // insert
        saveCosts();

        // get by date

        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterDayEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(), Collections.emptySet(),
                now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                now.plusDays(1l).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(), TimeFrame.DAY, Collections.emptySet());

        // insert
        saveCosts();

        // get by date

        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetCostWithDatesAndEntityIds() throws DbException, InvalidEntityCostsException {

        // insert
        saveCosts();

        final LocalDateTime now = LocalDateTime.now(clock);
        // get by date with ids.
        EntityCostFilter entityCostFilter = new EntityCostFilter(ImmutableSet.of(1L, 2L), Collections.EMPTY_SET,
                now.minusHours(1l).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                TimeFrame.LATEST, Collections.emptySet());
        Map<Long, Collection<StatRecord>> resultsWithIds = store.getEntityCostStats(entityCostFilter);
        validateResults(resultsWithIds, 1, 2, 4);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterWithEntityId() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter =
                new EntityCostFilter(ImmutableSet.of(1L, 2L), Collections.emptySet(),
                        now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        TimeFrame.LATEST,Collections.emptySet());

        // insert
        saveCosts();

        // get by date

        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        validateResults(results, 1, 2, 4);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterWithEntityTypeFilter() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        final EntityCostFilter entityCostFilter =
                new EntityCostFilter(ImmutableSet.of(1L, 2L), ImmutableSet.of(1),
                        now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        now.plusDays(1l).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        TimeFrame.LATEST, Collections.emptySet());

        // insert
        saveCosts();

        // get by date

        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);

        assertEquals(1, results.size());

        // ensure in the same timestamp, we have expected entity costs
        assertEquals(1, results.values().stream().map(statRecords -> statRecords.stream()
                .map(StatRecord::getAssociatedEntityId)).collect(toSet()).size());

        // ensure we have the right entity costs.
        assertTrue(results.values().stream().allMatch(statRecords ->
                isSameEntityCosts(statRecords, EntityCostToStatRecordConverter.convertEntityToStatRecord(entityCost))));
        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetCostsWithTwoTimeStamps() throws InterruptedException, InvalidEntityCostsException, DbException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(),
                Collections.emptySet(),
                now.minusHours(1l).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                TimeFrame.LATEST, Collections.emptySet());
        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        validateResults(results, 2, 2, 8);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());

    }

    @Test
    public void testGetLatestEntityCost() throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.EMPTY_SET,
                Collections.EMPTY_SET,
                0,
                0,
                TimeFrame.LATEST, Collections.emptySet());
        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        validateResults(results, 1, 2, 4);

        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetLatestEntityCostWithOidFilter() throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter entityCostFilter = new EntityCostFilter(ImmutableSet.of(1l),
                Collections.EMPTY_SET,
                0,
                0,
                TimeFrame.LATEST, Collections.emptySet());
        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        assertEquals(1, results.size());
        // ensure in the same timestamp, we have expected entity costs
        assertEquals(1, results.values().stream().map(statRecords -> statRecords.stream().map(StatRecord::getAssociatedEntityId).collect(toSet())).collect(toSet())
                .size());

        Map<Long, EntityCost> costsBySourceAndCategory = store.getLatestEntityCost(1l, CostCategory.ON_DEMAND_COMPUTE,
                new HashSet<>(Arrays.asList(CostSource.ON_DEMAND_RATE)));

        assertEquals(costsBySourceAndCategory.get(1L).getComponentCostCount(), 1);

        assertEquals(costsBySourceAndCategory.get(1L).getComponentCost(0), componentCost);
        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testGetLatestEntityCostWithTypeFilter() throws DbException, InvalidEntityCostsException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(), ImmutableSet.of(1),
                0, 0, TimeFrame.LATEST, Collections.emptySet());
        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        assertEquals(1, results.size());
        // ensure in the same timestamp, we have expected entity costs
        validateResults(results, 1, 1, 2);
        // clean up
        cleanEntityCosts(now);
        assertEquals(0, store.getEntityCostStats(globalDefaultCostFilter).size());
    }

    @Test
    public void testStoreCostJournal() throws DbException {
        final CostJournal<TopologyEntityDTO> journal1 = mockCostJournal(ID1, ASSOCIATED_ENTITY_TYPE1,
                ImmutableMap.of(CostCategory.ON_DEMAND_COMPUTE, 7.0, CostCategory.ON_DEMAND_LICENSE, 3.0));
        store.persistEntityCost(ImmutableMap.of(ID1, journal1));

        EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.EMPTY_SET, Collections.EMPTY_SET,
                LocalDateTime.now(clock).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                LocalDateTime.now(clock).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                TimeFrame.LATEST, Collections.emptySet());
        final Map<Long, Collection<StatRecord>> costs =
                store.getEntityCostStats(entityCostFilter);
        final Collection<StatRecord> costMap = costs.get(clock.millis());
        assertNotNull(costMap);
        EntityCost entityCost2 = EntityCost.newBuilder()
                .setAssociatedEntityId(ID1)
                .setAssociatedEntityType(ASSOCIATED_ENTITY_TYPE1)
                .addComponentCost(ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .setCostSource(CostSource.ON_DEMAND_RATE)
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                                .setAmount(7.0))
                        .build())
                .addComponentCost(ComponentCost.newBuilder()
                        .setCategory(CostCategory.ON_DEMAND_LICENSE)
                        .setCostSource(CostSource.ON_DEMAND_RATE)
                        .setAmount(CurrencyAmount.newBuilder()
                                .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                                .setAmount(3.0))
                        .build()).build();
        assertTrue(isSameEntityCosts(costMap, EntityCostToStatRecordConverter.convertEntityToStatRecord(entityCost2)));
    }


    @Test
    public void testGetCostWithGroupAndFilter() throws DbException, InvalidEntityCostsException {

        // insert
        saveCostsWithTwoTimeStamps();
        EntityCostFilter entityCostFilter = new EntityCostFilter(ImmutableSet.of(1l),
                Collections.emptySet(),
                0,
                0,
                TimeFrame.LATEST, Collections.singleton(ENTITY_COST.ASSOCIATED_ENTITY_ID.getName()));
        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        assertThat(results.values().stream().mapToLong(Collection::size).sum(), is(1L));
    }


    @Test
    public void testGetCostWithGroupAndFilterInvalidType() throws DbException, InvalidEntityCostsException {
        // insert
        saveCostsWithTwoTimeStamps();
        EntityCostFilter entityCostFilter = new EntityCostFilter(Collections.emptySet(),
                ImmutableSet.of(100),
                0,
                0,
                TimeFrame.LATEST, Collections.singleton(ENTITY_COST.ASSOCIATED_ENTITY_ID.getName()));
        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        assertThat(results.values().stream().mapToLong(Collection::size).sum(), is(0L));

        entityCostFilter = new EntityCostFilter(Collections.emptySet(),
                Collections.emptySet(),
                0,
                0,
                TimeFrame.LATEST, Collections.singleton(ENTITY_COST.ASSOCIATED_ENTITY_ID.getName()));
        results = store.getEntityCostStats(
                entityCostFilter);
        assertThat(results.values().stream().mapToLong(Collection::size).sum(), is(2L));
        entityCostFilter = new EntityCostFilter(Collections.emptySet(),
                Collections.emptySet(),
                100,
                120,
                TimeFrame.LATEST, Collections.singleton(ENTITY_COST.ASSOCIATED_ENTITY_ID.getName()));
        results = store.getEntityCostStats(entityCostFilter);
        assertThat(results.values().stream().mapToLong(Collection::size).sum(), is(0L));
    }

    @Test
    public void testGetStatsByGroupAndFilterByAssociatedType() throws DbException, InvalidEntityCostsException {
        EntityCost anotherEntityCost = entityCost.toBuilder()
                .setAssociatedEntityType(EntityType.PHYSICAL_MACHINE.getValue()).build();
        store.persistEntityCosts(ImmutableList.of(anotherEntityCost));
        EntityCostFilter entityCostFilter = new EntityCostFilter(ImmutableSet.of(1L),
                ImmutableSet.of(EntityType.PHYSICAL_MACHINE.getValue()),
                0,
                0,
                TimeFrame.LATEST, Collections.singleton(ENTITY_COST.ASSOCIATED_ENTITY_TYPE.getName()));
        Map<Long, Collection<StatRecord>> results = store.getEntityCostStats(entityCostFilter);
        assertTrue(results.values().stream().flatMap(Collection::stream).map(StatRecord::getAssociatedEntityType)
                .allMatch(i3 -> i3.equals(EntityType.PHYSICAL_MACHINE.getValue())));
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

    private void validateResults(final Map<Long, Collection<StatRecord>> map,
                                 final int expectedSizeOfEntries,
                                 final int uniqueAssociatedId, final int totalStatRecord) {
        //uniqueAssociatedId should be equal to number of entities around
        // ensure have expected entries (timestamps)
        assertEquals(expectedSizeOfEntries, map.size());

        Set<Long> uniqueEntities = Sets.newHashSet();
        Collection<Long> allStatRecords = Lists.newArrayList();
        map.values().forEach(i1 -> {
            List<Long> listOfAssociatedEntityId = i1.stream().map(
                    StatRecord::getAssociatedEntityId).collect(Collectors.toList());
            uniqueEntities.addAll(listOfAssociatedEntityId);
            allStatRecords.addAll(listOfAssociatedEntityId);
            // ensure in the same timestamp, we have expected entity costs
            assertEquals(uniqueAssociatedId, uniqueEntities.size());
        });

        assertEquals(totalStatRecord, allStatRecords.size());
    }

    private boolean isSameEntityCosts(final Collection<StatRecord> statRecord, final Collection<StatRecord> expectedStatRecord) {
        long associatedEntityId = statRecord.iterator().next().getAssociatedEntityId();
        assertTrue(statRecord.stream().allMatch(i1 -> i1.getAssociatedEntityId() == associatedEntityId));
        assertTrue(expectedStatRecord.stream().allMatch(i1 -> i1.getAssociatedEntityId() == associatedEntityId));
        return statRecord.size() == expectedStatRecord.size();
    }

    private void saveCosts() throws DbException, InvalidEntityCostsException {
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1));
    }

    private void saveCostsWithTwoTimeStamps() throws DbException, InvalidEntityCostsException {
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1));
        clock.changeInstant(clock.instant().plusMillis(1000));
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1));
    }


    /**
     * Clean up entity costs based on start date. All the entity costs before {@param startDate} will be cleaned.
     *
     * @param startDate start date
     * @throws DbException if anything goes wrong in the database
     */
    public void cleanEntityCosts(@Nonnull final LocalDateTime startDate) throws DbException {
        try {
            dsl.deleteFrom(ENTITY_COST)
                    .where(ENTITY_COST.CREATED_TIME.le(startDate))
                    .execute();
        } catch (DataAccessException e) {
            throw new DbException("Failed to clean entity costs to DB" + e.getMessage());
        }
    }
}
