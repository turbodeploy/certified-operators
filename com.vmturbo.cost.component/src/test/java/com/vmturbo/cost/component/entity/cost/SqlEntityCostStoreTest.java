package com.vmturbo.cost.component.entity.cost;

import static com.vmturbo.trax.Trax.trax;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
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
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.util.EntityCostFilter;
import com.vmturbo.cost.component.util.EntityCostFilter.EntityCostFilterBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=cost"})
public class SqlEntityCostStoreTest {
    private static final long ID1 = 1L;
    private static final long ID2 = 2L;
    private static final int ASSOCIATED_ENTITY_TYPE1 = 1;
    private static final int ASSOCIATED_ENTITY_TYPE2 = 2;
    private static final long ACCOUNT1_ID = 10;
    private static final long ACCOUNT2_ID = 11;
    private static final long REGION1_ID = 20;
    private static final long REGION2_ID = 21;
    private static final long AZ1_ID = 31;
    private static final long AZ2_ID = 32;

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
        EntityCostFilter filter = getLastHourFilter();
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(filter);
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(filter).size());
    }

    private EntityCostFilter getLastHourFilter() {
        final long startDuration = clock.instant().minus(1, ChronoUnit.HOURS).toEpochMilli();
        final long endDuration = clock.millis();
        return EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .duration(startDuration, endDuration)
                .build();
    }

    @Test
    public void testGetCostWithEntityCostFilterHourEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter entityCostFilter = EntityCostFilterBuilder
                .newBuilder(TimeFrame.HOUR)
                .duration(now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                        now.plusDays(1L).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli())
                .build();

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterMonthEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter entityCostFilter = EntityCostFilterBuilder
                .newBuilder(TimeFrame.MONTH)
                .duration(now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                        now.plusDays(1L).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli())
                .build();

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterDayEmpty() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter entityCostFilter = EntityCostFilterBuilder
                .newBuilder(TimeFrame.DAY)
                .duration(now.toInstant(OffsetDateTime.now().getOffset()).toEpochMilli(),
                        now.plusDays(1L).toInstant(OffsetDateTime.now().getOffset()).toEpochMilli())
                .build();

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);
        validateResults(results, 0, 0, 0);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testGetCostWithDatesAndEntityIds() throws DbException, InvalidEntityCostsException {

        // insert
        saveCosts();

        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter filter = EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .entityIds(ImmutableSet.of(1L, 2L))
                .duration(clock.instant().minus(1, ChronoUnit.HOURS).toEpochMilli(), clock.millis())
                .build();
        // get by date with ids.
        Map<Long, Map<Long, EntityCost>> resultsWithIds = store.getEntityCosts(filter);
        validateResults(resultsWithIds, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterWithEntityId() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter filter = EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .entityIds(ImmutableSet.of(1L, 2L))
                .duration(clock.instant().minus(1, ChronoUnit.DAYS).toEpochMilli(), clock.millis())
                .build();

        // insert
        saveCosts();

        // get by date

        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(filter);
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testGetCostWithEntityCostFilterWithEntityTypeFilter() throws DbException, InvalidEntityCostsException {
        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        EntityCostFilter entityCostFilter = EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .entityIds(ImmutableSet.of(1L, 2L))
                .entityTypes(Collections.singleton(1))
                .duration(clock.instant().minus(1, ChronoUnit.DAYS).toEpochMilli(), clock.millis())
                .build();

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
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testGetCostsWithTwoTimeStamps() throws InterruptedException, InvalidEntityCostsException, DbException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(getLastHourFilter());
        validateResults(results, 2, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());

    }

    @Test
    public void testGetLatestEntityCost() throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST).latestTimestampRequested(true).build());
        validateResults(results, 1, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testGetLatestEntityCostWithOidFilter() throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .entityIds(Collections.singleton(1L))
                .latestTimestampRequested(true).build());
        assertEquals(1, results.size());
        // ensure in the same timestamp, we have expected entity costs
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.size() == 1));

        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.values().stream()
                .allMatch(entityCost -> entityCost.getComponentCostCount() == 2)));

        Map<Long, EntityCost> costsBySourceAndCategory = store.getEntityCosts(EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .entityIds(Collections.singleton(1L))
            .costSources(false, Collections.singleton(CostSource.ON_DEMAND_RATE.getNumber()))
            .costCategoryFilter(CostCategoryFilter.newBuilder()
                    .setExclusionFilter(false)
                    .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                    .build())
            .latestTimestampRequested(true).build()).values().iterator().next();

        assertEquals(costsBySourceAndCategory.get(1L).getComponentCostCount(), 1);

        assertEquals(costsBySourceAndCategory.get(1L).getComponentCost(0), componentCost);
        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testGetLatestEntityCostWithTypeFilter() throws DbException, InvalidEntityCostsException, InterruptedException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(EntityCostFilterBuilder
                .newBuilder(TimeFrame.LATEST)
                .entityTypes(Collections.singleton(1))
                .latestTimestampRequested(true).build());
        assertEquals(1, results.size());
        // ensure in the same timestamp, we have expected entity costs
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.size() == 1));
        assertTrue(results.values().stream().allMatch(entityCosts -> entityCosts.values().stream()
                .allMatch(entityCost -> entityCost.getComponentCostCount() == 2)));
        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(getLastHourFilter()).size());
    }

    @Test
    public void testStoreCostJournal() throws DbException {
        final CostJournal<TopologyEntityDTO> journal1 = mockCostJournal(ID1, ASSOCIATED_ENTITY_TYPE1,
                ImmutableMap.of(CostCategory.ON_DEMAND_COMPUTE, 7.0, CostCategory.ON_DEMAND_LICENSE, 3.0));
        CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);
        when(topology.getOwner(anyLong())).thenReturn(Optional.empty());
        when(topology.getConnectedAvailabilityZone(anyLong())).thenReturn(Optional.empty());
        when(topology.getConnectedRegion(anyLong())).thenReturn(Optional.empty());
        store.persistEntityCost(ImmutableMap.of(ID1, journal1), topology);

        final Map<Long, Map<Long, EntityCost>> costs =
                store.getEntityCosts(EntityCostFilterBuilder
                        .newBuilder(TimeFrame.LATEST)
                        .duration(clock.millis(), clock.millis())
                        .build()
                );
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

    /**
     * Test the case when we retrieve entity costs based on the account id.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetCostsForAccount() throws Exception {
        // ARRANGE
        EntityCostFilter entityCostFilter = EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .accountIds(Collections.singleton(ACCOUNT1_ID))
            .build();

        // insert
        saveCosts();

        // ACT
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);

        //ASSERT
        assertThat(results.size(), is(1));
        final Map<Long, EntityCost> entityMap = results.values().iterator().next();
        assertThat(entityMap.size(), is(1));
        assertThat(entityMap.keySet().iterator().next(), is(ID1));
        assertThat(entityMap.get(ID1), is(entityCost.toBuilder().clearTotalAmount().build()));


        store.cleanEntityCosts(LocalDateTime.now(clock));
    }

    /**
     * Test the case when we retrieve entity costs based on the availability zone id.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetCostsForAvailabilityZone() throws Exception {
        // ARRANGE
        EntityCostFilter entityCostFilter = EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .availabilityZoneIds(Collections.singleton(AZ1_ID))
            .build();

        // insert
        saveCosts();

        // ACT
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);

        //ASSERT
        assertThat(results.size(), is(1));
        final Map<Long, EntityCost> entityMap = results.values().iterator().next();
        assertThat(entityMap.size(), is(1));
        assertThat(entityMap.keySet().iterator().next(), is(ID1));
        assertThat(entityMap.get(ID1), is(entityCost.toBuilder().clearTotalAmount().build()));


        store.cleanEntityCosts(LocalDateTime.now(clock));
    }

    /**
     * Test the case when we retrieve entity costs based on the region id.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testGetCostsForRegion() throws Exception {
        // ARRANGE
        EntityCostFilter entityCostFilter = EntityCostFilterBuilder
            .newBuilder(TimeFrame.LATEST)
            .regionIds(Collections.singleton(REGION1_ID))
            .build();

        // insert
        saveCosts();

        // ACT
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(entityCostFilter);

        //ASSERT
        assertThat(results.size(), is(1));
        final Map<Long, EntityCost> entityMap = results.values().iterator().next();
        assertThat(entityMap.size(), is(1));
        assertThat(entityMap.keySet().iterator().next(), is(ID1));
        assertThat(entityMap.get(ID1), is(entityCost.toBuilder().clearTotalAmount().build()));


        store.cleanEntityCosts(LocalDateTime.now(clock));
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
            when(journal.getHourlyCostBySourceAndCategory(category, CostSource.ON_DEMAND_RATE))
                    .thenReturn(trax(costsByCategory.getOrDefault(category, 0.0)));
            when(journal.getHourlyCostForCategory(category))
                    .thenReturn(trax(costsByCategory.getOrDefault(category, 0.0)));
        }
        return journal;
    }

    private void validateResults(final Map<Long, Map<Long, EntityCost>> map,
                                 final int expectedSizeOfEntries,
                                 final int expectedSizeOfEntityCosts,
                                 final int expectedSizeOfComponentCosts) {
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
        TopologyEntityDTO account1EntityDTO = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOid(ACCOUNT1_ID).build();
        TopologyEntityDTO region1EntityDTO = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.REGION_VALUE)
            .setOid(REGION1_ID).build();
        TopologyEntityDTO az1EntityDTO = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setOid(AZ1_ID).build();
        TopologyEntityDTO account2EntityDTO = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
            .setOid(ACCOUNT2_ID).build();
        TopologyEntityDTO region2EntityDTO = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.REGION_VALUE)
            .setOid(REGION2_ID).build();
        TopologyEntityDTO az2EntityDTO = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE)
            .setOid(AZ2_ID).build();

        CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);
        when(topology.getOwner(ID1)).thenReturn(Optional.of(account1EntityDTO));
        when(topology.getConnectedAvailabilityZone(ID1)).thenReturn(Optional.of(az1EntityDTO));
        when(topology.getConnectedRegion(ID1)).thenReturn(Optional.of(region1EntityDTO));
        when(topology.getOwner(ID2)).thenReturn(Optional.of(account2EntityDTO));
        when(topology.getConnectedAvailabilityZone(ID2)).thenReturn(Optional.of(az2EntityDTO));
        when(topology.getConnectedRegion(ID2)).thenReturn(Optional.of(region2EntityDTO));
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1), topology);
    }

    private void saveCostsWithTwoTimeStamps() throws DbException, InvalidEntityCostsException {
        CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);
        when(topology.getOwner(anyLong())).thenReturn(Optional.empty());
        when(topology.getConnectedAvailabilityZone(anyLong())).thenReturn(Optional.empty());
        when(topology.getConnectedRegion(anyLong())).thenReturn(Optional.empty());
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1), topology);
        clock.changeInstant(clock.instant().plusMillis(1000));
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1), topology);
    }
}
