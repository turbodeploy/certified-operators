package com.vmturbo.cost.component.entity.cost;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
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
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.calculation.CostJournal;
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
    private final int ASSOCIATED_ENTITY_TYPE2 = 1;

    private final LocalDateTime curTime = LocalDateTime.now();
    private final ComponentCost componentCost = ComponentCost.newBuilder()
            .setAmount(CurrencyAmount.newBuilder().setAmount(3.111).setCurrency(1))
            .setCategory(CostCategory.COMPUTE)
            .build();
    private final ComponentCost componentCost1 =
            ComponentCost.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder().setAmount(2.111).setCurrency(1))
                    .setCategory(CostCategory.IP)
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
    private SQLEntityCostStore store;
    private DSLContext dsl;

    /**
     * The clock can't start at too small of a number because TIMESTAMP starts in 1970, but
     * epoch millis starts in 1969.
     */
    private MutableFixedClock clock = new MutableFixedClock(Instant.ofEpochMilli(1_000_000_000), ZoneId.systemDefault());

    @Before
    public void setup() throws Exception {
        flyway = dbConfig.flyway();
        dsl = dbConfig.dsl();
        flyway.clean();
        flyway.migrate();
        store = new SQLEntityCostStore(dsl, clock, 1);
    }

    @Test
    public void testGetCostWithDates() throws DbException, InvalidEntityCostsException {

        // insert
        saveCosts();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(now, now.minusHours(1l));
        validateResults(results, 1, 2, 2);

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
    public void testGetCostsWithTwoTimeStamps() throws InterruptedException, InvalidEntityCostsException, DbException {

        // insert
        saveCostsWithTwoTimeStamps();

        // get by date
        final LocalDateTime now = LocalDateTime.now(clock);
        Map<Long, Map<Long, EntityCost>> results = store.getEntityCosts(now, now.minusHours(1l));
        validateResults(results, 2, 2, 2);

        // clean up
        store.cleanEntityCosts(now);
        assertEquals(0, store.getEntityCosts(now, now.minusHours(1l)).size());

    }

    @Test
    public void testStoreCostJournal() throws DbException {
        final CostJournal<TopologyEntityDTO> journal1 = mockCostJournal(ID1, ASSOCIATED_ENTITY_TYPE1,
                ImmutableMap.of(CostCategory.COMPUTE, 7.0, CostCategory.LICENSE, 3.0));
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
                    .setCategory(CostCategory.COMPUTE)
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(CurrencyAmount.getDefaultInstance().getCurrency())
                            .setAmount(7.0))
                    .build(),
                ComponentCost.newBuilder()
                    .setCategory(CostCategory.LICENSE)
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
            when(journal.getCostForCategory(category))
                .thenReturn(costsByCategory.getOrDefault(category, 0.0));
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

    private void saveCostsWithTwoTimeStamps() throws DbException, InvalidEntityCostsException, InterruptedException {
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1));
        clock.changeInstant(clock.instant().plusMillis(1000));
        store.persistEntityCosts(ImmutableList.of(entityCost, entityCost1));
    }
}
