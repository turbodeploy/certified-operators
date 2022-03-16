package com.vmturbo.cost.component.savings.bottomup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.savings.bottomup.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.bottomup.TopologyEvent.EventType;
import com.vmturbo.cost.component.savings.tem.VolumeProviderInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Tests persistent entity events journal.
 */
@RunWith(Parameterized.class)
public class SqlEntityEventsJournalTest extends MultiDbTestBase {
    /**
     * Persistent events journal being tested.
     */
    private final EntityEventsJournal journalStore;

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    /**
     * Context for DB ops.
     */
    private final DSLContext dsl;

    /**
     * DB read/write chunk size.
     */
    private final int chunkSize = 10;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SqlEntityEventsJournalTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
        this.journalStore = new SqlEntityEventsJournal(dsl, chunkSize);
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    /**
     * Setup before each test.
     */
    @Before
    public void setup() {
        insertRecords();
    }

    /**
     * Cleanup after test.
     */
    @After
    public void teardown() {
        this.journalStore.removeAllEvents();
    }

    /**
     * Makes a topology event using the given entity id as a factor for setting other values.
     *
     * @param entityId Entity id to use.
     * @return SavingsEvent instance.
     */
    static SavingsEvent createTopologyEvent(long entityId) {
        final Map<Integer, Double> volCommodityMap = ImmutableMap.of(
                CommodityType.STORAGE_AMOUNT_VALUE, 100d,
                CommodityType.IO_THROUGHPUT_VALUE, 200d,
                CommodityType.STORAGE_ACCESS_VALUE, 300d);
        return new SavingsEvent.Builder()
                .entityId(entityId)
                .timestamp(1000L + entityId)
                .expirationTime(2001L * entityId)
                .entityPriceChange(new EntityPriceChange.Builder()
                        .sourceCost(10.5d * (double)entityId)
                        .destinationCost(20.6d * (double)entityId)
                        .sourceOid(300L * entityId)
                        .destinationOid(400L * entityId)
                        .active(entityId % 2 == 0)
                        .build())
                .topologyEvent(new TopologyEvent.Builder()
                        .timestamp(1001L * entityId)
                        .entityOid(entityId)
                        .eventType(EventType.PROVIDER_CHANGE.getValue())
                        .entityType(EntityType.VIRTUAL_VOLUME.getValue())
                        .providerOid(5000L * entityId)
                        .entityRemoved(entityId % 3 == 0)
                        .providerInfo(new VolumeProviderInfo(5000L * entityId, volCommodityMap))
                        .build())
                .build();
    }

    /**
     * Makes an action event using the given entity id as a factor for setting other values.
     *
     * @param entityId Entity id to use.
     * @return SavingsEvent instance.
     */
    static SavingsEvent createActionEvent(long entityId) {
        return new SavingsEvent.Builder()
                .entityId(entityId)
                .timestamp(2000L + entityId)
                .expirationTime(2002L * entityId)
                .entityPriceChange(new EntityPriceChange.Builder()
                        .sourceCost(20.5d * (double)entityId)
                        .destinationCost(30.6d * (double)entityId)
                        .sourceOid(302L * entityId)
                        .destinationOid(402L * entityId)
                        .active(entityId % 2 != 0)
                        .build())
                .actionEvent(new ActionEvent.Builder()
                        .actionId(700L * entityId)
                        .eventType(ActionEventType.RECOMMENDATION_ADDED)
                        .description("Test description: " + entityId)
                        .entityType(EntityType.VIRTUAL_VOLUME.getValue())
                        .actionType(ActionType.SCALE_VALUE)
                        .actionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT_VALUE)
                        .build())
                .build();
    }

    /**
     * Run all existing tests that were written for in-memory store.
     */
    @Test
    public void addEvents() {
        assertEquals(20, journalStore.size());
    }

    /**
     * Tests timestamp based query.
     * Times in DB are 1001 - 1010 and 2011 to 2020
     */
    @Test
    public void getEventsBetween() {
        assertEquals(20, journalStore.size());

        // End time is exclusive
        long count = journalStore.getEventsBetween(1001, 1011).count();
        assertEquals(10, count);

        count = journalStore.getEventsBetween(2011, 2021).count();
        assertEquals(10, count);
    }

    /**
     * Verifies that query to get events in time range but for specific entityIds works.
     */
    @Test
    public void getEventsBetweenWithEntityIds() {
        // This should normally give 20 records, but filter by entityId as well.
        assertEquals(20, journalStore.size());

        long count = journalStore.getEventsBetween(1001, 2021,
                ImmutableSet.of(5L, 7L, 15L, 17L)).count();
        assertEquals(4, count);
    }

    /**
     * Removes events older than specified time (exclusive).
     */
    @Test
    public void purgeEventsOlderThan() {
        assertEquals(20, journalStore.size());
        // Older than 2018 (exclusive) means that will leave 3 records - 2018, 2019, 2020.
        int removedCount = journalStore.purgeEventsOlderThan(2018);
        assertEquals(17, removedCount);
        assertEquals(3, journalStore.size());
    }

    /**
     * Removes events since specified time (inclusive)
     */
    @Test
    public void removeEventsSince() {
        assertEquals(20, journalStore.size());
        // That should leave 2 records - 1001, 1002.
        int removedCount = journalStore.removeEventsSince(1003).size();
        assertEquals(18, removedCount);
        assertEquals(2, journalStore.size());
    }

    /**
     * Removes matching events between the specified times - start (inclusive) & end (exclusive)
     */
    @Test
    public void removeEventsBetweenWithEntityIds() {
        assertEquals(20, journalStore.size());
        // This should leave 2 records - 1001 and 2020.
        int removedCount = journalStore.removeEventsBetween(1002, 2020,
                ImmutableSet.of(6L, 10L, 13L)).size();
        assertEquals(3, removedCount);
        assertEquals(17, journalStore.size());
    }

    /**
     * Removes events between the specified times - start (inclusive) & end (exclusive)
     */
    @Test
    public void removeEventsBetween() {
        assertEquals(20, journalStore.size());
        // This should leave 2 records - 1001 and 2020.
        int removedCount = journalStore.removeEventsBetween(1002, 2020).size();
        assertEquals(18, removedCount);
        assertEquals(2, journalStore.size());
    }

    /**
     * Removes all existing events.
     */
    @Test
    public void removeAllEvents() {
        assertEquals(20, journalStore.size());
        int removedCount = journalStore.removeAllEvents().size();
        assertEquals(20, removedCount);
        assertEquals(0, journalStore.size());
    }

    /**
     * Checks time of the oldest event in DB.
     */
    @Test
    public void getOldestEventTime() {
        assertEquals(20, journalStore.size());
        Long oldestTime = journalStore.getOldestEventTime();
        assertNotNull(oldestTime);
        assertEquals(1001L, (long)oldestTime);
    }

    /**
     * Adds records to DB for testing.
     */
    private void insertRecords() {
        final List<SavingsEvent> events = new ArrayList<>();
        int counter = 1;
        while (counter <= 10) {
            events.add(createTopologyEvent(counter++));
        }
        while (counter <= 20) {
            events.add(createActionEvent(counter++));
        }
        journalStore.addEvents(events);
    }
}
