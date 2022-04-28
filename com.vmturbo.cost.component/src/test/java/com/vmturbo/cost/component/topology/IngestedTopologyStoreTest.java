package com.vmturbo.cost.component.topology;

import static com.vmturbo.cost.component.db.Tables.AGGREGATION_META_DATA;
import static com.vmturbo.cost.component.db.Tables.INGESTED_LIVE_TOPOLOGY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.scheduling.TaskScheduler;

import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.AggregationMetaDataRecord;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Test for {@link IngestedTopologyStore}.
 */
@RunWith(Parameterized.class)
public class IngestedTopologyStoreTest extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    private IngestedTopologyStore ingestedTopologyStore;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public IngestedTopologyStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost", TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    @Before
    public void setup() {
        this.ingestedTopologyStore = new IngestedTopologyStore(mock(TaskScheduler.class),
                Duration.ofHours(1), dsl);
    }

    /**
     * Test that cleanup works fine even if no aggregation happened.
     */
    @Test
    public void testCleanupWithoutAggregation() {
        // ensure no record in aggregation_meta_data
        dsl.deleteFrom(AGGREGATION_META_DATA).execute();
        // cleanup should not throw any exception
        ingestedTopologyStore.cleanup();
    }

    /**
     * Test that cleanup works as expected if aggregation happened.
     */
    @Test
    public void testCleanupWithAggregation() {
        // prepare
        dsl.insertInto(INGESTED_LIVE_TOPOLOGY).values(111L, Timestamp.valueOf("2022-04-01 19:05:34.000")).execute();
        dsl.insertInto(INGESTED_LIVE_TOPOLOGY).values(112L, Timestamp.valueOf("2022-04-24 19:05:34.000")).execute();
        final AggregationMetaDataRecord record = new AggregationMetaDataRecord();
        record.setAggregateTable("entity_cost");
        record.setLastAggregated(Timestamp.valueOf("2022-04-21 19:05:34.000"));
        record.setLastAggregatedByHour(Timestamp.valueOf("2022-04-21 19:03:56.000"));
        record.setLastAggregatedByDay(Timestamp.valueOf("2022-04-21 19:03:56.000"));
        record.setLastAggregatedByMonth(Timestamp.valueOf("2022-04-21 19:03:56.000"));
        dsl.insertInto(AGGREGATION_META_DATA).set(record).onDuplicateKeyUpdate().set(record).execute();

        // call
        ingestedTopologyStore.cleanup();
        // check that the record is removed
        Set<Long> res = dsl.select(INGESTED_LIVE_TOPOLOGY.TOPOLOGY_ID)
                .from(INGESTED_LIVE_TOPOLOGY)
                .fetchStream()
                .map(Record1::value1)
                .collect(Collectors.toSet());
        assertThat(res, containsInAnyOrder(112L));
        // clean
        dsl.deleteFrom(AGGREGATION_META_DATA).execute();
    }
}