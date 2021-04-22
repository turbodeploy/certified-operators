package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import java.math.BigDecimal;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

public class ComputeTierDemandStatsStoreTest {

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private final DSLContext dsl = dbConfig.getDslContext();


    @Test
    public void testPersistence() {

        // Construct 2 records across different hours
        final ComputeTierTypeHourlyByWeekRecord recordA = new ComputeTierTypeHourlyByWeekRecord(
                (byte)1, (byte)2, 3L, 4L, 5L, (byte)6, (byte)7, new BigDecimal("8.000"), new BigDecimal("9.000"));
        final ComputeTierTypeHourlyByWeekRecord recordB = new ComputeTierTypeHourlyByWeekRecord(
                (byte)1, (byte)3, 3L, 4L, 5L, (byte)6, (byte)7, new BigDecimal("8.000"), new BigDecimal("9.000"));

        final ComputeTierDemandStatsStore store = new ComputeTierDemandStatsStore(dsl, 1, 1);

        // persist the records
        store.persistComputeTierDemandStats(ImmutableList.of(recordA, recordB), false);

        // verify the records match
        final List<ComputeTierTypeHourlyByWeekRecord> actualRecords = dsl.selectFrom(Tables.COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                .fetchInto(ComputeTierTypeHourlyByWeekRecord.class);

        assertThat(actualRecords, hasSize(2));
        assertThat(actualRecords, containsInAnyOrder(recordA, recordB));
    }

}
