package com.vmturbo.cost.component.savings;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.BilledCostDailyRecord;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Test cases for SqlBillingRecordStore.
 */
@RunWith(Parameterized.class)
public class SqlBillingRecordStoreTest extends MultiDbTestBase {
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

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public SqlBillingRecordStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Test bill records that are updated for 2 days after the day of use.
     */
    @Test
    public void testBillRecordQuerySkipDay() {
        /* Today is Nov 16.
         sample_date  last_updated
         Nov 15       Nov 16 2:00
         Nov 14       Nov 16 3:00
         Nov 13       Nov 15 2:00
         Nov 12       Nov 14 3:00
         Nov 11       Nov 13 4:00
         */
        dsl.truncateTable(Tables.BILLED_COST_DAILY);
        List<BilledCostDailyRecord> records = new ArrayList<>();
        records.add(createBillingRecord(1L,
                LocalDateTime.of(2022, 11, 15, 0, 0),
                LocalDateTime.of(2022, 11, 16, 2, 0)));
        records.add(createBillingRecord(2L,
                LocalDateTime.of(2022, 11, 14, 0, 0),
                LocalDateTime.of(2022, 11, 16, 3, 0)));
        records.add(createBillingRecord(3L,
                LocalDateTime.of(2022, 11, 13, 0, 0),
                LocalDateTime.of(2022, 11, 15, 2, 0)));
        records.add(createBillingRecord(4L,
                LocalDateTime.of(2022, 11, 12, 0, 0),
                LocalDateTime.of(2022, 11, 14, 3, 0)));
        records.add(createBillingRecord(5L,
                LocalDateTime.of(2022, 11, 11, 0, 0),
                LocalDateTime.of(2022, 11, 13, 4, 0)));
        dsl.batchInsert(records).execute();

        SqlBillingRecordStore store = new SqlBillingRecordStore(dsl);

        // Today is Nov 16.
        long nowMillis = TimeUtil.localTimeToMillis(LocalDateTime.of(2022, 11, 16, 10, 44), Clock.systemUTC());
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMillis),
                ZoneId.from(ZoneOffset.UTC));

        LocalDateTime lastProcessed = LocalDateTime.of(2022, 11, 13, 10, 0);
        int daysToSkip = 0;
        List<BillingRecord> result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(12, 13, 14, 15)));

        daysToSkip = 1;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(12, 13, 14)));

        daysToSkip = 2;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(12, 13)));

        lastProcessed = LocalDateTime.of(2022, 11, 14, 10, 10);
        daysToSkip = 0;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(13, 14, 15)));

        daysToSkip = 1;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(13, 14)));

        daysToSkip = 2;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.singletonList(13)));

        // If billed_savings record does not exist in the aggregation_meta_data table,
        // the last_processed time is set to beginning of yesterday. The following scenario
        // simulates this use case.
        lastProcessed = LocalDateTime.of(2022, 11, 15, 0, 0);
        daysToSkip = 0;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(13, 14, 15)));

        daysToSkip = 1;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(13, 14)));

        daysToSkip = 2;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.singletonList(13)));
    }

    /**
     * Test bill records that are updated for 1 days after the day of use.
     */
    @Test
    public void testBillRecordQueryFinalCostInOneDay() {
        dsl.truncateTable(Tables.BILLED_COST_DAILY);
        /* Today is Nov 14.
         sample_date  last_updated
         Nov 13       Nov 14 2:00
         Nov 12       Nov 13 3:00
         Nov 11       Nov 12 2:00
         Nov 10       Nov 11 3:00
         */
        List<BilledCostDailyRecord> records = new ArrayList<>();
        records.add(createBillingRecord(1L,
                LocalDateTime.of(2022, 11, 13, 0, 0),
                LocalDateTime.of(2022, 11, 14, 2, 0)));
        records.add(createBillingRecord(2L,
                LocalDateTime.of(2022, 11, 12, 0, 0),
                LocalDateTime.of(2022, 11, 13, 3, 0)));
        records.add(createBillingRecord(3L,
                LocalDateTime.of(2022, 11, 11, 0, 0),
                LocalDateTime.of(2022, 11, 12, 2, 0)));
        records.add(createBillingRecord(4L,
                LocalDateTime.of(2022, 11, 10, 0, 0),
                LocalDateTime.of(2022, 11, 11, 3, 0)));
        dsl.batchInsert(records).execute();

        SqlBillingRecordStore store = new SqlBillingRecordStore(dsl);

        // Today is Nov 14.
        long nowMillis = TimeUtil.localTimeToMillis(LocalDateTime.of(2022, 11, 14, 10, 44), Clock.systemUTC());
        Clock clock = Clock.fixed(Instant.ofEpochMilli(nowMillis),
                ZoneId.from(ZoneOffset.UTC));

        LocalDateTime lastProcessed = LocalDateTime.of(2022, 11, 11, 10, 0);
        int daysToSkip = 0;
        List<BillingRecord> result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(11, 12, 13)));

        daysToSkip = 1;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(11, 12)));

        daysToSkip = 2;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.singletonList(11)));

        lastProcessed = LocalDateTime.of(2022, 11, 12, 10, 0);
        daysToSkip = 0;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(12, 13)));

        daysToSkip = 1;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.singletonList(12)));

        daysToSkip = 2;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.emptyList()));

        lastProcessed = LocalDateTime.of(2022, 11, 13, 0, 0);
        daysToSkip = 0;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Arrays.asList(12, 13)));

        daysToSkip = 1;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.singletonList(12)));

        daysToSkip = 2;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.emptyList()));

        lastProcessed = LocalDateTime.of(2022, 11, 14, 1, 0);
        daysToSkip = 0;
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.singletonList(13)));

        lastProcessed = LocalDateTime.of(2022, 11, 14, 10, 0);
        result = queryBill(store, clock, lastProcessed, daysToSkip);
        validateResult(result, dates(Collections.emptyList()));
    }

    private List<BillingRecord> queryBill(SqlBillingRecordStore store, Clock clock, LocalDateTime lastProcessed, int daysToSkip) {
        LocalDateTime endTime = LocalDateTime.now(clock).truncatedTo(ChronoUnit.DAYS).minusDays(daysToSkip);
        Stream<BillingRecord> result = store.getUpdatedBillRecords(TimeUtil.localTimeToMillis(lastProcessed, clock),
                endTime, Collections.singleton(22222L));
        return result.collect(Collectors.toList());
    }

    private void validateResult(List<BillingRecord> result, List<LocalDateTime> sampleTimes) {
        Assert.assertEquals(sampleTimes.size(), result.size());
        List<LocalDateTime> resultTimes = result.stream().map(BillingRecord::getSampleTime).collect(Collectors.toList());
        Assert.assertTrue(sampleTimes.containsAll(resultTimes));
    }

    private List<LocalDateTime> dates(List<Integer> dates) {
        return dates.stream().map(x -> LocalDateTime.of(2022, 11, x, 0, 0)).collect(Collectors.toList());
    }

    private BilledCostDailyRecord createBillingRecord(long id, LocalDateTime sampleTime, LocalDateTime updated) {
        return new BilledCostDailyRecord(id,
                sampleTime,
                22222L, (short)10, 1L, 2L, 3L, 4L, 5L,
                (short)1, (short)1, 6L, (short)56, (short)2047, 24.0, (short)0, (short)1, 2.0,
                TimeUtil.localTimeToMillis(updated, Clock.systemUTC()));
    }
}
