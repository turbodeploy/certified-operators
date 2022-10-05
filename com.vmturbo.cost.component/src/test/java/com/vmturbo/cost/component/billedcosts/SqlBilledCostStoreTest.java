package com.vmturbo.cost.component.billedcosts;

import static com.vmturbo.cost.component.db.tables.BilledCostDaily.BILLED_COST_DAILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.common.protobuf.cost.Cost.UploadBilledCostRequest.BillingDataPoint;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.rollup.RolledUpTable;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * For testing SqlBilledCostStore methods.
 */
@RunWith(Parameterized.class)
public class SqlBilledCostStoreTest extends MultiDbTestBase {
    /**
     * Instance of store.
     */
    private final SqlBilledCostStore billedCostStore;

    /**
     * DSL for DB access.
     */
    private final DSLContext dsl;

    /**
     * Base data point builder with some initial values filled in.
     */
    final BillingDataPoint.Builder baseDataPointBuilder = BillingDataPoint.newBuilder()
            .setAccountOid(101L)
            .setCloudServiceOid(202L)
            .setRegionOid(303L)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setEntityOid(1001L)
            .setPriceModel(PriceModel.ON_DEMAND)
            .setCostCategory(CostCategory.COMPUTE)
            .setProviderOid(2002L)
            .setProviderType(56)
            .setServiceProviderId(777L);

    /**
     * Used for multi-DB execution.
     *
     * @return DB types to exec test against.
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    /**
     * Creates a new instance of this test class.
     *
     * @param configurableDbDialect Db dialect flag.
     * @param dialect Type of dialect (Maria/PG).
     * @throws SQLException Thrown on DB issues.
     * @throws UnsupportedDialectException Thrown on bad dialect.
     * @throws InterruptedException Thrown on thread interruption.
     */
    public SqlBilledCostStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
        this.billedCostStore = new SqlBilledCostStore(dsl,
                new BatchInserter(10, 2,
                        new RollupTimesStore(dsl, RolledUpTable.BILLED_COST)),
                mock(TimeFrameCalculator.class));
    }

    /**
     * Verifies last_updated is set correctly when inserting a data point that doesn't have
     * tracking flag enabled.
     *
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    @Test
    public void oneUntrackedPoint() throws InterruptedException, ExecutionException {
        updateOnePoint(false);
    }

    /**
     * Verifies last_updated for insert of one record with tracking changes enabled.
     *
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    @Test
    public void oneTrackedPoint() throws InterruptedException, ExecutionException {
        updateOnePoint(true);
    }

    /**
     * Test 'CASE 1': When last_updated is 0, then set it to null.
     *
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    @Test
    public void duplicatePointsLastUpdatedNull() throws InterruptedException, ExecutionException {
        final TestDataValue dataValue1 = new TestDataValue(10.0, 0.120, 0L);
        final TestDataValue dataValue2 = new TestDataValue(24.0, 1.254, 0L);

        updateTwoPoints(dataValue1, dataValue2, null);
    }

    /**
     * Test 'CASE 2': When usage_amount is different, then use new value (30000L) for last_updated.
     *
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    @Test
    public void duplicatePointsLastUpdatedNewDifferentUsageAmounts()
            throws InterruptedException, ExecutionException {
        final TestDataValue dataValue1 = new TestDataValue(10.0, 1.254, 20000L);
        final TestDataValue dataValue2 = new TestDataValue(24.0, 1.254, 30000L);

        updateTwoPoints(dataValue1, dataValue2, 30000L);
    }

    /**
     * Test 'CASE 3': When cost is different, then use new value (30000L) for last_updated.
     *
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    @Test
    public void duplicatePointsLastUpdatedNewDifferentCosts()
            throws InterruptedException, ExecutionException {
        final TestDataValue dataValue1 = new TestDataValue(24.0, 0.512, 20000L);
        final TestDataValue dataValue2 = new TestDataValue(24.0, 1.254, 30000L);

        updateTwoPoints(dataValue1, dataValue2, 30000L);
    }

    /**
     * Test 'CASE 4': When cost and usage_amount values are same as before, use old value (20000L)
     * of last_updated.
     *
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    @Test
    public void duplicatePointsLastUpdatedOldSameUsageCost()
            throws InterruptedException, ExecutionException {
        final TestDataValue dataValue1 = new TestDataValue(24.0, 1.254, 20000L);
        final TestDataValue dataValue2 = new TestDataValue(24.0, 1.254, 30000L);

        updateTwoPoints(dataValue1, dataValue2, 20000L);
    }

    /**
     * Util method to add 2 data points to DB, with given input values. Used to test onDuplicateKeyUpdate
     * clause, to verify that last_updated field gets set as expected.
     * This is trying to test the 4 possible case options for last_updated in a statement like this:
     *
     * <p>insert into billed_cost_daily (sample_time, ...)
     * values (timestamp '2022-03-15 14:31:10.203', ...)
     * on duplicate key update
     *   last_updated = case
     *     when values(last_updated) = 0 then null                                <-- CASE 1
     *     when usage_amount <> values(usage_amount) then values(last_updated)    <-- CASE 2
     *     when cost <> values(cost) then values(last_updated)                    <-- CASE 3
     *     else last_updated                                                      <-- CASE 4
     *   end,
     *   currency = values(currency),
     *   usage_amount = values(usage_amount),
     *   unit = values(unit),
     *   cost = values(cost)
     *</p>
     *
     * @param dataValue1 Data point 1 to insert.
     * @param dataValue2 Data point 2 to insert.
     * @param expectedLastUpdated Expected value of last_updated after both records are inserted.
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    private void updateTwoPoints(TestDataValue dataValue1, TestDataValue dataValue2,
            @Nullable final Long expectedLastUpdated) throws InterruptedException, ExecutionException {
        long utcTime = System.currentTimeMillis();

        // Insert first data point and verify that last updated is set correctly always.
        final BillingDataPoint dataPoint1 = getDataPoint(true, utcTime,
                dataValue1.usageAmount, dataValue1.cost);
        int totalRows = insertDataPoints(dataValue1.lastUpdated, ImmutableList.of(dataPoint1));
        assertEquals(1, totalRows);
        Long actualLastUpdated = getLastUpdated();
        assertNotNull(actualLastUpdated);
        assertEquals(dataValue1.lastUpdated, (long)actualLastUpdated);

        // 2nd data point will hit the duplicate key update, so verify the last updated value.
        final BillingDataPoint dataPoint2 = getDataPoint(true, utcTime,
                dataValue2.usageAmount, dataValue2.cost);
        totalRows = insertDataPoints(dataValue2.lastUpdated, ImmutableList.of(dataPoint2));
        assertEquals(1, totalRows);
        actualLastUpdated = getLastUpdated();
        assertEquals(expectedLastUpdated, actualLastUpdated);
    }

    /**
     * Adds data points to DB.
     *
     * @param lastUpdatedTime Last update time to use.
     * @param dataPoints List of data points to add.
     * @return Rows inserted.
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    private int insertDataPoints(long lastUpdatedTime, final List<BillingDataPoint> dataPoints)
            throws InterruptedException, ExecutionException {
        List<Future<Integer>> futures = billedCostStore.insertBillingDataPoints(dataPoints,
                Collections.emptyMap(), Granularity.DAILY, lastUpdatedTime);
        assertNotNull(futures);
        int totalRows = 0;
        for (Future<Integer> f : futures) {
            totalRows += f.get();
        }
        return totalRows;
    }

    /**
     * Util to make up a data point instance with given values.
     *
     * @param trackChanges Whether to track changes for this record (i.e. update last_updated field)
     * @param utcTime Current time in millis.
     * @param usageAmount Usage amount hours.
     * @param cost Cost value.
     * @return BillingDataPoint with values filled in.
     */
    private BillingDataPoint getDataPoint(boolean trackChanges, long utcTime, double usageAmount,
            double cost) {
        return BillingDataPoint.newBuilder(baseDataPointBuilder.build())
                .setTimestampUtcMillis(utcTime)
                .setUsageAmount(usageAmount)
                .setTrackChanges(trackChanges)
                .setCost(CurrencyAmount.newBuilder()
                        .setAmount(cost)
                        .setCurrency(840)
                        .build())
                .build();
    }

    /**
     * Queries the DB table and gets the current value of last_updated column.
     *
     * @return Last updated value, can be null.
     */
    @Nullable
    private Long getLastUpdated() {
        return (Long)dsl
                .select(BILLED_COST_DAILY.LAST_UPDATED)
                .from(Tables.BILLED_COST_DAILY)
                .stream()
                .findFirst()
                .map(rec -> rec.get(0))
                .orElse(null);
    }

    /**
     * Util to update 1 data point into the DB table.
     *
     * @param trackChanges Whether to enable tracking changes - last_updated is set based on that.
     * @throws InterruptedException Thrown on thread interruption.
     * @throws ExecutionException Thrown on Db error.
     */
    private void updateOnePoint(boolean trackChanges) throws InterruptedException, ExecutionException {
        long insertedLastUpdatedTime = 10000L;

        final BillingDataPoint dataPoint = getDataPoint(trackChanges, System.currentTimeMillis(),
                5.0, 1.25);
        int totalRows = insertDataPoints(insertedLastUpdatedTime, ImmutableList.of(dataPoint));
        assertEquals(1, totalRows);
        Long lastUpdated = getLastUpdated();
        assertNotNull(lastUpdated);
        // If tracking changes, then the last updated is set correctly to 10000L, otherwise not set (0).
        assertEquals(trackChanges ? lastUpdated : 0L, (long)lastUpdated);
    }

    /**
     * Test class to keep some data values together.
     */
    private static class TestDataValue {
        /**
         * Usage amount value to test with. E.g. in hours.
         */
        double usageAmount;

        /**
         * Billed cost for the usage hours.
         */
        double cost;

        /**
         * Value to use for the last_updated during insertion of the record.
         */
        long lastUpdated;

        /**
         * Creates a new instance.
         *
         * @param ua Usage amount.
         * @param c Cost.
         * @param lu Last_updated value to use.
         */
        TestDataValue(double ua, double c, long lu) {
            usageAmount = ua;
            cost = c;
            lastUpdated = lu;
        }
    }
}
