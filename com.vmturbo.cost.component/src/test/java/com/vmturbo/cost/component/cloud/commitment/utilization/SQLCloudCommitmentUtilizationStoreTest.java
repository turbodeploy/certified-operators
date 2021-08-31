package com.vmturbo.cost.component.cloud.commitment.utilization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.cloud.common.stat.CloudGranularityCalculator;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket.CloudCommitmentDataPoint;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord.StatValue;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore.CloudCommitmentUtilizationFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore.CloudCommitmentUtilizationStatsFilter;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

@RunWith(MockitoJUnitRunner.class)
public class SQLCloudCommitmentUtilizationStoreTest {

    private static final CloudCommitmentDataPoint dataPoint1A = CloudCommitmentDataPoint.newBuilder()
            .setCommitmentId(1)
            .setAccountId(2)
            .setRegionId(3)
            .setServiceProviderId(4)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(1.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(2.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint1B = CloudCommitmentDataPoint.newBuilder()
            .setCommitmentId(1)
            .setAccountId(2)
            .setRegionId(3)
            .setServiceProviderId(4)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(2.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(2.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint5A = CloudCommitmentDataPoint.newBuilder()
            .setCommitmentId(5)
            .setAccountId(6)
            .setRegionId(7)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(3.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(4.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint5B = CloudCommitmentDataPoint.newBuilder()
            .setCommitmentId(5)
            .setAccountId(6)
            .setRegionId(7)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(4.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(4.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint9A = CloudCommitmentDataPoint.newBuilder()
            .setCommitmentId(9)
            .setAccountId(2)
            .setRegionId(3)
            .setServiceProviderId(4)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(3.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(5.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint9B = CloudCommitmentDataPoint.newBuilder()
            .setCommitmentId(9)
            .setAccountId(2)
            .setRegionId(3)
            .setServiceProviderId(4)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(5.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(5.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint10A = CloudCommitmentDataPoint.newBuilder()
            .setCommitmentId(10)
            .setAccountId(6)
            .setRegionId(7)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(123)
                            .setAmount(2.3)))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(123)
                            .setAmount(5.0)))
            .build();

    private static final CloudCommitmentDataPoint dataPoint10B = CloudCommitmentDataPoint.newBuilder()
            .setCommitmentId(10)
            .setAccountId(6)
            .setRegionId(7)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(123)
                            .setAmount(5.0)))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(123)
                            .setAmount(5.0)))
            .build();

    private final Map<CloudStatGranularity, TemporalAmount> bucketStepByGranularityMap =
                    ImmutableMap.<CloudStatGranularity, TemporalAmount>builder()
                                    .put(CloudStatGranularity.HOURLY, Duration.ofHours(1))
                                    .put(CloudStatGranularity.DAILY, Duration.ofDays(1))
                                    .put(CloudStatGranularity.MONTHLY, Duration.ofDays(30))
                                    .build();


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

    @Mock
    private CloudGranularityCalculator granularityCalculator;

    private SQLCloudCommitmentUtilizationStore utilizationStore;

    @Before
    public void setup() {
        utilizationStore = new SQLCloudCommitmentUtilizationStore(dsl, granularityCalculator);
    }

    /**
     * Tests hourly utilization persistence and query.
     */
    @Test
    public void testHourlyGranularityPersistence() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createUtilizationDataBuckets(CloudStatGranularity.HOURLY);
        utilizationStore.persistUtilizationSamples(dataBuckets);

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualHourlyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.HOURLY)
                                        .build());
        final List<CloudCommitmentDataBucket> actualDailyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());
        final List<CloudCommitmentDataBucket> actualMonthlyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.MONTHLY)
                                        .build());

        assertThat(actualHourlyDataBuckets, equalTo(dataBuckets));
        assertTrue(actualDailyDataBuckets.isEmpty());
        assertTrue(actualMonthlyDataBuckets.isEmpty());
    }

    /**
     * Tests daily utilization persistence and query.
     */
    @Test
    public void testDailyGranularityPersistence() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createUtilizationDataBuckets(CloudStatGranularity.DAILY);
        utilizationStore.persistUtilizationSamples(dataBuckets);

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualHourlyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.HOURLY)
                                        .build());
        final List<CloudCommitmentDataBucket> actualDailyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());
        final List<CloudCommitmentDataBucket> actualMonthlyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.MONTHLY)
                                        .build());

        assertTrue(actualHourlyDataBuckets.isEmpty());
        assertThat(actualDailyDataBuckets, equalTo(dataBuckets));
        assertTrue(actualMonthlyDataBuckets.isEmpty());
    }

    /**
     * Tests monthly utilization persistence and query.
     */
    @Test
    public void testMonthlyGranularityPersistence() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createUtilizationDataBuckets(CloudStatGranularity.MONTHLY);
        utilizationStore.persistUtilizationSamples(dataBuckets);

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualHourlyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.HOURLY)
                                        .build());
        final List<CloudCommitmentDataBucket> actualDailyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());
        final List<CloudCommitmentDataBucket> actualMonthlyDataBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.MONTHLY)
                                        .build());

        assertTrue(actualHourlyDataBuckets.isEmpty());
        assertTrue(actualDailyDataBuckets.isEmpty());
        assertThat(actualMonthlyDataBuckets, equalTo(dataBuckets));
    }

    /**
     * Test that a data point is not persisted if the cloud commitment ID is not set.
     */
    @Test
    public void testMissingCommitmentIdPersistence() {

        // Persist the data bucket
        TemporalAmount bucketStep = bucketStepByGranularityMap.get(CloudStatGranularity.DAILY);
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                        .setTimestampMillis(Instant.EPOCH.plus(bucketStep).toEpochMilli())
                        .setGranularity(CloudStatGranularity.DAILY)
                        .addSample(dataPoint1A.toBuilder().clearCommitmentId().build())
                        .build();
        utilizationStore.persistUtilizationSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());

        assertTrue(actualBuckets.isEmpty());
    }


    /**
     * Test that a data point is not persisted if the account ID is not set.
     */
    @Test
    public void testMissingAccountPersistence() {

        // Persist the data bucket
        TemporalAmount bucketStep = bucketStepByGranularityMap.get(CloudStatGranularity.DAILY);
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                        .setTimestampMillis(Instant.EPOCH.plus(bucketStep).toEpochMilli())
                        .setGranularity(CloudStatGranularity.DAILY)
                        .addSample(dataPoint1A.toBuilder().clearAccountId().build())
                        .build();
        utilizationStore.persistUtilizationSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    /**
     * Test that a data point is not persisted if the Service Provider is not set.
     */
    @Test
    public void testMissingServiceProviderPersistence() {

        // Persist the data bucket
        TemporalAmount bucketStep = bucketStepByGranularityMap.get(CloudStatGranularity.DAILY);
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                        .setTimestampMillis(Instant.EPOCH.plus(bucketStep).toEpochMilli())
                        .setGranularity(CloudStatGranularity.DAILY)
                        .addSample(dataPoint1A.toBuilder().clearServiceProviderId().build())
                        .build();
        utilizationStore.persistUtilizationSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    /**
     * Test that a data point is not persisted if the usage is not set.
     */
    @Test
    public void testMissingUsagePersistence() {

        // Persist the data bucket
        TemporalAmount bucketStep = bucketStepByGranularityMap.get(CloudStatGranularity.DAILY);
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                        .setTimestampMillis(Instant.EPOCH.plus(bucketStep).toEpochMilli())
                        .setGranularity(CloudStatGranularity.DAILY)
                        .addSample(dataPoint1A.toBuilder().clearUsed().build())
                        .build();
        utilizationStore.persistUtilizationSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    /**
     * Test that a data point is not persisted if the capacity is not set.
     */
    @Test
    public void testMissingCapacityPersistence() {

        // Persist the data bucket
        TemporalAmount bucketStep = bucketStepByGranularityMap.get(CloudStatGranularity.DAILY);
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                        .setTimestampMillis(Instant.EPOCH.plus(bucketStep).toEpochMilli())
                        .setGranularity(CloudStatGranularity.DAILY)
                        .addSample(dataPoint1A.toBuilder().clearCapacity().build())
                        .build();
        utilizationStore.persistUtilizationSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    /**
     * Test that a data point is not persisted if the usage and capacity value case don't match.
     */
    @Test
    public void testMismatchedValueCasePersistence() {

        // Persist the data bucket
        TemporalAmount bucketStep = bucketStepByGranularityMap.get(CloudStatGranularity.DAILY);
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(Instant.EPOCH.plus(bucketStep).toEpochMilli())
                .setGranularity(CloudStatGranularity.DAILY)
                .addSample(dataPoint1A.toBuilder()
                        .setUsed(CloudCommitmentAmount.newBuilder()
                            .setAmount(CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setCapacity(CloudCommitmentAmount.newBuilder()
                            .setCoupons(5.0)))
                .build();
        utilizationStore.persistUtilizationSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = utilizationStore.getUtilizationBuckets(
                        CloudCommitmentUtilizationFilter.builder()
                                        .granularity(CloudStatGranularity.DAILY)
                                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    @Test
    public void testBaseDailyGranularityStats() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createUtilizationDataBuckets(CloudStatGranularity.DAILY);
        utilizationStore.persistUtilizationSamples(dataBuckets);

        // query the stats records
        final CloudCommitmentUtilizationStatsFilter statsFilter = CloudCommitmentUtilizationStatsFilter.builder()
                .granularity(CloudStatGranularity.DAILY)
                .build();
        final List<CloudCommitmentStatRecord> actualStatRecords = utilizationStore.streamUtilizationStats(statsFilter)
                .collect(ImmutableList.toImmutableList());

        TemporalAmount bucketStep = bucketStepByGranularityMap.get(CloudStatGranularity.DAILY);
        Instant startTime = Instant.EPOCH.plus(bucketStep);

        // setup stat records
        final CloudCommitmentStatRecord firstDayCurrencyStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(startTime.toEpochMilli())
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMin(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(2.3)))
                        .setMax(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(2.3)))
                        .setTotal(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(2.3)))
                        .setAvg(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(2.3))))
                .setCapacity(StatValue.newBuilder()
                        .setMin(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setMax(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0))))
                .build();
        final CloudCommitmentStatRecord firstDayCouponStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(startTime.toEpochMilli())
                .setSampleCount(3)
                .setValues(StatValue.newBuilder()
                        .setMin(CloudCommitmentAmount.newBuilder()
                                .setCoupons(1.0))
                        .setMax(CloudCommitmentAmount.newBuilder()
                                .setCoupons(3.0))
                        .setTotal(CloudCommitmentAmount.newBuilder()
                                .setCoupons(7.0))
                        .setAvg(CloudCommitmentAmount.newBuilder()
                                .setCoupons(7.0 / 3.0)))
                .setCapacity(StatValue.newBuilder()
                        .setMin(CloudCommitmentAmount.newBuilder()
                                .setCoupons(2.0))
                        .setMax(CloudCommitmentAmount.newBuilder()
                                .setCoupons(5.0))
                        .setTotal(CloudCommitmentAmount.newBuilder()
                                .setCoupons(11.0))
                        .setAvg(CloudCommitmentAmount.newBuilder()
                                .setCoupons(11.0 / 3.0)))
                .build();
        final CloudCommitmentStatRecord secondDayCurrencyStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(startTime.plus(bucketStep).toEpochMilli())
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMin(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setMax(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0))))
                .setCapacity(StatValue.newBuilder()
                        .setMin(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setMax(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder()
                                .setAmount(CurrencyAmount.newBuilder()
                                        .setCurrency(123)
                                        .setAmount(5.0))))
                .build();
        final CloudCommitmentStatRecord secondDayCouponStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(startTime.plus(bucketStep).toEpochMilli())
                .setSampleCount(3)
                .setValues(StatValue.newBuilder()
                        .setMin(CloudCommitmentAmount.newBuilder()
                                .setCoupons(2.0))
                        .setMax(CloudCommitmentAmount.newBuilder()
                                .setCoupons(5.0))
                        .setTotal(CloudCommitmentAmount.newBuilder()
                                .setCoupons(11.0))
                        .setAvg(CloudCommitmentAmount.newBuilder()
                                .setCoupons(11.0 / 3.0)))
                .setCapacity(StatValue.newBuilder()
                        .setMin(CloudCommitmentAmount.newBuilder()
                                .setCoupons(2.0))
                        .setMax(CloudCommitmentAmount.newBuilder()
                                .setCoupons(5.0))
                        .setTotal(CloudCommitmentAmount.newBuilder()
                                .setCoupons(11.0))
                        .setAvg(CloudCommitmentAmount.newBuilder()
                                .setCoupons(11.0 / 3.0)))
                .build();


        // ASSERTIONS
        assertThat(actualStatRecords, hasSize(4));
        assertThat(actualStatRecords,
                hasItems(firstDayCouponStat, firstDayCurrencyStat,
                        secondDayCouponStat, secondDayCurrencyStat));
    }



    /**
     * Creates 2 data buckets, each with 4 cloud commitment samples. The buckets refer to the same commitments
     * with differing utilization.
     * @param granularity The cloud stat granularity for the buckets.
     * @return The list of data buckets
     */
    private List<CloudCommitmentDataBucket> createUtilizationDataBuckets(
                    @Nonnull CloudStatGranularity granularity) {

        final TemporalAmount bucketStep = bucketStepByGranularityMap.get(granularity);

        final Instant startTime = Instant.EPOCH.plus(bucketStep);
        final CloudCommitmentDataBucket dataBucketA = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(startTime.toEpochMilli())
                .setGranularity(granularity)
                .addAllSample(ImmutableList.of(dataPoint1A, dataPoint5A, dataPoint9A, dataPoint10A))
                .build();

        final CloudCommitmentDataBucket dataBucketB = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(startTime.plus(bucketStep).toEpochMilli())
                .setGranularity(granularity)
                .addAllSample(ImmutableList.of(dataPoint1B, dataPoint5B, dataPoint9B, dataPoint10B))
                .build();

        return ImmutableList.of(dataBucketA, dataBucketB);
    }
}
