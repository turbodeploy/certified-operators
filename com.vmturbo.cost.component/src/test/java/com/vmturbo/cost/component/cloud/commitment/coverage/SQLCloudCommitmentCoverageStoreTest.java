package com.vmturbo.cost.component.cloud.commitment.coverage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsRequest.GroupByCondition;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore.AccountCoverageFilter;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore.AccountCoverageStatsFilter;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

@RunWith(MockitoJUnitRunner.class)
public class SQLCloudCommitmentCoverageStoreTest {

    private static final CloudCommitmentDataPoint dataPoint1A = CloudCommitmentDataPoint.newBuilder()
            .setAccountId(1)
            .setCloudServiceId(2)
            .setRegionId(3)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(1.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(2.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint1B = CloudCommitmentDataPoint.newBuilder()
            .setAccountId(1)
            .setCloudServiceId(2)
            .setRegionId(3)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(2.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(2.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint5A = CloudCommitmentDataPoint.newBuilder()
            .setAccountId(5)
            .setCloudServiceId(6)
            .setRegionId(7)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(3.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(4.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint5B = CloudCommitmentDataPoint.newBuilder()
            .setAccountId(5)
            .setCloudServiceId(6)
            .setRegionId(7)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setCoupons(2.0))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setCoupons(4.0))
            .build();

    private static final CloudCommitmentDataPoint dataPoint59A = CloudCommitmentDataPoint.newBuilder()
            .setAccountId(5)
            .setCloudServiceId(9)
            .setRegionId(7)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(123)
                            .setAmount(2.0)))
            .setCapacity(CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(123)
                            .setAmount(5.0)))
            .build();
    private static final CloudCommitmentDataPoint dataPoint59B = CloudCommitmentDataPoint.newBuilder()
            .setAccountId(5)
            .setCloudServiceId(9)
            .setRegionId(7)
            .setServiceProviderId(8)
            .setUsed(CloudCommitmentAmount.newBuilder()
                    .setAmount(CurrencyAmount.newBuilder()
                            .setCurrency(123)
                            .setAmount(3.0)))
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

    private SQLCloudCommitmentCoverageStore coverageStore;

    @Before
    public void setup() {
        coverageStore = new SQLCloudCommitmentCoverageStore(dsl, granularityCalculator);
    }

    /**
     * Test hourly account data persistence
     */
    @Test
    public void testHourlyAccountPersistence() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createCoverageDataBuckets(CloudStatGranularity.HOURLY);
        coverageStore.persistCoverageSamples(dataBuckets);

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualHourlyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.HOURLY)
                        .build());
        final List<CloudCommitmentDataBucket> actualDailyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.DAILY)
                        .build());
        final List<CloudCommitmentDataBucket> actualMonthlyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.MONTHLY)
                        .build());

        assertThat(dataBuckets, equalTo(actualHourlyBuckets));
        assertTrue(actualDailyBuckets.isEmpty());
        assertTrue(actualMonthlyBuckets.isEmpty());

    }

    /**
     * Test daily account data persistence
     */
    @Test
    public void testDailyAccountPersistence() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createCoverageDataBuckets(CloudStatGranularity.DAILY);
        coverageStore.persistCoverageSamples(dataBuckets);

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualHourlyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.HOURLY)
                        .build());
        final List<CloudCommitmentDataBucket> actualDailyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.DAILY)
                        .build());
        final List<CloudCommitmentDataBucket> actualMonthlyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.MONTHLY)
                        .build());

        assertTrue(actualHourlyBuckets.isEmpty());
        assertThat(dataBuckets, equalTo(actualDailyBuckets));
        assertTrue(actualMonthlyBuckets.isEmpty());

    }

    /**
     * Test monthly account data persistence
     */
    @Test
    public void testMonthlyAccountPersistence() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createCoverageDataBuckets(CloudStatGranularity.MONTHLY);
        coverageStore.persistCoverageSamples(dataBuckets);

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualHourlyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.HOURLY)
                        .build());
        final List<CloudCommitmentDataBucket> actualDailyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.DAILY)
                        .build());
        final List<CloudCommitmentDataBucket> actualMonthlyBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.MONTHLY)
                        .build());

        assertTrue(actualHourlyBuckets.isEmpty());
        assertTrue(actualDailyBuckets.isEmpty());
        assertThat(dataBuckets, equalTo(actualMonthlyBuckets));
    }

    /**
     * Test that a data point is not persisted if the account ID is not set.
     */
    @Test
    public void testMissingAccountPersistence() {

        // Persist the data bucket
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(Instant.EPOCH.plus(1, ChronoUnit.HOURS).toEpochMilli())
                .setGranularity(CloudStatGranularity.HOURLY)
                .addSample(dataPoint1A.toBuilder().clearAccountId().build())
                .build();
        coverageStore.persistCoverageSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.HOURLY)
                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    /**
     * Test that a data point is not persisted if the region ID is not set.
     */
    @Test
    public void testMissingRegionPersistence() {

        // Persist the data bucket
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(Instant.EPOCH.plus(1, ChronoUnit.HOURS).toEpochMilli())
                .setGranularity(CloudStatGranularity.HOURLY)
                .addSample(dataPoint1A.toBuilder().clearRegionId().build())
                .build();
        coverageStore.persistCoverageSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.HOURLY)
                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    /**
     * Test that a data point is not persisted if the cloud service ID is not set.
     */
    @Test
    public void testMissingCloudServicePersistence() {

        // Persist the data bucket
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(Instant.EPOCH.plus(1, ChronoUnit.HOURS).toEpochMilli())
                .setGranularity(CloudStatGranularity.HOURLY)
                .addSample(dataPoint1A.toBuilder().clearCloudServiceId().build())
                .build();
        coverageStore.persistCoverageSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.HOURLY)
                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    /**
     * Test that a data point is not persisted if the service provider ID is not set.
     */
    @Test
    public void testMissingServiceProviderPersistence() {

        // Persist the data bucket
        final CloudCommitmentDataBucket dataBucket = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(Instant.EPOCH.plus(1, ChronoUnit.HOURS).toEpochMilli())
                .setGranularity(CloudStatGranularity.HOURLY)
                .addSample(dataPoint1A.toBuilder().clearServiceProviderId().build())
                .build();
        coverageStore.persistCoverageSamples(ImmutableList.of(dataBucket));

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualBuckets = coverageStore.getCoverageBuckets(
                AccountCoverageFilter.builder()
                        .granularity(CloudStatGranularity.HOURLY)
                        .build());

        assertTrue(actualBuckets.isEmpty());
    }

    /**
     * Tests the stats query with a group by condition of the service provider.
     */
    @Test
    public void testStatsGroupByServiceProvider() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createCoverageDataBuckets(CloudStatGranularity.HOURLY);
        coverageStore.persistCoverageSamples(dataBuckets);

        // query the stats
        final AccountCoverageStatsFilter statsFilter = AccountCoverageStatsFilter.builder()
                .granularity(CloudStatGranularity.HOURLY)
                .addGroupByList(GroupByCondition.SERVICE_PROVIDER)
                .build();
        final List<CloudCommitmentStatRecord> actualStats = coverageStore.streamCoverageStats(statsFilter)
                .collect(ImmutableList.toImmutableList());

        // Setup expected stat records

        // should be datapoint 1A and 5A
        final CloudCommitmentStatRecord firstCouponStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(0).getTimestampMillis())
                .setServiceProviderId(8)
                .setSampleCount(2)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(3.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(1.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(3.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(6.0))
                        .build())
                .build();

        // Should be data point 59A
        final CloudCommitmentStatRecord firstSpendStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(0).getTimestampMillis())
                .setServiceProviderId(8)
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(2.0)))
                        .setMin(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(2.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(2.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(2.0)))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setMin(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .build())
                .build();

        // should be datapoint 1B and 5B
        final CloudCommitmentStatRecord secondCouponStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(1).getTimestampMillis())
                .setServiceProviderId(8)
                .setSampleCount(2)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(3.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(6.0))
                        .build())
                .build();

        // Should be data point 59A
        final CloudCommitmentStatRecord secondSpendStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(1).getTimestampMillis())
                .setServiceProviderId(8)
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(3.0)))
                        .setMin(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(3.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(3.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(3.0)))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setMin(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .build())
                .build();

        // ASSERTIONS
        // 2 data timestamps x 2 coverage types x 1 service provider
        assertThat(actualStats, hasSize(4));
        assertThat(actualStats, hasItems(firstCouponStat, firstSpendStat, secondCouponStat, secondSpendStat));

    }

    /**
     * Tests the stats query with a group by cloud service
     */
    @Test
    public void testStatsGroupByCloudService() {

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createCoverageDataBuckets(CloudStatGranularity.HOURLY);
        coverageStore.persistCoverageSamples(dataBuckets);

        // query the stats
        final AccountCoverageStatsFilter statsFilter = AccountCoverageStatsFilter.builder()
                .granularity(CloudStatGranularity.HOURLY)
                .addGroupByList(GroupByCondition.CLOUD_SERVICE)
                .build();
        final List<CloudCommitmentStatRecord> actualStats = coverageStore.streamCoverageStats(statsFilter)
                .collect(ImmutableList.toImmutableList());

        /*
        Setup expected
         */

        // should be datapoint 1A
        final CloudCommitmentStatRecord firstCouponStatTwo = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(0).getTimestampMillis())
                .setCloudServiceId(2)
                .setServiceProviderId(8)
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(1.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(1.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(1.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(1.0))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .build())
                .build();

        // should be datapoint 5A
        final CloudCommitmentStatRecord firstCouponStatSix = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(0).getTimestampMillis())
                .setCloudServiceId(6)
                .setServiceProviderId(8)
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(3.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(3.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(3.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(3.0))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .build())
                .build();

        // Should be data point 59A
        final CloudCommitmentStatRecord firstSpendStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(0).getTimestampMillis())
                .setCloudServiceId(9)
                .setServiceProviderId(8)
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(2.0)))
                        .setMin(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(2.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(2.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(2.0)))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setMin(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .build())
                .build();

        // should be datapoint 1B
        final CloudCommitmentStatRecord secondCouponStatTwo = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(1).getTimestampMillis())
                .setCloudServiceId(2)
                .setServiceProviderId(8)
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .build())
                .build();

        // should be datapoint 5B
        final CloudCommitmentStatRecord secondCouponStatSix = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(1).getTimestampMillis())
                .setCloudServiceId(6)
                .setServiceProviderId(8)
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(2.0))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .setMin(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .setAvg(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .setTotal(CloudCommitmentAmount.newBuilder().setCoupons(4.0))
                        .build())
                .build();

        // Should be data point 59B
        final CloudCommitmentStatRecord secondSpendStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(dataBuckets.get(1).getTimestampMillis())
                .setCloudServiceId(9)
                .setServiceProviderId(8)
                .setSampleCount(1)
                .setValues(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(3.0)))
                        .setMin(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(3.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(3.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(3.0)))
                        .build())
                .setCapacity(StatValue.newBuilder()
                        .setMax(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setMin(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setAvg(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .setTotal(CloudCommitmentAmount.newBuilder().setAmount(
                                CurrencyAmount.newBuilder().setCurrency(123).setAmount(5.0)))
                        .build())
                .build();

        // ASSERTIONS
        // 2 data timestamps x 2 coverage types x 1 service provider
        assertThat(actualStats, hasSize(6));
        assertThat(actualStats, containsInAnyOrder(firstCouponStatTwo, firstCouponStatSix, firstSpendStat,
                secondCouponStatTwo, secondCouponStatSix, secondSpendStat));

    }

    /**
     * Creates 2 data buckets, each with 3 cloud commitment samples. The buckets refer to the same commitments
     * with differing coverage.
     * @param granularity The cloud stat granularity for the buckets.
     * @return The list of data buckets
     */
    private List<CloudCommitmentDataBucket> createCoverageDataBuckets(@Nonnull CloudStatGranularity granularity) {

        final TemporalAmount bucketStep = bucketStepByGranularityMap.get(granularity);

        final Instant startTime = Instant.EPOCH.plus(bucketStep);
        final CloudCommitmentDataBucket dataBucketA = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(startTime.toEpochMilli())
                .setGranularity(granularity)
                .addAllSample(ImmutableList.of(dataPoint1A, dataPoint5A, dataPoint59A))
                .build();

        final CloudCommitmentDataBucket dataBucketB = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(startTime.plus(bucketStep).toEpochMilli())
                .setGranularity(granularity)
                .addAllSample(ImmutableList.of(dataPoint1B, dataPoint5B, dataPoint59B))
                .build();

        return ImmutableList.of(dataBucketA, dataBucketB);
    }
}
