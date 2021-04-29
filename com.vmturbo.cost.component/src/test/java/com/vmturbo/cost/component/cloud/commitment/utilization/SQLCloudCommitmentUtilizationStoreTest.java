package com.vmturbo.cost.component.cloud.commitment.utilization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

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

    /**
     * Tests simple persistence and querying of all daily granularity data.
     */
    @Test
    public void testDailyGranularityPersistence() {

        // create the store
        final SQLCloudCommitmentUtilizationStore utilizationStore = new SQLCloudCommitmentUtilizationStore(dsl);

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createDailyDataBuckets();
        utilizationStore.persistUtilizationSamples(dataBuckets);

        // query the persisted data
        final List<CloudCommitmentDataBucket> actualDataBuckets = utilizationStore.getUtilizationBuckets(
                CloudCommitmentUtilizationFilter.builder()
                        .granularity(CloudStatGranularity.DAILY)
                        .build());

        assertThat(actualDataBuckets, equalTo(dataBuckets));
    }

    @Test
    public void testBaseDailyGranularityStats() {

        // create the store
        final SQLCloudCommitmentUtilizationStore utilizationStore = new SQLCloudCommitmentUtilizationStore(dsl);

        // persist data buckets
        final List<CloudCommitmentDataBucket> dataBuckets = createDailyDataBuckets();
        utilizationStore.persistUtilizationSamples(dataBuckets);

        // query the stats records
        final CloudCommitmentUtilizationStatsFilter statsFilter = CloudCommitmentUtilizationStatsFilter.builder()
                .granularity(CloudStatGranularity.DAILY)
                .build();
        final List<CloudCommitmentStatRecord> actualStatRecords = utilizationStore.streamUtilizationStats(statsFilter)
                .collect(ImmutableList.toImmutableList());

        // setup stat records
        final CloudCommitmentStatRecord firstDayCurrencyStat = CloudCommitmentStatRecord.newBuilder()
                .setSnapshotDate(Instant.EPOCH.toEpochMilli())
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
                .setSnapshotDate(Instant.EPOCH.toEpochMilli())
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
                .setSnapshotDate(Instant.EPOCH.plus(1, ChronoUnit.DAYS).toEpochMilli())
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
                .setSnapshotDate(Instant.EPOCH.plus(1, ChronoUnit.DAYS).toEpochMilli())
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
     * @return The list of data buckets
     */
    private List<CloudCommitmentDataBucket> createDailyDataBuckets() {

        final CloudCommitmentDataBucket dataBucketA = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(0)
                .setGranularity(CloudStatGranularity.DAILY)
                .addAllSample(ImmutableList.of(dataPoint1A, dataPoint5A, dataPoint9A, dataPoint10A))
                .build();

        final CloudCommitmentDataBucket dataBucketB = CloudCommitmentDataBucket.newBuilder()
                .setTimestampMillis(Instant.EPOCH.plus(1, ChronoUnit.DAYS).toEpochMilli())
                .setGranularity(CloudStatGranularity.DAILY)
                .addAllSample(ImmutableList.of(dataPoint1B, dataPoint5B, dataPoint9B, dataPoint10B))
                .build();

        return ImmutableList.of(dataBucketA, dataBucketB);
    }
}
