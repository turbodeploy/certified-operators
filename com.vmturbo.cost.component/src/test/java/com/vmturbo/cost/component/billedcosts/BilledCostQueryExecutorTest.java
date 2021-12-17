package com.vmturbo.cost.component.billedcosts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CostStatsSnapshot.StatRecord.TagKeyValuePair;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.GroupByType;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.TagFilter;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.BilledCostDaily;
import com.vmturbo.cost.component.db.tables.CostTag;
import com.vmturbo.cost.component.db.tables.CostTagGrouping;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit tests for {@link BilledCostQueryExecutor}.
 */
public class BilledCostQueryExecutorTest {

    private static final long VM1_ID = 1L;
    private static final long VM2_ID = 2L;
    private static final long VM3_ID = 3L;
    private static final long REGION_ID = 3L;
    private static final long ACCOUNT1_ID = 4L;
    private static final long ACCOUNT2_ID = 5L;

    private static final String TAG_OWNER = "owner";
    private static final String TAG_OWNER_ALICE = "alice";
    private static final String TAG_OWNER_BOB = "bob";
    private static final String TAG_LOCATION = "location";
    private static final String TAG_LOCATION_CHICAGO = "chicago";

    private static final double DELTA = 0.0001;

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static final DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);
    private static final DSLContext dsl = dbConfig.getDslContext();
    private static final LocalDateTime day2 = LocalDateTime.now();
    private static final LocalDateTime day1 = day2.minusDays(1);

    /**
     * Rule to automatically cleanup DB data.
     */
    @Rule
    public final DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private BilledCostQueryExecutor executor;

    /**
     * Set up the following VMs with billed costs:
     * VM 1:
     *   ID: 1
     *   Region: 3
     *   Account: 4
     *   Tags: owner = alice, location = chicago
     * VM 2:
     *   ID: 2
     *   Region: 3
     *   Account: 5
     *   Tags: owner = bob, location = chicago
     * VM 3:
     *   ID: 3
     *   Region: 3
     *   Account: 4
     *   Tags: no tags
     */
    @Before
    public void setup() {
        // Set up tags
        final CostTag costTag = Tables.COST_TAG;
        dsl.insertInto(costTag)
                .columns(costTag.TAG_ID, costTag.TAG_KEY, costTag.TAG_VALUE)
                .values(1L, TAG_OWNER, TAG_OWNER_ALICE)
                .execute();
        dsl.insertInto(costTag)
                .columns(costTag.TAG_ID, costTag.TAG_KEY, costTag.TAG_VALUE)
                .values(2L, TAG_OWNER, TAG_OWNER_BOB)
                .execute();
        dsl.insertInto(costTag)
                .columns(costTag.TAG_ID, costTag.TAG_KEY, costTag.TAG_VALUE)
                .values(3L, TAG_LOCATION, TAG_LOCATION_CHICAGO)
                .execute();
        final CostTagGrouping costTagGrouping = Tables.COST_TAG_GROUPING;
        dsl.insertInto(costTagGrouping)
                .columns(costTagGrouping.TAG_GROUP_ID, costTagGrouping.TAG_ID)
                .values(1L, 1L)
                .execute();
        dsl.insertInto(costTagGrouping)
                .columns(costTagGrouping.TAG_GROUP_ID, costTagGrouping.TAG_ID)
                .values(1L, 3L)
                .execute();
        dsl.insertInto(costTagGrouping)
                .columns(costTagGrouping.TAG_GROUP_ID, costTagGrouping.TAG_ID)
                .values(2L, 2L)
                .execute();
        dsl.insertInto(costTagGrouping)
                .columns(costTagGrouping.TAG_GROUP_ID, costTagGrouping.TAG_ID)
                .values(2L, 3L)
                .execute();

        // Set up billed costs
        insertBilledCost(day1, VM1_ID, ACCOUNT1_ID, 1L, 1D);
        insertBilledCost(day2, VM1_ID, ACCOUNT1_ID, 1L, 2D);
        insertBilledCost(day1, VM2_ID, ACCOUNT2_ID, 2L, 4D);
        insertBilledCost(day2, VM2_ID, ACCOUNT2_ID, 2L, 8D);
        insertBilledCost(day1, VM3_ID, ACCOUNT1_ID, 0L, 16D);
        insertBilledCost(day2, VM3_ID, ACCOUNT1_ID, 0L, 32D);

        final TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);
        when(timeFrameCalculator.millis2TimeFrame(anyLong())).thenReturn(TimeFrame.DAY);
        executor = new BilledCostQueryExecutor(dsl, timeFrameCalculator);
    }

    /**
     * Test getting billed costs grouped by tag for a VM without filtering.
     */
    @Test
    public void testGetBilledCostsGroupedByTagForVm() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM1_ID)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(2, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, TAG_OWNER, TAG_OWNER_ALICE, 1);
        verifyRecord(snapshot1, TAG_LOCATION, TAG_LOCATION_CHICAGO, 1);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(2, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, TAG_OWNER, TAG_OWNER_ALICE, 2);
        verifyRecord(snapshot2, TAG_LOCATION, TAG_LOCATION_CHICAGO, 2);
    }

    /**
     * Test getting billed costs (no grouping) for a VM without filtering.
     */
    @Test
    public void testGetBilledCostsTagForVm() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM1_ID)
                        .build())
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 1);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, null, null, 2);
    }

    /**
     * Test getting billed costs grouped by tag for a VM with tag filter.
     */
    @Test
    public void testGetBilledCostsGroupedByTagForVmWithTagFilter() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM2_ID)
                        .build())
                .addTagFilter(TagFilter.newBuilder()
                        .setTagKey(TAG_OWNER)
                        .addTagValue(TAG_OWNER_BOB)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, TAG_OWNER, TAG_OWNER_BOB, 4);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, TAG_OWNER, TAG_OWNER_BOB, 8);
    }

    /**
     * Test getting billed costs (no grouping) for a VM with tag filter.
     */
    @Test
    public void testGetBilledCostsForVmWithTagFilter() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM2_ID)
                        .build())
                .addTagFilter(TagFilter.newBuilder()
                        .setTagKey(TAG_OWNER)
                        .addTagValue(TAG_OWNER_BOB)
                        .build())
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 4);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, null, null, 8);
    }

    /**
     * Test getting billed costs grouped by tag for a VM with end date filter.
     */
    @Test
    public void testGetBilledCostsGroupedByTagForVmWithDateFilter() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM1_ID)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .setEndDate(toMillis(day2.minusHours(12)))
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(1, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(2, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, TAG_OWNER, TAG_OWNER_ALICE, 1);
        verifyRecord(snapshot1, TAG_LOCATION, TAG_LOCATION_CHICAGO, 1);
    }

    /**
     * Test getting billed costs (no grouping) for a VM with end date filter.
     */
    @Test
    public void testGetBilledCostsForVmWithDateFilter() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM1_ID)
                        .build())
                .setEndDate(toMillis(day2.minusHours(12)))
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(1, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 1);
    }

    /**
     * Test getting billed costs for a VM with non-existing tag.
     */
    @Test
    public void testGetBilledCostsForVmWithNotExistingTag() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM2_ID)
                        .build())
                .addTagFilter(TagFilter.newBuilder()
                        .setTagKey("NonExisting")
                        .addTagValue("NonExisting")
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertTrue(result.isEmpty());
    }

    /**
     * Test getting billed costs grouped by tag for an untagged VM.
     */
    @Test
    public void testGetBilledCostsGroupedByTagForUntaggedVm() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM3_ID)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 16);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, null, null, 32);
    }

    /**
     * Test getting billed costs (no grouping) for an untagged VM.
     */
    @Test
    public void testGetBilledCostsForUntaggedVm() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM3_ID)
                        .build())
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 16);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, null, null, 32);
    }

    /**
     * Test getting billed costs for a VM with end date filter specifying the date before the first
     * point.
     */
    @Test
    public void testGetBilledCostsForVmWithNonExistingEndDate() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setEntityFilter(EntityFilter.newBuilder()
                        .addEntityId(VM1_ID)
                        .build())
                .setEndDate(toMillis(day2.minusDays(2)))
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertTrue(result.isEmpty());
    }

    /**
     * Test getting billed costs grouped by tag for a region without filtering.
     */
    @Test
    public void testGetBilledCostsGroupedByTagForRegion() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setRegionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_ID)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(4, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, TAG_OWNER, TAG_OWNER_ALICE, 1);
        verifyRecord(snapshot1, TAG_OWNER, TAG_OWNER_BOB, 4);
        verifyRecord(snapshot1, TAG_LOCATION, TAG_LOCATION_CHICAGO, 5, 2.5, 1, 4);
        verifyRecord(snapshot1, null, null, 16);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(4, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, TAG_OWNER, TAG_OWNER_ALICE, 2);
        verifyRecord(snapshot2, TAG_OWNER, TAG_OWNER_BOB, 8);
        verifyRecord(snapshot2, TAG_LOCATION, TAG_LOCATION_CHICAGO, 10, 5, 2, 8);
        verifyRecord(snapshot2, null, null, 32);
    }

    /**
     * Test getting billed costs (no grouping) for a region without filtering.
     */
    @Test
    public void testGetBilledCostsForRegion() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setRegionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_ID)
                        .build())
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 21, 7, 1, 16);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, null, null, 42, 14, 2, 32);
    }

    /**
     * Test getting billed costs grouped by tag for a region with tag filter.
     */
    @Test
    public void testGetBilledCostsGroupedByTagForRegionWithTagFilter() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setRegionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_ID)
                        .build())
                .addTagFilter(TagFilter.newBuilder()
                        .setTagKey(TAG_LOCATION)
                        .addTagValue(TAG_LOCATION_CHICAGO)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, TAG_LOCATION, TAG_LOCATION_CHICAGO, 5, 2.5, 1, 4);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, TAG_LOCATION, TAG_LOCATION_CHICAGO, 10, 5, 2, 8);
    }

    /**
     * Test getting billed costs (no grouping) for a region with tag filter.
     */
    @Test
    public void testGetBilledCostsForRegionWithTagFilter() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setRegionFilter(RegionFilter.newBuilder()
                        .addRegionId(REGION_ID)
                        .build())
                .addTagFilter(TagFilter.newBuilder()
                        .setTagKey(TAG_LOCATION)
                        .addTagValue(TAG_LOCATION_CHICAGO)
                        .build())
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 5, 2.5, 1, 4);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, null, null, 10, 5, 2, 8);
    }

    /**
     * Test getting billed costs grouped by tag for an account without filtering.
     */
    @Test
    public void testGetBilledCostsGroupedByTagForAccount() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setAccountFilter(AccountFilter.newBuilder()
                        .addAccountId(ACCOUNT1_ID)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(3, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, TAG_OWNER, TAG_OWNER_ALICE, 1);
        verifyRecord(snapshot1, TAG_LOCATION, TAG_LOCATION_CHICAGO, 1);
        verifyRecord(snapshot1, null, null, 16);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(3, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, TAG_OWNER, TAG_OWNER_ALICE, 2);
        verifyRecord(snapshot2, TAG_LOCATION, TAG_LOCATION_CHICAGO, 2);
        verifyRecord(snapshot2, null, null, 32);
    }

    /**
     * Test getting billed costs (no grouping) for an account without filtering.
     */
    @Test
    public void testGetBilledCostsForAccount() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setAccountFilter(AccountFilter.newBuilder()
                        .addAccountId(ACCOUNT1_ID)
                        .build())
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 17, 8.5, 1, 16);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, null, null, 34, 17, 2, 32);
    }

    /**
     * Test getting billed costs grouped by tag for an account with tag filter.
     */
    @Test
    public void testGetBilledCostsGroupedByTagForAccountWithTagFilter() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setAccountFilter(AccountFilter.newBuilder()
                        .addAccountId(ACCOUNT2_ID)
                        .build())
                .addTagFilter(TagFilter.newBuilder()
                        .setTagKey(TAG_OWNER)
                        .addTagValue(TAG_OWNER_BOB)
                        .build())
                .addGroupBy(GroupByType.TAG)
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, TAG_OWNER, TAG_OWNER_BOB, 4);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, TAG_OWNER, TAG_OWNER_BOB, 8);
    }

    /**
     * Test getting billed costs (no grouping) for an account with tag filter.
     */
    @Test
    public void testGetBilledCostsForAccountWithTagFilter() {
        // ARRANGE
        final GetCloudBilledStatsRequest request = GetCloudBilledStatsRequest.newBuilder()
                .setAccountFilter(AccountFilter.newBuilder()
                        .addAccountId(ACCOUNT2_ID)
                        .build())
                .addTagFilter(TagFilter.newBuilder()
                        .setTagKey(TAG_OWNER)
                        .addTagValue(TAG_OWNER_BOB)
                        .build())
                .build();

        // ACT
        final List<CostStatsSnapshot> result = executor.getBilledCostStats(request);

        // ASSERT
        assertEquals(2, result.size());
        final CostStatsSnapshot snapshot1 = result.get(0);
        assertEquals(1, snapshot1.getStatRecordsCount());
        verifyRecord(snapshot1, null, null, 4);
        final CostStatsSnapshot snapshot2 = result.get(1);
        assertEquals(1, snapshot2.getStatRecordsCount());
        verifyRecord(snapshot2, null, null, 8);
    }

    private static void insertBilledCost(
            @Nonnull final LocalDateTime sampleTime,
            final long entityId,
            final long accountId,
            final long tagGroupId,
            final double cost) {
        final BilledCostDaily billedCost = Tables.BILLED_COST_DAILY;
        dsl.insertInto(billedCost)
                .columns(billedCost.SAMPLE_TIME,
                        billedCost.ENTITY_ID,
                        billedCost.REGION_ID,
                        billedCost.ACCOUNT_ID,
                        billedCost.TAG_GROUP_ID,
                        billedCost.COST,
                        billedCost.SERVICE_PROVIDER_ID,
                        billedCost.PRICE_MODEL,
                        billedCost.COST_CATEGORY,
                        billedCost.USAGE_AMOUNT,
                        billedCost.UNIT,
                        billedCost.CURRENCY)
                .values(sampleTime,
                        entityId,
                        REGION_ID,
                        accountId,
                        tagGroupId,
                        cost,
                        0L,
                        (short)0,
                        (short)0,
                        0D,
                        (short)0,
                        (short)0)
                .execute();
    }

    private static void verifyRecord(
            @Nonnull final CostStatsSnapshot snapshot,
            @Nullable final String tagKey,
            @Nullable final String tagValue,
            final double total,
            final double avg,
            final double min,
            final double max) {
        final StatRecord record = findByTag(snapshot.getStatRecordsList(), tagKey, tagValue);
        assertEquals(StringConstants.BILLED_COST, record.getName());
        assertEquals(total, record.getValue().getTotal(), DELTA);
        assertEquals(avg, record.getValue().getAvg(), DELTA);
        assertEquals(min, record.getValue().getMin(), DELTA);
        assertEquals(max, record.getValue().getMax(), DELTA);
    }

    private static void verifyRecord(
            @Nonnull final CostStatsSnapshot snapshot,
            @Nullable final String tagKey,
            @Nullable final String tagValue,
            final double value) {
        verifyRecord(snapshot, tagKey, tagValue, value, value, value, value);
    }

    @Nonnull
    private static StatRecord findByTag(
            @Nonnull final List<StatRecord> records,
            @Nullable final String tagKey,
            @Nullable final String tagValue) {
        final Optional<StatRecord> result = records.stream()
                .filter(r -> isMatching(r.getTagList(), tagKey, tagValue))
                .findAny();
        assertTrue(result.isPresent());
        return result.get();
    }

    private static boolean isMatching(
            @Nonnull final List<TagKeyValuePair> tags,
            @Nullable final String tagKey,
            @Nullable final String tagValue) {
        if (tagKey == null) {
            return tags.isEmpty();
        }
        return tags.stream().anyMatch(tag ->
                Objects.equal(tagKey, tag.getKey()) && Objects.equal(tagValue, tag.getValue()));
    }

    private static long toMillis(@Nonnull final LocalDateTime localDateTime) {
        return localDateTime.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli();
    }
}
