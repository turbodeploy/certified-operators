package com.vmturbo.cost.component.billed.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.cloud.common.persistence.DataQueueFactory;
import com.vmturbo.cloud.common.persistence.DataQueueFactory.DefaultDataQueueFactory;
import com.vmturbo.cloud.common.scope.CachedAggregateScopeIdentityProvider;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore;
import com.vmturbo.cloud.common.scope.CloudScopeIdentityStore.PersistenceRetryPolicy;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostBucket;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostFilter;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostItem;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostQuery;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.BilledCostSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.CostTagsSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostRequest.MetadataSegment;
import com.vmturbo.common.protobuf.cost.BilledCostServices.UploadBilledCloudCostResponse;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.billedcosts.TagGroupStore;
import com.vmturbo.cost.component.billedcosts.TagIdentityService;
import com.vmturbo.cost.component.billedcosts.TagStore;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.scope.SqlCloudScopeIdentityStore;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PriceModel;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingDataPoint.CostCategory;
import com.vmturbo.platform.sdk.common.CostBilling.CostTagGroup;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.sql.utils.partition.IPartitioningManager;

/**
 * Test class for {@link BilledCostRpcService}.
 */
@RunWith(Parameterized.class)
public class BilledCostRpcServiceTest extends MultiDbTestBase {

    private final DSLContext dsl;

    @Parameters
    public static Object[][] parameters() {
        // as of OM-89158, the partitioned billed cost tables do not support Postgres
        return DBENDPOINT_CONVERTED_PARAMS;
    }

    private static final Supplier<Long> pseudoIdentityProvider = new AtomicLong()::getAndIncrement;

    private final BilledCostPersistenceConfig billedCostPersistenceConfig =
            BilledCostPersistenceConfig.builder()
                    .persistenceTimeout(Duration.ofSeconds(30))
                    .concurrency(1)
                    .build();

    private TagGroupIdentityService tagGroupIdentityService;

    private SqlCloudCostStore sqlCloudCostStore;

    private BilledCostRpcService billedCostRpcService;

    private static final BilledCostItem VIRTUAL_MACHINE_USAGE_1 = BilledCostItem.newBuilder()
            .setCost(CurrencyAmount.newBuilder().setAmount(12).setCurrency(840).build())
            .setAccountId(pseudoIdentityProvider.get())
            .setEntityId(pseudoIdentityProvider.get())
            .setUsageAmount(3)
            .setCloudServiceId(pseudoIdentityProvider.get())
            .setEntityType(60)
            .setPriceModel(PriceModel.ON_DEMAND)
            .setCommodityType(2)
            .setCostCategory(CostCategory.COMPUTE)
            .setProviderId(pseudoIdentityProvider.get())
            .setProviderType(10)
            .setRegionId(pseudoIdentityProvider.get())
            .setCloudServiceId(pseudoIdentityProvider.get())
            .setCostTagGroupId(0L)
            .build();

    private static final BilledCostItem VIRTUAL_MACHINE_USAGE_2 =
            VIRTUAL_MACHINE_USAGE_1.toBuilder()
                    .setEntityId(pseudoIdentityProvider.get())
                    .setCostTagGroupId(1L)
                    .build();

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @throws SQLException if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    public BilledCostRpcServiceTest(final boolean configurableDbDialect, final SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Runs before each test.
     *
     * @throws Exception on error
     */
    @Before
    public void setup() throws Exception {
        // set up the cloud scope identity provider
        final CloudScopeIdentityStore scopeIdentityStore =
                new SqlCloudScopeIdentityStore(dsl, PersistenceRetryPolicy.DEFAULT_POLICY, false,
                        10);
        final IdentityProvider identityProvider = new DefaultIdentityProvider(0);

        final CachedAggregateScopeIdentityProvider cloudScopeIdentityProvider =
                new CachedAggregateScopeIdentityProvider(scopeIdentityStore, identityProvider,
                        Duration.ofSeconds(10));
        cloudScopeIdentityProvider.initialize();

        // set up the tag store and identity service
        final TagStore tagStore = new TagStore(dsl);
        final TagIdentityService tagIdentityService =
                new TagIdentityService(tagStore, identityProvider, 10);
        final TagGroupStore tagGroupStore = new TagGroupStore(dsl);
        tagGroupIdentityService =
                new TagGroupIdentityService(tagGroupStore, tagIdentityService, identityProvider,
                        10);

        final DataQueueFactory dataQueueFactory = new DefaultDataQueueFactory();

        // set up the time frame calculator
        final TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);
        when(timeFrameCalculator.millis2TimeFrame(anyLong())).thenReturn(TimeFrame.HOUR);

        final IPartitioningManager partitioningManager = mock(IPartitioningManager.class);

        sqlCloudCostStore = new SqlCloudCostStore(partitioningManager, tagGroupIdentityService,
                cloudScopeIdentityProvider, dataQueueFactory, timeFrameCalculator,
                super.getDslContext(), billedCostPersistenceConfig);

        billedCostRpcService = new BilledCostRpcService(sqlCloudCostStore);
    }

    /**
     * Upload cost tag groups, and force them to be sent in multiple chunks. Verify that they arrive
     * and are reassembled correctly.
     *
     * @throws Exception on error
     */
    @Test
    public void testUploadCostTagGroupsInMultipleChunks() throws Exception {

        final TestUploadResponseHandler responseHandler = new TestUploadResponseHandler();
        final StreamObserver<UploadBilledCloudCostRequest> requestSender =
                billedCostRpcService.uploadBilledCloudCost(responseHandler);

        final long billingFamilyId = pseudoIdentityProvider.get();
        final long serviceProviderId = pseudoIdentityProvider.get();

        // Send metadata
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCostMetadata(MetadataSegment.newBuilder()
                        .setBillingFamilyId(billingFamilyId)
                        .setServiceProviderId(serviceProviderId)
                        .build())
                .build());

        // Send cost tag groups
        final Map<Long, CostTagGroup> costTagGroups = ImmutableMap.<Long, CostTagGroup>builder()
                .put(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(), CostTagGroup.newBuilder()
                        .putTags("owner", "turbo")
                        .putTags(" created ", "-1d")
                        .build())
                .put(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                        CostTagGroup.newBuilder().putTags("lifetime", "30").build())
                .build();

        // Send 1 cost tag group per request, so 2 requests
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setCostTags(CostTagsSegment.newBuilder()
                        .putCostTagGroup(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                                costTagGroups.get(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId()))
                        .build())
                .build());

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setCostTags(CostTagsSegment.newBuilder()
                        .putCostTagGroup(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                                costTagGroups.get(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId()))
                        .build())
                .build());

        // Send billed cost buckets
        final List<BilledCostBucket> billedCostBuckets = ImmutableList.<BilledCostBucket>builder()
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665678645575L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2)
                        .build())
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665655200000L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2)
                        .build())
                .build();

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCost(
                        BilledCostSegment.newBuilder().addAllCostBuckets(billedCostBuckets).build())
                .build());

        // Complete the upload
        requestSender.onCompleted();

        responseHandler.awaitCompletion();

        // assertions

        assertTrue(responseHandler.isCompleted());
        assertThat(responseHandler.errors, empty());

        final List<BilledCostData> queryAllResults = sqlCloudCostStore.getCostData(
                BilledCostQuery.newBuilder()
                        .setFilter(BilledCostFilter.getDefaultInstance())
                        .setGranularity(Granularity.HOURLY)
                        .build());

        assertEquals(1, queryAllResults.size());
        final BilledCostData queryResult = queryAllResults.get(0);

        assertEquals(billingFamilyId, queryResult.getBillingFamilyId());
        assertEquals(serviceProviderId, queryResult.getServiceProviderId());

        assertEquals(2, queryResult.getCostBucketsCount());

        final List<BilledCostItem> bucket1 =
                queryResult.getCostBucketsList().get(0).getCostItemsList();
        final List<BilledCostItem> bucket2 =
                queryResult.getCostBucketsList().get(1).getCostItemsList();

        final Map<Long, Long> assignedTagGroupIds =
                tagGroupIdentityService.resolveIdForDiscoveredTagGroups(costTagGroups);

        final Function<BilledCostItem, BilledCostItem> useAssignedCostTagGroupId =
                item -> item.toBuilder()
                        .setCostTagGroupId(assignedTagGroupIds.get(item.getCostTagGroupId()))
                        .build();

        assertThat(bucket1,
                containsInAnyOrder(useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_1),
                        useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_2)));

        assertThat(bucket2,
                containsInAnyOrder(useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_1),
                        useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_2)));

        assertEquals(1, responseHandler.responses.size());
        assertEquals(0, responseHandler.errors.size());
    }

    /**
     * Upload cost buckets, and force them to be sent in multiple chunks. Verify that they arrive
     * and are reassembled correctly.
     *
     * @throws Exception on error
     */
    @Test
    public void testUploadBucketsInMultipleChunks() throws Exception {

        final TestUploadResponseHandler responseHandler = new TestUploadResponseHandler();
        final StreamObserver<UploadBilledCloudCostRequest> requestSender =
                billedCostRpcService.uploadBilledCloudCost(responseHandler);

        final long billingFamilyId = pseudoIdentityProvider.get();
        final long serviceProviderId = pseudoIdentityProvider.get();

        // Send metadata
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCostMetadata(MetadataSegment.newBuilder()
                        .setBillingFamilyId(billingFamilyId)
                        .setServiceProviderId(serviceProviderId)
                        .build())
                .build());

        // Send cost tag groups
        final Map<Long, CostTagGroup> costTagGroups = ImmutableMap.<Long, CostTagGroup>builder()
                .put(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(), CostTagGroup.newBuilder()
                        .putTags("owner", "turbo")
                        .putTags(" created ", "-1d")
                        .build())
                .put(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                        CostTagGroup.newBuilder().putTags("lifetime", "30").build())
                .build();

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setCostTags(CostTagsSegment.newBuilder().putAllCostTagGroup(costTagGroups).build())
                .build());

        // Send billed cost buckets
        final List<BilledCostBucket> billedCostBuckets = ImmutableList.<BilledCostBucket>builder()
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665678645575L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2)
                        .build())
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665655200000L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2)
                        .build())
                .build();

        // Send 1 bucket per request, so 2 separate requests
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCost(BilledCostSegment.newBuilder()
                        .addAllCostBuckets(billedCostBuckets.subList(0, 1))
                        .build())
                .build());

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCost(BilledCostSegment.newBuilder()
                        .addAllCostBuckets(billedCostBuckets.subList(1, 2))
                        .build())
                .build());

        // Complete the upload
        requestSender.onCompleted();

        responseHandler.awaitCompletion();

        // assertions

        assertTrue(responseHandler.isCompleted());
        assertThat(responseHandler.errors, empty());

        final List<BilledCostData> queryAllResults =
                sqlCloudCostStore.getCostData(BilledCostQuery.newBuilder().build());

        assertEquals(1, queryAllResults.size());
        final BilledCostData queryResult = queryAllResults.get(0);

        assertEquals(billingFamilyId, queryResult.getBillingFamilyId());
        assertEquals(serviceProviderId, queryResult.getServiceProviderId());

        assertEquals(2, queryResult.getCostBucketsCount());

        final List<BilledCostItem> bucket1 =
                queryResult.getCostBucketsList().get(0).getCostItemsList();
        final List<BilledCostItem> bucket2 =
                queryResult.getCostBucketsList().get(1).getCostItemsList();

        final Map<Long, Long> assignedTagGroupIds =
                tagGroupIdentityService.resolveIdForDiscoveredTagGroups(costTagGroups);

        final Function<BilledCostItem, BilledCostItem> useAssignedCostTagGroupId =
                item -> item.toBuilder()
                        .setCostTagGroupId(assignedTagGroupIds.get(item.getCostTagGroupId()))
                        .build();

        assertThat(bucket1,
                containsInAnyOrder(useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_1),
                        useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_2)));

        assertThat(bucket2,
                containsInAnyOrder(useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_1),
                        useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_2)));

        assertEquals(1, responseHandler.responses.size());
        assertEquals(0, responseHandler.errors.size());
    }


    /**
     * Upload some billed cost data, but try to send the billed cost buckets before the metadata.
     *
     * @throws Exception on error
     */
    @Test
    public void testUploadBucketsBeforeMetadata() throws Exception {

        final TestUploadResponseHandler responseHandler = new TestUploadResponseHandler();
        final StreamObserver<UploadBilledCloudCostRequest> requestSender =
                billedCostRpcService.uploadBilledCloudCost(responseHandler);

        final long billingFamilyId = pseudoIdentityProvider.get();
        final long serviceProviderId = pseudoIdentityProvider.get();

        // Send billed cost buckets
        final List<BilledCostBucket> billedCostBuckets = ImmutableList.<BilledCostBucket>builder()
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665678645575L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId())
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId())
                        .build())
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665655200000L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId())
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId())
                        .build())
                .build();

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCost(
                        BilledCostSegment.newBuilder().addAllCostBuckets(billedCostBuckets).build())
                .build());

        // Send metadata
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCostMetadata(MetadataSegment.newBuilder()
                        .setBillingFamilyId(billingFamilyId)
                        .setServiceProviderId(serviceProviderId)
                        .build())
                .build());

        // Try to complete the upload
        requestSender.onCompleted();

        responseHandler.awaitCompletion();

        // assertions

        assertThat(responseHandler.errors, hasSize(1));
        assertThat(responseHandler.errors.get(0), instanceOf(StatusException.class));
    }

    /**
     * Upload some billed cost data, but don't include a cost tag groups segment.
     *
     * @throws Exception on error
     */
    @Test
    public void testUploadNoCostTagsSegment() throws Exception {

        final TestUploadResponseHandler responseHandler = new TestUploadResponseHandler();
        final StreamObserver<UploadBilledCloudCostRequest> requestSender =
                billedCostRpcService.uploadBilledCloudCost(responseHandler);

        final long billingFamilyId = pseudoIdentityProvider.get();
        final long serviceProviderId = pseudoIdentityProvider.get();

        // Send metadata
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCostMetadata(MetadataSegment.newBuilder()
                        .setBillingFamilyId(billingFamilyId)
                        .setServiceProviderId(serviceProviderId)
                        .build())
                .build());

        // Don't send a cost tags segment

        // Try to send billed cost buckets that don't reference any tags
        final List<BilledCostBucket> billedCostBuckets = ImmutableList.<BilledCostBucket>builder()
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665678645575L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId())
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId())
                        .build())
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665655200000L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId())
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId())
                        .build())
                .build();

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCost(
                        BilledCostSegment.newBuilder().addAllCostBuckets(billedCostBuckets).build())
                .build());

        // Try to complete the upload
        requestSender.onCompleted();

        responseHandler.awaitCompletion();

        // assertions

        assertTrue(responseHandler.isCompleted());
        assertThat("the upload request was done incorrectly and should be rejected", responseHandler.errors,
                hasSize(1));
        assertThat(responseHandler.errors.get(0), instanceOf(StatusException.class));

        final List<BilledCostData> queryAllResults = sqlCloudCostStore.getCostData(
                BilledCostQuery.newBuilder()
                        .setFilter(BilledCostFilter.getDefaultInstance())
                        .setGranularity(Granularity.HOURLY)
                        .build());

        assertEquals("no data made it into the database", 0, queryAllResults.size());
    }

    /**
     * Upload some billed cost data, but don't include any cost tag groups.
     *
     * @throws Exception on error
     */
    @Test
    public void testUploadEmptyCostTagGroups() throws Exception {

        final TestUploadResponseHandler responseHandler = new TestUploadResponseHandler();
        final StreamObserver<UploadBilledCloudCostRequest> requestSender =
                billedCostRpcService.uploadBilledCloudCost(responseHandler);

        final long billingFamilyId = pseudoIdentityProvider.get();
        final long serviceProviderId = pseudoIdentityProvider.get();

        // Send metadata
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCostMetadata(MetadataSegment.newBuilder()
                        .setBillingFamilyId(billingFamilyId)
                        .setServiceProviderId(serviceProviderId)
                        .build())
                .build());

        // Send NO cost tag groups
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setCostTags(CostTagsSegment.getDefaultInstance())
                .build());

        // Send billed cost buckets
        final List<BilledCostBucket> billedCostBuckets = ImmutableList.<BilledCostBucket>builder()
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665678645575L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId())
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId())
                        .build())
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665655200000L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId())
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId())
                        .build())
                .build();

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCost(
                        BilledCostSegment.newBuilder().addAllCostBuckets(billedCostBuckets).build())
                .build());

        // Complete the upload
        requestSender.onCompleted();

        responseHandler.awaitCompletion();

        // assertions

        assertTrue(responseHandler.isCompleted());
        assertThat(responseHandler.errors, empty());

        final List<BilledCostData> queryAllResults = sqlCloudCostStore.getCostData(
                BilledCostQuery.newBuilder()
                        .setFilter(BilledCostFilter.getDefaultInstance())
                        .setGranularity(Granularity.HOURLY)
                        .build());

        assertEquals(1, queryAllResults.size());
        final BilledCostData queryResult = queryAllResults.get(0);

        assertEquals(billingFamilyId, queryResult.getBillingFamilyId());
        assertEquals(serviceProviderId, queryResult.getServiceProviderId());

        assertEquals(2, queryResult.getCostBucketsCount());

        final List<BilledCostItem> bucket1 =
                queryResult.getCostBucketsList().get(0).getCostItemsList();
        final List<BilledCostItem> bucket2 =
                queryResult.getCostBucketsList().get(1).getCostItemsList();

        assertThat(bucket1, containsInAnyOrder(
                VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId().build(),
                VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId().build()));

        assertThat(bucket2, containsInAnyOrder(
                VIRTUAL_MACHINE_USAGE_1.toBuilder().clearCostTagGroupId().build(),
                VIRTUAL_MACHINE_USAGE_2.toBuilder().clearCostTagGroupId().build()));

        assertEquals(1, responseHandler.responses.size());
        assertEquals(0, responseHandler.errors.size());
    }

    /**
     * Upload some billed cost data, but don't include any cost buckets.
     *
     * @throws Exception on error
     */
    @Test
    public void testUploadNoBuckets() throws Exception {

        final TestUploadResponseHandler responseHandler = new TestUploadResponseHandler();
        final StreamObserver<UploadBilledCloudCostRequest> requestSender =
                billedCostRpcService.uploadBilledCloudCost(responseHandler);

        final long billingFamilyId = pseudoIdentityProvider.get();
        final long serviceProviderId = pseudoIdentityProvider.get();

        // Send metadata
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCostMetadata(MetadataSegment.newBuilder()
                        .setBillingFamilyId(billingFamilyId)
                        .setServiceProviderId(serviceProviderId)
                        .build())
                .build());

        // Send cost tag groups
        final Map<Long, CostTagGroup> costTagGroups = ImmutableMap.<Long, CostTagGroup>builder()
                .put(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(), CostTagGroup.newBuilder()
                        .putTags("owner", "turbo")
                        .putTags(" created ", "-1d")
                        .build())
                .put(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                        CostTagGroup.newBuilder().putTags("lifetime", "30").build())
                .build();

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setCostTags(CostTagsSegment.newBuilder().putAllCostTagGroup(costTagGroups).build())
                .build());

        // Send NO billed cost buckets

        // Complete the upload
        requestSender.onCompleted();

        responseHandler.awaitCompletion();

        // assertions

        assertTrue(responseHandler.isCompleted());
        assertThat(responseHandler.errors, empty());

        final List<BilledCostData> queryAllResults = sqlCloudCostStore.getCostData(
                BilledCostQuery.newBuilder()
                        .setFilter(BilledCostFilter.getDefaultInstance())
                        .setGranularity(Granularity.HOURLY)
                        .build());

        assertEquals(0, queryAllResults.size());

        assertEquals(1, responseHandler.responses.size());
        assertEquals(0, responseHandler.errors.size());
    }

    @Test
    public void testMultipleConcurrentUploads() throws Exception {

        final List<BilledCostData> billedCostData = ImmutableList.<BilledCostData>builder()
                // Billed cost data #1
                .add(BilledCostData.newBuilder()
                        .setBillingFamilyId(pseudoIdentityProvider.get())
                        .setServiceProviderId(pseudoIdentityProvider.get())
                        .addCostBuckets(BilledCostBucket.newBuilder()
                                .setSampleTsUtc(1665678645575L)
                                .addCostItems(VIRTUAL_MACHINE_USAGE_1)
                                .build())
                        .putCostTagGroup(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(),
                                CostTagGroup.newBuilder()
                                        .putTags("owner", "turbo")
                                        .putTags(" created ", "-1d")
                                        .build())
                        .build())
                // Billed cost data #2
                .add(BilledCostData.newBuilder()
                        .setBillingFamilyId(pseudoIdentityProvider.get())
                        .setServiceProviderId(pseudoIdentityProvider.get())
                        .addCostBuckets(BilledCostBucket.newBuilder()
                                .setSampleTsUtc(1665655200000L)
                                .addCostItems(VIRTUAL_MACHINE_USAGE_2)
                                .build())
                        .putCostTagGroup(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                                CostTagGroup.newBuilder().putTags("lifetime", "30").build())
                        .build()).build();

        final List<TestUploadResponseHandler> responseHandlers = billedCostData.stream()
                .map(data -> new TestUploadResponseHandler())
                .collect(Collectors.toList());

        final List<StreamObserver<UploadBilledCloudCostRequest>> requestSenders =
                responseHandlers.stream()
                        .map(responseHandler -> billedCostRpcService.uploadBilledCloudCost(
                                responseHandler))
                        .collect(Collectors.toList());

        // Send metadata
        for (int i = 0; i < billedCostData.size(); i++) {
            requestSenders.get(i)
                    .onNext(UploadBilledCloudCostRequest.newBuilder()
                            .setBilledCostMetadata(MetadataSegment.newBuilder()
                                    .setBillingFamilyId(billedCostData.get(i).getBillingFamilyId())
                                    .setServiceProviderId(
                                            billedCostData.get(i).getServiceProviderId())
                                    .build())
                            .build());
        }

        // Send cost tag groups
        for (int i = 0; i < billedCostData.size(); i++) {
            requestSenders.get(i)
                    .onNext(UploadBilledCloudCostRequest.newBuilder()
                            .setCostTags(CostTagsSegment.newBuilder()
                                    .putAllCostTagGroup(billedCostData.get(i).getCostTagGroupMap())
                                    .build())
                            .build());
        }

        // Send billed cost buckets
        for (int i = 0; i < billedCostData.size(); i++) {
            requestSenders.get(i)
                    .onNext(UploadBilledCloudCostRequest.newBuilder()
                            .setBilledCost(BilledCostSegment.newBuilder()
                                    .addAllCostBuckets(billedCostData.get(i).getCostBucketsList())
                                    .build())
                            .build());
        }

        for (int i = 0; i < billedCostData.size(); i++) {
            // Complete the upload
            requestSenders.get(i).onCompleted();
            responseHandlers.get(i).awaitCompletion();
            assertTrue(responseHandlers.get(i).isCompleted());
            assertThat(responseHandlers.get(i).errors, empty());
        }

        // assertions

        final List<BilledCostData> persistedData = sqlCloudCostStore.getCostData(
                BilledCostQuery.newBuilder().setResolveTagGroups(true).build());

        assertEquals(2, persistedData.size());

        final List<BilledCostItem> allBilledCostItemsWithTagIdsRemoved = billedCostData.stream()
                .map(BilledCostData::getCostBucketsList)
                .flatMap(Collection::stream)
                .map(BilledCostBucket::getCostItemsList)
                .flatMap(Collection::stream)
                .map(i -> i.toBuilder().clearCostTagGroupId().build())
                .collect(Collectors.toList());

        final List<BilledCostItem> allPersistedItemsWithTagIdsRemoved = persistedData.stream()
                .map(BilledCostData::getCostBucketsList)
                .flatMap(Collection::stream)
                .map(BilledCostBucket::getCostItemsList)
                .flatMap(Collection::stream)
                .map(i -> i.toBuilder().clearCostTagGroupId().build())
                .collect(Collectors.toList());

        assertTrue(allPersistedItemsWithTagIdsRemoved.containsAll(
                allBilledCostItemsWithTagIdsRemoved));
    }

    /**
     * Simulate a basic set of upload requests, with all fields populated, but no chunking.
     *
     * @throws Exception errors
     */
    @Test
    public void testUploadBilledCloudCostWithNoChunking() throws Exception {

        final TestUploadResponseHandler responseHandler = new TestUploadResponseHandler();
        final StreamObserver<UploadBilledCloudCostRequest> requestSender =
                billedCostRpcService.uploadBilledCloudCost(responseHandler);

        final long billingFamilyId = pseudoIdentityProvider.get();
        final long serviceProviderId = pseudoIdentityProvider.get();

        // Send metadata
        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCostMetadata(MetadataSegment.newBuilder()
                        .setBillingFamilyId(billingFamilyId)
                        .setServiceProviderId(serviceProviderId)
                        .build())
                .build());

        // Send cost tag groups
        final Map<Long, CostTagGroup> costTagGroups = ImmutableMap.<Long, CostTagGroup>builder()
                .put(VIRTUAL_MACHINE_USAGE_1.getCostTagGroupId(), CostTagGroup.newBuilder()
                        .putTags("owner", "turbo")
                        .putTags(" created ", "-1d")
                        .build())
                .put(VIRTUAL_MACHINE_USAGE_2.getCostTagGroupId(),
                        CostTagGroup.newBuilder().putTags("lifetime", "30").build())
                .build();

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setCostTags(CostTagsSegment.newBuilder().putAllCostTagGroup(costTagGroups).build())
                .build());

        // Send billed cost buckets
        final List<BilledCostBucket> billedCostBuckets = ImmutableList.<BilledCostBucket>builder()
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665678645575L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2)
                        .build())
                .add(BilledCostBucket.newBuilder()
                        .setSampleTsUtc(1665655200000L)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_1)
                        .addCostItems(VIRTUAL_MACHINE_USAGE_2)
                        .build())
                .build();

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder()
                .setBilledCost(
                        BilledCostSegment.newBuilder().addAllCostBuckets(billedCostBuckets).build())
                .build());

        // Try to complete the errored-out upload, make sure it doesn't break anything
        requestSender.onCompleted();

        responseHandler.awaitCompletion();

        // assertions

        assertTrue(responseHandler.isCompleted());
        assertThat(responseHandler.errors, empty());

        final List<BilledCostData> queryAllResults = sqlCloudCostStore.getCostData(
                BilledCostQuery.newBuilder()
                        .setFilter(BilledCostFilter.getDefaultInstance())
                        .setGranularity(Granularity.HOURLY)
                        .build());

        assertEquals(1, queryAllResults.size());
        final BilledCostData queryResult = queryAllResults.get(0);

        assertEquals(billingFamilyId, queryResult.getBillingFamilyId());
        assertEquals(serviceProviderId, queryResult.getServiceProviderId());

        assertEquals(2, queryResult.getCostBucketsCount());

        final List<BilledCostItem> bucket1 =
                queryResult.getCostBucketsList().get(0).getCostItemsList();
        final List<BilledCostItem> bucket2 =
                queryResult.getCostBucketsList().get(1).getCostItemsList();

        final Map<Long, Long> assignedTagGroupIds =
                tagGroupIdentityService.resolveIdForDiscoveredTagGroups(costTagGroups);

        final Function<BilledCostItem, BilledCostItem> useAssignedCostTagGroupId =
                item -> item.toBuilder()
                        .setCostTagGroupId(assignedTagGroupIds.get(item.getCostTagGroupId()))
                        .build();

        assertThat(bucket1,
                containsInAnyOrder(useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_1),
                        useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_2)));

        assertThat(bucket2,
                containsInAnyOrder(useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_1),
                        useAssignedCostTagGroupId.apply(VIRTUAL_MACHINE_USAGE_2)));

        assertEquals(1, responseHandler.responses.size());
        assertEquals(0, responseHandler.errors.size());
    }

    /**
     * Send an empty request, and make sure the service impl doesn't block forever waiting.
     *
     * @throws Exception on error
     */
    @Test
    public void testThatResponseGetsCompletedWithEmptyRequest() throws Exception {
        final TestUploadResponseHandler responseHandler = new TestUploadResponseHandler();
        final StreamObserver<UploadBilledCloudCostRequest> requestSender =
                billedCostRpcService.uploadBilledCloudCost(responseHandler);

        requestSender.onNext(UploadBilledCloudCostRequest.newBuilder().build());
        requestSender.onCompleted();
        responseHandler.awaitCompletion(10);

        assertTrue(responseHandler.isCompleted());
        assertThat(responseHandler.errors, empty());

        assertEquals(1, responseHandler.responses.size());
        assertEquals(0, responseHandler.errors.size());
    }

    private static class TestUploadResponseHandler
            implements StreamObserver<UploadBilledCloudCostResponse> {

        private final List<UploadBilledCloudCostResponse> responses = new ArrayList<>();

        private final List<Throwable> errors = new ArrayList<>();

        private final CountDownLatch completionLatch = new CountDownLatch(1);

        @Override
        public void onNext(final UploadBilledCloudCostResponse response) {
            responses.add(response);
        }

        @Override
        public void onError(final Throwable error) {
            errors.add(error);
            completionLatch.countDown();
        }

        @Override
        public void onCompleted() {
            completionLatch.countDown();
        }

        public void awaitCompletion() throws InterruptedException {
            awaitCompletion(60);
        }

        private void awaitCompletion(final int seconds) throws InterruptedException {
            if (!completionLatch.await(seconds, TimeUnit.SECONDS)) {
                throw new AssertionError("Latch failed to close in time");
            }
        }

        public boolean isCompleted() {
            return completionLatch.getCount() == 0;
        }
    }
}