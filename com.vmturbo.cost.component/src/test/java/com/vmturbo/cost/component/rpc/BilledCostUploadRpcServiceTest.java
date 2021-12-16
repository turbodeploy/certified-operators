package com.vmturbo.cost.component.rpc;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import io.grpc.stub.StreamObserver;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.cost.component.billedcosts.BatchInserter;
import com.vmturbo.cost.component.billedcosts.BilledCostStore;
import com.vmturbo.cost.component.billedcosts.BilledCostUploadRpcService;
import com.vmturbo.cost.component.billedcosts.SqlBilledCostStore;
import com.vmturbo.cost.component.billedcosts.TagGroupIdentityService;
import com.vmturbo.cost.component.billedcosts.TagGroupStore;
import com.vmturbo.cost.component.billedcosts.TagIdentityService;
import com.vmturbo.cost.component.billedcosts.TagStore;
import com.vmturbo.cost.component.db.tables.BilledCostDaily;
import com.vmturbo.cost.component.db.tables.CostTag;
import com.vmturbo.cost.component.db.tables.CostTagGrouping;
import com.vmturbo.cost.component.db.tables.records.BilledCostDailyRecord;
import com.vmturbo.cost.component.db.tables.records.CostTagGroupingRecord;
import com.vmturbo.cost.component.db.tables.records.CostTagRecord;
import com.vmturbo.platform.sdk.common.CommonCost;
import com.vmturbo.platform.sdk.common.CostBilling;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit tests for BilledCostUploadRpcService.
 */
public class BilledCostUploadRpcServiceTest {

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(com.vmturbo.cost.component.db.Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();
    private final DSLContext context = dbConfig.getDslContext();

    private static final String BILLING_ID = "1111111";
    private static final long ACCOUNT_ID = 2222222L;
    private static final long CLOUD_SERVICE_ID = 333333L;
    private static final long ENTITY_ID = 444444L;
    private static final long ENTITY_ID_2 = 454545L;
    private static final long TAG_GROUP_ID = 1234567L;
    private static final long TAG_GROUP_ID_2 = 3243455L;
    private static final long TIMESTAMP = TimeUnit.DAYS.toMillis(1);
    private static final double COST = 10;
    private static final double DELTA = 0D;
    private static final long DEFAULT_TAG_GROUP_ID = 0L;
    private static final Map<String, String> TAG_GROUP_OWNER_PROJECT = ImmutableMap.of("owner", "alice", "project",
        "science");
    private static final Map<String, String> TAG_GROUP_OWNER = ImmutableMap.of("owner", "alice");
    private static final Map<String, String> TAG_GROUP_OWNER_2 = ImmutableMap.of("owner", "bob");


    private BilledCostUploadClient client;
    private StreamObserver<Cost.UploadBilledCostRequest> requestStreamObserver;
    private BilledCostUploadRpcService billedCostUploadRpcService;
    private TagStore tagStore;
    private TagGroupStore tagGroupStore;
    private IdentityProvider identityProvider;

    /**
     * Initialize test resources.
     *
     * @throws DbException if error encountered during db operations.
     * @throws ExecutionException if error encountered waiting on future.
     * @throws java.lang.InterruptedException if waiting on future is interrupted.
     */
    @Before
    public void setup() throws DbException, ExecutionException, InterruptedException {
        identityProvider = new IdentityProvider.DefaultIdentityProvider(4);
        final BatchInserter batchInserter = new BatchInserter(10, 1);
        tagStore = spy(new TagStore(context, batchInserter));
        tagGroupStore = spy(new TagGroupStore(context, batchInserter));
        final BilledCostStore billedCostStore = createBilledCostStore(false);
        initializeUploadClientAndService(billedCostStore);
    }

    private TagGroupIdentityService createTagGroupIdentityResolver() {
        final TagIdentityService tagIdentityService = new TagIdentityService(tagStore, identityProvider);
        return new TagGroupIdentityService(tagGroupStore, tagIdentityService, identityProvider);
    }

    private void initializeUploadClientAndService(final BilledCostStore billedCostStore) {
        billedCostUploadRpcService = new BilledCostUploadRpcService(createTagGroupIdentityResolver(), billedCostStore);
        client = new BilledCostUploadClient();
        requestStreamObserver = billedCostUploadRpcService.uploadBilledCost(client);
    }

    /**
     * Test that BilledCostUploadRpcService::uploadBilledCost sends onCompleted message to client when empty
     * UploadBilledCostRequest is sent to it.
     */
    @Test
    public void testEmptyBilledCostRequest() {
        requestStreamObserver.onNext(Cost.UploadBilledCostRequest.newBuilder().build());
        requestStreamObserver.onCompleted();
        Assert.assertTrue(client.getOnCompletedReceived());
    }

    /**
     * Test that when UploadBilledCostRequest contains a single DataChunk but without billingIdentifier set,
     * the DataChunk is not processed.
     */
    @Test
    public void testBillingCostRequestNoBillingIdentifier() {
        requestStreamObserver.onNext(Cost.UploadBilledCostRequest.newBuilder()
            .build());
        requestStreamObserver.onCompleted();
        Assert.assertNotNull(client.getResponse());
    }

    /**
     * Test that Billing Data point without a tag group id set is persisted successfully.
     */
    @Test
    public void testProcessBillingDataPointNoTagGroupIdSet() {
        final Cost.UploadBilledCostRequest.Builder request = addBillingDataPoint(createUploadRequest(),
            createBillingDataPoint(ENTITY_ID, null));
        requestStreamObserver.onNext(request.build());
        requestStreamObserver.onCompleted();
        verifyBilledCostPointPersisted(ENTITY_ID, COST, DEFAULT_TAG_GROUP_ID);
    }

    /**
     * Test that Billing Data point with tag group id set which is not found in the Cost tag group data is not processed
     * or persisted.
     */
    @Test
    public void testProcessBillingDataPointTagGroupNotFound() {
        requestStreamObserver.onNext(addBillingDataPoint(createUploadRequest(),
            createBillingDataPoint(ENTITY_ID, TAG_GROUP_ID)).build());
        requestStreamObserver.onCompleted();
        verifyBilledCostPointNotPersisted(ENTITY_ID);
    }

    /**
     * Test that Billing Data point with tag group id set and found in the Cost tag group data is processed and
     * persisted.
     */
    @Test
    public void testProcessBillingDataPointTagGroupFound() {
        final Cost.UploadBilledCostRequest.Builder request = createUploadRequest();
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID, TAG_GROUP_ID));
        addTagGroup(request, TAG_GROUP_ID, TAG_GROUP_OWNER);
        requestStreamObserver.onNext(request.build());
        requestStreamObserver.onCompleted();
        final long tagId = verifyTagPersistedAndGetTagId("owner", "alice");
        verifyTagGroupsPersisted(tagId, 1);
        verifyBilledCostPointPersisted(ENTITY_ID, COST, retrieveTagGroupId(TAG_GROUP_OWNER));
    }

    /**
     * Test that two Billing data points with the same primary key values are merged and their cost column is summed up.
     */
    @Test
    public void testBillingDataPointsWithSameKeysMerged() {
        final Cost.UploadBilledCostRequest.Builder request = createUploadRequest();
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID, TAG_GROUP_ID));
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID, TAG_GROUP_ID));
        addTagGroup(request, TAG_GROUP_ID, TAG_GROUP_OWNER);
        requestStreamObserver.onNext(request.build());
        requestStreamObserver.onCompleted();
        verifyBilledCostPointPersisted(ENTITY_ID, COST * 2, retrieveTagGroupId(TAG_GROUP_OWNER));
    }

    /**
     * Test that an already stored billing data point is updated when a new point with the same unique keys is
     * encountered in a subsequent upload.
     */
    @Test
    public void testBillingPointUpdateNoTagGroup() {
        requestStreamObserver.onNext(addBillingDataPoint(createUploadRequest(),
            createBillingDataPoint(ENTITY_ID_2, null)).build());
        requestStreamObserver.onCompleted();
        // next upload with updated cost
        final StreamObserver<Cost.UploadBilledCostRequest> nextUploadObserver = billedCostUploadRpcService
            .uploadBilledCost(client);
        final double updatedCost = COST * 3;
        final Cost.UploadBilledCostRequest.BillingDataPoint.Builder point =
            createBillingDataPoint(ENTITY_ID_2, null).toBuilder().setCost(CommonCost.CurrencyAmount.newBuilder()
                .setAmount(updatedCost).build());
        nextUploadObserver.onNext(createUploadRequest().addSamples(point.build()).build());
        nextUploadObserver.onCompleted();
        verifyBilledCostPointPersisted(ENTITY_ID_2, updatedCost, DEFAULT_TAG_GROUP_ID);
    }

    /**
     * Test that two points, one with tag group id and one without tag group id are successfully persisted.
     */
    @Test
    public void testTwoPointInsertWithAndWithoutTagGroup() {
        final Cost.UploadBilledCostRequest.Builder request = createUploadRequest();
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID_2, TAG_GROUP_ID_2));
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID, null));
        addTagGroup(request, TAG_GROUP_ID_2, TAG_GROUP_OWNER_2);
        requestStreamObserver.onNext(request.build());
        requestStreamObserver.onCompleted();
        verifyBilledCostPointPersisted(ENTITY_ID, COST, DEFAULT_TAG_GROUP_ID);
        verifyBilledCostPointPersisted(ENTITY_ID_2, COST, retrieveTagGroupId(TAG_GROUP_OWNER_2));
    }

    /**
     * Test that two tag groups with same key but different values are persisted correctly.
     */
    @Test
    public void testBillingDataPointsDifferentTagGroups() {
        final Cost.UploadBilledCostRequest.Builder request = createUploadRequest();
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID, TAG_GROUP_ID));
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID_2, TAG_GROUP_ID_2));
        addTagGroup(request, TAG_GROUP_ID, TAG_GROUP_OWNER);
        addTagGroup(request, TAG_GROUP_ID_2, TAG_GROUP_OWNER_2);
        requestStreamObserver.onNext(request.build());
        requestStreamObserver.onCompleted();
        final long tagId1 = verifyTagPersistedAndGetTagId("owner", "alice");
        final long tagId2 = verifyTagPersistedAndGetTagId("owner", "bob");
        verifyTagGroupsPersisted(tagId1, 1);
        verifyTagGroupsPersisted(tagId2, 1);
        verifyBilledCostPointPersisted(ENTITY_ID, COST, retrieveTagGroupId(TAG_GROUP_OWNER));
        verifyBilledCostPointPersisted(ENTITY_ID_2, COST, retrieveTagGroupId(TAG_GROUP_OWNER_2));
    }

    /**
     * Test that in two successive Upload requests, the tag and tag groups are persisted in the first upload and the
     * same tag and tag groups exist after the second upload.
     */
    @Test
    public void testTagAndTagGroupInsertionMultipleUploads() {
        sendUploadRequest(requestStreamObserver);

        final long tagId1 = verifyTagPersistedAndGetTagId("owner", "alice");
        final long tagId2 = verifyTagPersistedAndGetTagId("project", "science");
        verifyTagGroupsPersisted(tagId1, 2);
        verifyTagGroupsPersisted(tagId2, 1);
        requestStreamObserver.onCompleted();

        final StreamObserver<Cost.UploadBilledCostRequest> nextUploadRequest = billedCostUploadRpcService
            .uploadBilledCost(client);
        sendUploadRequest(nextUploadRequest);

        Assert.assertEquals(tagId1, verifyTagPersistedAndGetTagId("owner", "alice"));
        Assert.assertEquals(tagId2, verifyTagPersistedAndGetTagId("project", "science"));
        verifyTagGroupsPersisted(tagId1, 2);
        verifyTagGroupsPersisted(tagId2, 1);
        nextUploadRequest.onCompleted();
    }

    /**
     * Test that when 2 tags with the same keys but different casing are treated as separate tags with their own unique
     * ids. Azure supports case sensitivity only for tag values and not keys. AWS supports case sensitivity for both.
     */
    @Test
    public void testCaseSensitiveTags() {
        final Map<String, String> tagGroup1 = Collections.singletonMap("Owner", "Bob");
        final Map<String, String> tagGroup2 = Collections.singletonMap("owner", "bob");
        final Cost.UploadBilledCostRequest.Builder request = createUploadRequest();
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID, TAG_GROUP_ID));
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID_2, TAG_GROUP_ID_2));
        addTagGroup(request, TAG_GROUP_ID, tagGroup1);
        addTagGroup(request, TAG_GROUP_ID_2, tagGroup2);
        requestStreamObserver.onNext(request.build());
        requestStreamObserver.onCompleted();
        Assert.assertNotEquals(verifyTagPersistedAndGetTagId("Owner", "Bob"), verifyTagPersistedAndGetTagId("owner",
            "bob"));
    }

    /**
     * Test that tags with trailing spaces are de-duplicated to a single tag rather than separate tags. This follows
     * from not having NO PAD collation for the Cost Tag table.
     */
    @Test
    public void testTagsWithTrailingSpaces() {
        final Map<String, String> tagGroup1 = Collections.singletonMap("Owner ", "Bob");
        final Map<String, String> tagGroup2 = Collections.singletonMap("Owner", "Bob");
        final Cost.UploadBilledCostRequest.Builder request = createUploadRequest();
        addTagGroup(request, TAG_GROUP_ID, tagGroup1);
        addTagGroup(request, TAG_GROUP_ID_2, tagGroup2);
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID, TAG_GROUP_ID));
        addBillingDataPoint(request, createBillingDataPoint(ENTITY_ID_2, TAG_GROUP_ID_2));
        requestStreamObserver.onNext(request.build());
        requestStreamObserver.onCompleted();
        final long tagId = verifyTagPersistedAndGetTagId("Owner", "Bob");
        Assert.assertEquals(tagId, verifyTagPersistedAndGetTagId("Owner ", "Bob"));
        verifyTagGroupsPersisted(tagId, 1);
    }

    /**
     * Test that on failure to retrieve existing tag ids, the upload is failed.
     *
     * @throws com.vmturbo.sql.utils.DbException on encountering error during db operations.
     */
    @Test
    public void testExceptionWhileRetrievingExistingTags() throws DbException {
        when(tagStore.retrieveAllCostTags()).thenThrow(new DbException(""));
        sendUploadRequest(requestStreamObserver);
        requestStreamObserver.onCompleted();
        Assert.assertTrue(client.getErrorReceived());
    }

    /**
     * Test that on failure to insert new tag ids, the upload is failed.
     *
     * @throws DbException if error encountered during db operations.
     */
    @Test
    public void testExceptionWhileInsertingNewTags() throws DbException {
        doThrow(new DbException("")).when(tagStore).insertCostTagRecords(any());
        sendUploadRequest(requestStreamObserver);
        requestStreamObserver.onCompleted();
        Assert.assertTrue(client.getErrorReceived());
    }

    /**
     * Test that on failure to retrieve existing tag group ids, the upload is failed.
     *
     * @throws com.vmturbo.sql.utils.DbException on encountering an error during db operations.
     */
    @Test
    public void testExceptionWhileRetrievingTagGroups() throws DbException {
        when(tagGroupStore.retrieveAllTagGroups()).thenThrow(new DbException(""));
        sendUploadRequest(requestStreamObserver);
        requestStreamObserver.onCompleted();
        Assert.assertTrue(client.getErrorReceived());
    }

    /**
     * Test that on failure to insert new tag groups, the upload is failed.
     *
     * @throws DbException if error encountered during db operation.
     */
    @Test
    public void testExceptionWhileInsertingNewTagGroups() throws DbException {
        doThrow(new DbException("")).when(tagGroupStore).insertCostTagGroups(any());
        sendUploadRequest(requestStreamObserver);
        requestStreamObserver.onCompleted();
        Assert.assertTrue(client.getErrorReceived());
    }

    /**
     * Test that on failure to insert billing data points, the upload is failed.
     *
     * @throws ExecutionException if error encountered waiting on future.
     * @throws java.lang.InterruptedException if waiting on future is interrupted.
     */
    @Test
    public void testExceptionWhileInsertingBillingDataPoints() throws ExecutionException, InterruptedException {
        final BilledCostStore billedCostStore = createBilledCostStore(true);
        initializeUploadClientAndService(billedCostStore);
        sendUploadRequest(requestStreamObserver);
        requestStreamObserver.onCompleted();
        Assert.assertTrue(client.getErrorReceived());
        final long tagId1 = verifyTagPersistedAndGetTagId("owner", "alice");
        verifyTagGroupsPersisted(tagId1, 2);
    }

    private BilledCostStore createBilledCostStore(boolean throwsExceptionOnInsert) throws ExecutionException,
        InterruptedException {
        final BatchInserter batchInserter = spy(new BatchInserter(10, 1));
        if (throwsExceptionOnInsert) {
            final Future<?> future = mock(Future.class);
            when(future.get()).thenThrow(new ExecutionException(new DbException("")));
            doReturn(Collections.singletonList(future))
                .when(batchInserter).insertAsync(anyListOf(Record.class), any(Table.class), any(DSLContext.class),
                any(boolean.class));
        }
        return new SqlBilledCostStore(context, batchInserter);
    }

    private void sendUploadRequest(final StreamObserver<Cost.UploadBilledCostRequest> requestStreamObserver) {
        final Cost.UploadBilledCostRequest.Builder request1 = createUploadRequest();
        addTagGroup(request1, TAG_GROUP_ID, TAG_GROUP_OWNER_PROJECT);
        addBillingDataPoint(request1, createBillingDataPoint(ENTITY_ID, TAG_GROUP_ID));
        requestStreamObserver.onNext(request1.build());

        final Cost.UploadBilledCostRequest.Builder request2 = createUploadRequest();
        addTagGroup(request2, TAG_GROUP_ID_2, TAG_GROUP_OWNER);
        addBillingDataPoint(request2, createBillingDataPoint(ENTITY_ID_2, TAG_GROUP_ID_2));
        requestStreamObserver.onNext(request2.build());
    }

    private void verifyBilledCostPointPersisted(final long entityId, final double expectedCost,
                                                final long expectedTagGroupId) {
        final Result<BilledCostDailyRecord> billedCostEntries = context.selectFrom(BilledCostDaily.BILLED_COST_DAILY)
            .where(BilledCostDaily.BILLED_COST_DAILY.ENTITY_ID.eq(entityId))
            .fetch();
        Assert.assertEquals(1, billedCostEntries.size());
        final BilledCostDailyRecord record = billedCostEntries.iterator().next();
        Assert.assertEquals(expectedCost, record.getCost(), DELTA);
        Assert.assertEquals(expectedTagGroupId, (long)record.getTagGroupId());
    }

    private void verifyBilledCostPointNotPersisted(final long entityId) {
        Assert.assertTrue(context.selectFrom(BilledCostDaily.BILLED_COST_DAILY)
            .where(BilledCostDaily.BILLED_COST_DAILY.ENTITY_ID.eq(entityId))
            .fetch().isEmpty());
    }

    private void verifyTagGroupsPersisted(final long tagId, final long expectedNumTagGroups) {
        final Result<CostTagGroupingRecord> tagGroupingRecords = context.selectFrom(CostTagGrouping.COST_TAG_GROUPING)
            .where(CostTagGrouping.COST_TAG_GROUPING.TAG_ID.eq(tagId))
            .fetch();
        Assert.assertEquals(expectedNumTagGroups, tagGroupingRecords.size());
    }

    private long verifyTagPersistedAndGetTagId(final String key, final String value) {
        final Result<CostTagRecord> tagRecords = context.selectFrom(CostTag.COST_TAG)
            .where(CostTag.COST_TAG.TAG_KEY.eq(key)
                .and(CostTag.COST_TAG.TAG_VALUE.eq(value)))
            .fetch();
        Assert.assertEquals(1, tagRecords.size());
        return tagRecords.iterator().next().getTagId();
    }

    private long retrieveTagGroupId(final Map<String, String> tagGroup) {
        final Set<Long> tagIds = tagGroup.entrySet().stream()
            .map(e -> verifyTagPersistedAndGetTagId(e.getKey(), e.getValue())).collect(Collectors.toSet());
        Map<Long, Set<Long>> tagGroups = context.selectFrom(CostTagGrouping.COST_TAG_GROUPING).fetch().stream()
            .collect(Collectors.groupingBy(CostTagGroupingRecord::getTagGroupId,
                Collectors.mapping(CostTagGroupingRecord::getTagId, Collectors.toSet())));
        final Long tagGroupId = tagGroups.entrySet().stream().filter(groupEntry -> groupEntry.getValue().equals(tagIds))
            .map(Map.Entry::getKey)
            .findAny().orElse(null);
        Assert.assertNotNull(tagGroupId);
        return tagGroupId;
    }

    private Cost.UploadBilledCostRequest.Builder addTagGroup(final Cost.UploadBilledCostRequest.Builder request,
                                                             final long tagGroupId, final Map<String, String> tags) {
        return request.addCostTagGroupMap(Cost.UploadBilledCostRequest.CostTagGroupMap.newBuilder()
            .setGroupId(tagGroupId).setTags(CostBilling.CostTagGroup.newBuilder()
                .putAllTags(tags)
                .build()));
    }

    private Cost.UploadBilledCostRequest.Builder createUploadRequest() {
        return Cost.UploadBilledCostRequest.newBuilder()
            .setBillingIdentifier(BILLING_ID)
            .setGranularity(CostBilling.CloudBillingData.CloudBillingBucket.Granularity.DAILY);
    }

    private Cost.UploadBilledCostRequest.Builder addBillingDataPoint(Cost.UploadBilledCostRequest.Builder request,
        final Cost.UploadBilledCostRequest.BillingDataPoint point) {
        return request.addSamples(point);
    }

    private Cost.UploadBilledCostRequest.BillingDataPoint createBillingDataPoint(final long entityId,
                                                                                 final Long tagGroupId) {
        final Cost.UploadBilledCostRequest.BillingDataPoint.Builder builder =
            Cost.UploadBilledCostRequest.BillingDataPoint.newBuilder()
                .setTimestampUtcMillis(TIMESTAMP)
                .setAccountOid(ACCOUNT_ID)
                .setCloudServiceOid(CLOUD_SERVICE_ID)
                .setEntityOid(entityId)
                .setPriceModel(CommonCost.PriceModel.ON_DEMAND)
                .setCostCategory(CostBilling.CloudBillingDataPoint.CostCategory.COMPUTE)
                .setUsageAmount(5000)
                .setCost(CommonCost.CurrencyAmount.newBuilder()
                    .setAmount(COST)
                    .build());
        if (tagGroupId != null) {
            builder.setCostTagGroupId(tagGroupId);
        }
        return builder.build();
    }

    private static class BilledCostUploadClient implements StreamObserver<Cost.UploadBilledCostResponse> {
        private boolean onCompletedReceived = false;
        private boolean errorReceived = false;
        private Cost.UploadBilledCostResponse response;

        @Override
        public void onNext(Cost.UploadBilledCostResponse uploadBilledCostResponse) {
            response = uploadBilledCostResponse;
        }

        @Override
        public void onError(Throwable throwable) {
            errorReceived = true;
        }

        @Override
        public void onCompleted() {
            onCompletedReceived = true;
        }

        public boolean getOnCompletedReceived() {
            return onCompletedReceived;
        }

        public Cost.UploadBilledCostResponse getResponse() {
            return response;
        }

        public boolean getErrorReceived() {
            return errorReceived;
        }
    }
}